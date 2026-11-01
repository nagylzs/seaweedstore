package seaweedstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"mime"
	"net/http"
	"strings"
	"time"
)

// Assign asks the leader for a file id, or the other masters, which forward the request, when it
// can't reach the leader or there is none. While the answer is HTTP 503, as while SeaweedFS creates
// volumes or a new leader warms up, it asks again after the answer's Retry-After (1 second without
// one), until ctx is done. It doesn't retry other errors: HTTP 406, for example, can mean that there
// is no room, but SeaweedFS also answers it for a while after an election.
func (c *Cluster) Assign(ctx context.Context, opts AssignOptions) (*AssignResult, error) {
	if err := c.closed(); err != nil {
		return nil, err
	}
	for {
		var answer assignAnswer
		err := c.askMasters(ctx, "/dir/assign?"+opts.query(), &answer)
		var he *HTTPError
		if !errors.As(err, &he) || he.StatusCode != http.StatusServiceUnavailable {
			if err != nil {
				return nil, err
			}
			return &AssignResult{Fid: answer.Fid, Addr: answer.Url, PublicAddr: answer.PublicUrl, Count: answer.Count}, nil
		}
		wait := he.RetryAfter
		if wait <= 0 {
			wait = defaultRetryAfter
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("%w; the last answer: %w", ctx.Err(), err)
		case <-time.After(wait):
		}
	}
}

// quoteEscaper escapes a quoted string in a header, like mime/multipart. SeaweedFS reads these
// escapes in the file name, but not the RFC 2231 filename* parameter, so a name is never encoded.
var quoteEscaper = strings.NewReplacer(`\`, `\\`, `"`, `\"`)

// Upload uploads a file to the fid of an AssignResult, and returns what the volume server stored.
// The name and the content type are optional. Of a name with a path, SeaweedFS keeps only the part
// after the last slash or backslash.
//
// A negative size means that the size is not known. Otherwise the body must give exactly that many
// bytes, and the volume server must store them all: Upload fails when it reports another size. (It
// stores an empty file for the content type application/x-www-form-urlencoded, for example.)
//
// Only ctx limits the upload: the volume server answers after it has stored all copies, which takes
// longer for larger files.
func (c *Cluster) Upload(ctx context.Context, a *AssignResult, body io.Reader, size int64, name, contentType string) (*UploadResult, error) {
	if err := c.closed(); err != nil {
		return nil, err
	}
	if _, err := ParseFid(a.Fid); err != nil {
		return nil, err
	}
	header := http.Header{}
	if name != "" {
		header.Set("Content-Disposition", `attachment; filename="`+quoteEscaper.Replace(name)+`"`)
	}
	if contentType != "" {
		header.Set("Content-Type", contentType)
	}
	res, err := c.send(ctx, http.MethodPut, "http://"+a.Addr+"/"+a.Fid, header, body, size, 0)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusCreated {
		return nil, fmt.Errorf("upload to %s: %w", a.Addr, newHTTPError(res))
	}
	var answer uploadAnswer
	if err := json.NewDecoder(res.Body).Decode(&answer); err != nil {
		return nil, fmt.Errorf("upload to %s: %w", a.Addr, err)
	}
	if size >= 0 && answer.Size != size {
		return nil, fmt.Errorf("upload to %s: the volume server stored %d bytes of %d", a.Addr, answer.Size, size)
	}
	return &UploadResult{Name: answer.Name, Size: answer.Size, ETag: answer.ETag, ContentType: answer.Mime}, nil
}

// Get reads a file from a volume server that has a copy (see fileRequest). The header, with Range or
// If-None-Match for example, is sent with the request. It returns the response for success and
// redirect statuses, also for 206 and 304; you must close its body. It returns ErrNotFound when no
// copy has the file, and an *HTTPError for other error statuses.
func (c *Cluster) Get(ctx context.Context, fid string, header http.Header) (*http.Response, error) {
	res, err := c.fileRequest(ctx, http.MethodGet, fid, header)
	if err != nil {
		return nil, err
	}
	if res.StatusCode >= 400 {
		defer res.Body.Close()
		return nil, newHTTPError(res)
	}
	return res, nil
}

// Stat returns what a volume server that has a copy says about a file (see fileRequest), or
// ErrNotFound.
func (c *Cluster) Stat(ctx context.Context, fid string) (*FileInfo, error) {
	// With gzip, SeaweedFS would send the compressed size, and net/http would drop it.
	res, err := c.fileRequest(ctx, http.MethodHead, fid, http.Header{"Accept-Encoding": {"identity"}})
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return nil, newHTTPError(res)
	}
	info := &FileInfo{
		Size:        res.ContentLength,
		ContentType: res.Header.Get("Content-Type"),
		ETag:        strings.Trim(res.Header.Get("Etag"), `"`),
	}
	if t, err := http.ParseTime(res.Header.Get("Last-Modified")); err == nil {
		info.LastModified = t
	}
	if _, params, err := mime.ParseMediaType(res.Header.Get("Content-Disposition")); err == nil {
		info.Name = params["filename"]
	}
	return info, nil
}

// Delete deletes a file from a volume server that has a copy (see fileRequest), which deletes the
// other copies too. It returns ErrNotFound when no copy has the file.
func (c *Cluster) Delete(ctx context.Context, fid string) error {
	res, err := c.fileRequest(ctx, http.MethodDelete, fid, nil)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusAccepted && res.StatusCode != http.StatusOK {
		return newHTTPError(res)
	}
	return nil
}

// fileRequest sends a request for a file to the volume servers that have a copy, one after the
// other, until one answers with a status other than 404 or 5xx, and returns that response. A volume
// server that a request couldn't reach recently comes last (see tryOrder). Options.VolumeTimeout
// limits the wait for each one's response headers. It returns ErrNotFound when the volume doesn't
// exist or every copy answered 404, and the error of each copy otherwise.
func (c *Cluster) fileRequest(ctx context.Context, method, fid string, header http.Header) (*http.Response, error) {
	volumeID, err := ParseFid(fid)
	if err != nil {
		return nil, err
	}
	locations, err := c.LookupVolume(ctx, volumeID)
	if err != nil {
		return nil, err
	}
	var errs []error
	notFound := 0
	for _, loc := range c.tryOrder(locations) {
		res, err := c.send(ctx, method, loc.URL("/"+fid), header, nil, 0, c.options.VolumeTimeout)
		if err != nil {
			if ctx.Err() != nil {
				return nil, err
			}
			c.setReachable(loc.Addr, false)
			errs = append(errs, err)
			continue
		}
		c.setReachable(loc.Addr, true)
		if res.StatusCode != http.StatusNotFound && res.StatusCode < 500 {
			return res, nil
		}
		if res.StatusCode == http.StatusNotFound {
			notFound++
		}
		errs = append(errs, fmt.Errorf("%s: %w", loc.Addr, newHTTPError(res)))
		res.Body.Close()
	}
	if len(errs) == 0 {
		return nil, fmt.Errorf("no location of volume %d", volumeID)
	}
	if notFound == len(errs) {
		return nil, fmt.Errorf("%s: %w", fid, ErrNotFound)
	}
	return nil, errors.Join(errs...)
}

// tryOrder shuffles the locations, and moves those in Options.DataCenter to the front, and those that
// a request couldn't reach recently to the end. It changes the given slice.
func (c *Cluster) tryOrder(locations []VolumeLocation) []VolumeLocation {
	rand.Shuffle(len(locations), func(i, j int) { locations[i], locations[j] = locations[j], locations[i] })
	if dc := c.options.DataCenter; dc != "" {
		var local, other []VolumeLocation
		for _, l := range locations {
			if l.DataCenter == dc {
				local = append(local, l)
			} else {
				other = append(other, l)
			}
		}
		locations = append(local, other...)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return failedLast(c, locations, func(l VolumeLocation) string { return l.Addr })
}
