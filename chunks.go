package seaweedstore

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

// manifestType is the content type of the chunk manifests that Upload stores. SeaweedFS keeps it only
// on the copy that got the upload, so it doesn't tell a manifest from a file; manifestPrefix does.
const manifestType = "application/vnd.seaweedstore.manifest+json"

// manifestPrefix is how the manifests that Upload stores begin, which tells them from files when they
// are read with ?cm=false. SeaweedFS ignores the field.
var manifestPrefix = []byte(`{"seaweedstore":1,`)

// maxManifestSize limits the manifests that are read: one for a file of a terabyte in chunks of 32 MB
// has about 2 MB.
const maxManifestSize = 16 << 20

// chunkManifest is a SeaweedFS chunk manifest: the file is its chunks one after the other.
type chunkManifest struct {
	Seaweedstore int         `json:"seaweedstore,omitempty"` // 1 in the manifests that Upload stores
	Name         string      `json:"name,omitempty"`
	Mime         string      `json:"mime,omitempty"`
	Size         int64       `json:"size"`
	Chunks       []chunkInfo `json:"chunks"`
}

type chunkInfo struct {
	Fid    string `json:"fid"`
	Offset int64  `json:"offset"`
	Size   int64  `json:"size"`
}

// check returns an error if the chunks don't make up the file one after the other.
func (m *chunkManifest) check() error {
	var offset int64
	for i, ch := range m.Chunks {
		if _, err := ParseFid(ch.Fid); err != nil {
			return fmt.Errorf("chunk %d: %w", i, err)
		}
		if ch.Offset != offset || ch.Size <= 0 {
			return fmt.Errorf("chunk %d has %d bytes at %d, want it at %d", i, ch.Size, ch.Offset, offset)
		}
		offset += ch.Size
	}
	if offset != m.Size {
		return fmt.Errorf("the chunks have %d bytes, the file %d", offset, m.Size)
	}
	return nil
}

// baseName returns the part of a file name after the last slash or backslash, like SeaweedFS keeps
// it for an upload. It keeps the whole name in a manifest.
func baseName(name string) string {
	return name[strings.LastIndexAny(name, `/\`)+1:]
}

// uploadChunked uploads a file in chunks of Options.ChunkSize, each to a fid of its own, and then the
// manifest of the chunks to the fid of a. With an unknown size, it first reads a chunk, and uploads a
// file that turns out to be smaller as it is. When it fails, it deletes the chunks it has uploaded.
func (c *Cluster) uploadChunked(ctx context.Context, a *AssignResult, body io.Reader, size int64, name, contentType string) (*UploadResult, error) {
	chunkSize := c.options.ChunkSize
	if size < 0 {
		first := make([]byte, chunkSize+1)
		n, err := io.ReadFull(body, first)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return c.uploadFile(ctx, a, bytes.NewReader(first[:n]), int64(n), name, contentType, false)
		}
		if err != nil {
			return nil, err
		}
		body = io.MultiReader(bytes.NewReader(first), body)
	}
	br := bufio.NewReader(body)
	m := chunkManifest{Seaweedstore: 1, Name: baseName(name), Mime: contentType}
	opts := a.opts
	opts.Count = 0
	fail := func(err error) (*UploadResult, error) {
		c.deleteChunks(ctx, m.Chunks)
		return nil, fmt.Errorf("upload in chunks to %s: %w", a.Fid, err)
	}
	for size < 0 || m.Size < size {
		if size < 0 {
			if _, err := br.Peek(1); err == io.EOF {
				break
			} else if err != nil {
				return fail(err)
			}
		}
		n := chunkSize
		if size >= 0 {
			n = min(chunkSize, size-m.Size)
		}
		ca, err := c.Assign(ctx, opts)
		if err != nil {
			return fail(err)
		}
		chunk := &countingReader{r: io.LimitReader(br, n)}
		sent := n
		if size < 0 {
			sent = -1 // it may be shorter
		}
		ur, err := c.uploadFile(ctx, ca, chunk, sent, "", "application/octet-stream", false)
		if err == nil && ur.Size != chunk.n {
			err = fmt.Errorf("the volume server stored %d bytes of %d", ur.Size, chunk.n)
		}
		if err != nil {
			err = fmt.Errorf("chunk %d: %w", len(m.Chunks), err)
			m.Chunks = append(m.Chunks, chunkInfo{Fid: ca.Fid}) // the volume server may have stored it
			return fail(err)
		}
		m.Chunks = append(m.Chunks, chunkInfo{Fid: ca.Fid, Offset: m.Size, Size: ur.Size})
		m.Size += ur.Size
		if size < 0 && ur.Size < chunkSize {
			break
		}
	}
	if size >= 0 {
		if _, err := br.Peek(1); err == nil {
			return fail(fmt.Errorf("the body has more than %d bytes", size))
		} else if err != io.EOF {
			return fail(err)
		}
	}
	data, err := json.Marshal(m)
	if err != nil {
		return fail(err)
	}
	ur, err := c.uploadFile(ctx, a, bytes.NewReader(data), int64(len(data)), "", manifestType, true)
	if err != nil {
		return fail(fmt.Errorf("the manifest: %w", err))
	}
	result := &UploadResult{Name: m.Name, Size: m.Size, ETag: ur.ETag, ContentType: contentType}
	for _, ch := range m.Chunks {
		result.Chunks = append(result.Chunks, ch.Fid)
	}
	if result.Name == "" {
		result.Name = a.Fid // as SeaweedFS names a file without a name
	}
	return result, nil
}

// countingReader counts the bytes read from r.
type countingReader struct {
	r io.Reader
	n int64
}

func (cr *countingReader) Read(p []byte) (int, error) {
	n, err := cr.r.Read(p)
	cr.n += int64(n)
	return n, err
}

// deleteChunks deletes chunks of a failed upload, also when ctx is done, for at most a minute. It
// ignores errors: chunks that are left have no reference.
func (c *Cluster) deleteChunks(ctx context.Context, chunks []chunkInfo) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
	defer cancel()
	for _, ch := range chunks {
		c.Delete(ctx, ch.Fid)
	}
}

// open sends a GET or HEAD request for a file (see fileRequest), and returns the response; the caller
// must close its body. For a manifest that Upload stored, the library makes up the answer to a GET
// request: it reads the chunks itself (see chunkReader), and answers the Range, If-Range and
// conditional headers.
//
// SeaweedFS answers a HEAD request for a manifest from the manifest, without the chunks, so open
// passes it on. A GET request without a Range asks with ?cm=false, which returns a manifest as it is
// stored, and looks for manifestPrefix. With a Range, the answer would have only some of the
// manifest, so open asks with a HEAD request first whether the fid is a manifest. A manifest from
// another tool (without manifestPrefix) is joined by the volume server for a range, and returned as it
// is stored otherwise.
//
// A Range with several ranges is left out, so the whole file is sent, as RFC 9110 allows.
func (c *Cluster) open(ctx context.Context, method, fid string, header http.Header) (*http.Response, error) {
	if method == http.MethodHead {
		return c.fileRequest(ctx, method, fid, "", header)
	}
	if strings.Contains(header.Get("Range"), ",") {
		header = header.Clone()
		header.Del("Range")
		header.Del("If-Range")
	}
	if header.Get("Range") != "" {
		head, err := c.fileRequest(ctx, http.MethodHead, fid, "", nil)
		if err != nil {
			return nil, err
		}
		head.Body.Close()
		if head.Header.Get("X-File-Store") == "chunked" {
			m, manifestHeader, err := c.fetchManifest(ctx, fid)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", fid, err)
			}
			if m != nil {
				return c.manifestResponse(ctx, fid, m, header, manifestHeader), nil
			}
		}
		return c.fileRequest(ctx, method, fid, "", header)
	}
	res, err := c.fileRequest(ctx, method, fid, "cm=false", header)
	if err != nil {
		return nil, err
	}
	br := bufio.NewReader(res.Body)
	if prefix, _ := br.Peek(len(manifestPrefix)); !bytes.Equal(prefix, manifestPrefix) {
		res.Body = struct {
			io.Reader
			io.Closer
		}{br, res.Body}
		return res, nil
	}
	defer res.Body.Close()
	m, err := readManifest(br)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", fid, err)
	}
	return c.manifestResponse(ctx, fid, m, header, res.Header), nil
}

// fetchManifest reads the manifest at fid, and returns it with the headers of its answer. For a
// manifest without manifestPrefix, it returns nil.
func (c *Cluster) fetchManifest(ctx context.Context, fid string) (*chunkManifest, http.Header, error) {
	res, err := c.fileRequest(ctx, http.MethodGet, fid, "cm=false", nil)
	if err != nil {
		return nil, nil, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return nil, nil, fmt.Errorf("the manifest: %w", newHTTPError(res))
	}
	br := bufio.NewReader(res.Body)
	if prefix, _ := br.Peek(len(manifestPrefix)); !bytes.Equal(prefix, manifestPrefix) {
		return nil, nil, nil
	}
	m, err := readManifest(br)
	return m, res.Header, err
}

func readManifest(r io.Reader) (*chunkManifest, error) {
	var m chunkManifest
	if err := json.NewDecoder(io.LimitReader(r, maxManifestSize)).Decode(&m); err != nil {
		return nil, fmt.Errorf("invalid manifest: %w", err)
	}
	if err := m.check(); err != nil {
		return nil, fmt.Errorf("invalid manifest: %w", err)
	}
	return &m, nil
}

// notModified reports whether the conditional headers of a request say that the client has the
// version of the file with etag and lastModified.
func notModified(h http.Header, etag, lastModified string) bool {
	if inm := h.Get("If-None-Match"); inm != "" {
		for _, t := range strings.Split(inm, ",") {
			if t = strings.TrimSpace(t); t == "*" || strings.TrimPrefix(t, "W/") == strings.TrimPrefix(etag, "W/") {
				return true
			}
		}
		return false
	}
	ims, err1 := http.ParseTime(h.Get("If-Modified-Since"))
	mod, err2 := http.ParseTime(lastModified)
	return err1 == nil && err2 == nil && !mod.After(ims)
}

// manifestResponse makes up the response to a GET request for the file of a manifest. reqHeader is the
// header of the request, and manifestHeader the one of the answer with the manifest.
func (c *Cluster) manifestResponse(ctx context.Context, fid string, m *chunkManifest, reqHeader, manifestHeader http.Header) *http.Response {
	h := http.Header{}
	h.Set("Accept-Ranges", "bytes")
	contentType := m.Mime
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	h.Set("Content-Type", contentType)
	name := m.Name
	if name == "" {
		name = fid
	}
	h.Set("Content-Disposition", mime.FormatMediaType("inline", map[string]string{"filename": name}))
	etag, lastModified := manifestHeader.Get("Etag"), manifestHeader.Get("Last-Modified")
	for k, v := range map[string]string{"Etag": etag, "Last-Modified": lastModified} {
		if v != "" {
			h.Set(k, v)
		}
	}
	status, start, end := http.StatusOK, int64(0), m.Size
	if notModified(reqHeader, etag, lastModified) {
		for _, k := range []string{"Content-Type", "Content-Disposition", "Accept-Ranges"} {
			h.Del(k)
		}
		status, end = http.StatusNotModified, 0
	} else if r := reqHeader.Get("Range"); r != "" {
		ifRange := reqHeader.Get("If-Range")
		if ifRange == "" || ifRange == etag || ifRange == lastModified {
			if s, e, ok, satisfiable := parseRange(r, m.Size); ok && satisfiable {
				status, start, end = http.StatusPartialContent, s, e
				h.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", s, e-1, m.Size))
			} else if ok {
				status, start, end = http.StatusRequestedRangeNotSatisfiable, 0, 0
				h.Set("Content-Range", fmt.Sprintf("bytes */%d", m.Size))
			}
		}
	}
	if status != http.StatusNotModified {
		h.Set("Content-Length", strconv.FormatInt(end-start, 10))
	}
	var body io.ReadCloser = http.NoBody
	if end > start {
		body = &chunkReader{c: c, ctx: ctx, fid: fid, chunks: m.Chunks, pos: start, end: end}
	}
	return &http.Response{
		Status:        fmt.Sprintf("%d %s", status, http.StatusText(status)),
		StatusCode:    status,
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        h,
		Body:          body,
		ContentLength: end - start,
	}
}

// parseRange returns the range of a Range header for a file of size bytes, as [start, end). ok is false
// when the header doesn't have a single range of bytes: then the whole file is sent, as RFC 9110
// allows. satisfiable is false when the range is outside the file.
func parseRange(s string, size int64) (start, end int64, ok, satisfiable bool) {
	spec, found := strings.CutPrefix(s, "bytes=")
	if !found || strings.Contains(spec, ",") {
		return 0, 0, false, false
	}
	first, last, found := strings.Cut(strings.TrimSpace(spec), "-")
	if !found || first == "" && last == "" || first != "" && !onlyOf(first, "0123456789") ||
		last != "" && !onlyOf(last, "0123456789") {
		return 0, 0, false, false
	}
	if first == "" { // the last bytes
		n, err := strconv.ParseInt(last, 10, 64)
		if err != nil {
			return 0, 0, false, false
		}
		if n == 0 || size == 0 {
			return 0, 0, true, false
		}
		return max(0, size-n), size, true, true
	}
	a, err := strconv.ParseInt(first, 10, 64)
	if err != nil {
		return 0, 0, false, false
	}
	b := size - 1
	if last != "" {
		if b, err = strconv.ParseInt(last, 10, 64); err != nil || b < a {
			return 0, 0, false, false
		}
	}
	if a >= size {
		return 0, 0, true, false
	}
	return a, min(b+1, size), true, true
}

// chunkReader reads the bytes [pos, end) of a file from its chunks, each like a file (see
// fileRequest): from a copy in Options.DataCenter first, and from another copy when one can't be
// reached.
type chunkReader struct {
	c      *Cluster
	ctx    context.Context
	fid    string // of the manifest
	chunks []chunkInfo
	pos    int64
	end    int64
	cur    io.ReadCloser // the chunk being read
	left   int64         // the bytes of cur still to read
}

func (r *chunkReader) Read(p []byte) (int, error) {
	if r.pos >= r.end {
		return 0, io.EOF
	}
	if r.cur == nil {
		if err := r.openChunk(); err != nil {
			return 0, err
		}
	}
	if int64(len(p)) > r.left {
		p = p[:r.left]
	}
	n, err := r.cur.Read(p)
	r.pos += int64(n)
	r.left -= int64(n)
	if r.left == 0 {
		r.cur.Close()
		r.cur = nil
		return n, nil
	}
	if err == io.EOF {
		return n, fmt.Errorf("a chunk of %s ended early: %w", r.fid, io.ErrUnexpectedEOF)
	}
	return n, err
}

// openChunk starts reading the chunk that has the byte at pos.
func (r *chunkReader) openChunk() error {
	i := sort.Search(len(r.chunks), func(i int) bool { return r.chunks[i].Offset+r.chunks[i].Size > r.pos })
	ch := r.chunks[i]
	from, to := r.pos-ch.Offset, min(r.end, ch.Offset+ch.Size)-ch.Offset
	header := http.Header{"Accept-Encoding": {"identity"}}
	want := http.StatusOK
	if from > 0 || to < ch.Size {
		header.Set("Range", fmt.Sprintf("bytes=%d-%d", from, to-1))
		want = http.StatusPartialContent
	}
	res, err := r.c.fileRequest(r.ctx, http.MethodGet, ch.Fid, "", header)
	if err != nil {
		return fmt.Errorf("chunk %s of %s: %w", ch.Fid, r.fid, err)
	}
	if res.StatusCode != want {
		defer res.Body.Close()
		return fmt.Errorf("chunk %s of %s: %w", ch.Fid, r.fid, newHTTPError(res))
	}
	r.cur, r.left = res.Body, to-from
	return nil
}

func (r *chunkReader) Close() error {
	if r.cur != nil {
		r.cur.Close()
		r.cur = nil
	}
	r.pos = r.end
	return nil
}
