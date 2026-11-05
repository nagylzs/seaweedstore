package seaweedstore

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

// HTTPError is the error for an answer of a SeaweedFS server with an unexpected HTTP status.
type HTTPError struct {
	StatusCode int           // 406
	Status     string        // "406 Not Acceptable"
	Message    string        // the error field of the answer, if it has one
	RetryAfter time.Duration // from the Retry-After header, 0 if there is none
}

func (e *HTTPError) Error() string {
	if e.Message == "" {
		return "HTTP " + e.Status
	}
	return "HTTP " + e.Status + ": " + e.Message
}

// newHTTPError returns the error for an answer, with the error field of its body. It doesn't close
// the body.
func newHTTPError(res *http.Response) *HTTPError {
	e := &HTTPError{StatusCode: res.StatusCode, Status: res.Status}
	if s, err := strconv.Atoi(res.Header.Get("Retry-After")); err == nil && s > 0 {
		e.RetryAfter = time.Duration(s) * time.Second
	}
	var answer struct {
		Error string `json:"error"`
	}
	if json.NewDecoder(io.LimitReader(res.Body, 64<<10)).Decode(&answer) == nil {
		e.Message = answer.Error
	}
	return e
}

// send sends a request with the cluster's HTTP client. The caller must close the response body.
//
// A size of -1 means that the length of the body is not known, so net/http sends it in chunks. The
// timeout limits the wait for the response headers of a request without a body; zero means no limit.
// Sending a body and reading the response body are limited by ctx only.
func (c *Cluster) send(ctx context.Context, method, url string, header http.Header, body io.Reader, size int64, timeout time.Duration) (*http.Response, error) {
	ctx, cancel := context.WithCancel(ctx)
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		cancel()
		return nil, err
	}
	for k, vs := range header {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	if method == http.MethodPut || method == http.MethodDelete {
		// Both can be repeated for a fid, and this entry tells net/http so, with no header on the wire.
		// Then, like a read, it goes again on a new connection when a reused one fails, as when a volume
		// server closes it after its -idleTimeout just then. net/http does that for an upload only if
		// it was a single write: a body of a known in-memory type, not wrapped in anything, that fits in
		// its buffer with the headers. Other uploads rely on the IdleConnTimeout of the client (see
		// defaultClient).
		req.Header["Idempotency-Key"] = nil
	}
	if body != nil && size >= 0 {
		req.ContentLength = size
	}
	d := &deadline{timeout: timeout, cancel: cancel}
	d.start()
	res, err := c.client.Do(req)
	if d.stop() {
		if err == nil {
			res.Body.Close()
		}
		cancel()
		return nil, fmt.Errorf("%s %s: no response headers within %v: %w", method, url, timeout, os.ErrDeadlineExceeded)
	}
	if err != nil {
		cancel()
		return nil, err
	}
	res.Body = &cancelOnClose{ReadCloser: res.Body, cancel: cancel}
	return res, nil
}

// deadline cancels a request when its timeout has passed after start, unless stop comes first.
type deadline struct {
	timeout time.Duration // zero: no deadline
	cancel  context.CancelFunc
	mu      sync.Mutex
	timer   *time.Timer
	stopped bool
	expired bool
}

func (d *deadline) start() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.timeout <= 0 || d.timer != nil || d.stopped {
		return
	}
	d.timer = time.AfterFunc(d.timeout, func() {
		d.mu.Lock()
		defer d.mu.Unlock()
		if !d.stopped {
			d.expired = true
			d.cancel()
		}
	})
}

// stop reports whether the deadline has expired, and makes sure that it won't.
func (d *deadline) stop() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.stopped = true
	if d.timer != nil {
		d.timer.Stop()
	}
	return d.expired
}

// cancelOnClose releases the request's context when the response body is closed.
type cancelOnClose struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (b *cancelOnClose) Close() error {
	err := b.ReadCloser.Close()
	b.cancel()
	return err
}
