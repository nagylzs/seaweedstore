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
// timeout limits the wait for the response headers, after the body has been sent; zero means no
// limit. Sending the body and reading the response body are limited by ctx only.
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
	if body != nil && size >= 0 {
		req.ContentLength = size
	}
	d := &deadline{timeout: timeout, cancel: cancel}
	if req.Body == nil || req.Body == http.NoBody {
		d.start()
	} else {
		req.Body = &startOnEOF{ReadCloser: req.Body, d: d}
		if getBody := req.GetBody; getBody != nil {
			// net/http sends the body again from GetBody when it retries on a new connection
			req.GetBody = func() (io.ReadCloser, error) {
				b, err := getBody()
				if err != nil {
					return nil, err
				}
				return &startOnEOF{ReadCloser: b, d: d}, nil
			}
		}
	}
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

// startOnEOF starts the deadline when the request body has been read, that is, sent. net/http reads
// a body of known length once more to see that it has ended.
type startOnEOF struct {
	io.ReadCloser
	d *deadline
}

func (b *startOnEOF) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err == io.EOF {
		b.d.start()
	}
	return n, err
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
