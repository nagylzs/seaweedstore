package seaweedstore

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

// fileHeaders are the headers of SeaweedFS 4.50 for hello.txt, without Content-Length, which the fake
// server sets.
var fileHeaders = map[string]string{
	"Accept-Ranges":       "bytes",
	"Content-Disposition": "inline; filename=hello.txt",
	"Content-Type":        "text/plain",
	"Etag":                `"c99465aa"`,
	"Last-Modified":       "Sun, 01 Nov 2026 13:22:08 GMT",
	"Server":              "SeaweedFS Volume 30GB 4.50",
	"X-Amz-Request-Id":    "18E3EE843B8422BABF34F100",
}

// withHeaders returns fileHeaders with the given changes.
func withHeaders(changes map[string]string) map[string]string {
	h := map[string]string{}
	for k, v := range fileHeaders {
		h[k] = v
	}
	for k, v := range changes {
		h[k] = v
	}
	return h
}

// serve calls ServeFile for a request with the given method and headers, on a cluster with one
// volume server that answers a.
func serve(t *testing.T, method string, header http.Header, a answer) (*httptest.ResponseRecorder, request, error) {
	t.Helper()
	vol := newFakeServer(t)
	vol.answerFirst("/"+fid, a)
	c := volumeCluster(t, quietOptions(), vol.addr())
	r := httptest.NewRequest(method, "/files/1", nil)
	for k, vs := range header {
		r.Header[k] = vs
	}
	w := httptest.NewRecorder()
	err := c.ServeFile(w, r, fid)
	return w, onlyRequest(t, vol, "/"+fid), err
}

// ServeFile answers with the volume server's status, body and the headers that describe the body,
// and adds Vary. Without an Accept-Encoding from the client, it asks for the file without
// compression.
func TestServeFile(t *testing.T) {
	w, req, err := serve(t, http.MethodGet, nil, answer{status: http.StatusOK, body: "hello world", header: fileHeaders})
	if err != nil {
		t.Fatal(err)
	}
	if w.Code != http.StatusOK || w.Body.String() != "hello world" {
		t.Errorf("answer %d %q", w.Code, w.Body.String())
	}
	want := map[string]string{
		"Accept-Ranges": "bytes", "Content-Disposition": "inline; filename=hello.txt", "Content-Length": "11",
		"Content-Type": "text/plain", "Etag": `"c99465aa"`, "Last-Modified": "Sun, 01 Nov 2026 13:22:08 GMT",
		"Vary": "Accept-Encoding",
	}
	for k, v := range want {
		if got := w.Header().Get(k); got != v {
			t.Errorf("%s: %q, want %q", k, got, v)
		}
	}
	for _, k := range []string{"Server", "X-Amz-Request-Id"} {
		if v := w.Header().Get(k); v != "" {
			t.Errorf("%s was copied: %q", k, v)
		}
	}
	if req.method != http.MethodGet || req.header.Get("Accept-Encoding") != "identity" {
		t.Errorf("the volume server got %s with Accept-Encoding %q", req.method, req.header.Get("Accept-Encoding"))
	}
}

// With the client's Accept-Encoding, the volume server may answer compressed, and ServeFile passes
// that on unchanged.
func TestServeFileGzip(t *testing.T) {
	gzipped := "\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\xff\xcaH\xcd\xc9\xc9W(\xcf/\xcaI\x01\x04\x00\x00\xff\xff\x85\x11J\r\x0b\x00\x00\x00"
	w, req, err := serve(t, http.MethodGet, http.Header{"Accept-Encoding": {"gzip, deflate"}},
		answer{status: http.StatusOK, body: gzipped, header: withHeaders(map[string]string{"Content-Encoding": "gzip"})})
	if err != nil {
		t.Fatal(err)
	}
	if req.header.Get("Accept-Encoding") != "gzip, deflate" {
		t.Errorf("the volume server got Accept-Encoding %q", req.header.Get("Accept-Encoding"))
	}
	if w.Header().Get("Content-Encoding") != "gzip" || w.Body.String() != gzipped {
		t.Errorf("Content-Encoding %q, body %q", w.Header().Get("Content-Encoding"), w.Body.String())
	}
}

// A range is asked for without compression, and the answer has the volume server's Content-Range.
func TestServeFileRange(t *testing.T) {
	w, req, err := serve(t, http.MethodGet, http.Header{"Range": {"bytes=0-4"}, "If-Range": {`"c99465aa"`}, "Accept-Encoding": {"gzip"}},
		answer{status: http.StatusPartialContent, body: "hello", header: withHeaders(map[string]string{"Content-Range": "bytes 0-4/11"})})
	if err != nil {
		t.Fatal(err)
	}
	if req.header.Get("Range") != "bytes=0-4" || req.header.Get("If-Range") != `"c99465aa"` || req.header.Get("Accept-Encoding") != "identity" {
		t.Errorf("the volume server got %v", req.header)
	}
	if w.Code != http.StatusPartialContent || w.Header().Get("Content-Range") != "bytes 0-4/11" || w.Body.String() != "hello" {
		t.Errorf("answer %d, Content-Range %q, body %q", w.Code, w.Header().Get("Content-Range"), w.Body.String())
	}
}

// Conditional requests are forwarded, and a 304 is passed on.
func TestServeFileNotModified(t *testing.T) {
	w, req, err := serve(t, http.MethodGet, http.Header{"If-None-Match": {`"c99465aa"`}, "If-Modified-Since": {"Sun, 01 Nov 2026 13:22:08 GMT"}},
		answer{status: http.StatusNotModified, header: fileHeaders})
	if err != nil {
		t.Fatal(err)
	}
	if req.header.Get("If-None-Match") != `"c99465aa"` || req.header.Get("If-Modified-Since") == "" {
		t.Errorf("the volume server got %v", req.header)
	}
	if w.Code != http.StatusNotModified || w.Body.Len() != 0 {
		t.Errorf("answer %d %q", w.Code, w.Body.String())
	}
}

// A range that the file doesn't have is answered with 416, not an error.
func TestServeFileRangeNotSatisfiable(t *testing.T) {
	w, _, err := serve(t, http.MethodGet, http.Header{"Range": {"bytes=100-"}},
		answer{status: http.StatusRequestedRangeNotSatisfiable, header: map[string]string{"Content-Range": "bytes */11"}})
	if err != nil {
		t.Fatal(err)
	}
	if w.Code != http.StatusRequestedRangeNotSatisfiable || w.Header().Get("Content-Range") != "bytes */11" {
		t.Errorf("answer %d, Content-Range %q", w.Code, w.Header().Get("Content-Range"))
	}
}

// A HEAD request gets the headers without the body.
func TestServeFileHead(t *testing.T) {
	w, req, err := serve(t, http.MethodHead, nil, answer{status: http.StatusOK, body: "hello world", header: withHeaders(map[string]string{"Content-Length": "11"})})
	if err != nil {
		t.Fatal(err)
	}
	if req.method != http.MethodHead || w.Code != http.StatusOK || w.Header().Get("Content-Length") != "11" || w.Body.Len() != 0 {
		t.Errorf("the volume server got %s; answer %d, Content-Length %q, body %q", req.method, w.Code, w.Header().Get("Content-Length"), w.Body.String())
	}
}

// Headers that the caller has set are kept, except those that describe the body, which are removed
// when the volume server doesn't send them.
func TestServeFileKeepsCallerHeaders(t *testing.T) {
	vol := newFakeServer(t)
	vol.answerFirst("/"+fid, answer{status: http.StatusOK, body: "hello world", header: fileHeaders})
	c := volumeCluster(t, quietOptions(), vol.addr())
	w := httptest.NewRecorder()
	w.Header().Set("Content-Disposition", `attachment; filename="greeting.txt"`)
	w.Header().Set("Cache-Control", "max-age=3600")
	w.Header().Set("Content-Length", "999")
	w.Header().Set("Content-Encoding", "br")
	if err := c.ServeFile(w, httptest.NewRequest(http.MethodGet, "/", nil), fid); err != nil {
		t.Fatal(err)
	}
	for k, v := range map[string]string{
		"Content-Disposition": `attachment; filename="greeting.txt"`, "Cache-Control": "max-age=3600",
		"Content-Length": "11", "Content-Type": "text/plain", "Content-Encoding": "",
	} {
		if got := w.Header().Get(k); got != v {
			t.Errorf("%s: %q, want %q", k, got, v)
		}
	}
}

func TestServeFileMethodNotAllowed(t *testing.T) {
	vol := newFakeServer(t)
	c := volumeCluster(t, quietOptions(), vol.addr())
	w := httptest.NewRecorder()
	if err := c.ServeFile(w, httptest.NewRequest(http.MethodDelete, "/", nil), fid); err != nil {
		t.Fatal(err)
	}
	if w.Code != http.StatusMethodNotAllowed || w.Header().Get("Allow") != "GET, HEAD" {
		t.Errorf("answer %d, Allow %q", w.Code, w.Header().Get("Allow"))
	}
	if n := len(vol.requestsTo("/" + fid)); n != 0 {
		t.Errorf("the volume server got %d requests", n)
	}
}

// When ServeFile returns an error, it has written nothing: for a missing file, for copies that can't
// be reached, and for an invalid fid.
func TestServeFileErrors(t *testing.T) {
	missing := newFakeServer(t)
	for _, tt := range []struct {
		name     string
		servers  []string
		fid      string
		notFound bool
	}{
		{"not found", []string{missing.addr()}, fid, true},
		{"unreachable", []string{closedPort, closedPort2}, fid, false},
		{"invalid fid", []string{missing.addr()}, "3,../status", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := volumeCluster(t, quietOptions(), tt.servers...)
			w := httptest.NewRecorder()
			err := c.ServeFile(w, httptest.NewRequest(http.MethodGet, "/", nil), tt.fid)
			if err == nil || errors.Is(err, ErrNotFound) != tt.notFound {
				t.Errorf("ServeFile returned %v", err)
			}
			if len(w.Header()) != 0 || w.Body.Len() != 0 || w.Flushed {
				t.Errorf("ServeFile wrote headers %v and %q", w.Header(), w.Body.String())
			}
		})
	}
}
