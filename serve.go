package seaweedstore

import (
	"io"
	"net/http"
)

// forwardedHeaders are the request headers that ServeFile sends to the volume server.
var forwardedHeaders = []string{"Range", "If-Range", "If-None-Match", "If-Modified-Since", "Accept-Encoding"}

// bodyHeaders are the response headers that describe the body. ServeFile always takes them from the
// volume server.
var bodyHeaders = []string{"Content-Length", "Content-Range", "Content-Encoding", "Etag", "Last-Modified", "Accept-Ranges"}

// keptHeaders are the response headers that ServeFile takes from the volume server only if w doesn't
// have them yet.
var keptHeaders = []string{"Content-Type", "Content-Disposition"}

// ServeFile serves a file to an HTTP client, for GET and HEAD requests; other methods get 405. It
// sends the client's Range, If-Range, If-None-Match, If-Modified-Since and Accept-Encoding to a volume
// server that has a copy (see Get), and answers with its status (200, 206, 304, 416, ...), the
// headers that describe the body, and the body.
//
// Headers that w already has are kept, so set Content-Type, Content-Disposition or Cache-Control
// before calling ServeFile to override or add them. Content-Length, Content-Range, Content-Encoding,
// ETag, Last-Modified and Accept-Ranges always come from the volume server, and ServeFile adds
// Vary: Accept-Encoding.
//
// A request with several ranges gets the whole file, as RFC 9110 allows. A range is always asked for
// without compression, because SeaweedFS answers a range request with
// gzip with a range of the compressed file, under the same ETag as the uncompressed one. Without an
// Accept-Encoding from the client, the file is asked for without compression too, so that net/http
// doesn't decompress it unasked.
//
// When ServeFile returns an error, it has written nothing, so the caller can answer, or try another
// cluster: ErrNotFound when no copy has the file, the reason when no copy could be reached, and the
// error of an invalid fid or a closed cluster. Once it has written the response, it returns nil, even
// if the client went away while it sent the body.
func (c *Cluster) ServeFile(w http.ResponseWriter, r *http.Request, fid string) error {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return nil
	}
	header := http.Header{}
	for _, k := range forwardedHeaders {
		if vs := r.Header.Values(k); len(vs) > 0 {
			header[k] = vs
		}
	}
	if header.Get("Range") != "" || header.Get("Accept-Encoding") == "" {
		header.Set("Accept-Encoding", "identity")
	}
	res, err := c.open(r.Context(), r.Method, fid, header)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	h := w.Header()
	for _, k := range bodyHeaders {
		h.Del(k)
		if vs := res.Header.Values(k); len(vs) > 0 {
			h[k] = vs
		}
	}
	for _, k := range keptHeaders {
		if vs := res.Header.Values(k); len(vs) > 0 && h.Get(k) == "" {
			h[k] = vs
		}
	}
	h.Add("Vary", "Accept-Encoding")
	w.WriteHeader(res.StatusCode)
	io.Copy(w, res.Body) // empty for HEAD
	return nil
}
