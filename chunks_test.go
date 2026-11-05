package seaweedstore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"testing"
)

// chunkOptions returns test options with chunks of 4 bytes.
func chunkOptions() Options {
	opts := quietOptions()
	opts.ChunkSize = 4
	return opts
}

// helloInChunks uploads "hello world" in chunks of 4 bytes as dir/hello.txt, to a fid of a cluster
// with one memVolume, and returns them.
func helloInChunks(t *testing.T, opts Options) (*Cluster, *memVolume, *AssignResult) {
	t.Helper()
	vol := newMemVolume(t, newMemStore())
	c, _ := memCluster(t, opts, vol.addr())
	ar, err := c.Assign(context.Background(), AssignOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Upload(context.Background(), ar, strings.NewReader("hello world"), 11, "dir/hello.txt", "text/plain"); err != nil {
		t.Fatal(err)
	}
	return c, vol, ar
}

// manifestOf returns the manifest that the store has at fid.
func manifestOf(t *testing.T, s *memStore, fid string) chunkManifest {
	t.Helper()
	f := s.file(fid)
	if f == nil || !f.manifest || f.contentType != manifestType {
		t.Fatalf("%s is not a manifest: %+v", fid, f)
	}
	var m chunkManifest
	if err := json.Unmarshal(f.data, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

// A file larger than ChunkSize is uploaded in chunks, each to a fid assigned with the options of the
// file's fid, and a manifest of them to the file's fid. The name loses its path, as in other uploads.
func TestUploadInChunks(t *testing.T) {
	for _, size := range []int64{11, -1} {
		vol := newMemVolume(t, newMemStore())
		c, leader := memCluster(t, chunkOptions(), vol.addr())
		ar, err := c.Assign(context.Background(), AssignOptions{Collection: "docs", Count: 2})
		if err != nil {
			t.Fatal(err)
		}
		ur, err := c.Upload(context.Background(), ar, io.MultiReader(strings.NewReader("hello world")), size, `C:\dir\hello.txt`, "text/plain")
		if err != nil {
			t.Fatalf("size %d: %v", size, err)
		}
		m := manifestOf(t, vol.store, ar.Fid)
		want := UploadResult{Name: "hello.txt", Size: 11, ETag: vol.store.file(ar.Fid).etag(), ContentType: "text/plain"}
		for _, ch := range m.Chunks {
			want.Chunks = append(want.Chunks, ch.Fid)
		}
		if !reflect.DeepEqual(*ur, want) {
			t.Errorf("size %d: Upload = %+v, want %+v", size, *ur, want)
		}
		if m.Name != "hello.txt" || m.Mime != "text/plain" || m.Size != 11 || len(m.Chunks) != 3 {
			t.Fatalf("size %d: manifest %+v", size, m)
		}
		for i, want := range []string{"hell", "o wo", "rld"} {
			ch := m.Chunks[i]
			f := vol.store.file(ch.Fid)
			if f == nil || string(f.data) != want || ch.Offset != int64(4*i) || ch.Size != int64(len(want)) || f.contentType != "application/octet-stream" {
				t.Errorf("size %d: chunk %d %+v is %+v, want %q", size, i, ch, f, want)
			}
		}
		assigns := leader.requestsTo("/dir/assign")
		if len(assigns) != 4 {
			t.Fatalf("size %d: %d assigns, want 4", size, len(assigns))
		}
		for _, r := range assigns[1:] {
			if r.query != "collection=docs" {
				t.Errorf("size %d: a chunk was assigned with %q", size, r.query)
			}
		}
	}
}

// The TTL of AssignOptions goes with the upload of a file, of each chunk and of the manifest, and with
// the assigns of the chunks. Without one, the uploads have no ttl.
func TestUploadSendsTTL(t *testing.T) {
	for _, tt := range []struct {
		ttl, small, chunk, manifest string
	}{
		{"3m", "ttl=3m", "ttl=3m", "cm=true&ttl=3m"},
		{"", "", "", "cm=true"},
	} {
		vol := newMemVolume(t, newMemStore())
		c, leader := memCluster(t, chunkOptions(), vol.addr())
		small, _ := c.Assign(context.Background(), AssignOptions{TTL: tt.ttl})
		if _, err := c.Upload(context.Background(), small, strings.NewReader("hell"), 4, "", ""); err != nil {
			t.Fatal(err)
		}
		large, _ := c.Assign(context.Background(), AssignOptions{TTL: tt.ttl})
		ur, err := c.Upload(context.Background(), large, strings.NewReader("hello world"), 11, "", "")
		if err != nil {
			t.Fatal(err)
		}
		if r := onlyRequest(t, vol.fakeServer, "/"+small.Fid); r.query != tt.small {
			t.Errorf("TTL %q: the upload has the query %q, want %q", tt.ttl, r.query, tt.small)
		}
		if r := onlyRequest(t, vol.fakeServer, "/"+large.Fid); r.query != tt.manifest {
			t.Errorf("TTL %q: the manifest has the query %q, want %q", tt.ttl, r.query, tt.manifest)
		}
		for _, chunk := range ur.Chunks {
			if r := onlyRequest(t, vol.fakeServer, "/"+chunk); r.query != tt.chunk {
				t.Errorf("TTL %q: chunk %s has the query %q, want %q", tt.ttl, chunk, r.query, tt.chunk)
			}
		}
		for _, r := range leader.requestsTo("/dir/assign")[2:] {
			if want := strings.TrimSuffix("ttl="+tt.ttl, "ttl="); r.query != want {
				t.Errorf("TTL %q: a chunk was assigned with %q, want %q", tt.ttl, r.query, want)
			}
		}
	}
}

// Fsync of AssignOptions goes with the upload of a file, of each chunk and of the manifest, but not to the
// master, which doesn't know it. Without it, the uploads have no fsync.
func TestUploadSendsFsync(t *testing.T) {
	for _, tt := range []struct {
		opts                           AssignOptions
		small, chunk, manifest, assign string
	}{
		{AssignOptions{Fsync: true}, "fsync=true", "fsync=true", "cm=true&fsync=true", ""},
		{AssignOptions{Fsync: true, TTL: "3m"}, "fsync=true&ttl=3m", "fsync=true&ttl=3m", "cm=true&fsync=true&ttl=3m", "ttl=3m"},
		{AssignOptions{}, "", "", "cm=true", ""},
	} {
		vol := newMemVolume(t, newMemStore())
		c, leader := memCluster(t, chunkOptions(), vol.addr())
		small, _ := c.Assign(context.Background(), tt.opts)
		if _, err := c.Upload(context.Background(), small, strings.NewReader("hell"), 4, "", ""); err != nil {
			t.Fatal(err)
		}
		large, _ := c.Assign(context.Background(), tt.opts)
		ur, err := c.Upload(context.Background(), large, strings.NewReader("hello world"), 11, "", "")
		if err != nil {
			t.Fatal(err)
		}
		if r := onlyRequest(t, vol.fakeServer, "/"+small.Fid); r.query != tt.small {
			t.Errorf("%+v: the upload has the query %q, want %q", tt.opts, r.query, tt.small)
		}
		if r := onlyRequest(t, vol.fakeServer, "/"+large.Fid); r.query != tt.manifest {
			t.Errorf("%+v: the manifest has the query %q, want %q", tt.opts, r.query, tt.manifest)
		}
		for _, chunk := range ur.Chunks {
			if r := onlyRequest(t, vol.fakeServer, "/"+chunk); r.query != tt.chunk {
				t.Errorf("%+v: chunk %s has the query %q, want %q", tt.opts, chunk, r.query, tt.chunk)
			}
		}
		for _, r := range leader.requestsTo("/dir/assign") {
			if r.query != tt.assign {
				t.Errorf("%+v: an assign has the query %q, want %q", tt.opts, r.query, tt.assign)
			}
		}
	}
}

// A file of a multiple of ChunkSize has no empty last chunk, also with an unknown size.
func TestUploadInChunksExactSize(t *testing.T) {
	for _, size := range []int64{8, -1} {
		vol := newMemVolume(t, newMemStore())
		c, _ := memCluster(t, chunkOptions(), vol.addr())
		ar, _ := c.Assign(context.Background(), AssignOptions{})
		if _, err := c.Upload(context.Background(), ar, strings.NewReader("hello wo"), size, "", ""); err != nil {
			t.Fatal(err)
		}
		if m := manifestOf(t, vol.store, ar.Fid); len(m.Chunks) != 2 || m.Size != 8 {
			t.Errorf("size %d: manifest %+v", size, m)
		}
	}
}

// A file of an unknown size that turns out not to be larger than ChunkSize is uploaded as it is.
func TestUploadUnknownSizeSmall(t *testing.T) {
	vol := newMemVolume(t, newMemStore())
	c, _ := memCluster(t, chunkOptions(), vol.addr())
	ar, _ := c.Assign(context.Background(), AssignOptions{})
	if _, err := c.Upload(context.Background(), ar, strings.NewReader("hell"), -1, "", ""); err != nil {
		t.Fatal(err)
	}
	if f := vol.store.file(ar.Fid); f == nil || f.manifest || string(f.data) != "hell" {
		t.Errorf("the file is %+v", f)
	}
}

// When an upload in chunks fails, the chunks that were uploaded are deleted: when the body has fewer
// or more bytes than the size, and when a volume server refuses a chunk.
func TestUploadInChunksFails(t *testing.T) {
	for _, tt := range []struct {
		name  string
		size  int64
		fail  bool // the second chunk
		error string
	}{
		{"fewer bytes", 20, false, "ContentLength"},
		{"more bytes", 9, false, "more than 9 bytes"},
		{"chunk refused", 11, true, "chunk 1"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			store := newMemStore()
			vol := newMemVolume(t, store)
			c, _ := memCluster(t, chunkOptions(), vol.addr())
			ar, _ := c.Assign(context.Background(), AssignOptions{})
			if tt.fail {
				store.fail["3,030000abcd"] = http.StatusInternalServerError
			}
			_, err := c.Upload(context.Background(), ar, io.MultiReader(strings.NewReader("hello world")), tt.size, "", "")
			if err == nil || !strings.Contains(err.Error(), tt.error) {
				t.Errorf("Upload returned %v, want an error with %q", err, tt.error)
			}
			if len(store.files) != 0 {
				t.Errorf("the store has %d files left", len(store.files))
			}
		})
	}
}

// Get reads a file in chunks whole, or a range of it across chunks, with the headers of the manifest.
func TestGetChunked(t *testing.T) {
	c, vol, ar := helloInChunks(t, chunkOptions())
	etag := `"` + vol.store.file(ar.Fid).etag() + `"`
	for _, tt := range []struct {
		name         string
		header       http.Header
		status       int
		body         string
		contentRange string
	}{
		{"whole", nil, 200, "hello world", ""},
		{"across chunks", http.Header{"Range": {"bytes=3-8"}}, 206, "lo wor", "bytes 3-8/11"},
		{"in one chunk", http.Header{"Range": {"bytes=5-6"}}, 206, " w", "bytes 5-6/11"},
		{"from", http.Header{"Range": {"bytes=8-"}}, 206, "rld", "bytes 8-10/11"},
		{"last bytes", http.Header{"Range": {"bytes=-3"}}, 206, "rld", "bytes 8-10/11"},
		{"beyond the end", http.Header{"Range": {"bytes=0-100"}}, 206, "hello world", "bytes 0-10/11"},
		{"several ranges", http.Header{"Range": {"bytes=0-1,3-4"}}, 200, "hello world", ""},
		{"invalid range", http.Header{"Range": {"bytes=x-"}}, 200, "hello world", ""},
		{"If-Range matches", http.Header{"Range": {"bytes=0-4"}, "If-Range": {etag}}, 206, "hello", "bytes 0-4/11"},
		{"If-Range doesn't match", http.Header{"Range": {"bytes=0-4"}, "If-Range": {`"other"`}}, 200, "hello world", ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			res, err := c.Get(context.Background(), ar.Fid, tt.header)
			if err != nil {
				t.Fatal(err)
			}
			defer res.Body.Close()
			body, err := io.ReadAll(res.Body)
			if err != nil || res.StatusCode != tt.status || string(body) != tt.body || res.Header.Get("Content-Range") != tt.contentRange {
				t.Errorf("%s %q (%v), Content-Range %q", res.Status, body, err, res.Header.Get("Content-Range"))
			}
			for k, v := range map[string]string{
				"Content-Type": "text/plain", "Content-Disposition": "inline; filename=hello.txt", "Etag": etag,
				"Accept-Ranges": "bytes",
			} {
				if got := res.Header.Get(k); got != v {
					t.Errorf("%s: %q, want %q", k, got, v)
				}
			}
			if res.ContentLength != int64(len(tt.body)) {
				t.Errorf("ContentLength %d", res.ContentLength)
			}
		})
	}
}

// A range outside the file is HTTP 416. The volume server answers the range of the manifest with
// 416 in text, so Get asks again whether the fid is a manifest.
func TestGetChunkedRangeNotSatisfiable(t *testing.T) {
	c, _, ar := helloInChunks(t, chunkOptions())
	_, err := c.Get(context.Background(), ar.Fid, http.Header{"Range": {"bytes=300-"}})
	var he *HTTPError
	if !errors.As(err, &he) || he.StatusCode != http.StatusRequestedRangeNotSatisfiable {
		t.Errorf("Get returned %v, want HTTP 416", err)
	}
	if _, err := c.Get(context.Background(), ar.Fid, http.Header{"Range": {"bytes=200-205"}}); !errors.As(err, &he) {
		t.Errorf("Get of a range beyond the file but within the manifest returned %v", err)
	}
}

// The volume server answers If-None-Match for the manifest, whose ETag is the file's.
func TestGetChunkedNotModified(t *testing.T) {
	c, vol, ar := helloInChunks(t, chunkOptions())
	res, err := c.Get(context.Background(), ar.Fid, http.Header{"If-None-Match": {`"` + vol.store.file(ar.Fid).etag() + `"`}})
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusNotModified {
		t.Errorf("status %s, want 304", res.Status)
	}
}

// Only the chunks that the range needs are read.
func TestGetChunkedReadsOnlyNeededChunks(t *testing.T) {
	c, vol, ar := helloInChunks(t, chunkOptions())
	m := manifestOf(t, vol.store, ar.Fid)
	res, err := c.Get(context.Background(), ar.Fid, http.Header{"Range": {"bytes=5-6"}})
	if err != nil {
		t.Fatal(err)
	}
	io.ReadAll(res.Body)
	res.Body.Close()
	for i, ch := range m.Chunks {
		n := len(vol.requestsTo("/" + ch.Fid))
		want := 0
		if i == 1 {
			want = 2 // the upload and the read
		} else {
			want = 1
		}
		if n != want {
			t.Errorf("chunk %d got %d requests, want %d", i, n, want)
		}
	}
	if r := vol.requestsTo("/" + m.Chunks[1].Fid)[1]; r.header.Get("Range") != "bytes=1-2" {
		t.Errorf("chunk 1 was read with Range %q", r.header.Get("Range"))
	}
}

func TestStatChunked(t *testing.T) {
	c, vol, ar := helloInChunks(t, chunkOptions())
	info, err := c.Stat(context.Background(), ar.Fid)
	if err != nil {
		t.Fatal(err)
	}
	f := vol.store.file(ar.Fid)
	want := FileInfo{Name: "hello.txt", Size: 11, ContentType: "text/plain", ETag: f.etag(), LastModified: f.modified.UTC()}
	if *info != want {
		t.Errorf("Stat = %+v, want %+v", *info, want)
	}
}

// ServeFile serves a file in chunks with ranges, and HEAD requests.
func TestServeFileChunked(t *testing.T) {
	c, _, ar := helloInChunks(t, chunkOptions())
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.Header.Set("Range", "bytes=3-8")
	r.Header.Set("Accept-Encoding", "gzip")
	w := httptest.NewRecorder()
	if err := c.ServeFile(w, r, ar.Fid); err != nil {
		t.Fatal(err)
	}
	if w.Code != http.StatusPartialContent || w.Body.String() != "lo wor" || w.Header().Get("Content-Range") != "bytes 3-8/11" ||
		w.Header().Get("Content-Length") != "6" || w.Header().Get("Content-Type") != "text/plain" {
		t.Errorf("answer %d %q, headers %v", w.Code, w.Body.String(), w.Header())
	}
	w = httptest.NewRecorder()
	if err := c.ServeFile(w, httptest.NewRequest(http.MethodHead, "/", nil), ar.Fid); err != nil {
		t.Fatal(err)
	}
	if w.Code != http.StatusOK || w.Body.Len() != 0 || w.Header().Get("Content-Length") != "11" {
		t.Errorf("HEAD: answer %d %q, headers %v", w.Code, w.Body.String(), w.Header())
	}
}

// When a volume server with a copy of the chunks is down, they are read from another copy. The
// copies are tried in random order, so the test reads 20 times.
func TestGetChunkedCopyDown(t *testing.T) {
	store := newMemStore()
	vol := newMemVolume(t, store)
	c, _ := memCluster(t, chunkOptions(), vol.addr(), closedPort)
	ar, _ := c.Assign(context.Background(), AssignOptions{})
	if _, err := c.Upload(context.Background(), ar, strings.NewReader("hello world"), 11, "", ""); err != nil {
		t.Fatal(err)
	}
	for range 20 {
		res, err := c.Get(context.Background(), ar.Fid, nil)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(res.Body)
		res.Body.Close()
		if err != nil || string(body) != "hello world" {
			t.Fatalf("body %q, %v", body, err)
		}
	}
}

// With Options.DataCenter, the chunks are read from the copies in that data center.
func TestGetChunkedPrefersDataCenter(t *testing.T) {
	store := newMemStore()
	local, remote := newMemVolume(t, store), newMemVolume(t, store)
	opts := chunkOptions()
	opts.DataCenter = "dc2"
	c, _ := memClusterLayout(t, opts, remote.addr(), layout{
		"dc1": {"rack1": {remote.addr(): {3}}},
		"dc2": {"rack1": {local.addr(): {3}}},
	})
	ar, _ := c.Assign(context.Background(), AssignOptions{})
	if _, err := c.Upload(context.Background(), ar, strings.NewReader("hello world"), 11, "", ""); err != nil {
		t.Fatal(err)
	}
	before := len(remote.requests)
	res, err := c.Get(context.Background(), ar.Fid, nil)
	if err != nil {
		t.Fatal(err)
	}
	io.ReadAll(res.Body)
	res.Body.Close()
	if n := len(remote.requests) - before; n != 0 {
		t.Errorf("the remote data center got %d requests", n)
	}
	if n := len(local.requests); n != 4 {
		t.Errorf("the local data center got %d requests, want 4 (the manifest and 3 chunks)", n)
	}
}

// Delete deletes the manifest, and the volume server its chunks.
func TestDeleteChunked(t *testing.T) {
	c, vol, ar := helloInChunks(t, chunkOptions())
	if err := c.Delete(context.Background(), ar.Fid); err != nil {
		t.Fatal(err)
	}
	if n := len(vol.store.files); n != 0 {
		t.Errorf("the store has %d files left", n)
	}
	if _, err := c.Get(context.Background(), ar.Fid, nil); !errors.Is(err, ErrNotFound) {
		t.Errorf("Get after Delete returned %v", err)
	}
}

// SeaweedFS keeps the content type of a manifest only on the copy that got the upload. The manifest
// is still found on a copy that has another one, by its beginning, and with a Range by its X-File-Store.
func TestGetChunkedOnReplica(t *testing.T) {
	c, vol, ar := helloInChunks(t, chunkOptions())
	vol.store.file(ar.Fid).contentType = "text/plain; charset=utf-8"
	for _, header := range []http.Header{nil, {"Range": {"bytes=3-8"}}} {
		res, err := c.Get(context.Background(), ar.Fid, header)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(res.Body)
		res.Body.Close()
		if want := map[bool]string{false: "hello world", true: "lo wor"}[header != nil]; err != nil || string(body) != want {
			t.Errorf("Range %q: %q, %v", header.Get("Range"), body, err)
		}
	}
}

// A manifest that another tool stored, without manifestPrefix, is joined by the volume server for a
// range and for Stat, and returned as it is stored for a GET request without a range.
func TestForeignManifest(t *testing.T) {
	c, vol, ar := helloInChunks(t, chunkOptions())
	f := vol.store.file(ar.Fid)
	f.data = append([]byte("{"), bytes.TrimPrefix(f.data, manifestPrefix)...)
	res, err := c.Get(context.Background(), ar.Fid, http.Header{"Range": {"bytes=3-8"}})
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode != http.StatusPartialContent || string(body) != "lo wor" {
		t.Errorf("range: %s %q", res.Status, body)
	}
	if info, err := c.Stat(context.Background(), ar.Fid); err != nil || info.Size != 11 || info.Name != "hello.txt" {
		t.Errorf("Stat = %+v, %v", info, err)
	}
	res, err = c.Get(context.Background(), ar.Fid, nil)
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(res.Body)
	res.Body.Close()
	if !bytes.HasPrefix(body, []byte(`{"name":"hello.txt"`)) {
		t.Errorf("without a range: %q", body)
	}
}

// With a Range, the library answers If-None-Match and If-Modified-Since itself.
func TestGetChunkedRangeNotModified(t *testing.T) {
	c, vol, ar := helloInChunks(t, chunkOptions())
	f := vol.store.file(ar.Fid)
	for _, header := range []http.Header{
		{"Range": {"bytes=3-8"}, "If-None-Match": {`"other", "` + f.etag() + `"`}},
		{"Range": {"bytes=3-8"}, "If-Modified-Since": {f.modified.UTC().Format(http.TimeFormat)}},
	} {
		res, err := c.Get(context.Background(), ar.Fid, header)
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		if res.StatusCode != http.StatusNotModified {
			t.Errorf("%v: %s, want 304", header, res.Status)
		}
	}
	res, err := c.Get(context.Background(), ar.Fid, http.Header{"Range": {"bytes=3-8"}, "If-None-Match": {`"other"`}})
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusPartialContent {
		t.Errorf("another ETag: %s, want 206", res.Status)
	}
}

// When a chunk is missing, reading the file fails after the chunks before it.
func TestGetChunkedChunkMissing(t *testing.T) {
	c, vol, ar := helloInChunks(t, chunkOptions())
	m := manifestOf(t, vol.store, ar.Fid)
	delete(vol.store.files, m.Chunks[1].Fid)
	res, err := c.Get(context.Background(), ar.Fid, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if string(body) != "hell" || !errors.Is(err, ErrNotFound) || !strings.Contains(err.Error(), m.Chunks[1].Fid) {
		t.Errorf("read %q, %v", body, err)
	}
}

// A chunk with fewer bytes than the manifest says is an error, not a shorter file.
func TestGetChunkedChunkTooShort(t *testing.T) {
	c, vol, ar := helloInChunks(t, chunkOptions())
	m := manifestOf(t, vol.store, ar.Fid)
	vol.store.file(m.Chunks[1].Fid).data = []byte("o w")
	res, err := c.Get(context.Background(), ar.Fid, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if body, err := io.ReadAll(res.Body); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("read %q, %v", body, err)
	}
}

// A manifest with manifestPrefix whose chunks don't make up the file is an error.
func TestInvalidManifest(t *testing.T) {
	c, vol, ar := helloInChunks(t, chunkOptions())
	f := vol.store.file(ar.Fid)
	for _, data := range []string{
		`{"seaweedstore":1,"size":11,"chunks":[{"fid":"3,01","offset":0,"size":4},{"fid":"3,02","offset":5,"size":6}]}`,
		`{"seaweedstore":1,"size":12,"chunks":[{"fid":"3,01","offset":0,"size":11}]}`,
		`{"seaweedstore":1,"size":11,"chunks":[{"fid":"3,../x","offset":0,"size":11}]}`,
		`{"seaweedstore":1,not json`,
	} {
		f.data = []byte(data)
		if res, err := c.Get(context.Background(), ar.Fid, nil); err == nil {
			res.Body.Close()
			t.Errorf("Get with the manifest %s returned no error", data)
		}
	}
}

func TestParseRange(t *testing.T) {
	for _, tt := range []struct {
		header          string
		start, end      int64
		ok, satisfiable bool
	}{
		{"bytes=0-4", 0, 5, true, true},
		{"bytes=3-", 3, 11, true, true},
		{"bytes=-3", 8, 11, true, true},
		{"bytes=-30", 0, 11, true, true},
		{"bytes=0-100", 0, 11, true, true},
		{"bytes=11-", 0, 0, true, false},
		{"bytes=-0", 0, 0, true, false},
		{"bytes=4-3", 0, 0, false, false},
		{"bytes=0-1,3-4", 0, 0, false, false},
		{"bytes=+1-2", 0, 0, false, false},
		{"bytes=-", 0, 0, false, false},
		{"items=0-1", 0, 0, false, false},
	} {
		start, end, ok, satisfiable := parseRange(tt.header, 11)
		if got := []any{start, end, ok, satisfiable}; !slices.Equal(got, []any{tt.start, tt.end, tt.ok, tt.satisfiable}) {
			t.Errorf("parseRange(%q, 11) = %v", tt.header, got)
		}
	}
}
