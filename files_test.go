package seaweedstore

import (
	"context"
	"errors"
	"io"
	"mime"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

func TestAssign(t *testing.T) {
	leader := newMaster(t, true)
	c := readyCluster(t, quietOptions(), leader)
	leader.answer("/dir/assign", http.StatusOK,
		`{"fid":"5,01694433fe","url":"172.29.0.21:8080","publicUrl":"vol1.example.com:8080","count":2}`)
	ar, err := c.Assign(context.Background(), AssignOptions{Collection: "photos", Count: 2})
	if err != nil {
		t.Fatal(err)
	}
	want := AssignResult{Fid: "5,01694433fe", Addr: "172.29.0.21:8080", PublicAddr: "vol1.example.com:8080", Count: 2}
	if *ar != want {
		t.Errorf("Assign = %+v, want %+v", *ar, want)
	}
	if r := onlyRequest(t, leader, "/dir/assign"); r.query != "collection=photos&count=2" {
		t.Errorf("the query is %q", r.query)
	}
}

// When the master refuses to assign a fid with HTTP 406, the error has its reason, and Assign doesn't
// ask again.
func TestAssignError(t *testing.T) {
	leader := newMaster(t, true)
	c := readyCluster(t, quietOptions(), leader)
	leader.answer("/dir/assign", http.StatusNotAcceptable, assignNotAcceptable)
	ar, err := c.Assign(context.Background(), AssignOptions{Replication: "200"})
	var he *HTTPError
	if !errors.As(err, &he) || he.StatusCode != http.StatusNotAcceptable || !strings.Contains(he.Message, "no free volumes left") {
		t.Errorf("Assign returned %+v, %v", ar, err)
	}
	if n := len(leader.requestsTo("/dir/assign")); n != 1 {
		t.Errorf("Assign asked %d times, want once", n)
	}
}

// Assign asks again after an HTTP 503, after the Retry-After of the answer.
func TestAssignRetries503(t *testing.T) {
	leader := newMaster(t, true)
	c := readyCluster(t, quietOptions(), leader)
	leader.answerFirst("/dir/assign", answer{status: http.StatusServiceUnavailable, body: assignGrowing, header: map[string]string{"Retry-After": "1"}})
	leader.answer("/dir/assign", http.StatusOK, `{"fid":"5,01694433fe","url":"172.29.0.21:8080","count":1}`)
	start := time.Now()
	ar, err := c.Assign(context.Background(), AssignOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if ar.Fid != "5,01694433fe" {
		t.Errorf("Assign = %+v", ar)
	}
	if elapsed := time.Since(start); elapsed < time.Second {
		t.Errorf("Assign asked again after %v, want 1s", elapsed)
	}
	if n := len(leader.requestsTo("/dir/assign")); n != 2 {
		t.Errorf("Assign asked %d times, want twice", n)
	}
}

// When ctx is done while the master answers 503, the error has both.
func TestAssign503UntilContextDone(t *testing.T) {
	leader := newMaster(t, true)
	c := readyCluster(t, quietOptions(), leader)
	leader.answerFirst("/dir/assign", answer{status: http.StatusServiceUnavailable, body: assignGrowing, header: map[string]string{"Retry-After": "1"}})
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_, err := c.Assign(ctx, AssignOptions{})
	var he *HTTPError
	if !errors.Is(err, context.DeadlineExceeded) || !errors.As(err, &he) ||
		he.StatusCode != http.StatusServiceUnavailable || he.RetryAfter != time.Second {
		t.Errorf("Assign returned %v", err)
	}
}

// When Assign can't reach the leader, it asks the other masters, which forward the request. After
// that it asks the leader last, until a request reaches it again.
func TestAssignFallsBackToOtherMaster(t *testing.T) {
	hanging, accepted := hangingServer(t)
	follower := newMaster(t, false)
	follower.answer("/dir/assign", http.StatusOK, `{"fid":"5,01694433fe","url":"172.29.0.21:8080","count":1}`)
	opts := quietOptions()
	opts.Timeout = 200 * time.Millisecond
	c := newTestCluster(t, opts, hanging, follower.addr())
	c.leaderIdx = 0 // the leader has stopped answering since the last status update
	for range 2 {
		if _, err := c.Assign(context.Background(), AssignOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	if n := accepted.Load(); n != 1 {
		t.Errorf("the leader got %d connections, want 1", n)
	}
	if n := len(follower.requestsTo("/dir/assign")); n != 2 {
		t.Errorf("the follower got %d assign requests, want 2", n)
	}
}

// When no master can be reached, the error says why for each.
func TestAssignNoMasterReachable(t *testing.T) {
	c := newTestCluster(t, quietOptions(), closedPort, closedPort2)
	_, err := c.Assign(context.Background(), AssignOptions{})
	if err == nil {
		t.Fatal("Assign returned no error")
	}
	for _, addr := range []string{closedPort, closedPort2} {
		if !strings.Contains(err.Error(), addr) {
			t.Errorf("the error %q doesn't mention %s", err, addr)
		}
	}
}

func TestUpload(t *testing.T) {
	vol, ar := uploadServer(t)
	c := newTestCluster(t, quietOptions())
	ur, err := c.Upload(context.Background(), ar, strings.NewReader("hello world"), 11, "hello.txt", "text/plain")
	if err != nil {
		t.Fatal(err)
	}
	if want := (UploadResult{Name: "hello.txt", Size: 11, ETag: "c99465aa", ContentType: "text/plain"}); *ur != want {
		t.Errorf("Upload = %+v, want %+v", *ur, want)
	}
	r := onlyRequest(t, vol, "/"+fid)
	if r.method != http.MethodPut || r.body != "hello world" || r.header.Get("Content-Type") != "text/plain" ||
		r.header.Get("Content-Disposition") != `attachment; filename="hello.txt"` {
		t.Errorf("the volume server got %+v", r)
	}
}

// Without a name and a content type, the upload has no Content-Disposition and no Content-Type, and
// SeaweedFS uses the fid as the name.
func TestUploadWithoutNameAndType(t *testing.T) {
	vol, ar := uploadServer(t)
	c := newTestCluster(t, quietOptions())
	if _, err := c.Upload(context.Background(), ar, strings.NewReader("hello world"), 11, "", ""); err != nil {
		t.Fatal(err)
	}
	r := onlyRequest(t, vol, "/"+fid)
	for _, h := range []string{"Content-Disposition", "Content-Type"} {
		if v := r.header.Values(h); len(v) != 0 {
			t.Errorf("the upload has %s %q", h, v)
		}
	}
}

// The upload has the file's size in Content-Length, also when the body doesn't tell its size.
func TestUploadSendsContentLength(t *testing.T) {
	vol, ar := uploadServer(t)
	c := newTestCluster(t, quietOptions())
	body := io.MultiReader(strings.NewReader("hello world")) // net/http can't see its size
	if _, err := c.Upload(context.Background(), ar, body, 11, "hello.txt", "text/plain"); err != nil {
		t.Fatal(err)
	}
	if r := onlyRequest(t, vol, "/"+fid); r.contentLength != 11 || r.chunked {
		t.Errorf("the upload has Content-Length %d (chunked: %v), want 11", r.contentLength, r.chunked)
	}
}

// File names reach SeaweedFS unchanged. SeaweedFS 4.50 reads backslash escapes and UTF-8 in the
// quoted filename parameter, but it ignores the filename* parameter of RFC 2231.
func TestUploadFileNames(t *testing.T) {
	for _, name := range []string{"hello.txt", "my file.txt", "a;b.txt", `say "hi".txt`, "árvíztűrő tükörfúrógép.txt"} {
		t.Run(name, func(t *testing.T) {
			vol, ar := uploadServer(t)
			c := newTestCluster(t, quietOptions())
			if _, err := c.Upload(context.Background(), ar, strings.NewReader("hello world"), 11, name, "text/plain"); err != nil {
				t.Fatal(err)
			}
			cd := onlyRequest(t, vol, "/"+fid).header.Get("Content-Disposition")
			if strings.Contains(cd, "filename*") {
				t.Errorf("Content-Disposition %s has filename*, which SeaweedFS ignores", cd)
			}
			if _, params, err := mime.ParseMediaType(cd); err != nil || params["filename"] != name {
				t.Errorf("Content-Disposition %s has the name %q (%v), want %q", cd, params["filename"], err, name)
			}
		})
	}
}

// When the volume server reports another size than the upload had, Upload fails. SeaweedFS 4.50
// stores an empty file for this content type, and leaves the size out of its answer.
func TestUploadSizeMismatch(t *testing.T) {
	vol, ar := uploadServer(t)
	vol.answer("/"+fid, http.StatusCreated, `{"name":"3,01637037d6","eTag":"00000000","mime":"application/x-www-form-urlencoded"}`)
	c := newTestCluster(t, quietOptions())
	_, err := c.Upload(context.Background(), ar, strings.NewReader("hello world"), 11, "", "application/x-www-form-urlencoded")
	if err == nil || !strings.Contains(err.Error(), "stored 0 bytes of 11") {
		t.Errorf("Upload returned %v, want an error about the size", err)
	}
}

// A body that gives fewer or more bytes than the size makes the upload fail.
func TestUploadWrongLength(t *testing.T) {
	for _, size := range []int64{5, 20} {
		_, ar := uploadServer(t)
		c := newTestCluster(t, quietOptions())
		body := io.MultiReader(strings.NewReader("hello world"))
		if _, err := c.Upload(context.Background(), ar, body, size, "", ""); err == nil {
			t.Errorf("Upload of 11 bytes with size %d returned no error", size)
		}
	}
}

// A negative size sends the body in chunks.
func TestUploadUnknownLength(t *testing.T) {
	vol, ar := uploadServer(t)
	c := newTestCluster(t, quietOptions())
	body := io.MultiReader(strings.NewReader("hello world"))
	if _, err := c.Upload(context.Background(), ar, body, -1, "hello.txt", "text/plain"); err != nil {
		t.Fatal(err)
	}
	if r := onlyRequest(t, vol, "/"+fid); !r.chunked || r.body != "hello world" {
		t.Errorf("the upload is %+v, want hello world in chunks", r)
	}
}

// Only the context limits an upload: here the volume server answers after 300 ms, which is longer than
// every timeout in Options.
func TestUploadLimitedByContext(t *testing.T) {
	vol := slowServer(t, func(w http.ResponseWriter, r *http.Request, stop <-chan struct{}) {
		io.ReadAll(r.Body)
		select {
		case <-time.After(300 * time.Millisecond):
		case <-stop:
		}
		w.WriteHeader(http.StatusCreated)
		io.WriteString(w, uploadCreated)
	})
	opts := quietOptions()
	opts.Timeout = 100 * time.Millisecond
	opts.VolumeTimeout = 100 * time.Millisecond
	c := newTestCluster(t, opts)
	ar := &AssignResult{Fid: fid, Addr: vol}
	if _, err := c.Upload(context.Background(), ar, strings.NewReader("hello world"), 11, "", ""); err != nil {
		t.Error(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := c.Upload(ctx, ar, strings.NewReader("hello world"), 11, "", ""); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Upload with a context of 100ms returned %v", err)
	}
}

// Upload checks the fid like Get, because it goes into the URL too.
func TestUploadInvalidFid(t *testing.T) {
	vol, ar := uploadServer(t)
	ar.Fid = fid + "/../status"
	c := newTestCluster(t, quietOptions())
	if _, err := c.Upload(context.Background(), ar, strings.NewReader("x"), 1, "", ""); err == nil {
		t.Error("Upload with an invalid fid returned no error")
	}
	if len(vol.requestsTo("/status")) != 0 {
		t.Error("the volume server got the request")
	}
}

func TestUploadStatusError(t *testing.T) {
	vol, ar := uploadServer(t)
	vol.answer("/"+fid, http.StatusInternalServerError, `{"error":"internal error"}`)
	c := newTestCluster(t, quietOptions())
	_, err := c.Upload(context.Background(), ar, strings.NewReader("hello world"), 11, "", "")
	var he *HTTPError
	if !errors.As(err, &he) || he.StatusCode != http.StatusInternalServerError || he.Message != "internal error" {
		t.Errorf("Upload returned %v for HTTP 500", err)
	}
}

// Get sends the header to a volume server that has the file.
func TestGet(t *testing.T) {
	vol := newFakeServer(t)
	vol.answer("/"+fid, http.StatusOK, "hello world")
	c := volumeCluster(t, quietOptions(), vol.addr())
	res, err := c.Get(context.Background(), fid, http.Header{"Range": {"bytes=0-4"}})
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if body, err := io.ReadAll(res.Body); err != nil || string(body) != "hello world" {
		t.Errorf("body %q, %v", body, err)
	}
	if r := onlyRequest(t, vol, "/"+fid); r.method != http.MethodGet || r.header.Get("Range") != "bytes=0-4" {
		t.Errorf("the volume server got %s with Range %q", r.method, r.header.Get("Range"))
	}
}

// Get returns the response for 304, with no error.
func TestGetNotModified(t *testing.T) {
	vol := newFakeServer(t)
	vol.answer("/"+fid, http.StatusNotModified, "")
	c := volumeCluster(t, quietOptions(), vol.addr())
	res, err := c.Get(context.Background(), fid, http.Header{"If-None-Match": {`"c99465aa"`}})
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusNotModified {
		t.Errorf("status %s, want 304", res.Status)
	}
}

func TestGetInvalidFid(t *testing.T) {
	vol := newFakeServer(t)
	c := volumeCluster(t, quietOptions(), vol.addr())
	if _, err := c.Get(context.Background(), "01637037d6", nil); err == nil {
		t.Error("Get of a fid without a volume id returned no error")
	}
}

// When a volume server with a copy of the file is down, answers 404 or an HTTP 5xx status, Get reads
// another copy. It tries the copies in random order, so the test reads 20 times.
func TestGetUsesOtherCopy(t *testing.T) {
	for _, tt := range []struct {
		name string
		bad  func(t *testing.T) string
	}{
		{"down", func(t *testing.T) string { return closedPort }},
		{"404", func(t *testing.T) string {
			vol := newFakeServer(t)
			vol.answer("/"+fid, http.StatusNotFound, "")
			return vol.addr()
		}},
		{"500", func(t *testing.T) string {
			vol := newFakeServer(t)
			vol.answer("/"+fid, http.StatusInternalServerError, `{"error":"internal error"}`)
			return vol.addr()
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			vol := newFakeServer(t)
			vol.answer("/"+fid, http.StatusOK, "hello world")
			c := volumeCluster(t, quietOptions(), tt.bad(t), vol.addr())
			for range 20 {
				res, err := c.Get(context.Background(), fid, nil)
				if err != nil {
					t.Fatal(err)
				}
				body, _ := io.ReadAll(res.Body)
				res.Body.Close()
				if string(body) != "hello world" {
					t.Fatalf("body %q", body)
				}
			}
		})
	}
}

// With Options.DataCenter, Get reads the copy in that data center, and the others only when it can't
// read that one.
func TestGetPrefersDataCenter(t *testing.T) {
	local, remote := newFakeServer(t), newFakeServer(t)
	for _, vol := range []*fakeServer{local, remote} {
		vol.answer("/"+fid, http.StatusOK, "hello world")
	}
	leader := newMaster(t, true)
	opts := quietOptions()
	opts.DataCenter = "dc2"
	for _, tt := range []struct {
		name      string
		localAddr string
		wantLocal int
	}{
		{"local copy up", local.addr(), 20},
		{"local copy down", closedPort, 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			leader.answer("/vol/status", http.StatusOK, volStatus(layout{
				"dc1": {"rack1": {remote.addr(): {3}}},
				"dc2": {"rack1": {tt.localAddr: {3}}},
			}))
			c := readyCluster(t, opts, leader)
			before := len(local.requestsTo("/" + fid))
			for range 20 {
				res, err := c.Get(context.Background(), fid, nil)
				if err != nil {
					t.Fatal(err)
				}
				res.Body.Close()
			}
			if n := len(local.requestsTo("/"+fid)) - before; n != tt.wantLocal {
				t.Errorf("%d of 20 reads went to the local copy, want %d", n, tt.wantLocal)
			}
		})
	}
}

// Get returns ErrNotFound when every copy answers 404, or the volume doesn't exist.
func TestGetNotFound(t *testing.T) {
	vol1, vol2 := newFakeServer(t), newFakeServer(t)
	c := volumeCluster(t, quietOptions(), vol1.addr(), vol2.addr())
	if _, err := c.Get(context.Background(), fid, nil); !errors.Is(err, ErrNotFound) {
		t.Errorf("Get of a missing file returned %v", err)
	}
	if _, err := c.Get(context.Background(), "9,01637037d6", nil); !errors.Is(err, ErrNotFound) {
		t.Errorf("Get of a file on a missing volume returned %v", err)
	}
}

// Get returns an *HTTPError for other error statuses, and doesn't try the other copies for them.
func TestGetErrorStatus(t *testing.T) {
	vol1, vol2 := newFakeServer(t), newFakeServer(t)
	for _, vol := range []*fakeServer{vol1, vol2} {
		vol.answer("/"+fid, http.StatusRequestedRangeNotSatisfiable, "")
	}
	c := volumeCluster(t, quietOptions(), vol1.addr(), vol2.addr())
	_, err := c.Get(context.Background(), fid, http.Header{"Range": {"bytes=100-"}})
	var he *HTTPError
	if !errors.As(err, &he) || he.StatusCode != http.StatusRequestedRangeNotSatisfiable {
		t.Errorf("Get returned %v, want HTTP 416", err)
	}
	if n := len(vol1.requestsTo("/"+fid)) + len(vol2.requestsTo("/"+fid)); n != 1 {
		t.Errorf("the volume servers got %d requests, want 1", n)
	}
}

// After Get couldn't reach a volume server, it tries that server last, so that later reads don't
// wait for it.
func TestGetTriesFailedServerLast(t *testing.T) {
	hanging, accepted := hangingServer(t)
	vol := newFakeServer(t)
	vol.answer("/"+fid, http.StatusOK, "hello world")
	opts := quietOptions()
	opts.VolumeTimeout = 200 * time.Millisecond
	c := volumeCluster(t, opts, hanging, vol.addr())
	read := func() {
		res, err := c.Get(context.Background(), fid, nil)
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
	}
	// The order is random at first: read until a read has tried the hanging server
	for i := 0; accepted.Load() == 0; i++ {
		if i == 20 {
			t.Fatal("20 reads didn't try the hanging server")
		}
		read()
	}
	for range 10 {
		read()
	}
	if n := accepted.Load(); n != 1 {
		t.Errorf("the hanging server got %d connections, want 1", n)
	}
}

// Get stops when its context is done, instead of trying the other copies.
func TestGetContextDone(t *testing.T) {
	hanging, _ := hangingServer(t)
	other, _ := hangingServer(t)
	opts := quietOptions()
	opts.VolumeTimeout = time.Minute
	c := volumeCluster(t, opts, hanging, other)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := c.Get(ctx, fid, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Get returned %v, want %v", err, context.DeadlineExceeded)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("Get took %v with a context of 200ms", elapsed)
	}
}

// VolumeTimeout limits the wait for the response headers, not reading the body: here the body takes
// 300 ms, with a timeout of 100 ms.
func TestGetTimeoutOnlyForHeaders(t *testing.T) {
	vol := slowServer(t, func(w http.ResponseWriter, r *http.Request, _ <-chan struct{}) {
		io.WriteString(w, "hello")
		w.(http.Flusher).Flush()
		time.Sleep(300 * time.Millisecond)
		io.WriteString(w, " world")
	})
	opts := quietOptions()
	opts.VolumeTimeout = 100 * time.Millisecond
	c := volumeCluster(t, opts, vol)
	res, err := c.Get(context.Background(), fid, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if body, err := io.ReadAll(res.Body); err != nil || string(body) != "hello world" {
		t.Errorf("body %q, %v", body, err)
	}
}

// When the response headers don't arrive within VolumeTimeout, the error is a timeout.
func TestGetHeaderTimeout(t *testing.T) {
	vol := slowServer(t, func(w http.ResponseWriter, r *http.Request, stop <-chan struct{}) {
		<-stop
	})
	opts := quietOptions()
	opts.VolumeTimeout = 100 * time.Millisecond
	c := volumeCluster(t, opts, vol)
	start := time.Now()
	_, err := c.Get(context.Background(), fid, nil)
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Errorf("Get returned %v, want a timeout", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("Get took %v with a timeout of 100ms", elapsed)
	}
}

// Stat asks a volume server with a HEAD request, without gzip, so that it gets the real size.
func TestStat(t *testing.T) {
	vol := newFakeServer(t)
	vol.answerFirst("/"+fid, answer{status: http.StatusOK, body: "hello world", header: map[string]string{
		"Content-Disposition": "inline; filename=hello.txt",
		"Content-Length":      "11",
		"Content-Type":        "text/plain",
		"Etag":                `"c99465aa"`,
		"Last-Modified":       "Sun, 01 Nov 2026 13:22:08 GMT",
	}})
	c := volumeCluster(t, quietOptions(), vol.addr())
	info, err := c.Stat(context.Background(), fid)
	if err != nil {
		t.Fatal(err)
	}
	want := FileInfo{Name: "hello.txt", Size: 11, ContentType: "text/plain", ETag: "c99465aa",
		LastModified: time.Date(2026, 11, 1, 13, 22, 8, 0, time.UTC)}
	if *info != want {
		t.Errorf("Stat = %+v, want %+v", *info, want)
	}
	if r := onlyRequest(t, vol, "/"+fid); r.method != http.MethodHead || r.header.Get("Accept-Encoding") != "identity" {
		t.Errorf("the volume server got %s with Accept-Encoding %q", r.method, r.header.Get("Accept-Encoding"))
	}
}

func TestStatNotFound(t *testing.T) {
	c := volumeCluster(t, quietOptions(), newFakeServer(t).addr())
	if info, err := c.Stat(context.Background(), fid); !errors.Is(err, ErrNotFound) {
		t.Errorf("Stat of a missing file returned %+v, %v", info, err)
	}
}

func TestDelete(t *testing.T) {
	vol := newFakeServer(t)
	vol.answer("/"+fid, http.StatusAccepted, `{"size":40}`)
	c := volumeCluster(t, quietOptions(), vol.addr())
	if err := c.Delete(context.Background(), fid); err != nil {
		t.Fatal(err)
	}
	if r := onlyRequest(t, vol, "/"+fid); r.method != http.MethodDelete {
		t.Errorf("the volume server got %s", r.method)
	}
	vol.answer("/"+fid, http.StatusNotFound, `{"size":0}`)
	if err := c.Delete(context.Background(), fid); !errors.Is(err, ErrNotFound) {
		t.Errorf("Delete of a missing file returned %v", err)
	}
}
