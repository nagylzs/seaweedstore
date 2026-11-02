//go:build integration

// Integration tests against the docker cluster in this directory. They start the cluster if it isn't
// running, and leave it running and healthy, possibly with a different leader:
//
//	go test -tags integration -v ./testenv
package testenv

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"mime"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nagylzs/seaweedstore"
)

type server struct {
	name string
	addr string
	dc   string // for volume servers
}

var masters = []server{
	{"master1", "172.29.0.11:9333", ""},
	{"master2", "172.29.0.12:9333", ""},
	{"master3", "172.29.0.13:9333", ""},
}

var volumeServers = []server{
	{"dc1-vol1", "172.29.0.21:8080", "dc1"},
	{"dc1-vol2", "172.29.0.22:8080", "dc1"},
	{"dc2-vol1", "172.29.0.31:8080", "dc2"},
	{"dc2-vol2", "172.29.0.32:8080", "dc2"},
}

// A file is uploaded with its name and content type, and reads give them back, also for a range.
func TestUploadAndRead(t *testing.T) {
	cluster(t, "up")
	c := newCluster(t, time.Second)
	fid := upload(t, c, "hello.txt", "hello world")

	res := get(t, c, fid, nil)
	if body := readBody(t, res); res.StatusCode != http.StatusOK || body != "hello world" {
		t.Errorf("Get: %s %q", res.Status, body)
	}
	if ct := res.Header.Get("Content-Type"); ct != "text/plain" {
		t.Errorf("Content-Type %q, want text/plain", ct)
	}
	if name := fileName(t, res); name != "hello.txt" {
		t.Errorf("the name is %q, want hello.txt", name)
	}

	res = get(t, c, fid, http.Header{"Range": {"bytes=0-4"}})
	if body := readBody(t, res); res.StatusCode != http.StatusPartialContent || body != "hello" {
		t.Errorf("Get with Range: %s %q", res.Status, body)
	}
}

// SeaweedFS stores text compressed. Get gives back the original bytes, and Stat the original size.
func TestCompressedFile(t *testing.T) {
	cluster(t, "up")
	c := newCluster(t, time.Second)
	data := strings.Repeat("hello seaweed ", 10000)
	fid := upload(t, c, "text.txt", data)
	if body := readBody(t, get(t, c, fid, nil)); body != data {
		t.Errorf("Get gave %d bytes, want the %d uploaded", len(body), len(data))
	}
	info, err := c.Stat(context.Background(), fid)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size != int64(len(data)) {
		t.Errorf("Stat says %d bytes, want %d", info.Size, len(data))
	}
}

// Stat describes an uploaded file. Delete deletes all copies, after which the file is not found.
func TestStatAndDelete(t *testing.T) {
	cluster(t, "up")
	c := newCluster(t, time.Second)
	ctx := context.Background()
	ar := assign(t, c, 10*time.Second)
	before := time.Now().Add(-time.Second).Truncate(time.Second)
	ur, err := c.Upload(ctx, ar, strings.NewReader("hello world"), 11, "hello.txt", "text/plain")
	if err != nil {
		t.Fatal(err)
	}
	info, err := c.Stat(ctx, ar.Fid)
	if err != nil {
		t.Fatal(err)
	}
	if info.Name != "hello.txt" || info.Size != 11 || info.ContentType != "text/plain" || info.ETag != ur.ETag ||
		info.LastModified.Before(before) || info.LastModified.After(time.Now()) {
		t.Errorf("Stat = %+v, uploaded %+v", *info, *ur)
	}

	if err := c.Delete(ctx, ar.Fid); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Get(ctx, ar.Fid, nil); !errors.Is(err, seaweedstore.ErrNotFound) {
		t.Errorf("Get after Delete returned %v", err)
	}
	if _, err := c.Stat(ctx, ar.Fid); !errors.Is(err, seaweedstore.ErrNotFound) {
		t.Errorf("Stat after Delete returned %v", err)
	}
	if err := c.Delete(ctx, ar.Fid); !errors.Is(err, seaweedstore.ErrNotFound) {
		t.Errorf("Delete after Delete returned %v", err)
	}
}

// ServeFile serves a file that SeaweedFS stores compressed: uncompressed, or compressed if the client
// accepts it, and a range always of the uncompressed file. Conditional and HEAD requests work, and a
// missing file is an error that the handler can answer.
func TestServeFile(t *testing.T) {
	cluster(t, "up")
	c := newCluster(t, time.Second)
	data := strings.Repeat("hello seaweed ", 10000)
	fid := upload(t, c, "text.txt", data)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		err := c.ServeFile(w, r, strings.TrimPrefix(r.URL.Path, "/"))
		if errors.Is(err, seaweedstore.ErrNotFound) {
			http.Error(w, "no such file", http.StatusNotFound)
		} else if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
		}
	}))
	defer srv.Close()
	// The client must not ask for gzip by itself, or decompress
	client := &http.Client{Transport: &http.Transport{DisableCompression: true}}
	request := func(method, path string, header http.Header) (*http.Response, string) {
		t.Helper()
		req, err := http.NewRequest(method, srv.URL+"/"+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header = header
		res, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		body, err := io.ReadAll(res.Body)
		if err != nil {
			t.Fatal(err)
		}
		return res, string(body)
	}

	res, body := request(http.MethodGet, fid, http.Header{})
	if res.StatusCode != http.StatusOK || body != data || res.Header.Get("Content-Encoding") != "" ||
		res.ContentLength != int64(len(data)) || res.Header.Get("Vary") != "Accept-Encoding" {
		t.Errorf("plain: %s, %d bytes, Content-Length %d, headers %v", res.Status, len(body), res.ContentLength, res.Header)
	}
	if name := fileName(t, res); name != "text.txt" {
		t.Errorf("the name is %q", name)
	}
	etag := res.Header.Get("Etag")

	res, body = request(http.MethodGet, fid, http.Header{"Accept-Encoding": {"gzip"}})
	if res.StatusCode != http.StatusOK || res.Header.Get("Content-Encoding") != "gzip" || res.ContentLength != int64(len(body)) {
		t.Errorf("gzip: %s, %d bytes, Content-Length %d, headers %v", res.Status, len(body), res.ContentLength, res.Header)
	} else if zr, err := gzip.NewReader(bytes.NewReader([]byte(body))); err != nil {
		t.Errorf("gzip: %v", err)
	} else if unzipped, err := io.ReadAll(zr); err != nil || string(unzipped) != data {
		t.Errorf("gzip: %d bytes after decompression, %v", len(unzipped), err)
	} else {
		t.Logf("%d bytes were sent compressed in %d", len(data), len(body))
	}

	res, body = request(http.MethodGet, fid, http.Header{"Accept-Encoding": {"gzip"}, "Range": {"bytes=100-199"}})
	if res.StatusCode != http.StatusPartialContent || body != data[100:200] || res.Header.Get("Content-Encoding") != "" ||
		res.Header.Get("Content-Range") != fmt.Sprintf("bytes 100-199/%d", len(data)) {
		t.Errorf("range: %s, body %q, headers %v", res.Status, body, res.Header)
	}

	res, _ = request(http.MethodGet, fid, http.Header{"If-None-Match": {etag}})
	if res.StatusCode != http.StatusNotModified {
		t.Errorf("If-None-Match %s: %s", etag, res.Status)
	}

	res, body = request(http.MethodHead, fid, http.Header{})
	if res.StatusCode != http.StatusOK || body != "" || res.ContentLength != int64(len(data)) {
		t.Errorf("HEAD: %s, Content-Length %d, body %q", res.Status, res.ContentLength, body)
	}

	if err := c.Delete(context.Background(), fid); err != nil {
		t.Fatal(err)
	}
	if res, _ = request(http.MethodGet, fid, http.Header{}); res.StatusCode != http.StatusNotFound {
		t.Errorf("after Delete: %s", res.Status)
	}
}

// randomData returns n random bytes, the same in every run.
func randomData(n int) []byte {
	data := make([]byte, n)
	rand.NewChaCha8([32]byte{1}).Read(data)
	return data
}

// chunksOf returns the chunk fids of a manifest, read from a volume server as it is stored.
func chunksOf(t *testing.T, c *seaweedstore.Cluster, fid string) []string {
	t.Helper()
	locations := lookup(t, c, fid)
	res, err := http.Get(locations[0].URL("/" + fid + "?cm=false"))
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var m struct{ Chunks []struct{ Fid string } }
	if err := json.NewDecoder(res.Body).Decode(&m); err != nil {
		t.Fatal(err)
	}
	var fids []string
	for _, ch := range m.Chunks {
		fids = append(fids, ch.Fid)
	}
	return fids
}

func lookup(t *testing.T, c *seaweedstore.Cluster, fid string) []seaweedstore.VolumeLocation {
	t.Helper()
	volumeID, err := seaweedstore.ParseFid(fid)
	if err != nil {
		t.Fatal(err)
	}
	locations, err := c.LookupVolume(context.Background(), volumeID)
	if err != nil {
		t.Fatal(err)
	}
	return locations
}

// A file larger than ChunkSize is uploaded in chunks, with a known and an unknown size. It reads back
// whole and in ranges across chunks, also by other clients, for which the volume server joins the
// chunks. Delete deletes the chunks too.
func TestLargeFile(t *testing.T) {
	cluster(t, "up")
	c := newClusterOpts(t, seaweedstore.Options{StatusUpdateInterval: time.Second, ChunkSize: 1 << 20})
	ctx := context.Background()
	data := randomData(3500000)
	for _, size := range []int64{int64(len(data)), -1} {
		ar := assign(t, c, 10*time.Second)
		ur, err := c.Upload(ctx, ar, io.MultiReader(bytes.NewReader(data)), size, "big.bin", "application/octet-stream")
		if err != nil {
			t.Fatalf("size %d: %v", size, err)
		}
		if ur.Size != int64(len(data)) || ur.Name != "big.bin" {
			t.Errorf("size %d: Upload = %+v", size, *ur)
		}
		chunks := chunksOf(t, c, ar.Fid)
		if len(chunks) != 4 || !slices.Equal(ur.Chunks, chunks) {
			t.Errorf("size %d: the manifest has the chunks %v, Upload says %v, want 4", size, chunks, ur.Chunks)
		}

		if body := readBody(t, get(t, c, ar.Fid, nil)); body != string(data) {
			t.Errorf("size %d: Get gave %d bytes, not the uploaded ones", size, len(body))
		}
		res := get(t, c, ar.Fid, http.Header{"Range": {"bytes=1048570-1048585"}})
		if body := readBody(t, res); res.StatusCode != http.StatusPartialContent || body != string(data[1048570:1048586]) ||
			res.Header.Get("Content-Range") != "bytes 1048570-1048585/3500000" {
			t.Errorf("size %d: range across chunks: %s, %d bytes, Content-Range %q", size, res.Status, len(body), res.Header.Get("Content-Range"))
		}
		info, err := c.Stat(ctx, ar.Fid)
		if err != nil || info.Size != int64(len(data)) || info.Name != "big.bin" || info.ETag != ur.ETag {
			t.Errorf("size %d: Stat = %+v, %v", size, info, err)
		}
		// Another client gets the file joined by the volume server
		other, err := http.Get(lookup(t, c, ar.Fid)[0].URL("/" + ar.Fid))
		if err != nil {
			t.Fatal(err)
		}
		if body := readBody(t, other); body != string(data) {
			t.Errorf("size %d: a plain GET gave %d bytes, not the uploaded ones", size, len(body))
		}
		other.Body.Close()

		if err := c.Delete(ctx, ar.Fid); err != nil {
			t.Fatal(err)
		}
		for _, chunk := range chunks {
			if _, err := c.Get(ctx, chunk, nil); !errors.Is(err, seaweedstore.ErrNotFound) {
				t.Errorf("size %d: Get of chunk %s after Delete returned %v", size, chunk, err)
			}
		}
	}
}

// A file with a TTL, also one in chunks, can be read until the TTL has passed, and then not: Get and
// Stat give ErrNotFound, also for each chunk. A file without a TTL stays.
func TestTTL(t *testing.T) {
	cluster(t, "up")
	c := newClusterOpts(t, seaweedstore.Options{StatusUpdateInterval: time.Second, ChunkSize: 1 << 20})
	ctx := context.Background()
	ttl := seaweedstore.AssignOptions{TTL: "1m"}
	small := put(t, c, assignWith(t, c, ttl, 30*time.Second), "small.txt", "hello")
	data := randomData(2500000)
	ar := assignWith(t, c, ttl, 30*time.Second)
	ur, err := c.Upload(ctx, ar, bytes.NewReader(data), int64(len(data)), "large.bin", "")
	if err != nil {
		t.Fatal(err)
	}
	uploaded := time.Now()
	kept := upload(t, c, "kept.txt", "kept")
	files := append([]string{small, ar.Fid}, ur.Chunks...)
	for _, fid := range files {
		if _, err := c.Stat(ctx, fid); err != nil {
			t.Fatalf("Stat(%s) right after the upload: %v", fid, err)
		}
	}
	if body := readBody(t, get(t, c, ar.Fid, nil)); body != string(data) {
		t.Fatalf("Get of the file in chunks gave %d bytes, not the uploaded ones", len(body))
	}
	waitFor(t, 2*time.Minute, "expiry", func() error {
		_, err := c.Stat(ctx, ar.Fid)
		if err == nil {
			return errors.New("the file in chunks is still there")
		}
		if !errors.Is(err, seaweedstore.ErrNotFound) {
			t.Fatalf("Stat of the file in chunks: %v", err)
		}
		return nil
	})
	if expired := time.Since(uploaded); expired < 50*time.Second {
		t.Errorf("the file in chunks expired %v after the upload, with a TTL of a minute", expired)
	} else {
		t.Logf("the file in chunks expired %v after the upload", expired.Round(time.Second))
	}
	// The chunks and the small file were uploaded before the manifest, so they have expired too
	for _, fid := range files {
		if _, err := c.Get(ctx, fid, nil); !errors.Is(err, seaweedstore.ErrNotFound) {
			t.Errorf("Get(%s) after the TTL returned %v", fid, err)
		}
		if _, err := c.Stat(ctx, fid); !errors.Is(err, seaweedstore.ErrNotFound) {
			t.Errorf("Stat(%s) after the TTL returned %v", fid, err)
		}
	}
	if body := readBody(t, get(t, c, kept, nil)); body != "kept" {
		t.Errorf("the file without a TTL gave %q", body)
	}
}

// When a volume server with copies of chunks is killed, the file is read from the other copies. The
// cluster here updates its status only every minute, so it still lists the killed server, as it
// would right after a crash. Only the first read waits for it.
func TestLargeFileWhileVolumeServerIsKilled(t *testing.T) {
	cluster(t, "up")
	data := randomData(3500000)
	writer := newClusterOpts(t, seaweedstore.Options{StatusUpdateInterval: time.Second, ChunkSize: 1 << 20})
	ar := assign(t, writer, 10*time.Second)
	if _, err := writer.Upload(context.Background(), ar, bytes.NewReader(data), int64(len(data)), "big.bin", ""); err != nil {
		t.Fatal(err)
	}
	c := newClusterOpts(t, seaweedstore.Options{StatusUpdateInterval: time.Minute})
	killed := volumeServer(t, lookup(t, c, chunksOf(t, c, ar.Fid)[0])[0].Addr)
	cluster(t, "kill", killed.name)
	t.Cleanup(func() { cluster(t, "up") })
	slow := 0
	for i := range 10 {
		start := time.Now()
		if body := readBody(t, get(t, c, ar.Fid, nil)); body != string(data) {
			t.Fatalf("read %d while %s is killed gave %d bytes, not the uploaded ones", i+1, killed.name, len(body))
		}
		if time.Since(start) > 2*time.Second {
			slow++
		}
	}
	if slow > 1 {
		t.Errorf("%d of 10 reads took more than 2 seconds", slow)
	}
}

// File names that need quoting come back unchanged. Of a name with a path, SeaweedFS keeps only the
// part after the last slash or backslash.
func TestFileNames(t *testing.T) {
	cluster(t, "up")
	c := newCluster(t, time.Second)
	for _, tt := range []struct{ name, want string }{
		{"my file.txt", "my file.txt"},
		{"a;b.txt", "a;b.txt"},
		{`say "hi".txt`, `say "hi".txt`},
		{"árvíztűrő tükörfúrógép.txt", "árvíztűrő tükörfúrógép.txt"},
		{"dir/file.txt", "file.txt"},
		{`C:\dir\file.txt`, "file.txt"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fid := put(t, c, assign(t, c, 10*time.Second), tt.name, "x")
			if got := fileName(t, get(t, c, fid, nil)); got != tt.want {
				t.Errorf("the name is %q, want %q", got, tt.want)
			}
		})
	}
}

// With replication 100, a file has a copy in each data center, and its locations say which data
// center they are in.
func TestCopiesInBothDataCenters(t *testing.T) {
	cluster(t, "up")
	c := newCluster(t, time.Second)
	fid := upload(t, c, "hello.txt", "hello world")
	volumeID, err := seaweedstore.ParseFid(fid)
	if err != nil {
		t.Fatal(err)
	}
	// Wait for a status update, so that the volume is in the cache, even if it was created now
	time.Sleep(2500 * time.Millisecond)
	locations, err := c.LookupVolume(context.Background(), volumeID)
	if err != nil {
		t.Fatal(err)
	}
	dcs := map[string]int{}
	for _, loc := range locations {
		s := volumeServer(t, loc.Addr)
		dcs[s.dc]++
		if loc.DataCenter != s.dc {
			t.Errorf("the location %s (%s) has DataCenter %q, want %q", loc.Addr, s.name, loc.DataCenter, s.dc)
		}
	}
	if len(locations) != 2 || dcs["dc1"] != 1 || dcs["dc2"] != 1 {
		t.Errorf("volume %d has copies in %v, want one in dc1 and one in dc2", volumeID, dcs)
	}
}

// When a volume server with a copy of a file stops, the file is read from the other copy. The cluster
// here updates its status only every minute, so it still lists the stopped server, as it would right
// after the server failed. Only the first read that tries the stopped server waits for it.
func TestReadWhileVolumeServerIsDown(t *testing.T) {
	cluster(t, "up")
	fid := upload(t, newCluster(t, time.Second), "hello.txt", "hello world")
	c := newCluster(t, time.Minute)
	volumeID, err := seaweedstore.ParseFid(fid)
	if err != nil {
		t.Fatal(err)
	}
	locations, err := c.LookupVolume(context.Background(), volumeID)
	if err != nil {
		t.Fatal(err)
	}
	if len(locations) != 2 {
		t.Fatalf("volume %d is at %+v, want two copies", volumeID, locations)
	}
	stopped := volumeServer(t, locations[0].Addr)
	cluster(t, "stop", stopped.name)
	t.Cleanup(func() { cluster(t, "up") })
	slow := 0
	for i := range 10 {
		start := time.Now()
		body, err := read(c, fid)
		if err != nil {
			t.Fatalf("read %d failed while %s is stopped: %v", i+1, stopped.name, err)
		}
		if body != "hello world" {
			t.Fatalf("read %d: %q", i+1, body)
		}
		if time.Since(start) > time.Second {
			slow++
		}
	}
	if slow > 1 {
		t.Errorf("%d of 10 reads took more than a second", slow)
	}
}

// When the leader stops, files can still be read while the other masters elect a new leader. Once
// there is one, the cluster finds it at its next status update. Uploads work again when the volume
// servers have found the new leader. That took up to a minute after the election, because a
// follower can send a volume server to the stopped leader, and connecting to it times out.
func TestLeaderStops(t *testing.T) {
	cluster(t, "up")
	c := newCluster(t, time.Second)
	fid := upload(t, c, "hello.txt", "hello world")
	// Wait for a status update, so that the volume is in the cache, even if it was created now
	time.Sleep(2500 * time.Millisecond)
	old := currentLeader(t)

	cluster(t, "stop", old.name)
	t.Cleanup(func() { cluster(t, "up") })
	stopped := time.Now()
	var elected server
	reads, failed := 0, 0
	var firstErr error
	for {
		if l, ok := leader(); ok && l != old {
			elected = l
			break
		}
		if time.Since(stopped) > time.Minute {
			t.Fatalf("no new leader within a minute after %s stopped", old.name)
		}
		reads++
		if _, err := read(c, fid); err != nil {
			if failed == 0 {
				firstErr = err
			}
			failed++
		}
		time.Sleep(200 * time.Millisecond)
	}
	electedAt := time.Now()
	t.Logf("%s stopped, %s was elected after %v", old.name, elected.name, electedAt.Sub(stopped).Round(100*time.Millisecond))
	if failed > 0 {
		t.Errorf("%d of %d reads failed during the election, the first with: %v", failed, reads, firstErr)
	}

	waitFor(t, 5*time.Second, "the new leader", func() error {
		l, err := c.Leader()
		if err == nil && l != elected.addr {
			err = fmt.Errorf("the leader is %s", l)
		}
		return err
	})
	ar := assign(t, c, 2*time.Minute)
	t.Logf("the first fid was assigned %v after the election", time.Since(electedAt).Round(100*time.Millisecond))
	fid = put(t, c, ar, "after.txt", "after the election")
	if body, err := read(c, fid); err != nil || body != "after the election" {
		t.Errorf("reading the file uploaded after the election: %q, %v", body, err)
	}
}

// cluster runs cluster.sh with the arguments, and fails the test if it fails.
func cluster(t *testing.T, args ...string) {
	t.Helper()
	out, err := exec.Command("./cluster.sh", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("cluster.sh %s: %v\n%s", strings.Join(args, " "), err, out)
	}
}

// newCluster returns a ready cluster that updates its status every interval. It is closed when the
// test ends, and logs to the test.
func newCluster(t *testing.T, interval time.Duration) *seaweedstore.Cluster {
	t.Helper()
	return newClusterOpts(t, seaweedstore.Options{StatusUpdateInterval: interval})
}

// newClusterOpts is newCluster with options, to which it adds the logger and the timeouts.
func newClusterOpts(t *testing.T, opts seaweedstore.Options) *seaweedstore.Cluster {
	t.Helper()
	opts.Logger = slog.New(slog.NewTextHandler(t.Output(), nil))
	opts.Timeout = 2 * time.Second
	opts.VolumeTimeout = 5 * time.Second
	var addrs []string
	for _, m := range masters {
		addrs = append(addrs, m.addr)
	}
	c, err := seaweedstore.NewCluster(opts, addrs...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.WaitUntilReady(ctx); err != nil {
		t.Fatal(err)
	}
	return c
}

// waitFor calls f every 100 ms until it returns nil, and fails the test if that doesn't happen
// within timeout.
func waitFor(t *testing.T, timeout time.Duration, what string, f func() error) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		err := f()
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("no %s within %v: %v", what, timeout, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// upload assigns a fid and uploads data to it as a text/plain file with the name, and returns the
// fid.
func upload(t *testing.T, c *seaweedstore.Cluster, name, data string) string {
	t.Helper()
	return put(t, c, assign(t, c, 10*time.Second), name, data)
}

// assign calls Assign until it succeeds, for up to timeout. Assign itself retries the 503 answers
// of SeaweedFS, but not the 406 answers of a new leader (see TestLeaderStops).
func assign(t *testing.T, c *seaweedstore.Cluster, timeout time.Duration) *seaweedstore.AssignResult {
	t.Helper()
	return assignWith(t, c, seaweedstore.AssignOptions{}, timeout)
}

// assignWith is assign with options.
func assignWith(t *testing.T, c *seaweedstore.Cluster, opts seaweedstore.AssignOptions, timeout time.Duration) *seaweedstore.AssignResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	for {
		ar, err := c.Assign(ctx, opts)
		if err == nil {
			return ar
		}
		if ctx.Err() != nil {
			t.Fatalf("no fid within %v: %v", timeout, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// put uploads data to an assigned fid as a text/plain file with the name, and returns the fid.
func put(t *testing.T, c *seaweedstore.Cluster, ar *seaweedstore.AssignResult, name, data string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ur, err := c.Upload(ctx, ar, strings.NewReader(data), int64(len(data)), name, "text/plain")
	if err != nil {
		t.Fatalf("uploading %s: %v", name, err)
	}
	if ur.Name != name[strings.LastIndexAny(name, `/\`)+1:] || ur.Size != int64(len(data)) {
		t.Errorf("uploading %q (%d bytes) gave %+v", name, len(data), *ur)
	}
	return ar.Fid
}

func get(t *testing.T, c *seaweedstore.Cluster, fid string, header http.Header) *http.Response {
	t.Helper()
	res, err := c.Get(context.Background(), fid, header)
	if err != nil {
		t.Fatalf("Get(%s): %v", fid, err)
	}
	t.Cleanup(func() { res.Body.Close() })
	return res
}

func readBody(t *testing.T, res *http.Response) string {
	t.Helper()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

// fileName returns the file name from the Content-Disposition of a response.
func fileName(t *testing.T, res *http.Response) string {
	t.Helper()
	cd := res.Header.Get("Content-Disposition")
	_, params, err := mime.ParseMediaType(cd)
	if err != nil {
		t.Fatalf("Content-Disposition %q: %v", cd, err)
	}
	return params["filename"]
}

// read reads a file and returns its content.
func read(c *seaweedstore.Cluster, fid string) (string, error) {
	res, err := c.Get(context.Background(), fid, nil)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	return string(body), err
}

func volumeServer(t *testing.T, addr string) server {
	t.Helper()
	for _, s := range volumeServers {
		if s.addr == addr {
			return s
		}
	}
	t.Fatalf("no volume server at %s", addr)
	return server{}
}

// statusClient asks the masters for their status. A stopped master doesn't answer for 3 seconds.
var statusClient = &http.Client{Timeout: time.Second}

// leader asks the masters which one is the leader.
func leader() (server, bool) {
	for _, m := range masters {
		res, err := statusClient.Get("http://" + m.addr + "/cluster/status")
		if err != nil {
			continue
		}
		var status struct{ IsLeader bool }
		err = json.NewDecoder(res.Body).Decode(&status)
		res.Body.Close()
		if err == nil && status.IsLeader {
			return m, true
		}
	}
	return server{}, false
}

func currentLeader(t *testing.T) server {
	t.Helper()
	l, ok := leader()
	if !ok {
		t.Fatal("no master says it is the leader")
	}
	return l
}
