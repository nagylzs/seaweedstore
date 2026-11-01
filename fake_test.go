package seaweedstore

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"mime"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Answers of SeaweedFS 4.50 servers, captured with curl. A follower leaves IsLeader out, and the
// addresses in Leader and Peers end with the gRPC port.
const (
	leaderStatus   = `{"IsLeader":true,"Leader":"172.30.0.11:9333.19333","Peers":["172.30.0.13:9333.19333","172.30.0.12:9333.19333"],"MaxVolumeId":5}`
	followerStatus = `{"Leader":"172.30.0.11:9333.19333","Peers":["172.30.0.13:9333.19333","172.30.0.11:9333.19333"],"MaxVolumeId":5}`
	// One volume in a /vol/status answer, for replication 100. %d is the volume id.
	volumeInfo = `{"Collection":"","DiskType":"","RemoteStorageName":"","ReplicaPlacement":{"dc":1},"Ttl":{"Count":0,"Unit":0},"Size":160,"DeletedByteCount":0,"ModifiedAtSecond":1793539328,"Id":%d,"DiskId":0,"CompactRevision":0,"FileCount":2,"DeleteCount":0,"Version":3,"ReadOnly":false,"ReadOnlyCanDelete":false}`
	// The /dir/lookup answer for a volume that doesn't exist (HTTP 404).
	lookupNotFound = `{"volumeOrFileId":"9","error":"volume id 9 not found"}`
	// The /dir/assign answer when no volume can be created (HTTP 406).
	assignNotAcceptable = `{"error":"failed to find writable volumes for collection: replication:200 ttl: error: No writable volumes and no free volumes left for {\"replication\":{\"dc\":2},\"ttl\":{\"Count\":0,\"Unit\":0},\"version\":3}"}`
	// The /dir/assign answer while the master creates volumes (HTTP 503, with Retry-After: 1).
	assignGrowing = `{"error":"no writable volumes for {\"collection\":\"probe\",\"replication\":{},\"ttl\":{\"Count\":0,\"Unit\":0},\"version\":3}, volume growth in progress"}`
	// The volume server's answer to an upload (HTTP 201).
	uploadCreated = `{"name":"hello.txt","size":11,"eTag":"c99465aa","mime":"text/plain"}`
)

// closedPort and closedPort2 are addresses where nothing listens, so that requests to them fail
// right away.
const (
	closedPort  = "127.0.0.1:1"
	closedPort2 = "127.0.0.1:2"
)

// layout maps each data center to its racks, each rack to its volume servers, and each volume server
// to the ids of the volumes it has.
type layout map[string]map[string]map[string][]int

// volStatus returns the /vol/status answer of a master for the given layout.
func volStatus(l layout) string {
	dcs := map[string]map[string]map[string][]json.RawMessage{}
	for dc, racks := range l {
		dcs[dc] = map[string]map[string][]json.RawMessage{}
		for rack, servers := range racks {
			dcs[dc][rack] = map[string][]json.RawMessage{}
			for server, ids := range servers {
				volumes := []json.RawMessage{}
				for _, id := range ids {
					volumes = append(volumes, json.RawMessage(fmt.Sprintf(volumeInfo, id)))
				}
				dcs[dc][rack][server] = volumes
			}
		}
	}
	b, err := json.Marshal(map[string]any{
		"Version": "30GB 4.50 ec8f97935",
		"Volumes": map[string]any{"DataCenters": dcs, "Free": 10, "Max": 20},
	})
	if err != nil {
		panic(err)
	}
	return string(b)
}

// answer is what a fakeServer sends for a path.
type answer struct {
	status int
	body   string
	header map[string]string
}

// request is a request that a fakeServer got.
type request struct {
	method        string
	path          string
	query         string
	header        http.Header
	body          string
	contentLength int64 // -1 when unknown
	chunked       bool
}

// fakeServer stands in for a SeaweedFS master or volume server. It sends the answer set for the
// request URI, or for the path when there is none for the URI, and 404 when there is neither.
// Answers queued with answerFirst come before the one set with answer. It records every request.
type fakeServer struct {
	srv      *httptest.Server
	mu       sync.Mutex
	answers  map[string]answer
	queued   map[string][]answer
	requests []request
}

func newFakeServer(t *testing.T) *fakeServer {
	f := &fakeServer{answers: map[string]answer{}, queued: map[string][]answer{}}
	f.srv = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeServer) serve(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	f.mu.Lock()
	f.requests = append(f.requests, request{
		method:        r.Method,
		path:          r.URL.Path,
		query:         r.URL.RawQuery,
		header:        r.Header.Clone(),
		body:          string(body),
		contentLength: r.ContentLength,
		chunked:       len(r.TransferEncoding) > 0 && r.TransferEncoding[0] == "chunked",
	})
	a, ok := f.next(r.RequestURI)
	if !ok {
		a, ok = f.next(r.URL.Path)
	}
	f.mu.Unlock()
	if !ok {
		a = answer{status: http.StatusNotFound, body: `{"error":"not found"}`}
	}
	w.Header().Set("Content-Type", "application/json")
	for k, v := range a.header {
		w.Header().Set(k, v)
	}
	w.WriteHeader(a.status)
	io.WriteString(w, a.body)
}

// next returns the answer for uri. f.mu must be held.
func (f *fakeServer) next(uri string) (answer, bool) {
	if q := f.queued[uri]; len(q) > 0 {
		f.queued[uri] = q[1:]
		return q[0], true
	}
	a, ok := f.answers[uri]
	return a, ok
}

// answerFirst queues answers for uri, which are sent once each before the one set with answer.
func (f *fakeServer) answerFirst(uri string, answers ...answer) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.queued[uri] = append(f.queued[uri], answers...)
}

// addr returns the server's address as host:port, the way SeaweedFS lists servers.
func (f *fakeServer) addr() string {
	return f.srv.Listener.Addr().String()
}

// answer sets the answer for a path, or for a path with a query like "/dir/lookup?volumeId=7".
func (f *fakeServer) answer(uri string, status int, body string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.answers[uri] = answer{status: status, body: body}
}

// requestsTo returns the requests the server got for a path, ignoring the query.
func (f *fakeServer) requestsTo(path string) []request {
	f.mu.Lock()
	defer f.mu.Unlock()
	var result []request
	for _, r := range f.requests {
		if r.path == path {
			result = append(result, r)
		}
	}
	return result
}

// newMaster returns a fake master that is the leader or a follower, with no volumes.
func newMaster(t *testing.T, leader bool) *fakeServer {
	m := newFakeServer(t)
	m.setLeader(leader)
	m.answer("/vol/status", http.StatusOK, volStatus(nil))
	return m
}

func (f *fakeServer) setLeader(leader bool) {
	if leader {
		f.answer("/cluster/status", http.StatusOK, leaderStatus)
	} else {
		f.answer("/cluster/status", http.StatusOK, followerStatus)
	}
}

// slowServer runs handler for every request, and returns its address. The handler gets a channel
// that is closed when the test ends, before the server is closed, so a handler can wait for it.
func slowServer(t *testing.T, handler func(w http.ResponseWriter, r *http.Request, stop <-chan struct{})) string {
	stop := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handler(w, r, stop)
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(stop) })
	return srv.Listener.Addr().String()
}

// slowReader gives data one byte at a time, each after delay.
type slowReader struct {
	data  string
	delay time.Duration
}

func (r *slowReader) Read(p []byte) (int, error) {
	if r.data == "" {
		return 0, io.EOF
	}
	time.Sleep(r.delay)
	n := copy(p[:1], r.data)
	r.data = r.data[n:]
	return n, nil
}

// hangingServer accepts connections but never answers. It returns its address, and how many
// connections it has accepted.
func hangingServer(t *testing.T) (string, *atomic.Int32) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var conns []net.Conn
	t.Cleanup(func() {
		ln.Close()
		mu.Lock()
		defer mu.Unlock()
		for _, conn := range conns {
			conn.Close()
		}
	})
	var accepted atomic.Int32
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, conn)
			mu.Unlock()
			accepted.Add(1)
		}
	}()
	return ln.Addr().String(), &accepted
}

func quietOptions() Options {
	return Options{
		Logger:               slog.New(slog.NewTextHandler(io.Discard, nil)),
		StatusUpdateInterval: time.Second,
		Timeout:              time.Second,
		VolumeTimeout:        time.Second,
	}
}

// newTestCluster returns a cluster of the given master addresses, without status updates in the
// background, so that the tests decide when they happen. It is closed when the test ends.
func newTestCluster(t *testing.T, opts Options, masters ...string) *Cluster {
	c := newCluster(opts, masters...)
	t.Cleanup(c.Close)
	return c
}

// startCluster returns a cluster that updates its status in the background, and is closed when the
// test ends.
func startCluster(t *testing.T, opts Options, masters ...string) *Cluster {
	c, err := NewCluster(opts, masters...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

// readyCluster returns a test cluster whose status is updated once.
func readyCluster(t *testing.T, opts Options, masters ...*fakeServer) *Cluster {
	var addrs []string
	for _, m := range masters {
		addrs = append(addrs, m.addr())
	}
	c := newTestCluster(t, opts, addrs...)
	if err := c.updateStatus(context.Background()); err != nil {
		t.Fatal(err)
	}
	return c
}

// bodyTracker is a transport for Options.HTTPClient that counts the requests, and the response
// bodies that are not closed yet.
type bodyTracker struct {
	requests atomic.Int32
	open     atomic.Int32
}

func (b *bodyTracker) RoundTrip(req *http.Request) (*http.Response, error) {
	b.requests.Add(1)
	res, err := http.DefaultTransport.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	b.open.Add(1)
	res.Body = &trackedBody{ReadCloser: res.Body, tracker: b}
	return res, nil
}

type trackedBody struct {
	io.ReadCloser
	once    sync.Once
	tracker *bodyTracker
}

func (tb *trackedBody) Close() error {
	tb.once.Do(func() { tb.tracker.open.Add(-1) })
	return tb.ReadCloser.Close()
}

// fid is a file id on volume 3.
const fid = "3,01637037d6"

// volumeCluster returns a ready test cluster whose leader lists volume 3 on the given volume servers,
// all in data center dc1.
func volumeCluster(t *testing.T, opts Options, servers ...string) *Cluster {
	leader := newMaster(t, true)
	l := layout{"dc1": {"rack1": {}}}
	for _, s := range servers {
		l["dc1"]["rack1"][s] = []int{3}
	}
	leader.answer("/vol/status", http.StatusOK, volStatus(l))
	return readyCluster(t, opts, leader)
}

// uploadServer returns a fake volume server that accepts an upload of fid, and the AssignResult for
// it.
func uploadServer(t *testing.T) (*fakeServer, *AssignResult) {
	vol := newFakeServer(t)
	vol.answer("/"+fid, http.StatusCreated, uploadCreated)
	return vol, &AssignResult{Fid: fid, Addr: vol.addr(), PublicAddr: vol.addr(), Count: 1}
}

// onlyRequest returns the only request the server got for path.
func onlyRequest(t *testing.T, f *fakeServer, path string) request {
	t.Helper()
	reqs := f.requestsTo(path)
	if len(reqs) != 1 {
		t.Fatalf("%d requests for %s, want 1", len(reqs), path)
	}
	return reqs[0]
}

// syncBuffer is a bytes.Buffer that a cluster's goroutine and the test can use at the same time.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// memStore keeps the files of memVolumes, which can share it like copies of a volume.
type memStore struct {
	mu    sync.Mutex
	files map[string]*memFile // fid -> file
	fail  map[string]int      // fid -> HTTP status to answer to an upload
}

type memFile struct {
	data        []byte
	contentType string
	name        string
	manifest    bool // uploaded with ?cm=true
	modified    time.Time
}

func newMemStore() *memStore {
	return &memStore{files: map[string]*memFile{}, fail: map[string]int{}}
}

func (s *memStore) file(fid string) *memFile {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.files[fid]
}

// memVolume is a fake volume server that keeps what is uploaded to it in a memStore, and serves it
// with Range, If-Range and If-None-Match (by http.ServeContent). Like SeaweedFS, it sends a manifest
// as it is stored for ?cm=false, and otherwise the file of its chunks, which it joins for a GET
// request (HTTP 500 if one is missing), but not for a HEAD request. It deletes the chunks of a
// manifest with it.
type memVolume struct {
	*fakeServer // only for its requests and address
	store       *memStore
}

func newMemVolume(t *testing.T, store *memStore) *memVolume {
	v := &memVolume{store: store}
	v.fakeServer = &fakeServer{answers: map[string]answer{}, queued: map[string][]answer{}}
	v.srv = httptest.NewServer(http.HandlerFunc(v.serve))
	t.Cleanup(v.srv.Close)
	return v
}

func (v *memVolume) serve(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	v.mu.Lock()
	v.requests = append(v.requests, request{method: r.Method, path: r.URL.Path, query: r.URL.RawQuery,
		header: r.Header.Clone(), body: string(body), contentLength: r.ContentLength})
	v.mu.Unlock()
	fid := strings.TrimPrefix(r.URL.Path, "/")
	s := v.store
	s.mu.Lock()
	defer s.mu.Unlock()
	f := s.files[fid]
	switch r.Method {
	case http.MethodPut:
		if status := s.fail[fid]; status != 0 {
			http.Error(w, `{"error":"failed"}`, status)
			return
		}
		f = &memFile{data: body, contentType: r.Header.Get("Content-Type"), manifest: r.URL.Query().Get("cm") == "true",
			modified: time.Now().Truncate(time.Second)}
		if _, params, err := mime.ParseMediaType(r.Header.Get("Content-Disposition")); err == nil {
			f.name = params["filename"]
		}
		s.files[fid] = f
		name := f.name
		if name == "" {
			name = fid
		}
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]any{"name": name, "size": len(body), "eTag": f.etag(), "mime": f.contentType})
	case http.MethodGet, http.MethodHead:
		if f == nil {
			http.Error(w, "", http.StatusNotFound)
			return
		}
		if f.manifest && r.URL.Query().Get("cm") != "false" {
			var m chunkManifest
			json.Unmarshal(f.data, &m)
			w.Header().Set("X-File-Store", "chunked")
			w.Header().Set("Content-Type", m.Mime)
			w.Header().Set("Content-Disposition", mime.FormatMediaType("inline", map[string]string{"filename": m.Name}))
			w.Header().Set("Etag", `"`+f.etag()+`"`)
			if r.Method == http.MethodHead {
				w.Header().Set("Content-Length", strconv.FormatInt(m.Size, 10))
				w.Header().Set("Last-Modified", f.modified.UTC().Format(http.TimeFormat))
				return
			}
			var joined []byte
			for _, ch := range m.Chunks {
				cf := s.files[ch.Fid]
				if cf == nil {
					http.Error(w, "missing chunk", http.StatusInternalServerError)
					return
				}
				joined = append(joined, cf.data...)
			}
			http.ServeContent(w, r, "", f.modified, bytes.NewReader(joined))
			return
		}
		if f.contentType != "" {
			w.Header().Set("Content-Type", f.contentType)
		}
		name := f.name
		if name == "" {
			name = fid
		}
		w.Header().Set("Content-Disposition", mime.FormatMediaType("inline", map[string]string{"filename": name}))
		w.Header().Set("Etag", `"`+f.etag()+`"`)
		http.ServeContent(w, r, "", f.modified, bytes.NewReader(f.data))
	case http.MethodDelete:
		if f == nil {
			http.Error(w, `{"size":0}`, http.StatusNotFound)
			return
		}
		delete(s.files, fid)
		if f.manifest {
			var m chunkManifest
			json.Unmarshal(f.data, &m)
			for _, ch := range m.Chunks {
				delete(s.files, ch.Fid)
			}
		}
		w.WriteHeader(http.StatusAccepted)
		fmt.Fprintf(w, `{"size":%d}`, len(f.data))
	}
}

func (f *memFile) etag() string {
	return fmt.Sprintf("%08x", crc32.ChecksumIEEE(f.data))
}

// memCluster returns a ready test cluster whose leader lists volume 3 on the given servers, all in
// data center dc1, and assigns 100 fids on volume 3 on the first of them. It also returns the leader.
func memCluster(t *testing.T, opts Options, servers ...string) (*Cluster, *fakeServer) {
	return memClusterLayout(t, opts, servers[0], layout{"dc1": {"rack1": func() map[string][]int {
		m := map[string][]int{}
		for _, s := range servers {
			m[s] = []int{3}
		}
		return m
	}()}})
}

// memClusterLayout is memCluster with the volume servers in l, and fids assigned on upload.
func memClusterLayout(t *testing.T, opts Options, upload string, l layout) (*Cluster, *fakeServer) {
	leader := newMaster(t, true)
	leader.answer("/vol/status", http.StatusOK, volStatus(l))
	for i := range 100 {
		leader.answerFirst("/dir/assign", answer{status: http.StatusOK,
			body: fmt.Sprintf(`{"fid":"3,%02x0000abcd","url":"%s","publicUrl":"%s","count":1}`, i+1, upload, upload)})
	}
	return readyCluster(t, opts, leader), leader
}
