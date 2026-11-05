package seaweedstore

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// waitUntil calls cond every 10 ms until it returns true, and fails the test if that doesn't happen
// within 5 seconds.
func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for start := time.Now(); !cond(); time.Sleep(10 * time.Millisecond) {
		if time.Since(start) > 5*time.Second {
			t.Fatalf("not within 5s: %s", what)
		}
	}
}

func TestNewCluster(t *testing.T) {
	c := newTestCluster(t, quietOptions(), "172.29.0.11:9333", "172.29.0.12:9333")
	masters := c.Masters()
	if !slices.Equal(masters, []string{"172.29.0.11:9333", "172.29.0.12:9333"}) {
		t.Errorf("masters: %v", masters)
	}
	masters[0] = "changed"
	if c.Masters()[0] != "172.29.0.11:9333" {
		t.Error("changing the result of Masters changed the cluster")
	}
	if !c.StatusTime().IsZero() {
		t.Errorf("StatusTime before the first status update: %v", c.StatusTime())
	}
	if _, err := c.Leader(); !errors.Is(err, ErrNoLeader) {
		t.Errorf("Leader before the first status update returned %v, want ErrNoLeader", err)
	}
}

func TestNewClusterInvalidMasters(t *testing.T) {
	for _, masters := range [][]string{nil, {"master1"}, {"master1:"}, {":9333"}, {"master1:x"}, {"master1:9333", "http://master2:9333"}} {
		if c, err := NewCluster(quietOptions(), masters...); err == nil {
			c.Close()
			t.Errorf("NewCluster(%q) returned no error", masters)
		}
	}
}

// Zero Options fields get defaults, and a cluster with them works.
func TestDefaultOptions(t *testing.T) {
	leader := newMaster(t, true)
	c := startCluster(t, Options{}, leader.addr())
	if c.options.StatusUpdateInterval != 5*time.Second || c.options.Timeout != 5*time.Second || c.options.VolumeTimeout != 10*time.Second {
		t.Errorf("options: %+v", c.options)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.WaitUntilReady(ctx); err != nil {
		t.Fatal(err)
	}
}

// NewCluster starts the status updates, which go on every StatusUpdateInterval until Close. Requests
// can be made at the same time (go test -race checks that). After Close, the masters are not asked
// again.
func TestStatusUpdates(t *testing.T) {
	leader := newMaster(t, true)
	leader.answer("/vol/status", http.StatusOK, volStatus(layout{"dc1": {"rack1": {"172.29.0.21:8080": {1}}}}))
	opts := quietOptions()
	opts.StatusUpdateInterval = 50 * time.Millisecond
	c := startCluster(t, opts, leader.addr())
	waitUntil(t, "3 status updates", func() bool {
		c.Leader()
		c.LookupVolume(context.Background(), 1)
		return len(leader.requestsTo("/cluster/status")) >= 3
	})
	start := time.Now()
	c.Close()
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("Close took %v", elapsed)
	}
	select {
	case <-c.done:
	default:
		t.Error("Close returned before the status updates stopped")
	}
	n := len(leader.requestsTo("/cluster/status"))
	time.Sleep(200 * time.Millisecond)
	if m := len(leader.requestsTo("/cluster/status")); m != n {
		t.Errorf("the master was asked %d more times after Close", m-n)
	}
}

// Close cancels a status update in progress.
func TestCloseCancelsUpdate(t *testing.T) {
	hanging, accepted := hangingServer(t)
	opts := quietOptions()
	opts.StatusUpdateInterval = time.Minute
	opts.Timeout = time.Minute
	c := startCluster(t, opts, hanging)
	waitUntil(t, "the master is asked", func() bool { return accepted.Load() > 0 })
	start := time.Now()
	c.Close()
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("Close took %v", elapsed)
	}
}

// After Close, the methods return ErrClosed.
func TestClosed(t *testing.T) {
	leader := newMaster(t, true)
	leader.answer("/vol/status", http.StatusOK, volStatus(layout{"dc1": {"rack1": {"172.29.0.21:8080": {3}}}}))
	c := readyCluster(t, quietOptions(), leader)
	c.Close()
	ctx := context.Background()
	_, leaderErr := c.Leader()
	_, lookupErr := c.LookupVolume(ctx, 3)
	_, assignErr := c.Assign(ctx, AssignOptions{})
	_, uploadErr := c.Upload(ctx, &AssignResult{Fid: fid, Addr: "172.29.0.21:8080"}, strings.NewReader("x"), 1, "", "")
	_, getErr := c.Get(ctx, fid, nil)
	_, statErr := c.Stat(ctx, fid)
	for name, err := range map[string]error{
		"WaitUntilReady": c.WaitUntilReady(ctx), "Leader": leaderErr, "LookupVolume": lookupErr,
		"Assign": assignErr, "Upload": uploadErr, "Get": getErr, "Stat": statErr, "Delete": c.Delete(ctx, fid),
	} {
		if err != ErrClosed {
			t.Errorf("%s after Close returned %v", name, err)
		}
	}
}

func TestWaitUntilReady(t *testing.T) {
	leader := newMaster(t, true)
	c := startCluster(t, quietOptions(), leader.addr())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.WaitUntilReady(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Leader(); err != nil {
		t.Error(err)
	}
}

// WaitUntilReady returns when there is a leader. A failed status update is retried after a second,
// although StatusUpdateInterval is a minute: here the only master becomes the leader after 300 ms.
func TestWaitUntilReadyWaitsForLeader(t *testing.T) {
	m := newMaster(t, false)
	opts := quietOptions()
	opts.StatusUpdateInterval = time.Minute
	c := startCluster(t, opts, m.addr())
	time.AfterFunc(300*time.Millisecond, func() { m.setLeader(true) })
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := c.WaitUntilReady(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Leader(); err != nil {
		t.Errorf("WaitUntilReady returned, but Leader says: %v", err)
	}
}

// Without a leader, WaitUntilReady returns the context's error, and says why the last status update
// failed.
func TestWaitUntilReadyWithoutLeader(t *testing.T) {
	m := newMaster(t, false)
	c := startCluster(t, quietOptions(), m.addr())
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	err := c.WaitUntilReady(ctx)
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "no leader") {
		t.Errorf("WaitUntilReady without a leader returned %v", err)
	}
}

// Close makes WaitUntilReady return ErrClosed.
func TestWaitUntilReadyClosed(t *testing.T) {
	m := newMaster(t, false)
	c := startCluster(t, quietOptions(), m.addr())
	time.AfterFunc(200*time.Millisecond, c.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.WaitUntilReady(ctx); err != ErrClosed {
		t.Errorf("WaitUntilReady returned %v, want ErrClosed", err)
	}
}

// A nil Logger means slog.Default(), which gets the failed status updates.
func TestNilLogger(t *testing.T) {
	var log syncBuffer
	defer slog.SetDefault(slog.Default())
	slog.SetDefault(slog.New(slog.NewTextHandler(&log, nil)))
	leader := newMaster(t, true)
	leader.answer("/vol/status", http.StatusInternalServerError, `{"error":"internal error"}`)
	c := startCluster(t, Options{}, leader.addr())
	waitUntil(t, "a logged error", func() bool { return strings.Contains(log.String(), "internal error") })
	c.Close()
}

// The leader is the master that says so about itself. A master that doesn't answer doesn't keep the
// leader from being found, and only the leader is asked for the volume status.
func TestUpdateStatusFindsLeader(t *testing.T) {
	follower, leader := newMaster(t, false), newMaster(t, true)
	c := newTestCluster(t, quietOptions(), closedPort, follower.addr(), leader.addr())
	before := time.Now()
	if err := c.updateStatus(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got, err := c.Leader(); err != nil || got != leader.addr() {
		t.Errorf("Leader() = %s, %v, want %s", got, err, leader.addr())
	}
	if c.StatusTime().Before(before) {
		t.Errorf("StatusTime %v is before the update", c.StatusTime())
	}
	if n := len(follower.requestsTo("/vol/status")); n != 0 {
		t.Errorf("the follower was asked for the volume status %d times", n)
	}
	if n := len(leader.requestsTo("/vol/status")); n != 1 {
		t.Errorf("the leader was asked for the volume status %d times, want once", n)
	}
}

// Without a leader, the error says what each master answered.
func TestUpdateStatusWithoutLeader(t *testing.T) {
	follower := newMaster(t, false)
	c := newTestCluster(t, quietOptions(), follower.addr(), closedPort)
	err := c.updateStatus(context.Background())
	if err == nil {
		t.Fatal("updateStatus returned no error without a leader")
	}
	for _, want := range []string{follower.addr() + ": not the leader", closedPort + ": "} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the error %q doesn't have %q", err, want)
		}
	}
	if _, err := c.Leader(); !errors.Is(err, ErrNoLeader) {
		t.Errorf("Leader returned %v without a leader", err)
	}
}

// An update that its context ends doesn't forget the leader.
func TestCancelledUpdateKeepsLeader(t *testing.T) {
	c := readyCluster(t, quietOptions(), newMaster(t, true))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := c.updateStatus(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("the cancelled update returned %v", err)
	}
	if _, err := c.Leader(); err != nil {
		t.Errorf("Leader after a cancelled update: %v", err)
	}
}

// A master that accepts connections but never answers delays the status update by Timeout only.
func TestUpdateStatusTimeout(t *testing.T) {
	leader := newMaster(t, true)
	opts := quietOptions()
	opts.StatusUpdateInterval = 5 * time.Second
	opts.Timeout = 200 * time.Millisecond
	hanging, _ := hangingServer(t)
	c := newTestCluster(t, opts, hanging, leader.addr())
	start := time.Now()
	if err := c.updateStatus(context.Background()); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("the update took %v with Timeout %v", elapsed, opts.Timeout)
	}
}

// Timeout limits the whole request to a master, also when it sends the headers and then nothing.
func TestMasterTimeoutCoversBody(t *testing.T) {
	stalling := slowServer(t, func(w http.ResponseWriter, r *http.Request, stop <-chan struct{}) {
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		<-stop
	})
	opts := quietOptions()
	opts.StatusUpdateInterval = 5 * time.Second
	opts.Timeout = 200 * time.Millisecond
	c := newTestCluster(t, opts, stalling)
	start := time.Now()
	c.updateStatus(context.Background())
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("the update took %v with Timeout %v", elapsed, opts.Timeout)
	}
}

// A master that didn't answer the status update is asked last, until it answers again.
func TestUnreachableMasterLast(t *testing.T) {
	follower := newMaster(t, false)
	c := newTestCluster(t, quietOptions(), closedPort, follower.addr())
	c.updateStatus(context.Background())
	if order := c.masterOrder(); order[0].addr != follower.addr() {
		t.Errorf("the masters are asked in the order %v", order)
	}
}

// The volume status is indexed by volume id. A volume with copies on several volume servers has a
// location for each, with its data center.
func TestUpdateStatusIndexesVolumes(t *testing.T) {
	leader := newMaster(t, true)
	leader.answer("/vol/status", http.StatusOK, volStatus(layout{
		"dc1": {"rack1": {"172.29.0.21:8080": {1, 2}}},
		"dc2": {"rack2": {"172.29.0.31:8080": {1, 3}}},
	}))
	c := readyCluster(t, quietOptions(), leader)
	want := map[int][]VolumeLocation{
		1: {{"172.29.0.21:8080", "172.29.0.21:8080", "dc1"}, {"172.29.0.31:8080", "172.29.0.31:8080", "dc2"}},
		2: {{"172.29.0.21:8080", "172.29.0.21:8080", "dc1"}},
		3: {{"172.29.0.31:8080", "172.29.0.31:8080", "dc2"}},
	}
	if len(c.volumes) != len(want) {
		t.Errorf("%d volumes are indexed, want %d: %v", len(c.volumes), len(want), c.volumes)
	}
	for id, locations := range want {
		got := slices.Clone(c.volumes[id])
		slices.SortFunc(got, func(a, b VolumeLocation) int { return strings.Compare(a.Addr, b.Addr) })
		if !slices.Equal(got, locations) {
			t.Errorf("volume %d is at %+v, want %+v", id, got, locations)
		}
	}
}

// While there is no leader, for example during an election, or when the leader can't send the volume
// status, the cluster keeps the volume locations it has, so that files can still be read. It keeps
// the leader only in the second case.
func TestFailedUpdateKeepsVolumeLocations(t *testing.T) {
	for _, tt := range []struct {
		name       string
		fail       func(leader *fakeServer)
		keepLeader bool
	}{
		{"no leader", func(leader *fakeServer) { leader.setLeader(false) }, false},
		{"volume status fails", func(leader *fakeServer) {
			leader.answer("/vol/status", http.StatusInternalServerError, `{"error":"internal error"}`)
		}, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			leader := newMaster(t, true)
			leader.answer("/vol/status", http.StatusOK, volStatus(layout{"dc1": {"rack1": {"172.29.0.21:8080": {1}}}}))
			c := readyCluster(t, quietOptions(), leader)
			tt.fail(leader)
			if err := c.updateStatus(context.Background()); err == nil {
				t.Error("the failed update returned no error")
			}
			if _, err := c.Leader(); (err == nil) != tt.keepLeader {
				t.Errorf("Leader after the failed update: %v", err)
			}
			locations, err := c.LookupVolume(context.Background(), 1)
			if err != nil {
				t.Fatalf("LookupVolume after a failed update: %v", err)
			}
			if len(locations) != 1 || locations[0].Addr != "172.29.0.21:8080" {
				t.Errorf("volume 1 is at %+v, want 172.29.0.21:8080", locations)
			}
		})
	}
}

func TestLookupVolumeUsesCache(t *testing.T) {
	leader := newMaster(t, true)
	leader.answer("/vol/status", http.StatusOK, volStatus(layout{"dc1": {"rack1": {"172.29.0.21:8080": {1}}}}))
	c := readyCluster(t, quietOptions(), leader)
	locations, err := c.LookupVolume(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(locations) != 1 || locations[0].Addr != "172.29.0.21:8080" {
		t.Errorf("volume 1 is at %+v", locations)
	}
	locations[0].Addr = "changed"
	if again, _ := c.LookupVolume(context.Background(), 1); again[0].Addr != "172.29.0.21:8080" {
		t.Error("changing the result of LookupVolume changed the cluster")
	}
	if n := len(leader.requestsTo("/dir/lookup")); n != 0 {
		t.Errorf("the leader was asked %d times", n)
	}
}

// A volume that is not in the cache, for example because it was created after the last status
// update, is looked up at the leader.
func TestLookupVolumeAsksLeader(t *testing.T) {
	leader := newMaster(t, true)
	c := readyCluster(t, quietOptions(), leader)
	leader.answer("/dir/lookup?volumeId=7", http.StatusOK,
		`{"volumeOrFileId":"7","locations":[{"url":"172.29.0.21:8080","publicUrl":"vol1.example.com:8080","dataCenter":"dc1","grpcPort":18080}]}`)
	locations, err := c.LookupVolume(context.Background(), 7)
	if err != nil {
		t.Fatal(err)
	}
	want := VolumeLocation{Addr: "172.29.0.21:8080", PublicAddr: "vol1.example.com:8080", DataCenter: "dc1"}
	if len(locations) != 1 || locations[0] != want {
		t.Errorf("volume 7 is at %+v, want [%+v]", locations, want)
	}
	onlyRequest(t, leader, "/dir/lookup")
}

// Without a leader, a lookup goes to the other masters, which forward it to the leader.
func TestLookupVolumeWithoutLeader(t *testing.T) {
	follower := newMaster(t, false)
	follower.answer("/dir/lookup?volumeId=7", http.StatusOK, `{"volumeOrFileId":"7","locations":[{"url":"172.29.0.21:8080"}]}`)
	c := newTestCluster(t, quietOptions(), closedPort, follower.addr())
	locations, err := c.LookupVolume(context.Background(), 7)
	if err != nil {
		t.Fatal(err)
	}
	if len(locations) != 1 || locations[0].Addr != "172.29.0.21:8080" {
		t.Errorf("volume 7 is at %+v", locations)
	}
}

// For a volume that doesn't exist, the error is ErrNotFound, and has the master's answer.
func TestLookupVolumeNotFound(t *testing.T) {
	leader := newMaster(t, true)
	c := readyCluster(t, quietOptions(), leader)
	leader.answer("/dir/lookup?volumeId=9", http.StatusNotFound, lookupNotFound)
	locations, err := c.LookupVolume(context.Background(), 9)
	var he *HTTPError
	if !errors.Is(err, ErrNotFound) || !errors.As(err, &he) || he.Message != "volume id 9 not found" {
		t.Errorf("LookupVolume of a missing volume returned %+v, %v", locations, err)
	}
}

func TestParseFid(t *testing.T) {
	for _, tt := range []struct {
		fid  string
		want int
	}{
		{fid, 3},
		{fid + "_1", 3},
		{"12345,0abc", 12345},
		{"3,01637037D6", 3},
	} {
		if got, err := ParseFid(tt.fid); err != nil || got != tt.want {
			t.Errorf("ParseFid(%q) = %d, %v, want %d", tt.fid, got, err, tt.want)
		}
	}
	for _, bad := range []string{
		"", "3", "01637037d6", ",01637037d6", "x,01637037d6", "+3,01637037d6", "3,", "3,xyz",
		"3,01637037d6_", "3,01637037d6_x", "3,01637037d6_1_2", "3,01637037d6.jpg",
		"3,01637037d6/../status", "3,01637037d6?x=1", "3,01637037d6#x", "3,0163%2F",
	} {
		if got, err := ParseFid(bad); err == nil {
			t.Errorf("ParseFid(%q) = %d, want an error", bad, got)
		}
	}
}

// The cluster sends its requests with Options.HTTPClient, and closes every response body, also after
// an error, so that the connections can be reused.
func TestHTTPClient(t *testing.T) {
	var tracker bodyTracker
	opts := quietOptions()
	opts.HTTPClient = &http.Client{Transport: &tracker}
	leader := newMaster(t, true)
	vol, ar := uploadServer(t)
	leader.answer("/vol/status", http.StatusOK, volStatus(layout{"dc1": {"rack1": {vol.addr(): {3}}}}))
	c := readyCluster(t, opts, leader)
	leader.answer("/dir/lookup?volumeId=7", http.StatusOK, `{"volumeOrFileId":"7","locations":[{"url":"172.29.0.21:8080"}]}`)
	ctx := context.Background()
	for _, tt := range []struct {
		name string
		call func()
	}{
		{"status update", func() { c.updateStatus(ctx) }},
		{"lookup", func() { c.LookupVolume(ctx, 7) }},
		{"lookup not found", func() { c.LookupVolume(ctx, 9) }},
		{"assign", func() {
			leader.answer("/dir/assign", http.StatusOK, `{"fid":"5,01694433fe","url":"172.29.0.21:8080","count":1}`)
			c.Assign(ctx, AssignOptions{})
		}},
		{"assign refused", func() {
			leader.answer("/dir/assign", http.StatusNotAcceptable, assignNotAcceptable)
			c.Assign(ctx, AssignOptions{})
		}},
		{"upload", func() { c.Upload(ctx, ar, strings.NewReader("hello world"), 11, "", "") }},
		{"upload refused", func() {
			vol.answer("/"+fid, http.StatusInternalServerError, `{"error":"internal error"}`)
			c.Upload(ctx, ar, strings.NewReader("hello world"), 11, "", "")
		}},
		{"get", func() {
			vol.answer("/"+fid, http.StatusOK, "hello world")
			if res, err := c.Get(ctx, fid, nil); err == nil {
				io.ReadAll(res.Body)
				res.Body.Close()
			}
		}},
		{"get not found", func() {
			vol.answer("/"+fid, http.StatusNotFound, "")
			c.Get(ctx, fid, nil)
		}},
		{"get range not satisfiable", func() {
			vol.answer("/"+fid, http.StatusRequestedRangeNotSatisfiable, "")
			c.Get(ctx, fid, nil)
		}},
		{"stat", func() {
			vol.answer("/"+fid, http.StatusOK, "hello world")
			c.Stat(ctx, fid)
		}},
		{"delete", func() {
			vol.answer("/"+fid, http.StatusAccepted, `{"size":40}`)
			c.Delete(ctx, fid)
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			before := tracker.requests.Load()
			tt.call()
			if tracker.requests.Load() == before {
				t.Error("no request went through Options.HTTPClient")
			}
			if n := tracker.open.Load(); n != 0 {
				t.Errorf("%d response bodies are left open", n)
			}
		})
	}
}

// Without Options.HTTPClient, the cluster has a client of its own on a copy of http.DefaultTransport,
// which closes unused connections before a volume server with the default -idleTimeout of 30 seconds
// does, and Close closes them. Close leaves a client from Options.HTTPClient alone.
func TestDefaultClient(t *testing.T) {
	server := func(t *testing.T) (addr string, opened, closed *atomic.Int32) {
		opened, closed = &atomic.Int32{}, &atomic.Int32{}
		srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusAccepted)
		}))
		srv.Config.ConnState = func(_ net.Conn, state http.ConnState) {
			switch state {
			case http.StateNew:
				opened.Add(1)
			case http.StateClosed:
				closed.Add(1)
			}
		}
		srv.Start()
		t.Cleanup(srv.Close)
		return srv.Listener.Addr().String(), opened, closed
	}

	t.Run("default", func(t *testing.T) {
		addr, _, closed := server(t)
		c := volumeCluster(t, quietOptions(), addr)
		transport, ok := c.client.Transport.(*http.Transport)
		if !ok || transport == http.DefaultTransport {
			t.Fatalf("the client has the transport %v", c.client.Transport)
		}
		if d := transport.IdleConnTimeout; d <= 0 || d >= 30*time.Second {
			t.Errorf("IdleConnTimeout = %v, want it below 30 seconds", d)
		}
		if transport.Proxy == nil || transport.DialContext == nil {
			t.Error("the transport is not a copy of http.DefaultTransport")
		}
		if err := c.Delete(context.Background(), fid); err != nil {
			t.Fatal(err)
		}
		c.Close()
		waitUntil(t, "Close closes the unused connection", func() bool { return closed.Load() == 1 })
	})

	t.Run("given", func(t *testing.T) {
		addr, opened, _ := server(t)
		client := &http.Client{Transport: &http.Transport{}}
		defer client.CloseIdleConnections()
		opts := quietOptions()
		opts.HTTPClient = client
		c := volumeCluster(t, opts, addr)
		if err := c.Delete(context.Background(), fid); err != nil {
			t.Fatal(err)
		}
		c.Close()
		res, err := client.Get("http://" + addr + "/")
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		if n := opened.Load(); n != 1 {
			t.Errorf("the server saw %d connections: Close closed the unused one", n)
		}
	})
}
