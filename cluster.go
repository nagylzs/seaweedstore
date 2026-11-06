package seaweedstore

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	defaultStatusUpdateInterval = 5 * time.Second
	defaultTimeout              = 5 * time.Second
	defaultVolumeTimeout        = 10 * time.Second
	defaultChunkSize            = 32 << 20
	// The default client closes a connection unused for this long, which must come before a volume
	// server closes it: its -idleTimeout is 30 seconds by default.
	defaultIdleConnTimeout = 25 * time.Second
	// After a failed status update, the next one comes after this long, if that is shorter than
	// StatusUpdateInterval.
	retryInterval = time.Second
	// A server that a request could not reach is tried last for this long.
	failedServerPenalty = 30 * time.Second
	// Assign waits this long after a 503 answer without Retry-After.
	defaultRetryAfter = time.Second
)

// Cluster is a SeaweedFS cluster. It knows the leader master and the volume servers of every volume
// from status updates, which it makes in the background. All methods are safe for concurrent use.
type Cluster struct {
	options Options
	client  *http.Client
	ctx     context.Context
	cancel  context.CancelFunc // cancels ctx; called by Close
	done    chan struct{}      // closed when the status updates have stopped; nil if they never started
	ready   chan struct{}      // closed after the first complete status update
	wake    chan struct{}      // asks for a status update now, without waiting for the next one

	mu         sync.Mutex
	masters    []master                 // SetMasters replaces it, and never changes it in place
	leaderIdx  int                      // index in masters, -1 if there is no leader
	volumes    map[int][]VolumeLocation // volume id -> locations, from the last complete status update
	statusTime time.Time                // when the last complete status update finished
	lastErr    error                    // of the last status update
	failedAt   map[string]time.Time     // server address -> when a request could not reach it
}

// NewCluster returns a cluster of the given master addresses (host:port), and starts updating its
// status in the background: every Options.StatusUpdateInterval, and a second after a failed update,
// until Close. Call WaitUntilReady to wait for the first update.
func NewCluster(opts Options, masters ...string) (*Cluster, error) {
	if err := checkMasters(masters); err != nil {
		return nil, err
	}
	c := newCluster(opts, masters...)
	c.done = make(chan struct{})
	go c.run()
	return c, nil
}

// checkMasters checks the addresses of masters.
func checkMasters(masters []string) error {
	if len(masters) == 0 {
		return errors.New("seaweedstore: no masters")
	}
	for _, m := range masters {
		if host, port, err := net.SplitHostPort(m); err != nil || host == "" || !onlyOf(port, "0123456789") {
			return fmt.Errorf("seaweedstore: invalid master address %q, want host:port", m)
		}
	}
	return nil
}

// newCluster returns a cluster without starting the status updates.
func newCluster(opts Options, masters ...string) *Cluster {
	if opts.StatusUpdateInterval <= 0 {
		opts.StatusUpdateInterval = defaultStatusUpdateInterval
	}
	if opts.Timeout <= 0 {
		opts.Timeout = defaultTimeout
	}
	if opts.VolumeTimeout <= 0 {
		opts.VolumeTimeout = defaultVolumeTimeout
	}
	if opts.ChunkSize == 0 {
		opts.ChunkSize = defaultChunkSize
	}
	c := &Cluster{
		options:   opts,
		client:    opts.HTTPClient,
		ready:     make(chan struct{}),
		wake:      make(chan struct{}, 1),
		leaderIdx: -1,
		volumes:   make(map[int][]VolumeLocation),
		failedAt:  make(map[string]time.Time),
	}
	if c.client == nil {
		c.client = defaultClient()
	}
	c.ctx, c.cancel = context.WithCancel(context.Background())
	c.masters = masterList(masters)
	return c
}

func masterList(addrs []string) []master {
	masters := make([]master, 0, len(addrs))
	for _, addr := range addrs {
		masters = append(masters, master{addr: addr})
	}
	return masters
}

// defaultClient returns the client for a nil Options.HTTPClient: one on a copy of http.DefaultTransport
// with defaultIdleConnTimeout, or on http.DefaultTransport if a program has replaced it with something
// else.
func defaultClient() *http.Client {
	t, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return &http.Client{}
	}
	t = t.Clone()
	t.IdleConnTimeout = defaultIdleConnTimeout
	return &http.Client{Transport: t}
}

// run updates the status until Close.
func (c *Cluster) run() {
	defer close(c.done)
	for {
		wait := c.options.StatusUpdateInterval
		if err := c.updateStatus(c.ctx); err != nil {
			if c.ctx.Err() != nil {
				return
			}
			c.logger().Error("cluster status update failed", "error", err.Error())
			wait = min(wait, retryInterval)
		}
		select {
		case <-c.ctx.Done():
			return
		case <-time.After(wait):
		case <-c.wake:
		}
	}
}

// SetMasters replaces the masters, for a cluster whose masters change while the program runs. It returns an error if
// there are none or an address is not host:port, and then nothing changes. The leader stays if it is one of them, and a
// status update starts right away in the background. A request in progress goes on with the masters that it has started
// with.
func (c *Cluster) SetMasters(masters ...string) error {
	if err := c.closed(); err != nil {
		return err
	}
	if err := checkMasters(masters); err != nil {
		return err
	}
	list := masterList(masters)
	c.mu.Lock()
	changed := !slices.Equal(list, c.masters)
	if changed {
		leader := -1
		if c.leaderIdx >= 0 {
			leader = slices.Index(list, c.masters[c.leaderIdx])
		}
		c.masters, c.leaderIdx = list, leader
	}
	c.mu.Unlock()
	if changed {
		select {
		case c.wake <- struct{}{}:
		default: // one is due already
		}
	}
	return nil
}

// Done returns a channel that is closed by Close.
func (c *Cluster) Done() <-chan struct{} { return c.ctx.Done() }

// Close stops the status updates, cancelling one in progress, and waits until they have stopped.
// Requests in progress go on, but new ones return ErrClosed. It closes the unused connections of the
// default client, but not those of Options.HTTPClient.
func (c *Cluster) Close() {
	c.cancel()
	if c.done != nil {
		<-c.done
	}
	if c.options.HTTPClient == nil {
		c.client.CloseIdleConnections()
	}
}

// closed returns ErrClosed after Close, and nil before.
func (c *Cluster) closed() error {
	if c.ctx.Err() != nil {
		return ErrClosed
	}
	return nil
}

// logger returns Options.Logger, or slog.Default() if it is nil.
func (c *Cluster) logger() *slog.Logger {
	if c.options.Logger != nil {
		return c.options.Logger
	}
	return slog.Default()
}

// WaitUntilReady waits for the first complete status update: there is a leader, and it has sent the
// volume status. When ctx is done first, it returns ctx's error, with the error of the last status
// update.
func (c *Cluster) WaitUntilReady(ctx context.Context) error {
	if err := c.closed(); err != nil {
		return err
	}
	select {
	case <-c.ready:
		return nil
	default:
	}
	select {
	case <-c.ready:
		return nil
	case <-c.ctx.Done():
		return ErrClosed
	case <-ctx.Done():
		c.mu.Lock()
		last := c.lastErr
		c.mu.Unlock()
		if last != nil {
			return fmt.Errorf("%w; the last status update: %w", ctx.Err(), last)
		}
		return ctx.Err()
	}
}

// updateStatus asks every master for its status, and the leader for the volume status. Without a
// leader it forgets the leader. When there is no leader or it can't get the volume status, it keeps
// the volume locations it has, so that files can still be read. When ctx is done, it changes
// nothing.
func (c *Cluster) updateStatus(ctx context.Context) (err error) {
	defer func() {
		if ctx.Err() == nil {
			c.mu.Lock()
			c.lastErr = err
			c.mu.Unlock()
		}
	}()
	ctx2, cancel2 := context.WithTimeout(ctx, c.options.StatusUpdateInterval)
	defer cancel2()
	c.mu.Lock()
	masters := c.masters // SetMasters may replace them during the update
	c.mu.Unlock()
	statuses := make([]clusterStatus, len(masters))
	errs := make([]error, len(masters))
	var wg sync.WaitGroup
	for idx, m := range masters {
		wg.Go(func() {
			errs[idx] = c.getJSON(ctx2, m, "/cluster/status", &statuses[idx])
			if errs[idx] != nil {
				statuses[idx] = clusterStatus{}
			}
		})
	}
	wg.Wait()
	if err := ctx.Err(); err != nil {
		return err // not an answer from the masters
	}
	for idx, m := range masters {
		c.setReachable(m.addr, reached(errs[idx]))
	}
	// select leader
	lIdx := slices.IndexFunc(statuses, func(s clusterStatus) bool { return s.IsLeader })
	c.mu.Lock()
	c.leaderIdx = -1
	if lIdx >= 0 {
		c.leaderIdx = slices.Index(c.masters, masters[lIdx])
	}
	c.mu.Unlock()
	if lIdx == -1 {
		reasons := make([]string, len(masters))
		for idx, m := range masters {
			reasons[idx] = m.addr + ": not the leader"
			if errs[idx] != nil {
				reasons[idx] = m.addr + ": " + errs[idx].Error()
			}
		}
		return fmt.Errorf("no leader (%s)", strings.Join(reasons, "; "))
	}

	// get cluster info
	leader := masters[lIdx]
	ctx3, cancel3 := context.WithTimeout(ctx, c.options.StatusUpdateInterval)
	defer cancel3()
	var vs volumeStatus
	if err := c.getJSON(ctx3, leader, "/vol/status", &vs); err != nil {
		return fmt.Errorf("could not get the volume status from the leader %s: %w", leader.addr, err)
	}

	// index/collect volume addresses for volume ids
	iv := make(map[int][]VolumeLocation)
	for dcName, dc := range vs.Volumes.DataCenters {
		for _, rack := range dc {
			for addr, volumes := range rack {
				for _, vi := range volumes {
					iv[vi.Id] = append(iv[vi.Id], VolumeLocation{
						Addr:       addr,
						PublicAddr: addr,
						DataCenter: dcName,
					})
				}
			}
		}
	}

	c.mu.Lock()
	first := c.statusTime.IsZero()
	c.statusTime, c.volumes = time.Now(), iv
	c.mu.Unlock()
	if first {
		close(c.ready)
	}
	return nil
}

// Leader returns the address of the leader master as of the last status update, or ErrNoLeader.
func (c *Cluster) Leader() (string, error) {
	if err := c.closed(); err != nil {
		return "", err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.leaderIdx < 0 {
		return "", ErrNoLeader
	}
	return c.masters[c.leaderIdx].addr, nil
}

// Masters returns the addresses of the masters, in the order given to NewCluster or SetMasters.
func (c *Cluster) Masters() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	addrs := make([]string, len(c.masters))
	for idx, m := range c.masters {
		addrs[idx] = m.addr
	}
	return addrs
}

// StatusTime returns when the last complete status update finished, or the zero time before the
// first one.
func (c *Cluster) StatusTime() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.statusTime
}

// reached reports whether a request got an answer, maybe an error answer.
func reached(err error) bool {
	var he *HTTPError
	return err == nil || errors.As(err, &he)
}

// setReachable records whether a request could reach the server at addr.
func (c *Cluster) setReachable(addr string, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if ok {
		delete(c.failedAt, addr)
	} else {
		c.failedAt[addr] = time.Now()
	}
}

// failedLast returns the servers, with those that a request couldn't reach in the last
// failedServerPenalty moved to the end. c.mu must be held.
func failedLast[S any](c *Cluster, servers []S, addr func(S) string) []S {
	now := time.Now()
	maps.DeleteFunc(c.failedAt, func(_ string, t time.Time) bool { return now.Sub(t) >= failedServerPenalty })
	var reachable, failed []S
	for _, s := range servers {
		if _, ok := c.failedAt[addr(s)]; ok {
			failed = append(failed, s)
		} else {
			reachable = append(reachable, s)
		}
	}
	return append(reachable, failed...)
}

// masterOrder returns the leader, if there is one, and then the other masters, with those that a
// request couldn't reach recently at the end.
func (c *Cluster) masterOrder() []master {
	c.mu.Lock()
	defer c.mu.Unlock()
	order := make([]master, 0, len(c.masters))
	if c.leaderIdx >= 0 {
		order = append(order, c.masters[c.leaderIdx])
	}
	for idx, m := range c.masters {
		if idx != c.leaderIdx {
			order = append(order, m)
		}
	}
	return failedLast(c, order, func(m master) string { return m.addr })
}

// askMasters sends a GET request for path to the leader, and decodes the answer into v (see
// getJSON). When it can't reach the leader, or there is none, it goes on to the other masters (see
// masterOrder), which forward requests to the leader.
func (c *Cluster) askMasters(ctx context.Context, path string, v any) error {
	var errs []error
	for _, m := range c.masterOrder() {
		err := c.getJSON(ctx, m, path, v)
		if reached(err) {
			c.setReachable(m.addr, true)
			return err
		}
		if ctx.Err() != nil {
			return err
		}
		c.setReachable(m.addr, false)
		errs = append(errs, err)
	}
	if len(errs) == 0 {
		return errors.New("no masters")
	}
	return errors.Join(errs...)
}

// LookupVolume returns the volume servers that have a copy of a volume, from the last status
// update. If the volume is not known there, for example because it was created since, it asks the
// leader, or the other masters when it can't reach the leader. For a volume that doesn't exist, it
// returns ErrNotFound.
func (c *Cluster) LookupVolume(ctx context.Context, volumeID int) ([]VolumeLocation, error) {
	if err := c.closed(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	locations, ok := c.volumes[volumeID]
	c.mu.Unlock()
	if ok {
		return slices.Clone(locations), nil
	}
	var answer lookupAnswer
	err := c.askMasters(ctx, "/dir/lookup?volumeId="+strconv.Itoa(volumeID), &answer)
	var he *HTTPError
	if errors.As(err, &he) && he.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("volume %d: %w (%w)", volumeID, ErrNotFound, he)
	}
	if err != nil {
		return nil, err
	}
	locations = make([]VolumeLocation, len(answer.Locations))
	for idx, l := range answer.Locations {
		locations[idx] = VolumeLocation{Addr: l.Url, PublicAddr: l.PublicUrl, DataCenter: l.DataCenter}
	}
	return locations, nil
}

// ParseFid checks a file id, and returns its volume id. A fid is a volume id, a comma and hex
// digits, optionally followed by an underscore and a number, like 3,01637037d6 or 3,01637037d6_1.
// Anything else is an error, so that a fid can't change the meaning of a URL.
func ParseFid(fid string) (volumeID int, err error) {
	volume, key, ok := strings.Cut(fid, ",")
	key, n, hasN := strings.Cut(key, "_")
	if !ok || !onlyOf(volume, "0123456789") || !onlyOf(key, "0123456789abcdefABCDEF") ||
		hasN && !onlyOf(n, "0123456789") {
		return 0, fmt.Errorf("seaweedstore: invalid fid %q", fid)
	}
	return strconv.Atoi(volume)
}

// onlyOf reports whether s is not empty and has only the given bytes.
func onlyOf(s, bytes string) bool {
	return s != "" && strings.Trim(s, bytes) == ""
}
