package seaweedstore

import (
	"errors"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

var (
	// ErrClosed is returned by the methods of a closed Cluster.
	ErrClosed = errors.New("seaweedstore: cluster closed")
	// ErrNoLeader is returned by Leader while no master is known to be the leader, for example during
	// an election.
	ErrNoLeader = errors.New("seaweedstore: no leader")
	// ErrNotFound is returned for a file or a volume that doesn't exist.
	ErrNotFound = errors.New("seaweedstore: not found")
)

// Options configure a Cluster. The zero value is valid.
type Options struct {
	// Logger is where failed status updates are logged. Nil means slog.Default().
	Logger *slog.Logger
	// StatusUpdateInterval is the time between status updates, and the limit for each. After a failed
	// update, the next one comes after a second, if that is shorter. Zero means 5 seconds.
	StatusUpdateInterval time.Duration
	// Timeout limits each request to a master, including reading the answer. Zero means 5 seconds.
	Timeout time.Duration
	// VolumeTimeout limits the wait for a volume server's response headers when reading or deleting a
	// file, after which the next copy is tried. Zero means 10 seconds.
	VolumeTimeout time.Duration
	// DataCenter is the data center to read from first. Reads and deletes try the copies in it before
	// the others, for example to read from the closest copy. Empty means no preference.
	DataCenter string
	// HTTPClient sends all requests. Nil means a client on http.DefaultTransport. Its Timeout must be
	// zero, because it would also limit reading and sending files.
	HTTPClient *http.Client
}

// AssignOptions are the parameters of Assign. Zero fields are left out, so the master uses its
// defaults. See https://github.com/seaweedfs/seaweedfs/wiki/Master-Server-API#assign-a-file-key
type AssignOptions struct {
	// Collection is the collection of the file. Each volume belongs to one collection.
	Collection string
	// Replication is the replica placement for new volumes, e.g. "100" for one more copy in another
	// data center. The default is the master's -defaultReplication.
	Replication string
	// TTL is how long the file is kept, e.g. "3m" for 3 minutes. Units: m minute, h hour, d day,
	// w week, M month, y year. The default is forever.
	TTL string
	// DataCenter, Rack and DataNode (a volume server, host:port) prefer a place for the file.
	DataCenter string
	Rack       string
	DataNode   string
	// Disk is the disk type, if the volume servers have labelled disks.
	Disk string
	// Count is how many file ids to assign: the fid of the result, and <fid>_1 to <fid>_<Count-1>.
	Count int
	// WritableVolumeCount is how many volumes to create if no volume matches.
	WritableVolumeCount int
	// Preallocate is how many bytes to preallocate for new volumes.
	Preallocate int64
	// MemoryMapMaxSizeMb makes new volumes memory mapped files of this size. Windows only.
	MemoryMapMaxSizeMb int
}

// query returns the options as a URL query.
func (o AssignOptions) query() string {
	v := url.Values{}
	for key, value := range map[string]string{
		"collection": o.Collection, "replication": o.Replication, "ttl": o.TTL,
		"dataCenter": o.DataCenter, "rack": o.Rack, "dataNode": o.DataNode, "disk": o.Disk,
	} {
		if value != "" {
			v.Set(key, value)
		}
	}
	for key, value := range map[string]int64{
		"count": int64(o.Count), "writableVolumeCount": int64(o.WritableVolumeCount),
		"preallocate": o.Preallocate, "memoryMapMaxSizeMb": int64(o.MemoryMapMaxSizeMb),
	} {
		if value != 0 {
			v.Set(key, strconv.FormatInt(value, 10))
		}
	}
	return v.Encode()
}

// AssignResult is a file id, and the volume server to upload the file to.
type AssignResult struct {
	Fid        string // 3,01637037d6
	Addr       string // the volume server, host:port
	PublicAddr string // the volume server's public address, host:port
	Count      int    // how many fids are assigned, see AssignOptions.Count
}

// UploadResult is what the volume server answered to an upload.
type UploadResult struct {
	Name        string // the file name; without a path, see Upload
	Size        int64  // the size of the file, also when SeaweedFS stores it compressed
	ETag        string // without quotes
	ContentType string // empty if the upload had none
}

// FileInfo describes a stored file.
type FileInfo struct {
	Name         string    // the name it was uploaded with, or the fid
	Size         int64     // the size of the file, also when SeaweedFS stores it compressed
	ContentType  string    // as uploaded; SeaweedFS guesses one for none or application/octet-stream
	ETag         string    // without quotes, like UploadResult.ETag
	LastModified time.Time // when it was uploaded
}

// VolumeLocation is a volume server that has a copy of a volume.
type VolumeLocation struct {
	Addr       string // host:port, where the volume server is reached
	PublicAddr string // the public address; the same as Addr in the locations from status updates
	DataCenter string // the volume server's -dataCenter
}

// URL returns the URL of path on the volume server.
func (v *VolumeLocation) URL(path string) string {
	return "http://" + v.Addr + path
}
