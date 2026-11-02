# seaweedstore

seaweedstore is a Go client library for the HTTP API of [SeaweedFS](https://github.com/seaweedfs/seaweedfs) masters
and volume servers. It finds the leader master and keeps track of which volume servers have each volume in the
background. It assigns file ids, and uploads, reads and deletes files, also while a master or a volume server is
down.

```sh
go get github.com/nagylzs/seaweedstore
```

Requirements: Go 1.25 or later. It has no dependencies, and it is tested with SeaweedFS 4.50.

seaweedstore doesn't use the filer. It is for programs that keep the file id (fid) of each file themselves, for
example in a database row. If you need directories and paths, use the SeaweedFS filer or its S3 API.

## How it works

* `NewCluster` starts updating the cluster's status in the background: every `StatusUpdateInterval` (defaults to
  5 seconds), and a second after a failed update, until you call `Close()`. It asks every master for its status
  at the same time, and then asks the one that says it is the leader for the volume status, which lists every
  volume server and the volumes on it.
* Reads find the volume servers that have a file in the volume status of the last update. When an update fails,
  for example while the masters elect a new leader, the cluster keeps what it knew, so files can still be read. A
  volume that it doesn't know, because it was created since the last update, is looked up at the leader.
* `Assign` and lookups go to the leader. When the cluster can't reach the leader, or there is none, they go to the
  other masters, which forward them to the leader. While a master answers HTTP 503, for example while it creates
  volumes, `Assign` asks again after the `Retry-After` of the answer, until its context is done.
* `Get`, `Stat`, `Delete` and `ServeFile` try the volume servers that have a copy of the file in random order,
  those in `Options.DataCenter` first. When one can't be reached, or answers 404 or an HTTP 5xx status, they go on
  to the next one.
* A master or a volume server that a request couldn't reach is tried last for the next 30 seconds, so that only
  one request waits for a server that is down.
* A file larger than `Options.ChunkSize` (defaults to 32 MB) is uploaded in chunks, each to a fid of its own,
  and a manifest of the chunks to the file's fid. The library reads the chunks itself, each like a file, so a
  large file is read from the local data center and survives a volume server that is down like a small one.

## How to use

```go
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"github.com/nagylzs/seaweedstore"
)

func main() {
	c, err := seaweedstore.NewCluster(seaweedstore.Options{}, "master1:9333", "master2:9333", "master3:9333")
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// Wait until the leader has sent the volume status
	if err := c.WaitUntilReady(ctx); err != nil {
		log.Fatal(err)
	}

	// Get a file id. AssignOptions has the collection, the replication,
	// the TTL and the other parameters of /dir/assign.
	ar, err := c.Assign(ctx, seaweedstore.AssignOptions{})
	if err != nil {
		log.Fatal(err)
	}
	// Upload the file to it. Keep ar.Fid, for example in your database.
	data := "hello world"
	ur, err := c.Upload(ctx, ar, strings.NewReader(data), int64(len(data)), "hello.txt", "text/plain")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("stored", ur.Name, "as", ar.Fid)

	// Read it from a volume server that has a copy
	res, err := c.Get(ctx, ar.Fid, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(body))
}
```

### Options

| Field                  | Default | Meaning                                                                          |
|------------------------|---------|----------------------------------------------------------------------------------|
| `StatusUpdateInterval` | 5 s     | The time between status updates, and the limit for each. After a failed update, the next one comes after a second, if that is shorter. |
| `Timeout`              | 5 s     | The limit for each request to a master, including reading the answer.            |
| `VolumeTimeout`        | 10 s    | The limit for the wait for a volume server's response headers when reading or deleting a file. After it, the next copy is tried. |
| `ChunkSize`            | 32 MB   | Files larger than this are uploaded in chunks of this size (see Uploads). It must not be larger than the volume servers' `-fileSizeLimitMB`. Negative means never. |
| `DataCenter`           | ""      | The data center to read from first, for example the one closest to your program. Its copies are tried before the others. |
| `HTTPClient`           | nil     | The `*http.Client` that sends all requests, for TLS settings, proxies or connection limits. nil means a client on `http.DefaultTransport`. Its `Timeout` must be zero. |
| `Logger`               | nil     | The `*slog.Logger` that failed status updates are logged to. nil means `slog.Default()`. |

A zero duration or `ChunkSize` means the default.

### Cluster

* `NewCluster(opts, masters...)` - a cluster of the given masters (`host:port`). It starts the status updates in
  the background, and returns an error if there are no masters or an address is not `host:port`.
* `WaitUntilReady(ctx)` - wait for the first complete status update: there is a leader, and it has sent the volume
  status. If `ctx` is done first, it returns `ctx.Err()`, with the error of the last status update.
* `Close()` - stop the status updates, cancelling one in progress, and wait until they have stopped. Afterwards
  the methods return `ErrClosed`.
* `Assign(ctx, opts)` - get a file id, and the volume server to upload the file to. With `AssignOptions.Count` n,
  the fids `<fid>_1` to `<fid>_<n-1>` are yours too.
* `Upload(ctx, assignResult, body, size, name, contentType)` - upload a file to the fid of an `AssignResult`, and
  return what the volume server stored.
* `Get(ctx, fid, header)` - read a file. The header goes with the request, for example `Range` or
  `If-None-Match`. It returns the response for success and redirect statuses, also 206 and 304; you must close its
  body.
* `Stat(ctx, fid)` - the name, size, content type, ETag and modification time of a file.
* `Delete(ctx, fid)` - delete a file. The volume server deletes the other copies too.
* `ServeFile(w, r, fid)` - serve a file to an HTTP client (see below).
* `LookupVolume(ctx, volumeID)` - the volume servers that have a copy of a volume.
* `Leader()`, `Masters()`, `StatusTime()` - what the last status update found.

All methods are safe for concurrent use.

### Serving files over HTTP

`ServeFile` answers a GET or HEAD request with a file, for example in a handler that finds the fid of the file in
a database:

```go
mux.HandleFunc("GET /files/{id}", func(w http.ResponseWriter, r *http.Request) {
	fid := fidOf(r.PathValue("id")) // your lookup
	w.Header().Set("Cache-Control", "max-age=86400")
	err := c.ServeFile(w, r, fid)
	if errors.Is(err, seaweedstore.ErrNotFound) {
		http.Error(w, "no such file", http.StatusNotFound)
	} else if err != nil {
		http.Error(w, "the file server is not available", http.StatusBadGateway)
	}
})
```

* It forwards the client's `Range`, `If-Range`, `If-None-Match`, `If-Modified-Since` and `Accept-Encoding` to a
  volume server with a copy, and answers with its status (200, 206, 304, 416, ...), body and the headers that
  describe the body. It adds `Vary: Accept-Encoding`, which SeaweedFS doesn't send.
* SeaweedFS stores text compressed: a 140 KB text file was sent in 843 bytes to a client that accepts gzip.
  Others get the file uncompressed, with its `Content-Length`. A range is always of the uncompressed file,
  because SeaweedFS would answer a range request with gzip with a range of the compressed file, under the same
  ETag.
* Headers that you set before are kept, like `Cache-Control` above, or a `Content-Disposition` with another name.
  `Content-Length`, `Content-Range`, `Content-Encoding`, `ETag`, `Last-Modified` and `Accept-Ranges` always come
  from the volume server.
* When it returns an error, it has written nothing, so you can answer it, or try another cluster, for example
  one that has backup copies. Once it has started the answer, it returns nil, even if the client went away.
* A request with several ranges gets the whole file, as RFC 9110 allows.

### Uploads

`Upload` sends `size` as the length of the body, and the body must give exactly that many bytes. A negative `size`
means that the size is not known. The volume server reports how many bytes it stored, and `Upload` fails if that
is not `size`.

With `AssignOptions.TTL`, for example `"7d"`, the fid is on a volume for files with that TTL, and `Upload` sends
the TTL with the file too. After it, `Get`, `Stat` and `ServeFile` give `ErrNotFound`. (SeaweedFS 4.50 gives a
file on such a volume the volume's TTL anyway.) The TTL counts from the upload, so a file in chunks expires chunk
by chunk: for about as long as its upload took, the manifest is still there, but reading its first chunks fails.

#### Large files

A file larger than `Options.ChunkSize` (32 MB by default) is uploaded in chunks. Each chunk goes to a fid of its
own, which `Upload` assigns with the `AssignOptions` of the file's fid, and then a SeaweedFS chunk manifest, which
lists the chunks, goes to the file's fid. You keep only that fid; `UploadResult.Chunks` has the fids of the chunks,
for example to tell them from orphans when you compare the fids in your database with those on the volume
servers:

* `Get`, `Stat`, `ServeFile` and `Delete` handle it like any file. The library reads the chunks itself, each from
  a copy in `Options.DataCenter` first, and from another copy when one can't be reached, so only the chunks that a
  range needs are read.
* Other SeaweedFS clients read the fid like any file: the volume server joins the chunks for them. Up to
  SeaweedFS 4.51, it picks a copy of each chunk at random, and doesn't try another: after a volume server with a
  copy was killed, most such reads failed for up to 10 minutes
  ([seaweedfs#12308](https://github.com/seaweedfs/seaweedfs/issues/12308), fixed on master after 4.51).
* Deleting the file deletes its chunks too.
* With an unknown size, `Upload` holds up to a chunk in memory, to find out whether the file needs chunks.
  Otherwise it streams the body.
* When an upload in chunks fails, `Upload` deletes the chunks it has uploaded. If the program crashes in the
  middle of it, the chunks are left, and nothing refers to them.
* A file in chunks is sent uncompressed, also to clients that accept gzip.
* A chunk manifest that another tool stored, for example `weed upload -maxMB`, is read by the volume server for a
  range and for `Stat`. A GET request without a range gets the manifest as it is stored.

SeaweedFS has some other surprises with uploads, which were checked with SeaweedFS 4.50:

* Of a file name with a path, it keeps only the part after the last `/` or `\`, so `C:\docs\a.txt` becomes
  `a.txt`. Other characters, including quotes and UTF-8, are kept. Responses to `Get` have the name in their
  `Content-Disposition` header.
* With the content type `application/x-www-form-urlencoded`, the volume server stores an empty file, and still
  answers that the upload succeeded. `Upload` notices that from the size.
* For a file uploaded without a content type, or as `application/octet-stream`, the volume server guesses one
  when it sends the file, for example `text/plain; charset=utf-8` for text.
* The volume servers accept files up to their `-fileSizeLimitMB` (256 MB by default), and hold each upload in
  memory. The limit applies to each chunk of a large file.

### Timeouts

* `Options.Timeout` limits each request to a master, including reading its answer.
* `Options.VolumeTimeout` limits only the wait for a volume server's response headers when reading or deleting.
  Reading the body is limited only by `ctx`, so a large file can take as long as it needs. A volume server that
  doesn't answer in time gives an error for which `errors.Is(err, os.ErrDeadlineExceeded)` is true, and the next
  copy is tried.
* Uploads are limited only by `ctx`: the volume server answers after it has stored all copies, which takes longer
  for larger files.

### Errors

* `ErrNotFound` - the file or the volume doesn't exist: the master doesn't know the volume, or every copy answered
  404.
* `ErrNoLeader` - `Leader()` while no master is known to be the leader, for example during an election.
* `ErrClosed` - the cluster is closed.
* `*HTTPError` - a master or a volume server answered with an unexpected status. It has the status code, the
  reason that SeaweedFS gave in its answer, and the `Retry-After` of the answer.
* When no master or no volume server with a copy can be reached, the error has the error of each one.

`Assign` returns an HTTP 406 error when SeaweedFS can't create a volume, for example because the volume servers
are full. SeaweedFS also answers 406 for a while after a leader election (see below), so whether to retry it is
up to you.

### File ids

A fid is a volume id, a comma and hex digits, optionally followed by an underscore and a number, like
`3,01637037d6` or `3,01637037d6_1`. The methods return an error for anything else, so that a fid can't change the
meaning of the URL it goes into. A fid with a file extension, like `3,01637037d6.jpg`, is an error too, although
SeaweedFS accepts it. `ParseFid(fid)` checks a fid and returns its volume id.

## When servers fail

These were measured in the test cluster (see Testing) with SeaweedFS 4.50, three masters with the default
`-electionTimeout`, and replication `100`.

* **The leader master stops.** The other masters elect a new leader in 10 to 18 seconds. Files can be read all the
  time. `Assign` fails until the election, because the other masters wait for the stopped leader. The new leader
  first answers 503 (which `Assign` retries), then 406 until the volume servers have found it, and assigned the
  first fid 16 to 50 seconds after the election (16 on the CI runner, 30 to 50 on a laptop).
* **A volume server stops.** Its files are read from their other copies. The first read that tries the stopped
  server waits up to `VolumeTimeout`; the reads in the next 30 seconds try it last. The master forgets the server
  within a second, and the next status update drops it.

## Caveats

* Your program must be able to reach the volume servers at the address they register with the masters (their
  `-ip` and `-port`). `PublicAddr` is not used, and in the locations from the volume status it is the same as
  `Addr`, because the volume status doesn't have it.
* Only plain HTTP is supported, without TLS and without JWT authentication.
* Every status update fetches the volume status, which lists every volume of the cluster: about 300 bytes for each
  copy of a volume, uncompressed. With 10,000 volumes in two copies, that is 6 MB at every update, so a large
  cluster needs a longer `StatusUpdateInterval`.

## Testing

`go test ./...` runs the unit tests, which need no SeaweedFS. `testenv/` has a SeaweedFS 4.50 cluster in docker,
with three masters and four volume servers in two data centers, and integration tests that use it (on Linux; the
script needs curl and jq):

```sh
# Run the integration tests. They start the cluster if needed.
go test -tags integration -v ./testenv
# List the commands that control the cluster
testenv/cluster.sh
```

## License

MIT, see [LICENSE](LICENSE).
