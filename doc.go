// Package seaweedstore is a client for the HTTP API of SeaweedFS masters and volume servers.
//
// A Cluster keeps track of the leader master and of the volume servers that have each volume, with
// status updates in the background. It assigns file ids, and uploads, reads, serves and deletes
// files, also while a master or a volume server is down:
//
//	c, err := seaweedstore.NewCluster(seaweedstore.Options{}, "master1:9333", "master2:9333")
//	...
//	err = c.WaitUntilReady(ctx)
//	ar, err := c.Assign(ctx, seaweedstore.AssignOptions{})
//	ur, err := c.Upload(ctx, ar, body, size, "photo.jpg", "image/jpeg")
//	// keep ar.Fid, for example in a database, and later:
//	res, err := c.Get(ctx, fid, nil)
//
// It doesn't use the filer: the program keeps the file id (fid) of each file itself. The README at
// https://github.com/nagylzs/seaweedstore describes how the cluster behaves when servers fail, and
// what SeaweedFS does that may surprise you.
package seaweedstore
