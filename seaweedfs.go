package seaweedstore

// The answers of SeaweedFS servers, with the fields the library uses.

// clusterStatus is the answer of /cluster/status. A follower leaves out IsLeader.
// https://github.com/seaweedfs/seaweedfs/wiki/Master-Server-API#check-system-status
type clusterStatus struct {
	IsLeader bool   `json:"IsLeader"`
	Leader   string `json:"Leader"` // ip:port.grpcport
}

// volumeStatus is the answer of /vol/status: the volumes of every volume server, by data center and
// rack.
// https://github.com/seaweedfs/seaweedfs/wiki/Master-Server-API#check-volume-status
type volumeStatus struct {
	Volumes struct {
		// data center -> rack -> volume server (ip:port) -> volumes
		DataCenters map[string]map[string]map[string][]struct {
			Id int `json:"Id"`
		} `json:"DataCenters"`
	} `json:"Volumes"`
}

// lookupAnswer is the answer of /dir/lookup. It also has the volume id, as the string
// volumeOrFileId.
// https://github.com/seaweedfs/seaweedfs/wiki/Master-Server-API#lookup-volume
type lookupAnswer struct {
	Locations []struct {
		Url        string `json:"url"`
		PublicUrl  string `json:"publicUrl"`
		DataCenter string `json:"dataCenter"`
	} `json:"locations"`
}

// assignAnswer is the answer of /dir/assign.
type assignAnswer struct {
	Fid       string `json:"fid"`
	Url       string `json:"url"`
	PublicUrl string `json:"publicUrl"`
	Count     int    `json:"count"`
}

// uploadAnswer is the answer of a volume server to an upload. It leaves out size when it is 0.
type uploadAnswer struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
	ETag string `json:"eTag"`
	Mime string `json:"mime"`
}
