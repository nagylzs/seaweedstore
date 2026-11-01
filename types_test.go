package seaweedstore

import (
	"encoding/json"
	"testing"
)

func TestAssignOptionsQuery(t *testing.T) {
	tests := []struct {
		name string
		opts AssignOptions
		want string
	}{
		{"empty", AssignOptions{}, ""},
		{
			"all",
			AssignOptions{
				Collection: "photos", Replication: "100", TTL: "3m", DataCenter: "dc1", Rack: "rack1",
				DataNode: "172.29.0.21:8080", Disk: "ssd", Count: 3, WritableVolumeCount: 2,
				Preallocate: 1024, MemoryMapMaxSizeMb: 16,
			},
			"collection=photos&count=3&dataCenter=dc1&dataNode=172.29.0.21%3A8080&disk=ssd" +
				"&memoryMapMaxSizeMb=16&preallocate=1024&rack=rack1&replication=100&ttl=3m&writableVolumeCount=2",
		},
		{"escaped", AssignOptions{Collection: "a b&c=d"}, "collection=a+b%26c%3Dd"},
		{"a collection called 0", AssignOptions{Collection: "0"}, "collection=0"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.opts.query(); got != tt.want {
				t.Errorf("query() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestURLs(t *testing.T) {
	m := master{addr: "172.29.0.11:9333"}
	if got, want := m.url("/cluster/status"), "http://172.29.0.11:9333/cluster/status"; got != want {
		t.Errorf("master.url = %q, want %q", got, want)
	}
	v := VolumeLocation{Addr: "172.29.0.21:8080", PublicAddr: "vol1.example.com:8080"}
	if got, want := v.URL("/3,01637037d6"), "http://172.29.0.21:8080/3,01637037d6"; got != want {
		t.Errorf("VolumeLocation.URL = %q, want %q", got, want)
	}
}

// The answers of SeaweedFS 4.50 decode into the types in seaweedfs.go.
func TestDecodeAnswers(t *testing.T) {
	decode := func(s string, v any) {
		t.Helper()
		if err := json.Unmarshal([]byte(s), v); err != nil {
			t.Fatalf("cannot decode %s: %v", s, err)
		}
	}

	var leader, follower clusterStatus
	decode(leaderStatus, &leader)
	decode(followerStatus, &follower)
	if !leader.IsLeader || follower.IsLeader || leader.Leader != "172.30.0.11:9333.19333" {
		t.Errorf("leader %+v, follower %+v", leader, follower)
	}

	var vs volumeStatus
	decode(volStatus(layout{"dc1": {"rack1": {"172.29.0.21:8080": {4, 5}}}}), &vs)
	volumes := vs.Volumes.DataCenters["dc1"]["rack1"]["172.29.0.21:8080"]
	if len(volumes) != 2 || volumes[0].Id != 4 || volumes[1].Id != 5 {
		t.Errorf("volumes of dc1/rack1/172.29.0.21:8080: %+v", vs.Volumes.DataCenters)
	}

	var lr lookupAnswer
	decode(`{"volumeOrFileId":"5","locations":[{"url":"172.17.0.2:8080","publicUrl":"vol.example.com:8080","dataCenter":"dc1","grpcPort":18080}]}`, &lr)
	if len(lr.Locations) != 1 || lr.Locations[0].Url != "172.17.0.2:8080" ||
		lr.Locations[0].PublicUrl != "vol.example.com:8080" || lr.Locations[0].DataCenter != "dc1" {
		t.Errorf("lookup answer: %+v", lr)
	}

	var ar assignAnswer
	decode(`{"fid":"5,01694433fe","url":"172.17.0.2:8080","publicUrl":"vol.example.com:8080","count":2}`, &ar)
	if ar != (assignAnswer{Fid: "5,01694433fe", Url: "172.17.0.2:8080", PublicUrl: "vol.example.com:8080", Count: 2}) {
		t.Errorf("assign answer: %+v", ar)
	}

	var ur uploadAnswer
	decode(uploadCreated, &ur)
	if ur != (uploadAnswer{Name: "hello.txt", Size: 11, ETag: "c99465aa", Mime: "text/plain"}) {
		t.Errorf("upload answer: %+v", ur)
	}
}
