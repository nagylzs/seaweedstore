# TODO

Planned changes. Remove an item when it's done.

## Before v0.1.0

### 1. Tag v0.1.0

## Later

* TLS and JWT (SeaweedFS's `security.toml`). Only plain HTTP is supported.
* The real `PublicAddr` of cached locations. `/vol/status` doesn't have it, so it is the same as `Addr`; only
  lookups at the master have the real one.
* Smaller status updates. Each one fetches the whole `/vol/status`, about 300 bytes for each copy of a volume.
* Tests with erasure-coded volumes, a network partition, a killed master, and all volume servers
  restarting.
* Run staticcheck in CI.
