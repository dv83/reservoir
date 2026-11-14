# Tasks

## 1. Reproduce

- [x] 1.1 Store tests: SET EX, SET KEEPTTL, APPEND and INCR on a key with a TTL log the value followed by an EXPIRE with the key's deadline; APPEND and GETSET are logged
- [x] 1.2 Restart test: a real node recovers the value and TTL of keys written by SET EX, SET KEEPTTL, APPEND, INCR and GETSET

## 2. Fix

- [x] 2.1 Add logStringWrite (value entry plus EXPIRE when the key keeps a TTL, under the expiry read lock)
- [x] 2.2 Use it in Set, SetWithOptions, SetWithHLC, Append, GetSet, IncrBy and the PN-counter path
- [x] 2.3 GETSET discards the TTL
- [x] 2.4 Give commit-log entries strictly increasing timestamps, with a test

## 3. Verify

- [x] 3.1 Run `go test ./... -race` and confirm the restart test fails without the fix
