# Tasks

## 1. Reproduce

- [x] 1.1 Add a compaction test where SET then EXPIRE on one key must recover the value with its TTL
- [x] 1.2 Add a compaction test where several SADDs and HSETs on one key must recover every member and field
- [x] 1.3 Add a property test: for random op sequences, replaying the original segments and the compacted segments yields identical state

## 2. Fix

- [x] 2.1 Classify logged operations into reset and incremental (unknown ops count as incremental)
- [x] 2.2 Retain per key every entry since its last reset; drop keys whose last reset is DEL
- [x] 2.3 Handle FLUSHALL as a global reset
- [x] 2.4 Rewrite retained entries per key in timestamp order, preserving timestamps

## 3. Verify

- [x] 3.1 Run the commitlog tests and `go test ./... -race`
- [x] 3.2 Restart a node with compacted segments and check SET+EXPIRE, set and hash keys survive
