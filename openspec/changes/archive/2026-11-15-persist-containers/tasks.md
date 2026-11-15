## 1. Reproduce

- [x] 1.1 Restart test (standalone): sets, hashes and lists written by every write command, including SPOP, HINCRBY, LPOP, LSET, LREM, LTRIM and SDIFFSTORE, are recovered exactly
- [x] 1.2 Restart test (cluster-mode store): a list written through the CRDT path is recovered with the same element ids

## 2. Fix

- [x] 2.1 Log plain set and hash writes on standalone nodes
- [x] 2.2 Log standalone list writes and replay them in recovery
- [x] 2.3 Log cluster list deltas as LDELTA and replay them with ApplyListDelta
- [x] 2.4 Log SDIFFSTORE as DEL plus SADD

## 3. Verify

- [x] 3.1 Run `go test ./... -race` and confirm the restart tests fail without the fix
