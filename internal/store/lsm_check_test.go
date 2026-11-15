package store

import (
	"fmt"
	"os"
	"reflect"
	"sort"
	"testing"

	"reservoir/internal/lsmengine"
)

// LSM checker (add-lsm-storage, task 2.2). With RESERVOIR_TEST_LSM=1 every
// store made by createTestStore runs in LSM mode, and after every persisted
// write-unlock the whole shard is compared with the engine: the same keys, and
// each engine value decoding to the cached value's state. Any write path that
// changes a key without persisting it — an in-place mutation the dirty
// tracking missed, a map reset, a delete outside the helpers — shows up at the
// next write to that shard, in whichever test triggers it.
//
//	RESERVOIR_TEST_LSM=1 go test ./internal/store/

var lsmTestDir string

func TestMain(m *testing.M) {
	if os.Getenv("RESERVOIR_TEST_LSM") != "" {
		d, err := os.MkdirTemp("", "reservoir-lsm-test-")
		if err != nil {
			panic(err)
		}
		lsmTestDir = d
	}
	code := m.Run()
	if lsmTestDir != "" {
		os.RemoveAll(lsmTestDir)
	}
	os.Exit(code)
}

func enableTestLSM(s *LockFreeStore) {
	dir, err := os.MkdirTemp(lsmTestDir, "store-")
	if err != nil {
		panic(err)
	}
	if err := s.EnableLSM(dir, lsmengine.Options{CacheBytes: 1 << 20}); err != nil {
		panic(err)
	}
	s.lsmCheck = checkShardAgainstEngine
}

// checkShardAgainstEngine panics with the first difference between a shard's
// cache and the engine. The caller holds the shard write lock.
func checkShardAgainstEngine(sh *LockFreeShard) {
	s := sh.owner
	onDisk := map[string][]byte{}
	if err := s.engine.ScanPrefix([]byte{lsmKindValue, sh.idx}, func(k, v []byte) bool {
		onDisk[string(k[2:])] = append([]byte(nil), v...)
		return true
	}); err != nil {
		panic(err)
	}
	var missing, extra []string
	for k := range sh.data {
		if _, ok := onDisk[k]; !ok {
			missing = append(missing, k)
		}
	}
	for k := range onDisk {
		if _, ok := sh.data[k]; !ok {
			extra = append(extra, k)
		}
	}
	if len(missing)+len(extra) > 0 {
		sort.Strings(missing)
		sort.Strings(extra)
		panic(fmt.Sprintf("LSM check, shard %d: cached but not persisted %q; persisted but not cached %q", sh.idx, missing, extra))
	}
	for k, v := range sh.data {
		dec, err := decodeValue(onDisk[k])
		if err != nil {
			panic(fmt.Sprintf("LSM check: %q does not decode: %v", k, err))
		}
		if want, got := normalize(v), normalize(dec); !reflect.DeepEqual(want, got) {
			panic(fmt.Sprintf("LSM check: %q differs between cache and engine:\ncache  %v\nengine %v", k, want, got))
		}
	}
}

// TestLSMWriteThroughSmoke: the checker itself runs, and a restart-free store
// in LSM mode serves what it wrote.
func TestLSMWriteThroughSmoke(t *testing.T) {
	s := NewLockFreeStore(&Limits{MaxKeySize: 1024, MaxValueSize: 1 << 20, MaxMemoryUsage: 1 << 30})
	if err := s.EnableLSM(t.TempDir(), lsmengine.Options{}); err != nil {
		t.Fatal(err)
	}
	defer s.Stop()
	checks := 0
	s.lsmCheck = func(sh *LockFreeShard) { checks++; checkShardAgainstEngine(sh) }
	s.Set("a", "1")
	s.SAdd("s", "x", "y")
	s.HSet("h", "f", "v")
	s.RPush("l", "1", "2")
	s.Delete("a")
	if checks == 0 {
		t.Fatal("the checker never ran")
	}
	if v, _ := s.SMembers("s"); len(v) != 2 {
		t.Fatalf("SMEMBERS = %v", v)
	}
}
