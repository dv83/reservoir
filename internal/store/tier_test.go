package store

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	pkgErrors "reservoir/pkg/errors"
)

func newTieredStore(t *testing.T, budget int64) *LockFreeStore {
	t.Helper()
	s := NewStore(&Limits{MaxKeySize: 1024, MaxValueSize: 1 << 20, MaxMemoryUsage: budget})
	if err := s.EnableTiering(t.TempDir(), 1<<20); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Stop)
	return s
}

func val(i, n int) string {
	return fmt.Sprintf("%08d", i) + strings.Repeat("v", n-8)
}

func coldCount(s *LockFreeStore) int {
	n := 0
	for _, shard := range s.shards {
		shard.mu.RLock()
		for _, sv := range shard.data {
			if sv.cold != nil {
				n++
			}
		}
		shard.mu.RUnlock()
	}
	return n
}

// TestTieringSpillsInsteadOfRejecting (tasks 3.3, 3.4): with tiering, writing
// five times the memory budget succeeds, RAM stays near the budget, and every
// key reads back its value. Without tiering the same writes hit the limit.
func TestTieringSpillsInsteadOfRejecting(t *testing.T) {
	const budget, n, size = 1 << 20, 5000, 1024 // 5MB of values, 1MB budget
	s := newTieredStore(t, budget)
	for i := 0; i < n; i++ {
		if err := s.Set(fmt.Sprintf("k%d", i), val(i, size)); err != nil {
			t.Fatalf("Set k%d: %v", i, err)
		}
	}
	if used := s.GetMemoryUsage(); used > budget*11/10 {
		t.Errorf("memory used %d, budget %d", used, budget)
	}
	if c := coldCount(s); c < n/2 {
		t.Errorf("only %d of %d keys are cold", c, n)
	}
	for i := 0; i < n; i++ {
		v, ok := s.Get(fmt.Sprintf("k%d", i))
		if !ok || v != val(i, size) {
			t.Fatalf("k%d read back wrong (ok=%v, len=%d)", i, ok, len(v))
		}
	}

	plain := NewStore(&Limits{MaxKeySize: 1024, MaxValueSize: 1 << 20, MaxMemoryUsage: budget})
	defer plain.Stop()
	var err error
	for i := 0; i < n && err == nil; i++ {
		err = plain.Set(fmt.Sprintf("k%d", i), val(i, size))
	}
	if err == nil {
		t.Fatal("without tiering the writes should hit the memory limit")
	}
}

// TestColdValuesAreTransparent: commands that read the value see cold values.
func TestColdValuesAreTransparent(t *testing.T) {
	s := newTieredStore(t, 1<<30)
	long := strings.Repeat("x", 200)
	s.Set("ap", long)
	s.Set("gs", long)
	s.Set("sl", long)
	s.SetLWW("lw", long, 100, 0, 1)
	s.spill(nil, 1<<30)
	if coldCount(s) != 4 {
		t.Fatalf("setup: %d cold keys, want 4", coldCount(s))
	}

	if n, err := s.Append("ap", "y"); err != nil || n != 201 {
		t.Fatalf("APPEND on a cold key = (%d, %v), want 201", n, err)
	}
	if v, _ := s.Get("ap"); v != long+"y" {
		t.Fatalf("value after APPEND is wrong")
	}
	if old, ok, err := s.GetSet("gs", "new"); err != nil || !ok || old != long {
		t.Fatalf("GETSET on a cold key returned (%d bytes, %v, %v)", len(old), ok, err)
	}
	if n, _ := s.StrLen("sl"); n != 200 {
		t.Fatalf("STRLEN of a cold key = %d, want 200", n)
	}
	if _, err := s.Incr("sl"); !errors.Is(err, pkgErrors.ErrNotInteger) {
		t.Fatalf("INCR on a cold non-integer = %v, want not-an-integer", err)
	}
	found := false
	for i := 0; i < numShards; i++ {
		for _, e := range s.ShardLWWDigest(i) {
			if e.Key == "lw" {
				found = e.Value == long
			}
		}
	}
	if !found {
		t.Fatal("anti-entropy digest of a cold key does not carry its value")
	}
}

// TestCorruptColdValueFailsRead: a cold value that fails its checksum is never
// served.
func TestCorruptColdValueFailsRead(t *testing.T) {
	dir := t.TempDir()
	s := NewStore(&Limits{MaxKeySize: 1024, MaxValueSize: 1 << 20, MaxMemoryUsage: 1 << 30})
	defer s.Stop()
	if err := s.EnableTiering(dir, 1<<20); err != nil {
		t.Fatal(err)
	}
	s.Set("k", strings.Repeat("a", 500))
	s.spill(nil, 1<<30)
	s.tier.log.Flush()
	files, _ := filepath.Glob(filepath.Join(dir, "*.vlog"))
	f, _ := os.OpenFile(files[0], os.O_RDWR, 0)
	f.WriteAt([]byte("Z"), 100)
	f.Close()

	if _, _, err := s.GetString("k"); err == nil {
		t.Fatal("GetString served a corrupt cold value")
	}
	if v, ok := s.Get("k"); ok || v != "" {
		t.Fatal("Get served a corrupt cold value")
	}
}

// TestSpillIsNotAWrite: moving a value to disk keeps its stamp and TTL and
// logs nothing.
func TestSpillIsNotAWrite(t *testing.T) {
	s := newTieredStore(t, 1<<30)
	fake := &fakeCommitLog{}
	s.SetCommitLog(fake)
	s.SetLWW("k", strings.Repeat("a", 300), 500, 3, 9)
	deadline := time.Now().Add(time.Hour)
	s.SetExpiry("k", deadline)
	stamp, _ := s.GetHLC("k")
	logged := len(fake.writes)

	s.spill(nil, 1<<30)
	if coldCount(s) != 1 {
		t.Fatal("setup: key was not spilled")
	}
	if len(fake.writes) != logged {
		t.Fatalf("spill wrote %v to the commit log", fake.writes[logged:])
	}
	if after, _ := s.GetHLC("k"); after != stamp {
		t.Fatalf("spill changed the stamp: %v -> %v", stamp, after)
	}
	if ttl, ok := s.GetTTL("k"); !ok || ttl < 59*time.Minute {
		t.Fatalf("spill changed the TTL: %v %v", ttl, ok)
	}
}

// TestKeyIndexBound (task 3.4): when the keys alone fill the budget there is
// nothing to spill, and creating more keys fails with a memory-limit error.
func TestKeyIndexBound(t *testing.T) {
	s := newTieredStore(t, 64<<10)
	var err error
	i := 0
	for ; i < 100000 && err == nil; i++ {
		err = s.Set(fmt.Sprintf("key-%d", i), "small") // too short to spill
	}
	if err == nil {
		t.Fatal("writes never hit the key index bound")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "memory") {
		t.Fatalf("error = %v, want a memory-limit error", err)
	}
	t.Logf("key index full after %d keys", i)
}

// TestTouchedValueGetsSecondChance: a value read since the last pass is
// skipped once by the spiller (CLOCK).
func TestTouchedValueGetsSecondChance(t *testing.T) {
	s := newTieredStore(t, 1<<30)
	s.Set("hot", strings.Repeat("h", 300))
	s.Set("cold", strings.Repeat("c", 300))
	s.Get("hot")
	for _, k := range []string{"hot", "cold"} {
		shard := s.getShard(k)
		shard.mu.Lock()
		s.spillShard(shard, 1<<30)
		shard.mu.Unlock()
	}
	if s.getShard("hot").data["hot"].cold != nil {
		t.Fatal("recently read value was spilled on the first pass")
	}
	if s.getShard("cold").data["cold"].cold == nil {
		t.Fatal("unread value was not spilled")
	}
}

// TestValueLogGarbageIsCollected: overwritten cold values leave dead records,
// which collection reclaims while live cold values stay readable.
func TestValueLogGarbageIsCollected(t *testing.T) {
	s := newTieredStore(t, 1<<30)
	for round := 0; round < 3; round++ {
		for i := 0; i < 2000; i++ {
			s.Set(fmt.Sprintf("k%d", i), val(i+round, 512))
		}
		s.spill(nil, 1<<30)
	}
	s.Set("extra", strings.Repeat("e", 2<<20)) // rotate the active file
	s.spill(nil, 1<<30)
	before := s.tier.log.Stats()
	reclaimed := s.collectValueLog()
	after := s.tier.log.Stats()
	t.Logf("value log before %+v, after %+v, reclaimed %d", before, after, reclaimed)
	if reclaimed == 0 || after.DiskBytes >= before.DiskBytes {
		t.Fatal("collection reclaimed nothing")
	}
	for i := 0; i < 2000; i++ {
		if v, ok := s.Get(fmt.Sprintf("k%d", i)); !ok || v != val(i+2, 512) {
			t.Fatalf("k%d wrong after collection", i)
		}
	}
}

// TestTieringConcurrent: concurrent writers, readers, spills and collection
// keep every key readable (run with -race).
func TestTieringConcurrent(t *testing.T) {
	s := newTieredStore(t, 400<<10) // index ~200KB, values 480KB: spilling required
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 1500; i++ {
				k := fmt.Sprintf("w%d-%d", w, i%300)
				if err := s.Set(k, val(i, 400)); err != nil {
					t.Errorf("Set: %v", err)
					return
				}
				if _, _, err := s.GetString(fmt.Sprintf("w%d-%d", w, (i*7)%300)); err != nil {
					t.Errorf("GetString: %v", err)
					return
				}
			}
		}(w)
	}
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				s.collectValueLog()
				time.Sleep(time.Millisecond)
			}
		}
	}()
	wg.Wait()
	close(stop)
	for w := 0; w < 4; w++ {
		for i := 1200; i < 1500; i++ {
			k := fmt.Sprintf("w%d-%d", w, i%300)
			if v, ok := s.Get(k); !ok || v != val(i, 400) {
				t.Fatalf("%s wrong at the end", k)
			}
		}
	}
}
