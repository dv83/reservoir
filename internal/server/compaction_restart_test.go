package server

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"

	"reservoir/internal/commitlog"
	"reservoir/internal/store"
	"reservoir/pkg/config"
)

// logWrites runs writes against a real store backed by a real commit log in dir,
// then closes the log so every entry is flushed to its segment.
func logWrites(t *testing.T, dir string, writes func(st store.KVStore)) {
	t.Helper()
	cl, err := commitlog.NewCommitLog(dir)
	if err != nil {
		t.Fatalf("NewCommitLog: %v", err)
	}
	st := store.NewStore(&store.Limits{MaxKeySize: 1024, MaxValueSize: 1 << 20, MaxMemoryUsage: 1 << 30})
	st.SetCommitLog(cl)
	writes(st)
	if err := cl.Close(); err != nil {
		t.Fatalf("close commit log: %v", err)
	}
	st.Stop()
}

// TestRestartAfterCompactionKeepsState (task 3.2): a node whose commit log has
// been compacted must recover every key on restart — a value with a TTL, every
// member of a set built by several SADDs and every field of a hash built by
// several HSETs. Before the fix compaction kept only the last entry per key, so
// the value behind an EXPIRE and all but the last member/field were lost.
func TestRestartAfterCompactionKeepsState(t *testing.T) {
	dir := t.TempDir()
	deadline := time.Now().Add(time.Hour)

	// Segment 1: many superseded SETs (so compaction is worthwhile), then a
	// value with a TTL and the first set member and hash field.
	logWrites(t, dir, func(st store.KVStore) {
		for i := 0; i < 20; i++ {
			st.Set("k", fmt.Sprintf("old-%d", i))
		}
		st.Set("k", "v")
		st.SetExpiry("k", deadline)
		st.SAddLWW("s", 100, 0, 1, "a")
		st.HSetLWW("h", 100, 0, 1, "f1", "v1")
	})

	// Segment 2, written later: the second member and field. Compaction only
	// rewrites closed segments, at least two of them, so produce it in its own
	// directory and move it in as the next segment.
	other := t.TempDir()
	logWrites(t, other, func(st store.KVStore) {
		st.SAddLWW("s", 200, 0, 1, "b")
		st.HSetLWW("h", 200, 0, 1, "f2", "v2")
		st.Set("other", "x")
	})
	seg := func(d string, id int) string {
		return filepath.Join(d, fmt.Sprintf("%016d%s", id, commitlog.SegmentFileExtension))
	}
	if err := os.Rename(seg(other, 1), seg(dir, 2)); err != nil {
		t.Fatalf("move segment: %v", err)
	}

	// Compact the two closed segments (segment 3 would be the live one).
	if err := commitlog.NewCompactor(dir).Compact(3); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if segs, _ := filepath.Glob(filepath.Join(dir, "*"+commitlog.SegmentFileExtension)); len(segs) != 1 {
		t.Fatalf("expected compaction to leave 1 segment, found %v", segs)
	}

	// Restart: a real server recovers from the compacted log.
	cfg := config.DefaultConfig()
	cfg.CommitLogEnabled = true
	cfg.CommitLogDir = dir
	cfg.UIPort = ""
	cfg.ClusterEnabled = false
	srv, err := New(cfg)
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	defer srv.kvStore.Stop()

	if !eventually(10*time.Second, func() bool { return !srv.kvStore.IsRecoveryActive() }) {
		t.Fatal("recovery did not finish")
	}
	st := srv.kvStore

	if v, ok := st.Get("k"); !ok || v != "v" {
		t.Errorf("k = (%q, %v) after restart, want (\"v\", true): value behind EXPIRE lost", v, ok)
	}
	if _, ok := st.GetTTL("k"); !ok {
		t.Errorf("k lost its TTL after restart")
	}
	members, _ := st.SMembers("s")
	sort.Strings(members)
	if !reflect.DeepEqual(members, []string{"a", "b"}) {
		t.Errorf("s = %v after restart, want [a b]", members)
	}
	if f1, ok, _ := st.HGet("h", "f1"); !ok || f1 != "v1" {
		t.Errorf("h.f1 = (%q, %v) after restart, want (\"v1\", true)", f1, ok)
	}
	if f2, ok, _ := st.HGet("h", "f2"); !ok || f2 != "v2" {
		t.Errorf("h.f2 = (%q, %v) after restart, want (\"v2\", true)", f2, ok)
	}
	if v, _ := st.Get("other"); v != "x" {
		t.Errorf("other = %q after restart, want x", v)
	}
}
