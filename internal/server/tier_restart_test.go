package server

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"reservoir/internal/commitlog"
	"reservoir/internal/store"
	"reservoir/pkg/config"
)

func tieredConfig(dir string, budget int64) *config.Config {
	cfg := config.DefaultConfig()
	cfg.CommitLogEnabled = true
	cfg.CommitLogDir = filepath.Join(dir, "log")
	cfg.TieringEnabled = true
	cfg.TieringDir = filepath.Join(dir, "values")
	cfg.TieringFileSize = 1 << 20
	cfg.MaxMemoryUsage = budget
	cfg.UIPort = ""
	cfg.ClusterEnabled = false
	return cfg
}

func startTiered(t *testing.T, cfg *config.Config) store.KVStore {
	t.Helper()
	srv, err := New(cfg)
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	if !eventually(30*time.Second, func() bool { return !srv.kvStore.IsRecoveryActive() }) {
		t.Fatal("recovery did not finish")
	}
	return srv.kvStore
}

func tierValue(i int) string { return fmt.Sprintf("%06d", i) + strings.Repeat("v", 1018) }

// TestTieredRestartKeepsColdData (add-tiered-storage, task 3.5): a node with
// tiering holds 4x its memory budget, mostly on disk; after a restart every
// key is readable and recovery spilled at the budget again instead of failing.
func TestTieredRestartKeepsColdData(t *testing.T) {
	dir := t.TempDir()
	const budget, n = 1 << 20, 4000 // ~4MB of values
	cfg := tieredConfig(dir, budget)

	st := startTiered(t, cfg)
	for i := 0; i < n; i++ {
		if err := st.Set(fmt.Sprintf("k%d", i), tierValue(i)); err != nil {
			t.Fatalf("Set k%d: %v", i, err)
		}
	}
	st.Set("k7", "rewritten") // a cold key overwritten before the restart
	if cold := st.TierStats()["cold_keys"].(int64); cold == 0 {
		t.Fatal("setup: nothing was spilled")
	}
	if err := st.(*store.LockFreeStore).GetCommitLog().(*commitlog.CommitLog).Close(); err != nil {
		t.Fatal(err)
	}
	st.Stop()

	st2 := startTiered(t, cfg)
	defer st2.Stop()
	stats := st2.TierStats()
	t.Logf("after restart: %v", stats)
	if stats["cold_keys"].(int64) == 0 {
		t.Error("recovery did not spill: the whole log was replayed into RAM")
	}
	if used := st2.GetMemoryUsage(); used > budget*11/10 {
		t.Errorf("memory after recovery %d, budget %d", used, budget)
	}
	for i := 0; i < n; i++ {
		want := tierValue(i)
		if i == 7 {
			want = "rewritten"
		}
		v, ok, err := st2.GetString(fmt.Sprintf("k%d", i))
		if err != nil || !ok || v != want {
			t.Fatalf("k%d after restart: ok=%v err=%v len=%d", i, ok, err, len(v))
		}
	}
}

// TestTieringRequiresCommitLog: value files are rebuilt from the log on every
// start, so tiering without it is refused.
func TestTieringRequiresCommitLog(t *testing.T) {
	cfg := tieredConfig(t.TempDir(), 1<<20)
	cfg.CommitLogEnabled = false
	if _, err := New(cfg); err == nil {
		t.Fatal("tiering without the commit log was accepted")
	}
}
