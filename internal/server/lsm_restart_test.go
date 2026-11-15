package server

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"reservoir/internal/store"
	"reservoir/pkg/config"
)

func lsmConfig(dir string, maxMemory int64) *config.Config {
	cfg := config.DefaultConfig()
	cfg.Storage = "lsm"
	cfg.LSMDir = filepath.Join(dir, "lsm")
	cfg.MaxMemoryUsage = maxMemory
	cfg.CommitLogEnabled = false
	cfg.UIPort = ""
	cfg.ClusterEnabled = false
	return cfg
}

func startLSM(t *testing.T, cfg *config.Config) store.KVStore {
	t.Helper()
	srv, err := New(cfg)
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	return srv.kvStore
}

// TestLSMNodeRestart (add-lsm-storage, task 4.1): a node with --storage=lsm and
// a key cache far smaller than its data restarts without replaying a log and
// serves every key with its value, TTL and deletion.
func TestLSMNodeRestart(t *testing.T) {
	dir := t.TempDir()
	cfg := lsmConfig(dir, 2<<20) // 1MB key cache, 512KB block cache
	st := startLSM(t, cfg)
	const n = 20000
	for i := 0; i < n; i++ {
		if err := st.Set(fmt.Sprintf("k%d", i), fmt.Sprintf("value-%d-padding-padding", i)); err != nil {
			t.Fatal(err)
		}
	}
	st.SAdd("set", "a", "b")
	st.HSet("hash", "f", "v")
	st.RPush("list", "x", "y", "z")
	st.Incr("ctr")
	st.Incr("ctr")
	st.SetExpiry("k1", time.Now().Add(time.Hour))
	st.Delete("k2")
	st.Stop()

	st2 := startLSM(t, cfg)
	defer st2.Stop()
	if got := st2.GetKeyCount(); got != n-1+4 {
		t.Errorf("key count after restart = %d, want %d", got, n-1+4)
	}
	for i := 0; i < n; i++ {
		v, ok := st2.Get(fmt.Sprintf("k%d", i))
		if i == 2 {
			if ok {
				t.Fatal("deleted key k2 is back after restart")
			}
			continue
		}
		if !ok || v != fmt.Sprintf("value-%d-padding-padding", i) {
			t.Fatalf("k%d after restart = (%q, %v)", i, v, ok)
		}
	}
	if ttl, ok := st2.GetTTL("k1"); !ok || ttl < 59*time.Minute {
		t.Errorf("k1 lost its TTL: %v %v", ttl, ok)
	}
	if c, _ := st2.SCard("set"); c != 2 {
		t.Errorf("set card %d", c)
	}
	if v, _, _ := st2.HGet("hash", "f"); v != "v" {
		t.Errorf("hash field %q", v)
	}
	if r, _ := st2.LRange("list", 0, -1); fmt.Sprint(r) != "[x y z]" {
		t.Errorf("list %v", r)
	}
	if v, _ := st2.Get("ctr"); v != "2" {
		t.Errorf("counter %q", v)
	}
	if used := st2.GetMemoryUsage(); used > cfg.MaxMemoryUsage {
		t.Errorf("cache memory %d after reading every key, budget %d", used, cfg.MaxMemoryUsage/2)
	}
}

// TestLSMFlagCombinations: LSM mode refuses the commit log and tiering.
func TestLSMFlagCombinations(t *testing.T) {
	cfg := lsmConfig(t.TempDir(), 1<<20)
	cfg.CommitLogEnabled = true
	if _, err := New(cfg); err == nil {
		t.Error("--storage=lsm with the commit log was accepted")
	}
	cfg = lsmConfig(t.TempDir(), 1<<20)
	cfg.TieringEnabled = true
	if _, err := New(cfg); err == nil {
		t.Error("--storage=lsm with tiering was accepted")
	}
	cfg = lsmConfig(t.TempDir(), 1<<20)
	cfg.Storage = "rocks"
	if _, err := New(cfg); err == nil {
		t.Error("an unknown storage engine was accepted")
	}
}
