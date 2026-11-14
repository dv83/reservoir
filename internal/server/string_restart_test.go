package server

import (
	"testing"
	"time"

	"reservoir/internal/store"
	"reservoir/pkg/config"
)

// TestRestartKeepsStringValuesAndTTLs (task 1.2): a real node restarted from its
// commit log must recover the value and TTL of keys last written by SET EX,
// SET KEEPTTL, APPEND, INCR and GETSET. Before the fix SET EX/KEEPTTL and INCR
// lost the TTL, and APPEND/GETSET were not logged, so their values rolled back.
func TestRestartKeepsStringValuesAndTTLs(t *testing.T) {
	dir := t.TempDir()
	deadline := time.Now().Add(time.Hour)

	logWrites(t, dir, func(st store.KVStore) {
		st.SetWithOptions("ex", "v", store.SetOptions{SetTTL: true, TTL: time.Hour})

		st.Set("kt", "a")
		st.SetExpiry("kt", deadline)
		st.SetWithOptions("kt", "b", store.SetOptions{KeepTTL: true})

		st.Set("ap", "a")
		st.SetExpiry("ap", deadline)
		st.Append("ap", "b")

		st.Set("ctr", "1")
		st.SetExpiry("ctr", deadline)
		st.Incr("ctr")

		st.Set("gs", "old")
		st.SetExpiry("gs", deadline)
		st.GetSet("gs", "new")
	})

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

	for _, c := range []struct {
		key, value string
		ttl        bool
	}{
		{"ex", "v", true},
		{"kt", "b", true},
		{"ap", "ab", true},
		{"ctr", "2", true},
		{"gs", "new", false},
	} {
		v, ok := st.Get(c.key)
		if !ok || v != c.value {
			t.Errorf("%s = (%q, %v) after restart, want (%q, true)", c.key, v, ok, c.value)
		}
		if _, has := st.GetTTL(c.key); has != c.ttl {
			t.Errorf("%s hasTTL = %v after restart, want %v", c.key, has, c.ttl)
		}
	}
}
