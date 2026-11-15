package server

import (
	"reflect"
	"sort"
	"testing"
	"time"

	"reservoir/internal/store"
	"reservoir/pkg/config"
)

// restartFrom starts a real standalone server that recovers from the commit log
// in dir, and waits for recovery to finish.
func restartFrom(t *testing.T, dir string) store.KVStore {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.CommitLogEnabled = true
	cfg.CommitLogDir = dir
	cfg.UIPort = ""
	cfg.ClusterEnabled = false
	srv, err := New(cfg)
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	t.Cleanup(srv.kvStore.Stop)
	if !eventually(10*time.Second, func() bool { return !srv.kvStore.IsRecoveryActive() }) {
		t.Fatal("recovery did not finish")
	}
	return srv.kvStore
}

func sortedSet(st store.KVStore, key string) []string {
	m, _ := st.SMembers(key)
	sort.Strings(m)
	if m == nil {
		m = []string{}
	}
	return m
}

func fullList(st store.KVStore, key string) []string {
	r, _ := st.LRange(key, 0, -1)
	if r == nil {
		r = []string{}
	}
	return r
}

// TestRestartKeepsStandaloneContainers (persist-containers, task 1.1): a
// standalone node restarted from its commit log recovers every set, hash and
// list exactly. Before the change plain set and hash writes were not logged on
// standalone nodes and list writes were never logged.
func TestRestartKeepsStandaloneContainers(t *testing.T) {
	dir := t.TempDir()
	want := map[string][]string{}
	var wantHash map[string]string

	logWrites(t, dir, func(st store.KVStore) {
		// sets
		st.SAdd("s", "a", "b", "c", "d")
		st.SRem("s", "a")
		popped, _, _ := st.SPop("s")
		st.SAdd("other", "c", "z")
		st.SDiffStore("diff", "s", "other")

		// hashes
		st.HSet("h", "f", "1", "g", "x", "gone", "y")
		st.HIncrBy("h", "f", 2)
		st.HIncrByFloat("h", "fl", 1.5)
		st.HDel("h", "gone")

		// lists
		st.RPush("l", "a", "b", "c", "d", "e")
		st.LPush("l", "z")
		st.LPop("l", 1)
		st.RPop("l", 1)
		st.LSet("l", 0, "A")
		st.RPush("l", "b")
		st.LRem("l", 1, "b")
		st.LPushX("l", "x")
		st.RPushX("missing", "never")
		st.RPushUnique("l", "A", "q")
		st.LPushUnique("l", "q", "w")
		st.LTrim("l", 0, 5)

		want["s"] = sortedSet(st, "s")
		want["diff"] = sortedSet(st, "diff")
		want["l"] = fullList(st, "l")
		wantHash = map[string]string{}
		for _, f := range []string{"f", "g", "fl", "gone"} {
			if v, ok, _ := st.HGet("h", f); ok {
				wantHash[f] = v
			}
		}
		if len(want["s"]) != 2 || popped == "" {
			t.Fatalf("setup: s = %v, popped %q", want["s"], popped)
		}
	})

	st := restartFrom(t, dir)
	for _, k := range []string{"s", "diff"} {
		if got := sortedSet(st, k); !reflect.DeepEqual(got, want[k]) {
			t.Errorf("set %s after restart = %v, want %v", k, got, want[k])
		}
	}
	gotHash := map[string]string{}
	for _, f := range []string{"f", "g", "fl", "gone"} {
		if v, ok, _ := st.HGet("h", f); ok {
			gotHash[f] = v
		}
	}
	if !reflect.DeepEqual(gotHash, wantHash) {
		t.Errorf("hash after restart = %v, want %v", gotHash, wantHash)
	}
	if got := fullList(st, "l"); !reflect.DeepEqual(got, want["l"]) {
		t.Errorf("list after restart = %v, want %v", got, want["l"])
	}
	if st.Exists("missing") != 0 {
		t.Errorf("RPUSHX on a missing key created it after restart")
	}
}

// listElementIDs returns the RGA element ids (live and tombstoned) a store holds
// for key, as its anti-entropy digest reports them.
func listElementIDs(st store.KVStore, key string) []store.ListElemDesc {
	for shard := 0; shard < st.ShardCount(); shard++ {
		for _, e := range st.ShardLWWDigest(shard) {
			if e.Key == key && e.List != nil {
				elems := append([]store.ListElemDesc(nil), e.List.Elems...)
				sort.Slice(elems, func(i, j int) bool {
					a, b := elems[i], elems[j]
					if a.IDP != b.IDP {
						return a.IDP < b.IDP
					}
					return a.IDO < b.IDO
				})
				return elems
			}
		}
	}
	return nil
}

// TestRestartKeepsClusterListIdentity (persist-containers, task 1.2): a cluster
// node's list is recovered with the same RGA element ids, so after a restart its
// peers' anti-entropy merges into it instead of duplicating every element.
func TestRestartKeepsClusterListIdentity(t *testing.T) {
	dir := t.TempDir()
	var before []store.ListElemDesc
	var wantVals []string

	logWrites(t, dir, func(st store.KVStore) {
		st.(*store.LockFreeStore).SetLocalOrigin(42)
		st.RPush("cl", "a", "b", "c")
		st.LPush("cl", "z")
		st.LPop("cl", 1)
		st.LSet("cl", 0, "A")
		before = listElementIDs(st, "cl")
		wantVals = fullList(st, "cl")
	})
	if len(before) == 0 {
		t.Fatal("setup: no CRDT list state")
	}

	st := restartFrom(t, dir)
	if got := fullList(st, "cl"); !reflect.DeepEqual(got, wantVals) {
		t.Fatalf("list after restart = %v, want %v", got, wantVals)
	}
	if after := listElementIDs(st, "cl"); !reflect.DeepEqual(after, before) {
		t.Fatalf("list element ids changed across restart:\nbefore=%+v\nafter =%+v", before, after)
	}
}
