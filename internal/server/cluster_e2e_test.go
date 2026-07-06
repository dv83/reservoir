package server

import (
	"fmt"
	"net"
	"sort"
	"testing"
	"time"

	"reservoir/internal/cluster"
	"reservoir/internal/store"
)

// e2eNode bundles a real ClusterManager and its store, wired exactly as the
// server wires them, so writes flow through the live transport and apply path.
type e2eNode struct {
	cm    *cluster.ClusterManager
	store store.KVStore
	port  int
}

// freePort returns an available localhost TCP port.
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("freePort: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func newE2ENode(t *testing.T, clusterID cluster.UUIDv7) *e2eNode {
	t.Helper()
	st := store.NewStore(&store.Limits{MaxKeySize: 1024, MaxValueSize: 1 << 20, MaxMemoryUsage: 1 << 30})
	port := freePort(t)
	cm := cluster.NewClusterManager("127.0.0.1", port, clusterID)
	cm.SetReplicationHandler(&StoreReplicationHandler{Store: st})
	st.SetLocalOrigin(cm.Origin())
	if err := cm.Start(); err != nil {
		t.Fatalf("cluster start: %v", err)
	}
	return &e2eNode{cm: cm, store: st, port: port}
}

func (n *e2eNode) stop() {
	n.cm.Stop()
	n.store.Stop()
}

// --- write helpers mirroring server/handler.go's replicationCallback ---

func (n *e2eNode) set(key, val string) {
	p, l, o := n.cm.NextStamp()
	_, _ = n.store.SetLWW(key, val, p, l, o)
	n.cm.QueueReplicationLWW("SET", key, []byte(val), p, l, o)
}

func (n *e2eNode) sadd(key string, members ...string) {
	p, l, o := n.cm.NextStamp()
	_, _ = n.store.SAddLWW(key, p, l, o, members...)
	payload := key
	for _, m := range members {
		payload += "\x00" + m
	}
	n.cm.QueueReplicationLWW("SADD", key, []byte(payload), p, l, o)
}

func (n *e2eNode) incr(key string, delta int64) {
	_, _ = n.store.IncrBy(key, delta)
	if st, ok := n.store.CounterState(key); ok {
		if enc, err := encodeCounterState(st); err == nil {
			n.cm.QueueReplication("COUNTER", key, enc)
		}
	}
}

func (n *e2eNode) hset(key string, fieldValues ...string) {
	p, l, o := n.cm.NextStamp()
	_, _ = n.store.HSetLWW(key, p, l, o, fieldValues...)
	payload := key
	for _, fv := range fieldValues {
		payload += "\x00" + fv
	}
	n.cm.QueueReplicationLWW("HSET", key, []byte(payload), p, l, o)
}

func (n *e2eNode) rpush(key string, elems ...string) {
	_, _ = n.store.RPush(key, elems...)
	if d, ok := n.store.DrainListDelta(key); ok {
		if enc, err := encodeListDelta(d); err == nil {
			n.cm.QueueReplication("LDELTA", key, enc)
		}
	}
}

// eventually polls fn until it returns true or the timeout elapses, returning
// the final (possibly false) result.
func eventually(timeout time.Duration, fn func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fn()
}

// TestClusterEndToEndConvergence stands up a real 3-node cluster over TCP,
// applies each data type's writes on different nodes (including concurrent
// counter increments from two nodes), and asserts every node converges — the
// full stack: store CRDT -> replication queue -> transport -> apply path.
func TestClusterEndToEndConvergence(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping cluster e2e test in -short mode")
	}
	clusterID := cluster.NewUUIDv7()
	n0 := newE2ENode(t, clusterID)
	n1 := newE2ENode(t, clusterID)
	n2 := newE2ENode(t, clusterID)
	nodes := []*e2eNode{n0, n1, n2}
	defer func() {
		for _, n := range nodes {
			n.stop()
		}
	}()

	// Join n1 and n2 to n0 (the seed).
	if err := n1.cm.JoinCluster("127.0.0.1", n0.port); err != nil {
		t.Fatalf("n1 join: %v", err)
	}
	if err := n2.cm.JoinCluster("127.0.0.1", n0.port); err != nil {
		t.Fatalf("n2 join: %v", err)
	}

	// Wait for the full mesh: every node sees all three.
	if !eventually(10*time.Second, func() bool {
		for _, n := range nodes {
			if len(n.cm.GetNodes()) != 3 {
				return false
			}
		}
		return true
	}) {
		t.Fatalf("cluster mesh did not form: sizes = %d,%d,%d",
			len(n0.cm.GetNodes()), len(n1.cm.GetNodes()), len(n2.cm.GetNodes()))
	}

	// Writes spread across nodes, one per data type.
	n0.set("sk", "hello")                 // string on node 0
	n1.sadd("st", "a", "b", "c")          // set on node 1
	n0.hset("ht", "f1", "v1", "f2", "v2") // hash on node 0
	n1.rpush("lt", "x", "y")              // list on node 1
	n2.rpush("lt", "z")                   // concurrent list push on node 2

	// Concurrent counter increments from two different nodes must all survive.
	n2.incr("ct", 5)
	n0.incr("ct", 3)

	// Assert convergence on every node.
	converged := eventually(15*time.Second, func() bool {
		for _, n := range nodes {
			if v, ok := n.store.Get("sk"); !ok || v != "hello" {
				return false
			}
			m, _ := n.store.SMembers("st")
			sort.Strings(m)
			if fmt.Sprint(m) != fmt.Sprint([]string{"a", "b", "c"}) {
				return false
			}
			if v, ok, _ := n.store.HGet("ht", "f1"); !ok || v != "v1" {
				return false
			}
			if v, ok, _ := n.store.HGet("ht", "f2"); !ok || v != "v2" {
				return false
			}
			l, _ := n.store.LRange("lt", 0, -1)
			if len(l) != 3 {
				return false
			}
			if v, ok := n.store.Get("ct"); !ok || v != "8" {
				return false
			}
		}
		return true
	})

	if !converged {
		for i, n := range nodes {
			sk, _ := n.store.Get("sk")
			sm, _ := n.store.SMembers("st")
			sort.Strings(sm)
			lt, _ := n.store.LRange("lt", 0, -1)
			ct, _ := n.store.Get("ct")
			t.Logf("node %d: sk=%q st=%v lt=%v ct=%q", i, sk, sm, lt, ct)
		}
		t.Fatal("cluster did not converge across all data types")
	}

	// All nodes must agree on the exact same list order, not just the count.
	l0, _ := n0.store.LRange("lt", 0, -1)
	for i, n := range nodes[1:] {
		li, _ := n.store.LRange("lt", 0, -1)
		if fmt.Sprint(li) != fmt.Sprint(l0) {
			t.Fatalf("list order diverged: node0=%v node%d=%v", l0, i+1, li)
		}
	}
}
