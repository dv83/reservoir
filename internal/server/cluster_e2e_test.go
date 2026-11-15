package server

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"reflect"
	"sort"
	"strconv"
	"testing"
	"time"

	"reservoir/internal/cluster"
	"reservoir/internal/commands"
	"reservoir/internal/protocol"
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

	// Anti-entropy observability: within a few background rounds the counters
	// must climb — nodes both initiate digest requests and serve them.
	aeActive := eventually(8*time.Second, func() bool {
		var rounds, served uint64
		for _, n := range nodes {
			ae := n.cm.GetAntiEntropyStats()
			rounds += ae.RoundsInitiated
			served += ae.DigestsServed
		}
		return rounds > 0 && served > 0
	})
	if !aeActive {
		t.Error("anti-entropy counters did not advance across the cluster")
	}
}

// newE2ECluster starts three joined nodes and waits for the full mesh.
func newE2ECluster(t *testing.T) []*e2eNode {
	t.Helper()
	clusterID := cluster.NewUUIDv7()
	nodes := []*e2eNode{newE2ENode(t, clusterID), newE2ENode(t, clusterID), newE2ENode(t, clusterID)}
	t.Cleanup(func() {
		for _, n := range nodes {
			n.stop()
		}
	})
	for _, n := range nodes[1:] {
		if err := n.cm.JoinCluster("127.0.0.1", nodes[0].port); err != nil {
			t.Fatalf("join: %v", err)
		}
	}
	if !eventually(10*time.Second, func() bool {
		for _, n := range nodes {
			if len(n.cm.GetNodes()) != len(nodes) {
				return false
			}
		}
		return true
	}) {
		t.Fatal("cluster mesh did not form")
	}
	return nodes
}

// TestClusterDeferredExecutesOnce is the regression test for deferred commands
// in cluster mode: a deferred INCR must count once cluster-wide. Previously the
// schedule was replicated and every node executed it into its own PN-counter
// slot, so anti-entropy merged them into N× the intended increment.
func TestClusterDeferredExecutesOnce(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping cluster e2e test in -short mode")
	}
	nodes := newE2ECluster(t)

	// The origin node executes its due command through the server executor.
	origin := &Server{kvStore: nodes[0].store, clusterManager: nodes[0].cm}
	origin.executeDeferred("INCR", "dc", nil)
	origin.executeDeferred("INCRBY", "dc", []string{"4"})
	origin.executeDeferred("SET", "ds", []string{"once"})

	ok := eventually(10*time.Second, func() bool {
		for _, n := range nodes {
			if v, _ := n.store.Get("dc"); v != "5" {
				return false
			}
			if v, _ := n.store.Get("ds"); v != "once" {
				return false
			}
		}
		return true
	})
	if !ok {
		for i, n := range nodes {
			dc, _ := n.store.Get("dc")
			ds, _ := n.store.Get("ds")
			t.Logf("node %d: dc=%q ds=%q", i, dc, ds)
		}
		t.Fatal("deferred effects did not converge to exactly-once values")
	}

	// Force full anti-entropy between every pair (background anti-entropy only
	// visits one of 256 shards per 2s, so waiting would not cover this key). The
	// counter must stay at 5: under the old behavior each node had run the INCR
	// into its own PN slot, and this merge turned 1 increment into 3.
	fullReconcile(nodes)
	for i, n := range nodes {
		if v, _ := n.store.Get("dc"); v != "5" {
			t.Fatalf("node %d counter is %q after full reconciliation, want 5", i, v)
		}
	}
}

// fullReconcile pulls every shard's digest between every ordered pair of nodes,
// as background anti-entropy eventually would.
func fullReconcile(nodes []*e2eNode) {
	for _, a := range nodes {
		for _, b := range nodes {
			if a != b {
				reconcile(&StoreReplicationHandler{Store: a.store}, &StoreReplicationHandler{Store: b.store})
			}
		}
	}
}

// TestApplyReplicationIgnoresDefer verifies a follower does not schedule a
// replicated DEFER (it would execute once per node).
func TestApplyReplicationIgnoresDefer(t *testing.T) {
	st := store.NewStore(&store.Limits{MaxKeySize: 1024, MaxValueSize: 1 << 20, MaxMemoryUsage: 1 << 30})
	defer st.Stop()
	h := &StoreReplicationHandler{Store: st}

	if err := h.ApplyReplication(cluster.ReplicationEvent{
		Operation: "DEFER",
		Key:       "id-1",
		Value:     []byte("1:INCR:k"),
	}); err != nil {
		t.Fatalf("apply DEFER: %v", err)
	}
	if n := len(st.ListDeferredCommands()); n != 0 {
		t.Fatalf("follower scheduled %d deferred command(s), want 0", n)
	}
}

// runHandler executes a real command handler on a node, routing replication
// through the node's real cluster write path.
func runHandler(t *testing.T, srv *Server, h commands.ZeroCopyHandlerWithReplication, args ...string) {
	t.Helper()
	bargs := make([][]byte, len(args))
	for i, a := range args {
		bargs[i] = []byte(a)
	}
	var buf bytes.Buffer
	w := bufio.NewWriter(&buf)
	cmd := &protocol.ZeroCopyCommand{Command: []byte("TEST"), Args: bargs}
	if err := h(srv.kvStore, cmd, w, srv.replicateWrite); err != nil {
		t.Fatalf("handler: %v", err)
	}
	w.Flush()
}

// TestClusterSetEXKeepsTTL: "SET k v EX n" through the real handler and cluster
// write path keeps its TTL on the origin and on every peer. Previously the
// replicated SET (applied as a stamped LWW write) cleared the TTL on the origin
// itself, and peers never received it.
func TestClusterSetEXKeepsTTL(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping cluster e2e test in -short mode")
	}
	nodes := newE2ECluster(t)
	origin := &Server{kvStore: nodes[0].store, clusterManager: nodes[0].cm}
	runHandler(t, origin, commands.HandleZeroCopySetWithReplication, "tk", "v", "EX", "3600")

	ok := eventually(10*time.Second, func() bool {
		for _, n := range nodes {
			if v, _ := n.store.Get("tk"); v != "v" {
				return false
			}
			if ttl, has := n.store.GetTTL("tk"); !has || ttl < 3500*time.Second {
				return false
			}
		}
		return true
	})
	if !ok {
		for i, n := range nodes {
			v, _ := n.store.Get("tk")
			ttl, has := n.store.GetTTL("tk")
			t.Logf("node %d: v=%q ttl=%v has=%v", i, v, ttl, has)
		}
		t.Fatal("SET EX did not keep its TTL across the cluster")
	}
}

// TestClusterExpiredKeyNotResurrected: peers that missed an EXPIRE must still
// expire the key, instead of serving it forever and handing it back to the
// origin. Previously the anti-entropy digest carried no TTL, so the peers kept
// the value with no TTL at all. Now the TTL register travels in the digest and
// expires the key everywhere. (The reap tombstone that blocks re-sending an
// already-reaped value is covered at store level, where the reaper is callable.)
func TestClusterExpiredKeyNotResurrected(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping cluster e2e test in -short mode")
	}
	nodes := newE2ECluster(t)
	origin := &Server{kvStore: nodes[0].store, clusterManager: nodes[0].cm}

	// A stamped SET reaches every node.
	origin.replicateWrite("SET", "rk", []byte("v"))
	if !eventually(10*time.Second, func() bool {
		for _, n := range nodes {
			if v, _ := n.store.Get("rk"); v != "v" {
				return false
			}
		}
		return true
	}) {
		t.Fatal("SET did not replicate")
	}

	// The EXPIRE is applied on the origin only — peers miss it.
	p, l, o := nodes[0].cm.NextStamp()
	nodes[0].store.ExpireLWW("rk", time.Now().Add(200*time.Millisecond).UnixNano(), p, l, o)

	time.Sleep(300 * time.Millisecond) // let the deadline pass

	// Peers still serve the key: they never learned of the EXPIRE.
	if _, ok := nodes[1].store.Get("rk"); !ok {
		t.Fatal("precondition: peer should still hold the key before anti-entropy")
	}

	fullReconcile(nodes)

	for i, n := range nodes {
		if v, ok := n.store.Get("rk"); ok {
			ttl, has := n.store.GetTTL("rk")
			t.Fatalf("node %d still serves expired key: v=%q ttl=%v hasTTL=%v", i, v, ttl, has)
		}
	}
}

// TestClusterWriteAfterExpiryStartsFresh (fix-expired-key-access, task 2.3): an
// SADD on a set whose deadline has passed starts a new set with no TTL on the
// node that ran it, and every node converges on that new set — none keeps the
// expired members, and none reaps the new member together with the old set.
func TestClusterWriteAfterExpiryStartsFresh(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping cluster e2e test in -short mode")
	}
	nodes := newE2ECluster(t)
	origin := &Server{kvStore: nodes[0].store, clusterManager: nodes[0].cm}
	writer := &Server{kvStore: nodes[1].store, clusterManager: nodes[1].cm}

	runHandler(t, origin, commands.HandleZeroCopySAddWithReplication, "es", "a", "b")
	deadline := time.Now().Add(300 * time.Millisecond)
	origin.replicateWrite("EXPIRE", "es", []byte(strconv.FormatInt(deadline.UnixNano(), 10)))
	nodes[0].store.SetExpiry("es", deadline)
	if !eventually(10*time.Second, func() bool {
		for _, n := range nodes {
			if _, has := n.store.GetTTL("es"); !has {
				return false
			}
		}
		return true
	}) {
		t.Fatal("set and its TTL did not replicate")
	}
	time.Sleep(time.Until(deadline) + 100*time.Millisecond)

	// The reaper has not run (it runs every minute): the old set is still
	// stored everywhere, just expired.
	runHandler(t, writer, commands.HandleZeroCopySAddWithReplication, "es", "c")

	want := []string{"c"}
	converged := func() bool {
		for _, n := range nodes {
			m, _ := n.store.SMembers("es")
			sort.Strings(m)
			if !reflect.DeepEqual(m, want) {
				return false
			}
			if _, has := n.store.GetTTL("es"); has {
				return false
			}
		}
		return true
	}
	ok := eventually(10*time.Second, converged)
	if ok {
		fullReconcile(nodes)
		ok = converged()
	}
	if !ok {
		for i, n := range nodes {
			m, _ := n.store.SMembers("es")
			ttl, has := n.store.GetTTL("es")
			t.Logf("node %d: members=%v ttl=%v has=%v", i, m, ttl, has)
		}
		t.Fatalf("nodes did not converge on %v with no TTL", want)
	}
}

// seedEveryType writes a string, set, hash, counter and list on origin and waits
// until every node holds all of them.
func seedEveryType(t *testing.T, nodes []*e2eNode, origin *Server) []string {
	t.Helper()
	origin.replicateWrite("SET", "rs", []byte("v"))
	runHandler(t, origin, commands.HandleZeroCopySAddWithReplication, "rset", "a")
	runHandler(t, origin, commands.HandleZeroCopyHSetWithReplication, "rhash", "f", "v")
	runHandler(t, origin, commands.HandleZeroCopyIncrWithReplication, "rctr")
	runHandler(t, origin, commands.HandleZeroCopyRPushWithReplication, "rlist", "x")
	keys := []string{"rs", "rset", "rhash", "rctr", "rlist"}
	if !eventually(10*time.Second, func() bool {
		for _, n := range nodes {
			if n.store.Exists(keys...) != int64(len(keys)) {
				return false
			}
		}
		return true
	}) {
		t.Fatal("seed writes did not replicate")
	}
	return keys
}

// TestClusterDeleteNotUndoneByAntiEntropy (fix-key-reset-convergence, task 1.3):
// a DEL applied on one node only must win everywhere after anti-entropy, for
// every key type. Previously peers handed sets, hashes, counters and lists back
// to the deleting node, because container apply paths ignored the tombstone.
func TestClusterDeleteNotUndoneByAntiEntropy(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping cluster e2e test in -short mode")
	}
	nodes := newE2ECluster(t)
	origin := &Server{kvStore: nodes[0].store, clusterManager: nodes[0].cm}
	keys := seedEveryType(t, nodes, origin)

	// The DELs are applied on the origin only — peers miss them.
	for _, k := range keys {
		p, l, o := nodes[0].cm.NextStamp()
		nodes[0].store.DeleteLWW(k, p, l, o)
	}
	fullReconcile(nodes)
	fullReconcile(nodes)

	for i, n := range nodes {
		for _, k := range keys {
			if n.store.Exists(k) != 0 {
				t.Errorf("node %d: %s came back after DEL", i, k)
			}
		}
	}
}

// TestClusterFlushAllConverges (fix-key-reset-convergence, task 1.3): a
// FLUSHALL that a peer missed still empties every node through anti-entropy,
// and a write stamped after the FLUSHALL survives on every node.
func TestClusterFlushAllConverges(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping cluster e2e test in -short mode")
	}
	nodes := newE2ECluster(t)
	origin := &Server{kvStore: nodes[0].store, clusterManager: nodes[0].cm}
	keys := seedEveryType(t, nodes, origin)

	// FLUSHALL on the origin only — the peers miss the event.
	p, l, o := nodes[0].cm.NextStamp()
	nodes[0].store.FlushLWW(p, l, o)

	// A write after the FLUSHALL, on a peer that has not seen it yet.
	time.Sleep(time.Millisecond)
	peer := &Server{kvStore: nodes[1].store, clusterManager: nodes[1].cm}
	peer.replicateWrite("SET", "after", []byte("kept"))

	fullReconcile(nodes)
	fullReconcile(nodes)

	for i, n := range nodes {
		for _, k := range keys {
			if n.store.Exists(k) != 0 {
				t.Errorf("node %d: %s survived or came back after FLUSHALL", i, k)
			}
		}
		if v, _ := n.store.Get("after"); v != "kept" {
			t.Errorf("node %d: write after FLUSHALL = %q, want kept", i, v)
		}
	}
}
