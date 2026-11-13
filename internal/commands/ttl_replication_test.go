package commands

import (
	"bufio"
	"bytes"
	"strconv"
	"testing"
	"time"

	"reservoir/internal/protocol"
	"reservoir/internal/store"
)

type replOp struct {
	op, key, value string
}

// recordReplicate returns a replication callback that records each event.
func recordReplicate(ops *[]replOp) ReplicationCallback {
	return func(operation, key string, value []byte) {
		*ops = append(*ops, replOp{operation, key, string(value)})
	}
}

func runCmd(t *testing.T, h func(store.KVStore, *protocol.ZeroCopyCommand, *bufio.Writer, ReplicationCallback) error, st store.KVStore, rep ReplicationCallback, args ...string) {
	t.Helper()
	bargs := make([][]byte, len(args))
	for i, a := range args {
		bargs[i] = []byte(a)
	}
	var buf bytes.Buffer
	w := bufio.NewWriter(&buf)
	if err := h(st, createTestCommand(bargs), w, rep); err != nil {
		t.Fatalf("handler error: %v", err)
	}
	w.Flush()
}

// TestSetWithTTLReplicatesExpire verifies that commands leaving a key with a TTL
// follow the replicated SET (which, in cluster mode, clears the TTL as a plain
// SET) with a stamped EXPIRE carrying the absolute deadline.
func TestSetWithTTLReplicatesExpire(t *testing.T) {
	newStore := func() store.KVStore {
		return store.NewStore(&store.Limits{MaxKeySize: 1024, MaxValueSize: 1 << 20, MaxMemoryUsage: 1 << 30})
	}

	t.Run("SET EX", func(t *testing.T) {
		st := newStore()
		defer st.Stop()
		var ops []replOp
		before := time.Now()
		runCmd(t, HandleZeroCopySetWithReplication, st, recordReplicate(&ops), "k", "v", "EX", "100")
		if len(ops) != 2 || ops[0].op != "SET" || ops[1].op != "EXPIRE" {
			t.Fatalf("ops = %+v, want [SET EXPIRE]", ops)
		}
		d, err := strconv.ParseInt(ops[1].value, 10, 64)
		if err != nil {
			t.Fatalf("EXPIRE payload %q: %v", ops[1].value, err)
		}
		if got := time.Unix(0, d).Sub(before); got < 99*time.Second || got > 101*time.Second {
			t.Fatalf("EXPIRE deadline is %v after the command, want ~100s", got)
		}
	})

	t.Run("plain SET has no EXPIRE", func(t *testing.T) {
		st := newStore()
		defer st.Stop()
		var ops []replOp
		runCmd(t, HandleZeroCopySetWithReplication, st, recordReplicate(&ops), "k", "v", "NX")
		if len(ops) != 1 || ops[0].op != "SET" {
			t.Fatalf("ops = %+v, want [SET]", ops)
		}
	})

	t.Run("SET KEEPTTL", func(t *testing.T) {
		st := newStore()
		defer st.Stop()
		st.Set("k", "old")
		st.SetExpiry("k", time.Now().Add(time.Hour))
		var ops []replOp
		runCmd(t, HandleZeroCopySetWithReplication, st, recordReplicate(&ops), "k", "new", "KEEPTTL")
		if len(ops) != 2 || ops[1].op != "EXPIRE" {
			t.Fatalf("ops = %+v, want [SET EXPIRE]", ops)
		}
	})

	t.Run("APPEND keeps TTL", func(t *testing.T) {
		st := newStore()
		defer st.Stop()
		st.Set("k", "a")
		st.SetExpiry("k", time.Now().Add(time.Hour))
		var ops []replOp
		runCmd(t, HandleZeroCopyAppendWithReplication, st, recordReplicate(&ops), "k", "b")
		if len(ops) != 2 || ops[0].op != "SET" || ops[0].value != "ab" || ops[1].op != "EXPIRE" {
			t.Fatalf("ops = %+v, want [SET ab, EXPIRE]", ops)
		}
	})
}
