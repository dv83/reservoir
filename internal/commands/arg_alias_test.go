package commands

import (
	"bufio"
	"bytes"
	"sort"
	"strings"
	"testing"
	"time"

	"reservoir/internal/protocol"
	"reservoir/internal/store"
)

// aliasedCommand builds a command whose arguments are slices of one buffer, as
// the parser produces them, and returns a function that overwrites that buffer
// the way the next command on the connection would.
func aliasedCommand(args ...string) (*protocol.ZeroCopyCommand, func()) {
	buf := []byte(strings.Join(args, ""))
	cmd := &protocol.ZeroCopyCommand{Buffer: buf}
	off := 0
	for _, a := range args {
		cmd.Args = append(cmd.Args, buf[off:off+len(a)])
		off += len(a)
	}
	return cmd, func() {
		for i := range buf {
			buf[i] = 'Z'
		}
	}
}

// TestStoredArgumentsDoNotAliasTheReadBuffer: the parser reuses its read
// buffer for the next command on a connection, so anything the store keeps
// from a command — key, member, field, value — must be a copy. Before the
// fix ArgString returned the buffer itself: after a few more commands on the
// same connection sets, hashes and lists read back empty and KEYS listed
// corrupted names.
func TestStoredArgumentsDoNotAliasTheReadBuffer(t *testing.T) {
	st := store.NewStore(&store.Limits{MaxKeySize: 1024, MaxValueSize: 1 << 20, MaxMemoryUsage: 1 << 30})
	defer st.Stop()
	w := bufio.NewWriter(&bytes.Buffer{})
	run := func(h func(store.KVStore, *protocol.ZeroCopyCommand, *bufio.Writer, ReplicationCallback) error, args ...string) {
		cmd, clobber := aliasedCommand(args...)
		if err := h(st, cmd, w, nil); err != nil {
			t.Fatalf("%v: %v", args, err)
		}
		clobber()
	}
	run(HandleZeroCopySAddWithReplication, "myset", "member1", "member2")
	run(HandleZeroCopyHSetWithReplication, "myhash", "field1", "value1")
	run(HandleZeroCopyRPushWithReplication, "mylist", "elem1", "elem2")
	run(HandleZeroCopySetWithReplication, "mystr", "strvalue")
	run(HandleZeroCopySetWithReplication, "victim", "v")
	run(HandleZeroCopyExpireWithReplication, "victim", "3600")
	run(HandleZeroCopyDelWithReplication, "mystr")

	members, _ := st.SMembers("myset")
	sort.Strings(members)
	if strings.Join(members, ",") != "member1,member2" {
		t.Errorf("SMEMBERS = %v", members)
	}
	if v, ok, _ := st.HGet("myhash", "field1"); !ok || v != "value1" {
		t.Errorf("HGET = (%q, %v)", v, ok)
	}
	if l, _ := st.LRange("mylist", 0, -1); strings.Join(l, ",") != "elem1,elem2" {
		t.Errorf("LRANGE = %v", l)
	}
	if _, ok := st.Get("mystr"); ok {
		t.Error("deleted key still present")
	}
	if ttl, ok := st.GetTTL("victim"); !ok || ttl < 59*time.Minute {
		t.Errorf("victim TTL = %v %v", ttl, ok)
	}
	keys := st.GetAllKeys()
	sort.Strings(keys)
	if strings.Join(keys, ",") != "myhash,mylist,myset,victim" {
		t.Errorf("KEYS = %q", keys)
	}
}
