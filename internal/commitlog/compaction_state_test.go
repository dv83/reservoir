package commitlog

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// --- replay model ---------------------------------------------------------
//
// keyState models what recovery (server.go's apply function over the store)
// rebuilds for one key. The model only needs to mirror the store where it
// matters for compaction: SET-like ops and DEL fully reset a key (value, type
// and TTL), FLUSHALL clears everything, and the remaining ops modify an existing
// key in place. Both the original and the compacted log are replayed through the
// same model and the same Reader.ReadAll ordering that recovery uses.
type keyState struct {
	Kind string // "string", "set" or "hash"
	Str  string
	TTL  string
	Set  map[string]bool
	Hash map[string]string
}

func members(value []byte) []string {
	parts := strings.Split(string(value), "\x00")
	if len(parts) < 2 {
		return nil
	}
	return parts[1:]
}

func applyModel(state map[string]*keyState, e *Entry) {
	key := string(e.Key)
	if e.Type == EntryTypeDelete {
		delete(state, key)
		return
	}
	switch e.Operation {
	case "FLUSHALL", "FLUSHDB":
		for k := range state {
			delete(state, k)
		}
	case "SET", "INCR", "INCRBY", "DECR", "DECRBY":
		state[key] = &keyState{Kind: "string", Str: string(e.Value)}
	case "EXPIRE":
		if ks, ok := state[key]; ok {
			ks.TTL = string(e.Value)
		}
	case "PERSIST":
		if ks, ok := state[key]; ok {
			ks.TTL = ""
		}
	case "SADD":
		ks, ok := state[key]
		if !ok {
			ks = &keyState{Kind: "set", Set: map[string]bool{}}
			state[key] = ks
		}
		if ks.Kind != "set" {
			return // WRONGTYPE: recovery skips the entry
		}
		for _, m := range members(e.Value) {
			ks.Set[m] = true
		}
	case "SREM":
		ks, ok := state[key]
		if !ok || ks.Kind != "set" {
			return
		}
		for _, m := range members(e.Value) {
			delete(ks.Set, m)
		}
		if len(ks.Set) == 0 {
			delete(state, key)
		}
	case "HSET", "HMSET":
		ks, ok := state[key]
		if !ok {
			ks = &keyState{Kind: "hash", Hash: map[string]string{}}
			state[key] = ks
		}
		if ks.Kind != "hash" {
			return
		}
		fv := members(e.Value)
		for i := 0; i+1 < len(fv); i += 2 {
			ks.Hash[fv[i]] = fv[i+1]
		}
	case "HDEL":
		ks, ok := state[key]
		if !ok || ks.Kind != "hash" {
			return
		}
		for _, f := range members(e.Value) {
			delete(ks.Hash, f)
		}
		if len(ks.Hash) == 0 {
			delete(state, key)
		}
	}
}

// replayDir replays every segment in dir exactly as recovery orders it.
func replayDir(t *testing.T, dir string) map[string]*keyState {
	t.Helper()
	reader, err := NewReader(dir)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer reader.Close()
	state := map[string]*keyState{}
	if err := reader.ReadAll(func(e *Entry) error {
		applyModel(state, e)
		return nil
	}); err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	return state
}

// --- log construction -----------------------------------------------------

func writeSegment(t *testing.T, dir string, id int, entries []*Entry) {
	t.Helper()
	path := filepath.Join(dir, fmt.Sprintf("%016d%s", id, SegmentFileExtension))
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create segment: %v", err)
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	cl := &CommitLog{currentSegment: &Segment{Writer: w}}
	for _, e := range entries {
		e.CRC = e.CalculateCRC()
		if err := cl.writeEntry(e); err != nil {
			t.Fatalf("writeEntry: %v", err)
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
}

func wr(ts int64, op, key, value string) *Entry {
	return &Entry{Type: EntryTypeWrite, Timestamp: ts, Operation: op, Key: []byte(key), Value: []byte(value)}
}

func del(ts int64, key string) *Entry {
	return &Entry{Type: EntryTypeDelete, Timestamp: ts, Operation: "DEL", Key: []byte(key)}
}

// compactDir runs the real compaction pipeline (state building, rewrite and
// segment replacement) over every segment in dir.
func compactDir(t *testing.T, dir string) {
	t.Helper()
	segs, err := filepath.Glob(filepath.Join(dir, "*"+SegmentFileExtension))
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(segs)
	c := NewCompactor(dir)
	state, _, err := c.buildCurrentState(segs)
	if err != nil {
		t.Fatalf("buildCurrentState: %v", err)
	}
	path, err := c.writeCompactedSegment(state, segs[0])
	if err != nil {
		t.Fatalf("writeCompactedSegment: %v", err)
	}
	if err := c.replaceSegments(segs, path); err != nil {
		t.Fatalf("replaceSegments: %v", err)
	}
}

// --- tests ----------------------------------------------------------------

// TestCompactionKeepsValueWithTTL (task 1.1): SET followed by EXPIRE must still
// recover the value and its TTL after compaction.
func TestCompactionKeepsValueWithTTL(t *testing.T) {
	dir := t.TempDir()
	writeSegment(t, dir, 1, []*Entry{
		wr(100, "SET", "k", "v"),
		wr(200, "EXPIRE", "k", "999999"),
	})
	writeSegment(t, dir, 2, []*Entry{wr(300, "SET", "other", "x")})

	compactDir(t, dir)
	got := replayDir(t, dir)

	k, ok := got["k"]
	if !ok {
		t.Fatal("value of k was lost by compaction")
	}
	if k.Str != "v" || k.TTL != "999999" {
		t.Fatalf("k = %+v, want value v with TTL 999999", *k)
	}
}

// TestCompactionKeepsContainerDeltas (task 1.2): successive SADDs and HSETs on
// one key must all survive compaction.
func TestCompactionKeepsContainerDeltas(t *testing.T) {
	dir := t.TempDir()
	writeSegment(t, dir, 1, []*Entry{
		wr(100, "SADD", "s", "s\x00a"),
		wr(110, "HSET", "h", "h\x00f1\x00v1"),
	})
	writeSegment(t, dir, 2, []*Entry{
		wr(200, "SADD", "s", "s\x00b"),
		wr(210, "HSET", "h", "h\x00f2\x00v2"),
	})

	compactDir(t, dir)
	got := replayDir(t, dir)

	if s := got["s"]; s == nil || !reflect.DeepEqual(s.Set, map[string]bool{"a": true, "b": true}) {
		t.Fatalf("set s = %+v, want members a and b", s)
	}
	if h := got["h"]; h == nil || !reflect.DeepEqual(h.Hash, map[string]string{"f1": "v1", "f2": "v2"}) {
		t.Fatalf("hash h = %+v, want fields f1 and f2", h)
	}
}

// TestCompactionFlushAllOrdering: a FLUSHALL must only clear what was written
// before it, never writes that came after it.
func TestCompactionFlushAllOrdering(t *testing.T) {
	dir := t.TempDir()
	writeSegment(t, dir, 1, []*Entry{
		wr(100, "SET", "a", "old"),
		wr(200, "FLUSHALL", "", ""),
		wr(300, "SET", "b", "new"),
	})
	writeSegment(t, dir, 2, []*Entry{wr(400, "SET", "c", "x")})

	before := replayDir(t, dir)
	compactDir(t, dir)
	after := replayDir(t, dir)

	if !reflect.DeepEqual(before, after) {
		t.Fatalf("compaction changed recovered state:\nbefore=%s\nafter =%s", dump(before), dump(after))
	}
}

// TestCompactionReplayEquivalence (task 1.3): for random op sequences spread
// over several segments (including entries stored out of timestamp order),
// replaying the compacted log yields exactly the state the original log does.
func TestCompactionReplayEquivalence(t *testing.T) {
	keys := []string{"k1", "k2", "k3", "k4"}
	members := []string{"a", "b", "c"}
	rng := rand.New(rand.NewSource(1))

	randomEntry := func(ts int64) *Entry {
		k := keys[rng.Intn(len(keys))]
		m := members[rng.Intn(len(members))]
		switch rng.Intn(12) {
		case 0, 1:
			return wr(ts, "SET", k, fmt.Sprintf("v%d", ts))
		case 2:
			return wr(ts, "INCRBY", k, fmt.Sprint(rng.Intn(100)))
		case 3:
			return del(ts, k)
		case 4:
			return wr(ts, "EXPIRE", k, fmt.Sprint(ts+1000))
		case 5:
			return wr(ts, "PERSIST", k, "")
		case 6, 7:
			return wr(ts, "SADD", k, k+"\x00"+m)
		case 8:
			return wr(ts, "SREM", k, k+"\x00"+m)
		case 9:
			return wr(ts, "HSET", k, k+"\x00"+m+"\x00"+fmt.Sprint(ts))
		case 10:
			return wr(ts, "HDEL", k, k+"\x00"+m)
		default:
			if rng.Intn(4) == 0 {
				return wr(ts, "FLUSHALL", "", "")
			}
			return wr(ts, "SET", k, "x")
		}
	}

	for round := 0; round < 200; round++ {
		dir := t.TempDir()
		n := 5 + rng.Intn(40)
		entries := make([]*Entry, n)
		for i := range entries {
			entries[i] = randomEntry(int64(1000 + i*10)) // unique, increasing
		}
		// Spread over 1-3 segments, shuffling within each so file order differs
		// from timestamp order (recovery orders by timestamp, not by file).
		segs := 1 + rng.Intn(3)
		for s := 0; s < segs; s++ {
			var chunk []*Entry
			for i := s; i < n; i += segs {
				chunk = append(chunk, entries[i])
			}
			rng.Shuffle(len(chunk), func(i, j int) { chunk[i], chunk[j] = chunk[j], chunk[i] })
			writeSegment(t, dir, s+1, chunk)
		}

		before := replayDir(t, dir)
		compactDir(t, dir)
		after := replayDir(t, dir)

		if !reflect.DeepEqual(before, after) {
			var log []string
			for _, e := range entries {
				log = append(log, fmt.Sprintf("%d %s %s %q", e.Timestamp, e.Operation, e.Key, e.Value))
			}
			t.Fatalf("round %d: compaction changed recovered state\nlog:\n  %s\nbefore=%s\nafter =%s",
				round, strings.Join(log, "\n  "), dump(before), dump(after))
		}
	}
}

func dump(state map[string]*keyState) string {
	keys := make([]string, 0, len(state))
	for k := range state {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		fmt.Fprintf(&b, "%s=%+v ", k, *state[k])
	}
	return b.String()
}
