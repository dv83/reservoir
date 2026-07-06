package store

import "testing"

// TestCRDTStatsCounts verifies the CRDT snapshot tallies containers, live
// elements, and tombstones across every type.
func TestCRDTStatsCounts(t *testing.T) {
	s := newCRDTTestStore()
	s.SetLocalOrigin(1)
	defer s.Stop()

	// String LWW tombstone.
	s.SetLWW("sk", "v", 100, 0, 1)
	s.DeleteLWW("sk", 200, 0, 1)

	// Set: two live, one tombstoned.
	s.SAddLWW("st", 100, 0, 1, "a", "b", "c")
	s.SRemLWW("st", 200, 0, 1, "c")

	// Hash: one live field, one deleted.
	s.HSetLWW("ht", 100, 0, 1, "f1", "v1", "f2", "v2")
	s.HDelLWW("ht", 200, 0, 1, "f2")

	// List: three pushed, one popped.
	s.RPush("lt", "x", "y", "z")
	s.LPop("lt", 1)

	// Counter.
	s.IncrBy("ct", 5)

	st := s.CRDTStats()

	if st.StringTombstones != 1 {
		t.Errorf("StringTombstones = %d, want 1", st.StringTombstones)
	}
	if st.CRDTSets != 1 || st.SetElements != 2 || st.SetTombstones != 1 {
		t.Errorf("set stats = {sets:%d elems:%d tomb:%d}, want {1 2 1}", st.CRDTSets, st.SetElements, st.SetTombstones)
	}
	if st.CRDTHashes != 1 || st.HashFields != 1 || st.HashTombstones != 1 {
		t.Errorf("hash stats = {hashes:%d fields:%d tomb:%d}, want {1 1 1}", st.CRDTHashes, st.HashFields, st.HashTombstones)
	}
	if st.CRDTLists != 1 || st.ListElements != 2 || st.ListTombstones != 1 {
		t.Errorf("list stats = {lists:%d elems:%d tomb:%d}, want {1 2 1}", st.CRDTLists, st.ListElements, st.ListTombstones)
	}
	if st.Counters != 1 {
		t.Errorf("Counters = %d, want 1", st.Counters)
	}
}

// TestCRDTStatsEmpty verifies a fresh store reports zero CRDT state.
func TestCRDTStatsEmpty(t *testing.T) {
	s := newCRDTTestStore()
	defer s.Stop()
	st := s.CRDTStats()
	if st != (CRDTStats{}) {
		t.Fatalf("empty store CRDTStats = %+v, want zero", st)
	}
}
