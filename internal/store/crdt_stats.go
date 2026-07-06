package store

// CRDTStats is a point-in-time snapshot of the CRDT state a node holds, for
// observability: how many keys carry convergent state and how many tombstones
// (retained delete markers) are outstanding. Tombstones are the memory overhead
// that background GC reclaims once they age past the retention window, so a
// steadily growing tombstone count is the signal to watch.
type CRDTStats struct {
	// Tombstones retained per type.
	StringTombstones int64 `json:"string_tombstones"`
	SetTombstones    int64 `json:"set_tombstones"`  // retained element remove-stamps
	HashTombstones   int64 `json:"hash_tombstones"` // deleted field markers
	ListTombstones   int64 `json:"list_tombstones"` // deleted element markers

	// Keys carrying CRDT state (cluster mode).
	Counters   int64 `json:"counters"`    // string keys with PN-counter state
	CRDTSets   int64 `json:"crdt_sets"`   // sets with element-CRDT state
	CRDTHashes int64 `json:"crdt_hashes"` // hashes with per-field CRDT state
	CRDTLists  int64 `json:"crdt_lists"`  // lists with RGA state

	// Live (non-tombstone) container elements, for a tombstone-ratio view.
	SetElements  int64 `json:"set_elements"`
	HashFields   int64 `json:"hash_fields"`
	ListElements int64 `json:"list_elements"`
}

// CRDTStats walks every shard and tallies the node's CRDT state and tombstones.
// It is O(keys) and intended for an on-demand stats endpoint, not a hot path.
func (s *LockFreeStore) CRDTStats() CRDTStats {
	var st CRDTStats
	for i := 0; i < numShards; i++ {
		shard := s.shards[i]
		shard.mu.RLock()
		st.StringTombstones += int64(len(shard.tombstones))
		for _, v := range shard.data {
			if v == nil {
				continue
			}
			switch v.Type {
			case ValueTypeString:
				if v.counter != nil {
					st.Counters++
				}
			case ValueTypeSet:
				if v.SetVal != nil && v.SetVal.crdt != nil {
					st.CRDTSets++
					st.SetElements += int64(v.SetVal.totalSize)
					st.SetTombstones += v.SetVal.crdt.tombstoneCount()
				}
			case ValueTypeHash:
				if v.HashVal != nil && v.HashVal.crdt != nil {
					st.CRDTHashes++
					live, dead := v.HashVal.crdt.fieldCounts()
					st.HashFields += live
					st.HashTombstones += dead
				}
			case ValueTypeList:
				if v.ListVal != nil && v.ListVal.rga != nil {
					st.CRDTLists++
					live, dead := v.ListVal.rga.elementCounts()
					st.ListElements += live
					st.ListTombstones += dead
				}
			}
		}
		shard.mu.RUnlock()
	}
	return st
}

// tombstoneCount returns the number of elements currently hidden by a winning
// remove (retained tombstones).
func (s *lwwSet) tombstoneCount() int64 {
	var n int64
	for elem := range s.removes {
		if !s.contains(elem) {
			n++
		}
	}
	return n
}

// fieldCounts returns the live and deleted (tombstoned) field counts.
func (h *lwwHash) fieldCounts() (live, dead int64) {
	for _, f := range h.fields {
		if f.deleted {
			dead++
		} else {
			live++
		}
	}
	return live, dead
}

// elementCounts returns the live and tombstoned element counts.
func (r *rgaList) elementCounts() (live, dead int64) {
	for _, e := range r.elems {
		if e.deleted {
			dead++
		} else {
			live++
		}
	}
	return live, dead
}
