package store

import "sort"

// rgaList is a sequence CRDT for Redis lists. Because the only list mutations
// insert at the ends (LPUSH/RPUSH — there is no middle insert), every element
// can be given a self-contained, immutable position at creation instead of a
// reference to a neighbor: append takes maxPos+1, prepend takes minPos-1. The
// visible list is that set of elements sorted by (pos, id), so every replica
// orders the same elements identically — convergence holds by construction, with
// no causal delivery requirement.
//
// Deletes are per-element tombstones keyed by the element id, so an insert that
// arrives after its own delete cannot resurrect the element. Element values can
// be replaced (LSET) under a per-element last-write-wins stamp.
//
// Known boundary: two nodes that concurrently push several elements can have
// those runs interleave in the merged order (the classic sequence-CRDT
// interleaving anomaly). Every element still survives and the order is
// deterministic on all replicas; it is simply not guaranteed to keep each node's
// run contiguous.
type rgaList struct {
	elems map[hlcStamp]*rgaElem
}

// rgaElem is one list element. id is its globally-unique creation stamp (also
// the final ordering tiebreak); pos is its dense-ish ordering key; valStamp is
// the last-write-wins stamp of its value (creation, then any LSET). A tombstone
// carries deleted=true and may be a placeholder (no pos/value) recorded by a
// delete that arrived before the element itself.
type rgaElem struct {
	id       hlcStamp
	pos      int64
	value    string
	valStamp hlcStamp
	deleted  bool
	// delTime is the local unix-nanos time this node tombstoned the element,
	// used only for tombstone GC. It is never replicated (each node ages its own
	// tombstones from when it learned of the delete), so element ids can stay a
	// synthetic counter rather than a wall clock.
	delTime int64
}

func newRGAList() *rgaList {
	return &rgaList{elems: make(map[hlcStamp]*rgaElem)}
}

// posBounds returns the current min and max pos over all elements (visible and
// tombstoned), so a new push is ordered relative to everything ever seen. ok is
// false when the list is empty. Delete placeholders sit at pos 0, which can only
// pull the bounds outward and so never misplaces an append (max+1) or prepend
// (min-1).
func (r *rgaList) posBounds() (min, max int64, ok bool) {
	for _, e := range r.elems {
		if !ok {
			min, max, ok = e.pos, e.pos, true
			continue
		}
		if e.pos < min {
			min = e.pos
		}
		if e.pos > max {
			max = e.pos
		}
	}
	return min, max, ok
}

// localAppend assigns a tail position to a new element and inserts it, returning
// the element descriptor to replicate. If a tombstone already exists for id the
// element stays deleted.
func (r *rgaList) localAppend(id hlcStamp, value string) rgaElem {
	pos := int64(0)
	if _, max, ok := r.posBounds(); ok {
		pos = max + 1
	}
	return r.insertPositioned(id, pos, value)
}

// localPrepend assigns a head position to a new element and inserts it.
func (r *rgaList) localPrepend(id hlcStamp, value string) rgaElem {
	pos := int64(0)
	if min, _, ok := r.posBounds(); ok {
		pos = min - 1
	}
	return r.insertPositioned(id, pos, value)
}

func (r *rgaList) insertPositioned(id hlcStamp, pos int64, value string) rgaElem {
	e := rgaElem{id: id, pos: pos, value: value, valStamp: id}
	if cur, ok := r.elems[id]; ok && cur.deleted {
		// A tombstone already won: keep it deleted but record the position/value
		// so the state is complete.
		cur.pos, cur.value, cur.valStamp = pos, value, id
		return *cur
	}
	cp := e
	r.elems[id] = &cp
	return e
}

// applyInsert applies a replicated element. It is idempotent and tombstone-aware:
// a tombstoned id stays deleted, an unseen id is added, an already-present id is
// left as-is.
func (r *rgaList) applyInsert(e rgaElem) {
	cur, ok := r.elems[e.id]
	if !ok {
		cp := e
		r.elems[e.id] = &cp
		return
	}
	if cur.deleted {
		// Fill in position/value for a delete placeholder without resurrecting.
		if cur.pos == 0 && cur.value == "" {
			cur.pos, cur.value, cur.valStamp = e.pos, e.value, e.valStamp
		}
		return
	}
	// Already present and live: keep the newer value under LWW.
	if e.valStamp.After(cur.valStamp) {
		cur.value, cur.valStamp = e.value, e.valStamp
	}
}

// applyDelete tombstones the element id, recording a placeholder if the element
// has not arrived yet so a later insert cannot resurrect it.
func (r *rgaList) applyDelete(id hlcStamp) {
	r.applyDeleteAt(id, 0)
}

// applyDeleteAt tombstones the element id and stamps the local deletion time
// (unix nanos) used for GC. atNanos of 0 leaves the tombstone un-GC-able (used
// by pure tests).
func (r *rgaList) applyDeleteAt(id hlcStamp, atNanos int64) {
	if cur, ok := r.elems[id]; ok {
		cur.deleted = true
		if cur.delTime == 0 {
			cur.delTime = atNanos
		}
		return
	}
	r.elems[id] = &rgaElem{id: id, deleted: true, delTime: atNanos}
}

// gcTombstones removes tombstones whose local deletion time predates
// cutoffNanos, and reports whether the list is now fully empty (no elements at
// all) so the caller can reclaim the key. Same bounded tradeoff as the other
// CRDTs: a delivery delayed past the retention window can resurrect an element.
func (r *rgaList) gcTombstones(cutoffNanos int64) (pruned int, empty bool) {
	for id, e := range r.elems {
		if e.deleted && e.delTime > 0 && e.delTime < cutoffNanos {
			delete(r.elems, id)
			pruned++
		}
	}
	return pruned, len(r.elems) == 0
}

// setValue replaces the value of a live element under last-write-wins, returning
// whether it was applied.
func (r *rgaList) setValue(id hlcStamp, value string, stamp hlcStamp) bool {
	cur, ok := r.elems[id]
	if !ok || cur.deleted {
		return false
	}
	if stamp.After(cur.valStamp) {
		cur.value, cur.valStamp = value, stamp
		return true
	}
	return false
}

// visible returns the live elements in list order (pos, then id).
func (r *rgaList) visible() []rgaElem {
	out := make([]rgaElem, 0, len(r.elems))
	for _, e := range r.elems {
		if !e.deleted {
			out = append(out, *e)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].pos != out[j].pos {
			return out[i].pos < out[j].pos
		}
		// Equal position (concurrent push): order by id ascending.
		return out[j].id.After(out[i].id)
	})
	return out
}

// values returns the visible element values in list order.
func (r *rgaList) values() []string {
	vis := r.visible()
	out := make([]string, len(vis))
	for i, e := range vis {
		out[i] = e.value
	}
	return out
}

// length returns the number of visible elements.
func (r *rgaList) length() int {
	n := 0
	for _, e := range r.elems {
		if !e.deleted {
			n++
		}
	}
	return n
}
