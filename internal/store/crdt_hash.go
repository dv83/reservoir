package store

// lwwHash is a last-write-wins map CRDT: every field independently keeps the
// stamp of its most recent write, which is either a set (a value) or a delete (a
// tombstone). A field is present iff its winning write is a set. Because each
// field resolves by the same total order used for string keys, applying the same
// field writes in any order on any node converges to the same map without a
// coordinator.
type lwwHash struct {
	fields map[string]hashField
}

// hashField is one field's winning write: its stamp, the value (when it is a
// set), and whether that winning write was a delete.
type hashField struct {
	stamp   hlcStamp
	value   string
	deleted bool
}

func newLWWHash() *lwwHash {
	return &lwwHash{fields: make(map[string]hashField)}
}

// set records a set of field=value at stamp st, winning only if st is strictly
// newer than the field's current stamp. Returns whether it was applied.
func (h *lwwHash) set(field, value string, st hlcStamp) bool {
	if cur, ok := h.fields[field]; !ok || st.After(cur.stamp) {
		h.fields[field] = hashField{stamp: st, value: value}
		return true
	}
	return false
}

// del records a delete of field at stamp st as a tombstone, winning only if st
// is strictly newer than the field's current stamp. A delete of an unseen field
// still records a tombstone, so a concurrent older set cannot resurrect it.
// Returns whether it was applied.
func (h *lwwHash) del(field string, st hlcStamp) bool {
	if cur, ok := h.fields[field]; !ok || st.After(cur.stamp) {
		h.fields[field] = hashField{stamp: st, deleted: true}
		return true
	}
	return false
}

// get returns the field's value if its winning write is a set.
func (h *lwwHash) get(field string) (string, bool) {
	f, ok := h.fields[field]
	if !ok || f.deleted {
		return "", false
	}
	return f.value, true
}
