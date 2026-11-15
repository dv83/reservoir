package store

import "unsafe"

// Incremental payload sizes for sets and hashes. MemoryUsage used to walk every
// element on each call, and every write calls it, so a write to a container of
// n elements cost O(n) and filling one was quadratic. Each container (and its
// CRDT) now keeps a byte count that the helpers below update as elements
// change; every mutation of these maps must go through them.

var stampBytes = int64(unsafe.Sizeof(hlcStamp{}))

// putElem adds elem to the materialized members if absent. The caller holds sv.mu.
func (sv *SetValue) putElem(elem string) bool {
	if _, ok := sv.Elements[elem]; ok {
		return false
	}
	sv.Elements[elem] = struct{}{}
	sv.bytes += int64(len(elem))
	return true
}

// dropElem removes elem from the materialized members if present. The caller
// holds sv.mu.
func (sv *SetValue) dropElem(elem string) bool {
	if _, ok := sv.Elements[elem]; !ok {
		return false
	}
	delete(sv.Elements, elem)
	sv.bytes -= int64(len(elem))
	return true
}

// resetElems empties the materialized members.
func (sv *SetValue) resetElems(capacity int) {
	sv.Elements = make(map[string]struct{}, capacity)
	sv.bytes = 0
	sv.totalSize = 0
}

func (s *lwwSet) putAdd(elem string, st hlcStamp) {
	if _, ok := s.adds[elem]; !ok {
		s.bytes += int64(len(elem)) + stampBytes
	}
	s.adds[elem] = st
}

func (s *lwwSet) putRemove(elem string, st hlcStamp) {
	if _, ok := s.removes[elem]; !ok {
		s.bytes += int64(len(elem)) + stampBytes
	}
	s.removes[elem] = st
}

func (s *lwwSet) dropAdd(elem string) {
	if _, ok := s.adds[elem]; ok {
		delete(s.adds, elem)
		s.bytes -= int64(len(elem)) + stampBytes
	}
}

func (s *lwwSet) dropRemove(elem string) {
	if _, ok := s.removes[elem]; ok {
		delete(s.removes, elem)
		s.bytes -= int64(len(elem)) + stampBytes
	}
}

// putField sets field to value in the materialized fields and reports whether
// the field is new. The caller holds hv.mu.
func (hv *HashValue) putField(field, value string) bool {
	old, existed := hv.Fields[field]
	if existed {
		hv.bytes -= int64(len(field) + len(old))
	}
	hv.Fields[field] = value
	hv.bytes += int64(len(field) + len(value))
	return !existed
}

// dropField removes field from the materialized fields if present.
func (hv *HashValue) dropField(field string) bool {
	old, ok := hv.Fields[field]
	if !ok {
		return false
	}
	delete(hv.Fields, field)
	hv.bytes -= int64(len(field) + len(old))
	return true
}

// resetFields empties the materialized fields.
func (hv *HashValue) resetFields(capacity int) {
	hv.Fields = make(map[string]string, capacity)
	hv.bytes = 0
}

func (h *lwwHash) putField(field string, f hashField) {
	if old, ok := h.fields[field]; ok {
		h.bytes -= int64(len(field)+len(old.value)) + stampBytes
	}
	h.fields[field] = f
	h.bytes += int64(len(field)+len(f.value)) + stampBytes
}

func (h *lwwHash) dropField(field string) {
	if old, ok := h.fields[field]; ok {
		delete(h.fields, field)
		h.bytes -= int64(len(field)+len(old.value)) + stampBytes
	}
}

// recountBytes returns the payload sizes by walking the maps: the reference the
// incremental counts are checked against in tests.
func (sv *SetValue) recountBytes() (elems, crdt int64) {
	for e := range sv.Elements {
		elems += int64(len(e))
	}
	if sv.crdt != nil {
		for e := range sv.crdt.adds {
			crdt += int64(len(e)) + stampBytes
		}
		for e := range sv.crdt.removes {
			crdt += int64(len(e)) + stampBytes
		}
	}
	return elems, crdt
}

func (hv *HashValue) recountBytes() (fields, crdt int64) {
	for f, v := range hv.Fields {
		fields += int64(len(f) + len(v))
	}
	if hv.crdt != nil {
		for f, fl := range hv.crdt.fields {
			crdt += int64(len(f)+len(fl.value)) + stampBytes
		}
	}
	return fields, crdt
}
