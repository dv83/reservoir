package store

import (
	"encoding/binary"
	"errors"
	"fmt"

	"reservoir/pkg/types"
)

// Binary encoding of a StoredValue for the LSM storage mode (add-lsm-storage).
// It carries everything a key's behavior depends on — the value, the LWW
// stamp, the cached memory size (so accounting stays exact across eviction and
// reload) and every CRDT's state — so a value decoded from disk behaves exactly
// like the one that was encoded.
//
// Not encoded: the CLOCK bit (a cache hint), list deltas still pending
// replication (eviction skips such lists), and tiered-storage references (the
// LSM mode does not tier).
//
// Layout: version, type, flags, memory size, [hlc], then the type's payload.
// Integers are varints; byte strings are length-prefixed.

const codecVersion = 1

const (
	flagHLC     = 1 << 0 // an LWW stamp follows
	flagCounter = 1 << 1 // string: PN-counter state follows the value
	flagCRDT    = 1 << 2 // set/hash/list: CRDT state follows the plain view
)

var errCodec = errors.New("store codec: malformed value")

type encoder struct{ b []byte }

func (e *encoder) u(v uint64)   { e.b = binary.AppendUvarint(e.b, v) }
func (e *encoder) i(v int64)    { e.b = binary.AppendVarint(e.b, v) }
func (e *encoder) str(s string) { e.u(uint64(len(s))); e.b = append(e.b, s...) }
func (e *encoder) byte(v byte)  { e.b = append(e.b, v) }
func (e *encoder) stamp(s hlcStamp) {
	e.i(s.TS.Physical)
	e.u(uint64(s.TS.Logical))
	e.u(s.Origin)
}

type decoder struct {
	b   []byte
	err error
}

func (d *decoder) fail() {
	if d.err == nil {
		d.err = errCodec
	}
}

func (d *decoder) u() uint64 {
	v, n := binary.Uvarint(d.b)
	if n <= 0 {
		d.fail()
		return 0
	}
	d.b = d.b[n:]
	return v
}

func (d *decoder) i() int64 {
	v, n := binary.Varint(d.b)
	if n <= 0 {
		d.fail()
		return 0
	}
	d.b = d.b[n:]
	return v
}

// count reads a collection length, rejecting one that could not fit in the
// remaining bytes (every element takes at least one byte), so a corrupt length
// cannot trigger a huge allocation.
func (d *decoder) count() int {
	n := d.u()
	if n > uint64(len(d.b)) {
		d.fail()
		return 0
	}
	return int(n)
}

func (d *decoder) str() string {
	n := d.u()
	if n > uint64(len(d.b)) {
		d.fail()
		return ""
	}
	s := string(d.b[:n])
	d.b = d.b[n:]
	return s
}

func (d *decoder) byte() byte {
	if len(d.b) == 0 {
		d.fail()
		return 0
	}
	v := d.b[0]
	d.b = d.b[1:]
	return v
}

func (d *decoder) stamp() hlcStamp {
	p := d.i()
	l := d.u()
	o := d.u()
	return hlcStamp{TS: types.HLCTimestamp{Physical: p, Logical: uint32(l)}, Origin: o}
}

// encodeValue returns the binary encoding of sv. The caller holds the key's
// shard lock, so container state is stable.
func encodeValue(sv *StoredValue) ([]byte, error) {
	if sv.cold != nil {
		return nil, fmt.Errorf("store codec: cannot encode a tiered (cold) value")
	}
	e := &encoder{b: make([]byte, 0, 32+len(sv.StringVal))}
	var flags byte
	if !sv.hlc.TS.IsZero() || sv.hlc.Origin != 0 {
		flags |= flagHLC
	}
	switch sv.Type {
	case ValueTypeString:
		if sv.counter != nil {
			flags |= flagCounter
		}
	case ValueTypeSet:
		if sv.SetVal != nil && sv.SetVal.crdt != nil {
			flags |= flagCRDT
		}
	case ValueTypeHash:
		if sv.HashVal != nil && sv.HashVal.crdt != nil {
			flags |= flagCRDT
		}
	case ValueTypeList:
		if sv.ListVal != nil && sv.ListVal.rga != nil {
			flags |= flagCRDT
		}
	default:
		return nil, fmt.Errorf("store codec: unknown value type %d", sv.Type)
	}
	e.byte(codecVersion)
	e.byte(byte(sv.Type))
	e.byte(flags)
	e.i(sv.MemoryUsage())
	if flags&flagHLC != 0 {
		e.stamp(sv.hlc)
	}

	switch sv.Type {
	case ValueTypeString:
		e.str(sv.StringVal)
		if c := sv.counter; c != nil {
			e.i(c.Base)
			e.stamp(c.gen)
			encodeSlots(e, c.P)
			encodeSlots(e, c.N)
		}
	case ValueTypeSet:
		encodeSet(e, sv.SetVal)
	case ValueTypeHash:
		encodeHash(e, sv.HashVal)
	case ValueTypeList:
		encodeList(e, sv.ListVal)
	}
	return e.b, nil
}

func encodeSlots(e *encoder, m map[uint64]int64) {
	e.u(uint64(len(m)))
	for k, v := range m {
		e.u(k)
		e.i(v)
	}
}

func encodeSet(e *encoder, s *SetValue) {
	if s == nil {
		e.u(0)
		return
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	e.u(uint64(len(s.Elements)))
	for m := range s.Elements {
		e.str(m)
	}
	if s.crdt != nil {
		for _, side := range []map[string]hlcStamp{s.crdt.adds, s.crdt.removes} {
			e.u(uint64(len(side)))
			for m, st := range side {
				e.str(m)
				e.stamp(st)
			}
		}
	}
}

func encodeHash(e *encoder, h *HashValue) {
	if h == nil {
		e.u(0)
		return
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	e.u(uint64(len(h.Fields)))
	for f, v := range h.Fields {
		e.str(f)
		e.str(v)
	}
	if h.crdt != nil {
		e.u(uint64(len(h.crdt.fields)))
		for f, fl := range h.crdt.fields {
			e.str(f)
			e.stamp(fl.stamp)
			if fl.deleted {
				e.byte(1)
			} else {
				e.byte(0)
				e.str(fl.value)
			}
		}
	}
}

func encodeList(e *encoder, l *ListValue) {
	if l == nil {
		e.u(0) // capacity
		e.u(0) // elements
		return
	}
	l.mu.RLock()
	vals := l.deque.ToSlice()
	capacity := l.deque.Cap()
	l.mu.RUnlock()
	e.u(uint64(capacity))
	e.u(uint64(len(vals)))
	for _, v := range vals {
		e.str(v)
	}
	if r := l.rga; r != nil {
		e.stamp(l.gen)
		e.u(uint64(len(r.elems)))
		for _, el := range r.elems {
			e.stamp(el.id)
			e.i(el.pos)
			e.str(el.value)
			e.stamp(el.valStamp)
			if el.deleted {
				e.byte(1)
			} else {
				e.byte(0)
			}
			e.i(el.delTime)
		}
	}
}

// decodeValue rebuilds a StoredValue from its encoding.
func decodeValue(b []byte) (*StoredValue, error) {
	d := &decoder{b: b}
	if v := d.byte(); v != codecVersion {
		if d.err != nil {
			return nil, d.err
		}
		return nil, fmt.Errorf("store codec: unsupported version %d", v)
	}
	sv := &StoredValue{Type: ValueType(d.byte())}
	flags := d.byte()
	mem := d.i()
	if flags&flagHLC != 0 {
		sv.hlc = d.stamp()
	}
	switch sv.Type {
	case ValueTypeString:
		sv.StringVal = d.str()
		if flags&flagCounter != 0 {
			c := &pnCounter{Base: d.i()}
			c.gen = d.stamp()
			c.P = decodeSlots(d)
			c.N = decodeSlots(d)
			sv.counter = c
		}
	case ValueTypeSet:
		sv.SetVal = decodeSet(d, flags&flagCRDT != 0)
	case ValueTypeHash:
		sv.HashVal = decodeHash(d, flags&flagCRDT != 0)
	case ValueTypeList:
		sv.ListVal = decodeList(d, flags&flagCRDT != 0)
	default:
		d.fail()
	}
	if d.err == nil && len(d.b) != 0 {
		d.fail() // trailing bytes
	}
	if d.err != nil {
		return nil, d.err
	}
	sv.memorySize = mem
	return sv, nil
}

func decodeSlots(d *decoder) map[uint64]int64 {
	n := d.count()
	m := make(map[uint64]int64, n)
	for i := 0; i < n && d.err == nil; i++ {
		k := d.u()
		m[k] = d.i()
	}
	return m
}

func decodeSet(d *decoder, hasCRDT bool) *SetValue {
	s := NewSetValue()
	n := d.count()
	for i := 0; i < n && d.err == nil; i++ {
		s.Elements[d.str()] = struct{}{}
	}
	s.totalSize = int64(len(s.Elements))
	if hasCRDT {
		s.crdt = newLWWSet()
		for _, side := range []map[string]hlcStamp{s.crdt.adds, s.crdt.removes} {
			n := d.count()
			for i := 0; i < n && d.err == nil; i++ {
				m := d.str()
				side[m] = d.stamp()
			}
		}
	}
	return s
}

func decodeHash(d *decoder, hasCRDT bool) *HashValue {
	h := NewHashValue()
	n := d.count()
	for i := 0; i < n && d.err == nil; i++ {
		f := d.str()
		h.Fields[f] = d.str()
	}
	if hasCRDT {
		h.crdt = newLWWHash()
		n := d.count()
		for i := 0; i < n && d.err == nil; i++ {
			f := d.str()
			fl := hashField{stamp: d.stamp()}
			if d.byte() == 1 {
				fl.deleted = true
			} else {
				fl.value = d.str()
			}
			h.crdt.fields[f] = fl
		}
	}
	return h
}

func decodeList(d *decoder, hasCRDT bool) *ListValue {
	l := NewListValue()
	capacity := d.u()
	n := d.count()
	vals := make([]string, 0, n)
	for i := 0; i < n && d.err == nil; i++ {
		vals = append(vals, d.str())
	}
	if capacity > 1<<31 {
		d.fail()
		capacity = 0
	}
	l.deque = newDequeWithCapacity(vals, int(capacity))
	if hasCRDT {
		l.gen = d.stamp()
		r := newRGAList()
		n := d.count()
		for i := 0; i < n && d.err == nil; i++ {
			el := &rgaElem{id: d.stamp()}
			el.pos = d.i()
			el.value = d.str()
			el.valStamp = d.stamp()
			el.deleted = d.byte() == 1
			el.delTime = d.i()
			r.elems[el.id] = el
		}
		l.rga = r
	}
	return l
}
