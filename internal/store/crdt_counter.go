package store

import (
	"sync/atomic"

	pkgErrors "reservoir/pkg/errors"
)

// pnCounter is a positive-negative counter CRDT. Each node accumulates its own
// increments in P[origin] and its own decrements in N[origin]; both are
// monotonically non-decreasing, so two replicas merge by taking the element-wise
// maximum of every slot. The visible value is Base + ΣP − ΣN.
//
// Base carries a value seeded from a plain SET when a string key is first used
// as a counter, so INCR on a SET-created key keeps Redis semantics. Base merges
// by max, which converges exactly when the same base is seeded on every node
// (the common case: one SET replicated everywhere). A decreasing SET-reset that
// races live increments is the one boundary this does not model precisely — that
// path stays last-write-wins on the string side.
//
// Because each op replicates the whole {Base,P,N} state and merge is max, the
// result is independent of delivery order and never loses a concurrent
// increment — the property the previous "replicate the computed result" path
// could not provide.
type pnCounter struct {
	Base int64
	P    map[uint64]int64
	N    map[uint64]int64
}

func newPNCounter() *pnCounter {
	return &pnCounter{P: make(map[uint64]int64), N: make(map[uint64]int64)}
}

// add applies a signed delta from origin: a positive delta accrues to that
// node's increment slot, a negative one to its decrement slot. Both slots only
// grow, preserving the monotonicity that makes max-merge correct.
func (c *pnCounter) add(origin uint64, delta int64) {
	if delta >= 0 {
		c.P[origin] += delta
	} else {
		c.N[origin] += -delta
	}
}

// value returns the current counter value: Base + ΣP − ΣN.
func (c *pnCounter) value() int64 {
	v := c.Base
	for _, p := range c.P {
		v += p
	}
	for _, n := range c.N {
		v -= n
	}
	return v
}

// mergeFrom folds another replica's state in by taking the element-wise maximum
// of Base and every per-node slot. Merge is commutative, associative, and
// idempotent, so replicas converge regardless of order or duplication.
func (c *pnCounter) mergeFrom(o *pnCounter) {
	if o.Base > c.Base {
		c.Base = o.Base
	}
	for k, v := range o.P {
		if v > c.P[k] {
			c.P[k] = v
		}
	}
	for k, v := range o.N {
		if v > c.N[k] {
			c.N[k] = v
		}
	}
}

// clone returns a deep copy, so a snapshot handed to replication or anti-entropy
// cannot be mutated by later local ops.
func (c *pnCounter) clone() *pnCounter {
	cp := &pnCounter{Base: c.Base, P: make(map[uint64]int64, len(c.P)), N: make(map[uint64]int64, len(c.N))}
	for k, v := range c.P {
		cp.P[k] = v
	}
	for k, v := range c.N {
		cp.N[k] = v
	}
	return cp
}

// memoryEstimate approximates the counter's heap footprint (a key + value pair
// per contributing node in each of the two maps) for memory accounting.
func (c *pnCounter) memoryEstimate() int64 {
	return int64(len(c.P)+len(c.N)) * 16 // uint64 key + int64 value per entry
}

// CounterState is the exported snapshot of a PN-counter exchanged across the
// store boundary (replication and anti-entropy). Applying it via MergeCounter is
// an idempotent, order-independent max-merge.
type CounterState struct {
	Base int64
	P    map[uint64]int64
	N    map[uint64]int64
}

// counterFor returns the pnCounter to update for a counter op on storedValue,
// creating one (and seeding Base from an existing plain-int string) when the key
// is not yet a counter. It reports WRONGTYPE for a non-string value and
// ErrNotInteger for a non-numeric string. The caller holds the shard lock.
func counterFor(storedValue *StoredValue, exists bool) (*pnCounter, error) {
	if exists && storedValue != nil {
		if !storedValue.IsString() {
			return nil, pkgErrors.ErrWrongType
		}
		if storedValue.counter != nil {
			return storedValue.counter, nil
		}
		ctr := newPNCounter()
		if storedValue.StringVal != "" {
			base, err := parseIntegerStrict(storedValue.StringVal)
			if err != nil {
				return nil, pkgErrors.ErrNotInteger
			}
			ctr.Base = base
		}
		return ctr, nil
	}
	return newPNCounter(), nil
}

// storeCounter writes a materialized counter value back under key, replacing the
// stored value with a string carrying the counter state, updating memory
// accounting, and appending an (absolute) INCRBY record to the commit log for
// recovery. The caller holds the shard lock.
func (s *LockFreeStore) storeCounter(shard *LockFreeShard, key string, ctr *pnCounter, exists bool, old *StoredValue) {
	newValueStr := formatInt64Fast(ctr.value())
	var oldMem int64
	if exists && old != nil {
		oldMem = old.MemoryUsage()
	}
	newSV := &StoredValue{
		Type:       ValueTypeString,
		StringVal:  newValueStr,
		counter:    ctr,
		memorySize: int64(len(key)+len(newValueStr)) + ctr.memoryEstimate(),
	}
	shard.data[key] = newSV

	if exists {
		s.accountMemoryDelta(shard, newSV.memorySize-oldMem)
	} else {
		s.accountKeyAdded(shard, newSV.memorySize)
	}

	if atomic.LoadUint32(&s.recoveryActive) == 0 {
		if cl, ok := s.commitLog.(CommitLogger); ok && cl != nil {
			cl.Write("INCRBY", []byte(key), []byte(newValueStr))
		}
	}
}

// CounterState returns a snapshot of the key's PN-counter for replication, or
// ok=false if the key is not (yet) a counter.
func (s *LockFreeStore) CounterState(key string) (CounterState, bool) {
	shard := s.getShard(key)
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	v, ok := shard.data[key]
	if !ok || v == nil || v.counter == nil {
		return CounterState{}, false
	}
	c := v.counter.clone()
	return CounterState{Base: c.Base, P: c.P, N: c.N}, true
}

// MergeCounter folds a replicated PN-counter snapshot into the local counter
// (creating it, seeding Base from an existing plain string, if needed) and
// returns the materialized value. Merge is max-based, so applying the same
// snapshot repeatedly or out of order converges without double-counting.
func (s *LockFreeStore) MergeCounter(key string, st CounterState) (int64, error) {
	shard := s.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	storedValue, exists := shard.data[key]
	ctr, err := counterFor(storedValue, exists)
	if err != nil {
		return 0, err
	}
	ctr.mergeFrom(&pnCounter{Base: st.Base, P: st.P, N: st.N})
	s.storeCounter(shard, key, ctr, exists, storedValue)
	return ctr.value(), nil
}
