package store

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
