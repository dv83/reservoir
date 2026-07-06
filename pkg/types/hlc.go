package types

import (
	"sync"
	"time"
)

// HLCTimestamp is a hybrid logical clock timestamp. Physical is a wall-clock
// component (unix nanoseconds) and Logical is a counter that disambiguates
// events sharing the same physical instant. Timestamps are totally ordered
// (Physical, then Logical, then the originating node id as a final tiebreak,
// applied by callers), track causality, and stay close to real time — which is
// what last-write-wins conflict resolution needs.
type HLCTimestamp struct {
	Physical int64  `json:"p"`
	Logical  uint32 `json:"l"`
}

// Compare returns -1, 0, or 1 as a orders before, equal to, or after b.
func (a HLCTimestamp) Compare(b HLCTimestamp) int {
	switch {
	case a.Physical != b.Physical:
		if a.Physical < b.Physical {
			return -1
		}
		return 1
	case a.Logical != b.Logical:
		if a.Logical < b.Logical {
			return -1
		}
		return 1
	default:
		return 0
	}
}

// After reports whether a is strictly greater than b.
func (a HLCTimestamp) After(b HLCTimestamp) bool { return a.Compare(b) > 0 }

// IsZero reports whether the timestamp is the zero value (no write recorded).
func (a HLCTimestamp) IsZero() bool { return a.Physical == 0 && a.Logical == 0 }

// HLC is a hybrid logical clock. It is safe for concurrent use.
type HLC struct {
	mu   sync.Mutex
	last HLCTimestamp
	// now returns the current physical time in unix nanoseconds. Injectable so
	// tests can drive it deterministically.
	now func() int64
}

// NewHLC creates a hybrid logical clock backed by the wall clock.
func NewHLC() *HLC {
	return &HLC{now: func() int64 { return time.Now().UnixNano() }}
}

// newHLCWithClock is used by tests to supply a deterministic physical clock.
func newHLCWithClock(now func() int64) *HLC {
	return &HLC{now: now}
}

// Now returns a fresh timestamp for a locally-originated event. The physical
// component never goes backwards; when physical time has not advanced past the
// last observed instant, the logical counter is incremented instead.
func (h *HLC) Now() HLCTimestamp {
	h.mu.Lock()
	defer h.mu.Unlock()

	pt := h.now()
	if pt > h.last.Physical {
		h.last = HLCTimestamp{Physical: pt, Logical: 0}
	} else {
		h.last.Logical++
	}
	return h.last
}

// Update merges a timestamp received from another node and returns a fresh local
// timestamp that is strictly greater than both the local clock and the received
// one (standard HLC receive rule). Call this whenever a remote timestamp is
// observed so the local clock tracks the whole cluster's progress.
func (h *HLC) Update(remote HLCTimestamp) HLCTimestamp {
	h.mu.Lock()
	defer h.mu.Unlock()

	pt := h.now()
	prev := h.last

	// The new physical component is the maximum of local, remote, and wall time.
	maxPhysical := prev.Physical
	if remote.Physical > maxPhysical {
		maxPhysical = remote.Physical
	}
	if pt > maxPhysical {
		maxPhysical = pt
	}

	var logical uint32
	switch {
	case maxPhysical == prev.Physical && maxPhysical == remote.Physical:
		logical = prev.Logical
		if remote.Logical > logical {
			logical = remote.Logical
		}
		logical++
	case maxPhysical == prev.Physical:
		logical = prev.Logical + 1
	case maxPhysical == remote.Physical:
		logical = remote.Logical + 1
	default:
		logical = 0
	}

	h.last = HLCTimestamp{Physical: maxPhysical, Logical: logical}
	return h.last
}
