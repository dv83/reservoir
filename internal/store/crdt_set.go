package store

// lwwSet is a last-write-wins element set CRDT: each element carries the stamp
// of its most recent add and its most recent remove. An element is a member iff
// it has an add that is not superseded by a strictly-newer remove ("add-wins"
// bias on the exact-tie boundary). Applying the same adds/removes in any order
// on any node yields the same membership, so replicas converge without a
// coordinator.
type lwwSet struct {
	adds    map[string]hlcStamp
	removes map[string]hlcStamp
}

func newLWWSet() *lwwSet {
	return &lwwSet{
		adds:    make(map[string]hlcStamp),
		removes: make(map[string]hlcStamp),
	}
}

// add records an add of elem at stamp st, keeping only the newest add stamp.
// Returns whether elem became (or stayed) a member as a result.
func (s *lwwSet) add(elem string, st hlcStamp) {
	if cur, ok := s.adds[elem]; !ok || st.After(cur) {
		s.adds[elem] = st
	}
}

// remove records a remove of elem at stamp st, keeping only the newest remove.
func (s *lwwSet) remove(elem string, st hlcStamp) {
	if cur, ok := s.removes[elem]; !ok || st.After(cur) {
		s.removes[elem] = st
	}
}

// contains reports current membership under the add-wins rule: present iff an
// add exists and no remove is strictly newer than it.
func (s *lwwSet) contains(elem string) bool {
	a, ok := s.adds[elem]
	if !ok {
		return false
	}
	if r, rok := s.removes[elem]; rok && r.After(a) {
		return false
	}
	return true
}

// members returns the current member elements.
func (s *lwwSet) members() []string {
	out := make([]string, 0, len(s.adds))
	for elem := range s.adds {
		if s.contains(elem) {
			out = append(out, elem)
		}
	}
	return out
}

// card returns the number of current members.
func (s *lwwSet) card() int {
	n := 0
	for elem := range s.adds {
		if s.contains(elem) {
			n++
		}
	}
	return n
}
