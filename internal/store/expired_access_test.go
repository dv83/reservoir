package store

import (
	"reflect"
	"sort"
	"testing"
	"time"
)

// expireNow moves key's deadline into the past without waiting for the clock
// and without letting the background reaper run, so the key is expired but
// still physically present — the window this change is about.
func expireNow(t *testing.T, s *LockFreeStore, key string) {
	t.Helper()
	shard := s.getShard(key)
	shard.mu.RLock()
	_, ok := shard.data[key]
	shard.mu.RUnlock()
	if !ok {
		t.Fatalf("expireNow: %q does not exist", key)
	}
	shard.expiryMu.Lock()
	shard.expiry[key] = time.Now().Add(-time.Second)
	shard.expiryMu.Unlock()
}

func present(s *LockFreeStore, key string) bool {
	shard := s.getShard(key)
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	_, ok := shard.data[key]
	return ok
}

// TestExpiredKeysBehaveAsMissing (tasks 1.1, 1.2): once a key's deadline has
// passed, every command must treat it as missing — reads reply as for an absent
// key, writes start from an empty key — even before the reaper has reclaimed it.
// The access itself reclaims the key.
func TestExpiredKeysBehaveAsMissing(t *testing.T) {
	type tc struct {
		name  string
		setup func(s *LockFreeStore)
		check func(t *testing.T, s *LockFreeStore)
	}
	set := func(s *LockFreeStore) { s.SAdd("k", "a", "b") }
	hash := func(s *LockFreeStore) { s.HSet("k", "f", "v") }
	list := func(s *LockFreeStore) { s.RPush("k", "a", "b") }
	str := func(s *LockFreeStore) { s.Set("k", "old") }
	num := func(s *LockFreeStore) { s.Set("k", "41") }
	eq := func(t *testing.T, what string, got, want interface{}) {
		t.Helper()
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s = %#v, want %#v", what, got, want)
		}
	}

	cases := []tc{
		// sets
		{"SMEMBERS", set, func(t *testing.T, s *LockFreeStore) { m, _ := s.SMembers("k"); eq(t, "SMEMBERS", len(m), 0) }},
		{"SCARD", set, func(t *testing.T, s *LockFreeStore) { n, _ := s.SCard("k"); eq(t, "SCARD", n, int64(0)) }},
		{"SISMEMBER", set, func(t *testing.T, s *LockFreeStore) { ok, _ := s.SIsMember("k", "a"); eq(t, "SISMEMBER", ok, false) }},
		{"SPOP", set, func(t *testing.T, s *LockFreeStore) { _, ok, _ := s.SPop("k"); eq(t, "SPOP found", ok, false) }},
		{"SREM", set, func(t *testing.T, s *LockFreeStore) { n, _ := s.SRem("k", "a"); eq(t, "SREM", n, int64(0)) }},
		{"SUNION", set, func(t *testing.T, s *LockFreeStore) { m, _ := s.SUnion("k"); eq(t, "SUNION", len(m), 0) }},
		{"SINTER", set, func(t *testing.T, s *LockFreeStore) { m, _ := s.SInter("k"); eq(t, "SINTER", len(m), 0) }},
		{"SDIFF", set, func(t *testing.T, s *LockFreeStore) { m, _ := s.SDiff("k"); eq(t, "SDIFF", len(m), 0) }},
		{"SADD starts a new set", set, func(t *testing.T, s *LockFreeStore) {
			n, _ := s.SAdd("k", "c")
			eq(t, "SADD", n, int64(1))
			m, _ := s.SMembers("k")
			eq(t, "members", m, []string{"c"})
			_, ttl := s.GetTTL("k")
			eq(t, "has TTL", ttl, false)
		}},
		// hashes
		{"HGET", hash, func(t *testing.T, s *LockFreeStore) { _, ok, _ := s.HGet("k", "f"); eq(t, "HGET found", ok, false) }},
		{"HGETALL", hash, func(t *testing.T, s *LockFreeStore) { m, _ := s.HGetAll("k"); eq(t, "HGETALL", len(m), 0) }},
		{"HLEN", hash, func(t *testing.T, s *LockFreeStore) { n, _ := s.HLen("k"); eq(t, "HLEN", n, int64(0)) }},
		{"HEXISTS", hash, func(t *testing.T, s *LockFreeStore) { ok, _ := s.HExists("k", "f"); eq(t, "HEXISTS", ok, false) }},
		{"HKEYS", hash, func(t *testing.T, s *LockFreeStore) { m, _ := s.HKeys("k"); eq(t, "HKEYS", len(m), 0) }},
		{"HVALS", hash, func(t *testing.T, s *LockFreeStore) { m, _ := s.HVals("k"); eq(t, "HVALS", len(m), 0) }},
		{"HMGET", hash, func(t *testing.T, s *LockFreeStore) {
			_, found, _ := s.HMGet("k", "f")
			eq(t, "HMGET found", found, []bool{false})
		}},
		{"HDEL", hash, func(t *testing.T, s *LockFreeStore) { n, _ := s.HDel("k", "f"); eq(t, "HDEL", n, int64(0)) }},
		{"HINCRBY starts from zero", func(s *LockFreeStore) { s.HSet("k", "n", "10") }, func(t *testing.T, s *LockFreeStore) {
			n, _ := s.HIncrBy("k", "n", 1)
			eq(t, "HINCRBY", n, int64(1))
		}},
		{"HSET starts a new hash", hash, func(t *testing.T, s *LockFreeStore) {
			s.HSet("k", "g", "w")
			n, _ := s.HLen("k")
			eq(t, "HLEN", n, int64(1))
		}},
		// lists
		{"LRANGE", list, func(t *testing.T, s *LockFreeStore) { m, _ := s.LRange("k", 0, -1); eq(t, "LRANGE", len(m), 0) }},
		{"LLEN", list, func(t *testing.T, s *LockFreeStore) { n, _ := s.LLen("k"); eq(t, "LLEN", n, int64(0)) }},
		{"LINDEX", list, func(t *testing.T, s *LockFreeStore) { _, ok, _ := s.LIndex("k", 0); eq(t, "LINDEX found", ok, false) }},
		{"LPOP", list, func(t *testing.T, s *LockFreeStore) { m, _ := s.LPop("k", 1); eq(t, "LPOP", len(m), 0) }},
		{"RPOP", list, func(t *testing.T, s *LockFreeStore) { m, _ := s.RPop("k", 1); eq(t, "RPOP", len(m), 0) }},
		{"LPUSHX", list, func(t *testing.T, s *LockFreeStore) { n, _ := s.LPushX("k", "x"); eq(t, "LPUSHX", n, int64(0)) }},
		{"RPUSHX", list, func(t *testing.T, s *LockFreeStore) { n, _ := s.RPushX("k", "x"); eq(t, "RPUSHX", n, int64(0)) }},
		{"LSET", list, func(t *testing.T, s *LockFreeStore) {
			if err := s.LSet("k", 0, "x"); err == nil {
				t.Errorf("LSET on an expired list succeeded, want no-such-key error")
			}
		}},
		{"RPUSH starts a new list", list, func(t *testing.T, s *LockFreeStore) {
			n, _ := s.RPush("k", "c")
			eq(t, "RPUSH", n, int64(1))
		}},
		{"LPUSHUNIQUE starts a new list", list, func(t *testing.T, s *LockFreeStore) {
			n, _ := s.LPushUnique("k", "a")
			eq(t, "LPUSHUNIQUE", n, int64(1))
		}},
		// strings and keyspace
		{"STRLEN", str, func(t *testing.T, s *LockFreeStore) { n, _ := s.StrLen("k"); eq(t, "STRLEN", n, int64(0)) }},
		{"EXISTS", str, func(t *testing.T, s *LockFreeStore) { eq(t, "EXISTS", s.Exists("k"), int64(0)) }},
		{"DEL", str, func(t *testing.T, s *LockFreeStore) { eq(t, "DEL", s.Delete("k"), false) }},
		{"EXPIRE", str, func(t *testing.T, s *LockFreeStore) {
			eq(t, "EXPIRE", s.SetExpiry("k", time.Now().Add(time.Hour)), false)
		}},
		{"APPEND starts from empty", str, func(t *testing.T, s *LockFreeStore) {
			n, _ := s.Append("k", "x")
			eq(t, "APPEND", n, int64(1))
		}},
		{"GETSET", str, func(t *testing.T, s *LockFreeStore) {
			_, ok, _ := s.GetSet("k", "new")
			eq(t, "GETSET found", ok, false)
		}},
		{"INCR starts from zero", num, func(t *testing.T, s *LockFreeStore) {
			n, _ := s.Incr("k")
			eq(t, "INCR", n, int64(1))
		}},
		{"SET NX", str, func(t *testing.T, s *LockFreeStore) {
			ok, _ := s.SetWithOptions("k", "new", SetOptions{NX: true})
			eq(t, "SET NX applied", ok, true)
		}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := createTestStore()
			defer s.Stop()
			c.setup(s)
			if !s.SetExpiry("k", time.Now().Add(time.Hour)) {
				t.Fatal("setup: EXPIRE failed")
			}
			expireNow(t, s, "k")
			c.check(t, s)
		})
	}
}

// TestExpiredKeyIsReclaimedOnAccess (task 1.2): reading an expired container
// removes it right away, releasing its memory without waiting for the reaper.
func TestExpiredKeyIsReclaimedOnAccess(t *testing.T) {
	s := createTestStore()
	defer s.Stop()
	s.SAdd("k", "a", "b")
	s.SetExpiry("k", time.Now().Add(time.Hour))
	expireNow(t, s, "k")

	s.SCard("k")
	if present(s, "k") {
		t.Fatal("expired set still stored after SCARD")
	}
	if _, has := s.GetTTL("k"); has {
		t.Fatal("expired key still has a TTL record")
	}
}

// TestLiveKeysUnaffected guards the fast path: a key whose deadline is in the
// future is served normally and keeps its TTL.
func TestLiveKeysUnaffected(t *testing.T) {
	s := createTestStore()
	defer s.Stop()
	s.SAdd("k", "a")
	s.SetExpiry("k", time.Now().Add(time.Hour))
	if n, _ := s.SCard("k"); n != 1 {
		t.Fatalf("SCARD = %d, want 1", n)
	}
	if _, has := s.GetTTL("k"); !has {
		t.Fatal("live key lost its TTL")
	}
}

// TestRecoveryDoesNotExpireLazily: replay rebuilds state as of each logged write,
// so a write replayed onto a key whose logged deadline has since passed must
// build on the key (a later logged EXPIRE may still extend it), not start over.
func TestRecoveryDoesNotExpireLazily(t *testing.T) {
	s := createTestStore()
	defer s.Stop()
	s.SAdd("k", "a")
	s.SetExpiry("k", time.Now().Add(time.Hour))
	expireNow(t, s, "k")

	s.SetRecoveryActive(true)
	s.SAdd("k", "b")
	s.SetExpiry("k", time.Now().Add(time.Hour)) // the later, extending EXPIRE
	s.SetRecoveryActive(false)

	m, _ := s.SMembers("k")
	sort.Strings(m)
	if !reflect.DeepEqual(m, []string{"a", "b"}) {
		t.Fatalf("SMEMBERS after replay = %v, want [a b]", m)
	}
}
