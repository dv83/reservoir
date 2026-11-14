package store

import (
	"strconv"
	"testing"
	"time"
)

// TestStringWritesLogTheirTTL (task 1.1): a string write that leaves the key with
// a TTL must log the value followed by an EXPIRE carrying the key's absolute
// deadline (recovery replays the value as a TTL-clearing SET), and APPEND and
// GETSET must be logged at all.
func TestStringWritesLogTheirTTL(t *testing.T) {
	deadline := time.Now().Add(time.Hour)
	want := func(op, key, value string) recordedWrite { return recordedWrite{op: op, key: key, value: value} }
	expire := want("EXPIRE", "", strconv.FormatInt(deadline.UnixNano(), 10))

	cases := []struct {
		name  string
		setup func(s *LockFreeStore)
		write func(s *LockFreeStore)
		want  []recordedWrite
	}{
		{
			name:  "SET KEEPTTL",
			setup: func(s *LockFreeStore) { s.Set("k", "a"); s.SetExpiry("k", deadline) },
			write: func(s *LockFreeStore) { s.SetWithOptions("k", "b", SetOptions{KeepTTL: true}) },
			want:  []recordedWrite{want("SET", "k", "b"), expire},
		},
		{
			name:  "APPEND on a key with a TTL",
			setup: func(s *LockFreeStore) { s.Set("k", "a"); s.SetExpiry("k", deadline) },
			write: func(s *LockFreeStore) { s.Append("k", "b") },
			want:  []recordedWrite{want("SET", "k", "ab"), expire},
		},
		{
			name:  "APPEND without a TTL",
			setup: func(s *LockFreeStore) { s.Set("k", "a") },
			write: func(s *LockFreeStore) { s.Append("k", "b") },
			want:  []recordedWrite{want("SET", "k", "ab")},
		},
		{
			name:  "INCR on a key with a TTL",
			setup: func(s *LockFreeStore) { s.Set("k", "1"); s.SetExpiry("k", deadline) },
			write: func(s *LockFreeStore) { s.Incr("k") },
			want:  []recordedWrite{want("INCRBY", "k", "2"), expire},
		},
		{
			name:  "GETSET discards the TTL",
			setup: func(s *LockFreeStore) { s.Set("k", "a"); s.SetExpiry("k", deadline) },
			write: func(s *LockFreeStore) { s.GetSet("k", "b") },
			want:  []recordedWrite{want("SET", "k", "b")},
		},
		{
			name:  "plain SET clears the TTL",
			setup: func(s *LockFreeStore) { s.Set("k", "a"); s.SetExpiry("k", deadline) },
			write: func(s *LockFreeStore) { s.Set("k", "b") },
			want:  []recordedWrite{want("SET", "k", "b")},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := createTestStore()
			defer s.Stop()
			fake := &fakeCommitLog{}
			s.SetCommitLog(fake)
			c.setup(s)
			fake.writes = nil
			c.write(s)

			got := fake.writes
			if len(got) != len(c.want) {
				t.Fatalf("logged %+v, want %+v", got, c.want)
			}
			for i, w := range c.want {
				if w.key == "" {
					w.key = "k"
				}
				if got[i] != w {
					t.Fatalf("entry %d = %+v, want %+v (all: %+v)", i, got[i], w, got)
				}
			}
		})
	}
}

// TestSetEXLogsDeadline: SET with EX logs the value and the new absolute deadline.
func TestSetEXLogsDeadline(t *testing.T) {
	s := createTestStore()
	defer s.Stop()
	fake := &fakeCommitLog{}
	s.SetCommitLog(fake)

	before := time.Now()
	s.SetWithOptions("k", "v", SetOptions{SetTTL: true, TTL: time.Hour})

	if len(fake.writes) != 2 || fake.writes[0] != (recordedWrite{"SET", "k", "v"}) || fake.writes[1].op != "EXPIRE" {
		t.Fatalf("logged %+v, want SET then EXPIRE", fake.writes)
	}
	d, err := strconv.ParseInt(fake.writes[1].value, 10, 64)
	if err != nil {
		t.Fatalf("EXPIRE payload %q: %v", fake.writes[1].value, err)
	}
	if got := time.Unix(0, d).Sub(before); got < 59*time.Minute || got > 61*time.Minute {
		t.Fatalf("logged deadline is %v ahead, want about 1h", got)
	}
}
