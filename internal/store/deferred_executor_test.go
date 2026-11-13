package store

import (
	"strings"
	"testing"
	"time"
)

// TestDeferredExecutorHook verifies that an installed executor receives the due
// command with templates expanded exactly once, and that the scheduler's local
// execution is bypassed (the hook owns the effect).
func TestDeferredExecutorHook(t *testing.T) {
	s := createTestStore()
	defer s.Stop()

	type call struct {
		cmdType, key string
		args         []string
	}
	var calls []call
	s.SetDeferredExecutor(func(cmdType, key string, args []string) {
		calls = append(calls, call{cmdType, key, args})
	})

	cmd := &DeferredCommand{
		ID:          "t1",
		ExecuteAt:   time.Now(),
		CommandType: "SET",
		Key:         "k:__rand_int__",
		Args:        []string{"v:__rand_int__"},
	}
	s.deferredScheduler.executeCommand(cmd)

	if len(calls) != 1 {
		t.Fatalf("executor called %d times, want 1", len(calls))
	}
	c := calls[0]
	if c.cmdType != "SET" {
		t.Fatalf("cmdType = %q, want SET", c.cmdType)
	}
	if strings.Contains(c.key, "__rand_int__") || len(c.args) != 1 || strings.Contains(c.args[0], "__rand_int__") {
		t.Fatalf("templates not expanded before the hook: key=%q args=%v", c.key, c.args)
	}
	// Local execution was bypassed: the store must not hold the key.
	if _, ok := s.Get(c.key); ok {
		t.Fatalf("local execution ran despite the executor hook")
	}

	// Removing the hook restores local execution.
	s.SetDeferredExecutor(nil)
	s.deferredScheduler.executeCommand(&DeferredCommand{CommandType: "SET", Key: "local", Args: []string{"v"}})
	if v, ok := s.Get("local"); !ok || v != "v" {
		t.Fatalf("local execution after removing hook: (%q,%v), want (v,true)", v, ok)
	}
}
