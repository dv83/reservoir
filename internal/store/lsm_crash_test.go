package store

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"reservoir/internal/lsmengine"
)

// TestLSMCrashKeepsAcknowledgedWrites (add-lsm-storage, task 4.2): a child
// process writes keys in order and reports each acknowledged batch with its
// time; the parent kills it with SIGKILL mid-stream, reopens the engine and
// checks the durability guarantee: every write acknowledged before the last
// WAL sync — more than the 100ms sync interval (plus margin) before the kill —
// is there, with its value. Writes inside that window may be lost: without
// --lsm-sync-writes the engine buffers WAL records in the process, as the
// commit log does in memory mode.
func TestLSMCrashKeepsAcknowledgedWrites(t *testing.T) {
	if dir := os.Getenv("RESERVOIR_LSM_CRASH_CHILD"); dir != "" {
		lsmCrashChild(dir)
		return
	}
	if testing.Short() {
		t.Skip("spawns a child process")
	}
	for _, syncWrites := range []bool{false, true} {
		t.Run(fmt.Sprintf("sync-writes=%v", syncWrites), func(t *testing.T) { lsmCrashRun(t, syncWrites) })
	}
}

// lsmCrashRun runs one crash experiment. With syncWrites every acknowledged
// write must survive; without, those acknowledged before the sync window.
func lsmCrashRun(t *testing.T, syncWrites bool) {
	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestLSMCrashKeepsAcknowledgedWrites$", "-test.count=1")
	cmd.Env = append(os.Environ(), "RESERVOIR_LSM_CRASH_CHILD="+dir)
	if syncWrites {
		cmd.Env = append(cmd.Env, "RESERVOIR_LSM_CRASH_SYNC=1")
	}
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	type ack struct {
		i  int
		at time.Time
	}
	var acks []ack
	sc := bufio.NewScanner(out)
	deadline := time.Now().Add(1500 * time.Millisecond)
	for sc.Scan() {
		if f := strings.Fields(sc.Text()); len(f) == 3 && f[0] == "acked" {
			i, _ := strconv.Atoi(f[1])
			ns, _ := strconv.ParseInt(f[2], 10, 64)
			acks = append(acks, ack{i, time.Unix(0, ns)})
		}
		if time.Now().After(deadline) && len(acks) > 0 {
			break
		}
	}
	killedAt := time.Now()
	cmd.Process.Signal(syscall.SIGKILL)
	cmd.Wait()
	// The guarantee covers writes acknowledged before the last sync — with
	// sync writes, every acknowledged write.
	acked, lastAcked := -1, -1
	for _, a := range acks {
		lastAcked = a.i
		if syncWrites || a.at.Before(killedAt.Add(-150*time.Millisecond)) {
			acked = a.i
		}
	}
	if acked <= 0 {
		t.Fatal("child acknowledged no writes before the sync window")
	}

	s := NewLockFreeStore(&Limits{MaxKeySize: 1024, MaxValueSize: 1 << 20, MaxMemoryUsage: 1 << 30})
	if err := s.EnableLSM(dir, lsmengine.Options{}); err != nil {
		t.Fatalf("reopen after crash: %v", err)
	}
	defer s.Stop()
	for i := 0; i <= acked; i++ {
		if v, ok := s.Get(fmt.Sprintf("k%d", i)); !ok || v != fmt.Sprint(i) {
			t.Fatalf("acknowledged k%d lost in the crash (acked up to %d): (%q, %v)", i, acked, v, ok)
		}
	}
	lost := int64(lastAcked+1) - s.GetKeyCount()
	if lost < 0 {
		lost = 0 // keys written after the last acknowledgement also made it
	}
	t.Logf("all %d guaranteed writes survived SIGKILL; %d acknowledged within the last 150ms were lost", acked+1, lost)
}

// lsmCrashChild writes k0, k1, ... until killed, printing the index of the
// last acknowledged write after every batch.
func lsmCrashChild(dir string) {
	s := NewLockFreeStore(&Limits{MaxKeySize: 1024, MaxValueSize: 1 << 20, MaxMemoryUsage: 1 << 30})
	opts := lsmengine.Options{SyncWrites: os.Getenv("RESERVOIR_LSM_CRASH_SYNC") != ""}
	if err := s.EnableLSM(dir, opts); err != nil {
		fmt.Println("error", err)
		os.Exit(1)
	}
	w := bufio.NewWriter(os.Stdout)
	for i := 0; ; i++ {
		if err := s.Set(fmt.Sprintf("k%d", i), fmt.Sprint(i)); err != nil {
			fmt.Println("error", err)
			os.Exit(1)
		}
		if i%500 == 0 || opts.SyncWrites {
			fmt.Fprintf(w, "acked %d %d\n", i, time.Now().UnixNano())
			w.Flush()
		}
	}
}
