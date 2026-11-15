package lsmengine

import (
	"fmt"
	"sync"
	"testing"
)

func openTest(t *testing.T, dir string) *Engine {
	t.Helper()
	e, err := Open(dir, Options{CacheBytes: 8 << 20})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func TestGetSetDelete(t *testing.T) {
	e := openTest(t, t.TempDir())
	defer e.Close()
	b := e.NewBatch()
	b.Set([]byte("a"), []byte("1"))
	b.Set([]byte("b"), []byte("2"))
	if err := e.Commit(b); err != nil {
		t.Fatal(err)
	}
	if v, ok, err := e.Get([]byte("a")); err != nil || !ok || string(v) != "1" {
		t.Fatalf("Get a = (%q, %v, %v)", v, ok, err)
	}
	b = e.NewBatch()
	b.Delete([]byte("a"))
	e.Commit(b)
	if _, ok, _ := e.Get([]byte("a")); ok {
		t.Fatal("a still present after delete")
	}
	if _, ok, _ := e.Get([]byte("missing")); ok {
		t.Fatal("missing key reported present")
	}
}

func TestDiscardedBatchWritesNothing(t *testing.T) {
	e := openTest(t, t.TempDir())
	defer e.Close()
	b := e.NewBatch()
	b.Set([]byte("k"), []byte("v"))
	b.Discard()
	if _, ok, _ := e.Get([]byte("k")); ok {
		t.Fatal("discarded batch was applied")
	}
}

func TestScanPrefix(t *testing.T) {
	e := openTest(t, t.TempDir())
	defer e.Close()
	b := e.NewBatch()
	for _, k := range []string{"v\x01a", "v\x01b", "v\x02a", "v\xff\xffz", "w\x00"} {
		b.Set([]byte(k), []byte(k))
	}
	e.Commit(b)

	scan := func(prefix string) []string {
		var got []string
		if err := e.ScanPrefix([]byte(prefix), func(k, v []byte) bool {
			got = append(got, string(k))
			return true
		}); err != nil {
			t.Fatal(err)
		}
		return got
	}
	if got := fmt.Sprint(scan("v\x01")); got != fmt.Sprint([]string{"v\x01a", "v\x01b"}) {
		t.Fatalf("scan v\\x01 = %q", got)
	}
	if got := scan("v\xff\xff"); len(got) != 1 {
		t.Fatalf("scan of an all-0xff prefix = %q", got)
	}
	if got := scan("v"); len(got) != 4 {
		t.Fatalf("scan v = %q, want 4 keys", got)
	}
	n := 0
	e.ScanPrefix([]byte("v"), func(k, v []byte) bool { n++; return n < 2 })
	if n != 2 {
		t.Fatalf("scan did not stop when fn returned false (n=%d)", n)
	}
}

func TestDataSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	e := openTest(t, dir)
	for i := 0; i < 1000; i++ {
		b := e.NewBatch()
		b.Set([]byte(fmt.Sprintf("k%04d", i)), []byte(fmt.Sprint(i)))
		e.Commit(b)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	e = openTest(t, dir)
	defer e.Close()
	for i := 0; i < 1000; i++ {
		v, ok, _ := e.Get([]byte(fmt.Sprintf("k%04d", i)))
		if !ok || string(v) != fmt.Sprint(i) {
			t.Fatalf("k%04d after reopen = (%q, %v)", i, v, ok)
		}
	}
}

func TestSyncWritesMode(t *testing.T) {
	dir := t.TempDir()
	e, err := Open(dir, Options{SyncWrites: true})
	if err != nil {
		t.Fatal(err)
	}
	b := e.NewBatch()
	b.Set([]byte("k"), []byte("v"))
	if err := e.Commit(b); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentCommits(t *testing.T) {
	e := openTest(t, t.TempDir())
	defer e.Close()
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				b := e.NewBatch()
				k := []byte(fmt.Sprintf("w%d-%d", w, i))
				b.Set(k, k)
				if err := e.Commit(b); err != nil {
					t.Error(err)
					return
				}
				if v, ok, _ := e.Get(k); !ok || string(v) != string(k) {
					t.Errorf("read-your-write failed for %s", k)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	if s := e.Stats(); s.DiskBytes == 0 {
		t.Fatalf("stats report no disk usage: %+v", s)
	}
}
