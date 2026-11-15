package valuelog

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestAppendReadAcrossRotation(t *testing.T) {
	l, err := Open(t.TempDir(), 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	refs := map[string]Ref{}
	for i := 0; i < 200; i++ {
		k := fmt.Sprintf("key-%d", i)
		ref, err := l.Append([]byte(k), bytes.Repeat([]byte{byte(i)}, 100+i))
		if err != nil {
			t.Fatal(err)
		}
		refs[k] = ref
	}
	if l.Stats().Files < 5 {
		t.Fatalf("expected rotation into several files, got %d", l.Stats().Files)
	}
	for i := 0; i < 200; i++ {
		k := fmt.Sprintf("key-%d", i)
		v, err := l.Read(refs[k], []byte(k))
		if err != nil {
			t.Fatalf("Read %s: %v", k, err)
		}
		if !bytes.Equal(v, bytes.Repeat([]byte{byte(i)}, 100+i)) {
			t.Fatalf("Read %s returned the wrong value", k)
		}
	}
	// The wrong key for a reference is a corrupt read, never another value.
	if _, err := l.Read(refs["key-1"], []byte("key-2")); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Read with the wrong key: err = %v, want ErrCorrupt", err)
	}
}

func TestCorruptRecordFailsRead(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	ref, _ := l.Append([]byte("k"), []byte("value"))
	l.Flush()
	f, err := os.OpenFile(filepath.Join(dir, fmt.Sprintf("%08d%s", ref.File, fileExt)), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	f.WriteAt([]byte("X"), ref.Off+headerSize+1+2) // flip a value byte
	f.Close()
	if _, err := l.Read(ref, []byte("k")); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Read of a corrupted record: err = %v, want ErrCorrupt", err)
	}
}

func TestOpenDiscardsOldFiles(t *testing.T) {
	dir := t.TempDir()
	l, _ := Open(dir, 0)
	l.Append([]byte("k"), []byte("v"))
	l.Close()
	l2, err := Open(dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()
	if st := l2.Stats(); st.Files != 1 || st.DiskBytes != 0 {
		t.Fatalf("after reopen: %+v, want one empty file", st)
	}
}

// TestCollectMovesLiveRecords: garbage collection hands every live record of a
// mostly-dead file to the relocator, deletes the file, and the moved records
// stay readable at their new references.
func TestCollectMovesLiveRecords(t *testing.T) {
	l, err := Open(t.TempDir(), 2048)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	index := map[string]Ref{} // the "store": key -> current ref
	var mu sync.Mutex
	for i := 0; i < 60; i++ {
		k := fmt.Sprintf("k%02d", i)
		ref, _ := l.Append([]byte(k), bytes.Repeat([]byte("v"), 200))
		index[k] = ref
	}
	// Overwrite most keys: their old records die.
	for i := 0; i < 60; i++ {
		if i%5 == 0 {
			continue
		}
		k := fmt.Sprintf("k%02d", i)
		l.Release(index[k])
		ref, _ := l.Append([]byte(k), bytes.Repeat([]byte("w"), 200))
		index[k] = ref
	}
	before := l.Stats()

	moved := 0
	reclaimed, err := l.Collect(0.5, func(key []byte, old Ref, value []byte) bool {
		mu.Lock()
		defer mu.Unlock()
		if index[string(key)] != old {
			return false // dead record
		}
		ref, err := l.Append(key, value)
		if err != nil {
			t.Fatal(err)
		}
		l.Release(old)
		index[string(key)] = ref
		moved++
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
	after := l.Stats()
	t.Logf("before %+v, after %+v, reclaimed %d bytes, moved %d records", before, after, reclaimed, moved)
	if reclaimed == 0 || after.DiskBytes >= before.DiskBytes {
		t.Fatalf("Collect reclaimed nothing: before %+v after %+v", before, after)
	}
	for k, ref := range index {
		v, err := l.Read(ref, []byte(k))
		if err != nil {
			t.Fatalf("%s unreadable after Collect: %v", k, err)
		}
		want := byte('w')
		var n int
		fmt.Sscanf(k, "k%d", &n)
		if n%5 == 0 {
			want = 'v'
		}
		if len(v) != 200 || v[0] != want {
			t.Fatalf("%s has the wrong value after Collect", k)
		}
	}
}

func TestConcurrentAppendAndRead(t *testing.T) {
	l, err := Open(t.TempDir(), 1<<16)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				k := []byte(fmt.Sprintf("w%d-%d", w, i))
				v := bytes.Repeat([]byte{byte(w)}, i%300)
				ref, err := l.Append(k, v)
				if err != nil {
					t.Error(err)
					return
				}
				got, err := l.Read(ref, k)
				if err != nil || !bytes.Equal(got, v) {
					t.Errorf("read back %s: %v", k, err)
					return
				}
			}
		}(w)
	}
	wg.Wait()
}
