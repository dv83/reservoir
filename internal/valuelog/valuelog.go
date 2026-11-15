// Package valuelog stores values that were moved out of RAM (add-tiered-storage,
// Phase 1). It is an append-only set of files of CRC-checked records; a record
// is addressed by a Ref (file, offset, sizes). The commit log remains the source
// of truth: on startup the store discards these files and recovery spills again,
// so this package needs no recovery of its own.
//
// Record layout (big endian):
//
//	crc32   uint32  over everything after it
//	keyLen  uint32
//	valLen  uint32
//	key     [keyLen]byte
//	value   [valLen]byte
package valuelog

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

const (
	headerSize = 12
	fileExt    = ".vlog"
	// DefaultMaxFileSize is the size at which the active file is rotated.
	DefaultMaxFileSize = 256 << 20
	writeBufferSize    = 1 << 20
)

var (
	// ErrCorrupt reports a record that failed its checksum or does not hold
	// the expected key.
	ErrCorrupt = errors.New("valuelog: corrupt record")
	// ErrGone reports a reference to a file garbage collection has removed; the
	// caller should re-read the key's current reference.
	ErrGone = errors.New("valuelog: file no longer exists")
	// ErrClosed reports use after Close.
	ErrClosed = errors.New("valuelog: closed")
)

// Ref addresses one record.
type Ref struct {
	File   uint32
	KeyLen uint32
	Off    int64
	ValLen uint32
}

func (r Ref) size() int64 { return headerSize + int64(r.KeyLen) + int64(r.ValLen) }

type file struct {
	id    uint32
	f     *os.File
	size  int64 // bytes appended
	live  int64 // bytes of records not yet released
	alive bool
}

// Log is a set of value files. It is safe for concurrent use.
type Log struct {
	dir     string
	maxSize int64

	mu      sync.RWMutex // guards files and active; readers hold it shared during ReadAt
	files   map[uint32]*file
	active  *file
	w       *bufio.Writer
	flushed int64 // bytes of the active file already handed to the OS
	nextID  uint32
	closed  bool
}

// Open creates dir if needed, removes any value files left from a previous run
// (the commit log rebuilds them) and starts a fresh active file.
func Open(dir string, maxFileSize int64) (*Log, error) {
	if maxFileSize <= 0 {
		maxFileSize = DefaultMaxFileSize
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	old, err := filepath.Glob(filepath.Join(dir, "*"+fileExt))
	if err != nil {
		return nil, err
	}
	for _, p := range old {
		if err := os.Remove(p); err != nil {
			return nil, fmt.Errorf("valuelog: remove stale %s: %w", p, err)
		}
	}
	l := &Log{dir: dir, maxSize: maxFileSize, files: map[uint32]*file{}, nextID: 1}
	if err := l.rotateLocked(); err != nil {
		return nil, err
	}
	return l, nil
}

func (l *Log) path(id uint32) string {
	return filepath.Join(l.dir, fmt.Sprintf("%08d%s", id, fileExt))
}

// rotateLocked flushes the active file and opens a new one. Caller holds mu.
func (l *Log) rotateLocked() error {
	if l.w != nil {
		if err := l.w.Flush(); err != nil {
			return err
		}
	}
	id := l.nextID
	l.nextID++
	f, err := os.OpenFile(l.path(id), os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	nf := &file{id: id, f: f, alive: true}
	l.files[id] = nf
	l.active = nf
	l.w = bufio.NewWriterSize(f, writeBufferSize)
	l.flushed = 0
	return nil
}

// Append writes a record for key and value and returns its reference. The
// record is buffered; Read flushes when it needs unflushed bytes.
func (l *Log) Append(key, value []byte) (Ref, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return Ref{}, ErrClosed
	}
	size := int64(headerSize + len(key) + len(value))
	if l.active.size > 0 && l.active.size+size > l.maxSize {
		if err := l.rotateLocked(); err != nil {
			return Ref{}, err
		}
	}
	var hdr [headerSize]byte
	binary.BigEndian.PutUint32(hdr[4:8], uint32(len(key)))
	binary.BigEndian.PutUint32(hdr[8:12], uint32(len(value)))
	h := crc32.NewIEEE()
	h.Write(hdr[4:])
	h.Write(key)
	h.Write(value)
	binary.BigEndian.PutUint32(hdr[0:4], h.Sum32())

	ref := Ref{File: l.active.id, KeyLen: uint32(len(key)), Off: l.active.size, ValLen: uint32(len(value))}
	for _, b := range [][]byte{hdr[:], key, value} {
		if _, err := l.w.Write(b); err != nil {
			return Ref{}, err
		}
	}
	l.active.size += size
	l.active.live += size
	return ref, nil
}

// Flush hands buffered records to the OS.
func (l *Log) Flush() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.flushLocked()
}

func (l *Log) flushLocked() error {
	if l.closed || l.w == nil {
		return nil
	}
	if err := l.w.Flush(); err != nil {
		return err
	}
	l.flushed = l.active.size
	return nil
}

// Read returns the value at ref, verifying the record's checksum and that it
// holds key.
func (l *Log) Read(ref Ref, key []byte) ([]byte, error) {
	l.mu.RLock()
	needFlush := l.active != nil && ref.File == l.active.id && ref.Off+ref.size() > l.flushed
	l.mu.RUnlock()
	if needFlush {
		if err := l.Flush(); err != nil {
			return nil, err
		}
	}

	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.closed {
		return nil, ErrClosed
	}
	f, ok := l.files[ref.File]
	if !ok || !f.alive {
		return nil, ErrGone
	}
	buf := make([]byte, ref.size())
	if _, err := f.f.ReadAt(buf, ref.Off); err != nil {
		return nil, fmt.Errorf("valuelog: read: %w", err)
	}
	return decode(buf, key)
}

func decode(buf, key []byte) ([]byte, error) {
	if len(buf) < headerSize {
		return nil, ErrCorrupt
	}
	kl := binary.BigEndian.Uint32(buf[4:8])
	vl := binary.BigEndian.Uint32(buf[8:12])
	if int64(len(buf)) != headerSize+int64(kl)+int64(vl) {
		return nil, ErrCorrupt
	}
	if crc32.ChecksumIEEE(buf[4:]) != binary.BigEndian.Uint32(buf[0:4]) {
		return nil, ErrCorrupt
	}
	k := buf[headerSize : headerSize+kl]
	if key != nil && string(k) != string(key) {
		return nil, ErrCorrupt
	}
	return buf[headerSize+kl:], nil
}

// Release marks the record at ref dead (its key was overwritten, deleted or
// loaded back into RAM), so garbage collection can reclaim its file.
func (l *Log) Release(ref Ref) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if f, ok := l.files[ref.File]; ok {
		f.live -= ref.size()
	}
}

// Relocator is called by Collect for each record of a file being reclaimed. It
// must, atomically with respect to the key's other writers, check that key
// still refers to old and if so move it (typically by appending value again
// and pointing the key at the new reference). It reports whether it moved it.
type Relocator func(key []byte, old Ref, value []byte) bool

// Collect reclaims every non-active file whose live bytes are at most
// liveRatio of its size: it hands each record to relocate, then deletes the
// file. It returns the bytes reclaimed.
func (l *Log) Collect(liveRatio float64, relocate Relocator) (int64, error) {
	l.mu.RLock()
	var victims []*file
	for _, f := range l.files {
		if f != l.active && f.alive && f.size > 0 && float64(f.live) <= liveRatio*float64(f.size) {
			victims = append(victims, f)
		}
	}
	l.mu.RUnlock()
	return l.collect(victims, relocate)
}

func (l *Log) collect(victims []*file, relocate Relocator) (int64, error) {
	sort.Slice(victims, func(i, j int) bool { return victims[i].id < victims[j].id })
	var reclaimed int64
	for _, v := range victims {
		if err := l.scan(v, relocate); err != nil {
			return reclaimed, err
		}
		l.mu.Lock()
		v.alive = false
		delete(l.files, v.id)
		reclaimed += v.size
		l.mu.Unlock()
		v.f.Close()
		os.Remove(l.path(v.id))
	}
	return reclaimed, nil
}

// ClosedFileSizes returns the size of every file except the active one.
func (l *Log) ClosedFileSizes() map[uint32]int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	out := make(map[uint32]int64, len(l.files))
	for id, f := range l.files {
		if f != l.active && f.alive {
			out[id] = f.size
		}
	}
	return out
}

// CollectFiles reclaims the given closed files: it hands each record to
// relocate, then deletes the file. The caller decides which files are worth
// it (for example from live bytes counted in its own index). It returns the
// bytes reclaimed.
func (l *Log) CollectFiles(ids []uint32, relocate Relocator) (int64, error) {
	l.mu.RLock()
	var victims []*file
	for _, id := range ids {
		if f, ok := l.files[id]; ok && f != l.active && f.alive {
			victims = append(victims, f)
		}
	}
	l.mu.RUnlock()
	return l.collect(victims, relocate)
}

// scan streams every record of f to relocate.
func (l *Log) scan(f *file, relocate Relocator) error {
	r := bufio.NewReaderSize(io.NewSectionReader(f.f, 0, f.size), writeBufferSize)
	var off int64
	var hdr [headerSize]byte
	for off < f.size {
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			return fmt.Errorf("valuelog: scan %d: %w", f.id, err)
		}
		kl := binary.BigEndian.Uint32(hdr[4:8])
		vl := binary.BigEndian.Uint32(hdr[8:12])
		buf := make([]byte, headerSize+int(kl)+int(vl))
		copy(buf, hdr[:])
		if _, err := io.ReadFull(r, buf[headerSize:]); err != nil {
			return fmt.Errorf("valuelog: scan %d: %w", f.id, err)
		}
		ref := Ref{File: f.id, KeyLen: kl, Off: off, ValLen: vl}
		off += int64(len(buf))
		value, err := decode(buf, nil)
		if err != nil {
			// A corrupt record cannot be moved; the key that points at it
			// will fail its own read. Skip it.
			continue
		}
		relocate(buf[headerSize:headerSize+kl], ref, value)
	}
	return nil
}

// Stats reports file count, bytes on disk and live bytes.
type Stats struct {
	Files     int
	DiskBytes int64
	LiveBytes int64
}

// Stats returns current file statistics.
func (l *Log) Stats() Stats {
	l.mu.RLock()
	defer l.mu.RUnlock()
	var s Stats
	for _, f := range l.files {
		s.Files++
		s.DiskBytes += f.size
		s.LiveBytes += f.live
	}
	return s
}

// Close flushes and closes every file. The files stay on disk; Open removes them.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	err := l.flushLocked()
	l.closed = true
	for _, f := range l.files {
		f.f.Close()
	}
	return err
}
