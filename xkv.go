// Copyright xkv.org 2026
// SPDX-License-Identifier: Apache-2.0

package xkv

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// Version is the version of this package and of the xkv command.
const Version = "0.1.0"

// Errors returned by the store. Check them with errors.Is.
var (
	ErrNotFound   = errors.New("xkv: not found")
	ErrNoReason   = errors.New("xkv: a reason is required (strict mode)")
	ErrBadCause   = errors.New("xkv: bad cause")
	ErrCompacted  = errors.New("xkv: revision was removed by compaction")
	ErrLocked     = errors.New("xkv: file is open for writing by another handle")
	ErrReadOnly   = errors.New("xkv: store is read-only")
	ErrClosed     = errors.New("xkv: store is closed")
	ErrCorrupt    = errors.New("xkv: file is damaged")
	ErrTooLarge   = errors.New("xkv: write is too large")
	ErrInvalid    = errors.New("xkv: invalid argument")
	ErrTxFinished = errors.New("xkv: transaction is finished")
)

// Options configure Open. A nil *Options means the defaults.
type Options struct {
	// ReadOnly opens the file without locking it and never writes to it. A
	// read-only handle sees the store as it was when it was opened.
	ReadOnly bool

	// Relaxed accepts writes without a reason and records the reason as
	// "unspecified", so the gap stays visible in Why. By default (strict
	// mode) a write without a reason is refused with ErrNoReason.
	Relaxed bool

	// NoSync skips the fsync after each transaction. Faster, but a power
	// loss can lose the last transactions written. A killed process loses
	// nothing either way.
	NoSync bool

	// DefaultActor is recorded when neither the write nor its context names
	// an actor. If empty, "unknown" is recorded.
	DefaultActor string

	// Now returns the time recorded on each write. Tests set it; the
	// default is time.Now.
	Now func() time.Time
}

// CauseRef points at one revision: a key and its revision number.
type CauseRef struct {
	Key string `json:"key"`
	Rev uint64 `json:"rev"`
}

func (c CauseRef) String() string { return fmt.Sprintf("%s@%d", c.Key, c.Rev) }

// Revision is one recorded change to one key.
type Revision struct {
	Rev     uint64     // store-wide revision number, strictly increasing
	Tx      uint64     // transaction id; revisions written together share it
	Key     string     // the key
	Value   []byte     // the value; nil for a delete
	Deleted bool       // true when this revision deletes the key
	Actor   string     // who wrote it
	Reason  string     // why
	Ref     string     // optional outside reference: ticket, request id, tool call id
	Causes  []CauseRef // earlier revisions that led to this one
	Time    time.Time  // wall-clock write time, for display
}

func (r *rec) revision() Revision {
	return Revision{
		Rev: r.rev, Tx: r.tx, Key: r.key, Value: r.value, Deleted: r.deleted,
		Actor: r.actor, Reason: r.reason, Ref: r.ref, Causes: r.causes,
		Time: time.Unix(0, r.time).UTC(),
	}
}

// revMeta is the in-memory index entry for one revision.
type revMeta struct {
	rev     uint64
	tx      uint64
	off     int64
	n       uint32 // frame length, header included
	deleted bool
	time    int64
	key     string
}

// keyState holds one key's revisions, ascending, and the one copy of its
// name every index entry shares.
type keyState struct {
	name string
	revs []uint64
}

func (k *keyState) latest() uint64 { return k.revs[len(k.revs)-1] }

// DB is an open store. It is safe for concurrent use by multiple goroutines.
type DB struct {
	path string // "" for memory
	opts Options

	wmu sync.Mutex   // serialises writers and compaction
	mu  sync.RWMutex // guards the fields below; held for reading during reads

	st      storage
	file    *os.File // nil for memory
	hdr     header
	end     int64 // offset just past the last committed transaction
	lastRev uint64
	lastTx  uint64
	revs    []revMeta // ascending by rev
	keys    map[string]*keyState
	closed  bool
	dirty   bool // index changed since the snapshot was written
}

type actorKey struct{}

// WithActor returns a context that records actor on every write made with it,
// unless the write names its own actor.
func WithActor(ctx context.Context, actor string) context.Context {
	return context.WithValue(ctx, actorKey{}, actor)
}

// ActorFrom returns the actor set by WithActor, or "".
func ActorFrom(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	a, _ := ctx.Value(actorKey{}).(string)
	return a
}

// Open opens the store at path, creating it if it doesn't exist. A writable
// open takes an exclusive lock on the file and fails with ErrLocked if
// another handle holds it.
func Open(path string, opts *Options) (*DB, error) {
	db := newDB(opts)
	db.path = path
	if db.opts.ReadOnly {
		f, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		db.file = f
		db.st = &fileStorage{f}
	} else {
		f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
		if err != nil {
			return nil, err
		}
		if err := lockFile(f); err != nil {
			f.Close()
			if errors.Is(err, ErrLocked) {
				return nil, fmt.Errorf("%w: %s", ErrLocked, path)
			}
			return nil, err
		}
		db.file = f
		db.st = &fileStorage{f}
		// A compaction killed before its rename leaves its work file behind.
		os.Remove(path + compactSuffix)
	}
	if err := db.load(true); err != nil {
		db.st.Close()
		return nil, err
	}
	return db, nil
}

// OpenMemory returns a store that lives only in memory. It behaves like a
// file-backed store and is used by the WebAssembly demo and tests.
func OpenMemory(opts *Options) (*DB, error) {
	db := newDB(opts)
	db.opts.ReadOnly = false
	db.st = &memStorage{}
	if err := db.load(false); err != nil {
		return nil, err
	}
	return db, nil
}

func newDB(opts *Options) *DB {
	db := &DB{keys: map[string]*keyState{}}
	if opts != nil {
		db.opts = *opts
	}
	if db.opts.Now == nil {
		db.opts.Now = time.Now
	}
	return db
}

// load reads the header and builds the index, from the snapshot when one
// matches, then recovers the tail.
func (db *DB) load(useSnapshot bool) error {
	size, err := db.st.Size()
	if err != nil {
		return err
	}
	if size < headerSize {
		if db.opts.ReadOnly {
			if size == 0 {
				db.end = 0
				return nil
			}
			return fmt.Errorf("%w: file is shorter than its header", ErrCorrupt)
		}
		// New file, or one whose creation was cut short before any data.
		h, err := newHeader()
		if err != nil {
			return err
		}
		if err := db.st.Truncate(0); err != nil {
			return err
		}
		if _, err := db.st.WriteAt(h.encode(), 0); err != nil {
			return err
		}
		if err := db.st.Sync(); err != nil {
			return err
		}
		if db.path != "" {
			syncDir(db.path)
		}
		db.hdr = h
		db.end = headerSize
		return nil
	}
	hb := make([]byte, headerSize)
	if _, err := db.st.ReadAt(hb, 0); err != nil {
		return err
	}
	h, err := decodeHeader(hb)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrCorrupt, err)
	}
	db.hdr = h

	from := int64(headerSize)
	if useSnapshot && db.path != "" {
		if off, ok := db.loadSnapshot(size); ok {
			from = off
		}
	}
	res, err := scan(db.st, size, from, db.lastRev, db.lastTx, db.addRev)
	if err != nil {
		return err
	}
	if res.corrupt != "" {
		return fmt.Errorf("%w at offset %d: %s", ErrCorrupt, res.badAt, res.corrupt)
	}
	db.end = res.end
	db.lastRev = res.lastRev
	db.lastTx = res.lastTx
	if res.end < size && !db.opts.ReadOnly {
		// Trim the uncommitted tail a crash left behind.
		if err := db.st.Truncate(res.end); err != nil {
			return err
		}
		if err := db.st.Sync(); err != nil {
			return err
		}
	}
	// The snapshot needs rewriting if the scan found anything it didn't
	// cover, or if there was no snapshot to start from.
	db.dirty = res.end > headerSize && (from == headerSize || res.end != from)
	return nil
}

// addRev adds one committed revision to the index. Revisions arrive in
// ascending order.
func (db *DB) addRev(m revMeta) {
	k, ok := db.keys[m.key]
	if !ok {
		k = &keyState{name: m.key}
		db.keys[m.key] = k
	}
	k.revs = append(k.revs, m.rev)
	m.key = k.name
	db.revs = append(db.revs, m)
}

// scanResult is what a scan found.
type scanResult struct {
	end     int64 // just past the last complete transaction
	lastRev uint64
	lastTx  uint64
	txs     int
	corrupt string // non-empty when damage was found before the tail
	badAt   int64
}

// scan reads frames from offset from to size, handing each committed
// revision to add. It stops at the first frame that isn't intact. Whatever
// follows that point is an uncommitted tail (cut short by a crash) unless a
// complete transaction can be found after it, which means real damage.
func scan(st storage, size, from int64, lastRev, lastTx uint64, add func(revMeta)) (scanResult, error) {
	res := scanResult{end: from, lastRev: lastRev, lastTx: lastTx}
	r := io.NewSectionReader(st, from, size-from)
	const chunk = 1 << 20
	buf := make([]byte, 0, chunk)
	bufOff := from // file offset of buf[0]
	pos := from    // next frame
	eof := false

	fill := func(need int) bool {
		// make sure buf holds at least need bytes starting at pos
		start := int(pos - bufOff)
		if len(buf)-start >= need {
			return true
		}
		if eof {
			return false
		}
		// shift
		copy(buf, buf[start:])
		buf = buf[:len(buf)-start]
		bufOff = pos
		for len(buf) < need && !eof {
			if cap(buf)-len(buf) < chunk {
				nb := make([]byte, len(buf), max(2*cap(buf), need+chunk))
				copy(nb, buf)
				buf = nb
			}
			n, err := r.Read(buf[len(buf):cap(buf)])
			buf = buf[:len(buf)+n]
			if err == io.EOF {
				eof = true
			} else if err != nil {
				eof = true
				return false
			}
		}
		return len(buf) >= need
	}

	var pending []revMeta
	var pendingTx uint64
	for {
		if !fill(frameSize) {
			break
		}
		start := int(pos - bufOff)
		n := int(uint32(buf[start]) | uint32(buf[start+1])<<8 | uint32(buf[start+2])<<16 | uint32(buf[start+3])<<24)
		if n == 0 || n > maxRecord || pos+int64(frameSize+n) > size {
			break
		}
		if !fill(frameSize + n) {
			break
		}
		start = int(pos - bufOff)
		body, total := checkFrame(buf[start : start+frameSize+n])
		if body == nil {
			break
		}
		ok := false
		switch body[0] {
		case recRevision:
			rr, err := decodeRev(body)
			if err != nil {
				break
			}
			prev := res.lastRev
			if len(pending) > 0 {
				prev = pending[len(pending)-1].rev
			}
			if rr.rev <= prev {
				break
			}
			if len(pending) == 0 {
				if rr.tx <= res.lastTx {
					break
				}
				pendingTx = rr.tx
			} else if rr.tx != pendingTx {
				break
			}
			pending = append(pending, revMeta{rev: rr.rev, tx: rr.tx, off: pos, n: uint32(total), deleted: rr.deleted, time: rr.time, key: rr.key})
			ok = true
		case recCommit:
			c, err := decodeCommit(body)
			if err != nil || len(pending) == 0 || c.tx != pendingTx || c.count != uint64(len(pending)) || c.lastRev != pending[len(pending)-1].rev {
				break
			}
			for _, m := range pending {
				add(m)
			}
			res.lastRev = c.lastRev
			res.lastTx = c.tx
			res.end = pos + int64(total)
			res.txs++
			pending = pending[:0]
			ok = true
		}
		if !ok {
			break
		}
		pos += int64(total)
	}
	if res.end < size {
		bad, err := completeTxAfter(st, res.end, size, res.lastRev, res.lastTx)
		if err != nil {
			return res, err
		}
		if bad {
			res.corrupt = "a damaged record is followed by intact transactions"
			res.badAt = pos
		}
	}
	return res, nil
}

// completeTxAfter reports whether a whole, valid transaction can be found
// anywhere in st[from:size]. A crash only ever cuts short the transactions
// being written last, so finding a complete one after the break means the
// file was damaged in the middle, not torn at the end.
func completeTxAfter(st storage, from, size int64, lastRev, lastTx uint64) (bool, error) {
	if size-from > 2*MaxTxSize {
		return true, nil
	}
	buf := make([]byte, size-from)
	if _, err := st.ReadAt(buf, from); err != nil && err != io.EOF {
		return false, err
	}
	for i := 1; i+frameSize <= len(buf); i++ {
		body, total := checkFrame(buf[i:])
		if body == nil || body[0] != recRevision {
			continue
		}
		first, err := decodeRev(body)
		if err != nil || first.rev <= lastRev || first.tx <= lastTx {
			continue
		}
		// Try to read a transaction starting here.
		j := i + total
		count := uint64(1)
		last := first.rev
		for j+frameSize <= len(buf) {
			b, t := checkFrame(buf[j:])
			if b == nil {
				break
			}
			if b[0] == recCommit {
				c, err := decodeCommit(b)
				if err == nil && c.tx == first.tx && c.count == count && c.lastRev == last {
					return true, nil
				}
				break
			}
			rr, err := decodeRev(b)
			if err != nil || rr.tx != first.tx || rr.rev <= last {
				break
			}
			count++
			last = rr.rev
			j += t
		}
	}
	return false, nil
}

// findRev returns the index entry for rev.
func (db *DB) findRev(rev uint64) (revMeta, bool) {
	i := sort.Search(len(db.revs), func(i int) bool { return db.revs[i].rev >= rev })
	if i < len(db.revs) && db.revs[i].rev == rev {
		return db.revs[i], true
	}
	return revMeta{}, false
}

// readRec reads and decodes the record described by m.
func (db *DB) readRec(m revMeta) (*rec, error) {
	b := make([]byte, m.n)
	if _, err := db.st.ReadAt(b, m.off); err != nil && !(err == io.EOF) {
		return nil, err
	}
	body, total := checkFrame(b)
	if body == nil || total != int(m.n) {
		return nil, fmt.Errorf("%w: revision %d fails its checksum", ErrCorrupt, m.rev)
	}
	r, err := decodeRev(body)
	if err != nil || r.rev != m.rev {
		return nil, fmt.Errorf("%w: revision %d can't be read", ErrCorrupt, m.rev)
	}
	return r, nil
}

// Close writes the index snapshot (for a fast next open), releases the lock
// and closes the file.
func (db *DB) Close() error {
	db.wmu.Lock()
	defer db.wmu.Unlock()
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return nil
	}
	db.closed = true
	if db.path != "" && !db.opts.ReadOnly && db.dirty {
		db.writeSnapshot() // best effort: a missing snapshot only means a slower open
	}
	return db.st.Close()
}

// Path returns the file path, or "" for an in-memory store.
func (db *DB) Path() string { return db.path }

// Stats describes a store.
type Stats struct {
	Keys      int    // keys with at least one revision kept
	LiveKeys  int    // keys whose latest revision is not a delete
	Revisions int    // revisions kept
	LastRev   uint64 // highest revision number ever written
	LastTx    uint64 // highest transaction id ever written
	FileSize  int64  // bytes
}

// Stats returns counts for the store.
func (db *DB) Stats() (Stats, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return Stats{}, ErrClosed
	}
	s := Stats{Keys: len(db.keys), Revisions: len(db.revs), LastRev: db.lastRev, LastTx: db.lastTx, FileSize: db.end}
	for _, k := range db.keys {
		if m, ok := db.findRev(k.latest()); ok && !m.deleted {
			s.LiveKeys++
		}
	}
	return s, nil
}

func syncDir(path string) {
	d, err := os.Open(filepath.Dir(path))
	if err != nil {
		return
	}
	d.Sync()
	d.Close()
}
