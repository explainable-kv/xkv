// Copyright xkv.org 2026
// SPDX-License-Identifier: Apache-2.0

package xkv

import (
	"bytes"
	"context"
	"encoding/binary"
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
	ErrFailed     = errors.New("xkv: an earlier write failed; reopen the store")
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
	// loss can lose the last transactions written, and the next open may
	// then need Repair. A killed process loses nothing either way.
	NoSync bool

	// Repair lets a writable Open trim a file whose end is damaged rather
	// than refuse it with ErrCorrupt. Everything after the last transaction
	// that reads back whole is cut off, after being copied to a file beside
	// the store (see Trimmed). Use it after `xkv check` has shown what's
	// wrong.
	Repair bool

	// DefaultActor is recorded when neither the write nor its context names
	// an actor. If empty, "unknown" is recorded.
	DefaultActor string

	// Now returns the time recorded on each write. Tests set it; the
	// default is time.Now. A write is never recorded earlier than a
	// revision of its key the store holds: if Now reads earlier, the
	// write gets that revision's time plus a nanosecond.
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

	// Origin is set on an imported revision: the store it was first
	// written to and its number there, as "STOREID:REV". Empty for a
	// revision written to this store.
	Origin string

	// History is set on a revision imported for the record only, because
	// an imported revision names it as a cause. It never becomes its key's
	// current value and isn't listed in the key's History.
	History bool
}

func (r *rec) revision() Revision {
	v := Revision{
		Rev: r.rev, Tx: r.tx, Key: r.key, Value: r.value, Deleted: r.deleted,
		Actor: r.actor, Reason: r.reason, Ref: r.ref, Causes: r.causes,
		Time: time.Unix(0, r.time).UTC(), History: r.history,
	}
	if r.origin != nil {
		v.Origin = r.origin.String()
	}
	return v
}

// revMeta is the in-memory index entry for one revision.
type revMeta struct {
	rev     uint64
	tx      uint64
	off     int64
	n       uint32 // frame length, header included
	deleted bool
	gap     bool // compaction removed revisions of this key just before this one
	history bool // kept for the record only; not in its key's list of revisions
	time    int64
	key     string
	origin  *ident // set for an imported revision: its identity where first written
}

// keyState holds one key's revisions, ascending, and the one copy of its
// name every index entry shares.
type keyState struct {
	name string
	revs []uint64
	own  uint64 // the newest revision of the key written here (not imported), 0 if none
}

func (k *keyState) latest() uint64 { return k.revs[len(k.revs)-1] }

// DB is an open store. It is safe for concurrent use by multiple goroutines.
type DB struct {
	path string // "" for memory
	opts Options

	xmu sync.Mutex   // held by compaction, and by Export and Import, which need records to stay put
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
	origins map[ident]uint64       // identity of each imported revision -> its number here
	hist    map[string][]uint64    // history revisions of each key, ascending
	keySeen map[string][]seenEntry // per key, the highest revision held from each other store
	keyTime map[string]int64       // per key, the latest time of any revision held, history included
	closed  bool
	dirty   bool   // index changed since the snapshot was written
	failed  error  // set when a write left the file in an unknown state
	trimmed string // where Open saved the bytes it trimmed, if it trimmed any
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
		f, err := openLocked(path)
		if err != nil {
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

// openLocked opens path for writing and takes its lock. Compaction replaces
// the file with a rename; an open that raced it could end up holding the
// lock on the old, unlinked file, and its writes would go nowhere. So after
// locking it checks that the path still names the file it locked, and starts
// again if not.
func openLocked(path string) (*os.File, error) {
	for tries := 0; ; tries++ {
		f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
		if err != nil {
			return nil, err
		}
		if testHookOpened != nil {
			testHookOpened()
		}
		if err := lockFile(f); err != nil {
			f.Close()
			if errors.Is(err, ErrLocked) {
				return nil, fmt.Errorf("%w: %s", ErrLocked, path)
			}
			return nil, err
		}
		fi, err1 := f.Stat()
		pi, err2 := os.Stat(path)
		if err1 == nil && err2 == nil && os.SameFile(fi, pi) {
			return f, nil
		}
		f.Close()
		if tries >= 20 {
			return nil, fmt.Errorf("xkv: %s kept being replaced while it was opened", path)
		}
		time.Sleep(time.Millisecond)
	}
}

// testHookOpened runs between opening and locking the file, in tests only.
var testHookOpened func()

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
	db := &DB{keys: map[string]*keyState{}, origins: map[ident]uint64{}, hist: map[string][]uint64{}, keySeen: map[string][]seenEntry{}, keyTime: map[string]int64{}}
	if opts != nil {
		db.opts = *opts
	}
	if db.opts.Now == nil {
		db.opts.Now = time.Now
	}
	return db
}

// headerPrefix reports whether b could be the start of a header whose write
// was cut short: all zeros, or the magic bytes as far as they go.
func headerPrefix(b []byte) bool {
	zero := true
	for _, c := range b {
		if c != 0 {
			zero = false
			break
		}
	}
	if zero {
		return true
	}
	n := min(len(b), len(magic))
	return string(b[:n]) == string(magic[:n])
}

// load reads the header and builds the index, from the snapshot when it
// matches the file exactly, otherwise by reading every record; then it deals
// with whatever follows the last complete transaction.
func (db *DB) load(useSnapshot bool) error {
	size, err := db.st.Size()
	if err != nil {
		return err
	}
	hb := make([]byte, min(size, maxHeader))
	if _, err := db.st.ReadAt(hb, 0); err != nil && err != io.EOF {
		return err
	}
	h, herr := decodeHeader(hb)
	if herr != nil && size < headerSizeV2 {
		// Too short for a format 2 header and not a whole format 1 one:
		// either nothing yet, or a creation cut short before any data.
		if !headerPrefix(hb) {
			return fmt.Errorf("%w: not an xkv file (%d bytes, shorter than a header)", ErrCorrupt, size)
		}
		if db.opts.ReadOnly {
			return nil
		}
		// A new file, or one whose creation was cut short before any data.
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
		db.end = h.size()
		return nil
	}
	if herr != nil {
		return fmt.Errorf("%w: %v", ErrCorrupt, herr)
	}
	db.hdr = h

	from := h.size()
	if useSnapshot && db.path != "" && db.file != nil {
		if off, ok := db.loadSnapshot(size); ok {
			from = off
		}
	}
	res, err := scan(db.st, size, from, db.lastRev, db.lastTx, db.addRev)
	if err != nil {
		return err
	}
	if res.corrupt != "" && (db.opts.ReadOnly || !db.opts.Repair) {
		return fmt.Errorf("%w at offset %d: %s (xkv check shows where; xkv repair trims it, keeping a copy)", ErrCorrupt, res.badAt, res.corrupt)
	}
	db.end = res.end
	db.lastRev = res.lastRev
	db.lastTx = res.lastTx
	if res.end < size && !db.opts.ReadOnly {
		// Trim what follows the last complete transaction: the part of a
		// write a crash cut short, or, with Repair, damage. The bytes are
		// copied aside first, so nothing is lost for good.
		if err := db.saveTail(res.end, size); err != nil {
			return err
		}
		if err := db.st.Truncate(res.end); err != nil {
			return err
		}
		if err := db.st.Sync(); err != nil {
			return err
		}
	}
	db.dirty = res.end > h.size() && from != res.end
	return nil
}

// saveTail copies st[from:size] to a file beside the store before it is
// trimmed: <path>.trimmed-<UTC time>.
func (db *DB) saveTail(from, size int64) error {
	if db.path == "" {
		return nil
	}
	b := make([]byte, size-from)
	if _, err := db.st.ReadAt(b, from); err != nil && err != io.EOF {
		return err
	}
	name := db.path + ".trimmed-" + time.Now().UTC().Format("20060102T150405.000000000Z")
	f, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, fileMode(db.path))
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	db.trimmed = name
	return nil
}

// Trimmed returns the file Open copied trimmed bytes to, or "" if it didn't
// trim anything.
func (db *DB) Trimmed() string { return db.trimmed }

// fileMode returns the permission bits of the file at path, so files written
// beside it (the snapshot, compaction's work file, trimmed bytes) are no more
// readable than the store itself.
func fileMode(path string) os.FileMode {
	if fi, err := os.Stat(path); err == nil {
		return fi.Mode().Perm()
	}
	return 0o644
}

// addRev adds one committed revision to the index. Revisions arrive in
// ascending order.
func (db *DB) addRev(m revMeta, sl []seenEntry) {
	if t, ok := db.keyTime[m.key]; !ok || m.time > t {
		db.keyTime[m.key] = m.time
	}
	if m.origin != nil {
		db.origins[*m.origin] = m.rev
		db.noteSeen(m.key, *m.origin)
	}
	// What the revision's writer had seen, this store has now seen too.
	for _, e := range sl {
		db.noteSeen(m.key, ident{e.store, e.rev})
	}
	if !m.history {
		k, ok := db.keys[m.key]
		if !ok {
			k = &keyState{name: m.key}
			db.keys[m.key] = k
		}
		k.revs = append(k.revs, m.rev)
		if m.origin == nil {
			k.own = m.rev
		}
		m.key = k.name
	} else {
		db.hist[m.key] = append(db.hist[m.key], m.rev)
	}
	db.revs = append(db.revs, m)
}

// allRevs returns every revision of key the store holds, history ones
// included, ascending. Called with mu held.
func (db *DB) allRevs(key string) []uint64 {
	var a []uint64
	if k, ok := db.keys[key]; ok {
		a = k.revs
	}
	h := db.hist[key]
	if len(h) == 0 {
		return a
	}
	out := make([]uint64, 0, len(a)+len(h))
	i, j := 0, 0
	for i < len(a) || j < len(h) {
		if j == len(h) || (i < len(a) && a[i] < h[j]) {
			out = append(out, a[i])
			i++
		} else {
			out = append(out, h[j])
			j++
		}
	}
	return out
}

// seenEntry is one store's part of a seen list.
type seenEntry struct {
	store StoreID
	rev   uint64
}

// seen is a version vector for one key: for each store, the highest number
// among that store's revisions of the key that are held or known. Each
// store's revisions of a key form one line, so the number stands for that
// revision and every earlier one from the same store.
type seen map[StoreID]uint64

func (s seen) add(id ident) {
	if id.rev > s[id.store] {
		s[id.store] = id.rev
	}
}

// covers reports whether s has seen everything o has.
func (s seen) covers(o seen) bool {
	for st, rev := range o {
		if s[st] < rev {
			return false
		}
	}
	return true
}

// list returns s without store skip, sorted by store, as a record holds it.
func (s seen) list(skip StoreID) []seenEntry {
	var out []seenEntry
	for st, rev := range s {
		if st != skip && rev > 0 {
			out = append(out, seenEntry{st, rev})
		}
	}
	sort.Slice(out, func(i, j int) bool { return bytes.Compare(out[i].store[:], out[j].store[:]) < 0 })
	return out
}

// noteSeen records that the store holds imported identity id of key.
// Revisions written here aren't noted: this store's own line is read from
// the key's revisions when needed, so a store that never exchanges keeps
// nothing here. Called with mu held for writing.
func (db *DB) noteSeen(key string, id ident) {
	l := db.keySeen[key]
	for i := range l {
		if l[i].store == id.store {
			if id.rev > l[i].rev {
				l[i].rev = id.rev
			}
			return
		}
	}
	db.keySeen[key] = append(l, seenEntry{id.store, id.rev})
}

// haveOf returns what the store has seen of key: every store's line it
// holds, its own included. Called with mu held.
func (db *DB) haveOf(key string) seen {
	s := seen{}
	for _, e := range db.keySeen[key] {
		s[e.store] = e.rev
	}
	if k, ok := db.keys[key]; ok && k.own > 0 && db.hdr.version >= 2 {
		s.add(ident{db.hdr.store, k.own})
	}
	return s
}

// identOf returns the identity of the revision m describes: where it was
// first written.
func (db *DB) identOf(m revMeta) ident {
	if m.origin != nil {
		return *m.origin
	}
	return ident{store: db.hdr.store, rev: m.rev}
}

// lookupIdent returns the local revision with identity id, if the store
// holds it.
func (db *DB) lookupIdent(id ident) (uint64, bool) {
	if rev, ok := db.origins[id]; ok {
		return rev, true
	}
	if id.store == db.hdr.store && db.hdr.version >= 2 {
		if m, ok := db.findRev(id.rev); ok && m.origin == nil {
			return id.rev, true
		}
	}
	return 0, false
}

// StoreID returns the store's id. A format 1 store has none until it is
// upgraded; ok is false then.
func (db *DB) StoreID() (id StoreID, ok bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.hdr.store, db.hdr.version >= 2
}

// scanResult is what a scan found.
type scanResult struct {
	end     int64 // just past the last complete transaction
	lastRev uint64
	lastTx  uint64
	txs     int
	corrupt string // non-empty when damage was found, not just a torn end
	badAt   int64
}

// scan reads frames from offset from to size, handing each committed
// revision to add. It stops at the first frame that isn't intact. Whatever
// follows that point is an uncommitted tail (cut short by a crash) unless a
// complete transaction can be found after it, which means real damage.
func scan(st storage, size, from int64, lastRev, lastTx uint64, add func(revMeta, []seenEntry)) (scanResult, error) {
	res := scanResult{end: from, lastRev: lastRev, lastTx: lastTx}
	r := io.NewSectionReader(st, from, size-from)
	const chunk = 1 << 20
	buf := make([]byte, 0, chunk)
	bufOff := from // file offset of buf[0]
	pos := from    // next frame
	eof := false
	var readErr error

	fill := func(need int64) bool {
		// make sure buf holds at least need bytes starting at pos
		start := pos - bufOff
		if int64(len(buf))-start >= need {
			return true
		}
		if eof {
			return false
		}
		copy(buf, buf[start:])
		buf = buf[:int64(len(buf))-start]
		bufOff = pos
		for int64(len(buf)) < need && !eof {
			if cap(buf)-len(buf) < chunk {
				nb := make([]byte, len(buf), max(int64(2*cap(buf)), need+chunk))
				copy(nb, buf)
				buf = nb
			}
			n, err := r.Read(buf[len(buf):cap(buf)])
			buf = buf[:len(buf)+n]
			if err == io.EOF {
				eof = true
			} else if err != nil {
				eof = true
				readErr = err
				return false
			}
		}
		return int64(len(buf)) >= need
	}

	var pending []revMeta
	var pendingSeen [][]seenEntry
	var pendingTx uint64
	for {
		if !fill(frameSize) {
			break
		}
		start := pos - bufOff
		n := int64(binary.LittleEndian.Uint32(buf[start : start+4]))
		if n == 0 || n > maxRecord || pos+frameSize+n > size {
			break
		}
		if !fill(frameSize + n) {
			break
		}
		start = pos - bufOff
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
			pending = append(pending, revMeta{rev: rr.rev, tx: rr.tx, off: pos, n: uint32(total), deleted: rr.deleted, history: rr.history, time: rr.time, key: rr.key, origin: rr.origin})
			pendingSeen = append(pendingSeen, rr.seen)
			ok = true
		case recCommit:
			c, err := decodeCommit(body)
			if err != nil || len(pending) == 0 || c.tx != pendingTx || c.count != uint64(len(pending)) || c.lastRev != pending[len(pending)-1].rev {
				break
			}
			okGaps := true
			for _, g := range c.gaps {
				i := sort.Search(len(pending), func(i int) bool { return pending[i].rev >= g })
				if i == len(pending) || pending[i].rev != g {
					okGaps = false
					break
				}
				pending[i].gap = true
			}
			if !okGaps {
				break
			}
			for i, m := range pending {
				add(m, pendingSeen[i])
			}
			res.lastRev = c.lastRev
			res.lastTx = c.tx
			res.end = pos + int64(total)
			res.txs++
			pending = pending[:0]
			pendingSeen = pendingSeen[:0]
			ok = true
		}
		if !ok {
			break
		}
		pos += int64(total)
	}
	if readErr != nil {
		return res, readErr
	}
	if res.end < size {
		// Frames from res.end to pos are intact pieces of the transaction
		// that was cut short; the search for damage starts at the break.
		bad, err := completeTxAfter(st, pos, size, size-res.end, res.lastRev, res.lastTx)
		if err != nil {
			return res, err
		}
		if bad != "" {
			res.corrupt = bad
			res.badAt = pos
		}
	}
	return res, nil
}

// completeTxAfter looks for a whole, valid transaction anywhere in
// st[from:size], where from is the first frame that didn't read back. A
// crash only cuts short what was being written last, so a complete
// transaction after the break means the file was damaged in the middle,
// not torn at the end. It returns why it thinks so, or "".
//
// Frames that read back whole are stepped over as units, so a value that
// happens to contain xkv-looking bytes isn't mistaken for a record; only the
// unreadable stretches are searched byte by byte. Candidates must carry
// revision and transaction numbers above the last committed ones before
// their checksum is computed, and the total checksum work is capped.
func completeTxAfter(st storage, from, size, tail int64, lastRev, lastTx uint64) (string, error) {
	if size-from > 2*MaxTxSize {
		return fmt.Sprintf("%d bytes follow a damaged record, more than a cut-short write can leave", size-from), nil
	}
	buf := make([]byte, size-from)
	if _, err := st.ReadAt(buf, from); err != nil && err != io.EOF {
		return "", err
	}
	budget := int64(4*len(buf)) + 64<<20

	// frameAt checks for a plausible, intact frame at i. It returns its
	// body and length, or nil.
	frameAt := func(i int) ([]byte, int) {
		if i+frameSize > len(buf) {
			return nil, 0
		}
		n := int64(binary.LittleEndian.Uint32(buf[i : i+4]))
		if n == 0 || n > maxRecord || n > int64(len(buf)-i-frameSize) {
			return nil, 0
		}
		b := buf[i+frameSize : i+frameSize+int(n)]
		switch b[0] {
		case recRevision, recCommit:
		default:
			return nil, 0
		}
		// Cheap checks before any checksum. A revision needs numbers above
		// the last committed ones (compaction leaves gaps, so no upper
		// bound), a tx id no higher than its revision, a valid flags byte
		// and a plausible key length. A commit needs a tx id above the last
		// one and a revision count the bytes after the last commit could
		// hold (each revision frame takes at least 16 bytes).
		first, k := binary.Uvarint(b[1:])
		if k <= 0 {
			return nil, 0
		}
		if b[0] == recRevision {
			tx, k2 := binary.Uvarint(b[1+k:])
			if k2 <= 0 || first <= lastRev || tx <= lastTx || tx > first {
				return nil, 0
			}
			rest := b[1+k+k2:]
			if len(rest) < 3 || rest[0]&^flagsAll != 0 {
				return nil, 0
			}
			if _, k3 := binary.Varint(rest[1:]); k3 <= 0 {
				return nil, 0
			} else if kl, k4 := binary.Uvarint(rest[1+k3:]); k4 <= 0 || kl == 0 || kl > MaxKeySize || kl > uint64(n) {
				return nil, 0
			}
		} else {
			count, k2 := binary.Uvarint(b[1+k:])
			if first <= lastTx || k2 <= 0 || count == 0 || count > uint64(tail/16) || uint64(n) > 32+10*count {
				return nil, 0
			}
		}
		budget -= n
		if budget < 0 {
			return nil, -1
		}
		body, total := checkFrame(buf[i:])
		return body, total
	}

	for i := 0; i+frameSize <= len(buf); {
		body, total := frameAt(i)
		if total < 0 {
			return "too much to check after a damaged record", nil
		}
		if body == nil {
			i++
			continue
		}
		if body[0] == recRevision {
			if first, err := decodeRev(body); err == nil {
				// Does a whole transaction start here?
				j := i + total
				count, last := uint64(1), first.rev
				for {
					b, t := frameAt(j)
					if t < 0 {
						return "too much to check after a damaged record", nil
					}
					if b == nil {
						break
					}
					if b[0] == recCommit {
						c, err := decodeCommit(b)
						if err == nil && c.tx == first.tx && c.count == count && c.lastRev == last {
							return "a damaged record is followed by whole transactions", nil
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
		}
		// An intact frame: its bytes are a record, not something to search.
		i += total
	}
	return "", nil
}

// findRev returns the index entry for rev.
func (db *DB) findRev(rev uint64) (revMeta, bool) {
	i := sort.Search(len(db.revs), func(i int) bool { return db.revs[i].rev >= rev })
	if i < len(db.revs) && db.revs[i].rev == rev {
		return db.revs[i], true
	}
	return revMeta{}, false
}

// readRec reads and decodes the record described by m, and checks it is the
// record the index says it is.
func (db *DB) readRec(m revMeta) (*rec, error) {
	b := make([]byte, m.n)
	if _, err := db.st.ReadAt(b, m.off); err != nil && err != io.EOF {
		return nil, err
	}
	body, total := checkFrame(b)
	if body == nil || total != int(m.n) {
		return nil, fmt.Errorf("%w: revision %d fails its checksum", ErrCorrupt, m.rev)
	}
	r, err := decodeRev(body)
	if err != nil || r.rev != m.rev || r.tx != m.tx || r.key != m.key || r.deleted != m.deleted || r.history != m.history || (r.origin == nil) != (m.origin == nil) || (r.origin != nil && *r.origin != *m.origin) {
		return nil, fmt.Errorf("%w: revision %d doesn't read back as indexed", ErrCorrupt, m.rev)
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
	if db.path != "" && !db.opts.ReadOnly && db.dirty && db.failed == nil {
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
	Format    int    // file format version: 1 or 2
	StoreID   string // 32 hex digits; empty for a format 1 store
}

// Stats returns counts for the store.
func (db *DB) Stats() (Stats, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return Stats{}, ErrClosed
	}
	s := Stats{Keys: len(db.keys), Revisions: len(db.revs), LastRev: db.lastRev, LastTx: db.lastTx, FileSize: db.end, Format: int(db.hdr.version)}
	if db.hdr.version >= 2 {
		s.StoreID = db.hdr.store.String()
	}
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
