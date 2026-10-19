// Copyright xkv.org 2026
// SPDX-License-Identifier: Apache-2.0

package xkv

import (
	"context"
	"fmt"
	"math"
	"unicode/utf8"
)

// WriteOption sets what a write records about itself.
type WriteOption func(*writeOpts)

type causeSpec struct {
	key    string
	rev    uint64 // 0 means the key's latest revision
	latest bool
}

type writeOpts struct {
	reason    string
	hasReason bool
	actor     string
	ref       string
	causes    []causeSpec
	resolve   bool // the write settles an import conflict
}

// Reason records why the write was made. In strict mode (the default) every
// write needs one.
func Reason(s string) WriteOption {
	return func(o *writeOpts) { o.reason = s; o.hasReason = true }
}

// Actor records who made the write, overriding the context's actor.
func Actor(s string) WriteOption { return func(o *writeOpts) { o.actor = s } }

// Ref records an outside reference: a ticket, a request id, a tool call id.
func Ref(s string) WriteOption { return func(o *writeOpts) { o.ref = s } }

// Cause records that revision rev of key led to this write. The revision
// must exist, belong to key and be older than the write.
func Cause(key string, rev uint64) WriteOption {
	return func(o *writeOpts) { o.causes = append(o.causes, causeSpec{key: key, rev: rev}) }
}

// CauseLatest records that key's current revision led to this write. It is
// resolved when the write is made.
func CauseLatest(key string) WriteOption {
	return func(o *writeOpts) { o.causes = append(o.causes, causeSpec{key: key, latest: true}) }
}

// Tx is a transaction being built inside Update. Its writes become visible
// to other readers together, when Update returns, or not at all.
type Tx struct {
	db       *DB
	ctx      context.Context
	defaults writeOpts
	txid     uint64
	nextRev  uint64
	recs     []*rec
	frames   [][]byte
	size     int
	latest   map[string]*rec // this transaction's newest revision per key
	byRev    map[uint64]*rec
	now      int64
	limit    int // bytes the transaction may hold: MaxTxSize, more for an import
	finished bool
}

// Put sets key to value inside the transaction and returns the revision it
// will commit as.
func (tx *Tx) Put(key string, value []byte, opts ...WriteOption) (Revision, error) {
	return tx.write(key, value, false, opts)
}

// Delete removes key inside the transaction. Deleting is a revision too: it
// records who, why and its causes like any other write.
func (tx *Tx) Delete(key string, opts ...WriteOption) (Revision, error) {
	return tx.write(key, nil, true, opts)
}

// Get returns key's newest revision, including writes made earlier in this
// transaction.
func (tx *Tx) Get(key string) (Revision, error) {
	if tx.finished {
		return Revision{}, ErrTxFinished
	}
	if r, ok := tx.latest[key]; ok {
		if r.deleted {
			return Revision{}, ErrNotFound
		}
		return r.revision(), nil
	}
	return tx.db.Get(tx.ctx, key)
}

func (tx *Tx) write(key string, value []byte, deleted bool, opts []WriteOption) (Revision, error) {
	if tx.finished {
		return Revision{}, ErrTxFinished
	}
	o := writeOpts{
		reason: tx.defaults.reason, hasReason: tx.defaults.hasReason,
		actor: tx.defaults.actor, ref: tx.defaults.ref,
		causes: append([]causeSpec(nil), tx.defaults.causes...),
	}
	for _, f := range opts {
		f(&o)
	}
	if err := checkKey(key); err != nil {
		return Revision{}, err
	}
	if len(value) > MaxValueSize {
		return Revision{}, fmt.Errorf("%w: value is %d bytes, the limit is %d", ErrTooLarge, len(value), MaxValueSize)
	}
	db := tx.db
	reason := o.reason
	if !o.hasReason || reason == "" {
		if !db.opts.Relaxed {
			return Revision{}, ErrNoReason
		}
		reason = "unspecified"
	}
	actor := o.actor
	if actor == "" {
		actor = ActorFrom(tx.ctx)
	}
	if actor == "" {
		actor = db.opts.DefaultActor
	}
	if actor == "" {
		actor = "unknown"
	}
	if err := checkText("reason", reason, MaxReasonSize); err != nil {
		return Revision{}, err
	}
	if err := checkText("actor", actor, MaxActorSize); err != nil {
		return Revision{}, err
	}
	if err := checkText("ref", o.ref, MaxRefSize); err != nil {
		return Revision{}, err
	}
	if len(o.causes) > MaxCauses {
		return Revision{}, fmt.Errorf("%w: %d causes, the limit is %d", ErrInvalid, len(o.causes), MaxCauses)
	}
	causes := make([]CauseRef, 0, len(o.causes))
	seen := map[CauseRef]bool{}
	for _, c := range o.causes {
		ref, err := tx.resolveCause(c)
		if err != nil {
			return Revision{}, err
		}
		if seen[ref] {
			continue
		}
		seen[ref] = true
		causes = append(causes, ref)
	}
	r := &rec{
		rev: tx.nextRev, tx: tx.txid, deleted: deleted, time: tx.now,
		key: key, actor: actor, reason: reason, ref: o.ref, causes: causes,
		resolution: o.resolve,
	}
	if !deleted {
		r.value = append([]byte{}, value...)
	}
	f := frame(encodeRev(r))
	if tx.size+len(f) > tx.limit {
		return Revision{}, fmt.Errorf("%w: a transaction holds at most %d bytes", ErrTooLarge, tx.limit)
	}
	tx.nextRev++
	tx.recs = append(tx.recs, r)
	tx.frames = append(tx.frames, f)
	tx.size += len(f)
	tx.latest[key] = r
	tx.byRev[r.rev] = r
	return r.revision(), nil
}

// importRev adds an imported revision to the transaction as it is: its
// actor, reason, ref and time are kept, and its causes must already be
// numbers in this store. It gets a number here; r.origin keeps where it was
// first written.
func (tx *Tx) importRev(r *rec) (Revision, error) {
	if tx.finished {
		return Revision{}, ErrTxFinished
	}
	if err := checkKey(r.key); err != nil {
		return Revision{}, err
	}
	if len(r.value) > MaxValueSize {
		return Revision{}, fmt.Errorf("%w: value is %d bytes, the limit is %d", ErrTooLarge, len(r.value), MaxValueSize)
	}
	if err := checkText("reason", r.reason, MaxReasonSize); err != nil {
		return Revision{}, err
	}
	if err := checkText("actor", r.actor, MaxActorSize); err != nil {
		return Revision{}, err
	}
	if err := checkText("ref", r.ref, MaxRefSize); err != nil {
		return Revision{}, err
	}
	if len(r.causes) > MaxCauses {
		return Revision{}, fmt.Errorf("%w: %d causes, the limit is %d", ErrInvalid, len(r.causes), MaxCauses)
	}
	for _, c := range r.causes {
		if _, err := tx.resolveCause(causeSpec{key: c.Key, rev: c.Rev}); err != nil {
			return Revision{}, err
		}
	}
	r.rev, r.tx = tx.nextRev, tx.txid
	if r.deleted {
		r.value = nil
	}
	f := frame(encodeRev(r))
	if tx.size+len(f) > tx.limit {
		return Revision{}, fmt.Errorf("%w: a transaction holds at most %d bytes", ErrTooLarge, tx.limit)
	}
	tx.nextRev++
	tx.recs = append(tx.recs, r)
	tx.frames = append(tx.frames, f)
	tx.size += len(f)
	if !r.history {
		tx.latest[r.key] = r
	}
	tx.byRev[r.rev] = r
	return r.revision(), nil
}

// resolveCause checks one cause against the committed store and this
// transaction's own earlier writes.
func (tx *Tx) resolveCause(c causeSpec) (CauseRef, error) {
	if err := checkKey(c.key); err != nil {
		return CauseRef{}, fmt.Errorf("%w: %v", ErrBadCause, err)
	}
	db := tx.db
	if c.latest {
		if r, ok := tx.latest[c.key]; ok {
			return CauseRef{Key: c.key, Rev: r.rev}, nil
		}
		db.mu.RLock()
		k, ok := db.keys[c.key]
		var rev uint64
		if ok {
			rev = k.latest()
		}
		db.mu.RUnlock()
		if !ok {
			return CauseRef{}, fmt.Errorf("%w: %q has no revisions", ErrBadCause, c.key)
		}
		return CauseRef{Key: c.key, Rev: rev}, nil
	}
	if c.rev == 0 {
		return CauseRef{}, fmt.Errorf("%w: %s@0: revisions start at 1", ErrBadCause, c.key)
	}
	if r, ok := tx.byRev[c.rev]; ok {
		if r.key != c.key {
			return CauseRef{}, fmt.Errorf("%w: revision %d belongs to %q, not %q", ErrBadCause, c.rev, r.key, c.key)
		}
		return CauseRef{Key: c.key, Rev: c.rev}, nil
	}
	db.mu.RLock()
	m, ok := db.findRev(c.rev)
	last := db.lastRev
	db.mu.RUnlock()
	switch {
	case ok && m.key != c.key:
		return CauseRef{}, fmt.Errorf("%w: revision %d belongs to %q, not %q", ErrBadCause, c.rev, m.key, c.key)
	case ok:
		return CauseRef{Key: c.key, Rev: c.rev}, nil
	case c.rev > last:
		return CauseRef{}, fmt.Errorf("%w: revision %d doesn't exist yet", ErrBadCause, c.rev)
	default:
		return CauseRef{}, fmt.Errorf("%w: %s@%d: %w", ErrBadCause, c.key, c.rev, ErrCompacted)
	}
}

func checkKey(key string) error {
	if key == "" {
		return fmt.Errorf("%w: empty key", ErrInvalid)
	}
	if len(key) > MaxKeySize {
		return fmt.Errorf("%w: key is %d bytes, the limit is %d", ErrTooLarge, len(key), MaxKeySize)
	}
	if !utf8.ValidString(key) {
		return fmt.Errorf("%w: key is not valid UTF-8", ErrInvalid)
	}
	return nil
}

func checkText(name, s string, limit int) error {
	if len(s) > limit {
		return fmt.Errorf("%w: %s is %d bytes, the limit is %d", ErrTooLarge, name, len(s), limit)
	}
	if !utf8.ValidString(s) {
		return fmt.Errorf("%w: %s is not valid UTF-8", ErrInvalid, name)
	}
	return nil
}

// Update runs fn in a transaction. If fn returns nil, every write it made is
// committed together; otherwise none is. The options are defaults for every
// write inside: a reason, an actor, a ref, and causes added to each write.
//
// The store has one writer at a time, so fn must not call Put, Delete,
// Update, Compact or Close on the same store: they would wait for fn to
// finish, forever. Reads are fine. A Tx is for the goroutine running fn.
func (db *DB) Update(ctx context.Context, fn func(tx *Tx) error, opts ...WriteOption) error {
	_, err := db.update(ctx, fn, opts)
	return err
}

func (db *DB) update(ctx context.Context, fn func(tx *Tx) error, opts []WriteOption) ([]Revision, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	db.wmu.Lock()
	defer db.wmu.Unlock()
	db.mu.RLock()
	closed, last, lastTx, failed := db.closed, db.lastRev, db.lastTx, db.failed
	db.mu.RUnlock()
	if closed {
		return nil, ErrClosed
	}
	if db.opts.ReadOnly {
		return nil, ErrReadOnly
	}
	if failed != nil {
		return nil, fmt.Errorf("%w: %v", ErrFailed, failed)
	}
	if last >= math.MaxUint64-MaxTxSize || lastTx >= math.MaxUint64-1 {
		return nil, fmt.Errorf("%w: revision numbers are used up", ErrTooLarge)
	}
	tx := &Tx{
		db: db, ctx: ctx, txid: lastTx + 1, nextRev: last + 1,
		latest: map[string]*rec{}, byRev: map[uint64]*rec{},
		now: db.opts.Now().UnixNano(), limit: MaxTxSize,
	}
	for _, f := range opts {
		f(&tx.defaults)
	}
	err := fn(tx)
	tx.finished = true
	if err != nil {
		return nil, err
	}
	if len(tx.recs) == 0 {
		return nil, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	buf := make([]byte, 0, tx.size+64)
	for _, f := range tx.frames {
		buf = append(buf, f...)
	}
	buf = append(buf, frame(encodeCommit(commit{tx: tx.txid, count: uint64(len(tx.recs)), lastRev: tx.recs[len(tx.recs)-1].rev}))...)

	start := db.end
	if _, err := db.st.WriteAt(buf, start); err != nil {
		db.fail(err, start)
		return nil, err
	}
	if !db.opts.NoSync {
		if err := db.st.Sync(); err != nil {
			// After a failed fsync nobody can say what is on disk, and a
			// later fsync may report success over lost data. Stop writing
			// through this handle; reopening reads back what is there.
			db.fail(err, start)
			return nil, err
		}
	}

	db.mu.Lock()
	off := start
	out := make([]Revision, len(tx.recs))
	for i, r := range tx.recs {
		db.addRev(revMeta{rev: r.rev, tx: r.tx, off: off, n: uint32(len(tx.frames[i])), deleted: r.deleted, history: r.history, time: r.time, key: r.key, origin: r.origin})
		off += int64(len(tx.frames[i]))
		out[i] = r.revision()
	}
	db.end = start + int64(len(buf))
	db.lastRev = tx.recs[len(tx.recs)-1].rev
	db.lastTx = tx.txid
	db.dirty = true
	db.mu.Unlock()
	return out, nil
}

// fail marks the handle failed after a write error: no more writes go
// through it. It tries to take the half-done write back so readers of this
// handle and the file agree, but the next Open decides what survived.
func (db *DB) fail(err error, start int64) {
	db.st.Truncate(start)
	db.mu.Lock()
	db.failed = err
	db.mu.Unlock()
}

// Put sets key to value and returns the new revision.
func (db *DB) Put(ctx context.Context, key string, value []byte, opts ...WriteOption) (Revision, error) {
	revs, err := db.update(ctx, func(tx *Tx) error {
		_, err := tx.Put(key, value)
		return err
	}, opts)
	if err != nil {
		return Revision{}, err
	}
	return revs[0], nil
}

// Delete removes key and returns the delete revision. Deleting a key that
// doesn't exist is allowed and recorded.
func (db *DB) Delete(ctx context.Context, key string, opts ...WriteOption) (Revision, error) {
	revs, err := db.update(ctx, func(tx *Tx) error {
		_, err := tx.Delete(key)
		return err
	}, opts)
	if err != nil {
		return Revision{}, err
	}
	return revs[0], nil
}
