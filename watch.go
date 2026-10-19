// Copyright xkv.org 2026
// SPDX-License-Identifier: Apache-2.0

package xkv

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"
)

// WatchOptions choose what Watch delivers.
type WatchOptions struct {
	// Since delivers the revisions numbered above Since. Pass the store's
	// LastRev (from Stats) to see only what is written from now on, or the
	// last revision a watcher saw to carry on where it stopped.
	Since uint64

	// Prefix delivers only revisions of keys that start with it. Empty
	// means every key.
	Prefix string

	// Poll is how often a read-only handle looks for writes made by
	// another process. The default is 200 milliseconds. A writable handle
	// doesn't poll: it is told of each commit.
	Poll time.Duration

	// SkipCompacted delivers what the store still holds when compaction
	// has removed revisions Watch hadn't reached, instead of stopping with
	// ErrCompacted.
	SkipCompacted bool
}

// watchBatch is how many revisions Watch reads under one lock.
const watchBatch = 256

// Watch calls fn with every revision numbered above opts.Since, in order:
// first those already in the store, then each new one once its whole
// transaction has committed. It returns when ctx is done (ctx.Err()), when
// fn returns an error (that error), or when the store is closed (ErrClosed).
//
// Revisions imported as history are delivered too, with History set, like
// every other revision; skip them if you only want values.
//
// If compaction removed revisions above the point Watch had reached, before
// it could deliver them, Watch returns an error wrapping ErrCompacted that
// says which ones: a watcher never misses a revision without being told.
//
// On a read-only handle, Watch follows writes made by another process to
// the same file (see Refresh), checking every opts.Poll.
//
// fn runs on Watch's goroutine and holds no lock: it may read from the
// store, and write to it too.
func (db *DB) Watch(ctx context.Context, opts WatchOptions, fn func(Revision) error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	poll := opts.Poll
	if poll <= 0 {
		poll = 200 * time.Millisecond
	}
	follow := db.opts.ReadOnly && db.path != ""
	pos := opts.Since
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		batch, wake, full, err := db.watchRead(pos, opts.SkipCompacted)
		if err != nil {
			return err
		}
		for _, r := range batch {
			if strings.HasPrefix(r.Key, opts.Prefix) {
				if err := fn(r); err != nil {
					return err
				}
			}
			pos = r.Rev
		}
		if full {
			continue
		}
		if follow {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-wake:
			case <-time.After(poll):
				if err := db.Refresh(); err != nil {
					return err
				}
			}
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-wake:
		}
	}
}

// watchRead returns up to watchBatch revisions above pos, the channel that
// closes on the next change, and whether there may be more right away.
func (db *DB) watchRead(pos uint64, skip bool) ([]Revision, <-chan struct{}, bool, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return nil, nil, false, ErrClosed
	}
	i := sort.Search(len(db.revs), func(i int) bool { return db.revs[i].rev > pos })
	metas := db.revs[i:min(i+watchBatch, len(db.revs))]
	// Revision numbers are handed out one after another, so a number
	// missing between pos and what the store holds was compacted away.
	next := pos + 1
	if len(metas) > 0 && metas[0].rev != next && !skip {
		return nil, nil, false, fmt.Errorf("%w: revisions %d to %d were removed by compaction before Watch reached them", ErrCompacted, next, metas[0].rev-1)
	}
	if len(metas) == 0 && db.lastRev > pos && db.watchGap(pos) && !skip {
		return nil, nil, false, fmt.Errorf("%w: revisions %d to %d were removed by compaction before Watch reached them", ErrCompacted, next, db.lastRev)
	}
	out := make([]Revision, 0, len(metas))
	for j, m := range metas {
		if j > 0 && m.rev != metas[j-1].rev+1 && !skip {
			// Deliver up to the hole; the next read reports it.
			metas = metas[:j]
			break
		}
		r, err := db.readRec(m)
		if err != nil {
			return nil, nil, false, err
		}
		out = append(out, r.revision())
	}
	full := len(metas) == watchBatch || i+len(metas) < len(db.revs)
	return out, db.notify, full, nil
}

// watchGap reports whether revisions above pos are known to be gone: the
// store has counted past pos but holds nothing above it. Called with mu
// held.
func (db *DB) watchGap(pos uint64) bool {
	if len(db.revs) == 0 {
		return true
	}
	return db.revs[len(db.revs)-1].rev <= pos
}

// changed wakes every Watch. Called with mu held for writing.
func (db *DB) changed() {
	close(db.notify)
	db.notify = make(chan struct{})
}

// Refresh brings a read-only handle up to date with what another process
// has committed to the file since it was opened or last refreshed: new
// transactions are added, and if the file was replaced by a compaction it is
// opened again. Half-written transactions at the end are left for later. On
// a writable handle, which sees its own writes already, it does nothing.
func (db *DB) Refresh() error {
	if !db.opts.ReadOnly || db.path == "" {
		return nil
	}
	// An Export reading through this handle needs the file to stay put.
	db.xmu.Lock()
	defer db.xmu.Unlock()
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	fi, err := db.file.Stat()
	if err != nil {
		return err
	}
	pi, err := os.Stat(db.path)
	if err != nil {
		return err
	}
	if !os.SameFile(fi, pi) {
		// Compacted (or replaced) by the writer: read the new file.
		n := newDB(&db.opts)
		n.path = db.path
		f, err := os.Open(db.path)
		if err != nil {
			return err
		}
		n.file, n.st = f, &fileStorage{f}
		if err := n.load(true); err != nil {
			f.Close()
			return err
		}
		db.st.Close()
		db.file, db.st, db.hdr = n.file, n.st, n.hdr
		db.revs, db.keys, db.origins, db.hist = n.revs, n.keys, n.origins, n.hist
		db.keySeen, db.keyTime = n.keySeen, n.keyTime
		db.end, db.lastRev, db.lastTx = n.end, max(db.lastRev, n.lastRev), max(db.lastTx, n.lastTx)
		db.changed()
		return nil
	}
	size := pi.Size()
	if size <= db.end {
		return nil
	}
	before := len(db.revs)
	res, err := scan(db.st, size, db.end, db.lastRev, db.lastTx, db.addRev)
	if err != nil {
		return err
	}
	if res.corrupt != "" {
		return fmt.Errorf("%w at offset %d: %s", ErrCorrupt, res.badAt, res.corrupt)
	}
	db.end, db.lastRev, db.lastTx = res.end, res.lastRev, res.lastTx
	if len(db.revs) != before {
		db.changed()
	}
	return nil
}
