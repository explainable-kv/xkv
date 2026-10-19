// Copyright xkv.org 2026
// SPDX-License-Identifier: Apache-2.0

package xkv

import (
	"context"
	"fmt"
	"os"
	"sort"
	"time"
)

const compactSuffix = ".compact"

// CompactOptions choose which revisions compaction keeps. Whatever they say,
// the latest revision of every key is kept, and so is every revision a kept
// revision reaches through its causes.
type CompactOptions struct {
	// KeepLast keeps the newest KeepLast revisions of every key. Values
	// below 1 mean 1.
	KeepLast int

	// KeepSince keeps every revision written at or after this time. The
	// zero time keeps nothing extra.
	KeepSince time.Time
}

// CompactStats reports what a compaction did.
type CompactStats struct {
	RevisionsBefore int
	RevisionsAfter  int
	KeptForCauses   int // revisions kept only because a kept revision points at them
	BytesBefore     int64
	BytesAfter      int64
}

// Compact rewrites the store without the revisions the options don't keep.
// Writes wait while it runs; reads go on. The new file replaces the old one
// with an atomic rename, so a compaction cut short leaves the old file whole.
func (db *DB) Compact(ctx context.Context, opts CompactOptions) (CompactStats, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	db.wmu.Lock()
	defer db.wmu.Unlock()

	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return CompactStats{}, ErrClosed
	}
	if db.opts.ReadOnly {
		db.mu.RUnlock()
		return CompactStats{}, ErrReadOnly
	}
	st := CompactStats{RevisionsBefore: len(db.revs), BytesBefore: db.end}
	keep, causeOnly, err := db.markLive(ctx, opts)
	if err != nil {
		db.mu.RUnlock()
		return st, err
	}
	st.KeptForCauses = causeOnly

	// Copy the kept frames, in revision order, into a new file. Frames are
	// copied byte for byte; each transaction's commit record is rewritten
	// with the count of revisions that remain in it.
	revs := make([]revMeta, 0, len(keep))
	for _, m := range db.revs {
		if keep[m.rev] {
			revs = append(revs, m)
		}
	}
	// A kept revision whose key had revisions just before it that are now
	// dropped (or were dropped by an earlier compaction) is a gap: its
	// previous value, and the key's value in between, are no longer known.
	gap := map[uint64]bool{}
	for _, k := range db.keys {
		for i, rev := range k.revs {
			if !keep[rev] {
				continue
			}
			m, _ := db.findRev(rev)
			if m.gap || (i > 0 && !keep[k.revs[i-1]]) {
				gap[rev] = true
			}
		}
	}
	h, err := newHeader()
	if err != nil {
		db.mu.RUnlock()
		return st, err
	}
	var out storage
	var tmpF *os.File
	tmp := db.path + compactSuffix
	if db.path == "" {
		out = &memStorage{}
	} else {
		mode := fileMode(db.path)
		tmpF, err = os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, mode)
		if err == nil {
			err = tmpF.Chmod(mode)
		}
		if err != nil {
			if tmpF != nil {
				tmpF.Close()
				os.Remove(tmp)
			}
			db.mu.RUnlock()
			return st, err
		}
		if err := lockFile(tmpF); err != nil {
			tmpF.Close()
			db.mu.RUnlock()
			return st, err
		}
		out = &fileStorage{tmpF}
	}
	fail := func(err error) (CompactStats, error) {
		db.mu.RUnlock()
		if tmpF != nil {
			tmpF.Close()
			os.Remove(tmp)
		}
		return st, err
	}
	w := &bufWriter{st: out, off: 0}
	w.write(h.encode())
	for i := 0; i < len(revs); {
		if err := ctx.Err(); err != nil {
			return fail(err)
		}
		txid := revs[i].tx
		var count, last uint64
		var gaps []uint64
		for ; i < len(revs) && revs[i].tx == txid; i++ {
			m := revs[i]
			b := make([]byte, m.n)
			if _, err := db.st.ReadAt(b, m.off); err != nil {
				return fail(err)
			}
			if body, total := checkFrame(b); body == nil || total != int(m.n) {
				return fail(fmt.Errorf("%w: revision %d fails its checksum", ErrCorrupt, m.rev))
			}
			w.write(b)
			count++
			last = m.rev
			if gap[m.rev] {
				gaps = append(gaps, m.rev)
			}
		}
		w.write(frame(encodeCommit(commit{tx: txid, count: count, lastRev: last, gaps: gaps})))
	}
	if err := w.flush(); err != nil {
		return fail(err)
	}
	if err := out.Sync(); err != nil {
		return fail(err)
	}
	db.mu.RUnlock()

	// Swap. Readers wait for this part only.
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.path == "" {
		db.st = out
	} else {
		nf, swapped, err := replaceFile(tmpF, tmp, db.path, db.file)
		switch {
		case !swapped && nf != nil:
			// The rename didn't happen; the store goes on as it was (on
			// Windows through a handle opened again).
			os.Remove(tmp)
			if nf != db.file {
				db.file = nf
				db.st = &fileStorage{nf}
			}
			return st, fmt.Errorf("xkv: compaction couldn't replace the file, nothing changed: %w", err)
		case nf == nil:
			// The old handle is gone and no new one could be had.
			os.Remove(tmp)
			db.closed = true
			return st, fmt.Errorf("xkv: compaction couldn't finish replacing the file, reopen the store: %w", err)
		}
		db.file = nf
		db.st = &fileStorage{nf}
	}
	db.hdr = h
	db.keys = map[string]*keyState{}
	db.revs = db.revs[:0:0]
	lastRev, lastTx := db.lastRev, db.lastTx
	size, err := db.st.Size()
	if err != nil {
		db.closed = true
		db.st.Close()
		return st, err
	}
	res, err := scan(db.st, size, headerSize, 0, 0, db.addRev)
	if err != nil || res.end != size || res.corrupt != "" {
		db.closed = true
		db.st.Close()
		if err == nil {
			err = fmt.Errorf("%w: the compacted file doesn't read back whole", ErrCorrupt)
		}
		return st, err
	}
	db.end = res.end
	// The newest revision is always kept (it is its key's latest), so the
	// counters read back unchanged; keep them explicitly all the same.
	db.lastRev, db.lastTx = max(lastRev, res.lastRev), max(lastTx, res.lastTx)
	st.RevisionsAfter = len(db.revs)
	st.BytesAfter = db.end
	db.dirty = true
	if db.path != "" {
		db.writeSnapshot()
	}
	return st, nil
}

// markLive returns the set of revisions to keep, and how many of them are
// kept only because a kept revision points at them. Called with mu held for
// reading.
func (db *DB) markLive(ctx context.Context, opts CompactOptions) (map[uint64]bool, int, error) {
	keepLast := opts.KeepLast
	if keepLast < 1 {
		keepLast = 1
	}
	since := int64(0)
	useSince := !opts.KeepSince.IsZero()
	if useSince {
		since = opts.KeepSince.UnixNano()
	}
	keep := map[uint64]bool{}
	var queue []uint64
	for _, k := range db.keys {
		for i, rev := range k.revs {
			if i >= len(k.revs)-keepLast {
				keep[rev] = true
				continue
			}
			if useSince {
				if m, ok := db.findRev(rev); ok && m.time >= since {
					keep[rev] = true
				}
			}
		}
	}
	for rev := range keep {
		queue = append(queue, rev)
	}
	sort.Slice(queue, func(i, j int) bool { return queue[i] > queue[j] })
	causeOnly := 0
	for len(queue) > 0 {
		if err := ctx.Err(); err != nil {
			return nil, 0, err
		}
		rev := queue[len(queue)-1]
		queue = queue[:len(queue)-1]
		m, ok := db.findRev(rev)
		if !ok {
			continue
		}
		r, err := db.readRec(m)
		if err != nil {
			return nil, 0, err
		}
		for _, c := range r.causes {
			if !keep[c.Rev] {
				keep[c.Rev] = true
				causeOnly++
				queue = append(queue, c.Rev)
			}
		}
	}
	return keep, causeOnly, nil
}

// bufWriter batches writes to a storage.
type bufWriter struct {
	st  storage
	off int64
	buf []byte
	err error
}

func (w *bufWriter) write(b []byte) {
	if w.err != nil {
		return
	}
	w.buf = append(w.buf, b...)
	if len(w.buf) >= 1<<20 {
		w.flush()
	}
}

func (w *bufWriter) flush() error {
	if w.err != nil || len(w.buf) == 0 {
		return w.err
	}
	if _, err := w.st.WriteAt(w.buf, w.off); err != nil {
		w.err = err
		return err
	}
	w.off += int64(len(w.buf))
	w.buf = w.buf[:0]
	return nil
}
