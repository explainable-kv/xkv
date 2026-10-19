// Copyright xkv.org 2026
// SPDX-License-Identifier: Apache-2.0

package xkv

import (
	"context"
	"sort"
	"strings"
)

func (db *DB) readGuard(ctx context.Context) error {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	if db.closed {
		return ErrClosed
	}
	return nil
}

// Get returns key's current revision. It returns ErrNotFound if the key
// has never been written or its latest revision is a delete.
func (db *DB) Get(ctx context.Context, key string) (Revision, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if err := db.readGuard(ctx); err != nil {
		return Revision{}, err
	}
	k, ok := db.keys[key]
	if !ok {
		return Revision{}, ErrNotFound
	}
	m, _ := db.findRev(k.latest())
	if m.deleted {
		return Revision{}, ErrNotFound
	}
	r, err := db.readRec(m)
	if err != nil {
		return Revision{}, err
	}
	return r.revision(), nil
}

// GetAt returns key's revision as the store stood at revision rev: the
// newest revision of key numbered rev or lower. It returns ErrNotFound if
// the key had no value then (never written yet, or deleted), and
// ErrCompacted if compaction removed the revisions that would answer.
func (db *DB) GetAt(ctx context.Context, key string, rev uint64) (Revision, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if err := db.readGuard(ctx); err != nil {
		return Revision{}, err
	}
	k, ok := db.keys[key]
	if !ok {
		return Revision{}, ErrNotFound
	}
	i := sort.Search(len(k.revs), func(i int) bool { return k.revs[i] > rev })
	// Revisions of the key between the one kept before rev and the one kept
	// after it may have been compacted away; if so, the store can't say,
	// unless rev is the kept revision itself.
	if i < len(k.revs) && (i == 0 || k.revs[i-1] != rev) {
		if next, _ := db.findRev(k.revs[i]); next.gap {
			return Revision{}, ErrCompacted
		}
	}
	if i == 0 {
		return Revision{}, ErrNotFound
	}
	m, _ := db.findRev(k.revs[i-1])
	if m.deleted {
		return Revision{}, ErrNotFound
	}
	r, err := db.readRec(m)
	if err != nil {
		return Revision{}, err
	}
	return r.revision(), nil
}

// Revision returns revision rev exactly, whatever key it belongs to. A
// revision removed by compaction gives ErrCompacted; one never written gives
// ErrNotFound.
func (db *DB) Revision(ctx context.Context, rev uint64) (Revision, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if err := db.readGuard(ctx); err != nil {
		return Revision{}, err
	}
	m, ok := db.findRev(rev)
	if !ok {
		if rev >= 1 && rev <= db.lastRev {
			return Revision{}, ErrCompacted
		}
		return Revision{}, ErrNotFound
	}
	r, err := db.readRec(m)
	if err != nil {
		return Revision{}, err
	}
	return r.revision(), nil
}

// History returns key's revisions, newest first, deletes included, and
// revisions imported as history (marked History) among them. A limit of 0
// or less returns all of them.
func (db *DB) History(ctx context.Context, key string, limit int) ([]Revision, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if err := db.readGuard(ctx); err != nil {
		return nil, err
	}
	all := db.allRevs(key)
	if len(all) == 0 {
		return nil, ErrNotFound
	}
	n := len(all)
	if limit > 0 && limit < n {
		n = limit
	}
	out := make([]Revision, 0, n)
	for i := len(all) - 1; i >= 0 && len(out) < n; i-- {
		m, _ := db.findRev(all[i])
		r, err := db.readRec(m)
		if err != nil {
			return nil, err
		}
		out = append(out, r.revision())
	}
	return out, nil
}

// Keys returns the keys that start with prefix and currently have a value,
// sorted. An empty prefix returns them all.
func (db *DB) Keys(ctx context.Context, prefix string) ([]string, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if err := db.readGuard(ctx); err != nil {
		return nil, err
	}
	var out []string
	for name, k := range db.keys {
		if !strings.HasPrefix(name, prefix) {
			continue
		}
		if m, ok := db.findRev(k.latest()); ok && !m.deleted {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out, nil
}

// prevOf returns the index entry of the revision of the same key just before
// m, if the store still holds it. After compaction removed the revisions
// before m (m.gap), the one kept further back is not m's predecessor, so
// there is no answer.
func (db *DB) prevOf(m revMeta) (revMeta, bool) {
	if m.gap || m.history {
		return revMeta{}, false
	}
	k, ok := db.keys[m.key]
	if !ok {
		return revMeta{}, false
	}
	i := sort.Search(len(k.revs), func(i int) bool { return k.revs[i] >= m.rev })
	if i == 0 {
		return revMeta{}, false
	}
	return db.findRev(k.revs[i-1])
}
