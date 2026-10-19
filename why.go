// Copyright xkv.org 2026
// SPDX-License-Identifier: Apache-2.0

package xkv

import "context"

// DefaultWhyDepth is how many causes deep Why goes when asked for depth 0.
const DefaultWhyDepth = 16

// MaxWhyDepth caps the depth Why accepts.
const MaxWhyDepth = 256

// WhyNode is one revision in the tree Why returns, with the revision of the
// same key before it and the revisions that caused it.
type WhyNode struct {
	Revision

	// Prev is the key's revision just before this one, if the store still
	// holds it. Prev.Causes are left out; follow Prev.Rev to see them.
	Prev *Revision

	// Causes are the revisions this one names as causes, each expanded in
	// turn. Empty when the revision names none, or when Truncated or Repeat
	// is set.
	Causes []*WhyNode

	// Truncated is set when the revision has causes but the depth limit
	// stopped the walk before them.
	Truncated bool

	// Repeat is set when this revision already appears elsewhere in the
	// tree (two paths lead to it). Its causes are shown at that first place.
	Repeat bool
}

// Why returns the chain of decisions that led to key's current revision: the
// revision, its causes, their causes, and so on, newest first, down to
// revisions that name no cause or to depth levels (0 means DefaultWhyDepth).
// A key whose latest revision is a delete is explained too: the delete is
// the root.
func (db *DB) Why(ctx context.Context, key string, depth int) (*WhyNode, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if err := db.readGuard(ctx); err != nil {
		return nil, err
	}
	k, ok := db.keys[key]
	if !ok {
		return nil, ErrNotFound
	}
	return db.why(ctx, k.latest(), depth)
}

// WhyRev is Why for one exact revision.
func (db *DB) WhyRev(ctx context.Context, rev uint64, depth int) (*WhyNode, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if err := db.readGuard(ctx); err != nil {
		return nil, err
	}
	if _, ok := db.findRev(rev); !ok {
		if rev >= 1 && rev <= db.lastRev {
			return nil, ErrCompacted
		}
		return nil, ErrNotFound
	}
	return db.why(ctx, rev, depth)
}

func (db *DB) why(ctx context.Context, rev uint64, depth int) (*WhyNode, error) {
	if depth <= 0 {
		depth = DefaultWhyDepth
	}
	if depth > MaxWhyDepth {
		depth = MaxWhyDepth
	}
	seen := map[uint64]bool{}
	var walk func(rev uint64, level int) (*WhyNode, error)
	walk = func(rev uint64, level int) (*WhyNode, error) {
		if ctx != nil {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		m, ok := db.findRev(rev)
		if !ok {
			// Compaction keeps every revision a kept one points at, so
			// this only happens with a damaged file.
			return nil, ErrCompacted
		}
		r, err := db.readRec(m)
		if err != nil {
			return nil, err
		}
		n := &WhyNode{Revision: r.revision()}
		if pm, ok := db.prevOf(m); ok {
			pr, err := db.readRec(pm)
			if err != nil {
				return nil, err
			}
			p := pr.revision()
			p.Causes = nil
			n.Prev = &p
		}
		if seen[rev] {
			n.Repeat = len(r.causes) > 0
			return n, nil
		}
		seen[rev] = true
		if len(r.causes) == 0 {
			return n, nil
		}
		if level >= depth {
			n.Truncated = true
			return n, nil
		}
		for _, c := range r.causes {
			child, err := walk(c.Rev, level+1)
			if err != nil {
				return nil, err
			}
			n.Causes = append(n.Causes, child)
		}
		return n, nil
	}
	return walk(rev, 0)
}
