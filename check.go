// Copyright xkv.org 2026
// SPDX-License-Identifier: Apache-2.0

package xkv

import (
	"fmt"
	"io"
	"os"
)

// CheckReport is what Check found.
type CheckReport struct {
	Revisions    int
	Keys         int
	Transactions int
	LastRev      uint64
	TailBytes    int64    // bytes after the last complete transaction; the next writable open trims them
	Problems     []string // empty when the file is sound
}

// OK reports whether the check found no problems.
func (r CheckReport) OK() bool { return len(r.Problems) == 0 }

// Check reads every record of the file at path, ignoring the index
// snapshot, and confirms each revision reads back whole and every cause
// points at a revision of the named key that the file still holds. It opens
// the file read-only and changes nothing.
func Check(path string) (CheckReport, error) {
	var rep CheckReport
	f, err := os.Open(path)
	if err != nil {
		return rep, err
	}
	defer f.Close()
	db := newDB(&Options{ReadOnly: true})
	db.file = f
	db.st = &fileStorage{f}
	size, err := db.st.Size()
	if err != nil {
		return rep, err
	}
	if size == 0 {
		return rep, nil
	}
	hb := make([]byte, min(size, maxHeader))
	if _, err := f.ReadAt(hb, 0); err != nil && err != io.EOF {
		return rep, err
	}
	if db.hdr, err = decodeHeader(hb); err != nil {
		rep.Problems = append(rep.Problems, "header: "+err.Error())
		return rep, nil
	}
	res, err := scan(db.st, size, db.hdr.size(), 0, 0, db.addRev)
	if err != nil {
		return rep, err
	}
	rep.Revisions = len(db.revs)
	rep.Keys = len(db.keys)
	rep.Transactions = res.txs
	rep.LastRev = res.lastRev
	rep.TailBytes = size - res.end
	if res.corrupt != "" {
		rep.Problems = append(rep.Problems, fmt.Sprintf("offset %d: %s", res.badAt, res.corrupt))
	}
	for _, m := range db.revs {
		r, err := db.readRec(m)
		if err != nil {
			rep.Problems = append(rep.Problems, err.Error())
			continue
		}
		if r.origin != nil && db.hdr.version < 2 {
			rep.Problems = append(rep.Problems, fmt.Sprintf("revision %d carries an import identity, which a format 1 file can't", m.rev))
		}
		for _, c := range r.causes {
			cm, ok := db.findRev(c.Rev)
			switch {
			case !ok:
				rep.Problems = append(rep.Problems, fmt.Sprintf("revision %d (%s) names cause %s, which the file doesn't hold", m.rev, m.key, c))
			case cm.key != c.Key:
				rep.Problems = append(rep.Problems, fmt.Sprintf("revision %d names cause %s, but revision %d belongs to %q", m.rev, c, c.Rev, cm.key))
			}
		}
		if len(rep.Problems) > 100 {
			rep.Problems = append(rep.Problems, "stopped after 100 problems")
			break
		}
	}
	return rep, nil
}
