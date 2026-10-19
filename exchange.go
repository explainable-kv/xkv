// Copyright xkv.org 2026
// SPDX-License-Identifier: Apache-2.0

package xkv

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// ExchangeVersion is the version of the exchange file format Export writes
// and Import reads. docs/exchange.md in the repository describes it.
const ExchangeVersion = 1

// ErrBadExchange is returned by Import for a file that isn't a whole, valid
// exchange file. Nothing is written when it is returned.
var ErrBadExchange = errors.New("xkv: not a valid exchange file")

// ExportOptions choose what Export writes.
type ExportOptions struct {
	// Since exports the revisions numbered above Since: 0 exports them
	// all. Pass the Through of the previous export to send only what is
	// new since then.
	Since uint64
}

// ExportStats describes an export.
type ExportStats struct {
	Store     string // the exporting store's id
	Since     uint64 // as asked
	Through   uint64 // the store's last revision at the time: the Since for the next export
	Revisions int    // revisions exported as changes
	Context   int    // older revisions included because exported ones name them as causes
}

// ConflictPolicy says which value Import keeps when a key was changed on
// both sides since they last exchanged.
type ConflictPolicy int

const (
	// NewestWins keeps the value written later, by the time recorded on
	// each revision; for the same time, a value beats a delete and then the
	// bytewise larger value wins. Both stores pick the same winner, so two
	// stores that exchange both ways end up with the same values. The
	// default.
	NewestWins ConflictPolicy = iota
	// KeepOurs keeps the importing store's value.
	KeepOurs
	// TakeTheirs takes the imported value.
	TakeTheirs
)

// String returns "newest", "ours" or "theirs".
func (p ConflictPolicy) String() string {
	switch p {
	case KeepOurs:
		return "ours"
	case TakeTheirs:
		return "theirs"
	}
	return "newest"
}

// ParseConflictPolicy reads "newest", "ours" or "theirs".
func ParseConflictPolicy(s string) (ConflictPolicy, error) {
	switch s {
	case "", "newest":
		return NewestWins, nil
	case "ours":
		return KeepOurs, nil
	case "theirs":
		return TakeTheirs, nil
	}
	return 0, fmt.Errorf("%w: conflict policy %q (want newest, ours or theirs)", ErrInvalid, s)
}

// ImportOptions choose how Import treats conflicts.
type ImportOptions struct {
	Conflicts ConflictPolicy
}

// Conflict is one key both stores changed without seeing the other's
// change. Both are kept: the one the policy picks is the key's value, the
// other is in its history.
type Conflict struct {
	Key    string `json:"key"`
	Ours   uint64 `json:"ours"`   // the revision that was current here
	Theirs uint64 `json:"theirs"` // the imported revision, by its number here
	Kept   string `json:"kept"`   // "ours" or "theirs": the one that is the key's value now
}

// ImportReport describes an import.
type ImportReport struct {
	Store     string     `json:"store"`   // the store the file was exported from
	Since     uint64     `json:"since"`   // the export's Since
	Through   uint64     `json:"through"` // the export's Through: ask for the next export since this
	Imported  int        `json:"imported"`
	Context   int        `json:"context"` // older revisions imported as history
	Skipped   int        `json:"skipped"` // already here
	Agreed    int        `json:"agreed"`  // changed on both sides to the same value; the newer stands, the other is history
	Conflicts []Conflict `json:"conflicts"`
}

// The lines of an exchange file. See docs/exchange.md.
type exHeader struct {
	Exchange int       `json:"xkv_exchange"`
	Store    string    `json:"store"`
	Since    uint64    `json:"since"`
	Through  uint64    `json:"through"`
	Created  time.Time `json:"created"`
}

type exCause struct {
	Key string `json:"key"`
	ID  string `json:"id"`
}

type exRev struct {
	ID       string            `json:"id"`
	Key      string            `json:"key"`
	Value    *string           `json:"value,omitempty"`
	Encoding string            `json:"encoding,omitempty"`
	Deleted  bool              `json:"deleted,omitempty"`
	Actor    string            `json:"actor"`
	Reason   string            `json:"reason"`
	Ref      string            `json:"ref,omitempty"`
	Time     time.Time         `json:"time"`
	Causes   []exCause         `json:"causes,omitempty"`
	History  bool              `json:"history,omitempty"`
	Seen     map[string]uint64 `json:"seen,omitempty"`
}

type exLine struct {
	Tx  []exRev `json:"tx,omitempty"`
	End *exEnd  `json:"end,omitempty"`
}

type exEnd struct {
	Lines     int    `json:"lines"`     // lines before this one
	Revisions int    `json:"revisions"` // revisions in them
	SHA256    string `json:"sha256"`    // of every byte before this line
}

func parseIdent(s string) (ident, bool) {
	var id ident
	i := strings.IndexByte(s, ':')
	if i != 32 {
		return id, false
	}
	if _, err := hex.Decode(id.store[:], []byte(s[:32])); err != nil {
		return id, false
	}
	rev, err := strconv.ParseUint(s[33:], 10, 64)
	if err != nil || rev == 0 || s[33] == '0' || s[33] == '+' {
		return id, false
	}
	id.rev = rev
	return id, true
}

// Export writes the revisions numbered above opts.Since to w as an exchange
// file, with their reasons, actors, refs, times and causes, and with every
// older revision they reach through causes, so a chain arrives whole. Import
// reads it into another store. A format 1 store is upgraded first (see
// Upgrade). Reads and writes go on while Export runs; compaction waits.
func (db *DB) Export(ctx context.Context, w io.Writer, opts ExportOptions) (ExportStats, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := db.Upgrade(ctx); err != nil {
		return ExportStats{}, err
	}
	// Compaction would move records under the reads below; it waits.
	// Writers go on: what they add after the start isn't part of this
	// export.
	db.xmu.Lock()
	defer db.xmu.Unlock()

	db.mu.RLock()
	if err := db.readGuard(ctx); err != nil {
		db.mu.RUnlock()
		return ExportStats{}, err
	}
	st := ExportStats{Store: db.hdr.store.String(), Since: opts.Since, Through: db.lastRev}
	// Everything above Since: the changes, and the history this store took
	// in since (losing sides of conflicts, changes both sides made alike),
	// which tells the importer what was seen here.
	first := sort.Search(len(db.revs), func(i int) bool { return db.revs[i].rev > opts.Since })
	var changes []revMeta
	inChanges := map[uint64]bool{}
	var histAbove []revMeta
	for _, m := range db.revs[first:] {
		if !m.history {
			changes = append(changes, m)
			inChanges[m.rev] = true
		} else {
			histAbove = append(histAbove, m)
		}
	}
	// The context: everything the changes reach through causes that isn't
	// a change itself, found by following causes back.
	inContext := map[uint64]bool{}
	queue := make([]uint64, 0, len(changes))
	for _, m := range changes {
		queue = append(queue, m.rev)
	}
	for _, m := range histAbove {
		inContext[m.rev] = true
		queue = append(queue, m.rev)
	}
	for len(queue) > 0 {
		if err := ctx.Err(); err != nil {
			db.mu.RUnlock()
			return st, err
		}
		rev := queue[len(queue)-1]
		queue = queue[:len(queue)-1]
		m, ok := db.findRev(rev)
		if !ok {
			continue
		}
		r, err := db.readRec(m)
		if err != nil {
			db.mu.RUnlock()
			return st, err
		}
		for _, c := range r.causes {
			if !inChanges[c.Rev] && !inContext[c.Rev] {
				inContext[c.Rev] = true
				queue = append(queue, c.Rev)
			}
		}
	}
	// Everything goes out in this store's order, a line per transaction, so
	// every cause comes before the revision naming it.
	out := make([]revMeta, 0, len(changes)+len(inContext))
	out = append(out, changes...)
	for rev := range inContext {
		if m, ok := db.findRev(rev); ok {
			out = append(out, m)
		}
	}
	db.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].rev < out[j].rev })

	h := sha256.New()
	bw := bufio.NewWriterSize(io.MultiWriter(w, h), 1<<16)
	lines, revs := 0, 0
	writeLine := func(v any) error {
		b, err := json.Marshal(v)
		if err != nil {
			return err
		}
		b = append(b, '\n')
		_, err = bw.Write(b)
		lines++
		return err
	}
	if err := writeLine(exHeader{Exchange: ExchangeVersion, Store: st.Store, Since: opts.Since, Through: st.Through, Created: time.Now().UTC()}); err != nil {
		return st, err
	}
	// toEx reads one revision; called with mu held for reading.
	toEx := func(m revMeta) (exRev, error) {
		history := !inChanges[m.rev]
		r, err := db.readRec(m)
		if err != nil {
			return exRev{}, err
		}
		x := exRev{
			ID: db.identOf(m).String(), Key: r.key, Deleted: r.deleted, History: history,
			Actor: r.actor, Reason: r.reason, Ref: r.ref, Time: time.Unix(0, r.time).UTC(),
		}
		if !r.deleted {
			var v string
			if utf8.Valid(r.value) {
				v = string(r.value)
			} else {
				v = base64.StdEncoding.EncodeToString(r.value)
				x.Encoding = "base64"
			}
			x.Value = &v
		}
		for _, c := range r.causes {
			cm, ok := db.findRev(c.Rev)
			if !ok {
				return exRev{}, fmt.Errorf("%w: revision %d names cause %s, which the store doesn't hold", ErrCorrupt, m.rev, c)
			}
			x.Causes = append(x.Causes, exCause{Key: c.Key, ID: db.identOf(cm).String()})
		}
		if len(r.seen) > 0 {
			x.Seen = make(map[string]uint64, len(r.seen))
			for _, e := range r.seen {
				x.Seen[e.store.String()] = e.rev
			}
		}
		return x, nil
	}
	for i := 0; i < len(out); {
		if err := ctx.Err(); err != nil {
			return st, err
		}
		txid := out[i].tx
		var line []exRev
		db.mu.RLock()
		if db.closed {
			db.mu.RUnlock()
			return st, ErrClosed
		}
		for ; i < len(out) && out[i].tx == txid; i++ {
			x, err := toEx(out[i])
			if err != nil {
				db.mu.RUnlock()
				return st, err
			}
			line = append(line, x)
			if x.History {
				st.Context++
			} else {
				st.Revisions++
			}
		}
		db.mu.RUnlock()
		if err := writeLine(exLine{Tx: line}); err != nil {
			return st, err
		}
		revs += len(line)
	}
	if err := bw.Flush(); err != nil {
		return st, err
	}
	end, err := json.Marshal(exLine{End: &exEnd{Lines: lines, Revisions: revs, SHA256: hex.EncodeToString(h.Sum(nil))}})
	if err != nil {
		return st, err
	}
	if _, err := w.Write(append(end, '\n')); err != nil {
		return st, err
	}
	return st, nil
}

// inRev is one revision read from an exchange file and checked.
type inRev struct {
	id        ident
	r         rec // rev, tx and causes unset; causes are in causes
	causes    []ident
	causeKeys []string
	seen      seen // what the source had seen of the key before this change
}

type inGroup struct {
	history bool // every revision in it is history, so it may be split
	revs    []inRev
}

// readExchange reads and checks a whole exchange file. Any problem refuses
// the whole file.
func readExchange(rd io.Reader) (exHeader, []inGroup, error) {
	var hdr exHeader
	bad := func(format string, args ...any) (exHeader, []inGroup, error) {
		return hdr, nil, fmt.Errorf("%w: %s", ErrBadExchange, fmt.Sprintf(format, args...))
	}
	br := bufio.NewReaderSize(rd, 1<<16)
	h := sha256.New()
	var groups []inGroup
	lines, revs := 0, 0
	seen := map[ident]string{} // identity -> key, for every revision so far
	var end *exEnd
	for {
		line, err := br.ReadBytes('\n')
		if err != nil && err != io.EOF {
			return hdr, nil, err
		}
		if len(line) == 0 && err == io.EOF {
			break
		}
		if end != nil {
			return bad("there is more after the end line")
		}
		if len(line) == 0 || line[len(line)-1] != '\n' {
			return bad("line %d is cut short (the file is incomplete)", lines+1)
		}
		dec := json.NewDecoder(bytes.NewReader(line))
		dec.DisallowUnknownFields()
		if lines == 0 {
			if err := dec.Decode(&hdr); err != nil {
				return bad("the first line isn't an exchange header: %v", err)
			}
			if hdr.Exchange != ExchangeVersion {
				return bad("exchange format %d (this is xkv %s, which reads %d)", hdr.Exchange, Version, ExchangeVersion)
			}
			if len(hdr.Store) != 32 {
				return bad("the header names no store")
			}
			if _, err := hex.DecodeString(hdr.Store); err != nil {
				return bad("the header's store id isn't hex")
			}
			if hdr.Since > hdr.Through {
				return bad("the header's since is above its through")
			}
			if !canonical(hdr, line) {
				return bad("the header isn't written the way xkv writes it")
			}
			h.Write(line)
			lines++
			continue
		}
		var l exLine
		if err := dec.Decode(&l); err != nil {
			return bad("line %d: %v", lines+1, err)
		}
		if (l.Tx == nil) == (l.End == nil) {
			return bad("line %d holds neither or both of tx and end", lines+1)
		}
		// Each line must be exactly what Export writes: this refuses extra
		// text after the object, repeated fields, fields named in another
		// case, and anything else a decoder would quietly let through.
		if !canonical(l, line) {
			return bad("line %d isn't written the way xkv writes it", lines+1)
		}
		if l.End != nil {
			end = l.End
			continue
		}
		h.Write(line)
		lines++
		g := inGroup{history: true}
		if len(l.Tx) == 0 {
			return bad("line %d is empty", lines)
		}
		for i, x := range l.Tx {
			g.history = g.history && x.History
			in, err := checkExRev(x, seen)
			if err != nil {
				return bad("line %d, revision %d: %v", lines, i+1, err)
			}
			seen[in.id] = in.r.key
			g.revs = append(g.revs, in)
			revs++
		}
		groups = append(groups, g)
	}
	if lines == 0 {
		return bad("the file is empty")
	}
	if end == nil {
		return bad("there is no end line (the file is incomplete)")
	}
	if end.Lines != lines || end.Revisions != revs || end.SHA256 != hex.EncodeToString(h.Sum(nil)) {
		return bad("the end line doesn't match the file (it was changed or cut)")
	}
	return hdr, groups, nil
}

// canonical reports whether line is exactly v as json.Marshal writes it,
// followed by a newline.
func canonical(v any, line []byte) bool {
	b, err := json.Marshal(v)
	return err == nil && len(line) == len(b)+1 && bytes.Equal(line[:len(b)], b)
}

// The times a revision can carry: those whose nanoseconds since 1970 fit an
// int64.
var (
	minTime = time.Unix(0, math.MinInt64).UTC()
	maxTime = time.Unix(0, math.MaxInt64).UTC()
)

func checkExRev(x exRev, prior map[ident]string) (inRev, error) {
	var in inRev
	id, ok := parseIdent(x.ID)
	if !ok {
		return in, fmt.Errorf("bad id %q", x.ID)
	}
	if _, dup := prior[id]; dup {
		return in, fmt.Errorf("revision %s appears twice", x.ID)
	}
	in.id = id
	if err := checkKey(x.Key); err != nil {
		return in, err
	}
	if x.Deleted {
		if x.Value != nil {
			return in, errors.New("a delete with a value")
		}
	} else {
		if x.Value == nil {
			return in, errors.New("no value")
		}
		switch x.Encoding {
		case "":
			in.r.value = []byte(*x.Value)
		case "base64":
			v, err := base64.StdEncoding.DecodeString(*x.Value)
			if err != nil {
				return in, errors.New("value isn't valid base64")
			}
			in.r.value = v
		default:
			return in, fmt.Errorf("unknown encoding %q", x.Encoding)
		}
		if len(in.r.value) > MaxValueSize {
			return in, fmt.Errorf("%w: value is %d bytes", ErrTooLarge, len(in.r.value))
		}
	}
	for _, t := range []struct {
		name, s string
		max     int
	}{{"actor", x.Actor, MaxActorSize}, {"reason", x.Reason, MaxReasonSize}, {"ref", x.Ref, MaxRefSize}} {
		if err := checkText(t.name, t.s, t.max); err != nil {
			return in, err
		}
	}
	if x.Actor == "" || x.Reason == "" {
		return in, errors.New("no actor or no reason")
	}
	if x.Time.IsZero() || x.Time.Before(minTime) || x.Time.After(maxTime) {
		return in, errors.New("no time, or one out of range")
	}
	if len(x.Causes) > MaxCauses {
		return in, fmt.Errorf("%d causes, the limit is %d", len(x.Causes), MaxCauses)
	}
	for _, c := range x.Causes {
		cid, ok := parseIdent(c.ID)
		if !ok {
			return in, fmt.Errorf("bad cause id %q", c.ID)
		}
		if err := checkKey(c.Key); err != nil {
			return in, fmt.Errorf("cause: %v", err)
		}
		if k, ok := prior[cid]; ok && k != c.Key {
			return in, fmt.Errorf("cause %s is a revision of %q, not %q", c.ID, k, c.Key)
		}
		in.causes = append(in.causes, cid)
		in.causeKeys = append(in.causeKeys, c.Key)
	}
	if len(x.Seen) > 4096 {
		return in, errors.New("a seen list of more than 4096 stores")
	}
	in.seen = seen{}
	for st, rev := range x.Seen {
		var id StoreID
		if len(st) != 32 || rev == 0 {
			return in, fmt.Errorf("bad seen entry %q", st)
		}
		if _, err := hex.Decode(id[:], []byte(st)); err != nil {
			return in, fmt.Errorf("bad seen entry %q", st)
		}
		in.seen[id] = rev
	}
	in.r.key = x.Key
	in.r.deleted = x.Deleted
	in.r.actor = x.Actor
	in.r.reason = x.Reason
	in.r.ref = x.Ref
	in.r.time = x.Time.UnixNano()
	in.r.history = x.History
	o := id
	in.r.origin = &o
	return in, nil
}

// Import reads an exchange file written by Export and adds what this store
// doesn't have yet. Each imported revision keeps its key, value, actor,
// reason, ref, time and causes, gets a number here, and remembers where it
// was first written, so importing the same file again changes nothing and
// causes still point at the right revisions. Revisions the file holds only
// as causes of others are kept as history: Why and History show them, but
// they never become a key's current value.
//
// Each transaction in the file is imported as one transaction here. An
// imported change made knowing everything this store has of its key
// becomes the key's value. One made without seeing a change this store has
// is a conflict: both are kept, the one opts.Conflicts picks as the key's
// value and the other in its history, and the report lists it. Import
// writes nothing of its own.
//
// The whole file is read and checked against the store before anything is
// written; a file that is incomplete, changed, invalid, or that disagrees
// with what the store holds (as a copy of a store, or one restored from a
// backup, would) is refused with ErrBadExchange. A format 1 store is
// upgraded first (see Upgrade). Compaction waits while Import runs; other
// writes go on between its transactions.
func (db *DB) Import(ctx context.Context, rd io.Reader, opts ImportOptions) (ImportReport, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	var rep ImportReport
	if db.opts.ReadOnly {
		return rep, ErrReadOnly
	}
	hdr, groups, err := readExchange(rd)
	if err != nil {
		return rep, err
	}
	rep.Store, rep.Since, rep.Through = hdr.Store, hdr.Since, hdr.Through
	rep.Conflicts = []Conflict{}
	if _, err := db.checkAgainst(groups); err != nil {
		return rep, err
	}
	if err := db.Upgrade(ctx); err != nil {
		return rep, err
	}
	// Compaction waits until the import is done, so nothing the checks
	// found here is dropped part-way.
	db.xmu.Lock()
	defer db.xmu.Unlock()
	needed, err := db.checkAgainst(groups)
	if err != nil {
		return rep, err
	}
	im := &importer{db: db, opts: opts, needed: needed, rep: &rep}
	for _, g := range groups {
		if err := ctx.Err(); err != nil {
			return rep, err
		}
		if g.history {
			// History doesn't change any key's value, so it can go in as
			// several transactions, each kept under the size limit.
			for i := 0; i < len(g.revs); {
				n, err := im.history(ctx, g.revs[i:])
				if err != nil {
					return rep, err
				}
				i += n
			}
			continue
		}
		if err := im.tx(ctx, g.revs); err != nil {
			return rep, err
		}
	}
	return rep, nil
}

// checkAgainst checks a file against what the store holds, and returns the
// identities that revisions the import will add name as causes.
//
//   - A revision the store holds, by identity, must hold the same thing. Two
//     stores sharing a store id (a copied file, or one restored from a
//     backup and written to since) give the same identity to different
//     revisions; importing would mix them up.
//   - Nothing may claim this store's id with a number it hasn't reached:
//     no revision, and no seen list.
//   - Every cause is earlier in the file or held here, as a revision of the
//     key the file says.
//   - No time lies more than a year past this store's clock: a write here
//     is recorded later than every revision of its key, so a time at the
//     end of the range would leave no room for one.
//   - No key would have seen more stores than a revision's seen list can
//     hold.
func (db *DB) checkAgainst(groups []inGroup) (map[ident]bool, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return nil, ErrClosed
	}
	needed := map[ident]bool{}
	inFile := map[ident]bool{}
	own := db.hdr.version >= 2
	future := func(id ident) bool { return own && id.store == db.hdr.store && id.rev > db.lastRev }
	latest := db.opts.Now().Add(maxTimeAhead).UnixNano()
	stores := map[string]map[StoreID]bool{} // per key in the file, the stores it would have seen
	note := func(key string, st StoreID) error {
		if own && st == db.hdr.store {
			return nil
		}
		s := stores[key]
		if s == nil {
			s = map[StoreID]bool{}
			for _, e := range db.keySeen[key] {
				if !own || e.store != db.hdr.store {
					s[e.store] = true
				}
			}
			stores[key] = s
		}
		s[st] = true
		if len(s) > maxSeen-1 {
			return fmt.Errorf("%w: key %q would have seen revisions from more than %d stores", ErrBadExchange, key, maxSeen-1)
		}
		return nil
	}
	for _, g := range groups {
		for _, in := range g.revs {
			if in.r.time > latest {
				return nil, fmt.Errorf("%w: revision %s is dated %s, more than a year past this store's clock", ErrBadExchange, in.id, time.Unix(0, in.r.time).UTC().Format(time.RFC3339))
			}
			if err := note(in.r.key, in.id.store); err != nil {
				return nil, err
			}
			for st := range in.seen {
				if err := note(in.r.key, st); err != nil {
					return nil, err
				}
			}
			rev, held := db.lookupIdent(in.id)
			if held {
				m, _ := db.findRev(rev)
				r, err := db.readRec(m)
				if err != nil {
					return nil, err
				}
				if r.key != in.r.key || r.deleted != in.r.deleted || !bytes.Equal(r.value, in.r.value) || r.time != in.r.time || r.actor != in.r.actor || r.reason != in.r.reason || r.ref != in.r.ref {
					return nil, fmt.Errorf("%w: this store holds a different revision under the identity %s: the two stores share a store id (a copied file, or one restored from a backup); give the copy its own with xkv new-id", ErrBadExchange, in.id)
				}
			} else if future(in.id) {
				return nil, fmt.Errorf("%w: the file holds revision %s, which claims to come from this store but is above anything it wrote: it was restored from a backup or copied; give the copy its own id with xkv new-id", ErrBadExchange, in.id)
			}
			for st, rev := range in.seen {
				if future(ident{st, rev}) {
					return nil, fmt.Errorf("%w: revision %s claims to have seen revision %d of this store, which it hasn't written yet", ErrBadExchange, in.id, rev)
				}
			}
			for i, c := range in.causes {
				if !held {
					needed[c] = true
				}
				if inFile[c] {
					continue
				}
				crev, ok := db.lookupIdent(c)
				if !ok {
					return nil, fmt.Errorf("%w: %s names cause %s, which neither the file nor this store holds", ErrBadExchange, in.id, c)
				}
				if m, _ := db.findRev(crev); m.key != in.causeKeys[i] {
					return nil, fmt.Errorf("%w: cause %s of %s is a revision of %q here, not %q", ErrBadExchange, c, in.id, m.key, in.causeKeys[i])
				}
			}
			inFile[in.id] = true
		}
	}
	return needed, nil
}

// maxTimeAhead is how far past this store's clock an imported revision's
// time may lie.
const maxTimeAhead = 366 * 24 * time.Hour

// importTxLimit is how large an imported transaction may grow here: a
// source transaction up to MaxTxSize, plus the identity each revision now
// carries (at most 26 bytes on a revision of at least 30).
const importTxLimit = 2 * MaxTxSize

// importer carries one Import's settings and counts.
type importer struct {
	db     *DB
	opts   ImportOptions
	needed map[ident]bool // identities imported revisions name as causes
	rep    *ImportReport
}

// newer reports whether a wins over b under NewestWins: the later time, then,
// for the same time, a delete loses to a value and the bytewise larger value
// wins. It looks only at what the revisions hold, so every store comparing
// the same pair agrees.
func newer(a, b *rec) bool {
	if a.time != b.time {
		return a.time > b.time
	}
	if a.deleted != b.deleted {
		return b.deleted
	}
	return bytes.Compare(a.value, b.value) > 0
}

func sameValue(a, b *rec) bool {
	return a.deleted == b.deleted && bytes.Equal(a.value, b.value)
}

// lookup finds identity id here: written earlier in this transaction
// (pending) or committed.
func (db *DB) lookup(id ident, pending map[ident]uint64) (uint64, bool) {
	if rev, ok := pending[id]; ok {
		return rev, true
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.lookupIdent(id)
}

// localCauses maps an imported revision's causes to revisions here.
func (db *DB) localCauses(in inRev, pending map[ident]uint64) ([]CauseRef, error) {
	out := make([]CauseRef, 0, len(in.causes))
	seenRev := map[uint64]bool{}
	for i, c := range in.causes {
		rev, ok := db.lookup(c, pending)
		if !ok {
			return nil, fmt.Errorf("%w: cause %s of %s isn't here", ErrBadCause, c, in.id)
		}
		if seenRev[rev] {
			continue
		}
		seenRev[rev] = true
		out = append(out, CauseRef{Key: in.causeKeys[i], Rev: rev})
	}
	return out, nil
}

// add writes one imported revision into tx, as history when asked. A
// revision that becomes a value keeps the seen list it came with.
func (im *importer) add(tx *Tx, in inRev, history bool, pending map[ident]uint64) (uint64, error) {
	causes, err := im.db.localCauses(in, pending)
	if err != nil {
		return 0, err
	}
	r := in.r
	r.causes = causes
	r.history = history
	r.seen = in.seen.list(in.id.store)
	got, err := tx.importRev(&r)
	if err != nil {
		return 0, fmt.Errorf("importing %s: %w", in.id, err)
	}
	pending[in.id] = got.Rev
	return got.Rev, nil
}

// history imports history revisions from the start of revs, as many as fit
// in one transaction, and returns how many it went through. One that no
// revision being imported names as a cause isn't needed, and is skipped.
func (im *importer) history(ctx context.Context, revs []inRev) (int, error) {
	n, added, skipped := 0, 0, 0
	_, err := im.db.update(ctx, func(tx *Tx) error {
		pending := map[ident]uint64{}
		for _, in := range revs {
			if n > 0 && tx.size+len(in.r.value)+16<<10 > MaxTxSize/2 {
				break
			}
			n++
			if _, held := im.db.lookup(in.id, pending); held || !im.wantHistory(tx, in) {
				skipped++
				continue
			}
			if _, err := im.add(tx, in, true, pending); err != nil {
				return err
			}
			added++
		}
		return nil
	}, nil)
	if err != nil {
		return 0, err
	}
	im.rep.Context += added
	im.rep.Skipped += skipped
	return n, nil
}

// wantHistory reports whether a history revision from the file is worth
// keeping: something being imported names it as a cause, or it is news,
// a revision of its store's line of the key past what this store has.
func (im *importer) wantHistory(tx *Tx, in inRev) bool {
	if im.needed[in.id] {
		return true
	}
	return im.have(tx, in.r.key)[in.id.store] < in.id.rev
}

// have returns what this store has seen of key, counting tx.
func (im *importer) have(tx *Tx, key string) seen {
	db := im.db
	db.mu.RLock()
	h := db.haveOf(key)
	db.mu.RUnlock()
	for st, rev := range tx.imported[key] {
		h.add(ident{st, rev})
	}
	return h
}

// tx imports one transaction from the file as one transaction here.
func (im *importer) tx(ctx context.Context, revs []inRev) error {
	db := im.db
	var imported, skipped, agreed, context int
	var conflicts []Conflict
	_, err := db.update(ctx, func(tx *Tx) error {
		tx.limit = importTxLimit
		pending := map[ident]uint64{}
		for _, in := range revs {
			if _, held := db.lookup(in.id, pending); held {
				skipped++
				continue
			}
			if in.r.history {
				if !im.wantHistory(tx, in) {
					skipped++
					continue
				}
				if _, err := im.add(tx, in, true, pending); err != nil {
					return err
				}
				context++
				continue
			}
			cur, err := im.current(tx, in.r.key)
			if err != nil {
				return err
			}
			switch im.relation(tx, in, cur) {
			case relNew:
				if _, err := im.add(tx, in, false, pending); err != nil {
					return err
				}
				imported++
			case relStale:
				// An older revision of a line this store is already past.
				// Kept only if something names it.
				if !im.needed[in.id] {
					skipped++
					continue
				}
				if _, err := im.add(tx, in, true, pending); err != nil {
					return err
				}
				context++
			case relOlder:
				// Superseded already: kept as history so Why and History
				// show the same story on every store.
				if _, err := im.add(tx, in, true, pending); err != nil {
					return err
				}
				context++
			case relAgreed:
				// Both sides arrived at the same value. The newer of the
				// two stands as the value, as every store would pick, and
				// the other is history; the value reads the same either way.
				theirsNewer := newer(&in.r, cur)
				if _, err := im.add(tx, in, !theirsNewer, pending); err != nil {
					return err
				}
				agreed++
			case relConflict:
				theirsWin := false
				switch im.opts.Conflicts {
				case KeepOurs:
				case TakeTheirs:
					theirsWin = true
				default:
					theirsWin = newer(&in.r, cur)
				}
				got, err := im.add(tx, in, !theirsWin, pending)
				if err != nil {
					return err
				}
				kept := "ours"
				if theirsWin {
					kept = "theirs"
					imported++
				} else {
					context++
				}
				conflicts = append(conflicts, Conflict{Key: in.r.key, Ours: cur.rev, Theirs: got, Kept: kept})
			}
		}
		return nil
	}, nil)
	if err != nil {
		return err
	}
	im.rep.Imported += imported
	im.rep.Skipped += skipped
	im.rep.Agreed += agreed
	im.rep.Context += context
	im.rep.Conflicts = append(im.rep.Conflicts, conflicts...)
	return nil
}

// current returns key's current revision here, counting writes earlier in
// tx; nil if the key has none.
func (im *importer) current(tx *Tx, key string) (*rec, error) {
	if t, ok := tx.latest[key]; ok {
		return t, nil
	}
	db := im.db
	db.mu.RLock()
	defer db.mu.RUnlock()
	k, ok := db.keys[key]
	if !ok {
		return nil, nil
	}
	m, _ := db.findRev(k.latest())
	return db.readRec(m)
}

// contextOf is what the writer of r knew of its key: r itself and r's
// seen list.
func (db *DB) contextOf(r *rec) seen {
	c := seen{}
	for _, e := range r.seen {
		c.add(ident{e.store, e.rev})
	}
	if r.origin != nil {
		c.add(*r.origin)
	} else {
		c.add(ident{db.hdr.store, r.rev})
	}
	return c
}

type relation int

const (
	relNew      relation = iota // made knowing everything this store has: becomes the value
	relStale                    // older than what this store holds from the same store
	relOlder                    // the current value was written knowing it
	relAgreed                   // made without seeing ours, to the same value
	relConflict                 // made without seeing ours, to a different value
)

// relation decides how an incoming change relates to what this store has
// seen of its key (see seen).
func (im *importer) relation(tx *Tx, in inRev, cur *rec) relation {
	have := im.have(tx, in.r.key)
	if have[in.id.store] >= in.id.rev {
		// This store holds that store's line of the key up to this change
		// or past it.
		return relStale
	}
	// The change's writer had seen its own line up to it, and what its
	// seen list says of the others.
	theirs := seen{in.id.store: in.id.rev}
	for st, rev := range in.seen {
		theirs.add(ident{st, rev})
	}
	if cur == nil {
		return relNew
	}
	// Judge the change against the current value: if its writer had seen
	// that value (and all its writer had seen), it is an update. Revisions
	// kept only as history don't count; they lost already.
	now := im.db.contextOf(cur)
	switch {
	case theirs.covers(now):
		return relNew
	case now.covers(theirs):
		// The current value was written knowing this change.
		return relOlder
	case sameValue(cur, &in.r):
		return relAgreed
	}
	return relConflict
}
