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

// Conflict is one key both stores changed. Both sides are kept: the
// imported revision goes into the key's history, and a resolution revision
// written by the import names both as its causes.
type Conflict struct {
	Key        string `json:"key"`
	Ours       uint64 `json:"ours"`       // the revision that was current here
	Theirs     uint64 `json:"theirs"`     // the imported revision, by its number here
	Resolution uint64 `json:"resolution"` // the revision the import wrote to settle it
	Kept       string `json:"kept"`       // "ours" or "theirs"
}

// ImportReport describes an import.
type ImportReport struct {
	Store     string     `json:"store"`   // the store the file was exported from
	Since     uint64     `json:"since"`   // the export's Since
	Through   uint64     `json:"through"` // the export's Through: ask for the next export since this
	Imported  int        `json:"imported"`
	Context   int        `json:"context"` // older revisions imported as history
	Skipped   int        `json:"skipped"` // already here
	Agreed    int        `json:"agreed"`  // changed on both sides to the same value; kept as history
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
	ID            string    `json:"id"`
	Key           string    `json:"key"`
	Value         *string   `json:"value,omitempty"`
	Encoding      string    `json:"encoding,omitempty"`
	Deleted       bool      `json:"deleted,omitempty"`
	Actor         string    `json:"actor"`
	Reason        string    `json:"reason"`
	Ref           string    `json:"ref,omitempty"`
	Time          time.Time `json:"time"`
	Causes        []exCause `json:"causes,omitempty"`
	History       bool      `json:"history,omitempty"`
	Prev          string    `json:"prev,omitempty"`
	PrevCompacted bool      `json:"prev_compacted,omitempty"`
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
// Upgrade). Writes wait while Export reads; reads go on.
func (db *DB) Export(ctx context.Context, w io.Writer, opts ExportOptions) (ExportStats, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := db.Upgrade(ctx); err != nil {
		return ExportStats{}, err
	}
	// Holding the writer lock keeps the file still while it is read twice,
	// without blocking readers.
	db.wmu.Lock()
	defer db.wmu.Unlock()
	db.mu.RLock()
	defer db.mu.RUnlock()
	if err := db.readGuard(ctx); err != nil {
		return ExportStats{}, err
	}
	st := ExportStats{Store: db.hdr.store.String(), Since: opts.Since, Through: db.lastRev}

	// The changes: every revision above Since that is a key's revision,
	// not a history-only one.
	first := sort.Search(len(db.revs), func(i int) bool { return db.revs[i].rev > opts.Since })
	var changes []revMeta
	inChanges := map[uint64]bool{}
	for _, m := range db.revs[first:] {
		if !m.history {
			changes = append(changes, m)
			inChanges[m.rev] = true
		}
	}
	// The context: everything the changes reach through causes that isn't
	// a change itself, found by following causes back.
	inContext := map[uint64]bool{}
	queue := make([]uint64, 0, len(changes))
	for _, m := range changes {
		queue = append(queue, m.rev)
	}
	for len(queue) > 0 {
		if err := ctx.Err(); err != nil {
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
		if !history {
			if p, ok := db.prevOf(m); ok {
				x.Prev = db.identOf(p).String()
			} else if m.gap {
				x.PrevCompacted = true
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
		for ; i < len(out) && out[i].tx == txid; i++ {
			x, err := toEx(out[i])
			if err != nil {
				return st, err
			}
			line = append(line, x)
			if x.History {
				st.Context++
			} else {
				st.Revisions++
			}
		}
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
	id            ident
	r             rec // rev, tx and causes unset; causes are in causes
	causes        []ident
	causeKeys     []string
	prev          ident
	hasPrev       bool
	prevCompacted bool
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

func checkExRev(x exRev, seen map[ident]string) (inRev, error) {
	var in inRev
	id, ok := parseIdent(x.ID)
	if !ok {
		return in, fmt.Errorf("bad id %q", x.ID)
	}
	if _, dup := seen[id]; dup {
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
		if k, ok := seen[cid]; ok && k != c.Key {
			return in, fmt.Errorf("cause %s is a revision of %q, not %q", c.ID, k, c.Key)
		}
		in.causes = append(in.causes, cid)
		in.causeKeys = append(in.causeKeys, c.Key)
	}
	if x.Prev != "" || x.PrevCompacted {
		if x.History {
			return in, errors.New("a history revision with a prev")
		}
	}
	if x.Prev != "" {
		p, ok := parseIdent(x.Prev)
		if !ok {
			return in, fmt.Errorf("bad prev %q", x.Prev)
		}
		in.prev, in.hasPrev = p, true
	}
	in.prevCompacted = x.PrevCompacted
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
// reason, ref and time, gets a number here, and remembers where it was first
// written, so importing the same file again changes nothing and causes
// still point at the right revisions. Revisions the file holds only as
// causes of others are kept as history: Why follows them, but they never
// become a key's current value.
//
// Each transaction in the file is imported as one transaction here. When a
// key was changed here and in the file since the stores last exchanged,
// both changes are kept: the imported one goes into the key's history, and
// a resolution revision, written with the reason, actor and ref given in
// wopts and naming both as causes, sets the value opts.Conflicts chooses.
// The import needs a reason for that (strict mode).
//
// The whole file is read and checked before anything is written; a file
// that is incomplete, changed or invalid is refused with ErrBadExchange. A
// format 1 store is upgraded first (see Upgrade).
func (db *DB) Import(ctx context.Context, rd io.Reader, opts ImportOptions, wopts ...WriteOption) (ImportReport, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	var rep ImportReport
	var wo writeOpts
	for _, f := range wopts {
		f(&wo)
	}
	if len(wo.causes) > 0 {
		return rep, fmt.Errorf("%w: an import takes a reason, an actor and a ref, not causes", ErrInvalid)
	}
	reason := wo.reason
	if !wo.hasReason || reason == "" {
		if !db.opts.Relaxed {
			return rep, ErrNoReason
		}
		reason = "unspecified"
	}
	hdr, groups, err := readExchange(rd)
	if err != nil {
		return rep, err
	}
	rep.Store, rep.Since, rep.Through = hdr.Store, hdr.Since, hdr.Through
	rep.Conflicts = []Conflict{}
	if err := db.Upgrade(ctx); err != nil {
		return rep, err
	}

	// Every cause must be in the file before the revision naming it, or
	// already here. Checked now, so a file that can't be imported whole
	// isn't imported in part.
	db.mu.RLock()
	inFile := map[ident]bool{}
	for _, g := range groups {
		for _, in := range g.revs {
			for i, c := range in.causes {
				if inFile[c] {
					continue
				}
				if rev, ok := db.lookupIdent(c); ok {
					if m, _ := db.findRev(rev); m.key != in.causeKeys[i] {
						db.mu.RUnlock()
						return rep, fmt.Errorf("%w: cause %s of %s is a revision of %q here, not %q", ErrBadExchange, c, in.id, m.key, in.causeKeys[i])
					}
					continue
				}
				db.mu.RUnlock()
				return rep, fmt.Errorf("%w: %s names cause %s, which neither the file nor this store holds", ErrBadExchange, in.id, c)
			}
			inFile[in.id] = true
		}
	}
	db.mu.RUnlock()

	for _, g := range groups {
		if err := ctx.Err(); err != nil {
			return rep, err
		}
		if g.history {
			// History doesn't change any key's value, so it can go in as
			// several transactions, each kept under the size limit.
			for i := 0; i < len(g.revs); {
				n, err := db.importContext(ctx, g.revs[i:], &rep)
				if err != nil {
					return rep, err
				}
				i += n
			}
			continue
		}
		if err := db.importTx(ctx, g.revs, opts, wo, reason, &rep); err != nil {
			return rep, err
		}
	}
	return rep, nil
}

// newer reports whether a wins over b under NewestWins: the later time, then,
// for the same time, a delete loses to a value and the bytewise larger value
// wins. It looks only at what the revisions hold, so two stores comparing
// the same pair agree.
func newer(a, b *rec) bool {
	if a.time != b.time {
		return a.time > b.time
	}
	if a.deleted != b.deleted {
		return b.deleted
	}
	return bytes.Compare(a.value, b.value) > 0
}

// localCauses maps an imported revision's causes to revisions here: written
// earlier in this transaction (pending), or already committed.
func (db *DB) localCauses(in inRev, pending map[ident]uint64) ([]CauseRef, error) {
	out := make([]CauseRef, 0, len(in.causes))
	seen := map[uint64]bool{}
	for i, c := range in.causes {
		rev, ok := pending[c]
		if !ok {
			db.mu.RLock()
			rev, ok = db.lookupIdent(c)
			db.mu.RUnlock()
		}
		if !ok {
			return nil, fmt.Errorf("%w: cause %s of %s isn't here", ErrBadCause, c, in.id)
		}
		if seen[rev] {
			continue
		}
		seen[rev] = true
		out = append(out, CauseRef{Key: in.causeKeys[i], Rev: rev})
	}
	return out, nil
}

func (db *DB) has(id ident, pending map[ident]uint64) bool {
	if _, ok := pending[id]; ok {
		return true
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	_, ok := db.lookupIdent(id)
	return ok
}

// importContext imports history revisions from the start of revs, as many
// as fit in one transaction, and returns how many it went through.
func (db *DB) importContext(ctx context.Context, revs []inRev, rep *ImportReport) (int, error) {
	n, added, skipped := 0, 0, 0
	_, err := db.update(ctx, func(tx *Tx) error {
		pending := map[ident]uint64{}
		for _, in := range revs {
			if n > 0 && tx.size+len(in.r.value)+16<<10 > MaxTxSize/2 {
				break
			}
			n++
			if db.has(in.id, pending) {
				skipped++
				continue
			}
			causes, err := db.localCauses(in, pending)
			if err != nil {
				return err
			}
			r := in.r
			r.causes = causes
			got, err := tx.importRev(&r)
			if err != nil {
				return fmt.Errorf("importing %s: %w", in.id, err)
			}
			pending[in.id] = got.Rev
			added++
		}
		return nil
	}, nil)
	if err != nil {
		return 0, err
	}
	rep.Context += added
	rep.Skipped += skipped
	return n, nil
}

// importTx imports one transaction from the file as one transaction here,
// settling conflicts as it goes.
func (db *DB) importTx(ctx context.Context, revs []inRev, opts ImportOptions, wo writeOpts, reason string, rep *ImportReport) error {
	var imported, skipped, agreed, context int
	var conflicts []Conflict
	_, err := db.update(ctx, func(tx *Tx) error {
		pending := map[ident]uint64{}
		for _, in := range revs {
			if db.has(in.id, pending) {
				skipped++
				continue
			}
			causes, err := db.localCauses(in, pending)
			if err != nil {
				return err
			}
			r := in.r
			r.causes = causes
			if r.history {
				got, err := tx.importRev(&r)
				if err != nil {
					return fmt.Errorf("importing %s: %w", in.id, err)
				}
				pending[in.id] = got.Rev
				context++
				continue
			}

			// The key's current revision here, if any.
			var cur *rec
			var curID ident
			if t, ok := tx.latest[r.key]; ok {
				cur, curID = t, ident{store: db.hdr.store, rev: t.rev}
				if t.origin != nil {
					curID = *t.origin
				}
			} else {
				db.mu.RLock()
				if k, ok := db.keys[r.key]; ok {
					m, _ := db.findRev(k.latest())
					cur, err = db.readRec(m)
					curID = db.identOf(m)
				}
				db.mu.RUnlock()
				if err != nil {
					return err
				}
			}
			conflict := cur != nil &&
				!(in.hasPrev && curID == in.prev) &&
				!(curID.store == in.id.store && curID.rev < in.id.rev)
			if conflict && cur.deleted == r.deleted && bytes.Equal(cur.value, r.value) {
				// Both sides arrived at the same value: keep theirs as
				// history; nothing to settle.
				r.history = true
				got, err := tx.importRev(&r)
				if err != nil {
					return fmt.Errorf("importing %s: %w", in.id, err)
				}
				pending[in.id] = got.Rev
				agreed++
				continue
			}
			got, err := tx.importRev(&r)
			if err != nil {
				return fmt.Errorf("importing %s: %w", in.id, err)
			}
			pending[in.id] = got.Rev
			imported++
			if !conflict {
				continue
			}
			oursWins := false
			switch opts.Conflicts {
			case KeepOurs:
				oursWins = true
			case TakeTheirs:
				oursWins = false
			default:
				oursWins = newer(cur, &r)
			}
			win, kept := &r, "theirs"
			if oursWins {
				win, kept = cur, "ours"
			}
			why := fmt.Sprintf("%s (import conflict: kept %s)", reason, kept)
			wopts := []WriteOption{Reason(why), Actor(wo.actor), Ref(wo.ref), Cause(r.key, cur.rev), Cause(r.key, got.Rev)}
			// The resolution carries the winning change's time, so the rule
			// sees the change itself next time, not when it was settled.
			var res Revision
			now := tx.now
			tx.now = win.time
			if win.deleted {
				res, err = tx.Delete(r.key, wopts...)
			} else {
				res, err = tx.Put(r.key, win.value, wopts...)
			}
			tx.now = now
			if err != nil {
				return fmt.Errorf("settling the conflict on %q: %w", r.key, err)
			}
			conflicts = append(conflicts, Conflict{Key: r.key, Ours: cur.rev, Theirs: got.Rev, Resolution: res.Rev, Kept: kept})
		}
		return nil
	}, nil)
	if err != nil {
		return err
	}
	rep.Imported += imported
	rep.Skipped += skipped
	rep.Agreed += agreed
	rep.Context += context
	rep.Conflicts = append(rep.Conflicts, conflicts...)
	return nil
}
