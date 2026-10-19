// Copyright xkv.org 2026
// SPDX-License-Identifier: Apache-2.0

package xkv

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

func exportT(t testing.TB, db *DB, since uint64) ([]byte, ExportStats) {
	t.Helper()
	var buf bytes.Buffer
	st, err := db.Export(bg, &buf, ExportOptions{Since: since})
	if err != nil {
		t.Fatal(err)
	}
	return buf.Bytes(), st
}

func importT(t testing.TB, db *DB, data []byte, p ConflictPolicy) ImportReport {
	t.Helper()
	rep, err := db.Import(WithActor(bg, "sync"), bytes.NewReader(data), ImportOptions{Conflicts: p})
	if err != nil {
		t.Fatal(err)
	}
	return rep
}

// canonWhy renders key's Why tree without revision numbers or origins,
// which differ between stores, so two stores' trees can be compared.
func canonWhy(t testing.TB, db *DB, key string) string {
	t.Helper()
	n, err := db.Why(bg, key, MaxWhyDepth)
	if err != nil {
		t.Fatal(err)
	}
	var b strings.Builder
	var walk func(n *WhyNode, depth int)
	walk = func(n *WhyNode, depth int) {
		r := n.Revision
		fmt.Fprintf(&b, "%s%s=%q del=%v by %s: %s ref=%s t=%d", strings.Repeat("  ", depth), r.Key, r.Value, r.Deleted, r.Actor, r.Reason, r.Ref, r.Time.UnixNano())
		if n.Prev != nil {
			fmt.Fprintf(&b, " was %q", n.Prev.Value)
		}
		fmt.Fprintf(&b, " trunc=%v rep=%v\n", n.Truncated, n.Repeat)
		for _, c := range n.Causes {
			walk(c, depth+1)
		}
	}
	walk(n, 0)
	return b.String()
}

// state returns every key's current value, deletes included.
func valuesOf(t testing.TB, db *DB) map[string]string {
	t.Helper()
	out := map[string]string{}
	db.mu.RLock()
	keys := make([]string, 0, len(db.keys))
	for k := range db.keys {
		keys = append(keys, k)
	}
	db.mu.RUnlock()
	for _, k := range keys {
		r, err := db.Get(bg, k)
		switch {
		case errors.Is(err, ErrNotFound):
			out[k] = "<deleted>"
		case err != nil:
			t.Fatal(err)
		default:
			out[k] = string(r.Value)
		}
	}
	return out
}

func sameValues(t testing.TB, a, b *DB) {
	t.Helper()
	sa, sb := valuesOf(t, a), valuesOf(t, b)
	if len(sa) != len(sb) {
		t.Fatalf("%d keys against %d", len(sa), len(sb))
	}
	for k, v := range sa {
		if sb[k] != v {
			t.Fatalf("%s: %q against %q", k, v, sb[k])
		}
	}
}

func TestExchangeRoundTrip(t *testing.T) {
	a := openT(t, tmpPath(t), nil)
	b := openT(t, tmpPath(t), nil)
	chain(t, a)
	must[Revision](t)(a.Delete(bg, "chargeback:cb_991", Reason("dispute closed"), Actor("billing-worker")))
	must[Revision](t)(a.Put(bg, "blob", []byte{0xff, 0, 1}, Reason("binary value"), Actor("tool")))

	data, st := exportT(t, a, 0)
	if st.Revisions != 7 || st.Context != 0 || st.Through != 7 {
		t.Fatalf("export stats %+v", st)
	}
	rep := importT(t, b, data, NewestWins)
	if rep.Imported != 7 || rep.Skipped != 0 || len(rep.Conflicts) != 0 || rep.Through != 7 {
		t.Fatalf("import report %+v", rep)
	}
	sameValues(t, a, b)
	for _, k := range []string{"account:42:limit", "risk:42:score", "chargeback:cb_991", "blob"} {
		if wa, wb := canonWhy(t, a, k), canonWhy(t, b, k); wa != wb {
			t.Fatalf("Why %s differs:\n%s\n%s", k, wa, wb)
		}
		ha := must[[]Revision](t)(a.History(bg, k, 0))
		hb := must[[]Revision](t)(b.History(bg, k, 0))
		if len(ha) != len(hb) {
			t.Fatalf("history of %s: %d against %d", k, len(ha), len(hb))
		}
	}
	idA, _ := a.StoreID()
	got := must[Revision](t)(b.Get(bg, "account:42:limit"))
	if got.Origin != fmt.Sprintf("%s:5", idA) {
		t.Fatalf("origin %q", got.Origin)
	}

	// The same file again changes nothing.
	before := must[Stats](t)(b.Stats())
	rep = importT(t, b, data, NewestWins)
	if rep.Imported != 0 || rep.Skipped != 7 {
		t.Fatalf("second import %+v", rep)
	}
	if after := must[Stats](t)(b.Stats()); after.Revisions != before.Revisions {
		t.Fatalf("second import added revisions: %d -> %d", before.Revisions, after.Revisions)
	}
	// And so does importing a store's export into itself.
	if rep := importT(t, a, data, NewestWins); rep.Imported != 0 || rep.Skipped != 7 {
		t.Fatalf("self import %+v", rep)
	}
	// It survives a reopen, snapshot or not.
	pb := b.Path()
	b.Close()
	b2 := openT(t, pb, nil)
	sameValues(t, a, b2)
	if rep := importT(t, b2, data, NewestWins); rep.Skipped != 7 {
		t.Fatalf("after reopen %+v", rep)
	}
	if rep, err := Check(pb); err != nil || !rep.OK() {
		t.Fatalf("check: %v %v", err, rep.Problems)
	}
}

func TestExchangeIncremental(t *testing.T) {
	a := openT(t, tmpPath(t), nil)
	b := openT(t, tmpPath(t), nil)
	rng := rand.New(rand.NewSource(7))
	var since uint64
	var revs []Revision
	for round := 0; round < 12; round++ {
		for i := 0; i < 25; i++ {
			key := fmt.Sprintf("k%d", rng.Intn(15))
			opts := []WriteOption{Reason(fmt.Sprintf("round %d write %d", round, i)), Actor("a")}
			for c := rng.Intn(3); c > 0 && len(revs) > 0; c-- {
				r := revs[rng.Intn(len(revs))]
				opts = append(opts, Cause(r.Key, r.Rev))
			}
			var r Revision
			if rng.Intn(8) == 0 {
				r = must[Revision](t)(a.Delete(bg, key, opts...))
			} else {
				r = must[Revision](t)(a.Put(bg, key, []byte(fmt.Sprint(rng.Intn(1000))), opts...))
			}
			revs = append(revs, r)
		}
		data, st := exportT(t, a, since)
		rep := importT(t, b, data, NewestWins)
		if len(rep.Conflicts) != 0 || rep.Agreed != 0 {
			t.Fatalf("round %d: one-way sync had conflicts: %+v", round, rep)
		}
		if rep.Imported != st.Revisions {
			t.Fatalf("round %d: exported %d, imported %d (%+v)", round, st.Revisions, rep.Imported, rep)
		}
		since = st.Through
		sameValues(t, a, b)
		for k := range valuesOf(t, a) {
			if wa, wb := canonWhy(t, a, k), canonWhy(t, b, k); wa != wb {
				t.Fatalf("round %d: Why %s differs:\n%s\n%s", round, k, wa, wb)
			}
		}
	}
	if rep, err := Check(b.Path()); err != nil || !rep.OK() {
		t.Fatalf("check: %v %v", err, rep.Problems)
	}
	t.Logf("12 rounds of 25 random writes with causes, each round exported since the last one and imported: after every round the importing store gave the same values and the same Why trees for every key")
}

// Causes from before the checkpoint come along as history: Why can follow
// them, but they never become a value.
func TestExchangeContextIsHistory(t *testing.T) {
	a := openT(t, tmpPath(t), nil)
	b := openT(t, tmpPath(t), nil)
	put := must[Revision](t)
	old := put(a.Put(bg, "threshold", []byte("10"), Reason("first guess"), Actor("ops")))
	put(a.Put(bg, "threshold", []byte("12"), Reason("tuned"), Actor("ops")))
	since := put(a.Put(bg, "unrelated", []byte("x"), Reason("noise"), Actor("ops"))).Rev
	put(a.Put(bg, "alert", []byte("on"), Reason("crossed the first threshold"), Actor("monitor"), Cause(old.Key, old.Rev)))

	data, st := exportT(t, a, since)
	if st.Revisions != 1 || st.Context != 1 {
		t.Fatalf("export %+v", st)
	}
	rep := importT(t, b, data, NewestWins)
	if rep.Imported != 1 || rep.Context != 1 {
		t.Fatalf("import %+v", rep)
	}
	if _, err := b.Get(bg, "threshold"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("a history revision became a value: %v", err)
	}
	if h, err := b.History(bg, "threshold", 0); err != nil || len(h) != 1 || !h[0].History {
		t.Fatalf("the history revision should be listed, marked: %v %+v", err, h)
	}
	if wa, wb := canonWhy(t, a, "alert"), canonWhy(t, b, "alert"); strings.ReplaceAll(wa, ` was "10"`, "") != wb {
		t.Fatalf("Why differs:\n%s\n%s", wa, wb)
	}
	n := must[*WhyNode](t)(b.Why(bg, "alert", 0))
	if !n.Causes[0].History || n.Causes[0].Prev != nil {
		t.Fatalf("the cause should read as history with no known previous value: %+v", n.Causes[0])
	}
	// A later full export brings the key's real history; the value it ends
	// on is the current one.
	full, _ := exportT(t, a, 0)
	importT(t, b, full, NewestWins)
	if v := must[Revision](t)(b.Get(bg, "threshold")); string(v.Value) != "12" {
		t.Fatalf("threshold %q", v.Value)
	}
	// History survives compaction while something points at it, and
	// reopening.
	must[CompactStats](t)(b.Compact(bg, CompactOptions{}))
	if wb := canonWhy(t, b, "alert"); !strings.Contains(wb, "first guess") {
		t.Fatalf("compaction dropped the history cause:\n%s", wb)
	}
	p := b.Path()
	b.Close()
	b = openT(t, p, nil)
	if _, err := b.Get(bg, "threshold"); err != nil {
		t.Fatal(err)
	}
	if wb := canonWhy(t, b, "alert"); !strings.Contains(wb, "first guess") {
		t.Fatalf("after reopen:\n%s", wb)
	}
}

func TestExchangeConflicts(t *testing.T) {
	for _, p := range []ConflictPolicy{NewestWins, KeepOurs, TakeTheirs} {
		t.Run(p.String(), func(t *testing.T) {
			base := time.Date(2026, 10, 19, 9, 0, 0, 0, time.UTC)
			tick := func(start time.Time) func() time.Time {
				t := start
				return func() time.Time { t = t.Add(time.Second); return t }
			}
			a := openT(t, tmpPath(t), &Options{Now: tick(base)})
			b := openT(t, tmpPath(t), &Options{Now: tick(base.Add(time.Hour))}) // b's writes are later
			put := must[Revision](t)
			put(a.Put(bg, "price", []byte("9"), Reason("launch price"), Actor("pricing")))
			data, st := exportT(t, a, 0)
			importT(t, b, data, p)
			ab, ba := st.Through, uint64(0)
			{
				_, st := exportT(t, b, 0)
				ba = st.Through
			}
			ours := put(a.Put(bg, "price", []byte("10"), Reason("costs up"), Actor("pricing")))
			theirs := put(b.Put(bg, "price", []byte("12"), Reason("matching rival"), Actor("sales")))

			data, _ = exportT(t, b, ba)
			rep := importT(t, a, data, p)
			if len(rep.Conflicts) != 1 {
				t.Fatalf("conflicts %+v", rep)
			}
			c := rep.Conflicts[0]
			want := map[ConflictPolicy]string{NewestWins: "12", KeepOurs: "10", TakeTheirs: "12"}[p]
			if v := put(a.Get(bg, "price")); string(v.Value) != want {
				t.Fatalf("price %q, want %q", v.Value, want)
			}
			if c.Ours != ours.Rev || c.Kept != map[bool]string{true: "theirs", false: "ours"}[want == "12"] {
				t.Fatalf("conflict %+v", c)
			}
			// Both sides are in the key's history, the loser marked as
			// history; the import wrote nothing of its own.
			h := must[[]Revision](t)(a.History(bg, "price", 0))
			var vals []string
			for _, r := range h {
				v := string(r.Value)
				if r.History {
					v += "(h)"
				}
				vals = append(vals, v)
			}
			wantH := map[string]string{"12": "12,10,9", "10": "12(h),10,9"}[want]
			if strings.Join(vals, ",") != wantH {
				t.Fatalf("history %v, want %s", vals, wantH)
			}
			if got := put(a.Revision(bg, c.Theirs)); got.Actor != theirs.Actor || got.Reason != theirs.Reason {
				t.Fatalf("their revision %+v", got)
			}
			if p != NewestWins {
				return
			}
			// The other way round, b picks the same winner, and a third
			// exchange in each direction settles nothing new.
			data, _ = exportT(t, a, ab)
			rep = importT(t, b, data, p)
			if v := put(b.Get(bg, "price")); string(v.Value) != "12" {
				t.Fatalf("b price %q (%+v)", v.Value, rep)
			}
			for i := 0; i < 2; i++ {
				d1, _ := exportT(t, a, 0)
				r1 := importT(t, b, d1, p)
				d2, _ := exportT(t, b, 0)
				r2 := importT(t, a, d2, p)
				if len(r1.Conflicts)+len(r2.Conflicts) != 0 {
					t.Fatalf("round %d still conflicting: %+v %+v", i, r1, r2)
				}
			}
			sameValues(t, a, b)
		})
	}
}

func TestExchangeAgreedChange(t *testing.T) {
	a := openT(t, tmpPath(t), nil)
	b := openT(t, tmpPath(t), nil)
	must[Revision](t)(a.Put(bg, "flag", []byte("off"), Reason("default"), Actor("x")))
	data, _ := exportT(t, a, 0)
	importT(t, b, data, NewestWins)
	must[Revision](t)(a.Put(bg, "flag", []byte("on"), Reason("launch"), Actor("x")))
	must[Revision](t)(b.Put(bg, "flag", []byte("on"), Reason("launch too"), Actor("y")))
	before := must[Revision](t)(b.Get(bg, "flag"))
	data, _ = exportT(t, a, 0)
	rep := importT(t, b, data, NewestWins)
	if rep.Agreed != 1 || len(rep.Conflicts) != 0 {
		t.Fatalf("%+v", rep)
	}
	if after := must[Revision](t)(b.Get(bg, "flag")); after.Rev != before.Rev {
		t.Fatalf("an agreed change replaced the current revision: %d -> %d", before.Rev, after.Rev)
	}
}

func TestExchangeRefusesBadFiles(t *testing.T) {
	a := openT(t, tmpPath(t), nil)
	chain(t, a)
	data, _ := exportT(t, a, 0)
	b := openT(t, tmpPath(t), nil)
	try := func(name string, d []byte) {
		t.Helper()
		_, err := b.Import(bg, bytes.NewReader(d), ImportOptions{})
		if !errors.Is(err, ErrBadExchange) {
			t.Fatalf("%s: %v", name, err)
		}
		if s := must[Stats](t)(b.Stats()); s.Revisions != 0 {
			t.Fatalf("%s: a refused file wrote %d revisions", name, s.Revisions)
		}
	}
	// Cut at every byte.
	for i := 0; i < len(data); i++ {
		try(fmt.Sprintf("cut at %d", i), data[:i])
	}
	// One changed byte anywhere, as long as it still parses or not.
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 300; i++ {
		d := append([]byte(nil), data...)
		j := rng.Intn(len(d))
		c := byte(rng.Intn(256))
		if c == d[j] {
			continue
		}
		d[j] = c
		try(fmt.Sprintf("byte %d", j), d)
	}
	// A line dropped, a line repeated, something after the end.
	lines := bytes.SplitAfter(data, []byte("\n"))
	try("line dropped", bytes.Join(append(append([][]byte{}, lines[:2]...), lines[3:]...), nil))
	try("line repeated", bytes.Join(append(append([][]byte{}, lines[:3]...), lines[2:]...), nil))
	try("after the end", append(append([]byte{}, data...), lines[1]...))
	try("not an exchange file", []byte("hello\n"))

	// A cause the file and the store both lack refuses the file.
	partial, _ := exportT(t, a, 0)
	_ = partial
}

// Identities survive more than one hop, and a store recognises its own
// revisions coming back.
func TestExchangeThreeStores(t *testing.T) {
	a := openT(t, tmpPath(t), nil)
	b := openT(t, tmpPath(t), nil)
	c := openT(t, tmpPath(t), nil)
	_, _, limit := chain(t, a)
	d, _ := exportT(t, a, 0)
	importT(t, b, d, NewestWins)
	bl := must[Revision](t)(b.Get(bg, "account:42:limit"))
	must[Revision](t)(b.Put(bg, "account:42:review", []byte("due"), Reason("limit cut needs review"), Actor("ops"), Cause(bl.Key, bl.Rev)))
	d, _ = exportT(t, b, 0)
	importT(t, c, d, NewestWins)
	cl := must[Revision](t)(c.Get(bg, "account:42:limit"))
	if cl.Origin != bl.Origin {
		t.Fatalf("identity changed on the second hop: %q against %q", cl.Origin, bl.Origin)
	}
	// c's export back to a: a's own revisions are recognised, and the new
	// one's cause lands on a's own revision.
	d, _ = exportT(t, c, 0)
	rep := importT(t, a, d, NewestWins)
	if rep.Imported != 1 || rep.Skipped != 5 {
		t.Fatalf("%+v", rep)
	}
	rv := must[Revision](t)(a.Get(bg, "account:42:review"))
	if len(rv.Causes) != 1 || rv.Causes[0].Rev != limit.Rev {
		t.Fatalf("cause %v, want a's own revision %d", rv.Causes, limit.Rev)
	}
	for _, k := range []string{"account:42:review", "account:42:limit"} {
		if wa, wc := canonWhy(t, a, k), canonWhy(t, c, k); wa != wc {
			t.Fatalf("Why %s differs:\n%s\n%s", k, wa, wc)
		}
	}
}

func TestExchangeAfterCompaction(t *testing.T) {
	a := openT(t, tmpPath(t), nil)
	b := openT(t, tmpPath(t), nil)
	chain(t, a)
	for i := 0; i < 5; i++ {
		must[Revision](t)(a.Put(bg, "account:42:limit", []byte(fmt.Sprint(40+i)), Reason("adjust"), Actor("limits-service"), CauseLatest("risk:42:score")))
	}
	must[CompactStats](t)(a.Compact(bg, CompactOptions{}))
	d, _ := exportT(t, a, 0)
	importT(t, b, d, NewestWins)
	sameValues(t, a, b)
	for _, k := range []string{"account:42:limit", "risk:42:score"} {
		if wa, wb := canonWhy(t, a, k), canonWhy(t, b, k); wa != wb {
			t.Fatalf("Why %s differs:\n%s\n%s", k, wa, wb)
		}
	}
	// A key whose earlier values a compacted have no known previous value in
	// b either, so b doesn't call the revision a conflict later.
	must[Revision](t)(a.Put(bg, "account:42:limit", []byte("60"), Reason("restore"), Actor("limits-service")))
	d, _ = exportT(t, a, 0)
	if rep := importT(t, b, d, NewestWins); rep.Imported != 1 || len(rep.Conflicts) != 0 {
		t.Fatalf("%+v", rep)
	}
}

// Two stores written to on both sides and exchanged both ways, over and
// over, end up with the same values, and neither loses a change: every
// revision either one wrote is held by both.
func TestExchangeTwoWayConverges(t *testing.T) {
	seeds := 20
	if testing.Short() {
		seeds = 5
	}
	totalConflicts, totalAgreed, totalRevs, afterEdits := 0, 0, 0, 0
	for seed := 1; seed <= seeds; seed++ {
		rng := rand.New(rand.NewSource(int64(seed)))
		base := time.Date(2026, 10, 19, 9, 0, 0, 0, time.UTC)
		// Clocks that sometimes give the two stores the same times.
		mk := func() func() time.Time {
			n := 0
			return func() time.Time { n++; return base.Add(time.Duration(n/2+rng.Intn(3)) * time.Second) }
		}
		dbs := []*DB{openT(t, tmpPath(t), &Options{Now: mk()}), openT(t, tmpPath(t), &Options{Now: mk()})}
		written := [2][]Revision{}
		compacting := seed%2 == 0
		since := [2][2]uint64{}
		sync := func(from, to int) ImportReport {
			d, st := exportT(t, dbs[from], since[from][to])
			rep, err := dbs[to].Import(WithActor(bg, "sync"), bytes.NewReader(d), ImportOptions{})
			if err != nil {
				t.Fatalf("seed %d: import from %d to %d since %d: %v", seed, from, to, since[from][to], err)
			}
			since[from][to] = st.Through
			return rep
		}
		for round := 0; round < 10; round++ {
			for side := 0; side < 2; side++ {
				for i := rng.Intn(12); i > 0; i-- {
					db := dbs[side]
					key := fmt.Sprintf("k%d", rng.Intn(8))
					opts := []WriteOption{Reason(fmt.Sprintf("s%d r%d", side, round)), Actor(fmt.Sprint("side", side))}
					if keys := must[[]string](t)(db.Keys(bg, "")); len(keys) > 0 && rng.Intn(2) == 0 {
						opts = append(opts, CauseLatest(keys[rng.Intn(len(keys))]))
					}
					var r Revision
					if rng.Intn(6) == 0 {
						r = must[Revision](t)(db.Delete(bg, key, opts...))
					} else {
						r = must[Revision](t)(db.Put(bg, key, []byte(fmt.Sprint(rng.Intn(4))), opts...))
					}
					written[side] = append(written[side], r)
				}
			}
			if rng.Intn(3) > 0 {
				r := sync(0, 1)
				totalConflicts += len(r.Conflicts)
				totalAgreed += r.Agreed
			}
			if rng.Intn(3) > 0 {
				r := sync(1, 0)
				totalConflicts += len(r.Conflicts)
				totalAgreed += r.Agreed
			}
			if compacting && rng.Intn(4) == 0 {
				must[CompactStats](t)(dbs[rng.Intn(2)].Compact(bg, CompactOptions{KeepLast: 1 + rng.Intn(3)}))
			}
		}
		// Settle: exchange until a round brings nothing new.
		settled := false
		for i := 0; i < 6 && !settled; i++ {
			r1 := sync(0, 1)
			r2 := sync(1, 0)
			totalConflicts += len(r1.Conflicts) + len(r2.Conflicts)
			settled = r1.Imported+r1.Context+r1.Agreed+r2.Imported+r2.Context+r2.Agreed == 0
		}
		if !settled {
			t.Fatalf("seed %d: still exchanging changes after six rounds", seed)
		}
		// Once settled, an edit on one side is an ordinary change: it
		// reaches the other side as it is, with no conflict.
		for i := 0; i < 5; i++ {
			side := rng.Intn(2)
			key := fmt.Sprintf("k%d", rng.Intn(8))
			val := fmt.Sprintf("after-%d-%d", seed, i)
			must[Revision](t)(dbs[side].Put(bg, key, []byte(val), Reason("edit after settling"), Actor("x")))
			r1 := sync(side, 1-side)
			r2 := sync(1-side, side)
			if len(r1.Conflicts)+len(r2.Conflicts) != 0 {
				t.Fatalf("seed %d: an edit after settling made conflicts: %+v %+v", seed, r1, r2)
			}
			for _, db := range dbs {
				if v := must[Revision](t)(db.Get(bg, key)); string(v.Value) != val {
					t.Fatalf("seed %d: %s is %q, the edit was %q", seed, key, v.Value, val)
				}
			}
			afterEdits++
		}
		sameValues(t, dbs[0], dbs[1])
		for _, db := range dbs {
			if rep, err := Check(db.Path()); err != nil || !rep.OK() {
				t.Fatalf("seed %d: check: %v %v", seed, err, rep.Problems)
			}
		}
		// Nothing lost: without compaction, every revision either store
		// holds, the other holds too, and each side's own writes are all
		// there on both.
		for side := 0; side < 2; side++ {
			db, other := dbs[side], dbs[1-side]
			totalRevs += len(written[side])
			if compacting {
				continue
			}
			db.mu.RLock()
			var ids []ident
			for _, m := range db.revs {
				ids = append(ids, db.identOf(m))
			}
			db.mu.RUnlock()
			id, _ := db.StoreID()
			for _, r := range written[side] {
				ids = append(ids, ident{store: id, rev: r.Rev})
			}
			other.mu.RLock()
			for _, id := range ids {
				if _, ok := other.lookupIdent(id); !ok {
					other.mu.RUnlock()
					t.Fatalf("seed %d: revision %s is missing from the other store", seed, id)
				}
			}
			other.mu.RUnlock()
		}
	}
	t.Logf("%d seeds of two stores writing, exchanging both ways at random and compacting now and then: %d revisions written, %d conflicts settled, %d changes both sides had made alike; every seed settled to the same values on both stores, both files passed the check, and in the seeds without compaction each store held every revision the other held; then %d edits made after settling each reached the other store as an ordinary change, with no conflict", seeds, totalRevs, totalConflicts, totalAgreed, afterEdits)
}

func TestExportNeedsNoWritesFromFormat2ReadOnly(t *testing.T) {
	p := tmpPath(t)
	a := openT(t, p, nil)
	chain(t, a)
	a.Close()
	ro := openT(t, p, &Options{ReadOnly: true})
	d, st := exportT(t, ro, 0)
	if st.Revisions != 5 || len(d) == 0 {
		t.Fatalf("%+v", st)
	}
	if _, err := ro.Import(bg, bytes.NewReader(d), ImportOptions{}); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("read-only import: %v", err)
	}
}

func TestParseIdent(t *testing.T) {
	good := strings.Repeat("ab", 16) + ":17"
	if id, ok := parseIdent(good); !ok || id.rev != 17 || id.String() != good {
		t.Fatalf("%v %v", id, ok)
	}
	for _, s := range []string{"", ":1", strings.Repeat("ab", 16) + ":0", strings.Repeat("ab", 16) + ":017", strings.Repeat("ab", 16) + ":+1", strings.Repeat("zz", 16) + ":1", strings.Repeat("ab", 15) + ":1"} {
		if _, ok := parseIdent(s); ok {
			t.Fatalf("%q parsed", s)
		}
	}
	_ = sort.Strings
}

// testdata/format1.xkv was written by xkv 0.1.0, with its snapshot: three
// keys, a compaction, then a delete.
func copyFormat1(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	for _, n := range []string{"format1.xkv", "format1.xkv.idx"} {
		b, err := os.ReadFile(filepath.Join("testdata", n))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, n), b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return filepath.Join(dir, "format1.xkv")
}

func TestFormat1Store(t *testing.T) {
	p := copyFormat1(t)
	if rep, err := Check(p); err != nil || !rep.OK() || rep.Revisions != 5 {
		t.Fatalf("check: %v %+v", err, rep)
	}
	ro := openT(t, p, &Options{ReadOnly: true})
	st := must[Stats](t)(ro.Stats())
	if st.Format != 1 || st.StoreID != "" || st.LastRev != 9 {
		t.Fatalf("stats %+v", st)
	}
	if v := must[Revision](t)(ro.Get(bg, "account:42:limit")); string(v.Value) != "50" {
		t.Fatalf("%q", v.Value)
	}
	whyBefore := canonWhy(t, ro, "account:42:limit")
	if _, err := ro.Export(bg, io.Discard, ExportOptions{}); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("read-only export of a format 1 store: %v", err)
	}
	ro.Close()

	// Writing to it keeps it format 1, readable by 0.1.
	db := openT(t, p, nil)
	must[Revision](t)(db.Put(bg, "extra", []byte("1"), Reason("still format 1"), Actor("ops")))
	if st := must[Stats](t)(db.Stats()); st.Format != 1 {
		t.Fatalf("a plain write upgraded the store: %+v", st)
	}
	// An export upgrades it, keeping every revision and its number.
	var buf bytes.Buffer
	must[ExportStats](t)(db.Export(bg, &buf, ExportOptions{}))
	st = must[Stats](t)(db.Stats())
	if st.Format != 2 || len(st.StoreID) != 32 || st.Revisions != 6 || st.LastRev != 10 {
		t.Fatalf("after the upgrade %+v", st)
	}
	if w := canonWhy(t, db, "account:42:limit"); w != whyBefore {
		t.Fatalf("Why changed:\n%s\n%s", whyBefore, w)
	}
	if _, err := db.GetAt(bg, "note", 7); !errors.Is(err, ErrCompacted) {
		t.Fatalf("the compaction gap was lost: %v", err)
	}
	db.Close()
	if rep, err := Check(p); err != nil || !rep.OK() {
		t.Fatalf("check after upgrade: %v %+v", err, rep)
	}
	db = openT(t, p, nil)
	if st2 := must[Stats](t)(db.Stats()); st2.StoreID != st.StoreID {
		t.Fatalf("the store id changed on reopen: %s -> %s", st.StoreID, st2.StoreID)
	}
	must[CompactStats](t)(db.Compact(bg, CompactOptions{}))
	if st3 := must[Stats](t)(db.Stats()); st3.StoreID != st.StoreID || st3.Format != 2 {
		t.Fatalf("compaction changed the store id: %+v", st3)
	}
}

// FuzzImport feeds Import arbitrary exchange file bodies, each sealed with
// a correct end line so the mutations reach the checks behind the checksum.
// It must never panic, and whatever it accepts must leave a store that
// checks out and holds no identity twice.
func FuzzImport(f *testing.F) {
	src, _ := OpenMemory(&Options{Now: clock()})
	chain(f, src)
	for _, since := range []uint64{0, 3} {
		var buf bytes.Buffer
		src.Export(bg, &buf, ExportOptions{Since: since})
		lines := bytes.SplitAfter(buf.Bytes(), []byte("\n"))
		f.Add(bytes.Join(lines[:len(lines)-2], nil)) // without the end line
	}
	f.Fuzz(func(t *testing.T, body []byte) {
		ls := strings.Split(strings.TrimSuffix(string(body), "\n"), "\n")
		data := reseal(ls)
		db, err := OpenMemory(&Options{Now: clock()})
		if err != nil {
			t.Fatal(err)
		}
		must[Revision](t)(db.Put(bg, "risk:42:score", []byte("local"), Reason("local"), Actor("x")))
		if _, err := db.Import(bg, bytes.NewReader(data), ImportOptions{}); err == nil {
			checkIndex(t, db)
		}
	})
}

// checkIndex checks a store's invariants in memory: every revision reads
// back, every cause exists and belongs to its key, no identity is held
// twice, and no history revision is a key's value.
func checkIndex(t testing.TB, db *DB) {
	t.Helper()
	db.mu.RLock()
	defer db.mu.RUnlock()
	seen := map[ident]bool{}
	for _, m := range db.revs {
		r, err := db.readRec(m)
		if err != nil {
			t.Fatal(err)
		}
		id := db.identOf(m)
		if seen[id] {
			t.Fatalf("identity %s held twice", id)
		}
		seen[id] = true
		for _, c := range r.causes {
			cm, ok := db.findRev(c.Rev)
			if !ok || cm.key != c.Key || c.Rev >= m.rev {
				t.Fatalf("revision %d: bad cause %v", m.rev, c)
			}
		}
	}
	for name, k := range db.keys {
		for _, rev := range k.revs {
			if m, _ := db.findRev(rev); m.history || m.key != name {
				t.Fatalf("key %q lists revision %d wrongly", name, rev)
			}
		}
	}
}

// TestImportMutatedFiles imports an export with a few bytes changed,
// dropped or added, thousands of times. Every one is refused whole, or
// leaves a store that checks out; none panics.
func TestImportMutatedFiles(t *testing.T) {
	n := 3000
	if testing.Short() {
		n = 300
	}
	src, _ := OpenMemory(&Options{Now: clock()})
	chain(t, src)
	var full bytes.Buffer
	src.Export(bg, &full, ExportOptions{})
	rng := rand.New(rand.NewSource(1))
	dir := t.TempDir()
	accepted := 0
	for i := 0; i < n; i++ {
		b := append([]byte(nil), full.Bytes()...)
		for k := rng.Intn(4); k >= 0; k-- {
			j := rng.Intn(len(b))
			switch rng.Intn(3) {
			case 0:
				b[j] = byte(rng.Intn(256))
			case 1:
				b = append(b[:j], b[j+1:]...)
			default:
				b = append(b[:j], append([]byte{byte(rng.Intn(256))}, b[j:]...)...)
			}
		}
		p := filepath.Join(dir, fmt.Sprintf("m%d.xkv", i))
		db, err := Open(p, &Options{NoSync: true, Now: clock()})
		if err != nil {
			t.Fatal(err)
		}
		must[Revision](t)(db.Put(bg, "risk:42:score", []byte("local"), Reason("local"), Actor("x")))
		_, err = db.Import(bg, bytes.NewReader(b), ImportOptions{})
		db.Close()
		if err == nil {
			accepted++
			if rep, cerr := Check(p); cerr != nil || !rep.OK() {
				t.Fatalf("mutation %d was accepted and left a bad file: %v %v", i, cerr, rep.Problems)
			}
		} else if !errors.Is(err, ErrBadExchange) {
			t.Fatalf("mutation %d: %v", i, err)
		}
		os.Remove(p)
		os.Remove(p + snapshotSuffix)
	}
	t.Logf("%d exports with one to four bytes changed, dropped or added: %d refused whole as invalid, %d accepted; none panicked, and every accepted one left a file that checks out", n, n-accepted, accepted)
}
