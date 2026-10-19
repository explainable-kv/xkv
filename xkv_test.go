// Copyright xkv.org 2026
// SPDX-License-Identifier: Apache-2.0

package xkv

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

var bg = context.Background()

func clock() func() time.Time {
	t := time.Date(2026, 10, 19, 9, 0, 0, 0, time.UTC)
	return func() time.Time { t = t.Add(time.Second); return t }
}

func openT(t *testing.T, path string, opts *Options) *DB {
	t.Helper()
	if opts == nil {
		opts = &Options{}
	}
	if opts.Now == nil {
		opts.Now = clock()
	}
	db, err := Open(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func tmpPath(t *testing.T) string {
	return filepath.Join(t.TempDir(), "test.xkv")
}

func must[T any](t *testing.T) func(T, error) T {
	return func(v T, err error) T {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
}

// chain writes the three-key example from the README.
func chain(t *testing.T, db *DB) (cb, risk, limit Revision) {
	t.Helper()
	put := must[Revision](t)
	ctx := WithActor(bg, "billing-worker")
	cb = put(db.Put(ctx, "chargeback:cb_991", []byte("card dispute filed"), Reason("bank notice"), Ref("BANK-7731")))
	ctx = WithActor(bg, "risk-model")
	put(db.Put(ctx, "risk:42:score", []byte("0.31"), Reason("monthly rescore")))
	risk = put(db.Put(ctx, "risk:42:score", []byte("0.82"), Reason("new chargeback"), Cause(cb.Key, cb.Rev)))
	ctx = WithActor(bg, "limits-service")
	put(db.Put(ctx, "account:42:limit", []byte("100"), Reason("opening limit")))
	limit = put(db.Put(ctx, "account:42:limit", []byte("50"), Reason("risk policy v3"), CauseLatest("risk:42:score")))
	return
}

func TestPutGetReopen(t *testing.T) {
	path := tmpPath(t)
	db := openT(t, path, nil)
	cb, risk, limit := chain(t, db)
	if cb.Rev != 1 || risk.Rev != 3 || limit.Rev != 5 {
		t.Fatalf("revisions %d %d %d", cb.Rev, risk.Rev, limit.Rev)
	}
	if !reflect.DeepEqual(limit.Causes, []CauseRef{{"risk:42:score", 3}}) {
		t.Fatalf("CauseLatest resolved to %v", limit.Causes)
	}
	check := func(db *DB) {
		t.Helper()
		got := must[Revision](t)(db.Get(bg, "account:42:limit"))
		if string(got.Value) != "50" || got.Actor != "limits-service" || got.Reason != "risk policy v3" || got.Rev != 5 || got.Tx != 5 {
			t.Fatalf("got %+v", got)
		}
		if !reflect.DeepEqual(got.Causes, limit.Causes) || !got.Time.Equal(limit.Time) {
			t.Fatalf("causes or time differ: %+v vs %+v", got, limit)
		}
		c := must[Revision](t)(db.Get(bg, "chargeback:cb_991"))
		if c.Ref != "BANK-7731" {
			t.Fatalf("ref %q", c.Ref)
		}
	}
	check(db)
	db.Close()
	if _, err := os.Stat(path + snapshotSuffix); err != nil {
		t.Fatalf("no snapshot after close: %v", err)
	}
	check(openT(t, path, nil))
}

func TestStrictAndRelaxed(t *testing.T) {
	db := openT(t, tmpPath(t), nil)
	if _, err := db.Put(bg, "k", []byte("v")); !errors.Is(err, ErrNoReason) {
		t.Fatalf("strict mode accepted a write without a reason: %v", err)
	}
	st, _ := db.Stats()
	if st.Revisions != 0 {
		t.Fatal("refused write was stored")
	}
	r, err := OpenMemory(&Options{Relaxed: true, DefaultActor: "cron"})
	if err != nil {
		t.Fatal(err)
	}
	rv := must[Revision](t)(r.Put(bg, "k", []byte("v")))
	if rv.Reason != "unspecified" || rv.Actor != "cron" {
		t.Fatalf("relaxed write recorded %+v", rv)
	}
	rv = must[Revision](t)(r.Put(bg, "k", []byte("w"), Reason("x"), Actor("me")))
	if rv.Actor != "me" {
		t.Fatalf("actor option ignored: %q", rv.Actor)
	}
}

func TestBadCausesWriteNothing(t *testing.T) {
	path := tmpPath(t)
	db := openT(t, path, nil)
	cb, _, _ := chain(t, db)
	before, _ := db.Stats()
	size := before.FileSize
	cases := map[string]WriteOption{
		"unknown future rev": Cause("x", 999),
		"zero rev":           Cause("x", 0),
		"wrong key":          Cause("risk:42:score", cb.Rev),
		"latest of nothing":  CauseLatest("nobody"),
		"empty key":          Cause("", 1),
	}
	for name, opt := range cases {
		if _, err := db.Put(bg, "y", []byte("1"), Reason("r"), opt); !errors.Is(err, ErrBadCause) {
			t.Errorf("%s: want ErrBadCause, got %v", name, err)
		}
	}
	tooMany := []WriteOption{Reason("r")}
	for i := 0; i < MaxCauses+1; i++ {
		tooMany = append(tooMany, Cause(cb.Key, cb.Rev))
	}
	if _, err := db.Put(bg, "y", []byte("1"), tooMany...); !errors.Is(err, ErrInvalid) {
		t.Errorf("9 causes: %v", err)
	}
	after, _ := db.Stats()
	if after.FileSize != size || after.Revisions != before.Revisions || after.LastRev != before.LastRev {
		t.Fatalf("refused writes changed the store: %+v -> %+v", before, after)
	}
	fi, _ := os.Stat(path)
	if fi.Size() != size {
		t.Fatalf("file grew from %d to %d", size, fi.Size())
	}
}

func TestDuplicateCausesCollapse(t *testing.T) {
	db := openT(t, tmpPath(t), nil)
	cb, _, _ := chain(t, db)
	r := must[Revision](t)(db.Put(bg, "z", nil, Reason("r"), Cause(cb.Key, cb.Rev), CauseLatest(cb.Key)))
	if len(r.Causes) != 1 {
		t.Fatalf("causes %v", r.Causes)
	}
}

func TestTransactions(t *testing.T) {
	path := tmpPath(t)
	db := openT(t, path, nil)
	ctx := WithActor(bg, "pricing")
	var basic Revision
	err := db.Update(ctx, func(tx *Tx) error {
		var err error
		basic, err = tx.Put("price:basic", []byte("9"))
		if err != nil {
			return err
		}
		// A cause may point at a write earlier in the same transaction.
		if _, err := tx.Put("price:pro", []byte("29"), Cause("price:basic", basic.Rev)); err != nil {
			return err
		}
		got, err := tx.Get("price:basic")
		if err != nil || string(got.Value) != "9" {
			t.Fatalf("tx.Get: %v %v", got, err)
		}
		return nil
	}, Reason("Q4 price list"), Ref("PRICE-12"))
	if err != nil {
		t.Fatal(err)
	}
	pro := must[Revision](t)(db.Get(bg, "price:pro"))
	if pro.Tx != basic.Tx || pro.Reason != "Q4 price list" || pro.Ref != "PRICE-12" || pro.Actor != "pricing" {
		t.Fatalf("tx defaults not applied: %+v", pro)
	}
	// A failed transaction writes nothing.
	boom := errors.New("boom")
	err = db.Update(bg, func(tx *Tx) error {
		tx.Put("price:basic", []byte("0"))
		return boom
	}, Reason("bad"))
	if !errors.Is(err, boom) {
		t.Fatal(err)
	}
	if v := must[Revision](t)(db.Get(bg, "price:basic")); string(v.Value) != "9" {
		t.Fatalf("rolled-back write visible: %s", v.Value)
	}
	// The next transaction continues the numbering.
	next := must[Revision](t)(db.Put(bg, "x", []byte("1"), Reason("r")))
	if next.Rev != 3 || next.Tx != 2 {
		t.Fatalf("numbering after rollback: rev %d tx %d", next.Rev, next.Tx)
	}
	// A Tx can't be used after Update returns.
	var kept *Tx
	db.Update(bg, func(tx *Tx) error { kept = tx; return nil })
	if _, err := kept.Put("a", nil, Reason("r")); !errors.Is(err, ErrTxFinished) {
		t.Fatalf("finished tx accepted a write: %v", err)
	}
}

func TestDeleteHistoryGetAt(t *testing.T) {
	db := openT(t, tmpPath(t), nil)
	put := must[Revision](t)
	put(db.Put(bg, "a", []byte("1"), Reason("one")))
	put(db.Put(bg, "b", []byte("x"), Reason("other key")))
	put(db.Put(bg, "a", []byte("2"), Reason("two")))
	del := put(db.Delete(bg, "a", Reason("gone")))
	if _, err := db.Get(bg, "a"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted key: %v", err)
	}
	h := must[[]Revision](t)(db.History(bg, "a", 0))
	if len(h) != 3 || h[0].Rev != del.Rev || !h[0].Deleted || string(h[2].Value) != "1" {
		t.Fatalf("history %+v", h)
	}
	if h := must[[]Revision](t)(db.History(bg, "a", 2)); len(h) != 2 {
		t.Fatalf("limit ignored: %d", len(h))
	}
	if v := must[Revision](t)(db.GetAt(bg, "a", 2)); string(v.Value) != "1" {
		t.Fatalf("GetAt 2: %s", v.Value)
	}
	if v := must[Revision](t)(db.GetAt(bg, "a", 3)); string(v.Value) != "2" {
		t.Fatalf("GetAt 3: %s", v.Value)
	}
	if _, err := db.GetAt(bg, "a", 4); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetAt after delete: %v", err)
	}
	if v := must[Revision](t)(db.Revision(bg, 2)); v.Key != "b" {
		t.Fatalf("Revision(2) = %+v", v)
	}
	if _, err := db.Revision(bg, 99); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	keys := must[[]string](t)(db.Keys(bg, ""))
	if !reflect.DeepEqual(keys, []string{"b"}) {
		t.Fatalf("keys %v", keys)
	}
	put(db.Put(bg, "a", []byte("3"), Reason("back")))
	keys = must[[]string](t)(db.Keys(bg, "a"))
	if !reflect.DeepEqual(keys, []string{"a"}) {
		t.Fatalf("keys after re-put %v", keys)
	}
}

func TestWhyChain(t *testing.T) {
	db := openT(t, tmpPath(t), nil)
	cb, risk, limit := chain(t, db)
	n := must[*WhyNode](t)(db.Why(bg, "account:42:limit", 0))
	if n.Rev != limit.Rev || n.Prev == nil || string(n.Prev.Value) != "100" {
		t.Fatalf("root %+v", n)
	}
	if len(n.Causes) != 1 || n.Causes[0].Rev != risk.Rev || string(n.Causes[0].Prev.Value) != "0.31" {
		t.Fatalf("risk node %+v", n.Causes)
	}
	c := n.Causes[0].Causes
	if len(c) != 1 || c[0].Rev != cb.Rev || c[0].Prev != nil || len(c[0].Causes) != 0 || c[0].Truncated {
		t.Fatalf("chargeback node %+v", c)
	}
	// Depth 1 shows the direct cause and marks it truncated.
	n = must[*WhyNode](t)(db.Why(bg, "account:42:limit", 1))
	if len(n.Causes) != 1 || !n.Causes[0].Truncated || len(n.Causes[0].Causes) != 0 {
		t.Fatalf("depth 1: %+v", n.Causes[0])
	}
	if _, err := db.Why(bg, "nobody", 0); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	b, err := json.Marshal(must[*WhyNode](t)(db.WhyRev(bg, risk.Rev, 0)))
	if err != nil || !bytes.Contains(b, []byte(`"key":"chargeback:cb_991"`)) {
		t.Fatalf("json: %s %v", b, err)
	}
}

func TestWhyDiamondMarksRepeat(t *testing.T) {
	db := openT(t, tmpPath(t), nil)
	put := must[Revision](t)
	root := put(db.Put(bg, "root", []byte("r"), Reason("start")))
	a := put(db.Put(bg, "a", nil, Reason("from root"), Cause("root", root.Rev)))
	b := put(db.Put(bg, "b", nil, Reason("from root"), Cause("root", root.Rev)))
	put(db.Put(bg, "top", nil, Reason("both"), Cause("a", a.Rev), Cause("b", b.Rev)))
	n := must[*WhyNode](t)(db.Why(bg, "top", 0))
	if len(n.Causes) != 2 {
		t.Fatal(n)
	}
	first, second := n.Causes[0].Causes[0], n.Causes[1].Causes[0]
	if first.Rev != root.Rev || second.Rev != root.Rev || first.Repeat || second.Repeat {
		// root names no causes, so neither copy is marked.
		t.Fatalf("diamond: %+v %+v", first, second)
	}
	// A repeated node that has causes is marked and not expanded twice.
	put(db.Put(bg, "a2", nil, Reason("x"), Cause("a", a.Rev)))
	put(db.Put(bg, "top2", nil, Reason("y"), Cause("a", a.Rev), CauseLatest("a2")))
	n = must[*WhyNode](t)(db.Why(bg, "top2", 0))
	if n.Causes[0].Repeat || !n.Causes[1].Causes[0].Repeat || len(n.Causes[1].Causes[0].Causes) != 0 {
		t.Fatalf("repeat marking: %+v", n.Causes[1].Causes[0])
	}
}

func TestLocking(t *testing.T) {
	path := tmpPath(t)
	db := openT(t, path, nil)
	chain(t, db)
	if _, err := Open(path, nil); !errors.Is(err, ErrLocked) {
		t.Fatalf("second writer: %v", err)
	}
	ro, err := Open(path, &Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()
	if v := must[Revision](t)(ro.Get(bg, "account:42:limit")); string(v.Value) != "50" {
		t.Fatal(v)
	}
	if _, err := ro.Put(bg, "k", nil, Reason("r")); !errors.Is(err, ErrReadOnly) {
		t.Fatal(err)
	}
	if _, err := ro.Compact(bg, CompactOptions{}); !errors.Is(err, ErrReadOnly) {
		t.Fatal(err)
	}
	db.Close()
	db2 := openT(t, path, nil)
	if _, err := db2.Get(bg, "risk:42:score"); err != nil {
		t.Fatal(err)
	}
}

func TestClosed(t *testing.T) {
	db := openT(t, tmpPath(t), nil)
	db.Close()
	if _, err := db.Get(bg, "a"); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if _, err := db.Put(bg, "a", nil, Reason("r")); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestLimits(t *testing.T) {
	db, _ := OpenMemory(nil)
	big := strings.Repeat("k", MaxKeySize+1)
	if _, err := db.Put(bg, big, nil, Reason("r")); !errors.Is(err, ErrTooLarge) {
		t.Fatal(err)
	}
	if _, err := db.Put(bg, "", nil, Reason("r")); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := db.Put(bg, "\xff", nil, Reason("r")); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := db.Put(bg, "k", make([]byte, MaxValueSize+1), Reason("r")); !errors.Is(err, ErrTooLarge) {
		t.Fatal(err)
	}
	if _, err := db.Put(bg, "k", nil, Reason(strings.Repeat("r", MaxReasonSize+1))); !errors.Is(err, ErrTooLarge) {
		t.Fatal(err)
	}
	err := db.Update(bg, func(tx *Tx) error {
		for i := 0; i < 5; i++ {
			if _, err := tx.Put("k", make([]byte, MaxValueSize), Reason("r")); err != nil {
				return err
			}
		}
		return nil
	})
	if !errors.Is(err, ErrTooLarge) {
		t.Fatalf("oversized transaction: %v", err)
	}
	// Binary values round-trip, through JSON too.
	val := []byte{0, 1, 2, 0xff, 0xfe}
	r := must[Revision](t)(db.Put(bg, "bin", val, Reason("r")))
	b, _ := json.Marshal(r)
	var back Revision
	if err := json.Unmarshal(b, &back); err != nil || !bytes.Equal(back.Value, val) || !bytes.Contains(b, []byte(`"encoding":"base64"`)) {
		t.Fatalf("json round trip: %s %v", b, err)
	}
}

func TestSnapshotIsCheckedAgainstTheFile(t *testing.T) {
	path := tmpPath(t)
	db := openT(t, path, nil)
	chain(t, db)
	db.Close()

	// Writes after the snapshot are picked up by scanning the tail.
	db = openT(t, path, nil)
	must[Revision](t)(db.Put(bg, "late", []byte("1"), Reason("after snapshot")))
	// Simulate a crash: no Close, so the snapshot is not rewritten.
	db.st.Close()
	db.closed = true
	db = openT(t, path, nil)
	if _, err := db.Get(bg, "late"); err != nil {
		t.Fatalf("write after the snapshot lost: %v", err)
	}
	db.Close()

	// A snapshot from a different file is ignored.
	other := tmpPath(t)
	o := openT(t, other, nil)
	must[Revision](t)(o.Put(bg, "o", []byte("o"), Reason("r")))
	o.Close()
	snap, _ := os.ReadFile(other + snapshotSuffix)
	os.WriteFile(path+snapshotSuffix, snap, 0o644)
	db = openT(t, path, nil)
	if _, err := db.Get(bg, "o"); !errors.Is(err, ErrNotFound) {
		t.Fatal("foreign snapshot was trusted")
	}
	if _, err := db.Get(bg, "late"); err != nil {
		t.Fatal(err)
	}
	db.Close()

	// A garbled snapshot is ignored.
	snap, _ = os.ReadFile(path + snapshotSuffix)
	snap[len(snap)/2] ^= 0xff
	os.WriteFile(path+snapshotSuffix, snap, 0o644)
	db = openT(t, path, nil)
	if st, _ := db.Stats(); st.Revisions != 6 {
		t.Fatalf("revisions after garbled snapshot: %d", st.Revisions)
	}
}

func TestCheck(t *testing.T) {
	path := tmpPath(t)
	db := openT(t, path, nil)
	chain(t, db)
	db.Close()
	rep, err := Check(path)
	if err != nil || !rep.OK() || rep.Revisions != 5 || rep.Keys != 3 || rep.Transactions != 5 || rep.LastRev != 5 {
		t.Fatalf("%+v %v", rep, err)
	}
}

func TestMemoryStore(t *testing.T) {
	db, err := OpenMemory(&Options{Now: clock()})
	if err != nil {
		t.Fatal(err)
	}
	chain(t, db)
	st := must[CompactStats](t)(db.Compact(bg, CompactOptions{}))
	if st.RevisionsAfter != 3 || st.KeptForCauses != 0 {
		t.Fatalf("memory compaction %+v", st)
	}
	n := must[*WhyNode](t)(db.Why(bg, "account:42:limit", 0))
	if n.Causes[0].Causes[0].Key != "chargeback:cb_991" {
		t.Fatal("chain broken")
	}
}
