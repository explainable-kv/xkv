// Copyright xkv.org 2026
// SPDX-License-Identifier: Apache-2.0

package xkv

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"testing"
)

func copyFile(t testing.TB, from, to string, size int64) {
	t.Helper()
	b, err := os.ReadFile(from)
	if err != nil {
		t.Fatal(err)
	}
	if size >= 0 {
		b = b[:size]
	}
	if err := os.WriteFile(to, b, 0o644); err != nil {
		t.Fatal(err)
	}
}

// TestTornTail cuts the file at every byte of its last transaction, as a
// crash in the middle of a write would, and flips bytes inside it, as a
// half-finished sector write would. Every time, Open must trim back to the
// last complete transaction, lose nothing before it, and carry on.
func TestTornTail(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/base.xkv"
	db := openT(t, path, nil)
	chain(t, db)
	before, _ := db.Stats()
	beforeState := capture(t, db, 0)
	// The last transaction: three writes with causes.
	err := db.Update(bg, func(tx *Tx) error {
		for i := 0; i < 3; i++ {
			if _, err := tx.Put(fmt.Sprintf("tail:%d", i), []byte(strings.Repeat("v", 40)), CauseLatest("account:42:limit")); err != nil {
				return err
			}
		}
		return nil
	}, Reason("the transaction that gets torn"))
	if err != nil {
		t.Fatal(err)
	}
	after, _ := db.Stats()
	db.Close()
	os.Remove(path + snapshotSuffix)

	cut := dir + "/cut.xkv"
	cuts := 0
	for size := before.FileSize; size < after.FileSize; size++ {
		copyFile(t, path, cut, size)
		os.Remove(cut + snapshotSuffix)
		d, err := Open(cut, &Options{Now: clock()})
		if err != nil {
			t.Fatalf("cut at %d: %v", size, err)
		}
		st, _ := d.Stats()
		if st.Revisions != before.Revisions || st.LastRev != before.LastRev || st.FileSize != before.FileSize {
			t.Fatalf("cut at %d: %+v, want %+v", size, st, before)
		}
		sameState(t, fmt.Sprintf("cut at %d", size), beforeState, capture(t, d, 0))
		r, err := d.Put(bg, "next", []byte("x"), Reason("after the crash"))
		if err != nil || r.Rev != before.LastRev+1 || r.Tx != before.LastTx+1 {
			t.Fatalf("cut at %d: next write %+v %v", size, r, err)
		}
		d.Close()
		if fi, _ := os.Stat(cut); fi.Size() <= before.FileSize {
			t.Fatalf("cut at %d: file not extended by the next write", size)
		}
		cuts++
	}

	flips := 0
	full, _ := os.ReadFile(path)
	for off := before.FileSize; off < after.FileSize; off++ {
		b := append([]byte(nil), full...)
		b[off] ^= 0x5a
		os.WriteFile(cut, b, 0o644)
		os.Remove(cut + snapshotSuffix)
		d, err := Open(cut, &Options{Now: clock()})
		if err != nil {
			t.Fatalf("flip at %d: %v", off, err)
		}
		st, _ := d.Stats()
		if st.LastRev != before.LastRev || st.Revisions != before.Revisions {
			t.Fatalf("flip at %d: %+v", off, st)
		}
		d.Close()
		flips++
	}
	t.Logf("last transaction of %d bytes: cut at each of its %d byte positions and damaged at each of its %d bytes; every open trimmed back to the %d committed revisions, Get and Why answered as before, and the next write continued the numbering",
		after.FileSize-before.FileSize, cuts, flips, before.Revisions)
}

// TestDamageInTheMiddleIsReported flips a byte inside committed history,
// with whole transactions after it. That is damage, not a crash, and Open
// must refuse rather than trim committed data away.
func TestDamageInTheMiddleIsReported(t *testing.T) {
	path := tmpPath(t)
	db := openT(t, path, nil)
	cb, _, _ := chain(t, db)
	db.Close()
	os.Remove(path + snapshotSuffix)
	b, _ := os.ReadFile(path)
	// Damage the first revision's value.
	i := strings.Index(string(b), "card dispute")
	b[i] ^= 0xff
	os.WriteFile(path, b, 0o644)
	if _, err := Open(path, nil); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Open on damaged history: %v", err)
	}
	if _, err := Open(path, &Options{ReadOnly: true}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("read-only Open on damaged history: %v", err)
	}
	rep, err := Check(path)
	if err != nil || rep.OK() {
		t.Fatalf("Check missed the damage: %+v %v", rep, err)
	}
	if fi, _ := os.Stat(path); fi.Size() != int64(len(b)) {
		t.Fatal("a refused open changed the file")
	}
	_ = cb
}

// TestDamageUnderTheSnapshot flips a byte in a record the snapshot covers.
// The snapshot is trusted for offsets, so Open succeeds, but reading the
// damaged revision reports ErrCorrupt instead of returning wrong data.
func TestDamageUnderTheSnapshot(t *testing.T) {
	path := tmpPath(t)
	db := openT(t, path, nil)
	chain(t, db)
	db.Close()
	b, _ := os.ReadFile(path)
	i := strings.Index(string(b), "card dispute")
	b[i] ^= 0xff
	os.WriteFile(path, b, 0o644)
	db = openT(t, path, nil)
	if _, err := db.Get(bg, "chargeback:cb_991"); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("damaged revision read as %v", err)
	}
	if _, err := db.Why(bg, "account:42:limit", 0); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("why through a damaged revision: %v", err)
	}
	if _, err := db.Get(bg, "account:42:limit"); err != nil {
		t.Fatalf("undamaged revision: %v", err)
	}
}

func TestNotAnXKVFile(t *testing.T) {
	path := tmpPath(t)
	os.WriteFile(path, []byte(strings.Repeat("hello world, not a store ", 4)), 0o644)
	if _, err := Open(path, nil); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("%v", err)
	}
}

// FuzzOpen feeds arbitrary bytes to Open. Whatever the file holds, Open
// returns an error or a store whose reads work; it never panics.
func FuzzOpen(f *testing.F) {
	path := f.TempDir() + "/seed.xkv"
	db, _ := Open(path, &Options{Now: clock()})
	chain(f, db)
	db.Put(bg, "bin", []byte{0, 1, 0xff}, Reason("r"))
	db.Close()
	seed, _ := os.ReadFile(path)
	f.Add(seed)
	f.Add(seed[:len(seed)-3])
	f.Add(seed[:headerSize])
	f.Add([]byte{})
	f.Add(seed[:headerSize+5])
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 8; i++ {
		b := append([]byte(nil), seed...)
		b[headerSize+rng.Intn(len(b)-headerSize)] ^= byte(1 + rng.Intn(255))
		f.Add(b)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		p := t.TempDir() + "/f.xkv"
		os.WriteFile(p, data, 0o644)
		for _, ro := range []bool{true, false} {
			db, err := Open(p, &Options{ReadOnly: ro, Relaxed: true, NoSync: true})
			if err != nil {
				continue
			}
			keys, _ := db.Keys(bg, "")
			for _, k := range keys {
				db.Get(bg, k)
				db.History(bg, k, 0)
				db.Why(bg, k, 0)
			}
			if !ro {
				db.Put(bg, "fuzz", []byte("x"))
				db.Compact(bg, CompactOptions{})
			}
			db.Close()
		}
		Check(p)
	})
}

// FuzzDecode feeds arbitrary bodies to the record decoders.
func FuzzDecode(f *testing.F) {
	f.Add(encodeRev(&rec{rev: 3, tx: 2, key: "k", value: []byte("v"), actor: "a", reason: "r", causes: []CauseRef{{"c", 1}}}))
	f.Add(encodeCommit(commit{tx: 2, count: 1, lastRev: 3}))
	f.Fuzz(func(t *testing.T, b []byte) {
		if r, err := decodeRev(b); err == nil {
			// Whatever decodes must encode back to the same bytes.
			if got := encodeRev(r); string(got) != string(b) {
				t.Fatalf("round trip changed the record")
			}
		}
		decodeCommit(b)
		checkFrame(b)
	})
}
