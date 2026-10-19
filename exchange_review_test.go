// Copyright xkv.org 2026
// SPDX-License-Identifier: Apache-2.0

package xkv

// Regression tests for the findings of the independent review of 0.2.

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func putv(t testing.TB, db *DB, k, v string) Revision {
	t.Helper()
	return must[Revision](t)(db.Put(bg, k, []byte(v), Reason("r"), Actor("x")))
}

func valueOf(t testing.TB, db *DB, k string) string {
	t.Helper()
	r, err := db.Get(bg, k)
	if errors.Is(err, ErrNotFound) {
		return "<none>"
	}
	if err != nil {
		t.Fatal(err)
	}
	return string(r.Value)
}

// Finding 1: a copied store shares its id. Importing from the copy must be
// refused, not quietly skipped; after new-id on the copy it works, and the
// history the two share is recognised as shared.
func TestCopiedStoreIsRefusedThenNewID(t *testing.T) {
	dir := t.TempDir()
	pa, pb := filepath.Join(dir, "a.xkv"), filepath.Join(dir, "b.xkv")
	a := openT(t, pa, nil)
	putv(t, a, "k", "base")
	a.Close()
	b1, _ := os.ReadFile(pa)
	os.WriteFile(pb, b1, 0o644)
	a = openT(t, pa, nil)
	b := openT(t, pb, nil)
	putv(t, a, "x", "from-a")
	putv(t, b, "y", "from-b")
	d, _ := exportT(t, b, 0)
	if _, err := a.Import(bg, bytes.NewReader(d), ImportOptions{}, Reason("s")); !errors.Is(err, ErrBadExchange) || !strings.Contains(err.Error(), "new-id") {
		t.Fatalf("import from a copy: %v", err)
	}
	if valueOf(t, a, "y") != "<none>" {
		t.Fatal("a refused import wrote something")
	}
	// b's own write after the copy diverged: keep only what was shared.
	if err := b.NewStoreID(bg, 1); err != nil {
		t.Fatal(err)
	}
	idA, _ := a.StoreID()
	idB, _ := b.StoreID()
	if idA == idB {
		t.Fatal("new-id kept the id")
	}
	d, _ = exportT(t, b, 0)
	rep := importT(t, a, d, NewestWins)
	if valueOf(t, a, "y") != "from-b" || rep.Imported != 1 || rep.Skipped != 1 {
		t.Fatalf("after new-id: y=%s %+v", valueOf(t, a, "y"), rep)
	}
}

// Finding 1: a store restored from a backup must not take back revisions
// claiming its own id above what it has written.
func TestRestoredBackupIsRefused(t *testing.T) {
	dir := t.TempDir()
	pa := filepath.Join(dir, "a.xkv")
	a := openT(t, pa, nil)
	putv(t, a, "k", "1")
	a.Close()
	backup, _ := os.ReadFile(pa)
	a = openT(t, pa, nil)
	putv(t, a, "k", "2")
	putv(t, a, "k", "3")
	d, _ := exportT(t, a, 0)
	b := openT(t, tmpPath(t), nil)
	importT(t, b, d, NewestWins)
	a.Close()
	os.WriteFile(pa, backup, 0o644)
	os.Remove(pa + snapshotSuffix)
	a = openT(t, pa, nil)
	dB, _ := exportT(t, b, 0)
	if _, err := a.Import(bg, bytes.NewReader(dB), ImportOptions{}, Reason("s")); !errors.Is(err, ErrBadExchange) {
		t.Fatalf("restored store took back its own future: %v", err)
	}
	// A local write reuses identity a:2, which b holds as something else:
	// b refuses a's export rather than mixing the two up.
	putv(t, a, "k", "2-after-restore")
	dA, _ := exportT(t, a, 0)
	if _, err := b.Import(bg, bytes.NewReader(dA), ImportOptions{}, Reason("s")); !errors.Is(err, ErrBadExchange) {
		t.Fatalf("b took a colliding identity: %v", err)
	}
	// new-id keeping only what was written before the backup fixes it.
	if err := a.NewStoreID(bg, 1); err != nil {
		t.Fatal(err)
	}
	dA, _ = exportT(t, a, 0)
	rep := importT(t, b, dA, NewestWins)
	if len(rep.Conflicts) != 1 {
		t.Fatalf("the restored store's new write should meet b's later ones as a conflict: %+v", rep)
	}
	importT(t, a, must2(exportT(t, b, 0)), NewestWins)
	sameValues(t, a, b)
}

func must2(b []byte, _ ExportStats) []byte { return b }

// Finding 2: after both sides agree, or settle a conflict, a later edit on
// one side is an ordinary change, whatever the policy and the clocks.
func TestEditAfterSettlingIsNoConflict(t *testing.T) {
	for _, p := range []ConflictPolicy{NewestWins, KeepOurs, TakeTheirs} {
		for _, skew := range []time.Duration{0, -time.Hour, time.Hour} {
			t.Run(fmt.Sprintf("%v/%v", p, skew), func(t *testing.T) {
				ca := clock()
				cbBase := clock()
				a := openT(t, tmpPath(t), &Options{Now: ca})
				b := openT(t, tmpPath(t), &Options{Now: func() time.Time { return cbBase().Add(skew) }})
				var sa, sb uint64
				sync := func() (ImportReport, ImportReport) {
					d, st := exportT(t, a, sa)
					sa = st.Through
					r1 := importT(t, b, d, p)
					d, st = exportT(t, b, sb)
					sb = st.Through
					r2 := importT(t, a, d, p)
					return r1, r2
				}
				// Agreed: both write the same value.
				putv(t, a, "same", "v")
				putv(t, b, "same", "v")
				// Conflict: both write different values.
				putv(t, a, "k", "va")
				putv(t, b, "k", "vb")
				sync()
				sync()
				// Now each side edits one key the other doesn't touch.
				putv(t, b, "same", "w")
				putv(t, a, "k", "a-later")
				r1, r2 := sync()
				r3, r4 := sync()
				if n := len(r1.Conflicts) + len(r2.Conflicts) + len(r3.Conflicts) + len(r4.Conflicts); n != 0 {
					t.Fatalf("edits after settling made %d conflicts: %+v %+v", n, r1, r2)
				}
				if valueOf(t, a, "same") != "w" || valueOf(t, b, "same") != "w" {
					t.Fatalf("same: a=%s b=%s", valueOf(t, a, "same"), valueOf(t, b, "same"))
				}
				if valueOf(t, a, "k") != "a-later" || valueOf(t, b, "k") != "a-later" {
					t.Fatalf("k: a=%s b=%s", valueOf(t, a, "k"), valueOf(t, b, "k"))
				}
			})
		}
	}
}

// Finding 3: importing the same file again after the importer compacted
// changes nothing.
func TestReimportAfterCompactionChangesNothing(t *testing.T) {
	for _, p := range []ConflictPolicy{NewestWins, KeepOurs, TakeTheirs} {
		a := openT(t, tmpPath(t), nil)
		b := openT(t, tmpPath(t), nil)
		putv(t, a, "k", "v1")
		putv(t, a, "k", "v2")
		putv(t, a, "other", "x")
		d, _ := exportT(t, a, 0)
		importT(t, b, d, p)
		must[CompactStats](t)(b.Compact(bg, CompactOptions{}))
		before := must[Stats](t)(b.Stats())
		rep := importT(t, b, d, p)
		after := must[Stats](t)(b.Stats())
		if valueOf(t, b, "k") != "v2" || len(rep.Conflicts) != 0 || rep.Imported != 0 || after.Revisions != before.Revisions {
			t.Fatalf("%v: reimport after compaction: k=%s %+v, revisions %d -> %d", p, valueOf(t, b, "k"), rep, before.Revisions, after.Revisions)
		}
	}
}

// Finding 4: a source transaction at the size limit can be imported.
func TestImportOfAFullTransaction(t *testing.T) {
	if testing.Short() {
		t.Skip("slow")
	}
	a, _ := OpenMemory(nil)
	n := 0
	value := bytes.Repeat([]byte("v"), 4000)
	err := a.Update(bg, func(tx *Tx) error {
		for {
			_, err := tx.Put(fmt.Sprintf("k%06d", n), value)
			if errors.Is(err, ErrTooLarge) {
				return nil
			}
			if err != nil {
				return err
			}
			n++
		}
	}, Reason("fill"), Actor("x"))
	if err != nil {
		t.Fatal(err)
	}
	d, _ := exportT(t, a, 0)
	b, _ := OpenMemory(nil)
	rep, err := b.Import(bg, bytes.NewReader(d), ImportOptions{}, Reason("s"))
	if err != nil || rep.Imported != n {
		t.Fatalf("a full transaction of %d revisions: %v %+v", n, err, rep)
	}
}

// Finding 5: compaction on the importer while imports and writes run.
func TestImportWhileCompacting(t *testing.T) {
	a := openT(t, tmpPath(t), &Options{NoSync: true})
	b := openT(t, tmpPath(t), &Options{NoSync: true})
	var revs []Revision
	for i := 0; i < 300; i++ {
		opts := []WriteOption{Reason("r"), Actor("x")}
		if len(revs) > 0 {
			r := revs[len(revs)/2]
			opts = append(opts, Cause(r.Key, r.Rev))
		}
		revs = append(revs, must[Revision](t)(a.Put(bg, fmt.Sprintf("k%d", i%7), []byte(fmt.Sprint(i)), opts...)))
	}
	d, _ := exportT(t, a, 0)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 30; i++ {
			b.Compact(bg, CompactOptions{})
			b.Put(bg, "local", []byte(fmt.Sprint(i)), Reason("r"), Actor("y"))
		}
	}()
	for i := 0; i < 5; i++ {
		if _, err := b.Import(bg, bytes.NewReader(d), ImportOptions{}, Reason("s")); err != nil {
			t.Fatalf("import %d while compacting: %v", i, err)
		}
	}
	<-done
	for k := 0; k < 7; k++ {
		key := fmt.Sprintf("k%d", k)
		if valueOf(t, a, key) != valueOf(t, b, key) {
			t.Fatalf("%s differs", key)
		}
	}
}

func reseal(lines []string) []byte {
	body := strings.Join(lines, "\n") + "\n"
	h := sha256.Sum256([]byte(body))
	revs := 0
	for _, l := range lines[1:] {
		revs += strings.Count(l, `"id":`)
	}
	return []byte(body + fmt.Sprintf(`{"end":{"lines":%d,"revisions":%d,"sha256":"%s"}}`, len(lines), revs, hex.EncodeToString(h[:])) + "\n")
}

// Finding 6: lines must be exactly what Export writes.
func TestExchangeLinesMustBeCanonical(t *testing.T) {
	a := openT(t, tmpPath(t), nil)
	putv(t, a, "k", "v")
	d, _ := exportT(t, a, 0)
	lines := strings.Split(strings.TrimSuffix(string(d), "\n"), "\n")
	lines = lines[:len(lines)-1]
	b := openT(t, tmpPath(t), nil)
	ok := reseal(lines)
	if _, err := b.Import(bg, bytes.NewReader(ok), ImportOptions{}, Reason("s")); err != nil {
		t.Fatalf("resealed original refused: %v", err)
	}
	b = openT(t, tmpPath(t), nil)
	bad := map[string][]string{
		"text after the object": {lines[0], lines[1] + ` {"junk":1}`},
		"upper-case field":      {lines[0], strings.Replace(lines[1], `"key"`, `"KEY"`, 1)},
		"repeated field":        {lines[0], strings.Replace(lines[1], `"key":"k"`, `"key":"k","key":"k"`, 1)},
		"no time":               {lines[0], `{"tx":[{"id":"` + strings.Repeat("a", 32) + `:1","key":"z","value":"q","actor":"a","reason":"r","time":"0001-01-01T00:00:00Z"}]}`},
		"no reason":             {lines[0], `{"tx":[{"id":"` + strings.Repeat("a", 32) + `:1","key":"z","value":"q","actor":"a","reason":"","time":"2026-01-01T00:00:00Z"}]}`},
		"spaces":                {lines[0], strings.Replace(lines[1], `{"tx":`, `{ "tx":`, 1)},
	}
	for name, ls := range bad {
		if _, err := b.Import(bg, bytes.NewReader(reseal(ls)), ImportOptions{}, Reason("s")); !errors.Is(err, ErrBadExchange) {
			t.Fatalf("%s: %v", name, err)
		}
	}
	if s := must[Stats](t)(b.Stats()); s.Revisions != 0 {
		t.Fatalf("refused files wrote %d revisions", s.Revisions)
	}
}

// A long import reason still leaves room for the conflict note.
func TestLongReasonConflict(t *testing.T) {
	a := openT(t, tmpPath(t), nil)
	b := openT(t, tmpPath(t), nil)
	putv(t, a, "k", "a")
	putv(t, b, "k", "b")
	d, _ := exportT(t, a, 0)
	rep, err := b.Import(bg, bytes.NewReader(d), ImportOptions{}, Reason(strings.Repeat("é", MaxReasonSize/2)))
	if err != nil || len(rep.Conflicts) != 1 {
		t.Fatalf("%v %+v", err, rep)
	}
	res := must[Revision](t)(b.Revision(bg, rep.Conflicts[0].Resolution))
	if len(res.Reason) > MaxReasonSize || !strings.Contains(res.Reason, "(import conflict: kept ") {
		t.Fatalf("reason of %d bytes: ...%s", len(res.Reason), res.Reason[len(res.Reason)-30:])
	}
}
