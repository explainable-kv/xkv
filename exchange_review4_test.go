package xkv

import (
	mrand "math/rand"
	"os"
	"path/filepath"
	"testing"
)

// Compacting the copy before new-id drops its own revisions at or below
// keepThrough, so the newest shared one is unknown and the post-copy edit
// gets no seen entry for the old id: the original sees a conflict again.
func TestR4NewIDAfterCompaction(t *testing.T) {
	for _, p := range []ConflictPolicy{NewestWins, KeepOurs} {
		dir := t.TempDir()
		pa, pb := filepath.Join(dir, "a.xkv"), filepath.Join(dir, "b.xkv")
		a := openT(t, pa, nil)
		putv(t, a, "k", "base1")
		putv(t, a, "k", "base2")
		a.Close()
		b1, _ := os.ReadFile(pa)
		os.WriteFile(pb, b1, 0o644)
		a = openT(t, pa, nil)
		b := openT(t, pb, nil)
		putv(t, b, "k", "copy-edit")
		must[CompactStats](t)(b.Compact(bg, CompactOptions{}))
		if err := b.NewStoreID(bg, 2); err != nil {
			t.Fatal(err)
		}
		rep := importT(t, a, must2(exportT(t, b, 0)), p)
		if len(rep.Conflicts) != 0 || valueOf(t, a, "k") != "copy-edit" {
			t.Errorf("%v: %+v k=%s", p, rep, valueOf(t, a, "k"))
		}
	}
}

// new-id, then exchange, compaction, reopen: live, snapshot and scan agree,
// and the two stores settle.
func TestR4NewIDReopenEquivalence(t *testing.T) {
	dir := t.TempDir()
	pa, pb := filepath.Join(dir, "a.xkv"), filepath.Join(dir, "b.xkv")
	a := openT(t, pa, nil)
	for i := 0; i < 5; i++ {
		putv(t, a, "k", "v"+string(rune('0'+i)))
		putv(t, a, "j", "w"+string(rune('0'+i)))
	}
	a.Close()
	b1, _ := os.ReadFile(pa)
	os.WriteFile(pb, b1, 0o644)
	a = openT(t, pa, nil)
	b := openT(t, pb, nil)
	putv(t, b, "k", "copy")
	putv(t, a, "j", "orig")
	if err := b.NewStoreID(bg, 10); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		importT(t, a, must2(exportT(t, b, 0)), NewestWins)
		importT(t, b, must2(exportT(t, a, 0)), NewestWins)
		must[CompactStats](t)(b.Compact(bg, CompactOptions{}))
	}
	sameValues(t, a, b)
	for _, db := range []*DB{a, b} {
		live := indexState(db)
		p := db.Path()
		db.Close()
		s1db := must[*DB](t)(Open(p, nil))
		s1 := indexState(s1db)
		s1db.Close()
		os.Remove(p + snapshotSuffix)
		s2db := must[*DB](t)(Open(p, nil))
		s2 := indexState(s2db)
		s2db.Close()
		if live != s1 || s1 != s2 {
			t.Fatalf("%s: live/snapshot/scan differ\n%s\n%s\n%s", p, live, s1, s2)
		}
	}
}

func TestR4RandomCopiesWithNewID(t *testing.T) {
	for seed := int64(1); seed <= 60; seed++ {
		rng := newRand(seed)
		dir := t.TempDir()
		p0 := filepath.Join(dir, "0")
		d0 := openT(t, p0, &Options{NoSync: true})
		for i := 0; i < 30; i++ {
			putv(t, d0, "k"+string(rune('0'+rng.Intn(4))), string(rune('a'+rng.Intn(3))))
		}
		dbs := []*DB{d0}
		for c := 1; c <= 2; c++ {
			last := must[Stats](t)(dbs[0].Stats()).LastRev
			dbs[0].Close()
			b, _ := os.ReadFile(p0)
			pc := filepath.Join(dir, string(rune('0'+c)))
			os.WriteFile(pc, b, 0o644)
			dbs[0] = openT(t, p0, &Options{NoSync: true})
			dc := openT(t, pc, &Options{NoSync: true})
			if rng.Intn(2) == 0 {
				putv(t, dc, "k0", "pre-id") // a write between copy and new-id
			}
			if err := dc.NewStoreID(bg, last); err != nil {
				t.Fatal(err)
			}
			dbs = append(dbs, dc)
		}
		since := map[[2]int]uint64{}
		pull := func(to, from int) ImportReport {
			d, st := exportT(t, dbs[from], since[[2]int{from, to}])
			since[[2]int{from, to}] = st.Through
			return importT(t, dbs[to], d, NewestWins)
		}
		for step := 0; step < 150; step++ {
			i := rng.Intn(3)
			switch rng.Intn(5) {
			case 0, 1:
				putv(t, dbs[i], "k"+string(rune('0'+rng.Intn(4))), string(rune('a'+rng.Intn(3))))
			case 2, 3:
				j := (i + 1 + rng.Intn(2)) % 3
				pull(i, j)
			case 4:
				must[CompactStats](t)(dbs[i].Compact(bg, CompactOptions{}))
			}
		}
		for r := 0; r < 4; r++ {
			for i := 0; i < 3; i++ {
				for j := 0; j < 3; j++ {
					if i != j {
						pull(i, j)
					}
				}
			}
		}
		sameValues(t, dbs[0], dbs[1])
		sameValues(t, dbs[0], dbs[2])
		putv(t, dbs[2], "k1", "final")
		for r := 0; r < 2; r++ {
			for i := 0; i < 3; i++ {
				for j := 0; j < 3; j++ {
					if i != j {
						if rep := pull(i, j); len(rep.Conflicts) != 0 {
							t.Fatalf("seed %d: conflict after settling %+v", seed, rep)
						}
					}
				}
			}
		}
		for _, db := range dbs {
			checkIndex(t, db)
		}
	}
}

func newRand(seed int64) *mrand.Rand { return mrand.New(mrand.NewSource(seed)) }
