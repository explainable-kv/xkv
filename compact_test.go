// Copyright xkv.org 2026
// SPDX-License-Identifier: Apache-2.0

package xkv

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"
)

// randomWorkload writes n revisions over nkeys keys, in transactions of one
// to four writes, with up to three causes each, picked from every revision
// written so far (old ones included, so compaction has old revisions it must
// keep). About one write in ten is a delete.
func randomWorkload(t testing.TB, db *DB, rng *rand.Rand, n, nkeys int) {
	t.Helper()
	var all []CauseRef
	for written := 0; written < n; {
		size := 1 + rng.Intn(4)
		err := db.Update(WithActor(bg, fmt.Sprintf("actor-%d", rng.Intn(5))), func(tx *Tx) error {
			for i := 0; i < size; i++ {
				key := fmt.Sprintf("key:%03d", rng.Intn(nkeys))
				var opts []WriteOption
				nc := rng.Intn(4)
				for c := 0; c < nc && len(all) > 0; c++ {
					// Favour recent causes but reach back too.
					var pick CauseRef
					if rng.Intn(3) == 0 {
						pick = all[rng.Intn(len(all))]
					} else {
						pick = all[len(all)-1-rng.Intn(min(len(all), 20))]
					}
					opts = append(opts, Cause(pick.Key, pick.Rev))
				}
				var r Revision
				var err error
				if rng.Intn(10) == 0 {
					r, err = tx.Delete(key, opts...)
				} else {
					r, err = tx.Put(key, []byte(fmt.Sprintf("v%d", rng.Int63())), opts...)
				}
				if err != nil {
					return err
				}
				all = append(all, CauseRef{Key: r.Key, Rev: r.Rev})
			}
			return nil
		}, Reason(fmt.Sprintf("reason %d", written)))
		if err != nil {
			t.Fatal(err)
		}
		written += size
	}
}

// stripPrev drops Prev from a tree. Compaction may remove a key's older
// revisions, so Prev can become nil; everything else must stay the same.
func stripPrev(n *WhyNode) {
	n.Prev = nil
	for _, c := range n.Causes {
		stripPrev(c)
	}
}

type state struct {
	get     map[string]string
	why     map[string]string
	history map[string]string
}

func capture(t testing.TB, db *DB, keepLast int, only ...string) state {
	t.Helper()
	s := state{get: map[string]string{}, why: map[string]string{}, history: map[string]string{}}
	keys := only
	if len(keys) == 0 {
		db.mu.RLock()
		for k := range db.keys {
			keys = append(keys, k)
		}
		db.mu.RUnlock()
	}
	for _, k := range keys {
		g, err := db.Get(bg, k)
		if err != nil && !errors.Is(err, ErrNotFound) {
			t.Fatal(err)
		}
		gb, _ := json.Marshal(g)
		s.get[k] = string(gb)
		w, err := db.Why(bg, k, MaxWhyDepth)
		if err != nil {
			t.Fatal(err)
		}
		stripPrev(w)
		wb, _ := json.Marshal(w)
		s.why[k] = string(wb)
		h, err := db.History(bg, k, keepLast)
		if err != nil {
			t.Fatal(err)
		}
		hb, _ := json.Marshal(h)
		s.history[k] = string(hb)
	}
	return s
}

func sameState(t *testing.T, label string, a, b state) {
	t.Helper()
	if len(a.get) != len(b.get) {
		t.Fatalf("%s: %d keys before, %d after", label, len(a.get), len(b.get))
	}
	for k := range a.get {
		if a.get[k] != b.get[k] {
			t.Fatalf("%s: Get(%s) differs\nbefore %s\nafter  %s", label, k, a.get[k], b.get[k])
		}
		if a.why[k] != b.why[k] {
			t.Fatalf("%s: Why(%s) differs\nbefore %s\nafter  %s", label, k, a.why[k], b.why[k])
		}
		if a.history[k] != b.history[k] {
			t.Fatalf("%s: History(%s) differs", label, k)
		}
	}
}

// TestCompactionEquivalence is the compaction promise: on random workloads,
// Get, Why (every live key, full depth) and the kept part of History give
// the same answers before and after compaction, and after reopening.
func TestCompactionEquivalence(t *testing.T) {
	seeds := 20
	if testing.Short() {
		seeds = 4
	}
	totalBefore, totalAfter, totalCause := 0, 0, 0
	for seed := 1; seed <= seeds; seed++ {
		rng := rand.New(rand.NewSource(int64(seed)))
		path := tmpPath(t)
		// No fsync: this test is about what compaction keeps, not about
		// durability, which the crash tests cover.
		fast := &Options{NoSync: true}
		db := openT(t, path, fast)
		randomWorkload(t, db, rng, 1500, 40)
		keepLast := 1 + rng.Intn(3)
		before := capture(t, db, keepLast)
		st, err := db.Compact(bg, CompactOptions{KeepLast: keepLast})
		if err != nil {
			t.Fatal(err)
		}
		if st.RevisionsAfter >= st.RevisionsBefore || st.BytesAfter >= st.BytesBefore {
			t.Fatalf("seed %d: compaction kept everything: %+v", seed, st)
		}
		totalBefore += st.RevisionsBefore
		totalAfter += st.RevisionsAfter
		totalCause += st.KeptForCauses
		sameState(t, fmt.Sprintf("seed %d after compaction", seed), before, capture(t, db, keepLast))

		// The store keeps working: new writes, causes to kept revisions.
		r, err := db.Put(bg, "after", []byte("x"), Reason("after compaction"), CauseLatest("key:000"))
		if err != nil && !errors.Is(err, ErrBadCause) {
			t.Fatal(err)
		}
		if err == nil && r.Rev != uint64(st.RevisionsBefore)+1 {
			t.Fatalf("seed %d: numbering after compaction: %d", seed, r.Rev)
		}
		before = capture(t, db, keepLast)
		db.Close()
		db = openT(t, path, fast)
		sameState(t, fmt.Sprintf("seed %d after reopen", seed), before, capture(t, db, keepLast))
		db.Close()
		os.Remove(path + snapshotSuffix)
		db = openT(t, path, fast)
		sameState(t, fmt.Sprintf("seed %d after full rescan", seed), before, capture(t, db, keepLast))
		db.Close()
		if rep, err := Check(path); err != nil || !rep.OK() {
			t.Fatalf("seed %d: check after compaction: %+v %v", seed, rep, err)
		}
	}
	t.Logf("%d random workloads of 1500 writes: %d revisions before compaction, %d after, %d of them kept only because a kept revision names them as a cause; Get, Why and History matched before and after compaction, after reopening, and after a full rescan",
		seeds, totalBefore, totalAfter, totalCause)
}

func TestCompactKeepsOldCauses(t *testing.T) {
	db := openT(t, tmpPath(t), nil)
	put := must[Revision](t)
	old := put(db.Put(bg, "policy", []byte("v1"), Reason("first policy")))
	put(db.Put(bg, "policy", []byte("v2"), Reason("second policy")))
	put(db.Put(bg, "policy", []byte("v3"), Reason("third policy")))
	put(db.Put(bg, "limit", []byte("10"), Reason("set under v1"), Cause("policy", old.Rev)))
	st := must[CompactStats](t)(db.Compact(bg, CompactOptions{KeepLast: 1}))
	if st.RevisionsAfter != 3 || st.KeptForCauses != 1 {
		t.Fatalf("%+v", st)
	}
	if _, err := db.Revision(bg, 2); !errors.Is(err, ErrCompacted) {
		t.Fatalf("revision 2: %v", err)
	}
	if v := must[Revision](t)(db.Revision(bg, old.Rev)); string(v.Value) != "v1" {
		t.Fatal(v)
	}
	if _, err := db.Put(bg, "x", nil, Reason("r"), Cause("policy", 2)); !errors.Is(err, ErrCompacted) {
		t.Fatalf("cause to a compacted revision: %v", err)
	}
	// History shows what's left, newest first.
	h := must[[]Revision](t)(db.History(bg, "policy", 0))
	if len(h) != 2 || string(h[0].Value) != "v3" || string(h[1].Value) != "v1" {
		t.Fatalf("history %+v", h)
	}
}

func TestCompactKeepSince(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	db := openT(t, tmpPath(t), &Options{Now: func() time.Time { return now }})
	for i := 0; i < 10; i++ {
		now = now.Add(24 * time.Hour)
		must[Revision](t)(db.Put(bg, "k", []byte(fmt.Sprint(i)), Reason("daily")))
	}
	st := must[CompactStats](t)(db.Compact(bg, CompactOptions{KeepSince: now.Add(-72 * time.Hour)}))
	if st.RevisionsAfter != 4 {
		t.Fatalf("kept %d, want the last four days", st.RevisionsAfter)
	}
}

func TestCompactKeepsLatestDelete(t *testing.T) {
	path := tmpPath(t)
	db := openT(t, path, nil)
	must[Revision](t)(db.Put(bg, "k", []byte("1"), Reason("r")))
	must[Revision](t)(db.Delete(bg, "k", Reason("gone")))
	must[CompactStats](t)(db.Compact(bg, CompactOptions{}))
	if _, err := db.Get(bg, "k"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("a compacted-away delete brought the key back: %v", err)
	}
	r := must[Revision](t)(db.Put(bg, "k2", []byte("1"), Reason("r")))
	if r.Rev != 3 || r.Tx != 3 {
		t.Fatalf("numbering went back after compaction: %+v", r)
	}
}

// TestConcurrentUse runs readers, a writer and compactions at the same time.
// Run with -race. Readers must never see an error other than ErrNotFound,
// and every value they read must be one that was written.
func TestConcurrentUse(t *testing.T) {
	db := openT(t, tmpPath(t), &Options{NoSync: true})
	stop := make(chan struct{})
	errs := make(chan error, 16)
	done := make(chan struct{})
	go func() {
		defer close(done)
		var prev Revision
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			var opts []WriteOption
			if prev.Rev > 0 {
				opts = append(opts, Cause(prev.Key, prev.Rev))
			}
			r, err := db.Put(bg, fmt.Sprintf("k%d", i%20), []byte(fmt.Sprintf("v%d", i)), append(opts, Reason("w"))...)
			if err != nil {
				errs <- err
				return
			}
			prev = r
			if i%500 == 499 {
				if _, err := db.Compact(bg, CompactOptions{KeepLast: 2}); err != nil {
					errs <- err
					return
				}
			}
		}
	}()
	readers := make(chan struct{})
	for g := 0; g < 4; g++ {
		go func(g int) {
			defer func() { readers <- struct{}{} }()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				k := fmt.Sprintf("k%d", (i+g)%20)
				if v, err := db.Get(bg, k); err == nil && !strings.HasPrefix(string(v.Value), "v") {
					errs <- fmt.Errorf("read %q", v.Value)
					return
				} else if err != nil && !errors.Is(err, ErrNotFound) {
					errs <- err
					return
				}
				if _, err := db.Why(bg, k, 8); err != nil && !errors.Is(err, ErrNotFound) {
					errs <- err
					return
				}
			}
		}(g)
	}
	time.Sleep(2 * time.Second)
	close(stop)
	<-done
	for g := 0; g < 4; g++ {
		<-readers
	}
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	st, _ := db.Stats()
	t.Logf("2s of one writer, %d compactions and 4 readers at once: no read failed; %d revisions written", st.LastRev/500, st.LastRev)
}
