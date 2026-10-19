package xkv

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
)

// countdownCtx is cancelled after its Err has been asked n times: it cuts an
// import off between transactions, like a killed process would.
type countdownCtx struct {
	context.Context
	n *int
}

func (c countdownCtx) Err() error {
	if *c.n <= 0 {
		return context.Canceled
	}
	*c.n--
	return nil
}

type r3store struct {
	path string
	db   *DB
	opts *Options
}

func r3Values(t testing.TB, db *DB) map[string]string { return valuesOf(t, db) }

// TestR3RandomNetwork: 3-5 stores, puts, deletes, compaction, reopen, stale
// and repeated files, files relayed to stores they weren't meant for, imports
// cut short. Then settle and check everyone agrees, and edits after settling
// arrive with no conflict.
func TestR3RandomNetwork(t *testing.T) {
	seeds := 60
	if v := os.Getenv("R3_SEEDS"); v != "" {
		fmt.Sscan(v, &seeds)
	}
	if testing.Short() {
		seeds = 10
	}
	var stats struct{ conflicts, cut, stale, reopen, compact, dup, spurious, missed int }
	for seed := int64(1); seed <= int64(seeds); seed++ {
		rng := rand.New(rand.NewSource(seed))
		n := 3 + rng.Intn(3)
		st := make([]*r3store, n)
		dir := t.TempDir()
		for i := range st {
			base := time.Date(2026, 10, 19, 9, 0, 0, 0, time.UTC).Add(time.Duration(rng.Intn(5)-2) * 41 * time.Minute)
			k := 0
			// coarse clocks so equal times happen
			mode := int(seed % 3)
			now := func() time.Time {
				k++
				switch mode {
				case 1:
					return time.Date(2026, 10, 19, 9, 0, 0, 0, time.UTC)
				case 2:
					return base.Add(-time.Duration(k) * time.Second)
				}
				return base.Add(time.Duration(k/3) * time.Second)
			}
			p := filepath.Join(dir, fmt.Sprintf("s%d.xkv", i))
			o := &Options{NoSync: true, Now: now}
			db, err := Open(p, o)
			if err != nil {
				t.Fatal(err)
			}
			st[i] = &r3store{p, db, o}
		}
		defer func() {
			for _, s := range st {
				s.db.Close()
			}
		}()
		since := map[[2]int]uint64{}
		var files [][]byte
		reported := map[string]bool{}
		known := map[ident]map[ident]bool{}
		ever := make([]map[string]map[ident]bool, n)
		for i := range ever {
			ever[i] = map[string]map[ident]bool{}
		}
		refresh := func(i int) {
			db := st[i].db
			db.mu.RLock()
			defer db.mu.RUnlock()
			for _, m := range db.revs {
				id := db.identOf(m)
				if ever[i][m.key] == nil {
					ever[i][m.key] = map[ident]bool{}
				}
				ever[i][m.key][id] = true
			}
		}
		noteWrite := func(i int, key string, rev uint64) {
			db := st[i].db
			sid, _ := db.StoreID()
			id := ident{sid, rev}
			k := map[ident]bool{id: true}
			for h := range ever[i][key] {
				for x := range known[h] {
					k[x] = true
				}
			}
			known[id] = k
			refresh(i)
		}
		imp := func(to int, d []byte, ctx context.Context) (ImportReport, error) {
			return st[to].db.Import(ctx, bytes.NewReader(d), ImportOptions{})
		}
		pull := func(to, from int) ImportReport {
			d, es := exportT(t, st[from].db, since[[2]int{from, to}])
			files = append(files, d)
			r, err := imp(to, d, bg)
			if err != nil {
				t.Fatalf("seed %d: import %d<-%d: %v", seed, to, from, err)
			}
			since[[2]int{from, to}] = es.Through
			stats.conflicts += len(r.Conflicts)
			db := st[to].db
			for _, c := range r.Conflicts {
				db.mu.RLock()
				m, _ := db.findRev(c.Theirs)
				id := db.identOf(m)
				mo, _ := db.findRev(c.Ours)
				oid := db.identOf(mo)
				db.mu.RUnlock()
				if known[id][oid] || known[oid][id] {
					stats.spurious++
					if stats.spurious < 4 {
						t.Logf("seed %d: store %d: conflict %s vs %s, but one knew the other", seed, to, oid, id)
					}
				}
				key := fmt.Sprint(to, id)
				if reported[key] {
					stats.dup++
					if stats.dup < 4 {
						t.Logf("seed %d: store %d reported %s as a conflict again", seed, to, id)
					}
				}
				reported[key] = true
			}
			refresh(to)
			// every revision held is known by the value, or loses to it
			db.mu.RLock()
			for name, ks := range db.keys {
				cm, _ := db.findRev(ks.latest())
				cr, _ := db.readRec(cm)
				cid := db.identOf(cm)
				for _, rev := range db.allRevs(name) {
					hm, _ := db.findRev(rev)
					hid := db.identOf(hm)
					if hid == cid || known[cid][hid] {
						continue
					}
					hr, _ := db.readRec(hm)
					if !newer(cr, hr) && !sameValue(cr, hr) {
						stats.missed++
						if stats.missed < 4 {
							t.Logf("seed %d: store %d key %s: value %s neither knows nor beats %s", seed, to, name, cid, hid)
						}
					}
				}
			}
			db.mu.RUnlock()
			return r
		}
		for step := 0; step < 250; step++ {
			i := rng.Intn(n)
			db := st[i].db
			switch x := rng.Intn(20); {
			case x < 8:
				key := fmt.Sprintf("k%d", rng.Intn(5))
				opts := []WriteOption{Reason("r"), Actor(fmt.Sprint("s", i))}
				if rng.Intn(3) == 0 {
					opts = append(opts, CauseLatest(fmt.Sprintf("k%d", rng.Intn(5))))
				}
				var err error
				var rv Revision
				refresh(i)
				if rng.Intn(5) == 0 {
					rv, err = db.Delete(bg, key, opts...)
				} else {
					rv, err = db.Put(bg, key, []byte(fmt.Sprint(rng.Intn(4))), opts...)
				}
				if err == nil {
					noteWrite(i, key, rv.Rev)
				}
				if err != nil && !errors.Is(err, ErrBadCause) {
					t.Fatal(err)
				}
			case x < 15:
				j := rng.Intn(n)
				if j != i {
					pull(i, j)
				}
			case x < 16:
				// a stale or misdirected file
				if len(files) > 0 {
					stats.stale++
					if _, err := imp(i, files[rng.Intn(len(files))], bg); err != nil {
						t.Fatalf("seed %d: old file into %d: %v", seed, i, err)
					}
					refresh(i)
				}
			case x < 17:
				// an import cut short; since isn't advanced
				j := rng.Intn(n)
				if j != i {
					d, _ := exportT(t, st[j].db, since[[2]int{j, i}])
					c := rng.Intn(4)
					_, err := imp(i, d, countdownCtx{bg, &c})
					refresh(i)
					if err != nil && !errors.Is(err, context.Canceled) {
						t.Fatalf("seed %d: cut import: %v", seed, err)
					}
					stats.cut++
				}
			case x < 18:
				stats.compact++
				if _, err := db.Compact(bg, CompactOptions{KeepLast: 1 + rng.Intn(2)}); err != nil {
					t.Fatal(err)
				}
				checkIndex(t, db)
			default:
				stats.reopen++
				db.Close()
				if rng.Intn(2) == 0 {
					os.Remove(st[i].path + snapshotSuffix)
				}
				ndb, err := Open(st[i].path, st[i].opts)
				if err != nil {
					t.Fatal(err)
				}
				st[i].db = ndb
			}
		}
		// settle with a full mesh
		settled := false
		for round := 0; round < 3*n && !settled; round++ {
			changed := 0
			for a := 0; a < n; a++ {
				for b := 0; b < n; b++ {
					if a != b {
						r := pull(a, b)
						changed += r.Imported + r.Agreed + len(r.Conflicts)
					}
				}
			}
			settled = changed == 0
		}
		if !settled {
			t.Fatalf("seed %d: not settled", seed)
		}
		v0 := r3Values(t, st[0].db)
		for i := 1; i < n; i++ {
			vi := r3Values(t, st[i].db)
			if !reflect.DeepEqual(v0, vi) {
				t.Fatalf("seed %d: store 0 %v, store %d %v", seed, v0, i, vi)
			}
		}
		for _, s := range st {
			checkIndex(t, s.db)
		}
		// edits after settling
		for e := 0; e < 4; e++ {
			i := rng.Intn(n)
			key := fmt.Sprintf("k%d", rng.Intn(5))
			val := fmt.Sprintf("final-%d", e)
			putv(t, st[i].db, key, val)
			for round := 0; round < 2; round++ {
				for a := 0; a < n; a++ {
					for b := 0; b < n; b++ {
						if a != b {
							if r := pull(a, b); len(r.Conflicts) != 0 {
								t.Fatalf("seed %d: edit after settling conflicted: %+v", seed, r)
							}
						}
					}
				}
			}
			for j := range st {
				if v := valueOf(t, st[j].db, key); v != val {
					t.Fatalf("seed %d: store %d %s=%s want %s", seed, j, key, v, val)
				}
			}
		}
	}
	t.Logf("%+v", stats)
}

// indexState captures everything the index holds, for comparing a store with
// itself reopened.
func indexState(db *DB) string {
	db.mu.RLock()
	defer db.mu.RUnlock()
	var b bytes.Buffer
	fmt.Fprintf(&b, "last %d %d end %d\n", db.lastRev, db.lastTx, db.end)
	for _, m := range db.revs {
		o := m
		o.origin = nil
		fmt.Fprintf(&b, "%+v %v\n", o, func() string {
			if m.origin == nil {
				return ""
			}
			return m.origin.String()
		}())
	}
	var names []string
	for k := range db.keys {
		names = append(names, k)
	}
	sort.Strings(names)
	for _, k := range names {
		fmt.Fprintf(&b, "key %s %v own %d\n", k, db.keys[k].revs, db.keys[k].own)
	}
	names = names[:0]
	for k := range db.keySeen {
		names = append(names, k)
	}
	sort.Strings(names)
	for _, k := range names {
		l := append([]seenEntry(nil), db.keySeen[k]...)
		sort.Slice(l, func(i, j int) bool { return bytes.Compare(l[i].store[:], l[j].store[:]) < 0 })
		fmt.Fprintf(&b, "seen %s %v\n", k, l)
	}
	names = names[:0]
	for k := range db.keyTime {
		names = append(names, k)
	}
	sort.Strings(names)
	for _, k := range names {
		fmt.Fprintf(&b, "time %s %d\n", k, db.keyTime[k])
	}
	names = names[:0]
	for k := range db.hist {
		names = append(names, k)
	}
	sort.Strings(names)
	for _, k := range names {
		fmt.Fprintf(&b, "hist %s %v\n", k, db.hist[k])
	}
	return b.String()
}

// TestR3ReopenEquivalence: after random exchange and compaction, the index
// read from the snapshot, read by a full scan, and the live one agree.
func TestR3ReopenEquivalence(t *testing.T) {
	for seed := int64(1); seed <= 20; seed++ {
		rng := rand.New(rand.NewSource(seed))
		dir := t.TempDir()
		var dbs [3]*DB
		for i := range dbs {
			dbs[i] = must[*DB](t)(Open(filepath.Join(dir, fmt.Sprint(i)), &Options{NoSync: true, Now: clock()}))
		}
		for step := 0; step < 150; step++ {
			i := rng.Intn(3)
			switch rng.Intn(6) {
			case 0, 1, 2:
				putv(t, dbs[i], fmt.Sprintf("k%d", rng.Intn(4)), fmt.Sprint(rng.Intn(3)))
			case 3, 4:
				j := (i + 1 + rng.Intn(2)) % 3
				importT(t, dbs[i], must2(exportT(t, dbs[j], 0)), NewestWins)
			case 5:
				must[CompactStats](t)(dbs[i].Compact(bg, CompactOptions{KeepLast: 1}))
			}
		}
		for i, db := range dbs {
			live := indexState(db)
			p := db.Path()
			db.Close()
			viaSnap := must[*DB](t)(Open(p, &Options{NoSync: true}))
			s1 := indexState(viaSnap)
			viaSnap.Close()
			os.Remove(p + snapshotSuffix)
			viaScan := must[*DB](t)(Open(p, &Options{NoSync: true}))
			s2 := indexState(viaScan)
			viaScan.Close()
			if live != s1 || s1 != s2 {
				t.Fatalf("seed %d store %d: live/snapshot/scan differ:\nLIVE\n%s\nSNAP\n%s\nSCAN\n%s", seed, i, live, s1, s2)
			}
		}
	}
}

func localOf(t testing.TB, db *DB, key string) uint64 {
	t.Helper()
	return must[Revision](t)(db.Get(bg, key)).Rev
}

// R3-1: compaction's "newest per store" goes by local order, not by the
// origin's number. A store that got S:2 first and S:1 later (as a cause from
// a relay) keeps S:1 and drops S:2; the seen list forgets S:2, and the old
// file conflicts again.
func TestR3CompactionKeepsWrongNewest(t *testing.T) {
	for _, p := range []ConflictPolicy{NewestWins, TakeTheirs} {
		t.Run(p.String(), func(t *testing.T) {
			cs := clock()
			s := openT(t, tmpPath(t), &Options{Now: cs})
			late := clock()
			e := openT(t, tmpPath(t), &Options{Now: func() time.Time { return late().Add(time.Hour) }})
			d := openT(t, tmpPath(t), nil)
			a := openT(t, tmpPath(t), nil)
			putv(t, s, "k", "v1") // S:1
			putv(t, s, "k", "v2") // S:2
			putv(t, e, "k", "e")  // concurrent, newer
			importT(t, d, must2(exportT(t, s, 0)), NewestWins)
			// D: j caused by k@S:1 (its local 1)
			must[Revision](t)(d.Put(bg, "j", []byte("x"), Reason("r"), Actor("d"), Cause("k", 1)))
			full := must2(exportT(t, s, 0))
			importT(t, a, must2(exportT(t, s, 1)), p) // only S:2
			importT(t, a, must2(exportT(t, e, 0)), p) // conflict with e
			importT(t, a, must2(exportT(t, d, 0)), p) // S:1 arrives as history
			before := valueOf(t, a, "k")
			// reimport before compaction: nothing
			if r := importT(t, a, full, p); len(r.Conflicts) != 0 {
				t.Fatalf("before compaction: %+v", r)
			}
			must[CompactStats](t)(a.Compact(bg, CompactOptions{}))
			r := importT(t, a, full, p)
			after := valueOf(t, a, "k")
			if len(r.Conflicts) != 0 || r.Imported+r.Context != 0 || before != after {
				t.Fatalf("reimport after compaction: k %s -> %s, report %+v", before, after, r)
			}
		})
	}
}

// R3-2: a time at the top of the range would leave a later write no room to
// be later than what it knew. An import refuses times more than a year past
// the importer's clock, and a local write that can't be later is refused.
func TestR3MaxTimeBreaksConvergence(t *testing.T) {
	top := time.Unix(0, 1<<63-1)
	s := openT(t, tmpPath(t), &Options{Now: func() time.Time { return top }})
	base := time.Date(2026, 10, 19, 9, 0, 0, 0, time.UTC)
	a := openT(t, tmpPath(t), &Options{Now: func() time.Time { return base }})
	putv(t, s, "k", "x")
	if _, err := a.Import(bg, bytes.NewReader(must2(exportT(t, s, 0))), ImportOptions{}); !errors.Is(err, ErrBadExchange) {
		t.Fatalf("a file dated 2262: %v", err)
	}
	if _, err := s.Put(bg, "k", []byte("y"), Reason("r"), Actor("x")); !errors.Is(err, ErrInvalid) {
		t.Fatalf("a write with no later time left: %v", err)
	}
	// Up to a year ahead is accepted, and a write after it is still later.
	ahead := openT(t, tmpPath(t), &Options{Now: func() time.Time { return base.Add(300 * 24 * time.Hour) }})
	putv(t, ahead, "k", "x")
	importT(t, a, must2(exportT(t, ahead, 0)), NewestWins)
	if w := putv(t, a, "k", "a-edit"); !w.Time.After(base.Add(300 * 24 * time.Hour)) {
		t.Fatalf("a write after a change from a clock ahead: %v", w.Time)
	}
}

// R3-3: a file can claim, in a seen list, more of a third store C than C
// wrote. Importers can't check that (exchange.md says files must come from
// stores you run), but the claim doesn't pass C quietly: C refuses any file
// that carries it, saying what it found.
func TestR3SeenClaimPoisons(t *testing.T) {
	b := openT(t, tmpPath(t), nil)
	a := openT(t, tmpPath(t), nil)
	c := openT(t, tmpPath(t), nil)
	cid, _ := c.StoreID()
	putv(t, b, "k", "b")
	d := must2(exportT(t, b, 0))
	lines := strings.Split(strings.TrimSuffix(string(d), "\n"), "\n")
	lines = lines[:len(lines)-1]
	lines[1] = strings.Replace(lines[1], `}]}`, `,"seen":{"`+cid.String()+`":1000000}}]}`, 1)
	if _, err := a.Import(bg, bytes.NewReader(reseal(lines)), ImportOptions{}); err != nil {
		t.Fatalf("crafted file refused: %v", err)
	}
	putv(t, a, "k", "a-edit")
	_, err := c.Import(bg, bytes.NewReader(must2(exportT(t, a, 0))), ImportOptions{})
	if !errors.Is(err, ErrBadExchange) || !strings.Contains(err.Error(), "1000000") {
		t.Fatalf("c imports a file carrying the claim: %v", err)
	}
}

// R3-5: seen lists of 4094 other stores, the most an import accepts for a key: what does an
// import, and then a reopen without a snapshot, cost?
func TestR3BigSeenListsCost(t *testing.T) {
	n := 200
	rng := rand.New(rand.NewSource(1))
	stores := make([]string, 4094)
	for i := range stores {
		var id StoreID
		rng.Read(id[:])
		stores[i] = id.String()
	}
	sort.Strings(stores)
	var seenJSON strings.Builder
	seenJSON.WriteString(`{`)
	for i, s := range stores {
		if i > 0 {
			seenJSON.WriteString(",")
		}
		fmt.Fprintf(&seenJSON, `"%s":%d`, s, 1+i)
	}
	seenJSON.WriteString(`}`)
	src := strings.Repeat("a", 32)
	lines := []string{`{"xkv_exchange":1,"store":"` + src + `","since":0,"through":` + fmt.Sprint(n) + `,"created":"2026-10-19T09:00:00Z"}`}
	for i := 1; i <= n; i++ {
		lines = append(lines, fmt.Sprintf(`{"tx":[{"id":"%s:%d","key":"k","value":"v%d","actor":"a","reason":"r","time":"2026-10-19T09:00:%02dZ","seen":%s}]}`, src, i, i, i%60, seenJSON.String()))
	}
	data := reseal(lines)
	p := tmpPath(t)
	db := openT(t, p, &Options{NoSync: true})
	start := time.Now()
	rep, err := db.Import(bg, bytes.NewReader(data), ImportOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("file %d MB, import %v: %+v", len(data)>>20, time.Since(start), rep.Imported)
	start = time.Now()
	putv(t, db, "k", "local")
	t.Logf("one local write after: %v, file now %d bytes", time.Since(start), must[Stats](t)(db.Stats()).FileSize)
	db.Close()
	os.Remove(p + snapshotSuffix)
	start = time.Now()
	db2 := openT(t, p, &Options{NoSync: true})
	t.Logf("reopen by scan: %v", time.Since(start))
	_ = db2
}

// R3-6: imports, exports, local writes, compaction, reads and NewStoreID all
// at once on one store, under -race.
func TestR3Concurrent(t *testing.T) {
	a := openT(t, tmpPath(t), &Options{NoSync: true})
	b := openT(t, tmpPath(t), &Options{NoSync: true})
	for i := 0; i < 50; i++ {
		putv(t, b, fmt.Sprintf("k%d", i%7), fmt.Sprint(i))
	}
	done := make(chan struct{})
	errs := make(chan error, 16)
	run := func(f func(i int) error) {
		go func() {
			for i := 0; ; i++ {
				select {
				case <-done:
					errs <- nil
					return
				default:
				}
				if err := f(i); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	run(func(i int) error {
		_, err := a.Put(bg, fmt.Sprintf("k%d", i%7), []byte(fmt.Sprint(i)), Reason("r"), Actor("a"))
		return err
	})
	run(func(i int) error {
		_, err := b.Put(bg, fmt.Sprintf("k%d", i%7), []byte(fmt.Sprint(i)), Reason("r"), Actor("b"))
		return err
	})
	run(func(i int) error {
		var buf bytes.Buffer
		if _, err := b.Export(bg, &buf, ExportOptions{Since: uint64(i % 30)}); err != nil {
			return err
		}
		_, err := a.Import(bg, &buf, ImportOptions{})
		return err
	})
	run(func(i int) error {
		var buf bytes.Buffer
		if _, err := a.Export(bg, &buf, ExportOptions{}); err != nil {
			return err
		}
		_, err := b.Import(bg, &buf, ImportOptions{})
		return err
	})
	run(func(i int) error {
		_, err := a.Compact(bg, CompactOptions{KeepLast: 1 + i%3})
		return err
	})
	run(func(i int) error {
		_, err := a.Why(bg, fmt.Sprintf("k%d", i%7), 8)
		if errors.Is(err, ErrNotFound) {
			return nil
		}
		_, err2 := a.History(bg, fmt.Sprintf("k%d", i%7), 0)
		if err == nil {
			err = err2
		}
		return err
	})
	time.Sleep(3 * time.Second)
	close(done)
	for i := 0; i < 6; i++ {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	checkIndex(t, a)
	checkIndex(t, b)
}

// R3-7: maxRecord doesn't allow for a seen list. A key whose seen list is
// long (4096 stores, as a file may claim) and a value near MaxValueSize make
// a record longer than maxRecord: written and acknowledged, but unreadable.
func TestR3SeenListOverflowsMaxRecord(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	stores := make([]string, 4094)
	for i := range stores {
		var id StoreID
		rng.Read(id[:])
		stores[i] = id.String()
	}
	sort.Strings(stores)
	var sj strings.Builder
	sj.WriteString(`{`)
	for i, s := range stores {
		if i > 0 {
			sj.WriteString(",")
		}
		fmt.Fprintf(&sj, `"%s":%d`, s, uint64(1)<<40+uint64(i))
	}
	sj.WriteString(`}`)
	src := strings.Repeat("a", 32)
	lines := []string{`{"xkv_exchange":1,"store":"` + src + `","since":0,"through":1,"created":"2026-10-19T09:00:00Z"}`,
		fmt.Sprintf(`{"tx":[{"id":"%s:1","key":"k","value":"v","actor":"a","reason":"r","time":"2026-10-19T09:00:00Z","seen":%s}]}`, src, sj.String())}
	p := tmpPath(t)
	db := openT(t, p, &Options{NoSync: true})
	if _, err := db.Import(bg, bytes.NewReader(reseal(lines)), ImportOptions{}); err != nil {
		t.Fatal(err)
	}
	big := bytes.Repeat([]byte("x"), MaxValueSize)
	r, err := db.Put(bg, "k", big, Reason("big"), Actor("a"))
	t.Logf("put of a max value: rev %d err %v", r.Rev, err)
	if err != nil {
		return
	}
	putv(t, db, "other", "after")
	if _, err := db.Get(bg, "k"); err != nil {
		t.Errorf("get right after the write: %v", err)
	}
	db.Close()
	os.Remove(p + snapshotSuffix)
	db2, err := Open(p, &Options{NoSync: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()
	g, err := db2.Get(bg, "k")
	t.Logf("after reopen: len %d err %v", len(g.Value), err)
	if err != nil || len(g.Value) != MaxValueSize {
		t.Fatalf("acknowledged write lost")
	}
}

// R3-8: after new-id, the copy's writes made between the copy and new-id
// lose what they knew of the shared line: their seen lists left out the old
// id as their own store. The original sees an ordinary update as a conflict,
// and KeepOurs then keeps the stale value.
func TestR3NewIDForgetsSharedLine(t *testing.T) {
	for _, p := range []ConflictPolicy{NewestWins, KeepOurs} {
		dir := t.TempDir()
		pa, pb := filepath.Join(dir, "a.xkv"), filepath.Join(dir, "b.xkv")
		a := openT(t, pa, nil)
		putv(t, a, "k", "base")
		a.Close()
		b1, _ := os.ReadFile(pa)
		os.WriteFile(pb, b1, 0o644)
		a = openT(t, pa, nil)
		b := openT(t, pb, nil)
		putv(t, b, "k", "copy-edit") // written knowing "base"
		if err := b.NewStoreID(bg, 1); err != nil {
			t.Fatal(err)
		}
		rep := importT(t, a, must2(exportT(t, b, 0)), p)
		t.Logf("%v: %+v, k=%s", p, rep, valueOf(t, a, "k"))
		if len(rep.Conflicts) != 0 || valueOf(t, a, "k") != "copy-edit" {
			t.Errorf("%v: an edit made knowing the shared value came in as a conflict", p)
		}
	}
}

// A key may have seen at most maxSeen-1 other stores, so a local write's
// seen list always fits the format: a file that would pass that is refused.
func TestImportRefusesTooManyStoresForAKey(t *testing.T) {
	var sj strings.Builder
	sj.WriteString(`{`)
	ids := make([]string, maxSeen)
	for i := range ids {
		var id StoreID
		id[0], id[1], id[15] = byte(i>>8), byte(i), 1
		ids[i] = id.String()
	}
	sort.Strings(ids)
	for i, s := range ids {
		if i > 0 {
			sj.WriteString(",")
		}
		fmt.Fprintf(&sj, `"%s":%d`, s, i+1)
	}
	sj.WriteString(`}`)
	src := strings.Repeat("a", 32)
	lines := []string{`{"xkv_exchange":1,"store":"` + src + `","since":0,"through":1,"created":"2026-10-19T09:00:00Z"}`,
		fmt.Sprintf(`{"tx":[{"id":"%s:1","key":"k","value":"v","actor":"a","reason":"r","time":"2026-10-19T09:00:00Z","seen":%s}]}`, src, sj.String())}
	db := openT(t, tmpPath(t), nil)
	if _, err := db.Import(bg, bytes.NewReader(reseal(lines)), ImportOptions{}); !errors.Is(err, ErrBadExchange) {
		t.Fatalf("a key seeing %d stores: %v", maxSeen+1, err)
	}
	if s := must[Stats](t)(db.Stats()); s.Revisions != 0 {
		t.Fatalf("something was written: %+v", s)
	}
}
