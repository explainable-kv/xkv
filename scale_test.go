// Copyright xkv.org 2026
// SPDX-License-Identifier: Apache-2.0

package xkv

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"testing"
	"time"
)

// TestLongHistory builds a store with a long history and times the
// operations on it. It writes 100,000 revisions by default and 1,000,000
// with XKV_LONG=1, in transactions of 100 with an fsync each, over 10,000
// keys. Half the writes name the previous write as a cause, and one key is
// rewritten every transaction with its previous revision as cause, so it
// has a chain as deep as the history is long.
func TestLongHistory(t *testing.T) {
	if testing.Short() {
		t.Skip("long")
	}
	n := 100_000
	if os.Getenv("XKV_LONG") == "1" {
		n = 1_000_000
	}
	if v, err := strconv.Atoi(os.Getenv("XKV_LONG_REVS")); err == nil {
		n = v
	}
	path := tmpPath(t)
	db := openT(t, path, nil)
	rng := rand.New(rand.NewSource(11))
	value := make([]byte, 64)
	var prev Revision
	t0 := time.Now()
	for written := 0; written < n; {
		err := db.Update(WithActor(bg, "loader"), func(tx *Tx) error {
			for i := 0; i < 99; i++ {
				rng.Read(value)
				var opts []WriteOption
				if prev.Rev > 0 && rng.Intn(2) == 0 {
					opts = append(opts, Cause(prev.Key, prev.Rev))
				}
				r, err := tx.Put(fmt.Sprintf("key:%05d", rng.Intn(10000)), value, opts...)
				if err != nil {
					return err
				}
				prev = r
			}
			_, err := tx.Put("chain", []byte(strconv.Itoa(written)), CauseLatest("chain"), Cause(prev.Key, prev.Rev))
			if err != nil && written == 0 {
				_, err = tx.Put("chain", []byte("0"), Cause(prev.Key, prev.Rev))
			}
			return err
		}, Reason("bulk load"))
		if err != nil {
			t.Fatal(err)
		}
		written += 100
	}
	writeTime := time.Since(t0)
	st, _ := db.Stats()

	timeOp := func(count int, f func(i int)) time.Duration {
		t0 := time.Now()
		for i := 0; i < count; i++ {
			f(i)
		}
		return time.Since(t0) / time.Duration(count)
	}
	get := timeOp(10000, func(i int) { db.Get(bg, fmt.Sprintf("key:%05d", rng.Intn(10000))) })
	hist := timeOp(2000, func(i int) { db.History(bg, fmt.Sprintf("key:%05d", rng.Intn(10000)), 20) })
	var depth16 *WhyNode
	why16 := timeOp(500, func(i int) { depth16, _ = db.Why(bg, "chain", 16) })
	why256 := timeOp(50, func(i int) { db.Why(bg, "chain", 256) })
	nodes := 0
	var count func(*WhyNode)
	count = func(w *WhyNode) {
		nodes++
		for _, c := range w.Causes {
			count(c)
		}
	}
	count(depth16)
	db.Close()

	runtime.GC()
	var m0, m1 runtime.MemStats
	runtime.ReadMemStats(&m0)
	t0 = time.Now()
	db = openT(t, path, nil)
	openSnap := time.Since(t0)
	runtime.GC()
	runtime.ReadMemStats(&m1)
	indexBytes := float64(m1.HeapAlloc) - float64(m0.HeapAlloc)
	db.Close()
	os.Remove(path + snapshotSuffix)
	t0 = time.Now()
	db = openT(t, path, nil)
	openScan := time.Since(t0)

	t0 = time.Now()
	cs, err := db.Compact(bg, CompactOptions{KeepLast: 1})
	if err != nil {
		t.Fatal(err)
	}
	compactTime := time.Since(t0)

	t.Logf("%d revisions over %d keys in %d transactions (100 writes each, fsync per transaction): written in %s (%.0f revisions/s), file %.1f MB",
		st.Revisions, st.Keys, st.LastTx, writeTime.Round(time.Millisecond), float64(st.Revisions)/writeTime.Seconds(), float64(st.FileSize)/1e6)
	t.Logf("Get: %s on average; History of 20: %s; Why to depth 16 on the deep chain (%d nodes): %s; Why to depth 256: %s",
		get, hist, nodes, why16, why256)
	t.Logf("open with the index snapshot: %s; open with a full scan: %s; the open store's index takes %.0f MB of memory, %.0f bytes per revision",
		openSnap.Round(time.Millisecond), openScan.Round(time.Millisecond), indexBytes/1e6, indexBytes/float64(st.Revisions))
	t.Logf("compaction keeping each key's latest: %s, %d revisions -> %d (%d kept for causes), %.1f MB -> %.1f MB",
		compactTime.Round(time.Millisecond), cs.RevisionsBefore, cs.RevisionsAfter, cs.KeptForCauses, float64(cs.BytesBefore)/1e6, float64(cs.BytesAfter)/1e6)
}

func benchDB(b *testing.B, opts *Options) *DB {
	db, err := Open(b.TempDir()+"/b.xkv", opts)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { db.Close() })
	return db
}

// BenchmarkPut is one write per transaction, with its fsync.
func BenchmarkPut(b *testing.B) {
	db := benchDB(b, nil)
	v := make([]byte, 64)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Put(bg, "k"+strconv.Itoa(i%1000), v, Reason("bench")); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPutNoSync is one write per transaction without the fsync.
func BenchmarkPutNoSync(b *testing.B) {
	db := benchDB(b, &Options{NoSync: true})
	v := make([]byte, 64)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Put(bg, "k"+strconv.Itoa(i%1000), v, Reason("bench")); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPutWithCause is BenchmarkPutNoSync with one cause per write.
func BenchmarkPutWithCause(b *testing.B) {
	db := benchDB(b, &Options{NoSync: true})
	v := make([]byte, 64)
	prev, _ := db.Put(bg, "seed", v, Reason("bench"))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r, err := db.Put(bg, "k"+strconv.Itoa(i%1000), v, Reason("bench"), Cause(prev.Key, prev.Rev))
		if err != nil {
			b.Fatal(err)
		}
		prev = r
	}
}

// BenchmarkTx100 is 100 writes per transaction, with one fsync; the time is
// per transaction.
func BenchmarkTx100(b *testing.B) {
	db := benchDB(b, nil)
	v := make([]byte, 64)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := db.Update(bg, func(tx *Tx) error {
			for j := 0; j < 100; j++ {
				if _, err := tx.Put("k"+strconv.Itoa(j), v); err != nil {
					return err
				}
			}
			return nil
		}, Reason("bench"))
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGet(b *testing.B) {
	db := benchDB(b, &Options{NoSync: true})
	v := make([]byte, 64)
	for i := 0; i < 1000; i++ {
		db.Put(bg, "k"+strconv.Itoa(i), v, Reason("bench"))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Get(bg, "k"+strconv.Itoa(i%1000)); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkWhy walks a chain of 16 revisions across 16 keys.
func BenchmarkWhy(b *testing.B) {
	db := benchDB(b, &Options{NoSync: true})
	r, _ := db.Put(bg, "k0", []byte("v"), Reason("start"))
	for i := 1; i < 16; i++ {
		r, _ = db.Put(bg, "k"+strconv.Itoa(i), []byte("v"), Reason("step"), Cause(r.Key, r.Rev))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Why(bg, "k15", 0); err != nil {
			b.Fatal(err)
		}
	}
}

// TestExchangeScale exports 100,000 revisions in 1,000 transactions and
// imports them into an empty store, timing both.
func TestExchangeScale(t *testing.T) {
	if testing.Short() {
		t.Skip("long")
	}
	n := 100_000
	src := openT(t, tmpPath(t), &Options{NoSync: true})
	rng := rand.New(rand.NewSource(12))
	value := make([]byte, 32)
	var prev Revision
	for written := 0; written < n; written += 100 {
		err := src.Update(WithActor(bg, "loader"), func(tx *Tx) error {
			for i := 0; i < 100; i++ {
				rng.Read(value)
				var opts []WriteOption
				if prev.Rev > 0 && rng.Intn(2) == 0 {
					opts = append(opts, Cause(prev.Key, prev.Rev))
				}
				r, err := tx.Put(fmt.Sprintf("key:%05d", rng.Intn(10000)), value, opts...)
				if err != nil {
					return err
				}
				prev = r
			}
			return nil
		}, Reason("bulk load"))
		if err != nil {
			t.Fatal(err)
		}
	}
	var buf bytes.Buffer
	t0 := time.Now()
	st, err := src.Export(bg, &buf, ExportOptions{})
	if err != nil {
		t.Fatal(err)
	}
	exp := time.Since(t0)
	dst := openT(t, tmpPath(t), nil)
	t0 = time.Now()
	rep, err := dst.Import(bg, bytes.NewReader(buf.Bytes()), ImportOptions{})
	if err != nil {
		t.Fatal(err)
	}
	imp := time.Since(t0)
	if rep.Imported != n || st.Revisions != n {
		t.Fatalf("%+v %+v", st, rep)
	}
	t0 = time.Now()
	again, err := dst.Import(bg, bytes.NewReader(buf.Bytes()), ImportOptions{})
	if err != nil || again.Skipped != n {
		t.Fatalf("%v %+v", err, again)
	}
	re := time.Since(t0)
	t.Logf("exchange of %d revisions in %d transactions (binary values, half with a cause): export %s into a %.1f MB file; import %s with an fsync per transaction; importing the same file again %s, all skipped",
		n, n/100, exp.Round(time.Millisecond), float64(buf.Len())/1e6, imp.Round(time.Millisecond), re.Round(time.Millisecond))
}
