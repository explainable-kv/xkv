// Reviewer's tests for v0.3.0 Watch. Not part of the repo.

package xkv

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// R1: cancelling ctx (or Close) inside fn does not stop delivery of the rest
// of the batch already read.
func TestReviewCancelMidBatch(t *testing.T) {
	db := openT(t, tmpPath(t), nil)
	for i := 0; i < 10; i++ {
		must[Revision](t)(db.Put(bg, fmt.Sprint("k", i), []byte("v"), Reason("r"), Actor("x")))
	}
	ctx, cancel := context.WithCancel(bg)
	n := 0
	err := db.Watch(ctx, WatchOptions{}, func(Revision) error {
		n++
		cancel()
		return nil
	})
	t.Logf("err=%v calls after cancel=%d", err, n-1)
	if n != 1 {
		t.Errorf("fn called %d times after ctx was cancelled in the first call", n-1)
	}
	// Close inside fn
	db2 := openT(t, tmpPath(t), nil)
	for i := 0; i < 10; i++ {
		must[Revision](t)(db2.Put(bg, fmt.Sprint("k", i), []byte("v"), Reason("r"), Actor("x")))
	}
	m := 0
	err = db2.Watch(bg, WatchOptions{}, func(Revision) error {
		m++
		db2.Close()
		return nil
	})
	if m != 1 {
		t.Errorf("fn called %d times after Close inside fn (err %v)", m-1, err)
	}
}

// R2: a read-only handle opened on an empty (just created) file can't follow
// the writer that then writes the header.
func TestReviewFollowFromEmptyFile(t *testing.T) {
	p := tmpPath(t)
	if err := os.WriteFile(p, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	r := openT(t, p, &Options{ReadOnly: true})
	w := openT(t, p, nil)
	must[Revision](t)(w.Put(bg, "k", []byte("v"), Reason("r"), Actor("x")))
	if err := r.Refresh(); err != nil {
		t.Fatalf("Refresh after the writer created the store: %v", err)
	}
	st := must[Stats](t)(r.Stats())
	if st.Revisions != 1 || st.Format == 0 {
		t.Fatalf("follower sees %+v", st)
	}
}

// R3: the file is replaced by a different store (deleted and recreated, or
// a backup restored). The follower silently stops seeing writes.
func TestReviewFollowReplacedStore(t *testing.T) {
	skipReplacingOpenFile(t)
	p := tmpPath(t)
	w := openT(t, p, nil)
	for i := 0; i < 10; i++ {
		must[Revision](t)(w.Put(bg, "k", []byte(fmt.Sprint(i)), Reason("r"), Actor("x")))
	}
	r := openT(t, p, &Options{ReadOnly: true})
	c := watchT(t, r, WatchOptions{Since: 10, Poll: 5 * time.Millisecond})
	w.Close()
	os.Remove(p)
	os.Remove(p + snapshotSuffix)
	w2 := openT(t, p, nil)
	for i := 0; i < 5; i++ {
		must[Revision](t)(w2.Put(bg, "new", []byte(fmt.Sprint(i)), Reason("r"), Actor("x")))
	}
	time.Sleep(200 * time.Millisecond)
	if err := r.Refresh(); err != nil {
		t.Logf("Refresh: %v", err)
	}
	select {
	case err := <-c.done:
		t.Logf("watch ended: %v", err)
		return
	default:
	}
	c.mu.Lock()
	got := len(c.revs)
	c.mu.Unlock()
	v, err := r.Get(bg, "new")
	t.Errorf("follower silently stuck after replacement: watch delivered %d, Get(new)=%q err=%v (writer has 5)", got, v.Value, err)
}

// R4: a prefix watcher fails with ErrCompacted because of compaction of keys
// it never asked for.
func TestReviewPrefixWatcherToldOfAnyGap(t *testing.T) {
	db := openT(t, tmpPath(t), nil)
	must[Revision](t)(db.Put(bg, "a/1", []byte("v"), Reason("r"), Actor("x")))
	for i := 0; i < 5; i++ {
		must[Revision](t)(db.Put(bg, "b/noise", []byte(fmt.Sprint(i)), Reason("r"), Actor("x")))
	}
	must[CompactStats](t)(db.Compact(bg, CompactOptions{}))
	err := db.Watch(bg, WatchOptions{Since: 1, Prefix: "a/"}, func(Revision) error { return nil })
	// The store can't tell what the removed revisions held, so a prefix
	// watcher is told too (README: "Honest about gaps").
	if !errors.Is(err, ErrCompacted) {
		t.Fatalf("prefix a/ watcher, only b/ revisions compacted: %v", err)
	}
}

// R5: stress: concurrent multi-revision transactions, compaction, a watcher
// on the writer and a follower. Order, no repeats, whole transactions.
func TestReviewStress(t *testing.T) {
	skipReplacingOpenFile(t)
	p := tmpPath(t)
	w := openT(t, p, &Options{NoSync: true})
	r := openT(t, p, &Options{ReadOnly: true})
	d := 8 * time.Second
	if testing.Short() {
		d = 2 * time.Second
	}
	ctx, cancel := context.WithTimeout(bg, d)
	defer cancel()

	type res struct {
		revs []Revision
		err  error
	}
	run := func(db *DB, opts WatchOptions, out chan res) {
		var got []Revision
		err := db.Watch(ctx, opts, func(rv Revision) error {
			got = append(got, rv)
			if rv.Key == "stop" {
				return errors.New("stop")
			}
			return nil
		})
		out <- res{got, err}
	}
	o1, o2, o3 := make(chan res, 1), make(chan res, 1), make(chan res, 1)
	go run(w, WatchOptions{SkipCompacted: true}, o1)
	go run(r, WatchOptions{SkipCompacted: true, Poll: time.Millisecond}, o2)
	go run(w, WatchOptions{Prefix: "t"}, o3) // prefix t: every key below is unique, so compaction removes none

	var wg sync.WaitGroup
	var txs atomic.Int64
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 150; i++ {
				err := w.Update(bg, func(tx *Tx) error {
					for j := 0; j < 1+i%4; j++ {
						if _, err := tx.Put(fmt.Sprintf("t%d/%d/%d", g, i, j), []byte("v")); err != nil {
							return err
						}
					}
					return nil
				}, Reason("r"), Actor("x"))
				if err != nil {
					t.Error(err)
					return
				}
				txs.Add(1)
			}
		}(g)
	}
	stopC := make(chan struct{})
	go func() {
		for {
			select {
			case <-stopC:
				return
			default:
			}
			if _, err := w.Compact(bg, CompactOptions{}); err != nil {
				t.Error(err)
				return
			}
			time.Sleep(3 * time.Millisecond)
		}
	}()
	wg.Wait()
	close(stopC)
	must[Revision](t)(w.Put(bg, "stop", []byte("v"), Reason("r"), Actor("x")))
	check := func(name string, rr res) {
		t.Logf("%s: %d revisions, err %v", name, len(rr.revs), rr.err)
		if rr.err == nil || rr.err.Error() != "stop" {
			t.Errorf("%s ended with %v", name, rr.err)
		}
		seenTx := map[uint64]bool{}
		for i, rv := range rr.revs {
			if i > 0 && rv.Rev <= rr.revs[i-1].Rev {
				t.Errorf("%s: out of order / repeated %d after %d", name, rv.Rev, rr.revs[i-1].Rev)
			}
			if i > 0 && rr.revs[i-1].Tx != rv.Tx && seenTx[rv.Tx] {
				t.Errorf("%s: tx %d split", name, rv.Tx)
			}
			seenTx[rv.Tx] = true
		}
	}
	check("writer", <-o1)
	check("follower", <-o2)
	r3 := <-o3
	if r3.err != nil && r3.err.Error() != "stop" {
		// stop key doesn't match prefix t... it does not; so wait with ctx
	}
	t.Logf("prefix watcher: %d revisions err %v", len(r3.revs), r3.err)
}

// R6: fn calls Refresh and Compact (read-only and writable): no deadlock.
func TestReviewFnRefreshCompact(t *testing.T) {
	skipReplacingOpenFile(t)
	p := tmpPath(t)
	w := openT(t, p, nil)
	r := openT(t, p, &Options{ReadOnly: true})
	for i := 0; i < 3; i++ {
		must[Revision](t)(w.Put(bg, "k", []byte(fmt.Sprint(i)), Reason("r"), Actor("x")))
	}
	ctx, cancel := context.WithTimeout(bg, 5*time.Second)
	defer cancel()
	n := 0
	err := r.Watch(ctx, WatchOptions{Poll: time.Millisecond}, func(rv Revision) error {
		n++
		if err := r.Refresh(); err != nil {
			return err
		}
		if n == 3 {
			return errors.New("done")
		}
		return nil
	})
	if err == nil || err.Error() != "done" {
		t.Fatalf("read-only fn Refresh: %v n=%d", err, n)
	}
	n = 0
	err = w.Watch(ctx, WatchOptions{SkipCompacted: true}, func(rv Revision) error {
		n++
		if _, err := w.Compact(bg, CompactOptions{}); err != nil {
			return err
		}
		if n == 1 {
			_, err := w.Put(bg, "k", []byte("again"), Reason("r"), Actor("x"))
			return err
		}
		return errors.New("done")
	})
	if err == nil || err.Error() != "done" {
		t.Fatalf("writer fn Compact: %v n=%d", err, n)
	}
}

// R7: follower and torn tail whose bytes contain a whole xkv transaction
// (a value that is itself a copy of an xkv record): Refresh reports
// corruption while the writer is merely mid-write.
func TestReviewTornTailWithEmbeddedTx(t *testing.T) {
	// Build the bytes of a complete transaction with high numbers.
	src := openT(t, tmpPath(t), &Options{Relaxed: true})
	for i := 0; i < 50; i++ {
		must[Revision](t)(src.Put(bg, "x", []byte("y")))
	}
	src.mu.RLock()
	m := src.revs[len(src.revs)-1]
	b := make([]byte, src.end-m.off)
	src.st.ReadAt(b, m.off)
	src.mu.RUnlock()

	p := tmpPath(t)
	w := openT(t, p, &Options{Relaxed: true})
	must[Revision](t)(w.Put(bg, "a", []byte("1")))
	r := openT(t, p, &Options{ReadOnly: true})
	w.Close()
	// What a reader can see while a writer is in the middle of writing a
	// transaction whose value is b: the frame header, part of the value.
	full, _ := os.ReadFile(p)
	w2 := openT(t, p, &Options{Relaxed: true})
	val := append(append([]byte{}, b...), make([]byte, 4096)...)
	must[Revision](t)(w2.Put(bg, "blob", val))
	w2.Close()
	after, _ := os.ReadFile(p)
	cut := len(full) + 64 + len(b) // inside the value, past the embedded tx
	if err := os.WriteFile(p+".torn", after[:cut], 0o644); err != nil {
		t.Fatal(err)
	}
	// Point the follower's view at the torn copy: same inode via write in place.
	f, _ := os.OpenFile(p, os.O_WRONLY, 0)
	f.Truncate(int64(cut))
	f.Close()
	// That read can be judged damage; a following Watch gives Refresh a
	// few more polls, and once the writer finishes it reads fine.
	first := r.Refresh()
	t.Logf("Refresh during a write in progress: %v", first)
	if err := os.WriteFile(p, after, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := r.Refresh(); err != nil {
		t.Fatalf("Refresh once the write finished: %v", err)
	}
	if v := must[Revision](t)(r.Get(bg, "blob")); len(v.Value) != len(val) {
		t.Fatalf("blob: %d bytes", len(v.Value))
	}
}

// R8: Import wakes a watcher.
func TestReviewImportWakes(t *testing.T) {
	a := openT(t, tmpPath(t), nil)
	b := openT(t, tmpPath(t), nil)
	c := watchT(t, b, WatchOptions{})
	time.Sleep(10 * time.Millisecond)
	must[Revision](t)(a.Put(bg, "k", []byte("v"), Reason("r"), Actor("x")))
	pr, pw := ioPipe()
	go func() {
		_, err := a.Export(bg, pw, ExportOptions{})
		pw.CloseWithError(err)
	}()
	if _, err := b.Import(bg, pr, ImportOptions{}); err != nil {
		t.Fatal(err)
	}
	got := c.wait(t, 1)
	if got[0].Key != "k" {
		t.Fatalf("%+v", got)
	}
}

func ioPipe() (*io.PipeReader, *io.PipeWriter) { return io.Pipe() }
