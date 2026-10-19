// Copyright xkv.org 2026
// SPDX-License-Identifier: Apache-2.0

package xkv

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// collect runs Watch in a goroutine and hands back what it delivers.
type collector struct {
	mu   sync.Mutex
	revs []Revision
	done chan error
	stop context.CancelFunc
}

func watchT(t *testing.T, db *DB, opts WatchOptions) *collector {
	t.Helper()
	ctx, cancel := context.WithCancel(bg)
	c := &collector{done: make(chan error, 1), stop: cancel}
	go func() {
		c.done <- db.Watch(ctx, opts, func(r Revision) error {
			c.mu.Lock()
			c.revs = append(c.revs, r)
			c.mu.Unlock()
			return nil
		})
	}()
	t.Cleanup(cancel)
	return c
}

// wait waits until the collector holds n revisions.
func (c *collector) wait(t *testing.T, n int) []Revision {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		if len(c.revs) >= n {
			out := append([]Revision(nil), c.revs...)
			c.mu.Unlock()
			return out
		}
		c.mu.Unlock()
		select {
		case err := <-c.done:
			t.Fatalf("Watch returned early: %v", err)
		case <-time.After(2 * time.Millisecond):
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	t.Fatalf("waited for %d revisions, got %d", n, len(c.revs))
	return nil
}

func TestWatchCatchUpThenLive(t *testing.T) {
	db := openT(t, tmpPath(t), nil)
	chain(t, db) // revisions 1 to 5
	c := watchT(t, db, WatchOptions{Since: 2})
	got := c.wait(t, 3)
	if got[0].Rev != 3 || got[2].Rev != 5 || got[2].Key != "account:42:limit" || string(got[2].Value) != "50" || got[2].Reason != "risk policy v3" {
		t.Fatalf("catch-up: %+v", got)
	}
	must[Revision](t)(db.Put(bg, "live", []byte("1"), Reason("after the watch began"), Actor("x")))
	got = c.wait(t, 4)
	if got[3].Key != "live" || got[3].Rev != 6 {
		t.Fatalf("live: %+v", got[3])
	}
	c.stop()
	if err := <-c.done; !errors.Is(err, context.Canceled) {
		t.Fatalf("after cancel: %v", err)
	}
}

// Many writers, one watcher: every revision arrives once, in order, and a
// transaction's revisions arrive together, never before it committed.
func TestWatchConcurrentWriters(t *testing.T) {
	db := openT(t, tmpPath(t), &Options{NoSync: true})
	c := watchT(t, db, WatchOptions{})
	const writers, txs = 4, 150
	var wg sync.WaitGroup
	total := 0
	var mu sync.Mutex
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < txs; i++ {
				n := 1 + (i+w)%3
				err := db.Update(bg, func(tx *Tx) error {
					for j := 0; j < n; j++ {
						if _, err := tx.Put(fmt.Sprintf("w%d/k%d", w, j), []byte(fmt.Sprint(i)), CauseLatest("seed")); err != nil {
							return err
						}
					}
					return nil
				}, Reason("load"), Actor(fmt.Sprint("writer", w)))
				if err != nil && errors.Is(err, ErrBadCause) {
					// No seed yet: write it (racing writers may each try).
					db.Put(bg, "seed", []byte("s"), Reason("seed"), Actor("x"))
					i--
					continue
				}
				if err != nil {
					t.Error(err)
					return
				}
				mu.Lock()
				total += n
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()
	st := must[Stats](t)(db.Stats())
	got := c.wait(t, int(st.LastRev))
	txSeen := map[uint64]bool{}
	for i, r := range got {
		if r.Rev != uint64(i+1) {
			t.Fatalf("revision %d arrived at position %d", r.Rev, i)
		}
		if i > 0 && r.Tx != got[i-1].Tx && txSeen[r.Tx] {
			t.Fatalf("transaction %d arrived in pieces", r.Tx)
		}
		txSeen[r.Tx] = true
		if want := must[Revision](t)(db.Revision(bg, r.Rev)); want.Key != r.Key || string(want.Value) != string(r.Value) {
			t.Fatalf("revision %d delivered as %+v, stored as %+v", r.Rev, r, want)
		}
	}
	t.Logf("%d writers, %d transactions each: %d revisions delivered once each, in order, every transaction whole", writers, txs, len(got))
}

func TestWatchPrefix(t *testing.T) {
	db := openT(t, tmpPath(t), nil)
	c := watchT(t, db, WatchOptions{Prefix: "config/"})
	must[Revision](t)(db.Put(bg, "other", []byte("1"), Reason("r"), Actor("x")))
	must[Revision](t)(db.Put(bg, "config/a", []byte("1"), Reason("r"), Actor("x")))
	must[Revision](t)(db.Put(bg, "other", []byte("2"), Reason("r"), Actor("x")))
	must[Revision](t)(db.Put(bg, "config/b", []byte("1"), Reason("r"), Actor("x")))
	got := c.wait(t, 2)
	if len(got) != 2 || got[0].Key != "config/a" || got[1].Key != "config/b" {
		t.Fatalf("%+v", got)
	}
}

func TestWatchCompacted(t *testing.T) {
	db := openT(t, tmpPath(t), nil)
	for i := 0; i < 6; i++ {
		must[Revision](t)(db.Put(bg, "k", []byte(fmt.Sprint(i)), Reason("r"), Actor("x")))
	}
	must[CompactStats](t)(db.Compact(bg, CompactOptions{KeepLast: 2}))
	// Revisions 1 to 4 are gone: a watch from 0 is told so.
	err := db.Watch(bg, WatchOptions{}, func(Revision) error { return nil })
	if !errors.Is(err, ErrCompacted) {
		t.Fatalf("watch from before compaction: %v", err)
	}
	// From the oldest held, all is well; and asked to skip, it gives what's held.
	c := watchT(t, db, WatchOptions{Since: 4})
	if got := c.wait(t, 2); got[0].Rev != 5 {
		t.Fatalf("%+v", got)
	}
	c2 := watchT(t, db, WatchOptions{SkipCompacted: true})
	if got := c2.wait(t, 2); got[0].Rev != 5 || got[1].Rev != 6 {
		t.Fatalf("skip: %+v", got)
	}

	// A watcher that falls behind while compaction runs is told what it
	// missed when it gets there.
	db2 := openT(t, tmpPath(t), nil)
	for i := 0; i < 5; i++ {
		must[Revision](t)(db2.Put(bg, "k", []byte(fmt.Sprint(i)), Reason("r"), Actor("x")))
	}
	gate := make(chan struct{})
	res := make(chan error, 1)
	var seen []uint64
	go func() {
		res <- db2.Watch(bg, WatchOptions{}, func(r Revision) error {
			seen = append(seen, r.Rev)
			if r.Rev == 1 {
				<-gate
			}
			return nil
		})
	}()
	time.Sleep(20 * time.Millisecond)
	must[Revision](t)(db2.Put(bg, "k", []byte("x"), Reason("r"), Actor("x")))
	must[CompactStats](t)(db2.Compact(bg, CompactOptions{}))
	close(gate)
	// The first batch (1 to 5) was read before compaction; 6 is the only
	// revision left after it, and it follows 5, so nothing was missed.
	select {
	case err := <-res:
		t.Fatalf("watch stopped: %v (saw %v)", err, seen)
	case <-time.After(50 * time.Millisecond):
	}
	if len(seen) != 6 {
		t.Fatalf("saw %v", seen)
	}
	// Now a watcher stuck at 0 while revisions it hadn't read are dropped.
	gate2 := make(chan struct{})
	res2 := make(chan error, 1)
	db3 := openT(t, tmpPath(t), nil)
	must[Revision](t)(db3.Put(bg, "k", []byte("0"), Reason("r"), Actor("x")))
	go func() {
		res2 <- db3.Watch(bg, WatchOptions{}, func(r Revision) error {
			<-gate2
			return nil
		})
	}()
	time.Sleep(20 * time.Millisecond)
	for i := 1; i < 5; i++ {
		must[Revision](t)(db3.Put(bg, "k", []byte(fmt.Sprint(i)), Reason("r"), Actor("x")))
	}
	must[CompactStats](t)(db3.Compact(bg, CompactOptions{}))
	close(gate2)
	if err := <-res2; !errors.Is(err, ErrCompacted) {
		t.Fatalf("lagging watcher: %v", err)
	}
}

func TestWatchEnds(t *testing.T) {
	db := openT(t, tmpPath(t), nil)
	must[Revision](t)(db.Put(bg, "k", []byte("1"), Reason("r"), Actor("x")))
	stop := errors.New("enough")
	if err := db.Watch(bg, WatchOptions{}, func(Revision) error { return stop }); err != stop {
		t.Fatalf("fn's error: %v", err)
	}
	c := watchT(t, db, WatchOptions{Since: 1})
	time.Sleep(10 * time.Millisecond)
	db.Close()
	select {
	case err := <-c.done:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("after Close: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close didn't end the watch")
	}
}

// fn may write to the store it watches.
func TestWatchFnWrites(t *testing.T) {
	db := openT(t, tmpPath(t), nil)
	ctx, cancel := context.WithTimeout(bg, 5*time.Second)
	defer cancel()
	must[Revision](t)(db.Put(bg, "order:1", []byte("placed"), Reason("checkout"), Actor("shop")))
	n := 0
	err := db.Watch(ctx, WatchOptions{Prefix: "order:"}, func(r Revision) error {
		n++
		if _, err := db.Put(bg, "audit:"+r.Key, []byte("seen"), Reason("saw the order"), Actor("auditor"), Cause(r.Key, r.Rev)); err != nil {
			return err
		}
		if n == 2 {
			return context.Canceled
		}
		return db.Update(bg, func(tx *Tx) error {
			_, err := tx.Put("order:2", []byte("placed"))
			return err
		}, Reason("second order"), Actor("shop"))
	})
	if !errors.Is(err, context.Canceled) || n != 2 {
		t.Fatalf("%v %d", err, n)
	}
}

// A read-only handle follows a writable one on the same file, through
// compaction.
func TestWatchFollowsTheFile(t *testing.T) {
	p := tmpPath(t)
	w := openT(t, p, nil)
	must[Revision](t)(w.Put(bg, "k", []byte("0"), Reason("r"), Actor("x")))
	r := openT(t, p, &Options{ReadOnly: true})
	c := watchT(t, r, WatchOptions{Poll: 5 * time.Millisecond})
	c.wait(t, 1)
	for i := 1; i <= 20; i++ {
		must[Revision](t)(w.Put(bg, "k", []byte(fmt.Sprint(i)), Reason("r"), Actor("x")))
	}
	if got := c.wait(t, 21); got[20].Rev != 21 {
		t.Fatalf("%+v", got[20])
	}
	must[CompactStats](t)(w.Compact(bg, CompactOptions{}))
	must[Revision](t)(w.Put(bg, "k", []byte("after"), Reason("r"), Actor("x")))
	if got := c.wait(t, 22); string(got[21].Value) != "after" || got[21].Rev != 22 {
		t.Fatalf("after compaction: %+v", got[21])
	}
	// The handle itself now reads the compacted file.
	if v := must[Revision](t)(r.Get(bg, "k")); string(v.Value) != "after" {
		t.Fatalf("%q", v.Value)
	}
}
