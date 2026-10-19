package xkv

import (
	"fmt"
	"os"
	"testing"
	"time"
)

// Same store, an older copy restored (backup put back): the follower keeps
// lastRev from the newer file and never sees the writer's new revisions,
// which reuse numbers it has already counted.
func TestReview2RestoredBackupSameStore(t *testing.T) {
	p := tmpPath(t)
	w := openT(t, p, nil)
	for i := 0; i < 3; i++ {
		must[Revision](t)(w.Put(bg, "k", []byte(fmt.Sprint(i)), Reason("r"), Actor("x")))
	}
	w.Close()
	backup, _ := os.ReadFile(p)
	w = openT(t, p, nil)
	for i := 3; i < 10; i++ {
		must[Revision](t)(w.Put(bg, "k", []byte(fmt.Sprint(i)), Reason("r"), Actor("x")))
	}
	r := openT(t, p, &Options{ReadOnly: true})
	c := watchT(t, r, WatchOptions{Since: 10, Poll: 5 * time.Millisecond})
	w.Close()
	os.Remove(p + snapshotSuffix)
	os.WriteFile(p+".tmp", backup, 0o644)
	os.Rename(p+".tmp", p)
	w2 := openT(t, p, nil)
	for i := 0; i < 12; i++ { // revisions 4..15
		must[Revision](t)(w2.Put(bg, "after-restore", []byte(fmt.Sprint(i)), Reason("r"), Actor("x")))
	}
	time.Sleep(200 * time.Millisecond)
	select {
	case err := <-c.done:
		t.Logf("watch ended: %v", err)
		return
	default:
	}
	c.mu.Lock()
	got := c.revs
	c.mu.Unlock()
	v, err := r.Get(bg, "after-restore")
	t.Errorf("restored backup: watch delivered %d revisions (%v), Get=%q %v; writer is at rev 15", len(got), got, v.Value, err)
}

// A format 1 store deleted and recreated (as format 2): no store id to
// compare, so the follower is silently stuck as before.
func TestReview2Format1Recreated(t *testing.T) {
	p := tmpPath(t)
	b, _ := os.ReadFile("testdata/format1.xkv")
	os.WriteFile(p, b, 0o644)
	r := openT(t, p, &Options{ReadOnly: true})
	st := must[Stats](t)(r.Stats())
	c := watchT(t, r, WatchOptions{Since: st.LastRev, Poll: 5 * time.Millisecond})
	os.Remove(p)
	w := openT(t, p, nil)
	for i := 0; i < 6; i++ {
		must[Revision](t)(w.Put(bg, "new", []byte(fmt.Sprint(i)), Reason("r"), Actor("x")))
	}
	time.Sleep(200 * time.Millisecond)
	select {
	case err := <-c.done:
		t.Logf("watch ended: %v", err)
		return
	default:
	}
	c.mu.Lock()
	n := len(c.revs)
	c.mu.Unlock()
	t.Errorf("format 1 recreated: lastRev was %d, watch delivered %d of 6, no error", st.LastRev, n)
}
