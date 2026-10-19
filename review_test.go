// Copyright xkv.org 2026
// SPDX-License-Identifier: Apache-2.0

package xkv

// Regression tests for the findings of the pre-release review.

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"runtime"
	"testing"
	"time"
)

// An Open racing a compaction must not end up holding the lock on the old,
// unlinked file, where its writes would vanish.
func TestOpenRacingCompaction(t *testing.T) {
	path := tmpPath(t)
	first, err := Open(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	chain(t, first)
	fired := false
	testHookOpened = func() {
		if fired {
			return
		}
		fired = true
		// Between the second Open's OpenFile and its lock: compact, then
		// let go of the file.
		// Windows can't rename over a file another handle has open, so
		// there compaction refuses and changes nothing, and the race can't
		// happen.
		if _, err := first.Compact(bg, CompactOptions{}); err != nil && runtime.GOOS != "windows" {
			t.Error(err)
		} else if err != nil {
			t.Log(err)
		}
		first.Close()
	}
	defer func() { testHookOpened = nil }()
	second, err := Open(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	must[Revision](t)(second.Put(bg, "after-race", []byte("kept"), Reason("written by the racing opener")))
	second.Close()
	third := openT(t, path, nil)
	if v, err := third.Get(bg, "after-race"); err != nil || string(v.Value) != "kept" {
		t.Fatalf("a write by the handle that raced compaction was lost: %v", err)
	}
}

// A snapshot left beside a file restored from a backup must not be trusted.
func TestSnapshotBesideARestoredFile(t *testing.T) {
	path := tmpPath(t)
	db := openT(t, path, nil)
	must[Revision](t)(db.Put(bg, "secret", []byte("AAAA"), Reason("r")))
	db.Close()
	backup, _ := os.ReadFile(path)
	db = openT(t, path, nil)
	must[Revision](t)(db.Put(bg, "public", []byte("BBBB"), Reason("r")))
	must[Revision](t)(db.Delete(bg, "secret", Reason("r")))
	db.Close()
	// Restore the backup, leaving the newer snapshot in place.
	os.WriteFile(path, backup, 0o644)
	db = openT(t, path, nil)
	if v, err := db.Get(bg, "secret"); err != nil || string(v.Value) != "AAAA" {
		t.Fatalf("restored file read through a stale snapshot: %q %v", v.Value, err)
	}
	if _, err := db.Get(bg, "public"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("a key from the newer snapshot shows up: %v", err)
	}
}

// A large torn tail opens quickly and is trimmed, its bytes kept aside.
func TestLargeTornTailOpensFast(t *testing.T) {
	path := tmpPath(t)
	db := openT(t, path, &Options{NoSync: true})
	chain(t, db)
	before, _ := db.Stats()
	big := make([]byte, MaxValueSize)
	rand.New(rand.NewSource(1)).Read(big)
	must[Revision](t)(db.Put(bg, "big", big, Reason("big")))
	after, _ := db.Stats()
	db.Close()
	os.Remove(path + snapshotSuffix)
	b, _ := os.ReadFile(path)
	os.WriteFile(path, b[:before.FileSize+(after.FileSize-before.FileSize)/2], 0o644)
	t0 := time.Now()
	db, err := Open(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	took := time.Since(t0)
	st, _ := db.Stats()
	trimmed := db.Trimmed()
	db.Close()
	if st.LastRev != before.LastRev {
		t.Fatalf("revisions after trimming: %d", st.LastRev)
	}
	if fi, err := os.Stat(trimmed); err != nil || fi.Size() != (after.FileSize-before.FileSize)/2 {
		t.Fatalf("trimmed bytes not kept: %v", err)
	}
	if took > 3*time.Second {
		t.Fatalf("opening an 8 MiB torn tail took %s", took)
	}
	t.Logf("an 8 MiB torn tail was trimmed in %s, its bytes kept in %s", took.Round(time.Millisecond), trimmed)
}

// Bytes crafted to look like frame starts every few bytes must not make
// Open crawl.
func TestCraftedTornTailIsBounded(t *testing.T) {
	path := tmpPath(t)
	db := openT(t, path, &Options{NoSync: true})
	chain(t, db)
	before, _ := db.Stats()
	v := make([]byte, MaxValueSize)
	for i := 0; i+8 <= len(v); i += 4 {
		// length 16 MiB, then a revision byte and plausible numbers
		v[i], v[i+1], v[i+2], v[i+3] = 0, 0, 0, 1
	}
	for i := 8; i < len(v); i += 16 {
		v[i] = recRevision
		v[i+1], v[i+2] = 100, 100
	}
	must[Revision](t)(db.Put(bg, "crafted", v, Reason("r")))
	after, _ := db.Stats()
	db.Close()
	os.Remove(path + snapshotSuffix)
	b, _ := os.ReadFile(path)
	os.WriteFile(path, b[:before.FileSize+(after.FileSize-before.FileSize)*3/4], 0o644)
	t0 := time.Now()
	db, err := Open(path, &Options{Repair: true})
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	if took := time.Since(t0); took > 10*time.Second {
		t.Fatalf("crafted torn tail took %s to open", took)
	}
}

// A file that isn't a store, shorter than a header, is refused, not
// overwritten.
func TestShortForeignFileIsLeftAlone(t *testing.T) {
	path := tmpPath(t)
	os.WriteFile(path, []byte("my notes\n"), 0o644)
	if _, err := Open(path, nil); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Open on a 9-byte text file: %v", err)
	}
	if b, _ := os.ReadFile(path); string(b) != "my notes\n" {
		t.Fatal("the file was changed")
	}
	// A header cut short by a crash is still taken over.
	os.WriteFile(path, []byte("XKVF\x01"), 0o644)
	db := openT(t, path, nil)
	must[Revision](t)(db.Put(bg, "k", nil, Reason("r")))
}

type failingStorage struct {
	storage
	failSync, failTruncate bool
}

func (f *failingStorage) Sync() error {
	if f.failSync {
		return errors.New("EIO")
	}
	return f.storage.Sync()
}

func (f *failingStorage) Truncate(n int64) error {
	if f.failTruncate {
		return errors.New("EIO")
	}
	return f.storage.Truncate(n)
}

// After a failed fsync the handle refuses further writes.
func TestFailedSyncStopsWrites(t *testing.T) {
	db, _ := OpenMemory(nil)
	must[Revision](t)(db.Put(bg, "a", []byte("1"), Reason("r")))
	fs := &failingStorage{storage: db.st, failSync: true, failTruncate: true}
	db.st = fs
	if _, err := db.Put(bg, "b", []byte("2"), Reason("r")); err == nil {
		t.Fatal("a failed fsync was reported as success")
	}
	fs.failSync, fs.failTruncate = false, false
	if _, err := db.Put(bg, "c", []byte("3"), Reason("r")); !errors.Is(err, ErrFailed) {
		t.Fatalf("write after a failed fsync: %v", err)
	}
	if v, err := db.Get(bg, "a"); err != nil || string(v.Value) != "1" {
		t.Fatalf("reads after the failure: %v", err)
	}
}

// Why shows a revision's causes at its shallowest place, even when a deeper
// path reaches it first.
func TestWhyShallowestPathShowsCauses(t *testing.T) {
	db, _ := OpenMemory(nil)
	put := must[Revision](t)
	base := put(db.Put(bg, "base", nil, Reason("start")))
	x := put(db.Put(bg, "x", nil, Reason("x"), Cause("base", base.Rev)))
	a1 := put(db.Put(bg, "a1", nil, Reason("a1"), Cause("x", x.Rev)))
	a2 := put(db.Put(bg, "a2", nil, Reason("a2"), Cause("a1", a1.Rev)))
	// root's first cause leads to x three levels down; its second, one.
	put(db.Put(bg, "root", nil, Reason("root"), Cause("a2", a2.Rev), Cause("x", x.Rev)))
	n := must[*WhyNode](t)(db.Why(bg, "root", 3))
	direct := n.Causes[1]
	if direct.Rev != x.Rev || len(direct.Causes) != 1 || direct.Causes[0].Rev != base.Rev {
		t.Fatalf("x's cause hidden: %+v", direct)
	}
	deep := n.Causes[0].Causes[0].Causes[0]
	if deep.Rev != x.Rev || !deep.Repeat {
		t.Fatalf("deep copy of x: %+v", deep)
	}
}

// Files written beside the store keep its permissions.
func TestSideFilesKeepTheStoreMode(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("no Unix permissions")
	}
	path := tmpPath(t)
	db := openT(t, path, nil)
	chain(t, db)
	db.Close()
	os.Chmod(path, 0o600)
	db = openT(t, path, nil)
	must[Revision](t)(db.Put(bg, "k", nil, Reason("r")))
	must[CompactStats](t)(db.Compact(bg, CompactOptions{}))
	db.Close()
	for _, p := range []string{path, path + snapshotSuffix} {
		if fi, err := os.Stat(p); err != nil || fi.Mode().Perm() != 0o600 {
			t.Fatalf("%s: mode %v %v", p, fi.Mode().Perm(), err)
		}
	}
}

// Revision numbers can't wrap around.
func TestRevisionNumbersDontWrap(t *testing.T) {
	db, _ := OpenMemory(nil)
	db.lastRev = math.MaxUint64 - 1
	if _, err := db.Put(bg, "k", nil, Reason("r")); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("write at the top of the numbers: %v", err)
	}
}

// A WhyNode marshals the same as a value or a pointer, and reads back.
func TestWhyNodeJSONRoundTrip(t *testing.T) {
	db, _ := OpenMemory(nil)
	chain(t, db)
	n := must[*WhyNode](t)(db.Why(bg, "account:42:limit", 0))
	byPtr, _ := json.Marshal(n)
	byVal, _ := json.Marshal(*n)
	if !bytes.Equal(byPtr, byVal) {
		t.Fatal("value and pointer marshal differently")
	}
	var back WhyNode
	if err := json.Unmarshal(byPtr, &back); err != nil {
		t.Fatal(err)
	}
	again, _ := json.Marshal(back)
	if !bytes.Equal(again, byPtr) {
		t.Fatalf("round trip changed the tree:\n%s\n%s", byPtr, again)
	}
	if back.Causes[0].Causes[0].Ref != "BANK-7731" {
		t.Fatal(fmt.Sprint(back.Causes[0].Causes[0]))
	}
}

// Damage in the middle of a compacted file, where revision numbers have
// gaps, is still found.
func TestDamageInACompactedFile(t *testing.T) {
	path := tmpPath(t)
	db := openT(t, path, &Options{NoSync: true})
	randomWorkload(t, db, rand.New(rand.NewSource(5)), 3000, 50)
	must[CompactStats](t)(db.Compact(bg, CompactOptions{}))
	for i := 0; i < 50; i++ {
		must[Revision](t)(db.Put(bg, fmt.Sprintf("late:%d", i), []byte("x"), Reason("after compaction")))
	}
	st, _ := db.Stats()
	db.Close()
	b, _ := os.ReadFile(path)
	b[len(b)/3] ^= 0xff
	os.WriteFile(path, b, 0o644)
	if _, err := Open(path, nil); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("damage a third of the way into a compacted file of %d revisions: %v", st.Revisions, err)
	}
}

// A torn write whose value holds a whole store file looks like damage, so
// Open refuses; repair then trims it and keeps everything committed.
func TestTornWriteOfAStoreFileValue(t *testing.T) {
	inner := tmpPath(t)
	idb := openT(t, inner, &Options{NoSync: true})
	for i := 0; i < 20; i++ {
		must[Revision](t)(idb.Put(bg, fmt.Sprintf("i%d", i), []byte("v"), Reason("r")))
	}
	idb.Close()
	innerBytes, _ := os.ReadFile(inner)

	path := tmpPath(t)
	db := openT(t, path, nil)
	before, _ := db.Stats()
	must[Revision](t)(db.Put(bg, "backup", innerBytes, Reason("store a copy")))
	db.Close()
	os.Remove(path + snapshotSuffix)
	b, _ := os.ReadFile(path)
	os.WriteFile(path, b[:len(b)-20], 0o644)
	if _, err := Open(path, nil); err != nil && !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
	db, err := Open(path, &Options{Repair: true})
	if err != nil {
		t.Fatal(err)
	}
	if st, _ := db.Stats(); st.LastRev != before.LastRev {
		t.Fatalf("repair kept %d revisions, want %d", st.LastRev, before.LastRev)
	}
	db.Close()
}
