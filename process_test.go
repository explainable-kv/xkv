// Copyright xkv.org 2026
// SPDX-License-Identifier: Apache-2.0

//go:build unix

package xkv

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// The tests here start a copy of the test binary as a child process, using
// TestHelperProcess as its entry point, and kill it with SIGKILL at random
// moments.

func helper(t *testing.T, mode, path string) *exec.Cmd {
	cmd := exec.Command(os.Args[0], "-test.run=^TestHelperProcess$", "-test.v=false")
	cmd.Env = append(os.Environ(), "XKV_HELPER="+mode, "XKV_HELPER_PATH="+path)
	cmd.Stderr = os.Stderr
	return cmd
}

// TestHelperProcess is not a real test. It runs as the child process.
func TestHelperProcess(t *testing.T) {
	mode := os.Getenv("XKV_HELPER")
	if mode == "" {
		t.Skip("only runs as a child process")
	}
	path := os.Getenv("XKV_HELPER_PATH")
	out := bufio.NewWriter(os.Stdout)
	switch mode {
	case "writer":
		// Writes transactions of one to five revisions, each with causes
		// pointing at earlier revisions, and reports each one only after
		// Update has returned (the fsync is done).
		db, err := Open(path, nil)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		rng := rand.New(rand.NewSource(time.Now().UnixNano()))
		for {
			st, _ := db.Stats()
			size := 1 + rng.Intn(5)
			var first, last Revision
			err := db.Update(WithActor(bg, "writer"), func(tx *Tx) error {
				for i := 0; i < size; i++ {
					var opts []WriteOption
					if st.LastRev > 0 {
						// Cause: some earlier revision, found by number.
						for tries := 0; tries < 3; tries++ {
							rev := 1 + uint64(rng.Int63n(int64(st.LastRev)))
							if r, err := db.Revision(bg, rev); err == nil {
								opts = append(opts, Cause(r.Key, r.Rev))
								break
							}
						}
					}
					r, err := tx.Put(fmt.Sprintf("k%02d", rng.Intn(50)), []byte(strings.Repeat("x", rng.Intn(300))), opts...)
					if err != nil {
						return err
					}
					if i == 0 {
						first = r
					}
					last = r
				}
				return nil
			}, Reason("chaos"))
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(2)
			}
			fmt.Fprintf(out, "ack %d %d %d\n", first.Tx, first.Rev, last.Rev)
			out.Flush()
		}
	case "compactor":
		db, err := Open(path, &Options{NoSync: true})
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		fmt.Fprintln(out, "open")
		out.Flush()
		if _, err := db.Compact(bg, CompactOptions{KeepLast: 1}); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		fmt.Fprintln(out, "done")
		out.Flush()
		time.Sleep(time.Hour) // until killed
	case "importer":
		// Imports the exchange file named by XKV_HELPER_EXCHANGE.
		data, err := os.ReadFile(os.Getenv("XKV_HELPER_EXCHANGE"))
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		db, err := Open(path, nil)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		fmt.Fprintln(out, "start")
		out.Flush()
		if _, err := db.Import(WithActor(bg, "sync"), bytes.NewReader(data), ImportOptions{}); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		fmt.Fprintln(out, "done")
		out.Flush()
		time.Sleep(time.Hour) // until killed
	case "holder":
		db, err := Open(path, nil)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		if _, err := db.Put(bg, "held", []byte("1"), Reason("holder")); err != nil {
			os.Exit(2)
		}
		fmt.Fprintln(out, "ready")
		out.Flush()
		time.Sleep(time.Hour) // until killed
	}
}

type ack struct{ tx, first, last uint64 }

// TestCrashSIGKILL kills a writing process with SIGKILL, again and again, on
// the same file. After each kill the file is reopened and checked: every
// transaction the writer was told had committed is there in full, nothing
// half-written is visible, and every cause still resolves.
func TestCrashSIGKILL(t *testing.T) {
	runs := 200
	if testing.Short() {
		runs = 30
	}
	if v, err := strconv.Atoi(os.Getenv("XKV_CRASH_RUNS")); err == nil {
		runs = v
	}
	path := filepath.Join(t.TempDir(), "crash.xkv")
	rng := rand.New(rand.NewSource(1))
	var acked []ack
	torn, trimmed := 0, int64(0)
	start := time.Now()
	for run := 0; run < runs; run++ {
		cmd := helper(t, "writer", path)
		stdout, _ := cmd.StdoutPipe()
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		lines := make(chan string, 1024)
		go func() {
			sc := bufio.NewScanner(stdout)
			for sc.Scan() {
				lines <- sc.Text()
			}
			close(lines)
		}()
		// Let it write for a random while, then kill it mid-stride.
		deadline := time.After(time.Duration(5+rng.Intn(60)) * time.Millisecond)
	collect:
		for {
			select {
			case l, ok := <-lines:
				if !ok {
					break collect
				}
				var a ack
				fmt.Sscanf(l, "ack %d %d %d", &a.tx, &a.first, &a.last)
				acked = append(acked, a)
			case <-deadline:
				break collect
			}
		}
		cmd.Process.Signal(syscall.SIGKILL)
		cmd.Wait()
		for l := range lines {
			var a ack
			fmt.Sscanf(l, "ack %d %d %d", &a.tx, &a.first, &a.last)
			acked = append(acked, a)
		}

		rep, err := Check(path)
		if err != nil {
			t.Fatal(err)
		}
		if rep.TailBytes > 0 {
			torn++
			trimmed += rep.TailBytes
		}
		if len(rep.Problems) > 0 {
			t.Fatalf("run %d: %v", run, rep.Problems)
		}
		db, err := Open(path, nil)
		if err != nil {
			t.Fatalf("run %d: reopen: %v", run, err)
		}
		st, _ := db.Stats()
		for _, a := range acked {
			if a.last > st.LastRev {
				t.Fatalf("run %d: transaction %d was acknowledged but is missing (store ends at r%d)", run, a.tx, st.LastRev)
			}
			for rev := a.first; rev <= a.last; rev++ {
				r, err := db.Revision(bg, rev)
				if err != nil || r.Tx != a.tx {
					t.Fatalf("run %d: acknowledged r%d: %+v %v", run, rev, r, err)
				}
			}
		}
		db.Close()
	}
	db, _ := Open(path, &Options{ReadOnly: true})
	st, _ := db.Stats()
	db.Close()
	// A kill can't split a single write call, so torn tails come from power
	// loss rather than from SIGKILL; TestTornTail covers them byte by byte.
	t.Logf("%d SIGKILLs at random moments in %s: %d transactions acknowledged, 0 lost, 0 half-visible; %d more were on disk but killed before the writer could report them; %d kills left a torn tail (%d bytes); the file ends with %d revisions in %d transactions and every cause resolves",
		runs, time.Since(start).Round(time.Millisecond), len(acked), st.LastTx-uint64(len(acked)), torn, trimmed, st.Revisions, st.LastTx)
}

// TestKillDuringCompaction kills a compacting process at random moments.
// Whether the kill lands before or after the rename, the store answers Get
// and Why exactly as before, and no work file is left behind once reopened.
func TestKillDuringCompaction(t *testing.T) {
	runs := 40
	if testing.Short() {
		runs = 8
	}
	dir := t.TempDir()
	base := filepath.Join(dir, "base.xkv")
	db, err := Open(base, &Options{NoSync: true})
	if err != nil {
		t.Fatal(err)
	}
	randomWorkload(t, db, rand.New(rand.NewSource(7)), 20000, 400)
	// Compare every 20th key: full-depth Why trees over a random 20,000
	// revision graph are large, and the equivalence test covers every key.
	var sample []string
	for i := 0; i < 400; i += 20 {
		sample = append(sample, fmt.Sprintf("key:%03d", i))
	}
	want := capture(t, db, 1, sample...)
	st, _ := db.Stats()
	db.Close()
	os.Remove(base + snapshotSuffix)

	// Time one full compaction to aim the kills.
	probe := filepath.Join(dir, "probe.xkv")
	copyFile(t, base, probe, -1)
	pdb, _ := Open(probe, &Options{NoSync: true})
	t0 := time.Now()
	pdb.Compact(bg, CompactOptions{KeepLast: 1})
	full := time.Since(t0)
	pdb.Close()

	rng := rand.New(rand.NewSource(3))
	path := filepath.Join(dir, "c.xkv")
	before, after := 0, 0
	for run := 0; run < runs; run++ {
		copyFile(t, base, path, -1)
		os.Remove(path + snapshotSuffix)
		cmd := helper(t, "compactor", path)
		stdout, _ := cmd.StdoutPipe()
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		sc := bufio.NewScanner(stdout)
		sc.Scan() // "open": the store is loaded and compaction is starting
		time.Sleep(time.Duration(rng.Int63n(int64(full) * 3 / 2)))
		cmd.Process.Signal(syscall.SIGKILL)
		cmd.Wait()

		fi, _ := os.Stat(path)
		if fi.Size() < st.FileSize {
			after++
		} else {
			before++
		}
		db, err := Open(path, nil)
		if err != nil {
			t.Fatalf("run %d: %v", run, err)
		}
		sameState(t, fmt.Sprintf("run %d", run), want, capture(t, db, 1, sample...))
		db.Close()
		if _, err := os.Stat(path + compactSuffix); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("run %d: compaction work file left behind", run)
		}
		if rep, _ := Check(path); !rep.OK() {
			t.Fatalf("run %d: %v", run, rep.Problems)
		}
	}
	t.Logf("%d kills during compaction of %d revisions (a full compaction takes %s): %d landed before the rename, %d after; every time Get and full-depth Why on %d sampled keys matched the store before compaction and the check passed",
		runs, st.Revisions, full.Round(time.Millisecond), before, after, len(sample))
}

// TestTwoProcesses checks the lock across processes: while one process has
// the file open for writing, another can't, but can open it read-only.
func TestTwoProcesses(t *testing.T) {
	path := filepath.Join(t.TempDir(), "two.xkv")
	cmd := helper(t, "holder", path)
	stdout, _ := cmd.StdoutPipe()
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	sc := bufio.NewScanner(stdout)
	if !sc.Scan() || sc.Text() != "ready" {
		t.Fatal("holder didn't start")
	}
	if _, err := Open(path, nil); !errors.Is(err, ErrLocked) {
		t.Fatalf("second writer process: %v", err)
	}
	ro, err := Open(path, &Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	if v, err := ro.Get(bg, "held"); err != nil || string(v.Value) != "1" {
		t.Fatalf("read-only view: %v %v", v, err)
	}
	ro.Close()
	cmd.Process.Signal(syscall.SIGKILL)
	cmd.Wait()
	// The lock dies with the process.
	db, err := Open(path, nil)
	if err != nil {
		t.Fatalf("after the holder died: %v", err)
	}
	db.Close()
}

// TestKillDuringImport kills a process importing an exchange file at random
// moments, then imports the same file again. Every time the file checks out,
// and the second import finishes the job: the store ends with the same
// values as one clean import, and no revision was imported twice.
func TestKillDuringImport(t *testing.T) {
	if testing.Short() {
		t.Skip("slow")
	}
	runs := 40
	if n, err := strconv.Atoi(os.Getenv("XKV_IMPORT_KILLS")); err == nil && n > 0 {
		runs = n
	}
	dir := t.TempDir()
	// The source: 300 transactions with causes.
	src, err := Open(filepath.Join(dir, "src.xkv"), &Options{NoSync: true})
	if err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewSource(3))
	var revs []Revision
	for i := 0; i < 300; i++ {
		opts := []WriteOption{Reason(fmt.Sprint("change ", i)), Actor("source")}
		if len(revs) > 0 {
			r := revs[rng.Intn(len(revs))]
			opts = append(opts, Cause(r.Key, r.Rev))
		}
		err := src.Update(bg, func(tx *Tx) error {
			for j := 1 + rng.Intn(3); j > 0; j-- {
				r, err := tx.Put(fmt.Sprintf("k%02d", rng.Intn(40)), []byte(fmt.Sprint(rng.Intn(100))))
				if err != nil {
					return err
				}
				revs = append(revs, r)
			}
			return nil
		}, opts...)
		if err != nil {
			t.Fatal(err)
		}
	}
	var buf bytes.Buffer
	if _, err := src.Export(bg, &buf, ExportOptions{}); err != nil {
		t.Fatal(err)
	}
	src.Close()
	exPath := filepath.Join(dir, "changes.xkvx")
	if err := os.WriteFile(exPath, buf.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
	// The target starts with values of its own on some of the same keys,
	// so the import settles conflicts too.
	base := filepath.Join(dir, "base.xkv")
	tgt, err := Open(base, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		if _, err := tgt.Put(bg, fmt.Sprintf("k%02d", i*2), []byte("local"), Reason("local value"), Actor("target")); err != nil {
			t.Fatal(err)
		}
	}
	tgt.Close()
	baseBytes, _ := os.ReadFile(base)

	// A clean import gives the values to expect, and its length the
	// window the kills aim at.
	ref := filepath.Join(dir, "ref.xkv")
	os.WriteFile(ref, baseBytes, 0o644)
	rdb, err := Open(ref, nil)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	rrep, err := rdb.Import(WithActor(bg, "sync"), bytes.NewReader(buf.Bytes()), ImportOptions{})
	if err != nil {
		t.Fatal(err)
	}
	window := time.Since(start)
	want := valuesOf(t, rdb)
	rdb.Close()

	partial := 0
	for run := 0; run < runs; run++ {
		path := filepath.Join(dir, fmt.Sprintf("t%d.xkv", run))
		os.WriteFile(path, baseBytes, 0o644)
		cmd := helper(t, "importer", path)
		cmd.Env = append(cmd.Env, "XKV_HELPER_EXCHANGE="+exPath)
		stdout, _ := cmd.StdoutPipe()
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		sc := bufio.NewScanner(stdout)
		if !sc.Scan() || sc.Text() != "start" {
			t.Fatal("importer didn't start")
		}
		time.Sleep(time.Duration(rng.Int63n(int64(window) * 3 / 2)))
		cmd.Process.Signal(syscall.SIGKILL)
		cmd.Wait()
		if rep, err := Check(path); err != nil || !rep.OK() {
			t.Fatalf("run %d: check after the kill: %v %v", run, err, rep.Problems)
		}
		db, err := Open(path, nil)
		if err != nil {
			t.Fatalf("run %d: %v", run, err)
		}
		rep, err := db.Import(WithActor(bg, "sync"), bytes.NewReader(buf.Bytes()), ImportOptions{})
		if err != nil {
			t.Fatalf("run %d: import again: %v", run, err)
		}
		if rep.Skipped > 0 && rep.Imported+rep.Context > 0 {
			partial++
		}
		got := valuesOf(t, db)
		for k, v := range want {
			if got[k] != v {
				t.Fatalf("run %d: %s is %q, a clean import gives %q", run, k, got[k], v)
			}
		}
		seen := map[ident]bool{}
		db.mu.RLock()
		for _, m := range db.revs {
			if m.origin != nil {
				if seen[*m.origin] {
					db.mu.RUnlock()
					t.Fatalf("run %d: %s was imported twice", run, m.origin)
				}
				seen[*m.origin] = true
			}
		}
		n := len(seen)
		db.mu.RUnlock()
		if n != rrep.Imported+rrep.Context+rrep.Agreed {
			t.Fatalf("run %d: %d imported revisions, a clean import has %d", run, n, rrep.Imported+rrep.Context+rrep.Agreed)
		}
		db.Close()
		os.Remove(path)
		os.Remove(path + snapshotSuffix)
	}
	t.Logf("%d kills of a process importing %d revisions in %d transactions (a clean import takes %s and settles %d conflicts): %d landed part-way, and every time the file checked out and importing again ended with the values of a clean import and no revision imported twice",
		runs, rrep.Imported, 300, window.Round(time.Millisecond), len(rrep.Conflicts), partial)
}
