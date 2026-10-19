// Copyright xkv.org 2026
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/explainable-kv/xkv"
)

func cmdExport(args []string, stdout, stderr io.Writer) error {
	p, err := parseArgs(args, flagSpec{"since": false, "out": false})
	if err != nil {
		return usageError{err.Error()}
	}
	if len(p.pos) != 1 {
		return usagef("want FILE")
	}
	var opts xkv.ExportOptions
	if p.has("since") {
		if opts.Since, err = parseRev(p.str("since")); err != nil {
			return err
		}
	}
	db, err := openRead(p.pos[0])
	if err != nil {
		return err
	}
	defer db.Close()
	if s, err := db.Stats(); err == nil && s.Format < 2 {
		return fmt.Errorf("the store uses file format 1; run xkv upgrade %s first", p.pos[0])
	}
	report := stderr
	var st xkv.ExportStats
	if out := p.str("out"); out != "" {
		// Write next to the target and rename, so a half-written export
		// never sits under the final name.
		tmp, err := os.CreateTemp(filepath.Dir(out), ".xkv-export-*")
		if err != nil {
			return err
		}
		st, err = db.Export(context.Background(), tmp, opts)
		if err == nil {
			err = tmp.Sync()
		}
		if cerr := tmp.Close(); err == nil {
			err = cerr
		}
		if err == nil {
			err = os.Rename(tmp.Name(), out)
		}
		if err != nil {
			os.Remove(tmp.Name())
			return err
		}
		report = stdout
	} else if st, err = db.Export(context.Background(), stdout, opts); err != nil {
		return err
	}
	fmt.Fprintf(report, "exported %d revisions above r%d (and %d older ones their causes reach) from store %s, through r%d; next time: --since %d\n",
		st.Revisions, st.Since, st.Context, st.Store, st.Through, st.Through)
	return nil
}

func cmdImport(args []string, stdin io.Reader, stdout io.Writer) error {
	p, err := parseArgs(args, flagSpec{"conflicts": false, "json": true})
	if err != nil {
		return usageError{err.Error()}
	}
	if len(p.pos) != 2 {
		return usagef("want FILE EXCHANGE-FILE (or - for standard input)")
	}
	pol, err := xkv.ParseConflictPolicy(p.str("conflicts"))
	if err != nil {
		return usageError{err.Error()}
	}
	var in io.Reader = stdin
	if p.pos[1] != "-" {
		f, err := os.Open(p.pos[1])
		if err != nil {
			return err
		}
		defer f.Close()
		in = f
	}
	db, err := xkv.Open(p.pos[0], nil)
	if err != nil {
		return err
	}
	rep, err := db.Import(context.Background(), in, xkv.ImportOptions{Conflicts: pol})
	if cerr := db.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	if p.bools["json"] {
		return writeJSON(stdout, rep)
	}
	fmt.Fprintf(stdout, "from store %s, r%d to r%d: %d imported, %d older ones kept as history, %d already here, %d changed alike on both sides\n",
		rep.Store, rep.Since, rep.Through, rep.Imported, rep.Context, rep.Skipped, rep.Agreed)
	for _, c := range rep.Conflicts {
		fmt.Fprintf(stdout, "conflict on %s: ours r%d, theirs r%d; %s is the value, the other is in its history\n", c.Key, c.Ours, c.Theirs, c.Kept)
	}
	fmt.Fprintf(stdout, "next export from that store: --since %d\n", rep.Through)
	return nil
}

func cmdUpgrade(args []string, stdout io.Writer) error {
	p, err := parseArgs(args, flagSpec{})
	if err != nil {
		return usageError{err.Error()}
	}
	if len(p.pos) != 1 {
		return usagef("want FILE")
	}
	if _, err := os.Stat(p.pos[0]); err != nil {
		return err
	}
	db, err := xkv.Open(p.pos[0], nil)
	if err != nil {
		return err
	}
	before, _ := db.Stats()
	err = db.Upgrade(context.Background())
	after, _ := db.Stats()
	if cerr := db.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	if before.Format == after.Format {
		fmt.Fprintf(stdout, "already file format %d, store %s\n", after.Format, after.StoreID)
		return nil
	}
	fmt.Fprintf(stdout, "upgraded from file format %d to %d, every revision kept; store id %s\n", before.Format, after.Format, after.StoreID)
	return nil
}

func cmdNewID(args []string, stdout io.Writer) error {
	p, err := parseArgs(args, flagSpec{"keep-through": false})
	if err != nil {
		return usageError{err.Error()}
	}
	if len(p.pos) != 1 || !p.has("keep-through") {
		return usagef("want FILE --keep-through REV: the last revision the copy shares with the original (the store's last revision when it was copied). It can't be undone")
	}
	if _, err := os.Stat(p.pos[0]); err != nil {
		return err
	}
	db, err := xkv.Open(p.pos[0], nil)
	if err != nil {
		return err
	}
	before, _ := db.Stats()
	keep, err := parseRev(p.str("keep-through"))
	if err != nil {
		db.Close()
		return err
	}
	err = db.NewStoreID(context.Background(), keep)
	after, _ := db.Stats()
	if cerr := db.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "store id %s (was %s); revisions up to r%d keep their identity from the old id\n", after.StoreID, before.StoreID, keep)
	return nil
}
