// Copyright xkv.org 2026
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"io"

	"github.com/explainable-kv/xkv"
)

func cmdWhy(args []string, stdout io.Writer) error {
	p, err := parseArgs(args, flagSpec{"depth": false, "rev": false, "json": true, "prov": true})
	if err != nil {
		return usageError{err.Error()}
	}
	byRev := p.has("rev")
	if (byRev && len(p.pos) != 1) || (!byRev && len(p.pos) != 2) {
		return usagef("want FILE KEY, or FILE --rev REV")
	}
	depth := 0
	if p.has("depth") {
		d, err := parseUint("depth", p.str("depth"))
		if err != nil {
			return err
		}
		depth = int(d)
	}
	db, err := openRead(p.pos[0])
	if err != nil {
		return err
	}
	defer db.Close()
	var n *xkv.WhyNode
	if byRev {
		rev, err := parseUint("rev", p.str("rev"))
		if err != nil {
			return err
		}
		n, err = db.WhyRev(context.Background(), rev, depth)
		if err != nil {
			return err
		}
	} else {
		n, err = db.Why(context.Background(), p.pos[1], depth)
		if err != nil {
			return err
		}
	}
	if p.bools["prov"] {
		if p.bools["json"] {
			return usagef("--prov and --json don't go together")
		}
		id, ok := db.StoreID()
		if !ok {
			return fmt.Errorf("this store is in file format 1 and has no store id for PROV names; run xkv upgrade first")
		}
		b, err := n.PROV(id)
		if err != nil {
			return err
		}
		_, err = stdout.Write(b)
		return err
	}
	if p.bools["json"] {
		return writeJSON(stdout, n)
	}
	return n.WriteText(stdout)
}
