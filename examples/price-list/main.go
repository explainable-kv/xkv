// Copyright xkv.org 2026
// SPDX-License-Identifier: Apache-2.0

// Price list: the Q4 prices go out as one transaction with one reason and one
// ticket. A week later the Pro price is corrected after a complaint. Each
// price can say where it came from, and the history shows both steps.
//
//	go run ./examples/price-list
package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/explainable-kv/xkv"
)

func main() {
	if err := run(os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(w io.Writer) error {
	dir, err := os.MkdirTemp("", "xkv-example")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	db, err := xkv.Open(filepath.Join(dir, "prices.xkv"), nil)
	if err != nil {
		return err
	}
	defer db.Close()
	ctx := xkv.WithActor(context.Background(), "pricing-team")

	// The finance decision the prices follow.
	decision, err := db.Put(ctx, "decision:q4-pricing", []byte("raise list prices 10%, hold Basic"),
		xkv.Reason("Q4 planning meeting"), xkv.Ref("FIN-208"))
	if err != nil {
		return err
	}

	// Every price in one transaction: all of them change, or none does.
	// The reason, ref and cause given to Update apply to each write inside.
	err = db.Update(ctx, func(tx *xkv.Tx) error {
		for _, p := range [][2]string{
			{"price:basic", "9"},
			{"price:pro", "33"},
			{"price:team", "66"},
		} {
			if _, err := tx.Put(p[0], []byte(p[1])); err != nil {
				return err
			}
		}
		return nil
	}, xkv.Reason("Q4 price list"), xkv.Ref("PRICE-12"), xkv.Cause(decision.Key, decision.Rev))
	if err != nil {
		return err
	}

	// A complaint comes in, and Pro is corrected.
	complaint, err := db.Put(xkv.WithActor(ctx, "support"), "ticket:SUP-5521",
		[]byte("Pro now costs more than two Basic seats"), xkv.Reason("customer complaint"))
	if err != nil {
		return err
	}
	if _, err := db.Put(ctx, "price:pro", []byte("29"),
		xkv.Reason("keep Pro under two Basic seats"),
		xkv.Cause(complaint.Key, complaint.Rev),
		xkv.CauseLatest("price:pro")); err != nil {
		return err
	}

	fmt.Fprintln(w, "Why does Pro cost 29?")
	fmt.Fprintln(w)
	tree, err := db.Why(ctx, "price:pro", 0)
	if err != nil {
		return err
	}
	if err := tree.WriteText(w); err != nil {
		return err
	}
	fmt.Fprintln(w)
	fmt.Fprintln(w, "price:pro, newest first:")
	history, err := db.History(ctx, "price:pro", 0)
	if err != nil {
		return err
	}
	for _, r := range history {
		fmt.Fprintln(w, " ", r)
	}
	return nil
}
