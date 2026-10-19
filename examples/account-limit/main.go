// Copyright xkv.org 2026
// SPDX-License-Identifier: Apache-2.0

// Account limit: a customer's credit limit drops from 100 to 50. Three
// services each did one reasonable thing, and the reason only shows when the
// chain is read across all three keys.
//
//	go run ./examples/account-limit
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
	db, err := xkv.Open(filepath.Join(dir, "limits.xkv"), nil)
	if err != nil {
		return err
	}
	defer db.Close()
	bg := context.Background()

	// The limits service sets the opening limit.
	limits := xkv.WithActor(bg, "limits-service")
	if _, err := db.Put(limits, "account:42:limit", []byte("100"), xkv.Reason("opening limit")); err != nil {
		return err
	}

	// The risk model scores the account every month.
	risk := xkv.WithActor(bg, "risk-model")
	if _, err := db.Put(risk, "risk:42:score", []byte("0.31"), xkv.Reason("monthly rescore")); err != nil {
		return err
	}

	// Billing records a chargeback that came in from the bank.
	billing := xkv.WithActor(bg, "billing-worker")
	cb, err := db.Put(billing, "chargeback:cb_991", []byte("card dispute filed"),
		xkv.Reason("bank notice"), xkv.Ref("BANK-7731"))
	if err != nil {
		return err
	}

	// The risk model reacts to the chargeback, and says so.
	if _, err := db.Put(risk, "risk:42:score", []byte("0.82"),
		xkv.Reason("new chargeback"), xkv.Cause(cb.Key, cb.Rev)); err != nil {
		return err
	}

	// The limits service applies its policy to the new score.
	if _, err := db.Put(limits, "account:42:limit", []byte("50"),
		xkv.Reason("risk policy v3"), xkv.CauseLatest("risk:42:score")); err != nil {
		return err
	}

	// Support asks: why is this customer's limit 50?
	tree, err := db.Why(bg, "account:42:limit", 0)
	if err != nil {
		return err
	}
	fmt.Fprintln(w, "Why is account 42's limit 50?")
	fmt.Fprintln(w)
	return tree.WriteText(w)
}
