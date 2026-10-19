// Copyright xkv.org 2026
// SPDX-License-Identifier: Apache-2.0

// Agent setting: an AI agent lowers a cache TTL after reading a metric, and
// records the tool call that did it. An engineer later puts it back, pointing
// at the agent's change. Both people and agents leave the same kind of trail.
//
//	go run ./examples/agent-setting
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
	db, err := xkv.Open(filepath.Join(dir, "settings.xkv"), nil)
	if err != nil {
		return err
	}
	defer db.Close()
	bg := context.Background()

	ops := xkv.WithActor(bg, "platform-team")
	if _, err := db.Put(ops, "cache:catalog:ttl", []byte("300s"), xkv.Reason("default from the runbook")); err != nil {
		return err
	}

	// The monitoring job records what it saw.
	mon := xkv.WithActor(bg, "metrics-collector")
	stale, err := db.Put(mon, "observed:catalog:stale-reads", []byte("4.1% over the last hour"),
		xkv.Reason("hourly sample"))
	if err != nil {
		return err
	}

	// The agent acts on the observation. Its tool call id goes in Ref, so the
	// agent's own log can be matched to the change.
	agent := xkv.WithActor(bg, "ops-agent")
	change, err := db.Put(agent, "cache:catalog:ttl", []byte("30s"),
		xkv.Reason("stale reads above the 2% target; shorter TTL"),
		xkv.Ref("toolcall_01J9ZK4T3M"),
		xkv.Cause(stale.Key, stale.Rev))
	if err != nil {
		return err
	}

	// The database load goes up, and an engineer reverts, pointing at both
	// the agent's change and what it caused.
	load, err := db.Put(mon, "observed:catalog-db:cpu", []byte("91% average"),
		xkv.Reason("hourly sample"))
	if err != nil {
		return err
	}
	eng := xkv.WithActor(bg, "dana@platform")
	if _, err := db.Put(eng, "cache:catalog:ttl", []byte("300s"),
		xkv.Reason("revert: 30s TTL tripled catalog DB load"),
		xkv.Ref("INC-4512"),
		xkv.Cause(change.Key, change.Rev), xkv.Cause(load.Key, load.Rev)); err != nil {
		return err
	}

	fmt.Fprintln(w, "Why is the catalog cache TTL 300s again?")
	fmt.Fprintln(w)
	tree, err := db.Why(bg, "cache:catalog:ttl", 0)
	if err != nil {
		return err
	}
	return tree.WriteText(w)
}
