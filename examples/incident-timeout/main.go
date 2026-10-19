// Copyright xkv.org 2026
// SPDX-License-Identifier: Apache-2.0

// Incident timeout: during an incident the on-call engineer raises a database
// timeout to stop requests failing. Weeks later nobody remembers why it is 30
// seconds. The store does, and it can also say what the value was at any
// revision in between.
//
//	go run ./examples/incident-timeout
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
	db, err := xkv.Open(filepath.Join(dir, "config.xkv"), nil)
	if err != nil {
		return err
	}
	defer db.Close()
	bg := context.Background()

	cfg := xkv.WithActor(bg, "config-deploy")
	before, err := db.Put(cfg, "db.timeout", []byte("5s"), xkv.Reason("service default"))
	if err != nil {
		return err
	}

	// The alert that started the incident.
	alerts := xkv.WithActor(bg, "alertmanager")
	alert, err := db.Put(alerts, "alert:orders-db-latency", []byte("firing: p99 8.2s"),
		xkv.Reason("p99 above 5s for 10 minutes"), xkv.Ref("INC-4410"))
	if err != nil {
		return err
	}

	// On call raises the timeout to stop requests failing.
	oncall := xkv.WithActor(bg, "sam@oncall")
	if _, err := db.Put(oncall, "db.timeout", []byte("30s"),
		xkv.Reason("stop order requests timing out while the replica catches up"),
		xkv.Ref("INC-4410"), xkv.Cause(alert.Key, alert.Rev)); err != nil {
		return err
	}

	// The incident closes. Nobody lowers the timeout.
	if _, err := db.Put(alerts, "alert:orders-db-latency", []byte("resolved"),
		xkv.Reason("p99 back under 1s"), xkv.Ref("INC-4410"), xkv.CauseLatest("alert:orders-db-latency")); err != nil {
		return err
	}

	fmt.Fprintln(w, "Three weeks later: why is db.timeout 30s?")
	fmt.Fprintln(w)
	tree, err := db.Why(bg, "db.timeout", 0)
	if err != nil {
		return err
	}
	if err := tree.WriteText(w); err != nil {
		return err
	}
	old, err := db.GetAt(bg, "db.timeout", alert.Rev)
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "\nWhen the alert fired (r%d), db.timeout was %s, set at r%d.\n", alert.Rev, old.Value, before.Rev)
	now, err := db.Get(bg, "alert:orders-db-latency")
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "The alert is now %q: the timeout can go back.\n", now.Value)
	return nil
}
