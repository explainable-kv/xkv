// Copyright xkv.org 2026
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/explainable-kv/xkv"
)

// errWatchDone stops a watch after --count revisions.
var errWatchDone = errors.New("done")

func cmdWatch(args []string, stdout io.Writer) error {
	p, err := parseArgs(args, flagSpec{"since": false, "all": true, "prefix": false, "count": false, "json": true})
	if err != nil {
		return usageError{err.Error()}
	}
	if len(p.pos) != 1 {
		return usagef("want FILE")
	}
	if p.has("since") && p.bools["all"] {
		return usagef("--since and --all don't go together")
	}
	count := uint64(0)
	if p.has("count") {
		if count, err = parseUint("count", p.str("count")); err != nil {
			return err
		}
	}
	db, err := openRead(p.pos[0])
	if err != nil {
		return err
	}
	defer db.Close()
	opts := xkv.WatchOptions{Prefix: p.str("prefix"), Poll: 100 * time.Millisecond}
	switch {
	case p.bools["all"]:
		opts.SkipCompacted = true
	case p.has("since"):
		if opts.Since, err = parseRev(p.str("since")); err != nil {
			return err
		}
	default:
		st, err := db.Stats()
		if err != nil {
			return err
		}
		opts.Since = st.LastRev
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	enc := json.NewEncoder(stdout)
	n := uint64(0)
	err = db.Watch(ctx, opts, func(r xkv.Revision) error {
		if p.bools["json"] {
			if err := enc.Encode(r); err != nil {
				return err
			}
		} else if _, err := fmt.Fprintf(stdout, "%s  %s\n", r.Time.Local().Format("2006-01-02 15:04:05"), r); err != nil {
			return err
		}
		n++
		if count > 0 && n >= count {
			return errWatchDone
		}
		return nil
	})
	if errors.Is(err, errWatchDone) || errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}
