// Copyright xkv.org 2026
// SPDX-License-Identifier: Apache-2.0

// Command xkv reads and writes an xkv store from the command line, and can
// serve it over HTTP.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/explainable-kv/xkv"
)

const usage = `xkv %s: a key-value store that remembers why

Usage:
  xkv put FILE KEY VALUE --reason TEXT [--cause KEY[@REV]]... [--actor NAME] [--ref REF]
  xkv put FILE KEY --stdin --reason TEXT ...     read the value from standard input
  xkv del FILE KEY --reason TEXT [--cause KEY[@REV]]... [--actor NAME] [--ref REF]
  xkv get FILE KEY [--at REV] [--json]
  xkv log FILE KEY [--limit N] [--json]           a key's revisions, newest first
  xkv why FILE KEY [--depth N] [--json]           the chain of causes behind a value
  xkv why FILE --rev REV [--depth N] [--json]
  xkv rev FILE REV [--json]                       one revision by number
  xkv keys FILE [PREFIX]                          keys that have a value
  xkv stats FILE [--json]
  xkv check FILE                                  read every record and cause
  xkv compact FILE [--keep N] [--keep-days DAYS]
  xkv repair FILE                                 trim a damaged end, keeping a copy
  xkv serve FILE [--addr 127.0.0.1:7070] [--relaxed]
  xkv version

A cause is KEY@REV for one revision, or KEY alone for the key's current
revision. Writes need a reason unless --relaxed is given. The actor is
--actor, else $XKV_ACTOR, else your user name.

Read commands open the file read-only, so they work while another process
(such as xkv serve) has it open for writing.
`

func main() {
	os.Exit(run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
}

// flagSpec says which flags a command takes; true marks a boolean flag.
type flagSpec map[string]bool

type parsed struct {
	pos   []string
	vals  map[string][]string
	bools map[string]bool
}

func (p parsed) str(name string) string {
	if v := p.vals[name]; len(v) > 0 {
		return v[len(v)-1]
	}
	return ""
}

func (p parsed) has(name string) bool { return len(p.vals[name]) > 0 }

func parseArgs(args []string, spec flagSpec) (parsed, error) {
	p := parsed{vals: map[string][]string{}, bools: map[string]bool{}}
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--" {
			p.pos = append(p.pos, args[i+1:]...)
			break
		}
		if !strings.HasPrefix(a, "--") || a == "--" {
			p.pos = append(p.pos, a)
			continue
		}
		name, val, hasVal := strings.Cut(a[2:], "=")
		isBool, ok := spec[name]
		if !ok {
			return p, fmt.Errorf("unknown flag --%s", name)
		}
		if isBool {
			if hasVal {
				b, err := strconv.ParseBool(val)
				if err != nil {
					return p, fmt.Errorf("--%s takes true or false", name)
				}
				p.bools[name] = b
			} else {
				p.bools[name] = true
			}
			continue
		}
		if !hasVal {
			if i+1 >= len(args) {
				return p, fmt.Errorf("--%s needs a value", name)
			}
			i++
			val = args[i]
		}
		p.vals[name] = append(p.vals[name], val)
	}
	return p, nil
}

var writeFlags = flagSpec{"reason": false, "cause": false, "actor": false, "ref": false, "relaxed": true, "stdin": true, "json": true, "no-sync": true}

func run(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintf(stderr, usage, xkv.Version)
		return 2
	}
	cmd, rest := args[0], args[1:]
	var err error
	switch cmd {
	case "put":
		err = cmdWrite(rest, stdin, stdout, false)
	case "del", "delete":
		err = cmdWrite(rest, stdin, stdout, true)
	case "get":
		err = cmdGet(rest, stdout)
	case "log", "history":
		err = cmdLog(rest, stdout)
	case "why":
		err = cmdWhy(rest, stdout)
	case "rev":
		err = cmdRev(rest, stdout)
	case "keys":
		err = cmdKeys(rest, stdout)
	case "stats":
		err = cmdStats(rest, stdout)
	case "check":
		return cmdCheck(rest, stdout, stderr)
	case "compact":
		err = cmdCompact(rest, stdout)
	case "repair":
		err = cmdRepair(rest, stdout)
	case "serve":
		err = cmdServe(rest, stdout, stderr)
	case "version", "--version", "-v":
		fmt.Fprintf(stdout, "xkv %s (file format %d)\n", xkv.Version, xkv.FormatVersion)
	case "help", "--help", "-h":
		fmt.Fprintf(stdout, usage, xkv.Version)
	default:
		fmt.Fprintf(stderr, "xkv: unknown command %q\n\n", cmd)
		fmt.Fprintf(stderr, usage, xkv.Version)
		return 2
	}
	if err != nil {
		msg := strings.TrimPrefix(err.Error(), "xkv: ")
		fmt.Fprintf(stderr, "xkv %s: %s\n", cmd, msg)
		var ue usageError
		if errors.As(err, &ue) {
			return 2
		}
		if errors.Is(err, xkv.ErrNotFound) {
			return 3
		}
		return 1
	}
	return 0
}

type usageError struct{ msg string }

func (e usageError) Error() string { return e.msg }

func usagef(format string, a ...any) error { return usageError{fmt.Sprintf(format, a...)} }

func defaultActor() string {
	if a := os.Getenv("XKV_ACTOR"); a != "" {
		return a
	}
	for _, v := range []string{"USER", "USERNAME", "LOGNAME"} {
		if a := os.Getenv(v); a != "" {
			return a
		}
	}
	return ""
}

// parseCause reads KEY@REV, or KEY for the key's latest revision. A key may
// itself contain '@': only a numeric suffix after the last '@' is a revision.
func parseCause(s string) (xkv.WriteOption, error) {
	if i := strings.LastIndexByte(s, '@'); i > 0 {
		if rev, err := strconv.ParseUint(s[i+1:], 10, 64); err == nil {
			return xkv.Cause(s[:i], rev), nil
		}
	}
	if s == "" {
		return nil, usagef("--cause needs KEY or KEY@REV")
	}
	return xkv.CauseLatest(s), nil
}

func cmdWrite(args []string, stdin io.Reader, stdout io.Writer, del bool) error {
	p, err := parseArgs(args, writeFlags)
	if err != nil {
		return usageError{err.Error()}
	}
	want := 3
	if del || p.bools["stdin"] {
		want = 2
	}
	if len(p.pos) != want {
		if del {
			return usagef("want FILE KEY")
		}
		return usagef("want FILE KEY VALUE, or FILE KEY --stdin")
	}
	var value []byte
	if !del {
		if p.bools["stdin"] {
			value, err = io.ReadAll(io.LimitReader(stdin, xkv.MaxValueSize+1))
			if err != nil {
				return err
			}
		} else {
			value = []byte(p.pos[2])
		}
	}
	opts := []xkv.WriteOption{xkv.Ref(p.str("ref"))}
	if p.has("reason") {
		opts = append(opts, xkv.Reason(p.str("reason")))
	}
	if p.has("actor") {
		opts = append(opts, xkv.Actor(p.str("actor")))
	}
	for _, c := range p.vals["cause"] {
		o, err := parseCause(c)
		if err != nil {
			return err
		}
		opts = append(opts, o)
	}
	db, err := xkv.Open(p.pos[0], &xkv.Options{Relaxed: p.bools["relaxed"], NoSync: p.bools["no-sync"], DefaultActor: defaultActor()})
	if err != nil {
		return err
	}
	ctx := context.Background()
	var r xkv.Revision
	if del {
		r, err = db.Delete(ctx, p.pos[1], opts...)
	} else {
		r, err = db.Put(ctx, p.pos[1], value, opts...)
	}
	if cerr := db.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	if p.bools["json"] {
		return writeJSON(stdout, r)
	}
	verb := "set"
	if del {
		verb = "deleted"
	}
	fmt.Fprintf(stdout, "%s %s at r%d\n", verb, r.Key, r.Rev)
	return nil
}

func openRead(path string) (*xkv.DB, error) {
	if _, err := os.Stat(path); err != nil {
		return nil, err
	}
	return xkv.Open(path, &xkv.Options{ReadOnly: true})
}

// parseUint reads a non-negative whole number that fits an int on every
// platform.
func parseUint(name, s string) (uint64, error) {
	v, err := strconv.ParseUint(s, 10, 31)
	if err != nil {
		return 0, usagef("--%s takes a whole number, not %q", name, s)
	}
	return v, nil
}

func parseRev(s string) (uint64, error) {
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, usagef("a revision is a whole number, not %q", s)
	}
	return v, nil
}

func cmdGet(args []string, stdout io.Writer) error {
	p, err := parseArgs(args, flagSpec{"at": false, "json": true})
	if err != nil {
		return usageError{err.Error()}
	}
	if len(p.pos) != 2 {
		return usagef("want FILE KEY")
	}
	db, err := openRead(p.pos[0])
	if err != nil {
		return err
	}
	defer db.Close()
	var r xkv.Revision
	if p.has("at") {
		at, err := parseRev(p.str("at"))
		if err != nil {
			return err
		}
		r, err = db.GetAt(context.Background(), p.pos[1], at)
		if err != nil {
			return err
		}
	} else {
		r, err = db.Get(context.Background(), p.pos[1])
		if err != nil {
			return err
		}
	}
	if p.bools["json"] {
		return writeJSON(stdout, r)
	}
	stdout.Write(r.Value)
	if len(r.Value) > 0 && r.Value[len(r.Value)-1] != '\n' {
		fmt.Fprintln(stdout)
	}
	return nil
}

func cmdLog(args []string, stdout io.Writer) error {
	p, err := parseArgs(args, flagSpec{"limit": false, "json": true})
	if err != nil {
		return usageError{err.Error()}
	}
	if len(p.pos) != 2 {
		return usagef("want FILE KEY")
	}
	limit := 0
	if p.has("limit") {
		n, err := parseUint("limit", p.str("limit"))
		if err != nil {
			return err
		}
		limit = int(n)
	}
	db, err := openRead(p.pos[0])
	if err != nil {
		return err
	}
	defer db.Close()
	h, err := db.History(context.Background(), p.pos[1], limit)
	if err != nil {
		return err
	}
	if p.bools["json"] {
		return writeJSON(stdout, h)
	}
	for _, r := range h {
		fmt.Fprintf(stdout, "%s  %s\n", r.Time.Local().Format("2006-01-02 15:04:05"), r)
	}
	return nil
}

func cmdRev(args []string, stdout io.Writer) error {
	p, err := parseArgs(args, flagSpec{"json": true})
	if err != nil {
		return usageError{err.Error()}
	}
	if len(p.pos) != 2 {
		return usagef("want FILE REV")
	}
	rev, err := parseRev(p.pos[1])
	if err != nil {
		return err
	}
	db, err := openRead(p.pos[0])
	if err != nil {
		return err
	}
	defer db.Close()
	r, err := db.Revision(context.Background(), rev)
	if err != nil {
		return err
	}
	if p.bools["json"] {
		return writeJSON(stdout, r)
	}
	fmt.Fprintf(stdout, "%s\n  tx %d · %s\n", r, r.Tx, r.Time.Local().Format(time.RFC3339))
	return nil
}

func cmdKeys(args []string, stdout io.Writer) error {
	p, err := parseArgs(args, flagSpec{"json": true})
	if err != nil {
		return usageError{err.Error()}
	}
	if len(p.pos) < 1 || len(p.pos) > 2 {
		return usagef("want FILE [PREFIX]")
	}
	prefix := ""
	if len(p.pos) == 2 {
		prefix = p.pos[1]
	}
	db, err := openRead(p.pos[0])
	if err != nil {
		return err
	}
	defer db.Close()
	keys, err := db.Keys(context.Background(), prefix)
	if err != nil {
		return err
	}
	if p.bools["json"] {
		if keys == nil {
			keys = []string{}
		}
		return writeJSON(stdout, keys)
	}
	for _, k := range keys {
		fmt.Fprintln(stdout, k)
	}
	return nil
}

func cmdStats(args []string, stdout io.Writer) error {
	p, err := parseArgs(args, flagSpec{"json": true})
	if err != nil {
		return usageError{err.Error()}
	}
	if len(p.pos) != 1 {
		return usagef("want FILE")
	}
	db, err := openRead(p.pos[0])
	if err != nil {
		return err
	}
	defer db.Close()
	s, err := db.Stats()
	if err != nil {
		return err
	}
	if p.bools["json"] {
		return writeJSON(stdout, map[string]any{
			"keys": s.Keys, "live_keys": s.LiveKeys, "revisions": s.Revisions,
			"last_rev": s.LastRev, "last_tx": s.LastTx, "file_size": s.FileSize,
		})
	}
	fmt.Fprintf(stdout, "keys:       %d (%d with a value)\nrevisions:  %d (last r%d, %d transactions)\nfile size:  %d bytes\n",
		s.Keys, s.LiveKeys, s.Revisions, s.LastRev, s.LastTx, s.FileSize)
	return nil
}

func cmdCheck(args []string, stdout, stderr io.Writer) int {
	p, err := parseArgs(args, flagSpec{})
	if err != nil || len(p.pos) != 1 {
		fmt.Fprintln(stderr, "xkv check: want FILE")
		return 2
	}
	rep, err := xkv.Check(p.pos[0])
	if err != nil {
		fmt.Fprintf(stderr, "xkv check: %v\n", err)
		return 1
	}
	fmt.Fprintf(stdout, "%d revisions of %d keys in %d transactions, last r%d\n", rep.Revisions, rep.Keys, rep.Transactions, rep.LastRev)
	if rep.TailBytes > 0 {
		fmt.Fprintf(stdout, "%d bytes after the last complete transaction (left by a crash; the next write trims them)\n", rep.TailBytes)
	}
	if rep.OK() {
		fmt.Fprintln(stdout, "ok: every revision reads back whole and every cause resolves")
		return 0
	}
	for _, pr := range rep.Problems {
		fmt.Fprintln(stdout, "problem:", pr)
	}
	return 1
}

func cmdCompact(args []string, stdout io.Writer) error {
	p, err := parseArgs(args, flagSpec{"keep": false, "keep-days": false, "json": true})
	if err != nil {
		return usageError{err.Error()}
	}
	if len(p.pos) != 1 {
		return usagef("want FILE")
	}
	var opts xkv.CompactOptions
	if p.has("keep") {
		n, err := parseUint("keep", p.str("keep"))
		if err != nil {
			return err
		}
		opts.KeepLast = int(n)
	}
	if p.has("keep-days") {
		d, err := strconv.ParseFloat(p.str("keep-days"), 64)
		if err != nil || d < 0 || d > 100000 || d != d {
			return usagef("--keep-days takes a number of days, up to 100000")
		}
		opts.KeepSince = time.Now().Add(-time.Duration(d * 24 * float64(time.Hour)))
	}
	db, err := xkv.Open(p.pos[0], nil)
	if err != nil {
		return err
	}
	st, err := db.Compact(context.Background(), opts)
	if cerr := db.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	if p.bools["json"] {
		return writeJSON(stdout, map[string]any{
			"revisions_before": st.RevisionsBefore, "revisions_after": st.RevisionsAfter,
			"kept_for_causes": st.KeptForCauses, "bytes_before": st.BytesBefore, "bytes_after": st.BytesAfter,
		})
	}
	fmt.Fprintf(stdout, "%d revisions -> %d (%d kept because a kept revision names them as a cause)\n%d bytes -> %d\n",
		st.RevisionsBefore, st.RevisionsAfter, st.KeptForCauses, st.BytesBefore, st.BytesAfter)
	return nil
}

func cmdRepair(args []string, stdout io.Writer) error {
	p, err := parseArgs(args, flagSpec{})
	if err != nil {
		return usageError{err.Error()}
	}
	if len(p.pos) != 1 {
		return usagef("want FILE")
	}
	rep, err := xkv.Check(p.pos[0])
	if err != nil {
		return err
	}
	if rep.OK() && rep.TailBytes == 0 {
		fmt.Fprintln(stdout, "nothing to repair: every revision reads back whole and every cause resolves")
		return nil
	}
	for _, pr := range rep.Problems {
		fmt.Fprintln(stdout, "found:", pr)
	}
	db, err := xkv.Open(p.pos[0], &xkv.Options{Repair: true})
	if err != nil {
		return err
	}
	st, _ := db.Stats()
	trimmed := db.Trimmed()
	if err := db.Close(); err != nil {
		return err
	}
	if trimmed != "" {
		fmt.Fprintf(stdout, "trimmed the file after its last whole transaction; the bytes cut off are in %s\n", trimmed)
	}
	fmt.Fprintf(stdout, "the store now holds %d revisions, the last r%d\n", st.Revisions, st.LastRev)
	after, err := xkv.Check(p.pos[0])
	if err != nil {
		return err
	}
	for _, pr := range after.Problems {
		fmt.Fprintln(stdout, "still:", pr)
	}
	if !after.OK() {
		return fmt.Errorf("problems remain that trimming the end can't fix")
	}
	return nil
}

func writeJSON(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}
