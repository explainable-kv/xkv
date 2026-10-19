// Copyright xkv.org 2026
// SPDX-License-Identifier: Apache-2.0

//go:build js && wasm

// Command wasm runs an in-memory xkv store in the browser, for the demo on
// xkv.org. Build it with:
//
//	GOOS=js GOARCH=wasm go build -o xkv.wasm ./wasm
//
// and load it with the wasm_exec.js that ships with Go. It sets
// globalThis.xkv, whose functions take plain arguments and return JSON
// strings: {"ok": ...} on success, {"error": "..."} on failure.
//
//	xkv.put(key, value, reason, actor, causesJSON, ref)  causes: [{"key","rev"}], rev 0 = latest
//	xkv.del(key, reason, actor, causesJSON, ref)
//	xkv.get(key)
//	xkv.history(key, limit)
//	xkv.why(key, depth)
//	xkv.whyRev(rev, depth)
//	xkv.keys(prefix)
//	xkv.stats()
//	xkv.reset()
package main

import (
	"context"
	"encoding/json"
	"syscall/js"

	"github.com/explainable-kv/xkv"
)

var db *xkv.DB

func result(v any, err error) any {
	var out any
	if err != nil {
		out = map[string]string{"error": err.Error()}
	} else {
		out = map[string]any{"ok": v}
	}
	b, err := json.Marshal(out)
	if err != nil {
		b, _ = json.Marshal(map[string]string{"error": err.Error()})
	}
	return string(b)
}

func str(args []js.Value, i int) string {
	if i < len(args) && args[i].Type() == js.TypeString {
		return args[i].String()
	}
	return ""
}

func num(args []js.Value, i int) int {
	if i < len(args) && args[i].Type() == js.TypeNumber {
		return args[i].Int()
	}
	return 0
}

func writeOpts(reason, actor, causes, ref string) ([]xkv.WriteOption, error) {
	opts := []xkv.WriteOption{xkv.Reason(reason), xkv.Actor(actor), xkv.Ref(ref)}
	if causes != "" {
		var cs []xkv.CauseRef
		if err := json.Unmarshal([]byte(causes), &cs); err != nil {
			return nil, err
		}
		for _, c := range cs {
			if c.Rev == 0 {
				opts = append(opts, xkv.CauseLatest(c.Key))
			} else {
				opts = append(opts, xkv.Cause(c.Key, c.Rev))
			}
		}
	}
	return opts, nil
}

func reset() error {
	var err error
	db, err = xkv.OpenMemory(nil)
	return err
}

func main() {
	if err := reset(); err != nil {
		panic(err)
	}
	ctx := context.Background()
	api := map[string]any{
		"version": xkv.Version,
		"put": js.FuncOf(func(_ js.Value, a []js.Value) any {
			opts, err := writeOpts(str(a, 2), str(a, 3), str(a, 4), str(a, 5))
			if err != nil {
				return result(nil, err)
			}
			return result(db.Put(ctx, str(a, 0), []byte(str(a, 1)), opts...))
		}),
		"del": js.FuncOf(func(_ js.Value, a []js.Value) any {
			opts, err := writeOpts(str(a, 1), str(a, 2), str(a, 3), str(a, 4))
			if err != nil {
				return result(nil, err)
			}
			return result(db.Delete(ctx, str(a, 0), opts...))
		}),
		"get": js.FuncOf(func(_ js.Value, a []js.Value) any {
			return result(db.Get(ctx, str(a, 0)))
		}),
		"history": js.FuncOf(func(_ js.Value, a []js.Value) any {
			return result(db.History(ctx, str(a, 0), num(a, 1)))
		}),
		"why": js.FuncOf(func(_ js.Value, a []js.Value) any {
			return result(db.Why(ctx, str(a, 0), num(a, 1)))
		}),
		"whyRev": js.FuncOf(func(_ js.Value, a []js.Value) any {
			return result(db.WhyRev(ctx, uint64(num(a, 0)), num(a, 1)))
		}),
		"keys": js.FuncOf(func(_ js.Value, a []js.Value) any {
			return result(db.Keys(ctx, str(a, 0)))
		}),
		"stats": js.FuncOf(func(_ js.Value, a []js.Value) any {
			s, err := db.Stats()
			return result(map[string]any{"keys": s.Keys, "live_keys": s.LiveKeys, "revisions": s.Revisions, "last_rev": s.LastRev}, err)
		}),
		"reset": js.FuncOf(func(_ js.Value, a []js.Value) any {
			return result(true, reset())
		}),
	}
	js.Global().Set("xkv", js.ValueOf(api))
	// Tell the page the store is ready.
	if cb := js.Global().Get("xkvReady"); cb.Type() == js.TypeFunction {
		cb.Invoke()
	}
	select {}
}
