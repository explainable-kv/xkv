// Copyright xkv.org 2026
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/explainable-kv/xkv"
)

func xkvRun(t *testing.T, stdin string, args ...string) (string, string, int) {
	t.Helper()
	var out, errb bytes.Buffer
	code := run(args, strings.NewReader(stdin), &out, &errb)
	return out.String(), errb.String(), code
}

func TestCommandLine(t *testing.T) {
	f := filepath.Join(t.TempDir(), "c.xkv")
	steps := [][]string{
		{"put", f, "chargeback:cb_991", "card dispute filed", "--reason", "bank notice", "--ref", "BANK-7731", "--actor", "billing-worker"},
		{"put", f, "risk:42:score", "0.31", "--reason=monthly rescore", "--actor", "risk-model"},
		{"put", f, "risk:42:score", "0.82", "--reason", "new chargeback", "--cause", "chargeback:cb_991@1", "--actor", "risk-model"},
		{"put", f, "account:42:limit", "100", "--reason", "opening limit", "--actor", "limits-service"},
		{"put", f, "account:42:limit", "50", "--reason", "risk policy v3", "--cause", "risk:42:score", "--actor", "limits-service"},
	}
	for _, s := range steps {
		if _, e, code := xkvRun(t, "", s...); code != 0 {
			t.Fatalf("%v: %s", s, e)
		}
	}
	out, _, _ := xkvRun(t, "", "why", f, "account:42:limit")
	for _, want := range []string{
		"account:42:limit = 50", "was 100 · by limits-service · risk policy v3",
		"└─ risk:42:score = 0.82", "was 0.31 · by risk-model · new chargeback",
		"└─ chargeback:cb_991 = card dispute filed", "by billing-worker · bank notice · ref BANK-7731",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("why output lacks %q:\n%s", want, out)
		}
	}
	out, _, _ = xkvRun(t, "", "why", f, "--rev", "3", "--json")
	var tree struct {
		Revision struct{ Key string }
		Causes   []struct{ Revision struct{ Key string } }
	}
	if err := json.Unmarshal([]byte(out), &tree); err != nil || tree.Revision.Key != "risk:42:score" || tree.Causes[0].Revision.Key != "chargeback:cb_991" {
		t.Fatalf("why --json: %v\n%s", err, out)
	}
	if out, _, _ := xkvRun(t, "", "get", f, "account:42:limit"); out != "50\n" {
		t.Fatalf("get: %q", out)
	}
	if out, _, _ := xkvRun(t, "", "get", f, "account:42:limit", "--at", "4"); out != "100\n" {
		t.Fatalf("get --at: %q", out)
	}
	if out, _, _ := xkvRun(t, "", "log", f, "risk:42:score", "--limit", "1"); !strings.Contains(out, "caused by chargeback:cb_991@1") || strings.Contains(out, "0.31") {
		t.Fatalf("log: %s", out)
	}
	if out, _, _ := xkvRun(t, "", "keys", f, "risk:"); out != "risk:42:score\n" {
		t.Fatalf("keys: %q", out)
	}
	if _, e, code := xkvRun(t, "", "put", f, "x", "y"); code != 1 || !strings.Contains(e, "reason is required") {
		t.Fatalf("no reason: %d %s", code, e)
	}
	if _, _, code := xkvRun(t, "", "put", f, "x", "y", "--relaxed"); code != 0 {
		t.Fatal("relaxed put failed")
	}
	if _, e, code := xkvRun(t, "", "put", f, "x", "y", "--reason", "r", "--cause", "risk:42:score@1"); code != 1 || !strings.Contains(e, "belongs to") {
		t.Fatalf("bad cause: %d %s", code, e)
	}
	if _, _, code := xkvRun(t, "", "put", f, "bin", "--stdin", "--reason", "from stdin"); code != 0 {
		t.Fatal("stdin put")
	}
	if out, _, _ := xkvRun(t, "", "rev", f, "7"); !strings.Contains(out, "bin = ") {
		t.Fatalf("rev: %s", out)
	}
	if _, _, code := xkvRun(t, "", "del", f, "x", "--reason", "cleanup"); code != 0 {
		t.Fatal("del")
	}
	if _, _, code := xkvRun(t, "", "get", f, "x"); code != 3 {
		t.Fatalf("get after del: %d", code)
	}
	if out, _, code := xkvRun(t, "", "check", f); code != 0 || !strings.Contains(out, "ok:") {
		t.Fatalf("check: %d %s", code, out)
	}
	if out, _, code := xkvRun(t, "", "compact", f); code != 0 || !strings.Contains(out, "revisions ->") {
		t.Fatalf("compact: %d %s", code, out)
	}
	out, _, _ = xkvRun(t, "", "why", f, "account:42:limit")
	if !strings.Contains(out, "chargeback:cb_991 = card dispute filed") {
		t.Fatalf("chain lost after compaction:\n%s", out)
	}
	if _, _, code := xkvRun(t, "", "bogus"); code != 2 {
		t.Fatal("unknown command")
	}
	if _, _, code := xkvRun(t, "", "put", f, "k", "v", "--nope"); code != 2 {
		t.Fatal("unknown flag")
	}
	if out, _, _ := xkvRun(t, "", "version"); !strings.HasPrefix(out, "xkv "+xkv.Version) {
		t.Fatal(out)
	}
}

func TestParseCause(t *testing.T) {
	db, _ := xkv.OpenMemory(nil)
	ctx := t.Context()
	a, _ := db.Put(ctx, "user@example.com", []byte("1"), xkv.Reason("r"))
	db.Put(ctx, "user@example.com", []byte("2"), xkv.Reason("r"))
	for spec, want := range map[string]uint64{
		"user@example.com@1": a.Rev, // revision after the last '@'
		"user@example.com":   2,     // no numeric suffix: latest
	} {
		o, err := parseCause(spec)
		if err != nil {
			t.Fatal(err)
		}
		r, err := db.Put(ctx, "t", nil, xkv.Reason("r"), o)
		if err != nil || r.Causes[0].Rev != want || r.Causes[0].Key != "user@example.com" {
			t.Fatalf("%s: %+v %v", spec, r.Causes, err)
		}
	}
}

func TestHTTP(t *testing.T) {
	db, err := xkv.Open(filepath.Join(t.TempDir(), "h.xkv"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	srv := httptest.NewServer(newHandler(db))
	defer srv.Close()
	do := func(method, path, body string, hdr ...string) (int, map[string]any) {
		t.Helper()
		req, _ := http.NewRequest(method, srv.URL+path, strings.NewReader(body))
		for i := 0; i+1 < len(hdr); i += 2 {
			req.Header.Set(hdr[i], hdr[i+1])
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var m map[string]any
		json.NewDecoder(resp.Body).Decode(&m)
		return resp.StatusCode, m
	}
	code, m := do("PUT", "/v1/keys/config/db/timeout", `{"value":"30s","reason":"slow replica","ref":"INC-4410"}`, "X-Xkv-Actor", "oncall")
	if code != 200 || m["actor"] != "oncall" || m["rev"] != 1.0 {
		t.Fatalf("put: %d %v", code, m)
	}
	code, m = do("PUT", "/v1/keys/config/db/pool", `{"value":"//4=","encoding":"base64","reason":"follows timeout","causes":[{"key":"config/db/timeout","rev":0}]}`)
	if code != 200 || m["actor"] != "http" || m["encoding"] != "base64" {
		t.Fatalf("put base64 with latest cause: %d %v", code, m)
	}
	if code, m = do("GET", "/v1/keys/config/db/timeout", ""); code != 200 || m["value"] != "30s" {
		t.Fatalf("get: %d %v", code, m)
	}
	if code, m = do("GET", "/v1/why/config/db/pool", ""); code != 200 || len(m["causes"].([]any)) != 1 {
		t.Fatalf("why: %d %v", code, m)
	}
	if code, _ = do("PUT", "/v1/keys/x", `{"value":"1"}`); code != 400 {
		t.Fatalf("no reason: %d", code)
	}
	if code, _ = do("PUT", "/v1/keys/x", `{"value":"1","reason":"r","causes":[{"key":"nope","rev":9}]}`); code != 400 {
		t.Fatalf("bad cause: %d", code)
	}
	if code, _ = do("GET", "/v1/keys/nope", ""); code != 404 {
		t.Fatalf("missing: %d", code)
	}
	if code, _ = do("DELETE", "/v1/keys/config/db/pool", `{"reason":"rollback"}`); code != 200 {
		t.Fatalf("delete: %d", code)
	}
	if code, m = do("GET", "/v1/keys?prefix=config/", ""); code != 200 || len(m["keys"].([]any)) != 1 {
		t.Fatalf("keys: %d %v", code, m)
	}
	resp, err := http.Get(srv.URL + "/v1/history/config/db/pool")
	if err != nil {
		t.Fatal(err)
	}
	var h []map[string]any
	json.NewDecoder(resp.Body).Decode(&h)
	resp.Body.Close()
	if len(h) != 2 || h[0]["deleted"] != true {
		t.Fatalf("history: %v", h)
	}
	if code, m = do("GET", "/v1/revs/1/why?depth=2", ""); code != 200 || m["revision"].(map[string]any)["ref"] != "INC-4410" {
		t.Fatalf("rev why: %d %v", code, m)
	}
	if code, m = do("GET", "/v1/stats", ""); code != 200 || m["revisions"] != 3.0 {
		t.Fatalf("stats: %d %v", code, m)
	}
}
