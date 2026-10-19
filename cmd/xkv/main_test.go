// Copyright xkv.org 2026
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

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
	if code, _ = do("PUT", "/v1/keys/x", `{"value":"1","reasons":"typo"}`); code != 400 {
		t.Fatalf("unknown field: %d", code)
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

func TestRepairCommand(t *testing.T) {
	f := filepath.Join(t.TempDir(), "r.xkv")
	for i := 0; i < 3; i++ {
		if _, e, code := xkvRun(t, "", "put", f, "k", "v", "--reason", "r"); code != 0 {
			t.Fatal(e)
		}
	}
	if out, _, code := xkvRun(t, "", "repair", f); code != 0 || !strings.Contains(out, "nothing to repair") {
		t.Fatalf("repair on a sound file: %d %s", code, out)
	}
	b, _ := os.ReadFile(f)
	os.Remove(f + ".idx")
	// Damage the first revision: whole transactions follow, so Open refuses.
	b[56] ^= 0xff
	os.WriteFile(f, b, 0o644)
	if _, e, code := xkvRun(t, "", "get", f, "k"); code != 1 || !strings.Contains(e, "damaged") {
		t.Fatalf("get on a damaged file: %d %s", code, e)
	}
	out, e, code := xkvRun(t, "", "repair", f)
	if code != 0 || !strings.Contains(out, "trimmed") || !strings.Contains(out, "0 revisions") {
		t.Fatalf("repair: %d %s %s", code, out, e)
	}
	matches, _ := filepath.Glob(f + ".trimmed-*")
	if len(matches) != 1 {
		t.Fatalf("trimmed bytes not kept: %v", matches)
	}
}

func TestExchangeCommands(t *testing.T) {
	dir := t.TempDir()
	a, b := filepath.Join(dir, "a.xkv"), filepath.Join(dir, "b.xkv")
	ex := filepath.Join(dir, "a-changes.xkvx")
	put := func(f string, args ...string) {
		t.Helper()
		if _, e, code := xkvRun(t, "", append([]string{"put", f}, args...)...); code != 0 {
			t.Fatal(e)
		}
	}
	put(a, "chargeback:cb_991", "card dispute filed", "--reason", "bank notice", "--actor", "billing-worker")
	put(a, "risk:42:score", "0.82", "--reason", "new chargeback", "--cause", "chargeback:cb_991", "--actor", "risk-model")
	out, e, code := xkvRun(t, "", "export", a, "--out", ex)
	if code != 0 || !strings.Contains(out, "exported 2 revisions") || !strings.Contains(out, "--since 2") {
		t.Fatalf("export: %d %s %s", code, out, e)
	}
	out, e, code = xkvRun(t, "", "import", b, ex)
	if code != 0 || !strings.Contains(out, "2 imported") {
		t.Fatalf("import: %d %s %s", code, out, e)
	}
	out, _, _ = xkvRun(t, "", "why", b, "risk:42:score")
	if !strings.Contains(out, "card dispute filed") || !strings.Contains(out, "imported from") {
		t.Fatalf("why after import:\n%s", out)
	}
	// Both sides change the same key; the import settles it and says so.
	put(a, "risk:42:score", "0.9", "--reason", "second chargeback", "--actor", "risk-model")
	put(b, "risk:42:score", "0.5", "--reason", "manual review", "--actor", "analyst")
	exStdout, _, code := xkvRun(t, "", "export", a, "--since", "2")
	if code != 0 {
		t.Fatal("export to stdout failed")
	}
	out, e, code = xkvRun(t, exStdout, "import", b, "-", "--conflicts", "ours", "--json")
	if code != 0 {
		t.Fatalf("import from stdin: %s", e)
	}
	var rep xkv.ImportReport
	if err := json.Unmarshal([]byte(out), &rep); err != nil || len(rep.Conflicts) != 1 || rep.Conflicts[0].Kept != "ours" {
		t.Fatalf("report: %v %s", err, out)
	}
	if out, _, _ := xkvRun(t, "", "get", b, "risk:42:score"); strings.TrimSpace(out) != "0.5" {
		t.Fatalf("kept ours: %q", out)
	}
	if _, e, code := xkvRun(t, exStdout, "import", b, "-", "--conflicts", "mine"); code != 2 {
		t.Fatalf("bad policy: %d %s", code, e)
	}
	if _, e, code := xkvRun(t, exStdout[:len(exStdout)/2], "import", b, "-"); code != 1 || !strings.Contains(e, "not a valid exchange file") {
		t.Fatalf("cut file: %d %s", code, e)
	}
	out, _, _ = xkvRun(t, "", "stats", b)
	if !strings.Contains(out, "format:     2") || !strings.Contains(out, "store id:") {
		t.Fatalf("stats: %s", out)
	}

	// A 0.1 store: export says to upgrade; upgrade does it, once.
	old := filepath.Join(dir, "old.xkv")
	data, err := os.ReadFile(filepath.Join("..", "..", "testdata", "format1.xkv"))
	if err != nil {
		t.Fatal(err)
	}
	os.WriteFile(old, data, 0o644)
	if _, e, code := xkvRun(t, "", "export", old); code != 1 || !strings.Contains(e, "xkv upgrade") {
		t.Fatalf("export of a format 1 store: %d %s", code, e)
	}
	if out, e, code := xkvRun(t, "", "upgrade", old); code != 0 || !strings.Contains(out, "from file format 1 to 2") {
		t.Fatalf("upgrade: %d %s %s", code, out, e)
	}
	if out, _, code := xkvRun(t, "", "upgrade", old); code != 0 || !strings.Contains(out, "already file format 2") {
		t.Fatalf("second upgrade: %d %s", code, out)
	}
	if out, _, _ := xkvRun(t, "", "get", old, "account:42:limit"); strings.TrimSpace(out) != "50" {
		t.Fatalf("after upgrade: %q", out)
	}
}

func TestHTTPExchange(t *testing.T) {
	open := func(name string) (*xkv.DB, *httptest.Server) {
		db, err := xkv.Open(filepath.Join(t.TempDir(), name), nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { db.Close() })
		srv := httptest.NewServer(newHandler(db))
		t.Cleanup(srv.Close)
		return db, srv
	}
	a, sa := open("a.xkv")
	_, sb := open("b.xkv")
	if _, err := a.Put(t.Context(), "k", []byte("v"), xkv.Reason("r"), xkv.Actor("x")); err != nil {
		t.Fatal(err)
	}
	resp, err := http.Get(sa.URL + "/v1/export?since=0")
	if err != nil {
		t.Fatal(err)
	}
	var ex bytes.Buffer
	ex.ReadFrom(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || resp.Header.Get("X-Xkv-Through") != "1" {
		t.Fatalf("export: %d %v", resp.StatusCode, resp.Header)
	}
	post := func(q string, body []byte) (int, map[string]any) {
		resp, err := http.Post(sb.URL+"/v1/import"+q, "application/x-ndjson", bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var m map[string]any
		json.NewDecoder(resp.Body).Decode(&m)
		return resp.StatusCode, m
	}
	if code, m := post("?conflicts=theirs", ex.Bytes()); code != 200 || m["imported"] != 1.0 {
		t.Fatalf("import: %d %v", code, m)
	}
	if code, _ := post("", ex.Bytes()[:ex.Len()-3]); code != 400 {
		t.Fatalf("cut file: %d", code)
	}
	if code, _ := post("?conflicts=mine", ex.Bytes()); code != 400 {
		t.Fatalf("bad policy: %d", code)
	}
	if resp, _ := http.Get(sa.URL + "/v1/export?since=x"); resp.StatusCode != 400 {
		t.Fatalf("bad since: %d", resp.StatusCode)
	}
}

func TestNewIDAndUpgradeEndpoints(t *testing.T) {
	dir := t.TempDir()
	a := filepath.Join(dir, "a.xkv")
	if _, e, code := xkvRun(t, "", "put", a, "k", "v", "--reason", "r"); code != 0 {
		t.Fatal(e)
	}
	// new-id needs to be told what the copy shares.
	if _, _, code := xkvRun(t, "", "new-id", a); code != 2 {
		t.Fatalf("new-id without --keep-through: %d", code)
	}
	// A copy shares the id until new-id.
	data, _ := os.ReadFile(a)
	b := filepath.Join(dir, "b.xkv")
	os.WriteFile(b, data, 0o644)
	ex := filepath.Join(dir, "b.xkvx")
	if _, e, code := xkvRun(t, "", "put", b, "y", "1", "--reason", "r"); code != 0 {
		t.Fatal(e)
	}
	xkvRun(t, "", "export", b, "--out", ex)
	if _, e, code := xkvRun(t, "", "import", a, ex); code != 1 || !strings.Contains(e, "new-id") {
		t.Fatalf("import from a copy: %d %s", code, e)
	}
	if out, e, code := xkvRun(t, "", "new-id", b, "--keep-through", "1"); code != 0 || !strings.Contains(out, "store id") {
		t.Fatalf("new-id: %d %s %s", code, out, e)
	}
	xkvRun(t, "", "export", b, "--out", ex)
	if out, e, code := xkvRun(t, "", "import", a, ex); code != 0 || !strings.Contains(out, "1 imported") {
		t.Fatalf("import after new-id: %d %s %s", code, out, e)
	}

	// Over HTTP, a format 1 store needs POST /v1/upgrade before an export.
	old := filepath.Join(dir, "old.xkv")
	d1, _ := os.ReadFile(filepath.Join("..", "..", "testdata", "format1.xkv"))
	os.WriteFile(old, d1, 0o644)
	db, err := xkv.Open(old, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	srv := httptest.NewServer(newHandler(db))
	defer srv.Close()
	if resp, _ := http.Get(srv.URL + "/v1/export"); resp.StatusCode != http.StatusConflict {
		t.Fatalf("export of format 1: %d", resp.StatusCode)
	}
	resp, _ := http.Post(srv.URL+"/v1/upgrade", "", nil)
	var m map[string]any
	json.NewDecoder(resp.Body).Decode(&m)
	resp.Body.Close()
	if resp.StatusCode != 200 || m["format"] != 2.0 {
		t.Fatalf("upgrade: %d %v", resp.StatusCode, m)
	}
	if resp, _ := http.Get(srv.URL + "/v1/export"); resp.StatusCode != 200 {
		t.Fatalf("export after upgrade: %d", resp.StatusCode)
	}
}

func TestWatchCommand(t *testing.T) {
	f := filepath.Join(t.TempDir(), "w.xkv")
	for i := 0; i < 3; i++ {
		if _, e, code := xkvRun(t, "", "put", f, fmt.Sprintf("config/k%d", i), "v", "--reason", "r", "--actor", "ops"); code != 0 {
			t.Fatal(e)
		}
	}
	out, e, code := xkvRun(t, "", "watch", f, "--since", "1", "--count", "2")
	if code != 0 || strings.Count(out, "\n") != 2 || !strings.Contains(out, "config/k1") || !strings.Contains(out, "config/k2") {
		t.Fatalf("watch: %d %q %s", code, out, e)
	}
	out, _, code = xkvRun(t, "", "watch", f, "--all", "--count", "1", "--json", "--prefix", "config/k2")
	var r xkv.Revision
	if code != 0 || json.Unmarshal([]byte(out), &r) != nil || r.Key != "config/k2" || r.Rev != 3 {
		t.Fatalf("watch --json: %d %q", code, out)
	}
	// New writes from another handle reach a watch already running.
	done := make(chan string, 1)
	go func() {
		out, _, _ := xkvRun(t, "", "watch", f, "--count", "1")
		done <- out
	}()
	time.Sleep(300 * time.Millisecond)
	if _, e, code := xkvRun(t, "", "put", f, "late", "1", "--reason", "after the watch began"); code != 0 {
		t.Fatal(e)
	}
	select {
	case out := <-done:
		if !strings.Contains(out, "late") {
			t.Fatalf("live: %q", out)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the watch didn't see the new write")
	}
	if _, e, code := xkvRun(t, "", "watch", f, "--since", "1", "--all"); code != 2 {
		t.Fatalf("both flags: %d %s", code, e)
	}
}

func TestHTTPWatch(t *testing.T) {
	db, err := xkv.Open(filepath.Join(t.TempDir(), "w.xkv"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	srv := httptest.NewServer(newHandler(db))
	defer srv.Close()
	db.Put(t.Context(), "a", []byte("1"), xkv.Reason("r"), xkv.Actor("x"))

	// NDJSON from revision 0: the existing one, then a new one.
	resp, err := http.Get(srv.URL + "/v1/watch?since=0")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.Header.Get("Content-Type") != "application/x-ndjson" {
		t.Fatalf("content type %q", resp.Header.Get("Content-Type"))
	}
	sc := bufio.NewScanner(resp.Body)
	next := func() xkv.Revision {
		t.Helper()
		for sc.Scan() {
			if len(bytes.TrimSpace(sc.Bytes())) == 0 {
				continue
			}
			var r xkv.Revision
			if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
				t.Fatalf("%v: %s", err, sc.Bytes())
			}
			return r
		}
		t.Fatal("stream ended")
		return xkv.Revision{}
	}
	if r := next(); r.Key != "a" || r.Rev != 1 {
		t.Fatalf("%+v", r)
	}
	go db.Put(t.Context(), "b", []byte("2"), xkv.Reason("r"), xkv.Actor("x"), xkv.CauseLatest("a"))
	if r := next(); r.Key != "b" || len(r.Causes) != 1 {
		t.Fatalf("%+v", r)
	}

	// Server-Sent Events, filtered by prefix, from now on.
	req, _ := http.NewRequest("GET", srv.URL+"/v1/watch?prefix=c", nil)
	req.Header.Set("Accept", "text/event-stream")
	resp2, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	time.Sleep(50 * time.Millisecond)
	db.Put(t.Context(), "skip", []byte("x"), xkv.Reason("r"), xkv.Actor("x"))
	db.Put(t.Context(), "cfg", []byte("y"), xkv.Reason("r"), xkv.Actor("x"))
	br := bufio.NewReader(resp2.Body)
	var event []string
	for len(event) < 2 {
		l, err := br.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		if l = strings.TrimSpace(l); l != "" && !strings.HasPrefix(l, "retry:") {
			event = append(event, l)
		}
	}
	if event[0] != "id: 4" || !strings.Contains(event[1], `"key":"cfg"`) {
		t.Fatalf("event %q", event)
	}

	// Compacted away before the watch reached it: an error line.
	for i := 0; i < 3; i++ {
		db.Put(t.Context(), "a", []byte(fmt.Sprint(i)), xkv.Reason("r"), xkv.Actor("x"))
	}
	db.Compact(t.Context(), xkv.CompactOptions{})
	resp3, err := http.Get(srv.URL + "/v1/watch?since=0")
	if err != nil {
		t.Fatal(err)
	}
	defer resp3.Body.Close()
	sc3 := bufio.NewScanner(resp3.Body)
	for sc3.Scan() {
		if strings.Contains(sc3.Text(), `"error"`) {
			if !strings.Contains(sc3.Text(), "compaction") {
				t.Fatalf("error line %q", sc3.Text())
			}
			return
		}
	}
	t.Fatal("no error line for a compacted start")
}
