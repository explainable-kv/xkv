package main

import (
	"bufio"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/explainable-kv/xkv"
)

// An SSE client that hasn't received an event yet (e.g. a prefix watch with
// no matches) has no Last-Event-ID; on reconnect it starts at LastRev and
// misses what was written while it was away.
func TestReview2SSENoIDBeforeFirstEvent(t *testing.T) {
	db, _ := xkv.Open(filepath.Join(t.TempDir(), "w.xkv"), nil)
	defer db.Close()
	srv := httptest.NewServer(newHandler(db))
	defer srv.Close()
	req, _ := http.NewRequest("GET", srv.URL+"/v1/watch?prefix=c", nil)
	req.Header.Set("Accept", "text/event-stream")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	br := bufio.NewReader(resp.Body)
	var head []string
	for {
		l, _ := br.ReadString('\n')
		if strings.TrimSpace(l) == "" {
			break
		}
		head = append(head, strings.TrimSpace(l))
	}
	t.Logf("stream opens with %q", head)
	for _, l := range head {
		if strings.HasPrefix(l, "id:") {
			return
		}
	}
	t.Errorf("no id: before the first event, so a reconnect has no Last-Event-ID and skips what was written meanwhile")
}
