package main

import (
	"bufio"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/explainable-kv/xkv"
)

// SSE: EventSource reconnects to the same URL with Last-Event-ID; the server
// ignores it, so a stream without since skips what was written meanwhile.
func TestReviewSSELastEventID(t *testing.T) {
	db, _ := xkv.Open(filepath.Join(t.TempDir(), "w.xkv"), nil)
	defer db.Close()
	srv := httptest.NewServer(newHandler(db))
	defer srv.Close()
	for _, k := range []string{"a", "b", "c"} {
		db.Put(t.Context(), k, []byte("1"), xkv.Reason("r"), xkv.Actor("x"))
	}
	// Client saw id 1, disconnected; 2 and 3 were written; it reconnects.
	req, _ := http.NewRequest("GET", srv.URL+"/v1/watch", nil)
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Last-Event-ID", "1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	got := make(chan string, 1)
	go func() {
		br := bufio.NewReader(resp.Body)
		for {
			l, err := br.ReadString('\n')
			if err != nil {
				return
			}
			if strings.HasPrefix(l, "id:") {
				got <- strings.TrimSpace(l)
				return
			}
		}
	}()
	select {
	case id := <-got:
		if id != "id: 2" {
			t.Errorf("after Last-Event-ID 1 the first event is %q", id)
		}
	case <-time.After(500 * time.Millisecond):
		t.Errorf("Last-Event-ID: 1 ignored: revisions 2 and 3 never sent")
	}
}

// Client disconnects from an idle stream: the handler goroutine ends.
func TestReviewWatchDisconnectLeak(t *testing.T) {
	db, _ := xkv.Open(filepath.Join(t.TempDir(), "w.xkv"), nil)
	defer db.Close()
	srv := httptest.NewServer(newHandler(db))
	defer srv.Close()
	before := runtime.NumGoroutine()
	for i := 0; i < 20; i++ {
		resp, err := http.Get(srv.URL + "/v1/watch")
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
	}
	http.DefaultClient.CloseIdleConnections()
	time.Sleep(500 * time.Millisecond)
	if after := runtime.NumGoroutine(); after > before+4 {
		buf := make([]byte, 1<<16)
		n := runtime.Stack(buf, true)
		t.Errorf("goroutines %d -> %d\n%s", before, after, buf[:n])
	}
}
