// Copyright xkv.org 2026
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/explainable-kv/xkv"
)

func cmdServe(args []string, stdout, stderr io.Writer) error {
	p, err := parseArgs(args, flagSpec{"addr": false, "relaxed": true, "no-sync": true})
	if err != nil {
		return usageError{err.Error()}
	}
	if len(p.pos) != 1 {
		return usagef("want FILE")
	}
	addr := "127.0.0.1:7070"
	if p.has("addr") {
		addr = p.str("addr")
	}
	db, err := xkv.Open(p.pos[0], &xkv.Options{Relaxed: p.bools["relaxed"], NoSync: p.bools["no-sync"]})
	if err != nil {
		return err
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		db.Close()
		return err
	}
	srv := &http.Server{Handler: newHandler(db), ReadHeaderTimeout: 10 * time.Second}
	fmt.Fprintf(stdout, "xkv %s serving %s on http://%s\n", xkv.Version, p.pos[0], ln.Addr())

	done := make(chan struct{})
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigs
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(ctx)
		close(done)
	}()
	err = srv.Serve(ln)
	if errors.Is(err, http.ErrServerClosed) {
		<-done
		err = nil
	}
	if cerr := db.Close(); err == nil {
		err = cerr
	}
	return err
}

// writeBody is the JSON a PUT or DELETE takes.
type writeBody struct {
	Value    *string        `json:"value"`
	Encoding string         `json:"encoding"`
	Reason   string         `json:"reason"`
	Actor    string         `json:"actor"`
	Ref      string         `json:"ref"`
	Causes   []xkv.CauseRef `json:"causes"` // rev 0 means the key's current revision
}

// newHandler serves the store over HTTP with JSON bodies:
//
//	GET    /v1/keys?prefix=P          {"keys": [...]}
//	GET    /v1/keys/KEY[?at=REV]      the key's revision
//	PUT    /v1/keys/KEY               write; body {"value", "reason", "causes", ...}
//	DELETE /v1/keys/KEY               delete; body {"reason", "causes", ...}
//	GET    /v1/history/KEY[?limit=N]  revisions, newest first
//	GET    /v1/why/KEY[?depth=N]      the tree of causes
//	GET    /v1/revs/REV               one revision
//	GET    /v1/revs/REV/why[?depth=N]
//	GET    /v1/stats
func newHandler(db *xkv.DB) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/keys", func(w http.ResponseWriter, r *http.Request) {
		keys, err := db.Keys(r.Context(), r.URL.Query().Get("prefix"))
		if keys == nil {
			keys = []string{}
		}
		reply(w, map[string]any{"keys": keys}, err)
	})
	mux.HandleFunc("GET /v1/keys/{key...}", func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")
		if at := r.URL.Query().Get("at"); at != "" {
			rev, err := strconv.ParseUint(at, 10, 64)
			if err != nil {
				fail(w, http.StatusBadRequest, "at takes a revision number")
				return
			}
			v, err := db.GetAt(r.Context(), key, rev)
			reply(w, v, err)
			return
		}
		v, err := db.Get(r.Context(), key)
		reply(w, v, err)
	})
	write := func(del bool) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			var b writeBody
			body := http.MaxBytesReader(w, r.Body, 2*xkv.MaxValueSize+64<<10)
			if err := json.NewDecoder(body).Decode(&b); err != nil && !(del && errors.Is(err, io.EOF)) {
				var mbe *http.MaxBytesError
				if errors.As(err, &mbe) {
					fail(w, http.StatusRequestEntityTooLarge, "body too large")
					return
				}
				fail(w, http.StatusBadRequest, "body must be JSON: "+err.Error())
				return
			}
			var value []byte
			if !del {
				if b.Value == nil {
					fail(w, http.StatusBadRequest, `a PUT needs "value"`)
					return
				}
				switch b.Encoding {
				case "", "utf-8", "text":
					value = []byte(*b.Value)
				case "base64":
					v, err := base64.StdEncoding.DecodeString(*b.Value)
					if err != nil {
						fail(w, http.StatusBadRequest, "value is not valid base64")
						return
					}
					value = v
				default:
					fail(w, http.StatusBadRequest, `encoding must be "base64" or left out`)
					return
				}
			}
			actor := b.Actor
			if actor == "" {
				actor = r.Header.Get("X-Xkv-Actor")
			}
			if actor == "" {
				actor = "http"
			}
			opts := []xkv.WriteOption{xkv.Actor(actor), xkv.Ref(b.Ref)}
			if b.Reason != "" {
				opts = append(opts, xkv.Reason(b.Reason))
			}
			for _, c := range b.Causes {
				if c.Rev == 0 {
					opts = append(opts, xkv.CauseLatest(c.Key))
				} else {
					opts = append(opts, xkv.Cause(c.Key, c.Rev))
				}
			}
			var rev xkv.Revision
			var err error
			if del {
				rev, err = db.Delete(r.Context(), r.PathValue("key"), opts...)
			} else {
				rev, err = db.Put(r.Context(), r.PathValue("key"), value, opts...)
			}
			reply(w, rev, err)
		}
	}
	mux.HandleFunc("PUT /v1/keys/{key...}", write(false))
	mux.HandleFunc("DELETE /v1/keys/{key...}", write(true))
	mux.HandleFunc("GET /v1/history/{key...}", func(w http.ResponseWriter, r *http.Request) {
		limit, ok := intParam(w, r, "limit")
		if !ok {
			return
		}
		h, err := db.History(r.Context(), r.PathValue("key"), limit)
		reply(w, h, err)
	})
	mux.HandleFunc("GET /v1/why/{key...}", func(w http.ResponseWriter, r *http.Request) {
		depth, ok := intParam(w, r, "depth")
		if !ok {
			return
		}
		n, err := db.Why(r.Context(), r.PathValue("key"), depth)
		reply(w, n, err)
	})
	mux.HandleFunc("GET /v1/revs/{rev}", func(w http.ResponseWriter, r *http.Request) {
		rev, err := strconv.ParseUint(r.PathValue("rev"), 10, 64)
		if err != nil {
			fail(w, http.StatusBadRequest, "not a revision number")
			return
		}
		v, err := db.Revision(r.Context(), rev)
		reply(w, v, err)
	})
	mux.HandleFunc("GET /v1/revs/{rev}/why", func(w http.ResponseWriter, r *http.Request) {
		rev, err := strconv.ParseUint(r.PathValue("rev"), 10, 64)
		if err != nil {
			fail(w, http.StatusBadRequest, "not a revision number")
			return
		}
		depth, ok := intParam(w, r, "depth")
		if !ok {
			return
		}
		n, err := db.WhyRev(r.Context(), rev, depth)
		reply(w, n, err)
	})
	mux.HandleFunc("GET /v1/stats", func(w http.ResponseWriter, r *http.Request) {
		s, err := db.Stats()
		reply(w, map[string]any{
			"keys": s.Keys, "live_keys": s.LiveKeys, "revisions": s.Revisions,
			"last_rev": s.LastRev, "last_tx": s.LastTx, "file_size": s.FileSize,
		}, err)
	})
	return mux
}

func intParam(w http.ResponseWriter, r *http.Request, name string) (int, bool) {
	s := r.URL.Query().Get(name)
	if s == "" {
		return 0, true
	}
	n, err := strconv.Atoi(s)
	if err != nil || n < 0 {
		fail(w, http.StatusBadRequest, name+" takes a whole number")
		return 0, false
	}
	return n, true
}

func reply(w http.ResponseWriter, v any, err error) {
	if err != nil {
		status := http.StatusInternalServerError
		switch {
		case errors.Is(err, xkv.ErrCompacted) && !errors.Is(err, xkv.ErrBadCause):
			status = http.StatusGone
		case errors.Is(err, xkv.ErrNotFound):
			status = http.StatusNotFound
		case errors.Is(err, xkv.ErrTooLarge):
			status = http.StatusRequestEntityTooLarge
		case errors.Is(err, xkv.ErrNoReason), errors.Is(err, xkv.ErrBadCause), errors.Is(err, xkv.ErrInvalid):
			status = http.StatusBadRequest
		case errors.Is(err, xkv.ErrClosed):
			status = http.StatusServiceUnavailable
		}
		fail(w, status, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

func fail(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
