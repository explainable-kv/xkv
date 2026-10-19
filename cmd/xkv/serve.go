// Copyright xkv.org 2026
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
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

// maxImportBody caps an exchange file sent to /v1/import.
const maxImportBody = 1 << 30

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
//	GET    /v1/export[?since=REV]     an exchange file
//	POST   /v1/import[?conflicts=newest|ours|theirs]
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
			// JSON escaping can grow text with control characters up to six
			// times; base64 values grow by a third.
			body := http.MaxBytesReader(w, r.Body, 6*xkv.MaxValueSize+1<<20)
			dec := json.NewDecoder(body)
			dec.DisallowUnknownFields()
			if err := dec.Decode(&b); err != nil && !(del && errors.Is(err, io.EOF)) {
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
			"format": s.Format, "store_id": s.StoreID,
		}, err)
	})
	mux.HandleFunc("GET /v1/export", func(w http.ResponseWriter, r *http.Request) {
		var opts xkv.ExportOptions
		if s := r.URL.Query().Get("since"); s != "" {
			v, err := strconv.ParseUint(s, 10, 64)
			if err != nil {
				fail(w, http.StatusBadRequest, "since takes a revision number")
				return
			}
			opts.Since = v
		}
		// A GET doesn't rewrite the file: a format 1 store needs POST
		// /v1/upgrade first.
		if s, err := db.Stats(); err == nil && s.Format < 2 {
			fail(w, http.StatusConflict, "the store uses file format 1; POST /v1/upgrade first")
			return
		}
		// Built in memory first, so an error can still be reported as one
		// rather than as a cut-off file.
		var buf bytes.Buffer
		st, err := db.Export(r.Context(), &buf, opts)
		if err != nil {
			reply(w, nil, err)
			return
		}
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.Header().Set("X-Xkv-Through", strconv.FormatUint(st.Through, 10))
		w.Write(buf.Bytes())
	})
	mux.HandleFunc("POST /v1/import", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		pol, err := xkv.ParseConflictPolicy(q.Get("conflicts"))
		if err != nil {
			fail(w, http.StatusBadRequest, err.Error())
			return
		}
		body := http.MaxBytesReader(w, r.Body, maxImportBody)
		rep, err := db.Import(r.Context(), body, xkv.ImportOptions{Conflicts: pol})
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			fail(w, http.StatusRequestEntityTooLarge, "exchange file too large for one request")
			return
		}
		if err != nil && rep.Imported+rep.Context+rep.Agreed+len(rep.Conflicts) > 0 {
			// Part of the file went in before the failure: say so, with
			// what was done. Importing again carries on.
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]any{"error": err.Error(), "partial": true, "report": rep})
			return
		}
		reply(w, rep, err)
	})
	mux.HandleFunc("POST /v1/upgrade", func(w http.ResponseWriter, r *http.Request) {
		err := db.Upgrade(r.Context())
		s, _ := db.Stats()
		reply(w, map[string]any{"format": s.Format, "store_id": s.StoreID}, err)
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
		case errors.Is(err, xkv.ErrNoReason), errors.Is(err, xkv.ErrBadCause), errors.Is(err, xkv.ErrInvalid), errors.Is(err, xkv.ErrBadExchange):
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
