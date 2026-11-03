# Notes for Claude

This repo is the code for xkv, a key-value store that remembers why. Its site
is the separate repo photozeitgeist/xkv.org. The owner runs the project on
the low maintenance workflow: Claude does the code, tests, docs, CI, releases
and the site; the owner does not.

## Where the next session starts

Releases so far, each with tests aimed at its promises, independent
reviews, a fresh recording, a site post and a softdevstudio.com note:

- v0.1.0: the store, the xkv command with its HTTP mode, the WebAssembly
  build for the site demo, and four case-study examples.
- v0.2.0 Exchange: export and import changes with their reasons and causes;
  seen lists tell updates from conflicts; the loser stays in history; file
  format 2. See docs/exchange.md.
- v0.3.0 Watch: a stream of new revisions (`Watch`, `xkv watch`,
  `GET /v1/watch`), read-only handles following another process, and the
  demo's "Changes as they happen" panel.

The plan the owner approved on Oct 19 2026 ("do both") is done. XKV 0.4
(softdev-plan item 26) is in progress, below.

## Rules

- Releases are automated. To release, raise `const Version` in `xkv.go` and
  push to `main`; never ask the owner to publish one. [RELEASING.md](RELEASING.md)
  has the procedure and the checks to run afterwards.
- Every number on xkv.org comes from `test/results/`. After changing anything
  that affects them, run `sh scripts/record-tests.sh` and update the site.
- Changes to the file format need a new `FormatVersion` and an update to
  [docs/format.md](docs/format.md).
- The site's demo uses the WebAssembly build (`wasm/`). After changing it,
  rebuild and copy `xkv.wasm` and the matching `wasm_exec.js` to the site's
  `static/demo/`.

## XKV 0.4 (softdev-plan item 26, approved by YK 2026-11-03)

What and why: HTTP mode has no authentication, so the docs say to keep it on
localhost, which rules out the main multi-service use (other services writing
values with reasons). Agent frameworks are mostly Python and TypeScript and
have no client. Explanations stay inside xkv, while audit tooling reads W3C
PROV.

Design decisions:
- Phase A, tokens: `xkv serve --token-file FILE` (one write token) and
  `--read-token-file FILE` (GET only, watch included). With either, every
  request needs `Authorization: Bearer TOKEN`, compared in constant time;
  missing or wrong is 401 with `WWW-Authenticate: Bearer`, a read token on a
  write is 403. Files are read once at start, must hold one line of at least
  32 printable characters and may not be group or world readable on Unix.
  `--addr` on a non-loopback address is refused without a token. Optional
  `--tls-cert`/`--tls-key` (TLS 1.2+); without TLS a non-loopback listen
  prints a warning that tokens cross the network in clear. Tokens never reach
  logs or error text. The default stays 127.0.0.1 without a token.
- Phase B, clients: `clients/python/xkv_client.py` (standard library only,
  Python 3.9+) and `clients/typescript/xkv.ts` (no dependencies, fetch; built
  to `xkv.js` and `xkv.d.ts` with tsc, checked in and checked to match in
  CI). Both cover put, delete, get (with at), keys, history, why (and PROV),
  revisions, stats, export, import and watch (a generator or async iterator
  over the JSON-lines stream, resuming from the last revision), raise one
  error type carrying the HTTP status and message, and take the token.
  Tested in CI against a real `xkv serve`. Distributed from the repo (pip
  from git with a pyproject.toml; raw files at the release tag); registry
  publishing needs YK's accounts and is not in this phase.
- Phase C, PROV: `GET /v1/why/KEY?format=prov` (and on revs),
  `xkv why --prov`, and `(*WhyNode).PROV()` give the tree as W3C PROV-O in
  JSON-LD: each revision a prov:Entity, its transaction a prov:Activity with
  its time, the actor a prov:Agent, causes as prov:used, the previous value as
  prov:wasRevisionOf, reason and ref as xkv: terms. Ids are URNs under the
  store id, so trees from one store merge. Checked by parsing with rdflib in
  CI and with hand-written expected triples.
- Upkeep: README header version; go.mod and README to Go 1.26; CLAUDE.md gets
  a Next heading.
- Version 0.4.0.

Steps:
1. [x] Phase 0: this plan.
2. [x] Upkeep (939411d): go.mod and README to Go 1.26, README header version (raised again at release).
3. [x] Phase A (939411d): cmd/xkv/auth.go, serve flags; auth_test.go (token file rules, loopback detection, 401/403 matrix, TLS 1.2+ with a self-signed certificate, TLS 1.1 refused, refusals, the warning).
4. [x] Phase C (b7c6354): prov.go, why --prov, format=prov; prov_test.go (nodes, links, shared transactions, base64, depth limit, every reference resolves), scripts/check-prov.py with rdflib in a CI job.
5. [x] Phase B (6ef4ef0): clients/python (pip-installable), clients/typescript (tsc 5.9.3, checked-in build), both tested against a real server with tokens; CI on Python 3.9/Node 20 and 3.13/Node 24.
6. [x] Docs (6d594a9): http.md tokens and TLS, prov.md, client READMEs, README sections and limits.
7. [x] Independent review, two rounds, fixes; fresh test/results from the new record workflow (Go 1.27.x on GitHub, clients and PROV included).
   Review round 1: blocker, both clients let fetch or urllib resolve dot and empty key segments, so `put("tenantA/../tenantB/limit")` wrote `tenantB/limit` from TypeScript; both now percent-encode the whole key and refuse `.` and `..`, with tests on 6 odd keys. Should-fix, all fixed: watch could lose revisions on a reconnect before its first revision (start fixed from stats, all=1 kept with Last-Event-ID; a restart-mid-watch test); PROV gave a transaction one agent and `used` for causes it generated itself; `prov:label` is not PROV-O (now rdfs:label, and check-prov.py checks against rdflib's PROV namespace); previous values looked causeless (xkv:causesNotShown); the TypeScript watch had no read timeout (60 s idle). Nits fixed: token length timing (digest compare), localhost checked by where it listens, tabs after Bearer, pycache, a flaky teardown, error parsing, a private-CA note.
   Review round 2: every round 1 finding fixed (keys checked over 17 odd forms, watchloss, compacted all=1 resume). One should-fix from the idle-timer change: the TypeScript watch ignored an already-aborted signal and aborts during the retry wait; fixed with a test. Nits fixed: the starting stats call now retries, the idle timer pauses while the consumer holds a revision, localhost resolving elsewhere with a token warns instead of refusing. Left: watch errors classified by message text; `used` for depth-unexplored causes from the same transaction (rare).
8. [ ] 0.4.0 released by the workflow; assets at tag and latest; downloaded binary run.
9. [ ] xkv.org post and pages; softdevstudio.com note; softdev-plan item 26 done.

## Next

XKV 0.4 is in progress (above).
