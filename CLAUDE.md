# Notes for Claude

This repo is the code for xkv, a key-value store that remembers why. Its site
is the separate repo photozeitgeist/xkv.org. The owner runs the project on
the low maintenance workflow: Claude does the code, tests, docs, CI, releases
and the site; the owner does not.

## Where the next session starts

v0.1.0 was the first release: the store, the xkv command with its HTTP mode,
the WebAssembly build for the site demo, and four case-study examples.

The plan the owner approved on Oct 19 2026 ("do both"):

1. v0.2.0 Exchange: export the changes since a revision as an exchange file
   that carries reasons, actors, refs, times and causes; import them into
   another store, with conflicts kept and settled by a resolution revision.
   Needs file format 2 (a store id in the header, an origin on imported
   revisions, history-only revisions). See docs/exchange.md.
2. v0.3.0 Watch: a stream of new revisions (`Watch` in Go, `xkv watch`, a
   streaming HTTP endpoint), with a correct answer when the starting
   revision was compacted away; the site demo shows changes arriving.

Each ships like 0.1.0: tests aimed at its promises, an independent review,
a fresh recording, the automated release, a site post and a note on
softdevstudio.com. Nothing is planned after 0.3.0 until the owner decides.

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
