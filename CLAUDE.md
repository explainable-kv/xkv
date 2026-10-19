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

The plan the owner approved on Oct 19 2026 ("do both") is done. Nothing is
planned next until the owner decides; the plan goes here only after he
approves it.

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
