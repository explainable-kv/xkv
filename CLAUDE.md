# Notes for Claude

This repo is the code for xkv, a key-value store that remembers why. Its site
is the separate repo photozeitgeist/xkv.org. The owner runs the project on
the low maintenance workflow: Claude does the code, tests, docs, CI, releases
and the site; the owner does not.

## Where the next session starts

v0.1.0 is the first release: the store, the xkv command with its HTTP mode,
the WebAssembly build for the site demo, and four case-study examples.

Next, once agreed with the owner (the plan goes here only after he approves
it): nothing is scheduled. Candidates from the original plan are v0.2
Exchange (export changes since a checkpoint as a file that carries reasons and
causes; import with conflicts kept) and v0.3 Watch (a stream of new
revisions).

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
