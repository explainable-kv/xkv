# xkv

**A key-value store that remembers why.**

[xkv](https://xkv.org) is a small embedded key-value store for Go. Every
write records who made it, why, and which earlier change caused it. Ask any
value how it got there and xkv walks the chain back, across keys, to where it
started.

It's one file and one Go package with no dependencies. There's an `xkv`
command for the shell, a small HTTP mode for other languages, and a
WebAssembly build that runs in a browser.

Version 0.1.0 · Apache License 2.0 · [xkv.org](https://xkv.org)

- [Why a store should remember why](#why-a-store-should-remember-why)
- [Quick start](#quick-start)
- [Use it from Go](#use-it-from-go)
- [Causes](#causes)
- [Reading the past](#reading-the-past)
- [Compaction](#compaction)
- [The xkv command](#the-xkv-command)
- [HTTP mode](#http-mode)
- [In the browser](#in-the-browser)
- [What xkv promises, and what it doesn't](#what-xkv-promises-and-what-it-doesnt)
- [How it was tested](#how-it-was-tested)
- [Build it yourself](#build-it-yourself)
- [The name](#the-name)
- [License](#license)

## Why a store should remember why

Small values cause big trouble. A credit limit drops from 100 to 50 and a
customer calls. A database timeout is 30 seconds and nobody knows who set it.
A price changes overnight. The value is right there in the store. The reason
isn't.

The reason is usually spread across three places: an application log nobody
kept, a chat thread, and the memory of whoever was on call. Ordinary
key-value stores keep the current value. Some keep old values too. None of
them keep the reason, and none of them link one change to the change that
caused it.

xkv does both. A write to xkv is a revision with four things attached:

- the **actor**: who made it, a service, a person or an AI agent;
- the **reason**: a short sentence, required by default;
- the **ref**: an optional outside reference, such as a ticket, a request id
  or a tool call id;
- the **causes**: up to eight earlier revisions, of any key, that led to it.

Causes are the part that matters. The limits service doesn't just write
`account:42:limit = 50`. It writes it with `risk:42:score@3` as the cause,
and the risk model wrote that score with `chargeback:cb_991@1` as its cause.
So one call explains the limit:

```
$ xkv why limits.xkv account:42:limit
account:42:limit = 50   r5 · 2026-10-19 09:20:41
│ was 100 · by limits-service · risk policy v3
└─ risk:42:score = 0.82   r4 · 2026-10-19 09:20:41
   │ was 0.31 · by risk-model · new chargeback
   └─ chargeback:cb_991 = card dispute filed   r3 · 2026-10-19 09:20:41
        by billing-worker · bank notice · ref BANK-7731
```

Nothing is guessed. There's no AI in the explanation. It's built from what
each writer recorded at the moment it wrote.

## Quick start

Download the `xkv` binary for your system from the
[latest release](https://github.com/explainable-kv/xkv/releases/latest) and
put it on your PATH. Then:

```sh
xkv put limits.xkv chargeback:cb_991 "card dispute filed" \
    --reason "bank notice" --ref BANK-7731 --actor billing-worker
xkv put limits.xkv risk:42:score 0.82 \
    --reason "new chargeback" --cause chargeback:cb_991 --actor risk-model
xkv put limits.xkv account:42:limit 50 \
    --reason "risk policy v3" --cause risk:42:score --actor limits-service

xkv why limits.xkv account:42:limit
```

`--cause KEY` points at the key's current revision. `--cause KEY@REV` points
at one revision exactly.

## Use it from Go

```sh
go get github.com/explainable-kv/xkv
```

```go
db, err := xkv.Open("limits.xkv", nil)
if err != nil {
	return err
}
defer db.Close()

// Name the actor once per request; every write made with ctx records it.
ctx = xkv.WithActor(ctx, "limits-service")

r, err := db.Put(ctx, "account:42:limit", []byte("50"),
	xkv.Reason("risk policy v3"),
	xkv.CauseLatest("risk:42:score"))

v, err := db.Get(ctx, "account:42:limit")         // the current revision
tree, err := db.Why(ctx, "account:42:limit", 0)   // the chain behind it
tree.WriteText(os.Stdout)                        // printed like the xkv command
```

A `Revision` carries `Rev`, `Tx`, `Key`, `Value`, `Deleted`, `Actor`,
`Reason`, `Ref`, `Causes` and `Time`.

### Transactions

`Update` writes several keys at once. Either every write inside commits or
none does. The options given to `Update` apply to each write inside it.

```go
err := db.Update(ctx, func(tx *xkv.Tx) error {
	if _, err := tx.Put("price:basic", []byte("9")); err != nil {
		return err
	}
	_, err := tx.Put("price:pro", []byte("33"))
	return err
}, xkv.Reason("Q4 price list"), xkv.Ref("PRICE-12"), xkv.Cause("decision:q4-pricing", 1))
```

Return an error from the function and nothing is written. Don't call
`db.Put` or `db.Update` from inside it: the store has one writer at a time.

### Strict and relaxed

By default a write without a reason is refused with `ErrNoReason`. A store
that explains itself badly is worse than one that doesn't try, so the honest
path is the default. Open with `&xkv.Options{Relaxed: true}` and such writes
go through with the reason `unspecified`, which still shows up in `why`.

The actor comes from, in order: `xkv.Actor(...)` on the write, the actor set
with `xkv.WithActor` on the context, `Options.DefaultActor`, then `unknown`.

## Causes

A cause is a pointer to one earlier revision: a key and a revision number.

- `xkv.Cause(key, rev)` points at that exact revision.
- `xkv.CauseLatest(key)` points at the key's current revision, resolved when
  the write is made.

xkv checks every cause before writing. The revision must exist, must belong
to the named key, and must be older than the write. A write with a bad cause
is refused with `ErrBadCause` and nothing is written. Inside a transaction a
cause may point at a write made earlier in the same transaction.

Because causes always point backwards, the chains can't loop. Two causes can
lead to the same revision; `Why` shows it in full the first time and marks
it `Repeat` after that.

Good causes are the ones the writer actually acted on: the input it read, the
decision it followed, the alert it answered. A deletion can be a cause too.

## Reading the past

| Call | Returns |
|---|---|
| `Get(ctx, key)` | the current revision, or `ErrNotFound` if the key was never written or was deleted |
| `GetAt(ctx, key, rev)` | the key as the store stood at revision `rev`, or `ErrCompacted` if compaction removed that state |
| `Revision(ctx, rev)` | one revision by number, whatever its key |
| `History(ctx, key, limit)` | the key's revisions, newest first, deletes included |
| `Why(ctx, key, depth)` | the tree of causes behind the current value |
| `WhyRev(ctx, rev, depth)` | the same for one revision |
| `Keys(ctx, prefix)` | keys that currently have a value, sorted |

`Why` goes 16 levels deep by default (depth 0). A revision whose causes the
depth limit didn't reach is marked `Truncated`. Each node also carries `Prev`,
the key's revision just before it, so the tree shows what changed as well as
why. When compaction removed the revisions just before it, `Prev` is empty
and `PrevCompacted` is set, so an old value is never passed off as the one
it replaced.

## Compaction

Every revision is kept until you compact. `Compact` rewrites the file and
keeps:

- the latest revision of every key, deletes included;
- the newest `KeepLast` revisions of every key, and every revision written
  since `KeepSince`;
- every revision any of those reach through causes, at any depth.

The last rule is what makes it safe. A chain that `Why` can follow before
compaction, it can follow after. Old revisions that nothing points at go.

```go
stats, err := db.Compact(ctx, xkv.CompactOptions{KeepLast: 10, KeepSince: time.Now().AddDate(0, -3, 0)})
```

Compaction writes a new file next to the old one and renames it over the
original, so a compaction cut short leaves the old file whole. Reads carry
on while it runs; writes wait. On Windows a file can't be renamed over while
another process has it open, so there compaction fails with nothing changed
while a reader holds the file; run it again once the reader has closed.

## The xkv command

| Command | Does |
|---|---|
| `xkv put FILE KEY VALUE --reason R [--cause K[@REV]]... [--actor A] [--ref REF]` | write a value (`--stdin` reads it from standard input) |
| `xkv del FILE KEY --reason R ...` | delete a key |
| `xkv get FILE KEY [--at REV]` | print the value |
| `xkv log FILE KEY [--limit N]` | a key's revisions, newest first |
| `xkv why FILE KEY [--depth N]` | the chain of causes; `--rev REV` for one revision |
| `xkv rev FILE REV` | one revision by number |
| `xkv keys FILE [PREFIX]` | keys that have a value |
| `xkv stats FILE` | counts |
| `xkv check FILE` | read every record and every cause |
| `xkv compact FILE [--keep N] [--keep-days D]` | compact |
| `xkv repair FILE` | trim a damaged file back to its last good transaction |
| `xkv serve FILE [--addr HOST:PORT]` | HTTP mode |

Most commands take `--json`. Writes need a reason unless `--relaxed` is
given. The actor is `--actor`, else `$XKV_ACTOR`, else your user name. Read
commands open the file read-only, so they work while a server holds it.
Exit status is 0 on success, 3 when the key or revision isn't there, 2 for
a usage mistake and 1 for any other error.

## HTTP mode

`xkv serve` makes the store usable from any language, with JSON over HTTP on
localhost:

```sh
curl -X PUT localhost:7070/v1/keys/config/db/timeout \
  -d '{"value": "30s", "reason": "replica lagging", "causes": [{"key": "alert:orders-db-latency"}]}'
curl localhost:7070/v1/why/config/db/timeout
```

[docs/http.md](docs/http.md) lists every endpoint.

## In the browser

`OpenMemory` gives a store that lives in memory and behaves like a file. The
`wasm/` folder builds it for the browser; the demo on
[xkv.org](https://xkv.org) runs on it.

```sh
GOOS=js GOARCH=wasm go build -o xkv.wasm ./wasm
```

## What xkv promises, and what it doesn't

It promises:

- **A transaction is all or nothing.** Its writes become visible together.
  A crash in the middle of one leaves none of it behind.
- **An acknowledged write survives a crash.** When `Put` or `Update` returns,
  the data is fsynced. (Open with `NoSync` to trade that for speed; a killed
  process still loses nothing, a power cut can lose the last writes.)
- **Committed data is never dropped quietly.** A torn end of file is trimmed
  back to the last whole transaction. Damage in the middle of the file is
  refused with `ErrCorrupt`, never trimmed. Whatever an open does cut off
  is first copied to `FILE.trimmed-<time>`. `xkv repair` (or
  `Options.Repair`) trims a damaged file on purpose, keeping the cut bytes
  the same way.
- **A failed write stops the writer.** If a write or fsync fails, the store
  refuses further writes with `ErrFailed` until it is reopened, so it never
  carries on from a state it can't vouch for.
- **Compaction never breaks a chain.** See above.
- **Revision numbers only go up**, also across compaction.

It doesn't:

- **check that reasons are true.** It records what the writer said. A
  reason is as good as the code that writes it.
- **share one file between writing processes.** One process writes; others
  can open the file read-only and see it as it was when they opened it.
  Run `xkv serve` for many writers. The lock is `flock` on Unix and
  `LockFileEx` on Windows; on platforms with neither (Solaris, AIX, the
  browser) nothing stops two writers, so don't start two.
- **replicate, sync or authenticate.** The HTTP mode is for localhost.
- **scale to huge data.** The index of every revision lives in memory,
  roughly 70 bytes per revision. Values belong in the store; big blobs
  don't.

The file format is written up byte by byte in
[docs/format.md](docs/format.md).

## How it was tested

Every operation has tests, and the promises above have tests aimed at them.
The numbers here come from [test/results](test/results), written by
`scripts/record-tests.sh` on a 2-CPU Linux machine.

- **Torn writes.** The last transaction of a file was cut at each of its
  397 byte positions, and damaged at each of its bytes. Every open
  trimmed back to the committed revisions, answered `Get` and `Why` as before,
  and the next write carried on the numbering.
- **SIGKILL.** A writing process was killed at random moments
  1000 times on the same file. 53,701 transactions were
  acknowledged and none was lost; none was ever half visible; every cause in
  the file still resolved.
- **Damage.** A flipped byte in the middle of the file is refused, not
  trimmed, and `xkv check` points at it.
- **Compaction.** On 20 random workloads, `Get`, full-depth `Why`
  and the kept history gave the same answers before and after compaction,
  after reopening, and after a full rescan. 13,659 of the
  15,299 kept revisions were kept only because a kept revision names
  them as a cause.
- **Killed during compaction.** 40 kills landed at random points of
  a compaction; whether before or after the rename, the store answered as
  before and no work file was left behind.
- **Two processes.** A second writer gets `ErrLocked`; a read-only open
  works; the lock dies with the process.
- **Fuzzing.** Arbitrary bytes as a store file, and as records, never panic.
- **A million revisions.** One million revisions over ten thousand keys were
  written in 5.0 s with an fsync per 100-write transaction, into a 124 MB
  file. `Get` took 2.0 µs and `Why` 16 levels deep 87 µs. The file opened in
  181 ms with the index snapshot and 758 ms with a full scan. The index took
  67 bytes per revision. Compacting to each key's latest took 332 ms and
  left 5.2 MB, keeping 33,338 older revisions because kept ones name them as
  causes.

Benchmarks on the same machine: a write with its fsync takes
157 µs; without the fsync 1.8 µs; `Get` 0.76 µs;
`Why` through a 16-revision chain 25 µs.

## Build it yourself

You need Go 1.24 or later.

```sh
git clone https://github.com/explainable-kv/xkv
cd xkv
go test ./...
go build ./cmd/xkv
```

`go run ./examples/account-limit` and its three siblings run the case
studies from the site.

## The name

XKV is the explainable key-value store. The X is for the question every value
should be able to answer: why.

## License

Apache License 2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
Copyright xkv.org 2026.
