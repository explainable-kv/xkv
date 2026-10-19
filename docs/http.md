# HTTP mode

`xkv serve FILE` opens the store for writing and serves it over HTTP with
JSON bodies, so programs in any language can use it. It listens on
`127.0.0.1:7070` unless `--addr` says otherwise. There is no authentication:
keep it on localhost or behind something that checks who is calling.

```sh
xkv serve settings.xkv --addr 127.0.0.1:7070
```

## Endpoints

| Method and path | Does |
|---|---|
| `GET /v1/keys?prefix=P` | `{"keys": [...]}`: keys that have a value, sorted |
| `GET /v1/keys/KEY` | the key's current revision |
| `GET /v1/keys/KEY?at=REV` | the key's revision as the store stood at REV |
| `PUT /v1/keys/KEY` | write a value |
| `DELETE /v1/keys/KEY` | delete the key |
| `GET /v1/history/KEY?limit=N` | the key's revisions, newest first |
| `GET /v1/why/KEY?depth=N` | the tree of causes behind the current value |
| `GET /v1/revs/REV` | one revision by number |
| `GET /v1/revs/REV/why?depth=N` | the tree behind one revision |
| `GET /v1/stats` | counts, the file format and the store id |
| `GET /v1/export?since=REV` | an exchange file of the changes above REV (see [exchange.md](exchange.md)); the `X-Xkv-Through` header gives the REV for next time |
| `POST /v1/import` | import the exchange file in the body; `conflicts=newest\|ours\|theirs` picks the policy. Answers with the import report; a failure part-way answers 500 with `"partial":true` and the report of what was done |
| `POST /v1/upgrade` | rewrite a 0.1 store as file format 2; an export of a format 1 store answers 409 until then |
| `GET /v1/watch?since=REV&prefix=P` (or `all=1`) | a stream of revisions as they commit; see below |

Keys may contain slashes: `/v1/keys/config/db/timeout` is the key
`config/db/timeout`. Go's HTTP router cleans paths, so keys with empty,
`.` or `..` segments (`a//b`, `a/./b`, `../x`) can't be reached over HTTP;
use the Go package or the command for those.

## Writing

```sh
curl -X PUT localhost:7070/v1/keys/config/db/timeout \
  -H 'X-Xkv-Actor: sam@oncall' \
  -d '{"value": "30s",
       "reason": "stop requests timing out while the replica catches up",
       "ref": "INC-4410",
       "causes": [{"key": "alert:orders-db-latency"}]}'
```

| Field | Meaning |
|---|---|
| `value` | the value as text; required for PUT |
| `encoding` | `"base64"` when `value` is base64-encoded bytes |
| `reason` | why; required unless the server runs with `--relaxed` |
| `actor` | who; else the `X-Xkv-Actor` header; else `"http"` |
| `ref` | an outside reference |
| `causes` | `[{"key": K, "rev": R}]`; leave out `rev` (or send 0) for the key's current revision |

A DELETE takes the same body without `value`. Unknown fields are refused
with 400, so a misspelt `"reasons"` doesn't slip through as no reason.

## Revisions

```json
{
  "rev": 3,
  "tx": 3,
  "key": "config/db/timeout",
  "value": "30s",
  "actor": "sam@oncall",
  "reason": "stop requests timing out while the replica catches up",
  "ref": "INC-4410",
  "causes": [{"key": "alert:orders-db-latency", "rev": 2}],
  "time": "2026-10-19T09:20:42.118Z"
}
```

A value that isn't valid UTF-8 comes back base64-encoded, with
`"encoding": "base64"`. A delete has `"deleted": true` and no value.

## Why trees

```json
{
  "revision": { ...the revision... },
  "prev": { ...the key's revision before it, if still held... },
  "causes": [ { "revision": ..., "prev": ..., "causes": [...] } ],
  "prev_compacted": true,
  "truncated": true,
  "repeat": true
}
```

`truncated` means the revision has causes the depth limit didn't reach.
`repeat` means the revision appears elsewhere in the tree too; its causes
are shown at its shallowest place. `prev_compacted` means compaction removed
the key's revisions just before this one, so its previous value is unknown.

## Errors

Errors come back as `{"error": "..."}` with these statuses:

| Status | When |
|---|---|
| 400 | missing reason, bad cause, bad JSON or parameters, an invalid or incomplete exchange file |
| 404 | no such key or revision |
| 410 | the revision was removed by compaction |
| 413 | value or body too large (an import body may be up to 1 GiB) |
| 503 | the store is closed (the server is shutting down) |
| 409 | an export of a store still in file format 1 |
| 500 | anything else, including a store refusing writes after a failed write (reopen it). An import that failed part-way answers 500 with `"partial": true` and the report of what went in; importing again carries on |

## Watching

`GET /v1/watch` keeps the connection open and sends each revision as its
transaction commits, in revision order. Parameters: `since` (send the
revisions above this number; leave it out for new ones only), `prefix`, and
`all=1` (from the start, skipping what compaction removed). `all` and
`since` together answer 400.

By default the body is JSON lines (`application/x-ndjson`): one revision,
as above, per line. An empty line now and then is a keep-alive. With
`Accept: text/event-stream` it is Server-Sent Events instead, each revision
an event whose `id` is its revision number, with a `: keep-alive` comment
every 15 seconds; a browser's `EventSource` can read it. When `EventSource`
reconnects on its own it sends the last id it got as `Last-Event-ID`, and
the stream carries on from there (the header wins over `since` and `all`),
so a dropped connection loses nothing and repeats nothing. The stream
starts with `retry: 3000`, the time a browser waits before reconnecting.

The stream starts at once. If revisions above `since` were removed by
compaction before they could be sent, it ends with an `{"error": "..."}`
line (or an `xkv-error` event, named so it isn't taken for `EventSource`'s
own `error` event; close the `EventSource` when you get one, or it
reconnects and gets the same answer). A client that reconnects with the
last revision it received as `since` misses nothing. A client that stops
reading for a minute is dropped.

```sh
curl -N 'localhost:7070/v1/watch?prefix=config/'
```
