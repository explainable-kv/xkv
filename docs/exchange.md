# Exchange files

`Export` (and `xkv export`) writes the changes in one store as an exchange
file, and `Import` (`xkv import`) adds them to another. The reasons, actors,
refs, times and causes travel with the values, so `Why` gives the same
answer on both sides. This page describes the file, line by line, and what
an import does with it.

## The idea

Every revision has an identity that doesn't change when it moves: the id of
the store it was first written to and its number there, written
`STOREID:REV`. A store's id is 32 hex digits, drawn when the store is created
and kept through compaction (`xkv stats` shows it). An imported revision gets
a new number in the store that imports it, and keeps its identity. That is
how an import knows what it already has, and how causes find their way.

Export takes a revision number, `since`, and writes every revision above
it. It also writes every older revision those reach through causes, marked
as history, so a chain arrives whole. The export's `through` is the store's
last revision at the time; pass it as `since` next time to send only what's
new.

## The file

UTF-8 text, one JSON object per line, each line ending in `\n`.

The first line is the header:

```json
{"xkv_exchange":1,"store":"5fef3c64616e1ee9ff99d06c0c363ef3","since":0,"through":7,"created":"2026-10-19T09:20:42.118Z"}
```

Then one line per transaction of the exporting store, in its order:

```json
{"tx":[{"id":"5fef3c64616e1ee9ff99d06c0c363ef3:5","key":"account:42:limit","value":"50","actor":"limits-service","reason":"risk policy v3","time":"2026-10-19T09:00:05Z","causes":[{"key":"risk:42:score","id":"5fef3c64616e1ee9ff99d06c0c363ef3:3"}],"prev":"5fef3c64616e1ee9ff99d06c0c363ef3:4"}]}
```

| Field | Meaning |
|---|---|
| `id` | the revision's identity, `STOREID:REV` |
| `key` | the key |
| `value` | the value as text; left out for a delete |
| `encoding` | `"base64"` when `value` is base64-encoded bytes |
| `deleted` | `true` for a delete |
| `actor`, `reason`, `ref` | as written |
| `time` | the time recorded when it was first written |
| `causes` | `[{"key": K, "id": ID}]`, each an earlier revision in this file or one the importing store holds |
| `history` | `true` for a revision included as a cause only, or held as history by the exporting store |
| `prev` | the identity of the key's revision before this one in the exporting store, if it has one; not on history revisions |
| `prev_compacted` | `true` when the exporting store's compaction removed the revisions before this one |

A cause always comes before the revision naming it.

The last line closes the file:

```json
{"end":{"lines":3,"revisions":7,"sha256":"0a0d44dd..."}}
```

`lines` counts the lines before it, header included; `revisions` counts the
revisions in them; `sha256` is the SHA-256 of every byte before it. Nothing
may follow.

## What an import does

It reads the whole file and checks it first: the header, every field, the
end line's counts and checksum, and that every cause is either earlier in
the file or already in the store. A file that fails any check is refused
with `ErrBadExchange` and nothing is written. Then, for each transaction in
the file, in order, one transaction in the store:

- A revision the store already holds, by identity, is skipped. Importing the
  same file twice changes nothing, and so does importing a store's own
  revisions back into it.
- A history revision is added as history.
- A revision of a key the store hasn't changed since, as far as it can tell,
  is added and becomes the key's value. That is when the key's current
  revision here is the one named as `prev`, or an earlier revision from the
  same store as the incoming one, or there is none.
- Otherwise both sides changed the key. If both arrived at the same value,
  the incoming revision is added as history and nothing else happens.
  If the values differ, it is a conflict: the incoming revision is added to
  the key's history, then a resolution revision is written that names the
  key's revision here and the incoming one as its causes. Its value is the
  one the conflict policy picks; its actor and ref are the import's, its
  reason the import's reason followed by `(import conflict: kept ours)` or
  `(... kept theirs)`, and its time the winning change's time.

The policies:

| Policy | Keeps |
|---|---|
| `newest` (the default) | the change with the later time; for the same time, a value over a delete, then the bytewise larger value. Both stores pick the same winner, so two stores exchanging both ways end up alike |
| `ours` | the importing store's value |
| `theirs` | the incoming value |

An import needs a reason, like any write in strict mode, for the
resolutions it may write.

The import report gives the counts (imported, kept as history, already
here, changed alike) and each conflict: the key, our revision, theirs (by
its number here), and the resolution.

An import killed part-way leaves every transaction it finished, whole, and
none of the rest. Running it again finishes the job.

## Limits

- An exchange file is read into memory whole before anything is written.
- A revision the importing store holds only as history stays history, even
  if a later file carries it as a change.
- Exporting needs a version 2 store. `Export` and `Import` upgrade a
  writable version 1 store on their own; `xkv export` opens the file
  read-only and asks for `xkv upgrade` first.
