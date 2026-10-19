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

Each store's revisions of one key form one line, in the order it numbered
them. So for one key, a store's knowledge fits in a short list: for each
store, the highest number among that store's revisions of the key it holds.
That list is the key's *seen* list (a version vector). Every revision
stores the seen list its writer had just before writing it, and keeps it
when it moves to another store. That is how an import tells an update from
a conflict, even for a change that reached it through a third store.

Export takes a revision number, `since`, and writes every revision above
it: the changes, and the revisions this store took in as history since
then (the losing side of a conflict, a change both sides made alike). It
also writes every older revision those reach through causes, marked as
history, so a chain arrives whole. The export's `through` is the store's
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
{"tx":[{"id":"5fef3c64616e1ee9ff99d06c0c363ef3:5","key":"account:42:limit","value":"50","actor":"limits-service","reason":"risk policy v3","time":"2026-10-19T09:00:05Z","causes":[{"key":"risk:42:score","id":"5fef3c64616e1ee9ff99d06c0c363ef3:3"}],"seen":{"5fef3c64616e1ee9ff99d06c0c363ef3":4}}]}
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
| `seen` | `{"STOREID": REV, ...}`: the seen list the revision's writer had for the key just before writing it, history included, without the writer's own store (its line is implied by `id`); left out when empty |

The fields appear in that order, and each line is exactly what Go's
`encoding/json` writes for it, without spaces. An import refuses a line
written any other way (extra text, a repeated field, a field name in
another case), so a file has one form only. `actor`, `reason` and `time`
are required; `time` must lie between the years 1678 and 2262, the range
of a 64-bit count of nanoseconds since 1970.

A cause always comes before the revision naming it.

The last line closes the file:

```json
{"end":{"lines":3,"revisions":7,"sha256":"0a0d44dd..."}}
```

`lines` counts the lines before it, header included; `revisions` counts the
revisions in them; `sha256` is the SHA-256 of every byte before it. Nothing
may follow.

## What an import does

It reads the whole file and checks it first: the header, every line's form
and fields, and the end line's counts and checksum. Then it checks the file
against the store:

- a revision the store holds, by identity, must hold exactly the same key,
  value, actor, reason, ref and time;
- no revision may claim this store's id with a number above anything it has
  written, and no seen list may claim to have seen such a revision;
- no time may lie more than a year past this store's clock (a write here is
  always recorded later than the revisions of its key, and a time at the
  end of the range would leave no room);
- no key may end up having seen more than 4095 other stores, so the seen
  list of a write here always fits in a revision;
- every cause is earlier in the file or held by the store, as a revision of
  the key the file names.

Two stores that share a store id fail the first two checks: a copied store
file, or one restored from a backup and written to since. Run `xkv new-id`
on the copy (see below) and import again.

A file that fails any check is refused with `ErrBadExchange` and nothing is
written. Then, for each transaction in the file, in order, one transaction
in the store. Compaction waits until the import is done; other writes may
come between its transactions.

- A revision the store already holds, by identity, is skipped. Importing the
  same file twice changes nothing, and so does importing a store's own
  revisions back into it.
- A history revision is added as history if a revision the import adds
  names it as a cause, or if the store hasn't seen it; otherwise it is
  skipped.
- A change the store's seen list for the key already covers (it holds that
  revision's store's line of the key at or past it) is an older change,
  superseded here: skipped, or kept as history if something names it.
- A change is judged against the key's current value here. If its writer
  had seen that value, and everything that value's writer had seen, it is
  an update: it is added and becomes the value. If the current value's
  writer had seen the change instead, the change is added as history.
- Otherwise both sides changed the key without seeing the other's change.
  If both arrived at the same value, the newer of the two (as `newest`
  would pick) stands as the value and the other is history; the value
  reads the same either way. If the values differ, it is a
  conflict. The policy picks the winner: the winner is the key's value,
  and the loser is kept in its history, so `History` lists both and marks
  the loser. No new revision is written to settle it; a later change made
  knowing the winner is simply an update, on every store.

Compaction keeps, for every key, the newest revision from each store it has
revisions from (newest by that store's own numbering), so a key's seen list survives compaction and importing an
old file after compacting still changes nothing.

The policies:

| Policy | Keeps |
|---|---|
| `newest` (the default) | the change with the later time; for the same time, a value over a delete, then the bytewise larger value. Both stores pick the same winner, so two stores exchanging both ways end up alike |
| `ours` | the importing store's value |
| `theirs` | the incoming value |

`newest` can't be fooled by clocks into undoing a change made later: every
write records a time later than every revision of its key its store held
then, a nanosecond later if the clock reads earlier. So a change made
knowing another is also newer than it, and every store ends up with the
newest of the changes no one saw past.

An import writes nothing of its own, so it needs no reason: every revision
it adds keeps the actor, reason, ref and time it was written with.

The import report gives the counts (imported, kept as history, already
here, changed alike) and each conflict: the key, our revision, theirs (by
its number here), and which one is the value now.

An import killed part-way leaves every transaction it finished, whole, and
none of the rest. Running it again finishes the job.

An imported transaction may grow to twice `MaxTxSize` (128 MiB) here, room
for the identities and seen lists it now carries, so any transaction
another store could write can be imported.

## Copies of a store

Copying a store file copies its store id, and the copy's revision 5 and the
original's revision 5 then claim the same identity. An import between them
is refused. To make a copy that exchanges with the original, give it a new
id right after copying, before either side exchanges anything:

```sh
cp site-a.xkv staging.xkv
xkv new-id staging.xkv --keep-through 1200
```

`--keep-through` is required. It names the last revision the two stores
had in common: revisions up to it keep their old identity, so the two
stores still recognise their shared history, and revisions after it become
the copy's own, under its new id. Right after a copy that is the copy's
last revision (`xkv stats` shows it); for a store restored from an old
backup, it is the backup's last revision. A new id can't be undone.

## Limits

- An exchange file is read into memory whole before anything is written.
- A revision the importing store holds only as history stays history, even
  if a later file carries it as a change. Normally that is right: it arrived
  as history because a revision here already supersedes it. But an export
  with a `since` past changes the importer never got sends those only as
  context, and the stores' values can then differ until the key is written
  again. Keep one `since` per pair of stores: the `through` of the last
  file this store imported from that one.
- Exchange files are trusted input. An import checks a file's form, its
  checksum and everything it says about revisions this store holds or
  wrote, but it can't check what a seen list says about a third store. A
  file claiming more of store C than C wrote makes the importer skip C's
  real changes to that key as old ones, and C then refuses every file that
  carries the claim on. Import only files from stores you run.
- With `ours` or `theirs` on both sides, two stores can settle a conflict
  differently and keep different values; only `newest` makes them agree.
  A change made after that, knowing the value, is an update everywhere.
- Exporting needs a version 2 store. `Export` and `Import` upgrade a
  writable version 1 store on their own; `xkv export` opens the file
  read-only and asks for `xkv upgrade` first.
