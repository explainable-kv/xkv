# The xkv file format, versions 1 and 2

An xkv store is one file. This page describes it byte by byte, so the file
can be read without this package and checked by hand.

xkv 0.1 wrote version 1. xkv 0.2 writes version 2, which adds a store id to
the header and lets a revision say where it was imported from. 0.2 reads
both. A version 1 file stays version 1 while it is only written to; it
becomes version 2 when it is compacted, upgraded (`xkv upgrade`), or used
for an export or import. xkv 0.1 can't open a version 2 file.

All integers in fixed-size fields are little-endian. "uvarint" and "varint"
are the variable-length integers of Go's `encoding/binary`, in their shortest
form (a reader rejects any longer encoding, so a record has exactly one byte
form). Strings are UTF-8 and are written as a uvarint length followed by the
bytes. Checksums are CRC-32C (Castagnoli).

## Layout

```
header (32 bytes in version 1, 48 in version 2)
frame
frame
...
```

The file only ever grows at the end, except when compaction replaces it
whole, and when an open trims an incomplete transaction off the end.

## Header

Version 2:

| Offset | Size | Field |
|---|---|---|
| 0 | 4 | magic `XKVF` |
| 4 | 2 | format version, `2` |
| 6 | 2 | flags, `0` |
| 8 | 16 | generation: random bytes, new for every file (compaction writes a new one) |
| 24 | 16 | store id: random bytes drawn when the store is created or upgraded, kept by compaction |
| 40 | 4 | reserved, `0` |
| 44 | 4 | CRC-32C of bytes 0 to 43 |

Version 1 is the same without the store id: the reserved bytes at 24 and
the CRC-32C of bytes 0 to 27 at 28, 32 bytes in all.

A reader refuses a header whose flags or reserved bytes aren't zero, and a
version it doesn't know.

## Frames

Every record sits in a frame:

| Size | Field |
|---|---|
| 4 | body length *n*, from 1 to 16,826,513 (the largest record the limits allow) |
| 4 | CRC-32C of the body |
| *n* | body |

The first byte of the body is the record type: `1` for a revision, `2` for a
commit.

## Revision record (type 1)

| Field | Encoding |
|---|---|
| type | byte `1` |
| rev | uvarint, 1 or more, strictly increasing through the file |
| tx | uvarint, the transaction id, 1 or more |
| flags | byte: bit 0, this revision deletes the key; bit 1, it was imported and its origin follows the causes; bit 2, it is history only (see below); other bits zero |
| time | varint, nanoseconds since 1970-01-01 UTC |
| key | string, 1 to 4096 bytes |
| value | uvarint length + bytes, up to 16 MiB; empty for a delete |
| actor | string, up to 256 bytes |
| reason | string, up to 4096 bytes |
| ref | string, up to 1024 bytes |
| cause count | uvarint, 0 to 8 |
| causes | for each: key (string) then rev (uvarint) |
| origin | only with flag bit 1: the store id it was first written to (16 bytes), then its revision number there (uvarint, 1 or more) |

Bits 1 and 2 appear only in version 2 files. An imported revision keeps its
origin through every later exchange, so the pair (store id, revision) names
it in every store that holds it. A revision written to this store has no
origin field; its identity is this file's store id and its own number.

A history revision (bit 2) was imported for the record only: because an
imported revision names it as a cause, or because it made the same change
this store had already made. It is part of the file like any other, and causes may
point at it, but it is not in its key's list of revisions: it is never the
key's current value, `History` doesn't list it, and it has no known
previous revision.

Each cause names an earlier revision: its rev is at least 1 and lower than
this revision's rev. The writer checks, before writing, that the cause exists
and belongs to the named key.

## Commit record (type 2)

| Field | Encoding |
|---|---|
| type | byte `2` |
| tx | uvarint, the transaction being committed |
| count | uvarint, how many revision records the transaction holds |
| last rev | uvarint, the rev of its last revision |
| gap count | uvarint, 0 for an ordinary write; at most count |
| gaps | that many uvarints, ascending: revs of this transaction whose key had revisions just before them that compaction removed |

A transaction is its revision records, one after another with the same tx,
followed by its commit record. Transaction ids increase through the file.
The writer writes a whole transaction, commit included, with one write call,
then calls fsync (unless the store was opened with `NoSync`).

## Reading and recovery

A reader goes through the frames in order and keeps the revisions of the
open transaction aside until its commit record arrives. A transaction counts
only when its commit record is intact and matches: same tx, the right count,
the right last rev. Reading stops at the first frame that isn't intact (short,
too long, wrong checksum, malformed, or out of order).

What follows that point is one of two things:

- **A torn tail.** A crash or power loss in the middle of the last write
  leaves part of a transaction with no valid commit after it. A writable
  open truncates the file back to the end of the last complete transaction,
  after copying the bytes it cuts off to `<file>.trimmed-<UTC time>`.
  A read-only open ignores the tail and leaves the file alone.
- **Damage.** If a complete, valid transaction can be found after the break,
  the break isn't a crash: data in the middle of the file has changed. Open
  refuses with `ErrCorrupt` and changes nothing. `xkv check` reports where;
  `xkv repair` (or `Options.Repair`) trims at the break, copying the rest
  aside the same way.

The search for a complete transaction steps over frames that read back
whole, so the contents of values aren't searched. It only searches the
unreadable stretches byte by byte, and it computes a checksum only for a
candidate that passes cheap checks: numbers above the last committed ones,
a valid flags byte, a plausible key length or revision count. The checksum
work is capped; past the cap, or with more than 128 MiB after the break,
the file is treated as damaged.

Two cases can't be told apart from the bytes alone. Damage confined to the
last transaction looks like a torn write and is trimmed; its bytes are kept
in the `.trimmed-` file. A torn write whose value itself contains a whole
xkv file looks like damage and is refused until repaired. With `NoSync`, a
power cut can leave whole transactions after a torn one, which also needs a
repair.

## Index snapshot: `<file>.idx`

The snapshot is a cache that lets a large file open without reading every
record. It is written on `Close` and after compaction, through a temporary
file and a rename, with the data file's permissions.

| Field | Encoding |
|---|---|
| magic | `XKVI` |
| version | 2 bytes, `2` (xkv 0.1 wrote `1`; such a snapshot is ignored) |
| generation | 16 bytes, copied from the data file's header |
| end | uvarint, the data file's length when the snapshot was written |
| mtime | varint, the data file's modification time then, in nanoseconds |
| last commit offset | uvarint, offset of the last commit frame it covers |
| last commit checksum | 4 bytes, that frame's CRC-32C field |
| last rev, last tx | uvarints |
| key table | uvarint count, then each key as a string |
| store table | uvarint count, then 16-byte store ids, for the origins of imported revisions |
| entries | uvarint count, then for each revision: rev delta, tx, offset, frame length (uvarints), flags (byte: bit 0 deleted, bit 1 gap, bit 2 history, bit 3 imported), time (varint), then the key number (uvarint), or for a history revision the key itself (string), then for an imported one its origin's store number and revision (uvarints) |
| checksum | 4 bytes, CRC-32C of everything before it |

Open uses the snapshot only if it describes the data file exactly: its
checksum is right, the generation matches, the file is exactly `end` bytes
long with the same modification time, and the commit frame at the stated
offset still carries the stated checksum. Otherwise (writes after it, a
crash, a file restored from a backup) Open ignores it and reads the whole
file. Every record read through the index is checked against it too: rev,
tx, key and delete flag must match. Deleting the snapshot is always safe.

## Compaction work file: `<file>.compact`

Compaction writes the kept revisions to `<file>.compact`, with a new
version 2 header (the same store id, a new generation) and the data file's
permissions. Upgrading a version 1 file is a compaction that keeps every
revision. Frames are copied byte for
byte; each transaction's commit record is rewritten with the count of its
revisions that remain, and lists as gaps the kept revisions whose key had
revisions just before them that are now gone (or were already gaps). Gaps
are how `GetAt` knows to answer `ErrCompacted` instead of an older value,
and how `Why` knows a revision's previous value is unknown. The file is
fsynced and renamed over the original, which is atomic. A compaction killed
before the rename leaves the original whole; the next writable open deletes
the work file.

Compaction keeps:

1. the latest revision of every key (deletes included, so a deleted key stays
   deleted),
2. the newest *KeepLast* revisions of every key, and those written since
   *KeepSince*,
3. every revision reachable from those through causes, at any depth.

History revisions belong to no key's list, so only rule 3 keeps them.

Revision numbers and transaction ids are never reused.
