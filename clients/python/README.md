# xkv client for Python

One file, `xkv_client.py`, standard library only, Python 3.9 or later. It
talks to `xkv serve` (see [docs/http.md](../../docs/http.md)).

```sh
pip install "git+https://github.com/explainable-kv/xkv@v0.4.0#subdirectory=clients/python"
```

or copy `xkv_client.py` next to your code.

```python
from xkv_client import Client, XkvError

db = Client("https://xkv.internal:7070", token=open("/etc/xkv/token").read().strip(), actor="agent-7")
alert = db.put("alert:orders-db-latency", "p99 2.4s", reason="seen by the monitor")
db.put("config/db/timeout", "30s", reason="stop requests timing out",
       causes=["alert:orders-db-latency"], ref="INC-4410")

tree = db.why("config/db/timeout")             # xkv's tree of causes
prov = db.why("config/db/timeout", prov=True)  # the same as W3C PROV-O JSON-LD

for rev in db.watch(prefix="config/"):         # new revisions as they commit
    print(rev["key"], rev["value"], rev["reason"])
```

Methods: `put`, `delete`, `get(key, at=)`, `keys(prefix=)`, `history`,
`why`, `why_rev`, `revision`, `stats`, `export(since=)` (returns the bytes
and the `since` for next time), `import_exchange(data, conflicts=)` and
`watch(since=, prefix=, all=)`. Causes are keys (their current revision),
`(key, rev)` pairs or `{"key", "rev"}` dicts. Bytes values are sent as
base64; `value_bytes(revision)` turns any revision's value back into bytes.

Errors raise `XkvError` with `status` (the HTTP status, or 0 when the server
couldn't be reached) and `message`. `watch` reconnects from the last revision
it yielded when the connection drops, so it misses nothing and repeats
nothing; it raises `XkvError` (status 410) when compaction removed revisions
it hadn't sent yet.

Tests run against a real server: `XKV_BIN=path/to/xkv python3 -m unittest test_xkv_client`.
