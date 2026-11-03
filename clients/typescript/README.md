# xkv client for TypeScript and JavaScript

`xkv.ts`, built to `xkv.js` and `xkv.d.ts` (both checked in, and checked in
CI to match the source). No dependencies: it uses the platform's `fetch`, so
it runs in Node 20 or later, Deno, Bun and browsers. It talks to `xkv serve`
(see [docs/http.md](../../docs/http.md)).

Copy `xkv.js` and `xkv.d.ts` (or `xkv.ts`) into your project, or fetch them
for a release tag:

```sh
curl -fsSLO https://raw.githubusercontent.com/explainable-kv/xkv/v0.4.0/clients/typescript/xkv.js
curl -fsSLO https://raw.githubusercontent.com/explainable-kv/xkv/v0.4.0/clients/typescript/xkv.d.ts
```

```ts
import { Client, XkvError } from "./xkv.js";

const db = new Client("https://xkv.internal:7070", { token: process.env.XKV_TOKEN, actor: "agent-7" });
await db.put("config/db/timeout", "30s", {
  reason: "stop requests timing out",
  causes: ["alert:orders-db-latency"],
  ref: "INC-4410",
});

const tree = await db.why("config/db/timeout");      // xkv's tree of causes
const prov = await db.whyProv("config/db/timeout");  // W3C PROV-O JSON-LD

for await (const rev of db.watch({ prefix: "config/" })) {
  console.log(rev.key, rev.value, rev.reason);
}
```

Methods: `put`, `delete`, `get(key, at)`, `keys(prefix)`, `history`, `why`,
`whyProv`, `whyRev`, `whyRevProv`, `revision`, `stats`, `export(since)`
(the bytes and the `since` for next time), `importExchange(data, conflicts)`
and `watch({since, prefix, all, signal})`. Values are strings or
`Uint8Array`s (sent as base64); `valueBytes(revision)` gives any value back
as bytes.

Errors throw `XkvError` with `status` (the HTTP status, or 0 when the server
couldn't be reached). `watch` reopens a dropped connection from the last
revision it yielded, and throws `XkvError` (status 410) when compaction
removed revisions it hadn't sent. Browsers can't send the Authorization
header with `EventSource`, which is why `watch` reads the JSON-lines stream
with `fetch` instead.

To trust a private certificate authority, pass a `fetch` that uses it (in
Node, for example, one built on undici's `Agent` with your CA), or set
`NODE_EXTRA_CA_CERTS` when starting Node. `watch` reconnects when nothing
arrives for 60 seconds (`idleMs`); the server sends a keep-alive every 15.

Build and test: `npm ci && npx tsc -p tsconfig.json && XKV_BIN=path/to/xkv node --test test.mjs`.
