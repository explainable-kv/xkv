// Copyright xkv.org 2026
// SPDX-License-Identifier: Apache-2.0
// Tests the TypeScript client (its compiled xkv.js) against a real `xkv serve`.
//   XKV_BIN=path/to/xkv node --test test.mjs
import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdtempSync, writeFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { randomBytes } from "node:crypto";
import { createServer } from "node:net";
import { Client, XkvError, valueBytes } from "./xkv.js";

const XKV = process.env.XKV_BIN ?? "xkv";
const dir = mkdtempSync(join(tmpdir(), "xkv-ts-"));
const write = randomBytes(24).toString("hex");
const read = randomBytes(24).toString("hex");
let proc, url;

function freePort() {
  return new Promise((res) => {
    const s = createServer().listen(0, "127.0.0.1", () => { const p = s.address().port; s.close(() => res(p)); });
  });
}

before(async () => {
  writeFileSync(join(dir, "w"), write + "\n", { mode: 0o600 });
  writeFileSync(join(dir, "r"), read, { mode: 0o600 });
  const port = await freePort();
  url = `http://127.0.0.1:${port}`;
  proc = spawn(XKV, ["serve", join(dir, "s.xkv"), "--addr", `127.0.0.1:${port}`, "--token-file", join(dir, "w"), "--read-token-file", join(dir, "r")]);
  await new Promise((res, rej) => {
    proc.stdout.once("data", (d) => (String(d).includes("serving") ? res() : rej(new Error(String(d)))));
    proc.once("exit", (c) => rej(new Error("serve exited " + c)));
  });
});

after(async () => {
  // Wait for serve to exit before removing its directory.
  const exited = new Promise((res) => proc.once("exit", res));
  proc.kill();
  await exited;
  rmSync(dir, { recursive: true, force: true });
});

test("write, read and explain", async () => {
  const db = new Client(url, { token: write, actor: "agent-7" });
  const alert = await db.put("alert:latency", "p99 2.4s", { reason: "seen by the monitor" });
  const r = await db.put("config/db/timeout", "30s", { reason: "stop requests timing out", causes: ["alert:latency"], ref: "INC-1" });
  assert.equal(r.actor, "agent-7");
  assert.deepEqual(r.causes, [{ key: "alert:latency", rev: alert.rev }]);
  assert.equal((await db.get("config/db/timeout")).value, "30s");
  const tree = await db.why("config/db/timeout");
  assert.equal(tree.causes[0].revision.key, "alert:latency");
  const prov = await db.whyProv("config/db/timeout");
  assert.deepEqual(new Set(prov["@graph"].map((n) => n["@type"])), new Set(["prov:Entity", "prov:Activity", "prov:Agent"]));
  assert.equal((await db.whyRev(r.rev)).revision.rev, r.rev);
  assert.equal((await db.whyRevProv(r.rev))["@graph"][0]["@id"].endsWith(`:rev:${r.rev}`), true);
  assert.equal((await db.revision(r.rev)).ref, "INC-1");
  assert.ok((await db.keys("config/")).includes("config/db/timeout"));
  await db.put("config/db/timeout", "45s", { reason: "still short", causes: [{ key: "alert:latency", rev: alert.rev }], actor: "sam" });
  const hist = await db.history("config/db/timeout", 5);
  assert.deepEqual(hist.map((h) => h.value), ["45s", "30s"]);
  const d = await db.delete("config/db/timeout", { reason: "back to the default" });
  assert.equal(d.deleted, true);
  assert.equal(valueBytes(d), undefined);
  assert.ok((await db.stats()).revisions >= 4);
});

test("bytes and keys that need escaping", async () => {
  const db = new Client(url, { token: write });
  const r = await db.put("blob/a b?c#d", new Uint8Array([255, 0, 1]), { reason: "binary" });
  assert.equal(r.key, "blob/a b?c#d");
  assert.deepEqual([...valueBytes(await db.get("blob/a b?c#d"))], [255, 0, 1]);
});

test("keys with dot and empty segments stay as given", async () => {
  const db = new Client(url, { token: write });
  await db.put("real", "untouched", { reason: "setup" });
  for (const key of ["zz/../real", "q/./x", "x//y", "/lead", "trail/", "a%2Fb"]) {
    const r = await db.put(key, "v:" + key, { reason: "odd key" });
    assert.equal(r.key, key);
    assert.equal((await db.get(key)).value, "v:" + key);
  }
  assert.equal((await db.get("real")).value, "untouched");
  for (const bad of [".", ".."]) assert.throws(() => db.get(bad));
});

test("watch stops for an aborted signal", async () => {
  const db = new Client(url, { token: read });
  const ac = new AbortController();
  ac.abort();
  const got = [];
  for await (const rev of db.watch({ since: 0, signal: ac.signal })) got.push(rev);
  assert.deepEqual(got, []);
  // An abort during the wait before a reconnect ends watch at once.
  const ac2 = new AbortController();
  const dead = new Client(`http://127.0.0.1:${await freePort()}`, { token: read });
  const t0 = Date.now();
  setTimeout(() => ac2.abort(), 100);
  for await (const _ of dead.watch({ since: 0, signal: ac2.signal, retryMs: 10000 })) assert.fail("no revisions");
  assert.ok(Date.now() - t0 < 2000);
});

test("errors", async () => {
  const db = new Client(url, { token: write });
  await assert.rejects(db.put("k", "v"), (e) => e instanceof XkvError && e.status === 400);
  await assert.rejects(db.get("missing"), (e) => e.status === 404);
  await assert.rejects(new Client(url).stats(), (e) => e.status === 401);
  await assert.rejects(new Client(url, { token: read }).put("k", "v", { reason: "r" }), (e) => e.status === 403);
  assert.ok("revisions" in (await new Client(url, { token: read }).stats()));
  await assert.rejects(new Client(`http://127.0.0.1:${await freePort()}`).stats(), (e) => e.status === 0);
});

test("export and import", async () => {
  const db = new Client(url, { token: write });
  await db.put("shared/limit", "10", { reason: "initial" });
  const { data, through } = await db.export();
  assert.ok(through > 0 && data.length > 0);
  // Importing a store's own export skips everything; the report says so.
  const report = await db.importExchange(data);
  assert.equal(report.imported, 0);
});

test("watch", async () => {
  const db = new Client(url, { token: write });
  const start = (await db.stats()).last_rev;
  setTimeout(async () => { for (let i = 0; i < 3; i++) await db.put("watched/k", String(i), { reason: "tick" }); }, 200);
  const got = [];
  for await (const rev of new Client(url, { token: read }).watch({ since: start, prefix: "watched/" })) {
    got.push(rev.value);
    if (got.length === 3) break;
  }
  assert.deepEqual(got, ["0", "1", "2"]);
});
