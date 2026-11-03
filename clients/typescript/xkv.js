// Copyright xkv.org 2026
// SPDX-License-Identifier: Apache-2.0
/**
 * A client for xkv's HTTP mode (`xkv serve`), with no dependencies: it uses
 * the platform's fetch, so it runs in Node 20+, Deno, Bun and browsers.
 *
 *     import { Client } from "./xkv.js";
 *     const db = new Client("http://127.0.0.1:7070", { token, actor: "agent-7" });
 *     await db.put("config/db/timeout", "30s", {
 *       reason: "stop requests timing out", causes: ["alert:orders-db-latency"] });
 *
 * Methods return the server's JSON (see docs/http.md) and throw XkvError on
 * an error answer.
 */
export const version = "0.4.0";
/** An error answer: status is the HTTP status (0 when the server could not be reached). */
export class XkvError extends Error {
    status;
    partial;
    constructor(status, message, partial) {
        super(status ? `xkv: ${status}: ${message}` : `xkv: ${message}`);
        this.name = "XkvError";
        this.status = status;
        this.partial = partial;
    }
}
/** The value of a revision as bytes, decoding base64 when sent that way; undefined for a delete. */
export function valueBytes(r) {
    if (r.deleted)
        return undefined;
    const v = r.value ?? "";
    if (r.encoding === "base64") {
        const bin = atob(v);
        const out = new Uint8Array(bin.length);
        for (let i = 0; i < bin.length; i++)
            out[i] = bin.charCodeAt(i);
        return out;
    }
    return new TextEncoder().encode(v);
}
function toBase64(b) {
    let s = "";
    for (let i = 0; i < b.length; i++)
        s += String.fromCharCode(b[i]);
    return btoa(s);
}
// keyPath percent-encodes the whole key, "/" included, so dot and empty
// segments reach the server as part of the key instead of being resolved
// away by URL parsing.
function keyPath(key) {
    if (!key)
        throw new Error("empty key");
    if (key === "." || key === "..")
        throw new Error("the keys . and .. can't be reached over HTTP");
    return encodeURIComponent(key);
}
/** How long watch waits without a byte (the server sends a keep-alive every 15 s) before reconnecting. */
const watchIdleMs = 60_000;
export class Client {
    baseURL;
    token;
    actor;
    fetchFn;
    constructor(baseURL = "http://127.0.0.1:7070", opts = {}) {
        this.baseURL = baseURL.replace(/\/+$/, "");
        this.token = opts.token;
        this.actor = opts.actor;
        this.fetchFn = opts.fetch ?? globalThis.fetch.bind(globalThis);
    }
    url(path, params) {
        const q = new URLSearchParams();
        for (const [k, v] of Object.entries(params ?? {})) {
            if (v === undefined || v === false)
                continue;
            q.set(k, v === true ? "1" : String(v));
        }
        const s = q.toString();
        return this.baseURL + path + (s ? "?" + s : "");
    }
    async request(method, path, params, body, extra = {}, signal) {
        const headers = { Accept: "application/json", ...extra };
        if (this.token)
            headers.Authorization = "Bearer " + this.token;
        if (this.actor)
            headers["X-Xkv-Actor"] = this.actor;
        let payload;
        if (body instanceof Uint8Array) {
            payload = body;
        }
        else if (body !== undefined && typeof body === "object") {
            payload = JSON.stringify(body);
            headers["Content-Type"] = "application/json";
        }
        else {
            payload = body;
        }
        let resp;
        try {
            resp = await this.fetchFn(this.url(path, params), { method, headers, body: payload, signal });
        }
        catch (e) {
            if (signal?.aborted)
                throw e;
            throw new XkvError(0, e instanceof Error ? e.message : String(e));
        }
        if (!resp.ok) {
            const text = await resp.text();
            let msg = text.trim() || resp.statusText;
            let partial;
            try {
                const j = JSON.parse(text);
                if (typeof j.error === "string")
                    msg = j.error;
                if (j.partial)
                    partial = j;
            }
            catch {
                // not JSON
            }
            throw new XkvError(resp.status, msg, partial);
        }
        return resp;
    }
    async json(method, path, params, body) {
        return (await (await this.request(method, path, params, body)).json());
    }
    static writeBody(value, o) {
        const b = {};
        if (typeof value === "string")
            b.value = value;
        else if (value instanceof Uint8Array) {
            b.value = toBase64(value);
            b.encoding = "base64";
        }
        if (o.reason !== undefined)
            b.reason = o.reason;
        if (o.actor !== undefined)
            b.actor = o.actor;
        if (o.ref !== undefined)
            b.ref = o.ref;
        if (o.causes?.length)
            b.causes = o.causes.map((c) => (typeof c === "string" ? { key: c } : c));
        return b;
    }
    /** Write a value (a string, or bytes sent as base64) and return its revision. */
    put(key, value, opts = {}) {
        return this.json("PUT", "/v1/keys/" + keyPath(key), undefined, Client.writeBody(value, opts));
    }
    /** Delete a key and return the delete's revision. */
    delete(key, opts = {}) {
        return this.json("DELETE", "/v1/keys/" + keyPath(key), undefined, Client.writeBody(undefined, opts));
    }
    /** The key's current revision, or its revision as the store stood at revision `at`. */
    get(key, at) {
        return this.json("GET", "/v1/keys/" + keyPath(key), { at });
    }
    /** Keys that have a value, sorted. */
    async keys(prefix) {
        return (await this.json("GET", "/v1/keys", { prefix })).keys;
    }
    /** The key's revisions, newest first. */
    history(key, limit) {
        return this.json("GET", "/v1/history/" + keyPath(key), { limit });
    }
    /** The tree of causes behind the key's value. */
    why(key, depth) {
        return this.json("GET", "/v1/why/" + keyPath(key), { depth });
    }
    /** The same tree as W3C PROV-O in JSON-LD. */
    whyProv(key, depth) {
        return this.json("GET", "/v1/why/" + keyPath(key), { depth, format: "prov" });
    }
    /** why for one revision. */
    whyRev(rev, depth) {
        return this.json("GET", `/v1/revs/${rev}/why`, { depth });
    }
    /** whyProv for one revision. */
    whyRevProv(rev, depth) {
        return this.json("GET", `/v1/revs/${rev}/why`, { depth, format: "prov" });
    }
    /** One revision by number. */
    revision(rev) {
        return this.json("GET", `/v1/revs/${rev}`);
    }
    /** Counts, the file format and the store id. */
    stats() {
        return this.json("GET", "/v1/stats");
    }
    /** An exchange file of the changes above `since`, and the `since` to pass next time. */
    async export(since) {
        const resp = await this.request("GET", "/v1/export", { since });
        const t = resp.headers.get("X-Xkv-Through");
        return { data: new Uint8Array(await resp.arrayBuffer()), through: t ? Number(t) : undefined };
    }
    /** Import an exchange file; conflicts is newest, ours or theirs. Returns the import report. */
    async importExchange(data, conflicts) {
        const resp = await this.request("POST", "/v1/import", { conflicts }, data, { "Content-Type": "application/octet-stream" });
        return (await resp.json());
    }
    /**
     * Revisions as they commit, in revision order. `since` starts after that
     * revision (default: new ones only); `all` starts from the beginning. A
     * dropped connection is reopened from the last revision yielded, so
     * nothing is lost or repeated. Stop with `break` or `signal`. Revisions
     * removed by compaction before they could be sent end it with XkvError.
     */
    async *watch(opts = {}) {
        if (opts.all && opts.since !== undefined)
            throw new Error("all and since don't go together");
        const signal = opts.signal;
        let last = opts.since;
        for (;;) {
            if (signal?.aborted)
                return;
            // One controller per connection: the caller's signal or an idle
            // connection (a half-open socket sends nothing) aborts it.
            const ctl = new AbortController();
            const onAbort = () => ctl.abort();
            signal?.addEventListener("abort", onAbort, { once: true });
            if (signal?.aborted)
                ctl.abort();
            let idle;
            const arm = () => {
                if (idle !== undefined)
                    clearTimeout(idle);
                idle = setTimeout(() => ctl.abort(), opts.idleMs ?? watchIdleMs);
            };
            const pause = () => {
                if (idle !== undefined)
                    clearTimeout(idle);
                idle = undefined;
            };
            try {
                arm();
                if (last === undefined && !opts.all) {
                    // "New ones only" means after the revision current when watch
                    // starts, fixed before the first connection so a reconnect
                    // before the first revision misses nothing.
                    const st = await (await this.request("GET", "/v1/stats", undefined, undefined, {}, ctl.signal)).json();
                    last = Number(st.last_rev);
                }
                const params = { prefix: opts.prefix };
                const headers = { Accept: "application/x-ndjson" };
                if (opts.all) {
                    // all keeps skipping what compaction removed; where it got to
                    // goes in Last-Event-ID, which the server takes together with all.
                    params.all = true;
                    if (last !== undefined)
                        headers["Last-Event-ID"] = String(last);
                }
                else {
                    params.since = last;
                }
                const resp = await this.request("GET", "/v1/watch", params, undefined, headers, ctl.signal);
                if (!resp.body)
                    throw new XkvError(0, "no response body");
                const reader = resp.body.getReader();
                const decoder = new TextDecoder();
                let buf = "";
                try {
                    for (;;) {
                        arm();
                        const { done, value } = await reader.read();
                        if (done)
                            break;
                        buf += decoder.decode(value, { stream: true });
                        let nl;
                        while ((nl = buf.indexOf("\n")) >= 0) {
                            const line = buf.slice(0, nl).trim();
                            buf = buf.slice(nl + 1);
                            if (!line)
                                continue; // keep-alive
                            const msg = JSON.parse(line);
                            if (typeof msg.error === "string")
                                throw new XkvError(msg.error.includes("compact") ? 410 : 500, msg.error);
                            last = msg.rev;
                            // The consumer may take its time with a revision; the idle
                            // clock only runs while waiting on the server.
                            pause();
                            yield msg;
                        }
                    }
                }
                finally {
                    reader.cancel().catch(() => { });
                }
            }
            catch (e) {
                if (signal?.aborted)
                    return;
                if (e instanceof XkvError && e.status !== 0)
                    throw e;
            }
            finally {
                pause();
                signal?.removeEventListener("abort", onAbort);
            }
            if (signal?.aborted)
                return;
            // Wait before reconnecting; an abort ends the wait at once.
            await new Promise((res) => {
                const t = setTimeout(done, opts.retryMs ?? 1000);
                function done() {
                    clearTimeout(t);
                    signal?.removeEventListener("abort", done);
                    res();
                }
                signal?.addEventListener("abort", done, { once: true });
            });
        }
    }
}
