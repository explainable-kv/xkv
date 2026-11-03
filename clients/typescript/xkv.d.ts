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
export declare const version = "0.4.0";
/** A cause: a key (its current revision), or a key and a revision. */
export type Cause = string | {
    key: string;
    rev?: number;
};
export interface CauseRef {
    key: string;
    rev: number;
}
export interface Revision {
    rev: number;
    tx: number;
    key: string;
    value?: string;
    encoding?: "base64";
    deleted?: boolean;
    actor: string;
    reason: string;
    ref?: string;
    causes?: CauseRef[];
    time: string;
    origin?: string;
    history?: boolean;
}
export interface WhyTree {
    revision: Revision;
    prev?: Revision;
    causes?: WhyTree[];
    prev_compacted?: boolean;
    truncated?: boolean;
    repeat?: boolean;
}
/** W3C PROV-O as JSON-LD; see docs/prov.md. */
export interface ProvDocument {
    "@context": Record<string, string>;
    "@graph": Array<Record<string, unknown>>;
}
export interface WriteOptions {
    reason?: string;
    causes?: Cause[];
    actor?: string;
    ref?: string;
}
export interface ClientOptions {
    /** The bearer token, if serve was started with --token-file or --read-token-file. */
    token?: string;
    /** The default actor for writes, sent as X-Xkv-Actor. */
    actor?: string;
    /** Another fetch, for tests or custom agents. */
    fetch?: typeof fetch;
}
/** An error answer: status is the HTTP status (0 when the server could not be reached). */
export declare class XkvError extends Error {
    readonly status: number;
    readonly partial?: unknown;
    constructor(status: number, message: string, partial?: unknown);
}
/** The value of a revision as bytes, decoding base64 when sent that way; undefined for a delete. */
export declare function valueBytes(r: Revision): Uint8Array | undefined;
export declare class Client {
    readonly baseURL: string;
    private readonly token?;
    private readonly actor?;
    private readonly fetchFn;
    constructor(baseURL?: string, opts?: ClientOptions);
    private url;
    private request;
    private json;
    private static writeBody;
    /** Write a value (a string, or bytes sent as base64) and return its revision. */
    put(key: string, value: string | Uint8Array, opts?: WriteOptions): Promise<Revision>;
    /** Delete a key and return the delete's revision. */
    delete(key: string, opts?: WriteOptions): Promise<Revision>;
    /** The key's current revision, or its revision as the store stood at revision `at`. */
    get(key: string, at?: number): Promise<Revision>;
    /** Keys that have a value, sorted. */
    keys(prefix?: string): Promise<string[]>;
    /** The key's revisions, newest first. */
    history(key: string, limit?: number): Promise<Revision[]>;
    /** The tree of causes behind the key's value. */
    why(key: string, depth?: number): Promise<WhyTree>;
    /** The same tree as W3C PROV-O in JSON-LD. */
    whyProv(key: string, depth?: number): Promise<ProvDocument>;
    /** why for one revision. */
    whyRev(rev: number, depth?: number): Promise<WhyTree>;
    /** whyProv for one revision. */
    whyRevProv(rev: number, depth?: number): Promise<ProvDocument>;
    /** One revision by number. */
    revision(rev: number): Promise<Revision>;
    /** Counts, the file format and the store id. */
    stats(): Promise<Record<string, number | string>>;
    /** An exchange file of the changes above `since`, and the `since` to pass next time. */
    export(since?: number): Promise<{
        data: Uint8Array;
        through?: number;
    }>;
    /** Import an exchange file; conflicts is newest, ours or theirs. Returns the import report. */
    importExchange(data: Uint8Array, conflicts?: "newest" | "ours" | "theirs"): Promise<Record<string, unknown>>;
    /**
     * Revisions as they commit, in revision order. `since` starts after that
     * revision (default: new ones only); `all` starts from the beginning. A
     * dropped connection is reopened from the last revision yielded, so
     * nothing is lost or repeated. Stop with `break` or `signal`. Revisions
     * removed by compaction before they could be sent end it with XkvError.
     */
    watch(opts?: {
        since?: number;
        prefix?: string;
        all?: boolean;
        signal?: AbortSignal;
        retryMs?: number;
    }): AsyncGenerator<Revision>;
}
