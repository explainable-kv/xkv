# Copyright xkv.org 2026
# SPDX-License-Identifier: Apache-2.0
"""A client for xkv's HTTP mode (`xkv serve`), standard library only.

    from xkv_client import Client
    db = Client("http://127.0.0.1:7070", token=open("token").read().strip())
    db.put("config/db/timeout", "30s", reason="stop requests timing out",
           causes=["alert:orders-db-latency"], ref="INC-4410")
    print(db.why("config/db/timeout"))

Every method returns the server's JSON as Python values (revisions are
dicts, see docs/http.md) and raises XkvError on an error answer.
Python 3.9 or later.
"""

import base64
import json
import socket
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request

__all__ = ["Client", "XkvError", "value_bytes"]
__version__ = "0.4.0"


class XkvError(Exception):
    """An error answer from the server: status is the HTTP status (0 when
    the server could not be reached), message the server's error text."""

    def __init__(self, status, message, partial=None):
        super().__init__(f"xkv: {status}: {message}" if status else f"xkv: {message}")
        self.status = status
        self.message = message
        self.partial = partial  # the import report, when an import failed part-way


def value_bytes(revision):
    """The value of a revision as bytes, decoding base64 when the server
    sent it that way. None for a delete."""
    if revision.get("deleted"):
        return None
    v = revision.get("value", "")
    if revision.get("encoding") == "base64":
        return base64.b64decode(v)
    return v.encode("utf-8")


def _cause(c):
    if isinstance(c, str):
        return {"key": c}
    if isinstance(c, (tuple, list)):
        key, rev = c
        return {"key": key, "rev": int(rev)}
    if isinstance(c, dict):
        return c
    raise TypeError("a cause is a key, a (key, rev) pair or a dict")


class Client:
    """A connection to one `xkv serve`.

    base_url: such as "http://127.0.0.1:7070" or "https://xkv.internal:7070".
    token: the bearer token, if the server was started with --token-file or
    --read-token-file. actor: the default actor for writes (sent as
    X-Xkv-Actor). ssl_context: for https with a private certificate.
    """

    def __init__(self, base_url="http://127.0.0.1:7070", token=None, actor=None, timeout=30.0, ssl_context=None):
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.actor = actor
        self.timeout = timeout
        self.ssl_context = ssl_context

    # -- plumbing -----------------------------------------------------------

    def _url(self, path, params=None):
        url = self.base_url + path
        if params:
            q = {k: v for k, v in params.items() if v is not None and v is not False}
            if q:
                url += "?" + urllib.parse.urlencode({k: ("1" if v is True else v) for k, v in q.items()})
        return url

    def _request(self, method, path, params=None, body=None, headers=None, stream=False, timeout=None):
        h = {"Accept": "application/json"}
        if self.token:
            h["Authorization"] = "Bearer " + self.token
        if self.actor:
            h["X-Xkv-Actor"] = self.actor
        if isinstance(body, (dict, list)):
            body = json.dumps(body).encode("utf-8")
            h["Content-Type"] = "application/json"
        h.update(headers or {})
        req = urllib.request.Request(self._url(path, params), data=body, method=method, headers=h)
        try:
            resp = urllib.request.urlopen(req, timeout=timeout or self.timeout, context=self.ssl_context)
        except urllib.error.HTTPError as e:
            raw = e.read()
            try:
                j = json.loads(raw)
            except ValueError:
                j = None
            if isinstance(j, dict) and isinstance(j.get("error"), str):
                raise XkvError(e.code, j["error"], j if j.get("partial") else None) from None
            raise XkvError(e.code, raw.decode("utf-8", "replace").strip() or str(e.reason)) from None
        except (urllib.error.URLError, OSError) as e:
            raise XkvError(0, str(getattr(e, "reason", e))) from None
        if stream:
            return resp
        with resp:
            return resp.read(), resp.headers

    def _json(self, method, path, params=None, body=None):
        raw, _ = self._request(method, path, params, body)
        return json.loads(raw)

    @staticmethod
    def _key(key):
        # Every character but unreserved ones is percent-encoded, "/" too, so
        # dot and empty segments reach the server as part of the key rather
        # than being resolved away on the path.
        if not key:
            raise ValueError("empty key")
        if key in (".", ".."):
            raise ValueError("the keys . and .. can't be reached over HTTP")
        return urllib.parse.quote(key, safe="")

    @staticmethod
    def _write_body(value, reason, causes, actor, ref):
        b = {}
        if value is not None:
            if isinstance(value, str):
                b["value"] = value
            else:
                b["value"] = base64.b64encode(bytes(value)).decode("ascii")
                b["encoding"] = "base64"
        if reason is not None:
            b["reason"] = reason
        if actor is not None:
            b["actor"] = actor
        if ref is not None:
            b["ref"] = ref
        if causes:
            b["causes"] = [_cause(c) for c in causes]
        return b

    # -- the API ------------------------------------------------------------

    def put(self, key, value, reason=None, causes=None, actor=None, ref=None):
        """Write a value (str, or bytes sent as base64) and return its revision.
        causes: keys (their current revision), (key, rev) pairs or dicts."""
        if value is None:
            raise ValueError("put needs a value; use delete")
        return self._json("PUT", "/v1/keys/" + self._key(key), body=self._write_body(value, reason, causes, actor, ref))

    def delete(self, key, reason=None, causes=None, actor=None, ref=None):
        """Delete a key and return the delete's revision."""
        return self._json("DELETE", "/v1/keys/" + self._key(key), body=self._write_body(None, reason, causes, actor, ref))

    def get(self, key, at=None):
        """The key's current revision, or its revision as the store stood at rev at."""
        return self._json("GET", "/v1/keys/" + self._key(key), {"at": at})

    def keys(self, prefix=None):
        """Keys that have a value, sorted."""
        return self._json("GET", "/v1/keys", {"prefix": prefix})["keys"]

    def history(self, key, limit=None):
        """The key's revisions, newest first."""
        return self._json("GET", "/v1/history/" + self._key(key), {"limit": limit})

    def why(self, key, depth=None, prov=False):
        """The tree of causes behind the key's value; prov=True gives W3C
        PROV-O as JSON-LD instead (parsed into Python values)."""
        return self._json("GET", "/v1/why/" + self._key(key), {"depth": depth, "format": "prov" if prov else None})

    def why_rev(self, rev, depth=None, prov=False):
        """why for one revision."""
        return self._json("GET", f"/v1/revs/{int(rev)}/why", {"depth": depth, "format": "prov" if prov else None})

    def revision(self, rev):
        """One revision by number."""
        return self._json("GET", f"/v1/revs/{int(rev)}")

    def stats(self):
        """Counts, the file format and the store id."""
        return self._json("GET", "/v1/stats")

    def export(self, since=None):
        """An exchange file of the changes above since: (bytes, through), where
        through is the since to pass next time."""
        raw, headers = self._request("GET", "/v1/export", {"since": since})
        through = headers.get("X-Xkv-Through")
        return raw, int(through) if through else None

    def import_exchange(self, data, conflicts=None):
        """Import an exchange file (bytes); conflicts is newest, ours or theirs.
        Returns the import report."""
        raw, _ = self._request("POST", "/v1/import", {"conflicts": conflicts}, body=bytes(data),
                               headers={"Content-Type": "application/octet-stream"})
        return json.loads(raw)

    def watch(self, since=None, prefix=None, all=False, reconnect=True, retry_seconds=1.0):
        """Yield revisions as they commit, in revision order, forever.

        since: start after this revision (default: new ones only). all=True
        starts from the beginning. When the connection drops it reconnects
        from the last revision it yielded, so nothing is lost or repeated;
        reconnect=False ends the generator instead. Revisions removed by
        compaction before they could be sent end it with XkvError."""
        if all and since is not None:
            raise ValueError("all and since don't go together")
        last = since
        if last is None and not all:
            # "New ones only" means after the revision current now, fixed
            # here so a reconnect before the first revision misses nothing.
            last = self.stats()["last_rev"]
        while True:
            params = {"prefix": prefix}
            headers = {"Accept": "application/x-ndjson"}
            if all:
                # all keeps skipping what compaction removed; where it got
                # to goes in Last-Event-ID, which the server takes with all.
                params["all"] = True
                if last is not None:
                    headers["Last-Event-ID"] = str(last)
            else:
                params["since"] = last
            try:
                resp = self._request("GET", "/v1/watch", params, headers=headers, stream=True, timeout=120)
                with resp:
                    for line in resp:
                        line = line.strip()
                        if not line:
                            continue  # keep-alive
                        msg = json.loads(line)
                        if "error" in msg:
                            raise XkvError(410 if "compact" in msg["error"] else 500, msg["error"])
                        last = msg["rev"]
                        yield msg
            except XkvError as e:
                if e.status != 0 or not reconnect:
                    raise
            except (OSError, socket.timeout, ssl.SSLError, ValueError):
                if not reconnect:
                    raise
            if not reconnect:
                return
            time.sleep(retry_seconds)
