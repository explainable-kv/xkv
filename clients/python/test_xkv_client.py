# Copyright xkv.org 2026
# SPDX-License-Identifier: Apache-2.0
"""Tests the Python client against a real `xkv serve`.

    XKV_BIN=path/to/xkv python3 -m unittest -v test_xkv_client
"""

import os
import re
import secrets
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from xkv_client import Client, XkvError, value_bytes  # noqa: E402

XKV = os.environ.get("XKV_BIN", "xkv")


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Server:
    def __init__(self, tmp, name):
        self.file = os.path.join(tmp, name + ".xkv")
        self.write = secrets.token_hex(24)
        self.read = secrets.token_hex(24)
        wf, rf = os.path.join(tmp, name + ".token"), os.path.join(tmp, name + ".read")
        for path, tok in ((wf, self.write), (rf, self.read)):
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
            os.write(fd, (tok + "\n").encode())
            os.close(fd)
        self.port = free_port()
        self.url = f"http://127.0.0.1:{self.port}"
        self.proc = subprocess.Popen([XKV, "serve", self.file, "--addr", f"127.0.0.1:{self.port}", "--token-file", wf, "--read-token-file", rf],
                                     stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        line = self.proc.stdout.readline()
        if "serving" not in line:
            raise RuntimeError("serve did not start: " + line + self.proc.stderr.read())

    def stop(self):
        self.proc.terminate()
        self.proc.wait(10)


class ClientTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.a = Server(cls.tmp.name, "a")
        cls.b = Server(cls.tmp.name, "b")

    @classmethod
    def tearDownClass(cls):
        cls.a.stop()
        cls.b.stop()
        cls.tmp.cleanup()

    def client(self, server=None, token="write", **kw):
        s = server or self.a
        return Client(s.url, token={"write": s.write, "read": s.read, None: None}[token], **kw)

    def test_write_read_why(self):
        db = self.client(actor="agent-7")
        alert = db.put("alert:latency", "p99 2.4s", reason="seen by the monitor")
        r = db.put("config/db/timeout", "30s", reason="stop requests timing out", causes=["alert:latency"], ref="INC-1")
        self.assertEqual(r["actor"], "agent-7")
        self.assertEqual(r["causes"], [{"key": "alert:latency", "rev": alert["rev"]}])
        self.assertEqual(db.get("config/db/timeout")["value"], "30s")
        with self.assertRaises(XkvError) as e:
            db.get("config/db/timeout", at=alert["rev"])  # not written yet then
        self.assertEqual(e.exception.status, 404)
        tree = db.why("config/db/timeout")
        self.assertEqual(tree["revision"]["reason"], "stop requests timing out")
        self.assertEqual(tree["causes"][0]["revision"]["key"], "alert:latency")
        prov = db.why("config/db/timeout", prov=True)
        types = {n["@type"] for n in prov["@graph"]}
        self.assertEqual(types, {"prov:Entity", "prov:Activity", "prov:Agent"})
        self.assertEqual(db.why_rev(r["rev"])["revision"]["rev"], r["rev"])
        self.assertEqual(db.revision(r["rev"])["ref"], "INC-1")
        self.assertIn("config/db/timeout", db.keys(prefix="config/"))
        db.put("config/db/timeout", "45s", reason="still short", causes=[("alert:latency", alert["rev"])], actor="sam")
        hist = db.history("config/db/timeout", limit=5)
        self.assertEqual([h["value"] for h in hist], ["45s", "30s"])
        self.assertEqual(hist[0]["actor"], "sam")
        self.assertGreaterEqual(db.stats()["revisions"], 3)
        d = db.delete("config/db/timeout", reason="back to the default")
        self.assertTrue(d["deleted"])
        self.assertIsNone(value_bytes(d))

    def test_bytes_and_keys_that_need_escaping(self):
        db = self.client()
        r = db.put("blob/a b?c#d", b"\xff\x00\x01", reason="binary")
        self.assertEqual(value_bytes(db.get("blob/a b?c#d")), b"\xff\x00\x01")
        self.assertEqual(r["key"], "blob/a b?c#d")

    def test_errors(self):
        db = self.client()
        with self.assertRaises(XkvError) as e:
            db.put("k", "v")  # no reason
        self.assertEqual(e.exception.status, 400)
        with self.assertRaises(XkvError) as e:
            db.get("missing")
        self.assertEqual(e.exception.status, 404)
        with self.assertRaises(XkvError) as e:
            self.client(token=None).stats()
        self.assertEqual(e.exception.status, 401)
        with self.assertRaises(XkvError) as e:
            self.client(token="read").put("k", "v", reason="r")
        self.assertEqual(e.exception.status, 403)
        self.assertIn("revisions", self.client(token="read").stats())
        with self.assertRaises(XkvError) as e:
            Client(f"http://127.0.0.1:{free_port()}").stats()
        self.assertEqual(e.exception.status, 0)

    def test_export_import(self):
        a, b = self.client(), self.client(self.b)
        a.put("shared/limit", "10", reason="initial")
        data, through = a.export()
        self.assertTrue(through)
        report = b.import_exchange(data)
        self.assertGreaterEqual(report["imported"], 1)
        self.assertEqual(report["through"], through)
        self.assertEqual(b.get("shared/limit")["value"], "10")
        again, through2 = a.export(since=through)
        self.assertGreaterEqual(through2, through)

    def test_watch(self):
        db = self.client()
        start = db.stats()["last_rev"]
        got = []

        def writer():
            time.sleep(0.3)
            for i in range(3):
                db.put("watched/k", str(i), reason="tick")

        threading.Thread(target=writer).start()
        for rev in self.client(token="read").watch(since=start, prefix="watched/"):
            got.append(rev["value"])
            if len(got) == 3:
                break
        self.assertEqual(got, ["0", "1", "2"])


if __name__ == "__main__":
    unittest.main()
