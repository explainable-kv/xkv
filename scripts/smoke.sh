#!/bin/sh
# Copyright xkv.org 2026
# SPDX-License-Identifier: Apache-2.0
#
# A quick check that a built xkv binary works: writes a chain of causes,
# explains it, compacts, checks the file, and serves it over HTTP for a moment.
#
#   sh scripts/smoke.sh ./xkv
set -eu
bin=$(cd "$(dirname "$1")" && pwd)/$(basename "$1")
dir=$(mktemp -d)
f="$dir/smoke.xkv"

"$bin" version
"$bin" put "$f" chargeback:cb_991 "card dispute filed" --reason "bank notice" --actor billing-worker >/dev/null
"$bin" put "$f" risk:42:score 0.82 --reason "new chargeback" --cause chargeback:cb_991 --actor risk-model >/dev/null
"$bin" put "$f" account:42:limit 50 --reason "risk policy v3" --cause risk:42:score --actor limits-service >/dev/null
"$bin" why "$f" account:42:limit | tee "$dir/why.txt"
grep -q "chargeback:cb_991 = card dispute filed" "$dir/why.txt"
if "$bin" put "$f" x y 2>/dev/null; then
	echo "smoke: a write without a reason was accepted"
	exit 1
fi
"$bin" compact "$f" >/dev/null
"$bin" check "$f"
test "$("$bin" get "$f" account:42:limit)" = "50"

"$bin" serve "$f" --addr 127.0.0.1:7671 >/dev/null 2>&1 &
server=$!
sleep 1
curl -fsS -X PUT 127.0.0.1:7671/v1/keys/over/http -d '{"value": "yes", "reason": "smoke test", "causes": [{"key": "account:42:limit"}]}' >/dev/null
curl -fsS 127.0.0.1:7671/v1/why/over/http | grep -q 'chargeback:cb_991'
kill -TERM "$server"
wait "$server" || true
"$bin" get "$f" over/http | grep -q yes
rm -rf "$dir"
echo "smoke test passed"
