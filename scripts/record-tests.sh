#!/bin/sh
# Copyright xkv.org 2026
# SPDX-License-Identifier: Apache-2.0
#
# Runs every test, the race detector, the long-history test at a million
# revisions, the fuzzers for a while and the benchmarks, and writes the output
# to test/results/ with a note of the machine they ran on. Every number on
# xkv.org comes from these files.
#
#   sh scripts/record-tests.sh
set -eu
cd "$(dirname "$0")/.."
out=test/results
mkdir -p "$out"

header() {
	echo "# $1"
	echo "# recorded $(date -u '+%Y-%m-%d %H:%M UTC') with $(go version | cut -d' ' -f3-)"
	echo "# $(uname -sr), $(grep -m1 'model name' /proc/cpuinfo 2>/dev/null | cut -d: -f2- | sed 's/^ //'), $(nproc 2>/dev/null || echo '?') CPUs"
	echo "# command: $2"
	echo
}

go vet ./...

cmd="go test -count=1 -v ./..."
{ header "Every test, verbose" "$cmd"; $cmd 2>&1; } > "$out/tests.txt"

cmd="go test -race -count=1 ./..."
{ header "Every test with the race detector" "$cmd"; $cmd 2>&1; } > "$out/race.txt"

cmd="go test -count=1 -v -run TestLongHistory ."
{ header "The long-history test at one million revisions (XKV_LONG=1)" "XKV_LONG=1 $cmd"; XKV_LONG=1 $cmd 2>&1; } > "$out/long.txt"

cmd="go test -count=1 -v -run TestCrashSIGKILL ."
{ header "1000 SIGKILLs of a writing process (XKV_CRASH_RUNS=1000)" "XKV_CRASH_RUNS=1000 $cmd"; XKV_CRASH_RUNS=1000 $cmd 2>&1; } > "$out/crash.txt"

{
	header "The fuzzers, two minutes each" "go test -run ^\$ -fuzz FuzzOpen -fuzztime 2m . ; go test -run ^\$ -fuzz FuzzDecode -fuzztime 2m . ; go test -run ^\$ -fuzz FuzzImport -fuzztime 2m ."
	go test -run '^$' -fuzz FuzzOpen -fuzztime 2m . 2>&1 | tail -n 3
	go test -run '^$' -fuzz FuzzDecode -fuzztime 2m . 2>&1 | tail -n 3
	go test -run '^$' -fuzz FuzzImport -fuzztime 2m . 2>&1 | tail -n 3
} > "$out/fuzz.txt"

cmd="go test -run ^$ -bench . -benchtime 2s -count 3 ."
{ header "Benchmarks, three runs each. BenchmarkPut and BenchmarkTx100 fsync every transaction" "$cmd"; $cmd 2>&1; } > "$out/bench.txt"

grep -hE '^(ok|FAIL|--- FAIL)' "$out"/*.txt || true
echo "results written to $out/"
