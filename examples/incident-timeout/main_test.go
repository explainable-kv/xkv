// Copyright xkv.org 2026
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestExample(t *testing.T) {
	var out bytes.Buffer
	if err := run(&out); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"\u2514\u2500 alert:orders-db-latency = firing: p99 8.2s",
		"db.timeout was 5s, set at r1",
		"The alert is now \"resolved\"",
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("output lacks %q:\n%s", want, out.String())
		}
	}
}
