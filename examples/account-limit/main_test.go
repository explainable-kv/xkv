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
		"\u2514\u2500 chargeback:cb_991 = card dispute filed",
		"ref BANK-7731",
		"was 100 \u00b7 by limits-service \u00b7 risk policy v3",
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("output lacks %q:\n%s", want, out.String())
		}
	}
}
