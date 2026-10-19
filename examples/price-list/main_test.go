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
		"\u251c\u2500 ticket:SUP-5521",
		"\u2514\u2500 decision:q4-pricing",
		"price:pro = 33 (r3) \u00b7 by pricing-team \u00b7 Q4 price list \u00b7 ref PRICE-12",
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("output lacks %q:\n%s", want, out.String())
		}
	}
}
