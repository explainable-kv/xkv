// Copyright xkv.org 2026
// SPDX-License-Identifier: Apache-2.0

package xkv

import (
	"fmt"
	"io"
	"strings"
	"unicode"
	"unicode/utf8"
)

// WriteText prints the tree as indented text, root first, the way the xkv
// command shows it:
//
//	account:42:limit = 50   r5 · 2026-10-19 09:00:05
//	│ was 100 · by limits-service · risk policy v3
//	└─ risk:42:score = 0.82   r3 · 2026-10-19 09:00:03
//	   │ was 0.31 · by risk-model · new chargeback
//	   └─ chargeback:cb_991 = card dispute filed   r1 · 2026-10-19 09:00:01
//	        by billing-worker · bank notice · ref BANK-7731
func (n *WhyNode) WriteText(w io.Writer) error {
	ew := &errWriter{w: w}
	w = ew
	var walk func(n *WhyNode, prefix string, root, last bool)
	walk = func(n *WhyNode, prefix string, root, last bool) {
		branch, cont := "", ""
		if !root {
			if last {
				branch, cont = "└─ ", "   "
			} else {
				branch, cont = "├─ ", "│  "
			}
		}
		fmt.Fprintf(w, "%s%s%s   r%d · %s\n", prefix, branch, textHead(n.Revision), n.Rev, n.Time.Local().Format("2006-01-02 15:04:05"))
		bar := "  "
		if len(n.Causes) > 0 {
			bar = "│ "
		}
		fmt.Fprintf(w, "%s%s%s%s\n", prefix, cont, bar, textDetail(n.Revision, n.Prev, false, n.PrevCompacted))
		switch {
		case n.Truncated:
			fmt.Fprintf(w, "%s%s  (%s not shown; raise --depth)\n", prefix, cont, textPlural(len(n.Revision.Causes), "cause"))
		case n.Repeat:
			fmt.Fprintf(w, "%s%s  (its causes are shown above)\n", prefix, cont)
		}
		for i, c := range n.Causes {
			walk(c, prefix+cont, false, i == len(n.Causes)-1)
		}
	}
	walk(n, "", true, true)
	return ew.err
}

type errWriter struct {
	w   io.Writer
	err error
}

func (e *errWriter) Write(p []byte) (int, error) {
	if e.err != nil {
		return 0, e.err
	}
	n, err := e.w.Write(p)
	e.err = err
	return n, err
}

// textHead is "key = value", or "key deleted".
func textHead(r Revision) string {
	if r.Deleted {
		return r.Key + " deleted"
	}
	return r.Key + " = " + textShort(r.Value)
}

// textDetail is "was OLD · by ACTOR · REASON · ref REF", with the causes listed
// when the caller doesn't draw them as a tree.
func textDetail(r Revision, prev *Revision, withCauses, prevCompacted bool) string {
	var parts []string
	if prevCompacted {
		parts = append(parts, "earlier values compacted")
	} else if prev != nil {
		if prev.Deleted {
			parts = append(parts, "was deleted")
		} else {
			parts = append(parts, "was "+textShort(prev.Value))
		}
	}
	parts = append(parts, "by "+r.Actor, r.Reason)
	if r.Ref != "" {
		parts = append(parts, "ref "+r.Ref)
	}
	if withCauses && len(r.Causes) > 0 {
		cs := make([]string, len(r.Causes))
		for i, c := range r.Causes {
			cs[i] = c.String()
		}
		parts = append(parts, "caused by "+strings.Join(cs, ", "))
	}
	return strings.Join(parts, " · ")
}

// textShort shows a value on one line: printable text up to 60 characters,
// otherwise its size.
func textShort(v []byte) string {
	if !utf8.Valid(v) {
		return fmt.Sprintf("<%d bytes>", len(v))
	}
	s := string(v)
	for _, r := range s {
		if !unicode.IsPrint(r) {
			return fmt.Sprintf("<%d bytes>", len(v))
		}
	}
	if utf8.RuneCountInString(s) > 60 {
		rs := []rune(s)
		return string(rs[:57]) + "..."
	}
	if s == "" {
		return `""`
	}
	return s
}

func textPlural(n int, word string) string {
	if n == 1 {
		return "1 " + word
	}
	return fmt.Sprintf("%d %ss", n, word)
}

// String shows the revision on one line: key and value, revision number,
// actor, reason, ref and causes.
func (r Revision) String() string {
	return fmt.Sprintf("%s (r%d) · %s", textHead(r), r.Rev, textDetail(r, nil, true, false))
}
