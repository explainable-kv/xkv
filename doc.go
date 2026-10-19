// Copyright xkv.org 2026
// SPDX-License-Identifier: Apache-2.0

// Package xkv is a small embedded key-value store that remembers why every
// value changed.
//
// Every write is a revision. A revision records who made it (the actor), why
// (the reason), an optional outside reference such as a ticket or request id,
// and up to eight causes: pointers to earlier revisions, of any key, that led
// to it. [DB.Why] follows those pointers back and returns the chain of
// decisions behind a value, across keys.
//
//	db, err := xkv.Open("limits.xkv", nil)
//	ctx = xkv.WithActor(ctx, "limits-service")
//	r, err := db.Put(ctx, "account:42:limit", []byte("50"),
//		xkv.Reason("risk policy v3"),
//		xkv.Cause("risk:42:score", 17))
//	tree, err := db.Why(ctx, "account:42:limit", 0)
//
// The store is one append-only file. A process that is killed mid-write never
// leaves a half-written transaction behind: on the next open, anything after
// the last complete transaction is trimmed. [DB.Compact] drops old revisions
// but keeps every revision a kept value can still reach through its causes, so
// a chain that Why can follow before compaction stays followable after it.
//
// One process at a time may open a file for writing. Other processes can
// open it read-only and see the store as it was when they opened it.
package xkv
