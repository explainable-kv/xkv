// Copyright xkv.org 2026
// SPDX-License-Identifier: Apache-2.0

package xkv

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"unicode/utf8"
)

// FormatVersion is the version of the file format this package writes. It
// reads version 1 files too, and rewrites one as version 2 the first time it
// compacts it, upgrades it, or exports or imports through it.
// docs/format.md in the repository describes both byte by byte.
const FormatVersion = 2

const (
	headerSizeV1 = 32
	headerSizeV2 = 48
	maxHeader    = headerSizeV2
	frameSize    = 8 // length uint32 + crc uint32

	recRevision = 1
	recCommit   = 2

	flagDeleted    = 1
	flagOrigin     = 2 // the revision was imported; its first identity follows the causes
	flagHistory    = 4 // kept for the record (an imported cause); never a key's current value
	flagResolution = 8 // written by an import to settle a conflict
	flagsAll       = flagDeleted | flagOrigin | flagHistory | flagResolution
)

// Limits on what one write may hold.
const (
	MaxKeySize    = 4 << 10  // 4 KiB
	MaxValueSize  = 16 << 20 // 16 MiB
	MaxActorSize  = 256
	MaxReasonSize = 4 << 10
	MaxRefSize    = 1 << 10
	MaxCauses     = 8
	MaxTxSize     = 64 << 20 // 64 MiB of encoded records per transaction
)

// maxRecord bounds the body length a frame may claim; anything larger is
// treated as damage, never allocated.
const maxRecord = MaxValueSize + 4*MaxKeySize + MaxActorSize + MaxReasonSize + MaxRefSize + MaxCauses*(MaxKeySize+16) + 96

var (
	magic   = [4]byte{'X', 'K', 'V', 'F'}
	castTab = crc32.MakeTable(crc32.Castagnoli)
)

// StoreID names a store. It is drawn at random when the store is created
// (or upgraded from format 1) and kept through compaction, so a revision's
// identity, its store and its number, stays the same wherever it travels.
type StoreID [16]byte

// String returns the id as 32 hex digits.
func (id StoreID) String() string { return hex.EncodeToString(id[:]) }

// ident is a revision's identity across stores: the store it was first
// written to and its number there.
type ident struct {
	store StoreID
	rev   uint64
}

func (i ident) String() string { return fmt.Sprintf("%s:%d", i.store, i.rev) }

type header struct {
	version    uint16
	generation [16]byte // new on every rewrite; ties the snapshot to the file
	store      StoreID  // version 2 only
}

func (h header) size() int64 {
	if h.version == 1 {
		return headerSizeV1
	}
	return headerSizeV2
}

func newHeader() (header, error) {
	h := header{version: FormatVersion}
	if _, err := rand.Read(h.generation[:]); err != nil {
		return h, err
	}
	if _, err := rand.Read(h.store[:]); err != nil {
		return h, err
	}
	return h, nil
}

// rewrite returns the header for a rewrite of the file h heads: a new
// generation, the same store id, format 2. A format 1 file gets its store id
// here.
func (h header) rewrite() (header, error) {
	n, err := newHeader()
	if err != nil {
		return n, err
	}
	if h.version >= 2 {
		n.store = h.store
	}
	return n, nil
}

func (h header) encode() []byte {
	size := h.size()
	b := make([]byte, size)
	copy(b[0:4], magic[:])
	binary.LittleEndian.PutUint16(b[4:6], h.version)
	// b[6:8] flags, zero
	copy(b[8:24], h.generation[:])
	if h.version >= 2 {
		copy(b[24:40], h.store[:])
	}
	// four reserved bytes, zero, then the checksum
	binary.LittleEndian.PutUint32(b[size-4:], crc32.Checksum(b[:size-4], castTab))
	return b
}

// decodeHeader parses the header at the start of b, which holds the first
// maxHeader bytes of the file or the whole file if it is shorter.
func decodeHeader(b []byte) (header, error) {
	var h header
	if len(b) < 6 || !bytes.Equal(b[0:4], magic[:]) {
		if len(b) < headerSizeV1 {
			return h, errors.New("header too short")
		}
		return h, errors.New("not an xkv file")
	}
	h.version = binary.LittleEndian.Uint16(b[4:6])
	if h.version != 1 && h.version != 2 {
		return h, fmt.Errorf("unsupported file format version %d (this is xkv %s, which reads 1 and 2)", h.version, Version)
	}
	size := int(h.size())
	if len(b) < size {
		return h, errors.New("header too short")
	}
	if crc32.Checksum(b[0:size-4], castTab) != binary.LittleEndian.Uint32(b[size-4:size]) {
		return h, errors.New("header checksum mismatch")
	}
	if binary.LittleEndian.Uint16(b[6:8]) != 0 || binary.LittleEndian.Uint32(b[size-8:size-4]) != 0 {
		return h, errors.New("header flags or reserved bytes are set")
	}
	copy(h.generation[:], b[8:24])
	if h.version >= 2 {
		copy(h.store[:], b[24:40])
	}
	return h, nil
}

// rec is a decoded revision record.
type rec struct {
	rev        uint64
	tx         uint64
	deleted    bool
	time       int64
	key        string
	value      []byte
	actor      string
	reason     string
	ref        string
	causes     []CauseRef
	history    bool   // flagHistory
	resolution bool   // flagResolution
	origin     *ident // flagOrigin: where an imported revision was first written
}

type commit struct {
	tx      uint64
	count   uint64
	lastRev uint64
	// gaps lists revisions of this transaction whose key had revisions
	// before them that compaction removed. Empty for an ordinary write.
	gaps []uint64
}

// frame wraps a record body in its length and checksum.
func frame(body []byte) []byte {
	out := make([]byte, frameSize+len(body))
	binary.LittleEndian.PutUint32(out[0:4], uint32(len(body)))
	binary.LittleEndian.PutUint32(out[4:8], crc32.Checksum(body, castTab))
	copy(out[frameSize:], body)
	return out
}

func appendBytes(b []byte, s []byte) []byte {
	b = binary.AppendUvarint(b, uint64(len(s)))
	return append(b, s...)
}

func appendString(b []byte, s string) []byte {
	b = binary.AppendUvarint(b, uint64(len(s)))
	return append(b, s...)
}

func encodeRev(r *rec) []byte {
	n := 32 + len(r.key) + len(r.value) + len(r.actor) + len(r.reason) + len(r.ref)
	for _, c := range r.causes {
		n += len(c.Key) + 12
	}
	b := make([]byte, 0, n)
	b = append(b, recRevision)
	b = binary.AppendUvarint(b, r.rev)
	b = binary.AppendUvarint(b, r.tx)
	var flags byte
	if r.deleted {
		flags |= flagDeleted
	}
	if r.origin != nil {
		flags |= flagOrigin
	}
	if r.history {
		flags |= flagHistory
	}
	if r.resolution {
		flags |= flagResolution
	}
	b = append(b, flags)
	b = binary.AppendVarint(b, r.time)
	b = appendString(b, r.key)
	b = appendBytes(b, r.value)
	b = appendString(b, r.actor)
	b = appendString(b, r.reason)
	b = appendString(b, r.ref)
	b = binary.AppendUvarint(b, uint64(len(r.causes)))
	for _, c := range r.causes {
		b = appendString(b, c.Key)
		b = binary.AppendUvarint(b, c.Rev)
	}
	if r.origin != nil {
		b = append(b, r.origin.store[:]...)
		b = binary.AppendUvarint(b, r.origin.rev)
	}
	return b
}

func encodeCommit(c commit) []byte {
	b := make([]byte, 0, 32)
	b = append(b, recCommit)
	b = binary.AppendUvarint(b, c.tx)
	b = binary.AppendUvarint(b, c.count)
	b = binary.AppendUvarint(b, c.lastRev)
	b = binary.AppendUvarint(b, uint64(len(c.gaps)))
	for _, g := range c.gaps {
		b = binary.AppendUvarint(b, g)
	}
	return b
}

var errBadRecord = errors.New("malformed record")

type decoder struct {
	b   []byte
	err bool
}

func (d *decoder) uvarint() uint64 {
	if d.err {
		return 0
	}
	v, n := binary.Uvarint(d.b)
	// Only the shortest encoding is accepted, so each record has exactly
	// one byte form.
	if n <= 0 || (n > 1 && d.b[n-1] == 0) {
		d.err = true
		return 0
	}
	d.b = d.b[n:]
	return v
}

func (d *decoder) varint() int64 {
	if d.err {
		return 0
	}
	v, n := binary.Varint(d.b)
	if n <= 0 || (n > 1 && d.b[n-1] == 0) {
		d.err = true
		return 0
	}
	d.b = d.b[n:]
	return v
}

func (d *decoder) byte1() byte {
	if d.err || len(d.b) < 1 {
		d.err = true
		return 0
	}
	v := d.b[0]
	d.b = d.b[1:]
	return v
}

func (d *decoder) bytes(max int) []byte {
	n := d.uvarint()
	if d.err || n > uint64(max) || n > uint64(len(d.b)) {
		d.err = true
		return nil
	}
	v := d.b[:n:n]
	d.b = d.b[n:]
	return v
}

func (d *decoder) str(max int) string {
	v := d.bytes(max)
	if d.err || !utf8.Valid(v) {
		d.err = true
		return ""
	}
	return string(v)
}

// decodeRev parses a revision body. The value slice is copied so the caller
// may reuse body.
func decodeRev(body []byte) (*rec, error) {
	if len(body) == 0 || body[0] != recRevision {
		return nil, errBadRecord
	}
	d := decoder{b: body[1:]}
	r := &rec{}
	r.rev = d.uvarint()
	r.tx = d.uvarint()
	flags := d.byte1()
	r.deleted = flags&flagDeleted != 0
	r.history = flags&flagHistory != 0
	r.resolution = flags&flagResolution != 0
	r.time = d.varint()
	r.key = d.str(MaxKeySize)
	r.value = append([]byte(nil), d.bytes(MaxValueSize)...)
	r.actor = d.str(MaxActorSize)
	r.reason = d.str(MaxReasonSize)
	r.ref = d.str(MaxRefSize)
	n := d.uvarint()
	if n > MaxCauses {
		return nil, errBadRecord
	}
	for i := uint64(0); i < n && !d.err; i++ {
		k := d.str(MaxKeySize)
		v := d.uvarint()
		r.causes = append(r.causes, CauseRef{Key: k, Rev: v})
	}
	if flags&flagOrigin != 0 && !d.err {
		if len(d.b) < 16 {
			return nil, errBadRecord
		}
		o := &ident{}
		copy(o.store[:], d.b[:16])
		d.b = d.b[16:]
		o.rev = d.uvarint()
		if o.rev == 0 {
			return nil, errBadRecord
		}
		r.origin = o
	}
	if d.err || len(d.b) != 0 || flags&^flagsAll != 0 || r.rev == 0 || r.tx == 0 || r.key == "" {
		return nil, errBadRecord
	}
	for _, c := range r.causes {
		if c.Rev == 0 || c.Rev >= r.rev || c.Key == "" {
			return nil, errBadRecord
		}
	}
	return r, nil
}

func decodeCommit(body []byte) (commit, error) {
	var c commit
	if len(body) == 0 || body[0] != recCommit {
		return c, errBadRecord
	}
	d := decoder{b: body[1:]}
	c.tx = d.uvarint()
	c.count = d.uvarint()
	c.lastRev = d.uvarint()
	n := d.uvarint()
	if d.err || c.tx == 0 || c.count == 0 || c.lastRev == 0 || n > c.count {
		return c, errBadRecord
	}
	var prev uint64
	for i := uint64(0); i < n && !d.err; i++ {
		g := d.uvarint()
		if g <= prev || g > c.lastRev {
			return c, errBadRecord
		}
		c.gaps = append(c.gaps, g)
		prev = g
	}
	if d.err || len(d.b) != 0 {
		return c, errBadRecord
	}
	return c, nil
}

// checkFrame validates a frame header against the bytes that follow it and
// returns the body, or nil if the frame is not intact.
func checkFrame(b []byte) (body []byte, total int) {
	if len(b) < frameSize {
		return nil, 0
	}
	n := binary.LittleEndian.Uint32(b[0:4])
	if n == 0 || n > maxRecord || int64(n) > int64(len(b))-frameSize {
		return nil, 0
	}
	body = b[frameSize : frameSize+int(n)]
	if crc32.Checksum(body, castTab) != binary.LittleEndian.Uint32(b[4:8]) {
		return nil, 0
	}
	return body, frameSize + int(n)
}
