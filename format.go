// Copyright xkv.org 2026
// SPDX-License-Identifier: Apache-2.0

package xkv

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"unicode/utf8"
)

// FormatVersion is the version of the file format this package writes and
// reads. FORMAT.md in the repository describes it byte by byte.
const FormatVersion = 1

const (
	headerSize = 32
	frameSize  = 8 // length uint32 + crc uint32

	recRevision = 1
	recCommit   = 2

	flagDeleted = 1
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
const maxRecord = MaxValueSize + 4*MaxKeySize + MaxActorSize + MaxReasonSize + MaxRefSize + MaxCauses*(MaxKeySize+16) + 64

var (
	magic   = [4]byte{'X', 'K', 'V', 'F'}
	castTab = crc32.MakeTable(crc32.Castagnoli)
)

type header struct {
	version    uint16
	generation [16]byte
}

func newHeader() (header, error) {
	h := header{version: FormatVersion}
	if _, err := rand.Read(h.generation[:]); err != nil {
		return h, err
	}
	return h, nil
}

func (h header) encode() []byte {
	b := make([]byte, headerSize)
	copy(b[0:4], magic[:])
	binary.LittleEndian.PutUint16(b[4:6], h.version)
	// b[6:8] flags, zero
	copy(b[8:24], h.generation[:])
	// b[24:28] reserved, zero
	binary.LittleEndian.PutUint32(b[28:32], crc32.Checksum(b[0:28], castTab))
	return b
}

func decodeHeader(b []byte) (header, error) {
	var h header
	if len(b) < headerSize {
		return h, errors.New("header too short")
	}
	if !bytes.Equal(b[0:4], magic[:]) {
		return h, errors.New("not an xkv file")
	}
	if crc32.Checksum(b[0:28], castTab) != binary.LittleEndian.Uint32(b[28:32]) {
		return h, errors.New("header checksum mismatch")
	}
	h.version = binary.LittleEndian.Uint16(b[4:6])
	if h.version != FormatVersion {
		return h, errors.New("unsupported file format version")
	}
	copy(h.generation[:], b[8:24])
	return h, nil
}

// rec is a decoded revision record.
type rec struct {
	rev     uint64
	tx      uint64
	deleted bool
	time    int64
	key     string
	value   []byte
	actor   string
	reason  string
	ref     string
	causes  []CauseRef
}

type commit struct {
	tx      uint64
	count   uint64
	lastRev uint64
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
	return b
}

func encodeCommit(c commit) []byte {
	b := make([]byte, 0, 32)
	b = append(b, recCommit)
	b = binary.AppendUvarint(b, c.tx)
	b = binary.AppendUvarint(b, c.count)
	b = binary.AppendUvarint(b, c.lastRev)
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
	if d.err || len(d.b) != 0 || flags&^flagDeleted != 0 || r.rev == 0 || r.tx == 0 || r.key == "" {
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
	if d.err || len(d.b) != 0 || c.tx == 0 || c.count == 0 || c.lastRev == 0 {
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
	if n == 0 || n > maxRecord || int64(n) > int64(len(b)-frameSize) {
		return nil, 0
	}
	body = b[frameSize : frameSize+int(n)]
	if crc32.Checksum(body, castTab) != binary.LittleEndian.Uint32(b[4:8]) {
		return nil, 0
	}
	return body, frameSize + int(n)
}
