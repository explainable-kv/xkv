// Copyright xkv.org 2026
// SPDX-License-Identifier: Apache-2.0

package xkv

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"os"
)

// The index snapshot is a sidecar file, <path>.idx, written on Close and
// after compaction. It lets Open skip reading every record of a large file.
// It is a cache, used only when it describes the data file exactly: the
// same generation, the same length, the same modification time, and the
// same last commit. Anything else (writes after it, a crash, a file restored
// from a backup) means a full scan, and the next Close writes a new one.

const snapshotSuffix = ".idx"

var snapMagic = [4]byte{'X', 'K', 'V', 'I'}

func (db *DB) writeSnapshot() error {
	if err := db.st.Sync(); err != nil {
		return err
	}
	lastCRC, lastOff, ok := db.lastCommitFrame()
	if !ok || db.file == nil {
		return nil
	}
	fi, err := db.file.Stat()
	if err != nil || fi.Size() != db.end {
		return err
	}
	b := make([]byte, 0, 64+len(db.revs)*16)
	b = append(b, snapMagic[:]...)
	b = binary.LittleEndian.AppendUint16(b, FormatVersion)
	b = append(b, db.hdr.generation[:]...)
	b = binary.AppendUvarint(b, uint64(db.end))
	b = binary.AppendVarint(b, fi.ModTime().UnixNano())
	b = binary.AppendUvarint(b, uint64(lastOff))
	b = binary.LittleEndian.AppendUint32(b, lastCRC)
	b = binary.AppendUvarint(b, db.lastRev)
	b = binary.AppendUvarint(b, db.lastTx)

	ids := make(map[string]uint64, len(db.keys))
	b = binary.AppendUvarint(b, uint64(len(db.keys)))
	for name := range db.keys {
		ids[name] = uint64(len(ids))
		b = appendString(b, name)
	}
	b = binary.AppendUvarint(b, uint64(len(db.revs)))
	var prev uint64
	for _, m := range db.revs {
		b = binary.AppendUvarint(b, m.rev-prev)
		prev = m.rev
		b = binary.AppendUvarint(b, m.tx)
		b = binary.AppendUvarint(b, uint64(m.off))
		b = binary.AppendUvarint(b, uint64(m.n))
		var fl byte
		if m.deleted {
			fl |= 1
		}
		if m.gap {
			fl |= 2
		}
		b = append(b, fl)
		b = binary.AppendVarint(b, m.time)
		b = binary.AppendUvarint(b, ids[m.key])
	}
	b = binary.LittleEndian.AppendUint32(b, crc32.Checksum(b, castTab))

	tmp := db.path + snapshotSuffix + ".tmp"
	mode := fi.Mode().Perm()
	if err := os.WriteFile(tmp, b, mode); err != nil {
		os.Remove(tmp)
		return err
	}
	// WriteFile's mode is cut by the umask and ignored for an existing file.
	if err := os.Chmod(tmp, mode); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, db.path+snapshotSuffix); err != nil {
		os.Remove(tmp)
		return err
	}
	db.dirty = false
	return nil
}

// lastCommitFrame finds the frame of the last commit record, by walking back
// from the last revision of the last transaction.
func (db *DB) lastCommitFrame() (crc uint32, off int64, ok bool) {
	if len(db.revs) == 0 {
		return 0, 0, false
	}
	m := db.revs[len(db.revs)-1]
	off = m.off + int64(m.n)
	hdr := make([]byte, frameSize)
	if _, err := db.st.ReadAt(hdr, off); err != nil {
		return 0, 0, false
	}
	return binary.LittleEndian.Uint32(hdr[4:8]), off, true
}

// loadSnapshot fills the index from the snapshot and returns the offset to
// scan from. On any mismatch it leaves the index empty and returns false.
func (db *DB) loadSnapshot(size int64) (int64, bool) {
	fi, err := db.file.Stat()
	if err != nil {
		return 0, false
	}
	b, err := os.ReadFile(db.path + snapshotSuffix)
	if err != nil || len(b) < 4+2+16+4 {
		return 0, false
	}
	body := b[:len(b)-4]
	if crc32.Checksum(body, castTab) != binary.LittleEndian.Uint32(b[len(b)-4:]) {
		return 0, false
	}
	if !bytes.Equal(body[0:4], snapMagic[:]) || binary.LittleEndian.Uint16(body[4:6]) != FormatVersion {
		return 0, false
	}
	if !bytes.Equal(body[6:22], db.hdr.generation[:]) {
		return 0, false
	}
	d := decoder{b: body[22:]}
	end := int64(d.uvarint())
	mtime := d.varint()
	lastOff := int64(d.uvarint())
	if d.err || len(d.b) < 4 {
		return 0, false
	}
	lastCRC := binary.LittleEndian.Uint32(d.b[:4])
	d.b = d.b[4:]
	lastRev := d.uvarint()
	lastTx := d.uvarint()
	if d.err || end != size || mtime != fi.ModTime().UnixNano() || lastOff < headerSize || lastOff >= end {
		return 0, false
	}
	// The commit the snapshot ends on must still be in the file, unchanged.
	fr := make([]byte, frameSize)
	if _, err := db.st.ReadAt(fr, lastOff); err != nil || binary.LittleEndian.Uint32(fr[4:8]) != lastCRC {
		return 0, false
	}
	nk := d.uvarint()
	if d.err || nk > uint64(len(d.b)) {
		return 0, false
	}
	names := make([]string, 0, nk)
	for i := uint64(0); i < nk && !d.err; i++ {
		names = append(names, d.str(MaxKeySize))
	}
	nr := d.uvarint()
	if d.err || nr > uint64(len(d.b)) {
		return 0, false
	}
	keys := make(map[string]*keyState, nk)
	revs := make([]revMeta, 0, nr)
	var prev uint64
	for i := uint64(0); i < nr && !d.err; i++ {
		delta := d.uvarint()
		txid := d.uvarint()
		off := int64(d.uvarint())
		n := d.uvarint()
		fl := d.byte1()
		t := d.varint()
		id := d.uvarint()
		if d.err || delta == 0 || prev+delta < prev || txid == 0 || txid > lastTx || id >= uint64(len(names)) || n < frameSize || n > maxRecord+frameSize || off < headerSize || off+int64(n) > end {
			return 0, false
		}
		prev += delta
		name := names[id]
		k, ok := keys[name]
		if !ok {
			k = &keyState{name: name}
			keys[name] = k
		}
		k.revs = append(k.revs, prev)
		revs = append(revs, revMeta{rev: prev, tx: txid, off: off, n: uint32(n), deleted: fl&1 != 0, gap: fl&2 != 0, time: t, key: k.name})
	}
	if d.err || len(d.b) != 0 || prev != lastRev && nr > 0 {
		return 0, false
	}
	db.keys = keys
	db.revs = revs
	db.lastRev = lastRev
	db.lastTx = lastTx
	return end, true
}
