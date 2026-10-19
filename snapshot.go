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

// snapshotVersion is the layout of the snapshot file. Version 1 snapshots,
// written by xkv 0.1, are ignored: the next open reads the whole file and
// Close writes a version 2 one.
const snapshotVersion = 2

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
	b = binary.LittleEndian.AppendUint16(b, snapshotVersion)
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
	// The stores imported revisions came from, numbered for the entries.
	storeIDs := map[StoreID]uint64{}
	var storeList []StoreID
	for _, m := range db.revs {
		if m.origin != nil {
			if _, ok := storeIDs[m.origin.store]; !ok {
				storeIDs[m.origin.store] = uint64(len(storeList))
				storeList = append(storeList, m.origin.store)
			}
		}
	}
	b = binary.AppendUvarint(b, uint64(len(storeList)))
	for _, id := range storeList {
		b = append(b, id[:]...)
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
		if m.history {
			fl |= 4
		}
		if m.origin != nil {
			fl |= 8
		}
		b = append(b, fl)
		b = binary.AppendVarint(b, m.time)
		if m.history {
			b = appendString(b, m.key)
		} else {
			b = binary.AppendUvarint(b, ids[m.key])
		}
		if m.origin != nil {
			b = binary.AppendUvarint(b, storeIDs[m.origin.store])
			b = binary.AppendUvarint(b, m.origin.rev)
		}
	}
	// What the store has seen of each key from other stores (see seen).
	b = binary.AppendUvarint(b, uint64(len(db.keySeen)))
	for name, l := range db.keySeen {
		b = appendString(b, name)
		b = binary.AppendUvarint(b, uint64(len(l)))
		for _, e := range l {
			b = append(b, e.store[:]...)
			b = binary.AppendUvarint(b, e.rev)
		}
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
	if !bytes.Equal(body[0:4], snapMagic[:]) || binary.LittleEndian.Uint16(body[4:6]) != snapshotVersion {
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
	if d.err || end != size || mtime != fi.ModTime().UnixNano() || lastOff < db.hdr.size() || lastOff >= end {
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
	ns := d.uvarint()
	if d.err || ns > uint64(len(d.b))/16 {
		return 0, false
	}
	stores := make([]StoreID, ns)
	for i := range stores {
		copy(stores[i][:], d.b[:16])
		d.b = d.b[16:]
	}
	nr := d.uvarint()
	if d.err || nr > uint64(len(d.b)) {
		return 0, false
	}
	keys := make(map[string]*keyState, nk)
	origins := map[ident]uint64{}
	hist := map[string][]uint64{}
	keyTime := map[string]int64{}
	revs := make([]revMeta, 0, nr)
	var prev uint64
	for i := uint64(0); i < nr && !d.err; i++ {
		delta := d.uvarint()
		txid := d.uvarint()
		off := int64(d.uvarint())
		n := d.uvarint()
		fl := d.byte1()
		t := d.varint()
		var name string
		if fl&4 != 0 {
			name = d.str(MaxKeySize)
		} else if id := d.uvarint(); id < uint64(len(names)) {
			name = names[id]
		} else {
			return 0, false
		}
		var origin *ident
		if fl&8 != 0 {
			si := d.uvarint()
			orev := d.uvarint()
			if si >= uint64(len(stores)) || orev == 0 {
				return 0, false
			}
			origin = &ident{store: stores[si], rev: orev}
		}
		if d.err || fl&^15 != 0 || name == "" || delta == 0 || prev+delta < prev || txid == 0 || txid > lastTx || n < frameSize || n > maxRecord+frameSize || off < db.hdr.size() || off+int64(n) > end {
			return 0, false
		}
		prev += delta
		m := revMeta{rev: prev, tx: txid, off: off, n: uint32(n), deleted: fl&1 != 0, gap: fl&2 != 0, history: fl&4 != 0, time: t, key: name, origin: origin}
		if kt, ok := keyTime[name]; !ok || t > kt {
			keyTime[name] = t
		}
		if !m.history {
			k, ok := keys[name]
			if !ok {
				k = &keyState{name: name}
				keys[name] = k
			}
			k.revs = append(k.revs, prev)
			if origin == nil {
				k.own = prev
			}
			m.key = k.name
		} else {
			hist[name] = append(hist[name], prev)
		}
		if origin != nil {
			origins[*origin] = prev
		}
		revs = append(revs, m)
	}
	nks := d.uvarint()
	if d.err || nks > uint64(len(d.b)) {
		return 0, false
	}
	keySeen := make(map[string][]seenEntry, nks)
	for i := uint64(0); i < nks && !d.err; i++ {
		name := d.str(MaxKeySize)
		n := d.uvarint()
		if d.err || n == 0 || n > uint64(len(d.b))/17 {
			return 0, false
		}
		l := make([]seenEntry, n)
		for j := range l {
			copy(l[j].store[:], d.b[:16])
			d.b = d.b[16:]
			l[j].rev = d.uvarint()
		}
		keySeen[name] = l
	}
	if d.err || len(d.b) != 0 || prev != lastRev && nr > 0 {
		return 0, false
	}
	db.keys = keys
	db.origins = origins
	db.hist = hist
	db.keyTime = keyTime
	db.keySeen = keySeen
	db.revs = revs
	db.lastRev = lastRev
	db.lastTx = lastTx
	return end, true
}
