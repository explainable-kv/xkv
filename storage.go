// Copyright xkv.org 2026
// SPDX-License-Identifier: Apache-2.0

package xkv

import (
	"io"
	"os"
	"sync"
)

// storage is the byte store under a DB: a file on disk, or memory.
type storage interface {
	io.ReaderAt
	io.WriterAt
	Size() (int64, error)
	Truncate(size int64) error
	Sync() error
	Close() error
}

type fileStorage struct {
	f *os.File
}

func (s *fileStorage) ReadAt(p []byte, off int64) (int, error)  { return s.f.ReadAt(p, off) }
func (s *fileStorage) WriteAt(p []byte, off int64) (int, error) { return s.f.WriteAt(p, off) }
func (s *fileStorage) Truncate(n int64) error                   { return s.f.Truncate(n) }
func (s *fileStorage) Sync() error                              { return s.f.Sync() }
func (s *fileStorage) Close() error                             { return s.f.Close() }
func (s *fileStorage) Size() (int64, error) {
	st, err := s.f.Stat()
	if err != nil {
		return 0, err
	}
	return st.Size(), nil
}

// memStorage keeps the whole file in memory. It backs OpenMemory, used by the
// WebAssembly demo and by tests.
type memStorage struct {
	mu  sync.RWMutex
	buf []byte
}

func (m *memStorage) ReadAt(p []byte, off int64) (int, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if off < 0 {
		return 0, io.EOF
	}
	if off >= int64(len(m.buf)) {
		return 0, io.EOF
	}
	n := copy(p, m.buf[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (m *memStorage) WriteAt(p []byte, off int64) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	end := off + int64(len(p))
	if end > int64(len(m.buf)) {
		nb := make([]byte, end, end+end/4)
		copy(nb, m.buf)
		m.buf = nb
	}
	copy(m.buf[off:], p)
	return len(p), nil
}

func (m *memStorage) Size() (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return int64(len(m.buf)), nil
}

func (m *memStorage) Truncate(n int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if n < int64(len(m.buf)) {
		m.buf = m.buf[:n]
	} else {
		m.buf = append(m.buf, make([]byte, n-int64(len(m.buf)))...)
	}
	return nil
}

func (m *memStorage) Sync() error  { return nil }
func (m *memStorage) Close() error { return nil }
