// Copyright xkv.org 2026
// SPDX-License-Identifier: Apache-2.0

//go:build unix

package xkv

import (
	"errors"
	"os"
	"syscall"
)

// lockFile takes an exclusive, non-blocking lock on f. It fails with
// ErrLocked when another open handle, in this process or another, holds it.
func lockFile(f *os.File) error {
	for {
		err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			return nil
		}
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return ErrLocked
		}
		return err
	}
}

// replaceFile moves the compacted file over the original and returns the
// handle for the file now at path. On Unix the rename is atomic and both
// handles stay open across it, so the lock is never let go.
func replaceFile(tmpF *os.File, tmp, path string, closeOld func() error) (*os.File, error) {
	if err := os.Rename(tmp, path); err != nil {
		return nil, err
	}
	syncDir(path)
	return tmpF, closeOld()
}
