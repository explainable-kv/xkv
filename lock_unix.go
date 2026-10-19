// Copyright xkv.org 2026
// SPDX-License-Identifier: Apache-2.0

//go:build unix && !solaris && !aix

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

// replaceFile moves the compacted file over the original. On Unix the
// rename is atomic and both handles stay open across it, so the lock is
// never let go. It returns the handle for the file now at path and whether
// the swap happened. If it didn't, the old handle is returned untouched and
// the work file is closed.
func replaceFile(tmpF *os.File, tmp, path string, old *os.File) (*os.File, bool, error) {
	if err := os.Rename(tmp, path); err != nil {
		tmpF.Close()
		return old, false, err
	}
	syncDir(path)
	old.Close()
	return tmpF, true, nil
}
