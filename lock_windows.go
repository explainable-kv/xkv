// Copyright xkv.org 2026
// SPDX-License-Identifier: Apache-2.0

//go:build windows

package xkv

import (
	"os"
	"syscall"
	"unsafe"
)

var (
	modkernel32    = syscall.NewLazyDLL("kernel32.dll")
	procLockFileEx = modkernel32.NewProc("LockFileEx")
)

const (
	lockfileExclusiveLock   = 0x2
	lockfileFailImmediately = 0x1
	errorLockViolation      = syscall.Errno(33)
)

// lockFile takes an exclusive lock on one byte far past any data (offset
// 2^62). Windows locks are mandatory, so locking real bytes would block
// other handles from reading them; the lock is only a marker.
func lockFile(f *os.File) error {
	ol := syscall.Overlapped{OffsetHigh: 1 << 30}
	r, _, err := procLockFileEx.Call(f.Fd(), lockfileExclusiveLock|lockfileFailImmediately, 0, 1, 0, uintptr(unsafe.Pointer(&ol)))
	if r != 0 {
		return nil
	}
	if err == errorLockViolation || err == syscall.ERROR_IO_PENDING {
		return ErrLocked
	}
	return err
}

// replaceFile moves the compacted file over the original. Windows can't
// rename over a file that is open, so both handles close first and the file
// at path is opened and locked again after. If the rename fails (another
// process has the file open) the original is reopened, so the store goes on.
// It returns the handle for the file at path and whether the swap happened;
// a nil handle means none could be had.
func replaceFile(tmpF *os.File, tmp, path string, old *os.File) (*os.File, bool, error) {
	tmpF.Close()
	old.Close()
	renameErr := os.Rename(tmp, path)
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err == nil {
		if err = lockFile(f); err != nil {
			f.Close()
			f = nil
		}
	} else {
		f = nil
	}
	if renameErr != nil {
		if f == nil {
			return nil, false, renameErr
		}
		return f, false, renameErr
	}
	return f, true, err
}
