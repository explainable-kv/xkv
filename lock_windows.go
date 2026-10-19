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

func lockFile(f *os.File) error {
	var ol syscall.Overlapped
	r, _, err := procLockFileEx.Call(f.Fd(), lockfileExclusiveLock|lockfileFailImmediately, 0, 1, 0, uintptr(unsafe.Pointer(&ol)))
	if r != 0 {
		return nil
	}
	if err == errorLockViolation || err == syscall.ERROR_IO_PENDING {
		return ErrLocked
	}
	return err
}

// replaceFile moves the compacted file over the original and returns a
// locked handle for it. Windows can't rename a file that is open, so both
// handles close first and the new file is opened and locked again after.
func replaceFile(tmpF *os.File, tmp, path string, closeOld func() error) (*os.File, error) {
	if err := tmpF.Close(); err != nil {
		return nil, err
	}
	if err := closeOld(); err != nil {
		return nil, err
	}
	if err := os.Rename(tmp, path); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	if err := lockFile(f); err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}
