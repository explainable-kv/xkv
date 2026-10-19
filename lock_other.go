// Copyright xkv.org 2026
// SPDX-License-Identifier: Apache-2.0

//go:build !unix && !windows

package xkv

import "os"

// lockFile does nothing on platforms without file locks (WebAssembly).
func lockFile(f *os.File) error { return nil }

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
	return os.OpenFile(path, os.O_RDWR, 0)
}
