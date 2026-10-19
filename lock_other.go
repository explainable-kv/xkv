// Copyright xkv.org 2026
// SPDX-License-Identifier: Apache-2.0

//go:build (!unix && !windows) || solaris || aix

package xkv

import "os"

// lockFile does nothing on platforms without flock (WebAssembly, Solaris,
// AIX): there, keeping to one writer per file is up to the caller.
func lockFile(f *os.File) error { return nil }

func replaceFile(tmpF *os.File, tmp, path string, old *os.File) (*os.File, bool, error) {
	tmpF.Close()
	old.Close()
	renameErr := os.Rename(tmp, path)
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		f = nil
	}
	if renameErr != nil {
		return f, false, renameErr
	}
	return f, true, err
}
