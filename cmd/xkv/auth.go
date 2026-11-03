// Copyright xkv.org 2026
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"runtime"
	"strings"
)

// minTokenLen is the shortest bearer token serve accepts.
const minTokenLen = 32

// loadToken reads a bearer token from a file: one line of printable ASCII
// without spaces, at least minTokenLen long, in a file only its owner can
// read (on systems with Unix permissions). Errors never include the token.
func loadToken(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("token file: %w", err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return "", fmt.Errorf("token file: %w", err)
	}
	if !info.Mode().IsRegular() {
		return "", errors.New("token file: not a regular file")
	}
	if runtime.GOOS != "windows" && info.Mode().Perm()&0o077 != 0 {
		return "", fmt.Errorf("token file %s can be read by others (mode %04o); chmod 600 it", path, info.Mode().Perm())
	}
	if info.Size() > 4096 {
		return "", errors.New("token file: larger than 4096 bytes")
	}
	buf := make([]byte, info.Size()+1)
	n, _ := f.Read(buf)
	s := string(buf[:n])
	s = strings.TrimSuffix(s, "\n")
	s = strings.TrimSuffix(s, "\r")
	if len(s) < minTokenLen {
		return "", fmt.Errorf("token file: the token must be one line of at least %d characters", minTokenLen)
	}
	for i := 0; i < len(s); i++ {
		if s[i] < 0x21 || s[i] > 0x7e {
			return "", errors.New("token file: the token must be one line of printable ASCII without spaces")
		}
	}
	return s, nil
}

// isLoopback reports whether a listen address only accepts local
// connections. An empty host means every interface, so it is not loopback.
func isLoopback(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// requireToken wraps h so every request must carry Authorization: Bearer
// with the write token, or with the read token for GET and HEAD. Digests of
// equal length are compared, both comparisons always run, in constant time,
// so timing says nothing about either token, its length included.
func requireToken(h http.Handler, write, read string) http.Handler {
	wd, rd := sha256.Sum256([]byte(write)), sha256.Sum256([]byte(read))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, ok := bearer(r.Header.Get("Authorization"))
		gd := sha256.Sum256([]byte(got))
		isWrite := subtle.ConstantTimeCompare(gd[:], wd[:]) == 1 && ok && write != ""
		isRead := subtle.ConstantTimeCompare(gd[:], rd[:]) == 1 && ok && read != ""
		switch {
		case isWrite:
		case isRead && (r.Method == http.MethodGet || r.Method == http.MethodHead):
		case isRead:
			fail(w, http.StatusForbidden, "this token can only read")
			return
		default:
			w.Header().Set("WWW-Authenticate", `Bearer realm="xkv"`)
			fail(w, http.StatusUnauthorized, "a bearer token is required")
			return
		}
		h.ServeHTTP(w, r)
	})
}

// bearer takes the token from an Authorization header value.
func bearer(v string) (string, bool) {
	f := strings.Fields(v)
	if len(f) != 2 || !strings.EqualFold(f[0], "Bearer") {
		return "", false
	}
	return f[1], true
}
