// Copyright xkv.org 2026
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/explainable-kv/xkv"
)

const (
	writeTok = "w-0123456789abcdef0123456789abcdef"
	readTok  = "r-0123456789abcdef0123456789abcdef"
)

func tokenFile(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLoadToken(t *testing.T) {
	for _, ok := range []string{writeTok, writeTok + "\n", writeTok + "\r\n"} {
		if got, err := loadToken(tokenFile(t, ok)); err != nil || got != writeTok {
			t.Fatalf("%q: %q %v", ok, got, err)
		}
	}
	for _, bad := range []string{"short", writeTok + " x", writeTok + "\nsecond", "\t" + writeTok, ""} {
		_, err := loadToken(tokenFile(t, bad))
		if err == nil {
			t.Fatalf("%q accepted", bad)
		}
		if strings.Contains(err.Error(), "0123456789") {
			t.Fatalf("error shows the token: %v", err)
		}
	}
	if runtime.GOOS != "windows" {
		p := tokenFile(t, writeTok)
		os.Chmod(p, 0o644)
		if _, err := loadToken(p); err == nil || !strings.Contains(err.Error(), "chmod 600") {
			t.Fatalf("readable by others: %v", err)
		}
	}
	if _, err := loadToken(t.TempDir()); err == nil {
		t.Fatal("directory accepted")
	}
}

func TestIsLoopback(t *testing.T) {
	for addr, want := range map[string]bool{
		"127.0.0.1:7070": true, "[::1]:7070": true, "localhost:7070": true, "127.1.2.3:80": true,
		":7070": false, "0.0.0.0:7070": false, "[::]:7070": false, "10.0.0.5:7070": false, "example.com:80": false, "nonsense": false,
	} {
		if isLoopback(addr) != want {
			t.Fatalf("%s: want %v", addr, want)
		}
	}
}

func TestRequireToken(t *testing.T) {
	db, err := xkv.Open(filepath.Join(t.TempDir(), "s.xkv"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	srv := httptest.NewServer(requireToken(newHandler(db), writeTok, readTok))
	defer srv.Close()
	do := func(method, path, auth, body string) int {
		req, _ := http.NewRequest(method, srv.URL+path, strings.NewReader(body))
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if bytes.Contains(b, []byte("0123456789")) {
			t.Fatalf("body shows a token: %s", b)
		}
		if resp.StatusCode == 401 && resp.Header.Get("WWW-Authenticate") == "" {
			t.Fatal("401 without WWW-Authenticate")
		}
		return resp.StatusCode
	}
	put := `{"value":"1","reason":"test"}`
	for _, c := range []struct {
		method, path, auth, body string
		want                     int
	}{
		{"GET", "/v1/stats", "", "", 401},
		{"GET", "/v1/stats", "Bearer wrong", "", 401},
		{"GET", "/v1/stats", "Basic " + writeTok, "", 401},
		{"GET", "/v1/stats", "Bearer " + writeTok[:len(writeTok)-1], "", 401},
		{"PUT", "/v1/keys/a", "", put, 401},
		{"PUT", "/v1/keys/a", "Bearer " + readTok, put, 403},
		{"POST", "/v1/import", "Bearer " + readTok, "", 403},
		{"PUT", "/v1/keys/a", "bearer " + writeTok, put, 200},
		{"GET", "/v1/keys/a", "Bearer " + readTok, "", 200},
		{"HEAD", "/v1/stats", "Bearer " + readTok, "", 200},
		{"GET", "/v1/why/a", "Bearer " + writeTok, "", 200},
		{"DELETE", "/v1/keys/a", "Bearer " + readTok, `{"reason":"x"}`, 403},
	} {
		if got := do(c.method, c.path, c.auth, c.body); got != c.want {
			t.Fatalf("%s %s %q: %d, want %d", c.method, c.path, c.auth, got, c.want)
		}
	}
}

// selfSigned writes a certificate and key for 127.0.0.1 and returns their paths
// and a pool that trusts the certificate.
func selfSigned(t *testing.T) (string, string, *x509.CertPool) {
	t.Helper()
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "xkv test"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IPAddresses: nil, DNSNames: []string{"localhost"}}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	kb, _ := x509.MarshalECPrivateKey(key)
	dir := t.TempDir()
	cp, kp := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	os.WriteFile(cp, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600)
	os.WriteFile(kp, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kb}), 0o600)
	cert, _ := x509.ParseCertificate(der)
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	return cp, kp, pool
}

// startServe runs the serve command and returns its address and a stop func.
func startServe(t *testing.T, args ...string) (string, *bytes.Buffer, func() error) {
	t.Helper()
	addrs := make(chan string, 1)
	var sigs chan<- os.Signal
	serveListening = func(a string) { addrs <- a }
	serveSignals = func(c chan<- os.Signal) { sigs = c }
	var stdout, stderr bytes.Buffer
	done := make(chan error, 1)
	go func() { done <- cmdServe(args, &stdout, &stderr) }()
	select {
	case a := <-addrs:
		return a, &stderr, func() error { sigs <- os.Interrupt; return <-done }
	case err := <-done:
		t.Fatalf("serve stopped: %v (%s)", err, stderr.String())
	case <-time.After(10 * time.Second):
		t.Fatal("serve did not start")
	}
	return "", nil, nil
}

func TestServeTokensAndTLS(t *testing.T) {
	file := filepath.Join(t.TempDir(), "s.xkv")
	wf, rf := tokenFile(t, writeTok+"\n"), tokenFile(t, readTok)
	cert, key, pool := selfSigned(t)
	addr, _, stop := startServe(t, file, "--addr", "localhost:0", "--token-file", wf, "--read-token-file", rf, "--tls-cert", cert, "--tls-key", key)
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, ServerName: "localhost"}}}
	req, _ := http.NewRequest("PUT", "https://"+addr+"/v1/keys/k", strings.NewReader(`{"value":"v","reason":"over tls"}`))
	req.Header.Set("Authorization", "Bearer "+writeTok)
	resp, err := client.Do(req)
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("put over TLS: %v %v", err, resp)
	}
	resp.Body.Close()
	resp, err = client.Get("https://" + addr + "/v1/stats")
	if err != nil || resp.StatusCode != 401 {
		t.Fatalf("no token over TLS: %v %v", err, resp)
	}
	resp.Body.Close()
	// TLS 1.1 is refused.
	old := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, ServerName: "localhost", MaxVersion: tls.VersionTLS11}}}
	if _, err := old.Get("https://" + addr + "/v1/stats"); err == nil {
		t.Fatal("TLS 1.1 accepted")
	}
	if err := stop(); err != nil {
		t.Fatal(err)
	}
}

func TestServeRefusals(t *testing.T) {
	file := filepath.Join(t.TempDir(), "s.xkv")
	wf := tokenFile(t, writeTok)
	cert, key, _ := selfSigned(t)
	for _, args := range [][]string{
		{file, "--addr", "0.0.0.0:0"},
		{file, "--addr", ":0"},
		{file, "--token-file", wf, "--read-token-file", wf},
		{file, "--token-file", wf, "--tls-cert", cert},
		{file, "--token-file", wf, "--tls-cert", key, "--tls-key", key},
		{file, "--token-file", filepath.Join(t.TempDir(), "missing")},
	} {
		var out, errb bytes.Buffer
		if err := cmdServe(args, &out, &errb); err == nil {
			t.Fatalf("%v: served", args)
		} else if strings.Contains(err.Error(), "0123456789") {
			t.Fatalf("%v: error shows the token: %v", args, err)
		}
	}
	// A non-loopback address with a token starts, and warns without TLS.
	_, stderr, stop := startServe(t, file, "--addr", "0.0.0.0:0", "--token-file", wf)
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stderr.String(), "without TLS") {
		t.Fatalf("no warning: %q", stderr.String())
	}
}

func TestHTTPWhyPROV(t *testing.T) {
	db, err := xkv.Open(filepath.Join(t.TempDir(), "s.xkv"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	srv := httptest.NewServer(newHandler(db))
	defer srv.Close()
	for _, body := range []string{`{"value":"a","reason":"first"}`, `{"value":"b","reason":"second","causes":[{"key":"k"}]}`} {
		req, _ := http.NewRequest("PUT", srv.URL+"/v1/keys/k", strings.NewReader(body))
		resp, err := http.DefaultClient.Do(req)
		if err != nil || resp.StatusCode != 200 {
			t.Fatalf("put: %v %v", err, resp)
		}
		resp.Body.Close()
	}
	for path, want := range map[string]int{"/v1/why/k?format=prov": 200, "/v1/revs/2/why?format=prov": 200, "/v1/why/k?format=xml": 400, "/v1/why/nope?format=prov": 404} {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != want {
			t.Fatalf("%s: %d %s", path, resp.StatusCode, b)
		}
		if want == 200 && (resp.Header.Get("Content-Type") != "application/ld+json" || !bytes.Contains(b, []byte(`"prov:wasRevisionOf"`))) {
			t.Fatalf("%s: %s %s", path, resp.Header.Get("Content-Type"), b)
		}
	}
}
