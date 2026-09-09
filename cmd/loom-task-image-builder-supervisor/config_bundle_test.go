package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"
)

func bundleTLSFixture(t *testing.T) ([]byte, tls.Certificate) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "bundle-root"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true,
		BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	leaf := &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "bundle-server"},
		NotBefore: ca.NotBefore, NotAfter: ca.NotAfter, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}}
	leafDER, err := x509.CreateCertificate(rand.Reader, leaf, ca, &leafKey.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}),
		tls.Certificate{Certificate: [][]byte{leafDER}, PrivateKey: leafKey}
}

func TestConfigBundleTrustDrivesRealTLSDownloadWithoutAmbientRoots(t *testing.T) {
	caPEM, certificate := bundleTLSFixture(t)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for _, file := range registeredManifestVector() {
			if strings.HasSuffix(r.URL.Path, "/"+file.RelativePath) {
				_, _ = w.Write(make([]byte, file.SizeBytes))
				return
			}
		}
		http.NotFound(w, r)
	}))
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{certificate}}
	server.StartTLS()
	defer server.Close()
	secret, plan, session, _ := registeredDownloadFixture(t, server)
	path, release, ca, _ := configuredBundleFixture(t)
	rewriteBundleConfigFixture(t, ca, caPEM)
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var config map[string]any
	if err := json.Unmarshal(payload, &config); err != nil {
		t.Fatal(err)
	}
	bundle := config["bundle"].(map[string]any)
	bundle["origin"], bundle["bucket"] = server.URL, plan.Bucket
	bundle["ca"].(map[string]any)["sha256"] = sha256FileHex(t, ca)
	payload, err = json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	rewriteBundleConfigFixture(t, path, payload)
	cfg, err := LoadConfig(path, release)
	if err != nil || cfg.Bundle == nil {
		t.Fatalf("load TLS trust: %v", err)
	}
	// Neither a later release-file replacement nor environment trust controls
	// the already loaded owned certificate pool.
	rewriteBundleConfigFixture(t, ca, bundleCAFixture(t))
	t.Setenv("SSL_CERT_FILE", ca)
	fd := openDirectoryFD(t, t.TempDir())
	defer syscall.Close(fd)
	got, err := DownloadRegisteredBundle(context.Background(), secret, fd, plan, session, *cfg.Bundle, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if err := got.Close(); err != nil {
		t.Fatal(err)
	}
	// A correctly hashed but unrelated configured root must not be augmented
	// with ambient roots, even if the server root is present there.
	bundle["ca"].(map[string]any)["sha256"] = sha256FileHex(t, ca)
	payload, err = json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	rewriteBundleConfigFixture(t, path, payload)
	cfg, err = LoadConfig(path, release)
	if err != nil {
		t.Fatal(err)
	}
	rewriteBundleConfigFixture(t, ca, caPEM)
	if got, err := DownloadRegisteredBundle(context.Background(), secret, fd, plan, session, *cfg.Bundle, time.Now); err == nil || got != nil {
		t.Fatal("download accepted unconfigured TLS root")
	}
}

func bundleCAFixture(t *testing.T) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "disposable-bundle-ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true,
		BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, ca, ca, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func rewriteBundleConfigFixture(t *testing.T, path string, payload []byte) {
	t.Helper()
	rewriteReadOnlyFixture(t, path, payload)
	// The older negative-test helper deliberately leaves the file writable.
	// These fixtures must restore the valid mode so the intended boundary fails.
	if err := os.Chmod(path, 0o444); err != nil {
		t.Fatal(err)
	}
}

func configuredBundleFixture(t *testing.T) (string, string, string, string) {
	t.Helper()
	root, release := t.TempDir(), strings.Repeat("a", 64)
	useTestConfigPolicy(t, root)
	paths := makeReleaseTree(t, root, release)
	configPath := writeConfigFixture(t, root, release, runtime.GOARCH, paths, nil)
	releaseRoot := filepath.Join(root, "releases", release)
	if err := os.Chmod(releaseRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	trustDir := filepath.Join(releaseRoot, "trust")
	if err := os.Mkdir(trustDir, 0o755); err != nil {
		t.Fatal(err)
	}
	caPath := filepath.Join(trustDir, "bundle-ca.pem")
	if err := os.WriteFile(caPath, bundleCAFixture(t), 0o444); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(trustDir, 0o555); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(releaseRoot, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(trustDir, 0o755) })
	payload, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	var config map[string]any
	if err := json.Unmarshal(payload, &config); err != nil {
		t.Fatal(err)
	}
	config["bundle"] = map[string]any{"origin": "https://objects.example:9443", "bucket": "loom-bundles",
		"ca": map[string]any{"path": caPath, "sha256": sha256FileHex(t, caPath)}}
	payload, err = json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	rewriteBundleConfigFixture(t, configPath, payload)
	return configPath, release, caPath, trustDir
}

func TestConfigLoadsBundleTrustOnlyFromOwnedDigestBoundReleaseMember(t *testing.T) {
	path, release, ca, _ := configuredBundleFixture(t)
	cfg, err := LoadConfig(path, release)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Bundle == nil || cfg.Bundle.Origin != "https://objects.example:9443" || cfg.Bundle.Bucket != "loom-bundles" || cfg.Bundle.Roots == nil {
		t.Fatal("bundle trust was not loaded")
	}
	// The loaded certificate pool is an owned snapshot, not a later path read.
	rewriteBundleConfigFixture(t, ca, []byte("changed"))
	if _, _, err := registeredBundleHTTPClient(*cfg.Bundle); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadConfig(path, release); err == nil {
		t.Fatal("accepted changed release CA")
	}
}

func TestConfigRejectsInvalidBundleTrustBeforeRuntime(t *testing.T) {
	for _, kind := range []string{"digest", "foreign_path", "symlink", "parent_symlink", "writable", "writable_parent", "hardlink", "fifo", "oversized", "invalid_pem", "mixed_private_key", "malformed_prefix", "origin", "bucket", "null", "missing_ca"} {
		t.Run(kind, func(t *testing.T) {
			path, release, ca, trustDir := configuredBundleFixture(t)
			if _, err := LoadConfig(path, release); err != nil {
				t.Fatalf("invalid baseline: %v", err)
			}
			payload, _ := os.ReadFile(path)
			var config map[string]any
			if err := json.Unmarshal(payload, &config); err != nil {
				t.Fatal(err)
			}
			bundle := config["bundle"].(map[string]any)
			member := bundle["ca"].(map[string]any)
			switch kind {
			case "digest":
				member["sha256"] = strings.Repeat("6", 64)
			case "foreign_path":
				member["path"] = filepath.Join(t.TempDir(), "ca.pem")
			case "origin":
				bundle["origin"] = "https://user:secret@objects.example:9443/"
			case "bucket":
				bundle["bucket"] = "other/objects"
			case "null":
				config["bundle"] = nil
			case "missing_ca":
				delete(bundle, "ca")
			case "writable":
				if err := os.Chmod(ca, 0o644); err != nil {
					t.Fatal(err)
				}
			case "writable_parent":
				if err := os.Chmod(trustDir, 0o755); err != nil {
					t.Fatal(err)
				}
			case "symlink", "fifo", "hardlink":
				if err := os.Chmod(trustDir, 0o755); err != nil {
					t.Fatal(err)
				}
				if kind == "hardlink" {
					if err := os.Link(ca, filepath.Join(trustDir, "alias.pem")); err != nil {
						t.Fatal(err)
					}
				} else {
					if err := os.Rename(ca, ca+".original"); err != nil {
						t.Fatal(err)
					}
					if kind == "symlink" {
						if err := os.Symlink(ca+".original", ca); err != nil {
							t.Fatal(err)
						}
					} else {
						if err := syscall.Mkfifo(ca, 0o444); err != nil {
							t.Fatal(err)
						}
					}
				}
				if err := os.Chmod(trustDir, 0o555); err != nil {
					t.Fatal(err)
				}
			case "parent_symlink":
				parent := filepath.Dir(trustDir)
				if err := os.Chmod(parent, 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.Rename(trustDir, trustDir+".original"); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(trustDir+".original", trustDir); err != nil {
					t.Fatal(err)
				}
				if err := os.Chmod(parent, 0o555); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = os.Chmod(trustDir+".original", 0o755) })
			default:
				bad := []byte("not a certificate")
				if kind == "oversized" {
					bad = []byte(strings.Repeat("a", 128*1024+1))
				}
				if kind == "mixed_private_key" {
					bad, _ = os.ReadFile(ca)
					bad = append(bad, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: []byte("fixture-not-a-key")})...)
				}
				if kind == "malformed_prefix" {
					valid, _ := os.ReadFile(ca)
					bad = append([]byte("-----BEGIN CERTIFICATE-----\nmalformed\n"), valid...)
				}
				rewriteBundleConfigFixture(t, ca, bad)
				member["sha256"] = sha256FileHex(t, ca)
			}
			payload, _ = json.Marshal(config)
			rewriteBundleConfigFixture(t, path, payload)
			if _, err := LoadConfig(path, release); err == nil {
				t.Fatal("accepted invalid bundle trust")
			}
		})
	}
}
