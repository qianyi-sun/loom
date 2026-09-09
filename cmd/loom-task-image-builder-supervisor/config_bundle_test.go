package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"
)

func bundleCAFixture(t *testing.T) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil { t.Fatal(err) }
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "disposable-bundle-ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true,
		BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, ca, ca, &key.PublicKey, key)
	if err != nil { t.Fatal(err) }
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func configuredBundleFixture(t *testing.T) (string, string, string, string) {
	t.Helper()
	root, release := t.TempDir(), strings.Repeat("a", 64)
	useTestConfigPolicy(t, root)
	paths := makeReleaseTree(t, root, release)
	configPath := writeConfigFixture(t, root, release, runtime.GOARCH, paths, nil)
	releaseRoot := filepath.Join(root, "releases", release)
	if err := os.Chmod(releaseRoot, 0o755); err != nil { t.Fatal(err) }
	trustDir := filepath.Join(releaseRoot, "trust")
	if err := os.Mkdir(trustDir, 0o755); err != nil { t.Fatal(err) }
	caPath := filepath.Join(trustDir, "bundle-ca.pem")
	if err := os.WriteFile(caPath, bundleCAFixture(t), 0o444); err != nil { t.Fatal(err) }
	if err := os.Chmod(trustDir, 0o555); err != nil { t.Fatal(err) }
	if err := os.Chmod(releaseRoot, 0o555); err != nil { t.Fatal(err) }
	t.Cleanup(func() { _ = os.Chmod(trustDir, 0o755) })
	payload, err := os.ReadFile(configPath)
	if err != nil { t.Fatal(err) }
	var config map[string]any
	if err := json.Unmarshal(payload, &config); err != nil { t.Fatal(err) }
	config["bundle"] = map[string]any{"origin": "https://objects.example:9443", "bucket": "loom-bundles",
		"ca": map[string]any{"path": caPath, "sha256": sha256FileHex(t, caPath)}}
	payload, err = json.Marshal(config)
	if err != nil { t.Fatal(err) }
	rewriteReadOnlyFixture(t, configPath, payload)
	return configPath, release, caPath, trustDir
}

func TestConfigLoadsBundleTrustOnlyFromOwnedDigestBoundReleaseMember(t *testing.T) {
	path, release, ca, _ := configuredBundleFixture(t)
	cfg, err := LoadConfig(path, release)
	if err != nil { t.Fatal(err) }
	if cfg.Bundle == nil || cfg.Bundle.Origin != "https://objects.example:9443" || cfg.Bundle.Bucket != "loom-bundles" || cfg.Bundle.Roots == nil {
		t.Fatal("bundle trust was not loaded")
	}
	// The loaded certificate pool is an owned snapshot, not a later path read.
	rewriteReadOnlyFixture(t, ca, []byte("changed"))
	if _, _, err := registeredBundleHTTPClient(*cfg.Bundle); err != nil { t.Fatal(err) }
	if _, err := LoadConfig(path, release); err == nil { t.Fatal("accepted changed release CA") }
}

func TestConfigRejectsInvalidBundleTrustBeforeRuntime(t *testing.T) {
	for _, kind := range []string{"digest", "foreign_path", "symlink", "parent_symlink", "writable", "writable_parent", "hardlink", "fifo", "oversized", "invalid_pem", "mixed_private_key", "origin", "bucket", "null", "missing_ca"} {
		t.Run(kind, func(t *testing.T) {
			path, release, ca, trustDir := configuredBundleFixture(t)
			payload, _ := os.ReadFile(path)
			var config map[string]any
			if err := json.Unmarshal(payload, &config); err != nil { t.Fatal(err) }
			bundle := config["bundle"].(map[string]any)
			member := bundle["ca"].(map[string]any)
			switch kind {
			case "digest": member["sha256"] = strings.Repeat("6", 64)
			case "foreign_path": member["path"] = filepath.Join(t.TempDir(), "ca.pem")
			case "origin": bundle["origin"] = "https://user:secret@objects.example:9443/"
			case "bucket": bundle["bucket"] = "other/objects"
			case "null": config["bundle"] = nil
			case "missing_ca": delete(bundle, "ca")
			case "writable": if err := os.Chmod(ca, 0o644); err != nil { t.Fatal(err) }
			case "writable_parent": if err := os.Chmod(trustDir, 0o755); err != nil { t.Fatal(err) }
			case "symlink", "fifo", "hardlink":
				if err := os.Chmod(trustDir, 0o755); err != nil { t.Fatal(err) }
				if kind == "hardlink" {
					if err := os.Link(ca, filepath.Join(trustDir, "alias.pem")); err != nil { t.Fatal(err) }
				} else {
					if err := os.Rename(ca, ca+".original"); err != nil { t.Fatal(err) }
					if kind == "symlink" { if err := os.Symlink(ca+".original", ca); err != nil { t.Fatal(err) } } else {
						if err := syscall.Mkfifo(ca, 0o444); err != nil { t.Fatal(err) }
					}
				}
				if err := os.Chmod(trustDir, 0o555); err != nil { t.Fatal(err) }
			case "parent_symlink":
				parent := filepath.Dir(trustDir)
				if err := os.Chmod(parent, 0o755); err != nil { t.Fatal(err) }
				if err := os.Rename(trustDir, trustDir+".original"); err != nil { t.Fatal(err) }
				if err := os.Symlink(trustDir+".original", trustDir); err != nil { t.Fatal(err) }
				if err := os.Chmod(parent, 0o555); err != nil { t.Fatal(err) }
				t.Cleanup(func() { _ = os.Chmod(trustDir+".original", 0o755) })
			default:
				bad := []byte("not a certificate")
				if kind == "oversized" { bad = []byte(strings.Repeat("a", 128*1024+1)) }
				if kind == "mixed_private_key" { bad, _ = os.ReadFile(ca); bad = append(bad, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: []byte("fixture-not-a-key")})...) }
				rewriteReadOnlyFixture(t, ca, bad)
				member["sha256"] = sha256FileHex(t, ca)
			}
			payload, _ = json.Marshal(config)
			rewriteReadOnlyFixture(t, path, payload)
			if _, err := LoadConfig(path, release); err == nil { t.Fatal("accepted invalid bundle trust") }
		})
	}
}
