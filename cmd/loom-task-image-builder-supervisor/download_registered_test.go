package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func registeredDownloadFixture(t *testing.T, server *httptest.Server) (*SecretBuffer, RegisteredBundlePlan, BundleSessionBinding, BundleDownloadTrust) {
	t.Helper()
	wire, plan, session, trust, _ := registeredCapabilityFixture(t)
	now := time.Now().UTC().Truncate(time.Second)
	session.ExpiresAt = now.Add(time.Minute)
	wire.IssuedAt, wire.ExpiresAt = now.Format(time.RFC3339), now.Add(40*time.Second).Format(time.RFC3339)
	trust.Origin, trust.Roots = server.URL, x509.NewCertPool()
	trust.Roots.AddCert(server.Certificate())
	for i := range wire.Objects {
		wire.Objects[i].URL = strings.Replace(wire.Objects[i].URL, "https://objects.example:9443", server.URL, 1)
		wire.Objects[i].URL = strings.Replace(wire.Objects[i].URL, "20260909T140000Z", now.Format("20060102T150405Z"), 1)
	}
	payload, err := json.Marshal(wire)
	if err != nil { t.Fatal(err) }
	fd := createMemfdFixture(t, "registered-bundle", payload, requiredMemfdSeals, true)
	secret, err := NewSecretBuffer(fd, maxRegisteredCapabilityBytes)
	if err != nil { t.Fatal(err) }
	t.Cleanup(secret.Close)
	return secret, plan, session, trust
}

func TestDownloadRegisteredBundleUsesIndependentTLSAndPrivateDataOnlyContext(t *testing.T) {
	files := registeredManifestVector()
	var calls atomic.Int32
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" || !strings.Contains(r.URL.RawQuery, "X-Amz-Signature=") || r.Header.Get("Authorization") != "" {
			t.Error("signed request target lost or ambient authorization added")
		}
		for _, file := range files {
			if strings.HasSuffix(r.URL.Path, "/"+file.RelativePath) {
				calls.Add(1)
				_, _ = w.Write(make([]byte, file.SizeBytes))
				return
			}
		}
		http.NotFound(w, r)
	}))
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS13}
	server.StartTLS()
	defer server.Close()
	secret, plan, session, trust := registeredDownloadFixture(t, server)
	t.Setenv("HTTPS_PROXY", "https://127.0.0.1:1")
	t.Setenv("AWS_SESSION_TOKEN", "not-authorized-for-native-download")
	workspace := t.TempDir()
	if err := os.WriteFile(filepath.Join(workspace, "runtime-state"), []byte("not Docker context"), 0o600); err != nil { t.Fatal(err) }
	fd := openDirectoryFD(t, workspace)
	defer syscall.Close(fd)
	got, err := DownloadRegisteredBundle(context.Background(), secret, fd, plan, session, trust, time.Now)
	if err != nil { t.Fatal(err) }
	defer got.Close()
	if got.Root == workspace || filepath.Dir(got.Root) != workspace || got.RelativeRoot != filepath.Base(got.Root) || calls.Load() != 4 {
		t.Fatal("download did not isolate complete data context")
	}
	for _, file := range files {
		mode := os.FileMode(0o644)
		if file.Mode == "0755" { mode = 0o755 }
		assertFilePayloadAndMode(t, filepath.Join(got.Root, file.RelativePath), make([]byte, file.SizeBytes), mode)
	}
	if entries, err := os.ReadDir(got.Root); err != nil || len(entries) != len(files) { t.Fatal("extra context bytes") }
	if err := got.Close(); err != nil { t.Fatal(err) }
	if entries, err := os.ReadDir(workspace); err != nil || len(entries) != 1 || entries[0].Name() != "runtime-state" {
		t.Fatal("owned download cleanup touched unrelated job state")
	}
}

func TestDownloadRegisteredBundleRejectsTransportDriftAndCleansOwnedContext(t *testing.T) {
	for _, kind := range []string{"untrusted_tls", "redirect", "hash", "size", "encoding", "expired", "regression", "cancel"} {
		t.Run(kind, func(t *testing.T) {
			var calls atomic.Int32
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if kind == "redirect" { http.Redirect(w, r, "https://other.invalid/secret", http.StatusFound); return }
				if kind == "cancel" { cancel(); <-r.Context().Done(); return }
				if kind == "encoding" { w.Header().Set("Content-Encoding", "gzip") }
				for _, file := range registeredManifestVector() {
					if strings.HasSuffix(r.URL.Path, "/"+file.RelativePath) {
						payload := make([]byte, file.SizeBytes)
						if kind == "size" { payload = append(payload, 1) }
						if kind == "hash" && len(payload) != 0 { payload[0] = 1 }
						_, _ = w.Write(payload)
						return
					}
				}
			}))
			defer server.Close()
			secret, plan, session, trust := registeredDownloadFixture(t, server)
			if kind == "untrusted_tls" { trust.Roots = x509.NewCertPool() }
			clock := func() time.Time {
				now := time.Now()
				if calls.Load() > 0 && kind == "expired" { return now.Add(time.Hour) }
				if calls.Load() > 0 && kind == "regression" { return now.Add(-time.Hour) }
				return now
			}
			workspace := t.TempDir()
			fd := openDirectoryFD(t, workspace)
			defer syscall.Close(fd)
			got, err := DownloadRegisteredBundle(ctx, secret, fd, plan, session, trust, clock)
			if err == nil || got != nil { t.Fatal("accepted incomplete or expired native bundle") }
			if strings.Contains(err.Error(), "X-Amz-") || strings.Contains(err.Error(), server.URL) { t.Fatal("signed target leaked") }
			if entries, err := os.ReadDir(workspace); err != nil || len(entries) != 0 { t.Fatal("failed native download left context") }
		})
	}
}

func TestRegisteredURLParsingPreservesLiteralEncodedSeparators(t *testing.T) {
	origin, _ := url.Parse("https://objects.example")
	now := time.Date(2026, 9, 9, 14, 0, 0, 0, time.UTC)
	value := "https://objects.example/bucket/a%252Fb%2B%25?X-Amz-Date=20260909T140000Z&X-Amz-Expires=40&X-Amz-Signature=unchanged"
	if err := validateRegisteredBundleURL(value, origin, "/bucket/a%2Fb+%", now, now.Add(40*time.Second)); err != nil { t.Fatal(err) }
	request, err := http.NewRequest(http.MethodGet, value, nil)
	if err != nil || request.URL.RequestURI() != strings.TrimPrefix(value, "https://objects.example") {
		t.Fatal("request construction changed signed RawPath/RawQuery")
	}
}
