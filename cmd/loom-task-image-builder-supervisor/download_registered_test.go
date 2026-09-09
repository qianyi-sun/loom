package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
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
	if err != nil {
		t.Fatal(err)
	}
	fd := createMemfdFixture(t, "registered-bundle", payload, requiredMemfdSeals, true)
	secret, err := NewSecretBuffer(fd, maxRegisteredCapabilityBytes)
	if err != nil {
		t.Fatal(err)
	}
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
	if err := os.WriteFile(filepath.Join(workspace, "runtime-state"), []byte("not Docker context"), 0o600); err != nil {
		t.Fatal(err)
	}
	fd := openDirectoryFD(t, workspace)
	defer syscall.Close(fd)
	got, err := DownloadRegisteredBundle(context.Background(), secret, fd, plan, session, trust, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	defer got.Close()
	contextFD, err := got.DupDirectoryFD()
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.Close(contextFD)
	contextRoot := fmt.Sprintf("/proc/self/fd/%d", contextFD)
	if got.RelativeRoot == "" || calls.Load() != 4 {
		t.Fatal("download did not isolate complete data context")
	}
	for _, file := range files {
		mode := os.FileMode(0o644)
		if file.Mode == "0755" {
			mode = 0o755
		}
		assertFilePayloadAndMode(t, filepath.Join(contextRoot, file.RelativePath), make([]byte, file.SizeBytes), mode)
	}
	if entries, err := os.ReadDir(contextRoot); err != nil || len(entries) != len(files) {
		t.Fatal("extra context bytes")
	}
	if err := got.Close(); err != nil {
		t.Fatal(err)
	}
	if entries, err := os.ReadDir(workspace); err != nil || len(entries) != 1 || entries[0].Name() != "runtime-state" {
		t.Fatal("owned download cleanup touched unrelated job state")
	}
}

func TestDownloadRegisteredBundleRejectsTransportDriftAndCleansOwnedContext(t *testing.T) {
	for _, kind := range []string{"untrusted_tls", "redirect", "hash", "size", "encoding", "informational", "expired", "regression", "cancel"} {
		t.Run(kind, func(t *testing.T) {
			var calls atomic.Int32
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if kind == "informational" {
					w.WriteHeader(http.StatusEarlyHints)
				}
				if kind == "redirect" {
					http.Redirect(w, r, "https://other.invalid/secret", http.StatusFound)
					return
				}
				if kind == "cancel" {
					cancel()
					<-r.Context().Done()
					return
				}
				if kind == "encoding" {
					w.Header().Set("Content-Encoding", "gzip")
				}
				for _, file := range registeredManifestVector() {
					if strings.HasSuffix(r.URL.Path, "/"+file.RelativePath) {
						payload := make([]byte, file.SizeBytes)
						if kind == "size" {
							payload = append(payload, 1)
						}
						if kind == "hash" && len(payload) != 0 {
							payload[0] = 1
						}
						_, _ = w.Write(payload)
						return
					}
				}
			}))
			defer server.Close()
			secret, plan, session, trust := registeredDownloadFixture(t, server)
			if kind == "untrusted_tls" {
				trust.Roots = x509.NewCertPool()
			}
			clock := func() time.Time {
				now := time.Now()
				if calls.Load() > 0 && kind == "expired" {
					return now.Add(time.Hour)
				}
				if calls.Load() > 0 && kind == "regression" {
					return now.Add(-time.Hour)
				}
				return now
			}
			workspace := t.TempDir()
			fd := openDirectoryFD(t, workspace)
			defer syscall.Close(fd)
			got, err := DownloadRegisteredBundle(ctx, secret, fd, plan, session, trust, clock)
			if err == nil || got != nil {
				t.Fatal("accepted incomplete or expired native bundle")
			}
			if strings.Contains(err.Error(), "X-Amz-") || strings.Contains(err.Error(), server.URL) {
				t.Fatal("signed target leaked")
			}
			if entries, err := os.ReadDir(workspace); err != nil || len(entries) != 0 {
				t.Fatal("failed native download left context")
			}
		})
	}
}

func TestRegisteredDownloadHandoffRemainsDescriptorBoundWhenJobPathIsReplaced(t *testing.T) {
	outer := t.TempDir()
	job := filepath.Join(outer, "job")
	if err := os.Mkdir(job, 0o700); err != nil {
		t.Fatal(err)
	}
	fd := openDirectoryFD(t, job)
	defer syscall.Close(fd)
	var moved atomic.Bool
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if moved.CompareAndSwap(false, true) {
			entries, err := os.ReadDir(job)
			if err != nil || len(entries) != 1 {
				t.Error("missing private input root")
				http.Error(w, "fixture", 500)
				return
			}
			if err := os.Rename(job, filepath.Join(outer, "moved")); err != nil {
				t.Error(err)
				return
			}
			replacement := filepath.Join(job, entries[0].Name())
			if err := os.MkdirAll(replacement, 0o700); err != nil {
				t.Error(err)
				return
			}
			if err := os.WriteFile(filepath.Join(replacement, "foreign"), []byte("must survive"), 0o600); err != nil {
				t.Error(err)
				return
			}
		}
		for _, file := range registeredManifestVector() {
			if strings.HasSuffix(r.URL.Path, "/"+file.RelativePath) {
				_, _ = w.Write(make([]byte, file.SizeBytes))
				return
			}
		}
	}))
	defer server.Close()
	secret, plan, session, trust := registeredDownloadFixture(t, server)
	got, err := DownloadRegisteredBundle(context.Background(), secret, fd, plan, session, trust, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	defer got.Close()
	contextFD, err := got.DupDirectoryFD()
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.Close(contextFD)
	for _, object := range registeredManifestVector() {
		got, err := HashFileAt(contextFD, object.RelativePath)
		if err != nil || got.SHA256 != object.SHA256 {
			t.Fatal("handoff followed replaced job pathname")
		}
	}
	if err := got.Close(); err != nil {
		t.Fatal(err)
	}
	if payload, err := os.ReadFile(filepath.Join(job, got.RelativeRoot, "foreign")); err != nil || string(payload) != "must survive" {
		t.Fatal("cleanup followed replaced job pathname")
	}
}

func TestRegisteredURLParsingPreservesLiteralEncodedSeparators(t *testing.T) {
	origin, _ := url.Parse("https://objects.example")
	now := time.Date(2026, 9, 9, 14, 0, 0, 0, time.UTC)
	value := "https://objects.example/bucket/a%252Fb%2B%25?X-Amz-Date=20260909T140000Z&X-Amz-Expires=40&X-Amz-Signature=unchanged"
	if err := validateRegisteredBundleURL(value, origin, "/bucket/a%2Fb+%", now, now.Add(40*time.Second)); err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodGet, value, nil)
	if err != nil || request.URL.RequestURI() != strings.TrimPrefix(value, "https://objects.example") {
		t.Fatal("request construction changed signed RawPath/RawQuery")
	}
}

// The Python integration lane supplies a real authority-issued V2 capability
// from verified upload and TLS MinIO. No fake downloader or signature verifier.
func TestRegisteredBundleExternalMinIO(t *testing.T) {
	fixturePath := os.Getenv("LOOM_REGISTERED_BUNDLE_FIXTURE")
	if fixturePath == "" {
		t.Skip("external TLS MinIO fixture not configured")
	}
	payload, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatal("external native fixture unavailable")
	}
	var fixture struct {
		Plan                           RegisteredBundlePlan
		Session                        BundleSessionBinding
		Origin, CAFile, CapabilityFile string
	}
	if decodeStrictJSON(payload, &fixture) != nil {
		t.Fatal("external native fixture invalid")
	}
	ca, err := os.ReadFile(fixture.CAFile)
	if err != nil {
		t.Fatal("external native CA unavailable")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		t.Fatal("external native CA invalid")
	}
	payload, err = os.ReadFile(fixture.CapabilityFile)
	if err != nil {
		t.Fatal("external native capability unavailable")
	}
	secretFD := createMemfdFixture(t, "registered-minio-capability", payload, requiredMemfdSeals, true)
	secret, err := NewSecretBuffer(secretFD, maxRegisteredCapabilityBytes)
	if err != nil {
		t.Fatal(err)
	}
	defer secret.Close()
	workspaceFD := openDirectoryFD(t, t.TempDir())
	defer syscall.Close(workspaceFD)
	bundle, err := DownloadRegisteredBundle(context.Background(), secret, workspaceFD, fixture.Plan, fixture.Session,
		BundleDownloadTrust{Origin: fixture.Origin, Bucket: fixture.Plan.Bucket, Roots: roots}, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	defer bundle.Close()
	contextFD, err := bundle.DupDirectoryFD()
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.Close(contextFD)
	var capability registeredBundleWire
	if decodeStrictJSON(payload, &capability) != nil {
		t.Fatal("external native capability invalid")
	}
	for _, object := range capability.Objects {
		got, err := HashFileAt(contextFD, object.RelativePath)
		mode := uint32(0o644)
		if object.Mode == "0755" {
			mode = 0o755
		}
		if err != nil || got.SHA256 != object.SHA256 || got.SizeBytes != object.SizeBytes || got.Mode != mode {
			t.Fatal("external native descriptor acceptance changed")
		}
	}
	if err := bundle.Close(); err != nil {
		t.Fatal(err)
	}
}
