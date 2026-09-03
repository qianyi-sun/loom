package main

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

func TestDownloadBundleMaterializesVerifiedFilesWithExactTLSAndNoProxy(t *testing.T) {
	files := map[string][]byte{
		"/bundle/context/Dockerfile": []byte("FROM scratch\nCOPY app /app\n"),
		"/bundle/context/app":        []byte("payload\n"),
	}
	server := bundleTLSServer(t, files, nil)
	defer server.Close()
	t.Setenv("HTTPS_PROXY", "https://127.0.0.1:1")
	workspace := t.TempDir()
	workspaceFD := openDirectoryFD(t, workspace)
	defer syscall.Close(workspaceFD)
	capability := bundleSecret(t, bundleCapability(t, server, []TaskImageBundleFileV1{
		bundleFile("context/Dockerfile", "/bundle/context/Dockerfile", files["/bundle/context/Dockerfile"], 0o444),
		bundleFile("context/app", "/bundle/context/app", files["/bundle/context/app"], 0o400),
	}, int64(len(files["/bundle/context/Dockerfile"])+len(files["/bundle/context/app"]))))
	defer capability.Close()

	result, err := DownloadBundle(context.Background(), capability, workspaceFD)
	if err != nil {
		t.Fatalf("DownloadBundle() error = %v", err)
	}
	if result.TotalBytes != int64(len(files["/bundle/context/Dockerfile"])+len(files["/bundle/context/app"])) {
		t.Fatalf("TotalBytes = %d", result.TotalBytes)
	}
	assertFilePayloadAndMode(t, filepath.Join(workspace, "context/Dockerfile"), files["/bundle/context/Dockerfile"], 0o444)
	assertFilePayloadAndMode(t, filepath.Join(workspace, "context/app"), files["/bundle/context/app"], 0o400)
}

func TestDownloadBundleRejectsRedirectTLSDowngradeTraversalSymlinkAndMetadataDrift(t *testing.T) {
	payload := []byte("FROM scratch\n")
	successFiles := []TaskImageBundleFileV1{
		bundleFile("Dockerfile", "/bundle/Dockerfile", payload, 0o444),
	}

	tests := []struct {
		name       string
		server     func(*testing.T) *httptest.Server
		files      []TaskImageBundleFileV1
		maxBytes   int64
		mutateCap  func(*TaskImageBundleCapabilityV1)
		beforeCall func(*testing.T, string)
		assert     func(*testing.T, string)
	}{
		{
			name: "redirect",
			server: func(t *testing.T) *httptest.Server {
				return bundleTLSServer(t, nil, func(w http.ResponseWriter, r *http.Request) {
					http.Redirect(w, r, "/elsewhere", http.StatusFound)
				})
			},
			files:    successFiles,
			maxBytes: int64(len(payload)),
		},
		{
			name: "tls downgrade",
			server: func(t *testing.T) *httptest.Server {
				return bundleTLSDowngradeServer(t, map[string][]byte{"/bundle/Dockerfile": payload})
			},
			files:    successFiles,
			maxBytes: int64(len(payload)),
		},
		{
			name: "traversal",
			server: func(t *testing.T) *httptest.Server {
				return bundleTLSServer(t, map[string][]byte{"/bundle/Dockerfile": payload}, nil)
			},
			files: []TaskImageBundleFileV1{
				bundleFile("../escape", "/bundle/Dockerfile", payload, 0o444),
			},
			maxBytes: int64(len(payload)),
			assert: func(t *testing.T, workspace string) {
				if _, err := os.Stat(filepath.Join(filepath.Dir(workspace), "escape")); !os.IsNotExist(err) {
					t.Fatalf("escape file exists or stat error = %v", err)
				}
			},
		},
		{
			name: "symlink",
			server: func(t *testing.T) *httptest.Server {
				return bundleTLSServer(t, map[string][]byte{"/bundle/Dockerfile": payload}, nil)
			},
			files: []TaskImageBundleFileV1{
				bundleFile("Dockerfile", "/bundle/Dockerfile", payload, 0o444),
			},
			maxBytes: int64(len(payload)),
			beforeCall: func(t *testing.T, workspace string) {
				outside := filepath.Join(filepath.Dir(workspace), "outside")
				if err := os.WriteFile(outside, []byte("outside\n"), 0o600); err != nil {
					t.Fatalf("WriteFile(%q) error = %v", outside, err)
				}
				if err := os.Symlink(outside, filepath.Join(workspace, "Dockerfile")); err != nil {
					t.Fatalf("Symlink() error = %v", err)
				}
			},
			assert: func(t *testing.T, workspace string) {
				outside := filepath.Join(filepath.Dir(workspace), "outside")
				assertFilePayloadAndMode(t, outside, []byte("outside\n"), 0o600)
			},
		},
		{
			name: "metadata digest",
			server: func(t *testing.T) *httptest.Server {
				return bundleTLSServer(t, map[string][]byte{"/bundle/Dockerfile": payload}, nil)
			},
			files:    successFiles,
			maxBytes: int64(len(payload)),
			mutateCap: func(capability *TaskImageBundleCapabilityV1) {
				capability.MetadataSHA256 = strings.Repeat("e", 64)
			},
		},
		{
			name: "file count quota",
			server: func(t *testing.T) *httptest.Server {
				return bundleTLSServer(t, map[string][]byte{"/bundle/Dockerfile": payload}, nil)
			},
			files:    successFiles,
			maxBytes: int64(len(payload)),
			mutateCap: func(capability *TaskImageBundleCapabilityV1) {
				capability.MaxFiles = 0
			},
		},
		{
			name: "aggregate byte quota",
			server: func(t *testing.T) *httptest.Server {
				return bundleTLSServer(t, map[string][]byte{"/bundle/Dockerfile": payload}, nil)
			},
			files:    successFiles,
			maxBytes: int64(len(payload)) - 1,
		},
		{
			name: "content digest",
			server: func(t *testing.T) *httptest.Server {
				return bundleTLSServer(t, map[string][]byte{"/bundle/Dockerfile": []byte("changed\n")}, nil)
			},
			files:    successFiles,
			maxBytes: int64(len(payload)),
		},
		{
			name: "partial download",
			server: func(t *testing.T) *httptest.Server {
				return bundleTLSServer(t, nil, func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Length", fmt.Sprint(len(payload)+10))
					_, _ = w.Write(payload[:4])
				})
			},
			files:    successFiles,
			maxBytes: int64(len(payload) + 10),
			mutateCap: func(capability *TaskImageBundleCapabilityV1) {
				capability.Files[0].SizeBytes = int64(len(payload) + 10)
				capability.Files[0].SHA256 = strings.Repeat("a", 64)
				capability.MetadataSHA256 = testBundleMetadataDigest(t, capability.Files)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := tt.server(t)
			defer server.Close()
			workspace := t.TempDir()
			if tt.beforeCall != nil {
				tt.beforeCall(t, workspace)
			}
			workspaceFD := openDirectoryFD(t, workspace)
			defer syscall.Close(workspaceFD)
			capabilityValue := bundleCapability(t, server, tt.files, tt.maxBytes)
			if tt.mutateCap != nil {
				tt.mutateCap(&capabilityValue)
			}
			capability := bundleSecret(t, capabilityValue)
			defer capability.Close()

			_, err := DownloadBundle(context.Background(), capability, workspaceFD)
			if err == nil {
				t.Fatal("DownloadBundle() succeeded, want error")
			}
			if tt.name != "symlink" {
				if _, statErr := os.Stat(filepath.Join(workspace, "Dockerfile")); statErr == nil {
					t.Fatal("partial Dockerfile remains after failed download")
				} else if !os.IsNotExist(statErr) {
					t.Fatalf("Stat(Dockerfile) error = %v", statErr)
				}
			}
			if tt.assert != nil {
				tt.assert(t, workspace)
			}
		})
	}
}

func bundleTLSServer(t *testing.T, payloads map[string][]byte, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	if handler == nil {
		handler = func(w http.ResponseWriter, r *http.Request) {
			payload, ok := payloads[r.URL.Path]
			if !ok {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Content-Length", fmt.Sprint(len(payload)))
			_, _ = w.Write(payload)
		}
	}
	server := httptest.NewUnstartedServer(http.HandlerFunc(handler))
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS13}
	server.StartTLS()
	return server
}

func bundleTLSDowngradeServer(t *testing.T, payloads map[string][]byte) *httptest.Server {
	t.Helper()
	handler := func(w http.ResponseWriter, r *http.Request) {
		payload, ok := payloads[r.URL.Path]
		if !ok {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Length", fmt.Sprint(len(payload)))
		_, _ = w.Write(payload)
	}
	server := httptest.NewUnstartedServer(http.HandlerFunc(handler))
	server.TLS = &tls.Config{MaxVersion: tls.VersionTLS12}
	server.StartTLS()
	return server
}

func bundleCapability(t *testing.T, server *httptest.Server, files []TaskImageBundleFileV1, maxBytes int64) TaskImageBundleCapabilityV1 {
	t.Helper()
	capability := TaskImageBundleCapabilityV1{
		Schema:         taskImageBundleCapabilitySchema,
		BaseURL:        server.URL,
		ServerName:     "example.com",
		CAPEM:          string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})),
		MaxFiles:       len(files),
		MaxBytes:       maxBytes,
		Files:          files,
		MetadataSHA256: testBundleMetadataDigest(t, files),
	}
	return capability
}

func bundleSecret(t *testing.T, capability TaskImageBundleCapabilityV1) *SecretBuffer {
	t.Helper()
	payload, err := json.Marshal(capability)
	if err != nil {
		t.Fatalf("Marshal(capability) error = %v", err)
	}
	fd := createMemfdFixture(t, "bundle-capability", payload, requiredMemfdSeals, true)
	buffer, err := NewSecretBuffer(fd, maxSecretBytes)
	if err != nil {
		t.Fatalf("NewSecretBuffer() error = %v", err)
	}
	return buffer
}

func bundleFile(path string, urlPath string, payload []byte, mode uint32) TaskImageBundleFileV1 {
	sum := sha256.Sum256(payload)
	return TaskImageBundleFileV1{
		Path:      path,
		URLPath:   urlPath,
		SizeBytes: int64(len(payload)),
		Mode:      mode,
		SHA256:    hex.EncodeToString(sum[:]),
	}
}

func assertFilePayloadAndMode(t *testing.T, path string, want []byte, mode os.FileMode) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%q) error = %v", path, err)
	}
	if string(got) != string(want) {
		t.Fatalf("ReadFile(%q) = %q, want %q", path, got, want)
	}
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatalf("Lstat(%q) error = %v", path, err)
	}
	if info.Mode().Perm() != mode {
		t.Fatalf("mode(%q) = %#o, want %#o", path, info.Mode().Perm(), mode)
	}
}
