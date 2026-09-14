package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"encoding/pem"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func configuredPublicationFixture(t *testing.T) (string, string, string) {
	t.Helper()
	path, release, ca, _ := configuredBundleFixture(t)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var config map[string]any
	if err := json.Unmarshal(raw, &config); err != nil {
		t.Fatal(err)
	}
	config["publication"] = map[string]any{
		"origin": "https://registry.example:5443", "service": "loom-registry",
		"server_name": "registry.example", "issuer": "loom-authority", "key_id": strings.Repeat("k", 43),
		"ca": map[string]any{"path": ca, "sha256": sha256FileHex(t, ca)},
	}
	delete(config, "bundle") // Publication must load and validate its own trust.
	raw, err = json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	rewriteBundleConfigFixture(t, path, raw)
	return path, release, ca
}

func TestProductionPublicationUsesExactReleasePinnedConfig(t *testing.T) {
	path, release, ca := configuredPublicationFixture(t)
	cfg, err := LoadConfig(path, release)
	if err != nil {
		t.Fatal(err)
	}
	supervisor := productionOrchestrator(testGrantID, cfg)
	handoff, ok := supervisor.Handoff.(*RegistryPublicationHandoff)
	if !ok {
		t.Fatalf("configured production handoff = %T, want registry lifecycle", supervisor.Handoff)
	}
	want := PublicationRegistryExpectation{RegistryOrigin: "https://registry.example:5443",
		RegistryService: "loom-registry", RegistryIssuer: "loom-authority", RegistryKeyID: strings.Repeat("k", 43)}
	if handoff.PublicationRegistryExpectation() != want {
		t.Fatal("production registry expectation changed")
	}
	// Subsequent CA path changes cannot replace the loaded TLS trust snapshot.
	rewriteBundleConfigFixture(t, ca, []byte("changed"))
	if handoff.PublicationRegistryExpectation() != want {
		t.Fatal("loaded publication trust changed")
	}
	if _, err := LoadConfig(path, release); err == nil {
		t.Fatal("accepted changed CA release member")
	}
}

func TestPublicationConfigRejectsMissingNullOrAmbiguousFields(t *testing.T) {
	for _, mutation := range []string{"null", "origin", "service", "server_name", "issuer", "key_id", "ca", "ca-path", "ca-sha256", "extra", "case", "nested-case", "http", "userinfo", "path", "empty-name", "issuer-invalid", "key-invalid"} {
		t.Run(mutation, func(t *testing.T) {
			path, release, _ := configuredPublicationFixture(t)
			if _, err := LoadConfig(path, release); err != nil {
				t.Fatalf("invalid baseline: %v", err)
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var config map[string]any
			if err := json.Unmarshal(raw, &config); err != nil {
				t.Fatal(err)
			}
			publication := config["publication"].(map[string]any)
			switch mutation {
			case "null":
				config["publication"] = nil
			case "ca-path":
				delete(publication["ca"].(map[string]any), "path")
			case "ca-sha256":
				delete(publication["ca"].(map[string]any), "sha256")
			case "extra":
				publication["token"] = "must-not-be-configured"
			case "case":
				delete(config, "publication")
				config["Publication"] = publication
			case "nested-case":
				publication["Origin"] = publication["origin"]
				delete(publication, "origin")
			case "http":
				publication["origin"] = "http://registry.example"
			case "userinfo":
				publication["origin"] = "https://user:secret@registry.example"
			case "path":
				publication["origin"] = "https://registry.example/v2/"
			case "empty-name":
				publication["server_name"] = ""
			case "issuer-invalid":
				publication["issuer"] = "arbitrary issuer\n"
			case "key-invalid":
				publication["key_id"] = strings.Repeat("k", 1024)
			default:
				delete(publication, mutation)
			}
			raw, err = json.Marshal(config)
			if err != nil {
				t.Fatal(err)
			}
			rewriteBundleConfigFixture(t, path, raw)
			if _, err := LoadConfig(path, release); err == nil {
				t.Fatal("accepted unsafe publication config")
			}
		})
	}
}

func TestPublicationConfigRejectsUntrustedCABeforeStartupEffects(t *testing.T) {
	for _, kind := range []string{"digest", "foreign-path", "writable", "writable-parent", "symlink", "malformed-prefix", "private-key", "leaf"} {
		t.Run(kind, func(t *testing.T) {
			path, release, ca := configuredPublicationFixture(t)
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var config map[string]any
			if err := json.Unmarshal(raw, &config); err != nil {
				t.Fatal(err)
			}
			member := config["publication"].(map[string]any)["ca"].(map[string]any)
			switch kind {
			case "digest":
				member["sha256"] = strings.Repeat("b", 64)
			case "foreign-path":
				member["path"] = filepath.Join(t.TempDir(), "ca.pem")
			case "writable":
				if err := os.Chmod(ca, 0o644); err != nil {
					t.Fatal(err)
				}
			case "writable-parent":
				if err := os.Chmod(filepath.Dir(ca), 0o755); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				if err := os.Chmod(filepath.Dir(ca), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.Rename(ca, ca+".original"); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(ca+".original", ca); err != nil {
					t.Fatal(err)
				}
				if err := os.Chmod(filepath.Dir(ca), 0o555); err != nil {
					t.Fatal(err)
				}
			default:
				valid, err := os.ReadFile(ca)
				if err != nil {
					t.Fatal(err)
				}
				bad := append([]byte("-----BEGIN CERTIFICATE-----\nmalformed\n"), valid...)
				if kind == "private-key" {
					bad = append(valid, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: []byte("not-a-key")})...)
				}
				if kind == "leaf" {
					_, cert := bundleTLSFixture(t)
					bad = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Certificate[0]})
				}
				rewriteBundleConfigFixture(t, ca, bad)
				member["sha256"] = sha256FileHex(t, ca)
			}
			raw, err = json.Marshal(config)
			if err != nil {
				t.Fatal(err)
			}
			rewriteBundleConfigFixture(t, path, raw)
			previousPath, previousIdentity := compiledConfigPath, supervisorReleaseIdentity
			previousFactory, previousApply := guardClientFactory, applyProcessEnvironment
			compiledConfigPath, supervisorReleaseIdentity = path, func() (string, error) { return release, nil }
			guardClientFactory = func(Config) TaskImageGuard { t.Fatal("guard reached with invalid publication trust"); return nil }
			applyProcessEnvironment = func([]string) error { t.Fatal("environment changed with invalid publication trust"); return nil }
			t.Cleanup(func() {
				compiledConfigPath, supervisorReleaseIdentity = previousPath, previousIdentity
				guardClientFactory, applyProcessEnvironment = previousFactory, previousApply
			})
			if err := run([]string{"--grant-id", testGrantID}, nil); err == nil {
				t.Fatal("untrusted publication configuration accepted")
			}
		})
	}
}

func TestProductionPublicationConfigDrivesTLSUploadWithOwnedTrust(t *testing.T) {
	output, registry := newUploadFixture(t, 128)
	caPEM, cert := bundleTLSFixture(t)
	server := httptest.NewUnstartedServer(registry)
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{cert}}
	server.StartTLS()
	defer server.Close()
	path, release, ca := configuredPublicationFixture(t)
	rewriteBundleConfigFixture(t, ca, caPEM)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var config map[string]any
	if err := json.Unmarshal(raw, &config); err != nil {
		t.Fatal(err)
	}
	publication := config["publication"].(map[string]any)
	publication["origin"], publication["service"], publication["server_name"] = server.URL, "test-registry", "127.0.0.1"
	publication["ca"].(map[string]any)["sha256"] = sha256FileHex(t, ca)
	raw, err = json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	rewriteBundleConfigFixture(t, path, raw)
	cfg, err := LoadConfig(path, release)
	if err != nil {
		t.Fatal(err)
	}
	handoff := productionOrchestrator(testGrantID, cfg).Handoff.(*RegistryPublicationHandoff)
	rewriteBundleConfigFixture(t, ca, bundleCAFixture(t))
	t.Setenv("SSL_CERT_FILE", ca)
	source := &uploadTestSource{t: t, origin: server.URL}
	defer source.checkClosed()
	manifest, err := handoff.uploader.Upload(context.Background(), output, source)
	if err != nil || manifest.Digest != output.TopLevelDigest {
		t.Fatalf("configured TLS upload failed: %v", err)
	}
	// A newly loaded unrelated root cannot be rescued by ambient server trust.
	publication["ca"].(map[string]any)["sha256"] = sha256FileHex(t, ca)
	raw, err = json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	rewriteBundleConfigFixture(t, path, raw)
	cfg, err = LoadConfig(path, release)
	if err != nil {
		t.Fatal(err)
	}
	rewriteBundleConfigFixture(t, ca, caPEM)
	handoff = productionOrchestrator(testGrantID, cfg).Handoff.(*RegistryPublicationHandoff)
	if _, err := handoff.uploader.Upload(context.Background(), output, source); err == nil {
		t.Fatal("unconfigured TLS root accepted")
	}
}
