package main

import (
	"encoding/json"
	"os"
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
		"server_name": "registry.example", "issuer": "loom-authority", "key_id": "registry-1",
		"ca": map[string]any{"path": ca, "sha256": sha256FileHex(t, ca)},
	}
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
		RegistryService: "loom-registry", RegistryIssuer: "loom-authority", RegistryKeyID: "registry-1"}
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
