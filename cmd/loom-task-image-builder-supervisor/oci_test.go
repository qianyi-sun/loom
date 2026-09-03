package main

import (
	"archive/tar"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestOCIValidateAcceptsMinimalLayoutAndReturnsImmutableDigestAndTarSHA(t *testing.T) {
	tarPath, expected := writeOCILayoutTar(t, "amd64", nil)

	output, err := ValidateOCIOutput(tarPath, "linux/amd64")
	if err != nil {
		t.Fatalf("ValidateOCIOutput() error = %v", err)
	}
	if output.TopLevelDigest != "sha256:"+expected.manifestDigest {
		t.Fatalf("TopLevelDigest = %q, want manifest digest", output.TopLevelDigest)
	}
	if output.FileSHA256 != expected.tarDigest {
		t.Fatalf("FileSHA256 = %q, want %q", output.FileSHA256, expected.tarDigest)
	}
	if output.OS != "linux" || output.Architecture != "amd64" {
		t.Fatalf("platform = %s/%s, want linux/amd64", output.OS, output.Architecture)
	}
}

func TestOCIValidateRejectsUnsafeTarEntriesAndDescriptorMutations(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*ociLayoutFixture)
	}{
		{name: "duplicate path", mutate: func(f *ociLayoutFixture) {
			f.extraEntries = append(f.extraEntries, tarEntry{name: "index.json", body: []byte("{}")})
		}},
		{name: "path escape", mutate: func(f *ociLayoutFixture) {
			f.extraEntries = append(f.extraEntries, tarEntry{name: "../index.json", body: []byte("{}")})
		}},
		{name: "symlink", mutate: func(f *ociLayoutFixture) {
			f.extraEntries = append(f.extraEntries, tarEntry{name: "link", entryType: tar.TypeSymlink, linkname: "index.json"})
		}},
		{name: "device", mutate: func(f *ociLayoutFixture) {
			f.extraEntries = append(f.extraEntries, tarEntry{name: "dev/null", entryType: tar.TypeChar})
		}},
		{name: "manifest digest", mutate: func(f *ociLayoutFixture) { f.index.Manifests[0].Digest = "sha256:" + strings.Repeat("d", 64) }},
		{name: "config size", mutate: func(f *ociLayoutFixture) { f.manifest.Config.Size++ }},
		{name: "extra manifest", mutate: func(f *ociLayoutFixture) { f.index.Manifests = append(f.index.Manifests, f.index.Manifests[0]) }},
		{name: "wrong architecture", mutate: func(f *ociLayoutFixture) { f.config.Architecture = "arm64" }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tarPath, _ := writeOCILayoutTar(t, "amd64", tt.mutate)
			if _, err := ValidateOCIOutput(tarPath, "linux/amd64"); err == nil {
				t.Fatal("ValidateOCIOutput() succeeded, want rejection")
			}
		})
	}
}

type ociExpected struct {
	manifestDigest string
	tarDigest      string
}

type tarEntry struct {
	name      string
	body      []byte
	entryType byte
	linkname  string
}

type ociLayoutFixture struct {
	layout       map[string]string
	index        ociIndex
	manifest     ociManifest
	config       ociConfig
	extraEntries []tarEntry
}

func writeOCILayoutTar(t *testing.T, arch string, mutate func(*ociLayoutFixture)) (string, ociExpected) {
	t.Helper()
	fixture := ociLayoutFixture{
		layout: map[string]string{"imageLayoutVersion": "1.0.0"},
		config: ociConfig{
			Architecture: arch,
			OS:           "linux",
			RootFS:       map[string]any{"type": "layers", "diff_ids": []any{}},
			Config:       map[string]any{},
		},
	}
	configBytes := mustJSON(t, fixture.config)
	configDigest := sha256Hex(configBytes)
	fixture.manifest = ociManifest{
		SchemaVersion: 2,
		MediaType:     "application/vnd.oci.image.manifest.v1+json",
		Config: ociDescriptor{
			MediaType: "application/vnd.oci.image.config.v1+json",
			Digest:    "sha256:" + configDigest,
			Size:      int64(len(configBytes)),
		},
		Layers: []ociDescriptor{},
	}
	manifestBytes := mustJSON(t, fixture.manifest)
	manifestDigest := sha256Hex(manifestBytes)
	fixture.index = ociIndex{
		SchemaVersion: 2,
		Manifests: []ociDescriptor{
			{
				MediaType: "application/vnd.oci.image.manifest.v1+json",
				Digest:    "sha256:" + manifestDigest,
				Size:      int64(len(manifestBytes)),
				Platform:  &ociPlatform{OS: "linux", Architecture: arch},
			},
		},
	}
	if mutate != nil {
		mutate(&fixture)
	}
	configBytes = mustJSON(t, fixture.config)
	configDigest = sha256Hex(configBytes)
	if !strings.Contains(fixture.manifest.Config.Digest, strings.Repeat("d", 64)) {
		fixture.manifest.Config.Digest = "sha256:" + configDigest
	}
	if fixture.manifest.Config.Size == int64(len(mustJSON(t, ociConfig{Architecture: arch, OS: "linux", RootFS: map[string]any{"type": "layers", "diff_ids": []any{}}, Config: map[string]any{}}))) {
		fixture.manifest.Config.Size = int64(len(configBytes))
	}
	manifestBytes = mustJSON(t, fixture.manifest)
	manifestDigest = sha256Hex(manifestBytes)
	if len(fixture.index.Manifests) > 0 && !strings.Contains(fixture.index.Manifests[0].Digest, strings.Repeat("d", 64)) {
		fixture.index.Manifests[0].Digest = "sha256:" + manifestDigest
		fixture.index.Manifests[0].Size = int64(len(manifestBytes))
	}
	indexBytes := mustJSON(t, fixture.index)
	layoutBytes := mustJSON(t, fixture.layout)

	tarPath := filepath.Join(t.TempDir(), "image.tar")
	var tarBuffer bytes.Buffer
	writer := tar.NewWriter(&tarBuffer)
	addTarFile(t, writer, "oci-layout", layoutBytes, tar.TypeReg, "")
	addTarFile(t, writer, "index.json", indexBytes, tar.TypeReg, "")
	addTarFile(t, writer, "blobs/sha256/"+manifestDigest, manifestBytes, tar.TypeReg, "")
	addTarFile(t, writer, "blobs/sha256/"+configDigest, configBytes, tar.TypeReg, "")
	for _, entry := range fixture.extraEntries {
		addTarFile(t, writer, entry.name, entry.body, entry.entryType, entry.linkname)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("tar.Close() error = %v", err)
	}
	if err := os.WriteFile(tarPath, tarBuffer.Bytes(), 0o600); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", tarPath, err)
	}
	tarDigest := sha256Hex(tarBuffer.Bytes())
	return tarPath, ociExpected{manifestDigest: manifestDigest, tarDigest: tarDigest}
}

func addTarFile(t *testing.T, writer *tar.Writer, name string, body []byte, entryType byte, linkname string) {
	t.Helper()
	if entryType == 0 {
		entryType = tar.TypeReg
	}
	header := &tar.Header{
		Name:     name,
		Mode:     0o444,
		Size:     int64(len(body)),
		Typeflag: entryType,
		Linkname: linkname,
	}
	if entryType != tar.TypeReg {
		header.Size = 0
	}
	if err := writer.WriteHeader(header); err != nil {
		t.Fatalf("WriteHeader(%q) error = %v", name, err)
	}
	if entryType == tar.TypeReg {
		if _, err := io.Copy(writer, bytes.NewReader(body)); err != nil {
			t.Fatalf("tar write(%q) error = %v", name, err)
		}
	}
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	payload, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	return payload
}

func sha256Hex(payload []byte) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}
