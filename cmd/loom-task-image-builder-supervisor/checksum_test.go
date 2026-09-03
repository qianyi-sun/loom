package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

func TestChecksumMetadataBindsPathModeSizeAndContentDigest(t *testing.T) {
	file := TaskImageBundleFileV1{
		Path:      "context/Dockerfile",
		URLPath:   "/bundle/context/Dockerfile",
		SizeBytes: 19,
		Mode:      0o444,
		SHA256:    strings.Repeat("a", 64),
	}
	want := testBundleMetadataDigest(t, []TaskImageBundleFileV1{file})

	got, err := BundleMetadataSHA256([]TaskImageBundleFileV1{file})
	if err != nil {
		t.Fatalf("BundleMetadataSHA256() error = %v", err)
	}
	if got != want {
		t.Fatalf("metadata digest = %q, want %q", got, want)
	}

	mutations := []struct {
		name string
		file TaskImageBundleFileV1
	}{
		{name: "path", file: TaskImageBundleFileV1{Path: "Dockerfile", URLPath: file.URLPath, SizeBytes: file.SizeBytes, Mode: file.Mode, SHA256: file.SHA256}},
		{name: "mode", file: TaskImageBundleFileV1{Path: file.Path, URLPath: file.URLPath, SizeBytes: file.SizeBytes, Mode: 0o400, SHA256: file.SHA256}},
		{name: "size", file: TaskImageBundleFileV1{Path: file.Path, URLPath: file.URLPath, SizeBytes: file.SizeBytes + 1, Mode: file.Mode, SHA256: file.SHA256}},
		{name: "content digest", file: TaskImageBundleFileV1{Path: file.Path, URLPath: file.URLPath, SizeBytes: file.SizeBytes, Mode: file.Mode, SHA256: strings.Repeat("b", 64)}},
	}
	for _, tt := range mutations {
		t.Run(tt.name, func(t *testing.T) {
			digest, err := BundleMetadataSHA256([]TaskImageBundleFileV1{tt.file})
			if err != nil {
				t.Fatalf("BundleMetadataSHA256() error = %v", err)
			}
			if digest == want {
				t.Fatalf("metadata digest unchanged for %s mutation", tt.name)
			}
		})
	}
}

func TestChecksumFileAtRejectsSymlinkTraversalAndMetadataDrift(t *testing.T) {
	root := t.TempDir()
	payload := []byte("FROM scratch\n")
	path := filepath.Join(root, "Dockerfile")
	if err := os.WriteFile(path, payload, 0o444); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", path, err)
	}
	rootFD := openDirectoryFD(t, root)
	defer syscall.Close(rootFD)

	got, err := HashFileAt(rootFD, "Dockerfile")
	if err != nil {
		t.Fatalf("HashFileAt() error = %v", err)
	}
	sum := sha256.Sum256(payload)
	if got.SHA256 != hex.EncodeToString(sum[:]) || got.SizeBytes != int64(len(payload)) || got.Mode != 0o444 {
		t.Fatalf("HashFileAt() = %#v, want digest/size/mode for Dockerfile", got)
	}

	if _, err := HashFileAt(rootFD, "../escape"); err == nil {
		t.Fatal("HashFileAt() accepted path traversal")
	}
	link := filepath.Join(root, "link")
	if err := os.Symlink(path, link); err != nil {
		t.Fatalf("Symlink(%q, %q) error = %v", path, link, err)
	}
	if _, err := HashFileAt(rootFD, "link"); err == nil {
		t.Fatal("HashFileAt() accepted symlink")
	}

	spec := TaskImageBundleFileV1{
		Path:      "Dockerfile",
		URLPath:   "/bundle/Dockerfile",
		SizeBytes: int64(len(payload)),
		Mode:      0o444,
		SHA256:    hex.EncodeToString(sum[:]),
	}
	if err := VerifyBundleFileAt(rootFD, spec); err != nil {
		t.Fatalf("VerifyBundleFileAt() error = %v", err)
	}
	spec.Mode = 0o400
	if err := VerifyBundleFileAt(rootFD, spec); err == nil {
		t.Fatal("VerifyBundleFileAt() accepted mode drift")
	}
}

func testBundleMetadataDigest(t *testing.T, files []TaskImageBundleFileV1) string {
	t.Helper()
	values := make([]map[string]any, 0, len(files))
	for _, file := range files {
		values = append(values, map[string]any{
			"path":       file.Path,
			"url_path":   file.URLPath,
			"size_bytes": file.SizeBytes,
			"mode":       file.Mode,
			"sha256":     file.SHA256,
		})
	}
	payload, err := json.Marshal(values)
	if err != nil {
		t.Fatalf("Marshal(metadata) error = %v", err)
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}
