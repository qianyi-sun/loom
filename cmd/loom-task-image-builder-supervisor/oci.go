package main

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
)

type OCIOutput struct {
	Path              string
	TopLevelDigest    string
	ManifestSize      int64
	ManifestMediaType string
	FileSHA256        string
	SizeBytes         int64
	OS                string
	Architecture      string
}

type ociIndex struct {
	SchemaVersion int               `json:"schemaVersion"`
	MediaType     string            `json:"mediaType,omitempty"`
	Manifests     []ociDescriptor   `json:"manifests"`
	Annotations   map[string]string `json:"annotations,omitempty"`
}

type ociManifest struct {
	SchemaVersion int               `json:"schemaVersion"`
	MediaType     string            `json:"mediaType"`
	Config        ociDescriptor     `json:"config"`
	Layers        []ociDescriptor   `json:"layers"`
	Annotations   map[string]string `json:"annotations,omitempty"`
}

type ociConfig struct {
	Created      string           `json:"created,omitempty"`
	Author       string           `json:"author,omitempty"`
	Architecture string           `json:"architecture"`
	OS           string           `json:"os"`
	OSVersion    string           `json:"os.version,omitempty"`
	OSFeatures   []string         `json:"os.features,omitempty"`
	Variant      string           `json:"variant,omitempty"`
	RootFS       map[string]any   `json:"rootfs"`
	Config       map[string]any   `json:"config"`
	History      []map[string]any `json:"history,omitempty"`
}

type ociDescriptor struct {
	MediaType   string            `json:"mediaType"`
	Digest      string            `json:"digest"`
	Size        int64             `json:"size"`
	Platform    *ociPlatform      `json:"platform,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty"`
}

type ociPlatform struct {
	OS           string `json:"os"`
	Architecture string `json:"architecture"`
}

const (
	maxOCIJSONBytes   int64 = 4 << 20
	maxOCIDescriptors       = 256
	maxOCILayers            = 128
	// Existing rootless provider / guard project quota (100 GiB).
	maxOCITarBytes       int64 = 107374182400
	ociManifestMediaType       = "application/vnd.oci.image.manifest.v1+json"
)

type ociEntry struct {
	offset, size int64
	digest       string
}
type scannedOCI struct {
	entries  map[string]ociEntry
	manifest ociManifest
}
type ociCountingReader struct {
	ctx context.Context
	r   io.Reader
	n   int64
}

func (r *ociCountingReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	n, err := r.r.Read(p)
	r.n += int64(n)
	return n, err
}

func ValidateOCIOutput(name string, platform string) (OCIOutput, error) {
	file, err := os.Open(name)
	if err != nil {
		return OCIOutput{}, errors.New("OCI file unavailable")
	}
	defer file.Close()
	output, _, err := scanOCIFile(context.Background(), file, name, platform)
	return output, err
}

// Only offsets, sizes and hashes survive the tar pass. JSON is read exactly from
// bounded sections of the same open file; potentially large blobs are hashed.
func scanOCIFile(ctx context.Context, file *os.File, name, platform string) (OCIOutput, scannedOCI, error) {
	fail := func(message string) (OCIOutput, scannedOCI, error) {
		return OCIOutput{}, scannedOCI{}, errors.New(message)
	}
	expectedOS, expectedArch, err := parsePlatform(platform)
	if err != nil {
		return fail("OCI platform invalid")
	}
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > maxOCITarBytes {
		return fail("OCI tar size invalid")
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return fail("OCI file unavailable")
	}
	hash := sha256.New()
	counter := &ociCountingReader{ctx: ctx, r: io.TeeReader(io.LimitReader(file, maxOCITarBytes+1), hash)}
	reader := tar.NewReader(counter)
	entries := map[string]ociEntry{}
	seen := map[string]bool{}
	blobs := 0
	for {
		header, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fail("OCI tar invalid")
		}
		canonicalName, err := validateTarEntry(header)
		if err != nil {
			return fail("OCI tar entry invalid")
		}
		if header.Size < 0 || header.Size > maxOCITarBytes || header.Size > info.Size()-counter.n {
			return fail("OCI tar entry size invalid")
		}
		if seen[canonicalName] {
			return fail("OCI tar contains duplicate path")
		}
		seen[canonicalName] = true
		if len(seen) > maxOCIDescriptors+16 {
			return fail("OCI tar entry count exceeded")
		}
		if header.Typeflag == tar.TypeDir {
			continue
		}
		if canonicalName == "index.json" || canonicalName == "oci-layout" {
			if header.Size > maxOCIJSONBytes {
				return fail("OCI JSON size exceeded")
			}
		} else {
			suffix, ok := strings.CutPrefix(canonicalName, "blobs/sha256/")
			if !ok || !isDigest(suffix) {
				return fail("OCI blob path invalid")
			}
			blobs++
			if blobs > maxOCIDescriptors {
				return fail("OCI descriptor count exceeded")
			}
		}
		entry := ociEntry{offset: counter.n, size: header.Size}
		h := sha256.New()
		n, err := io.Copy(h, reader)
		if err != nil || n != header.Size {
			return fail("OCI tar entry size mismatch")
		}
		entry.digest = hex.EncodeToString(h.Sum(nil))
		if strings.HasPrefix(canonicalName, "blobs/") && canonicalName != "blobs/sha256/"+entry.digest {
			return fail("OCI descriptor digest mismatch")
		}
		entries[canonicalName] = entry
	}
	if _, err := io.Copy(io.Discard, counter); err != nil || counter.n != info.Size() || counter.n > maxOCITarBytes {
		return fail("OCI tar size changed")
	}
	indexPayload, err := readOCIJSON(file, entries, "index.json")
	if err != nil {
		return fail("OCI index invalid")
	}
	layoutPayload, err := readOCIJSON(file, entries, "oci-layout")
	if err != nil {
		return fail("OCI layout missing")
	}
	var layout struct {
		Version string `json:"imageLayoutVersion"`
	}
	if decodeStrictJSON(layoutPayload, &layout) != nil || layout.Version != "1.0.0" {
		return fail("OCI layout invalid")
	}
	var index ociIndex
	if decodeStrictJSON(indexPayload, &index) != nil || index.SchemaVersion != 2 || index.MediaType != "" && index.MediaType != "application/vnd.oci.image.index.v1+json" || len(index.Manifests) != 1 {
		return fail("OCI index must contain exactly one manifest")
	}
	md := index.Manifests[0]
	if md.MediaType != ociManifestMediaType || md.Platform == nil || md.Platform.OS != expectedOS || md.Platform.Architecture != expectedArch {
		return fail("OCI manifest platform or type mismatch")
	}
	if _, err := descriptorEntry(entries, md); err != nil {
		return fail("OCI manifest descriptor invalid")
	}
	manifestPayload, err := readOCIJSON(file, entries, "blobs/sha256/"+strings.TrimPrefix(md.Digest, "sha256:"))
	if err != nil {
		return fail("OCI manifest JSON invalid")
	}
	var manifest ociManifest
	if decodeStrictJSON(manifestPayload, &manifest) != nil || manifest.SchemaVersion != 2 || manifest.MediaType != ociManifestMediaType || len(manifest.Layers) > maxOCILayers {
		return fail("OCI manifest invalid")
	}
	if manifest.Config.MediaType != "application/vnd.oci.image.config.v1+json" {
		return fail("OCI config type invalid")
	}
	if _, err := descriptorEntry(entries, manifest.Config); err != nil {
		return fail("OCI config descriptor invalid")
	}
	for _, layer := range manifest.Layers {
		switch layer.MediaType {
		case "application/vnd.oci.image.layer.v1.tar", "application/vnd.oci.image.layer.v1.tar+gzip", "application/vnd.oci.image.layer.v1.tar+zstd":
		default:
			return fail("OCI layer type invalid")
		}
		if _, err := descriptorEntry(entries, layer); err != nil {
			return fail("OCI layer descriptor invalid")
		}
	}
	configPayload, err := readOCIJSON(file, entries, "blobs/sha256/"+strings.TrimPrefix(manifest.Config.Digest, "sha256:"))
	if err != nil {
		return fail("OCI config JSON invalid")
	}
	var config ociConfig
	if decodeStrictJSON(configPayload, &config) != nil || config.OS != expectedOS || config.Architecture != expectedArch {
		return fail("OCI config platform mismatch")
	}
	return OCIOutput{Path: name, TopLevelDigest: md.Digest, ManifestSize: md.Size, ManifestMediaType: md.MediaType, FileSHA256: hex.EncodeToString(hash.Sum(nil)), SizeBytes: info.Size(), OS: config.OS, Architecture: config.Architecture}, scannedOCI{entries: entries, manifest: manifest}, nil
}

func descriptorEntry(entries map[string]ociEntry, d ociDescriptor) (ociEntry, error) {
	digest, err := parseSHA256Descriptor(d.Digest)
	if err != nil || d.Size < 0 || d.Size > maxOCITarBytes {
		return ociEntry{}, errors.New("OCI descriptor invalid")
	}
	e, ok := entries["blobs/sha256/"+digest]
	if !ok || e.size != d.Size || e.digest != digest {
		return ociEntry{}, errors.New("OCI descriptor blob mismatch")
	}
	return e, nil
}

func readOCIJSON(file *os.File, entries map[string]ociEntry, name string) ([]byte, error) {
	e, ok := entries[name]
	if !ok || e.size < 0 || e.size > maxOCIJSONBytes {
		return nil, errors.New("OCI JSON size invalid")
	}
	b := make([]byte, int(e.size))
	if _, err := io.ReadFull(io.NewSectionReader(file, e.offset, e.size), b); err != nil {
		return nil, errors.New("OCI JSON read failed")
	}
	sum := sha256.Sum256(b)
	if hex.EncodeToString(sum[:]) != e.digest {
		return nil, errors.New("OCI JSON changed")
	}
	return b, nil
}

func parseSHA256Descriptor(value string) (string, error) {
	digest, ok := strings.CutPrefix(value, "sha256:")
	if !ok || !isDigest(digest) {
		return "", errors.New("OCI descriptor digest invalid")
	}
	return digest, nil
}

func validateTarEntry(header *tar.Header) (string, error) {
	if header == nil {
		return "", errors.New("OCI tar header missing")
	}
	canonicalName, err := canonicalOCITarPath(header.Name, header.Typeflag == tar.TypeDir)
	if err != nil {
		return "", err
	}
	switch header.Typeflag {
	case tar.TypeReg, tar.TypeRegA, tar.TypeDir:
		return canonicalName, nil
	default:
		return "", fmt.Errorf("OCI tar entry type forbidden: %d", header.Typeflag)
	}
}

func canonicalOCITarPath(raw string, directory bool) (string, error) {
	if directory && (raw == "." || raw == "./") {
		return ".", nil
	}
	name := strings.TrimPrefix(raw, "./")
	if raw == "" || name == "" || path.IsAbs(name) || filepath.IsAbs(name) || name == ".." || strings.HasPrefix(name, "../") || strings.Contains(name, "/../") {
		return "", errors.New("OCI tar path invalid")
	}
	cleaned := path.Clean(name)
	if cleaned == "." {
		if directory {
			return cleaned, nil
		}
		return "", errors.New("OCI tar path invalid")
	}
	if !directory && cleaned != name {
		return "", errors.New("OCI tar path invalid")
	}
	if strings.HasPrefix(cleaned, "../") || cleaned == ".." {
		return "", errors.New("OCI tar path invalid")
	}
	return cleaned, nil
}

func parsePlatform(platform string) (string, string, error) {
	osName, arch, ok := strings.Cut(platform, "/")
	if !ok || osName != "linux" || (arch != "amd64" && arch != "arm64") {
		return "", "", errors.New("OCI platform invalid")
	}
	return osName, arch, nil
}

func canonicalJSON(value any) []byte {
	payload, _ := encodeCanonicalJSON(value)
	return bytes.TrimSpace(payload)
}
