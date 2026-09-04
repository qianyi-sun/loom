package main

import (
	"archive/tar"
	"bytes"
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
	Path           string
	TopLevelDigest string
	FileSHA256     string
	SizeBytes      int64
	OS             string
	Architecture   string
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

func ValidateOCIOutput(path string, platform string) (OCIOutput, error) {
	expectedOS, expectedArch, err := parsePlatform(platform)
	if err != nil {
		return OCIOutput{}, err
	}
	file, err := os.Open(path)
	if err != nil {
		return OCIOutput{}, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return OCIOutput{}, err
	}
	hash := sha256.New()
	reader := tar.NewReader(io.TeeReader(file, hash))
	entries := map[string][]byte{}
	for {
		header, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return OCIOutput{}, err
		}
		canonicalName, err := validateTarEntry(header)
		if err != nil {
			return OCIOutput{}, err
		}
		if header.Typeflag == tar.TypeDir {
			continue
		}
		if _, ok := entries[canonicalName]; ok {
			return OCIOutput{}, errors.New("OCI tar contains duplicate path")
		}
		payload, err := io.ReadAll(reader)
		if err != nil {
			return OCIOutput{}, err
		}
		if int64(len(payload)) != header.Size {
			return OCIOutput{}, errors.New("OCI tar entry size mismatch")
		}
		entries[canonicalName] = payload
	}
	if _, err := io.Copy(hash, file); err != nil {
		return OCIOutput{}, err
	}
	indexPayload, ok := entries["index.json"]
	if !ok {
		return OCIOutput{}, errors.New("OCI index missing")
	}
	if _, ok := entries["oci-layout"]; !ok {
		return OCIOutput{}, errors.New("OCI layout missing")
	}
	var index ociIndex
	if err := decodeStrictJSON(indexPayload, &index); err != nil {
		return OCIOutput{}, err
	}
	if index.SchemaVersion != 2 || index.MediaType != "" && index.MediaType != "application/vnd.oci.image.index.v1+json" || len(index.Manifests) != 1 {
		return OCIOutput{}, errors.New("OCI index must contain exactly one manifest")
	}
	manifestDescriptor := index.Manifests[0]
	if manifestDescriptor.Platform == nil || manifestDescriptor.Platform.OS != expectedOS || manifestDescriptor.Platform.Architecture != expectedArch {
		return OCIOutput{}, errors.New("OCI manifest platform mismatch")
	}
	manifestPayload, err := descriptorPayload(entries, manifestDescriptor)
	if err != nil {
		return OCIOutput{}, err
	}
	var manifest ociManifest
	if err := decodeStrictJSON(manifestPayload, &manifest); err != nil {
		return OCIOutput{}, err
	}
	if manifest.SchemaVersion != 2 || manifest.MediaType != "application/vnd.oci.image.manifest.v1+json" {
		return OCIOutput{}, errors.New("OCI manifest invalid")
	}
	configPayload, err := descriptorPayload(entries, manifest.Config)
	if err != nil {
		return OCIOutput{}, err
	}
	for _, layer := range manifest.Layers {
		if _, err := descriptorPayload(entries, layer); err != nil {
			return OCIOutput{}, err
		}
	}
	var config ociConfig
	if err := decodeStrictJSON(configPayload, &config); err != nil {
		return OCIOutput{}, err
	}
	if config.OS != expectedOS || config.Architecture != expectedArch {
		return OCIOutput{}, errors.New("OCI config platform mismatch")
	}
	return OCIOutput{
		Path:           path,
		TopLevelDigest: manifestDescriptor.Digest,
		FileSHA256:     hex.EncodeToString(hash.Sum(nil)),
		SizeBytes:      info.Size(),
		OS:             config.OS,
		Architecture:   config.Architecture,
	}, nil
}

func descriptorPayload(entries map[string][]byte, descriptor ociDescriptor) ([]byte, error) {
	digest, err := parseSHA256Descriptor(descriptor.Digest)
	if err != nil {
		return nil, err
	}
	if descriptor.Size < 0 {
		return nil, errors.New("OCI descriptor size invalid")
	}
	payload, ok := entries["blobs/sha256/"+digest]
	if !ok {
		return nil, errors.New("OCI descriptor blob missing")
	}
	if int64(len(payload)) != descriptor.Size {
		return nil, errors.New("OCI descriptor size mismatch")
	}
	sum := sha256.Sum256(payload)
	if hex.EncodeToString(sum[:]) != digest {
		return nil, errors.New("OCI descriptor digest mismatch")
	}
	return payload, nil
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
