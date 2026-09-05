package main

import (
	"archive/tar"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"
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

func TestOCIValidateAcceptsBuildKitDirectoryHeaders(t *testing.T) {
	tarPath, _ := writeOCILayoutTar(t, "amd64", func(f *ociLayoutFixture) {
		f.extraEntries = append(f.extraEntries,
			tarEntry{name: "./", entryType: tar.TypeDir},
			tarEntry{name: "blobs/", entryType: tar.TypeDir},
			tarEntry{name: "blobs/sha256/", entryType: tar.TypeDir},
		)
	})

	if _, err := ValidateOCIOutput(tarPath, "linux/amd64"); err != nil {
		t.Fatalf("ValidateOCIOutput() error = %v", err)
	}
}

func TestOCIValidateHashesCompleteTarFile(t *testing.T) {
	tarPath, _ := writeOCILayoutTar(t, "amd64", nil)
	file, err := os.OpenFile(tarPath, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatalf("OpenFile(%q) error = %v", tarPath, err)
	}
	if _, err := file.Write([]byte("trailing-audit-bytes")); err != nil {
		file.Close()
		t.Fatalf("append tar trailer error = %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("Close(%q) error = %v", tarPath, err)
	}
	completePayload, err := os.ReadFile(tarPath)
	if err != nil {
		t.Fatalf("ReadFile(%q) error = %v", tarPath, err)
	}

	output, err := ValidateOCIOutput(tarPath, "linux/amd64")
	if err != nil {
		t.Fatalf("ValidateOCIOutput() error = %v", err)
	}
	if output.FileSHA256 != sha256Hex(completePayload) {
		t.Fatalf("FileSHA256 = %q, want complete tar file digest", output.FileSHA256)
	}
	if output.SizeBytes != int64(len(completePayload)) {
		t.Fatalf("SizeBytes = %d, want complete tar file size %d", output.SizeBytes, len(completePayload))
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

func TestOCIFixRetainsManifestSize(t *testing.T) {
	var size int64
	p, _ := writeOCILayoutTar(t, "amd64", func(f *ociLayoutFixture) { size = f.index.Manifests[0].Size })
	output, err := ValidateOCIOutput(p, "linux/amd64")
	if err != nil {
		t.Fatal(err)
	}
	field := reflect.ValueOf(output).FieldByName("ManifestSize")
	if !field.IsValid() {
		t.Fatal("validated manifest size missing from OCIOutput")
	}
	if field.Int() != size || size <= 0 || size == output.SizeBytes {
		t.Fatalf("manifest size=%d expected=%d archive=%d", field.Int(), size, output.SizeBytes)
	}
}

func TestOCIBounds(t *testing.T) {
	layer := []byte("layer payload")
	d := ociDescriptor{MediaType: "application/vnd.oci.image.layer.v1.tar", Digest: "sha256:" + sha256Hex(layer), Size: int64(len(layer))}
	for _, tc := range []struct {
		name   string
		mutate func(*ociLayoutFixture)
	}{
		{"oversized index", func(f *ociLayoutFixture) {
			f.index.Annotations = map[string]string{"padding": strings.Repeat("x", 4<<20)}
		}},
		{"oversized manifest", func(f *ociLayoutFixture) {
			f.manifest.Annotations = map[string]string{"padding": strings.Repeat("x", 4<<20)}
		}},
		{"257 descriptors", func(f *ociLayoutFixture) {
			for i := 0; i < 257; i++ {
				b := []byte{byte(i), byte(i >> 8)}
				f.extraEntries = append(f.extraEntries, tarEntry{name: "blobs/sha256/" + sha256Hex(b), body: b})
			}
		}},
		{"129 layers", func(f *ociLayoutFixture) {
			for i := 0; i < 129; i++ {
				f.manifest.Layers = append(f.manifest.Layers, d)
			}
			f.extraEntries = append(f.extraEntries, tarEntry{name: "blobs/sha256/" + sha256Hex(layer), body: layer})
		}},
		{"negative size", func(f *ociLayoutFixture) { f.manifest.Config.Size = -1 }},
		{"huge size", func(f *ociLayoutFixture) { f.manifest.Config.Size = 1<<63 - 1 }},
		{"duplicate blob", func(f *ociLayoutFixture) {
			f.extraEntries = append(f.extraEntries, tarEntry{name: "blobs/sha256/" + sha256Hex(layer), body: layer}, tarEntry{name: "blobs/sha256/" + sha256Hex(layer), body: layer})
		}},
		{"missing payload", func(f *ociLayoutFixture) { f.manifest.Layers = []ociDescriptor{d} }},
		{"digest mutation", func(f *ociLayoutFixture) {
			f.manifest.Layers = []ociDescriptor{d}
			f.extraEntries = append(f.extraEntries, tarEntry{name: "blobs/sha256/" + sha256Hex(layer), body: []byte("Layer payload")})
		}},
		{"descriptor media type", func(f *ociLayoutFixture) { f.index.Manifests[0].MediaType = "text/plain" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, _ := writeOCILayoutTar(t, "amd64", tc.mutate)
			if _, err := ValidateOCIOutput(p, "linux/amd64"); err == nil {
				t.Fatal("accepted invalid OCI layout")
			}
		})
	}
}

func TestOCIBoundedTarAndOverflow(t *testing.T) {
	t.Run("total tar quota", func(t *testing.T) {
		p, _ := writeOCILayoutTar(t, "amd64", nil)
		if err := os.Truncate(p, 107374182400+1); err != nil {
			t.Fatal(err)
		}
		// A sparse oversized archive must be rejected before reading its holes.
		done := make(chan error, 1)
		go func() { _, err := ValidateOCIOutput(p, "linux/amd64"); done <- err }()
		select {
		case err := <-done:
			if err == nil {
				t.Fatal("accepted oversized tar")
			}
		case <-time.After(time.Second):
			t.Fatal("oversized tar was not rejected before scanning")
		}
	})
	for _, size := range []string{"9223372036854775808", "-1"} {
		t.Run("JSON size "+size, func(t *testing.T) {
			p, _ := writeOCILayoutTar(t, "amd64", nil)
			rewriteOCITar(t, p, func(name string, b []byte) []byte {
				if name == "index.json" {
					var v map[string]any
					if err := json.Unmarshal(b, &v); err != nil {
						t.Fatal(err)
					}
					v["manifests"].([]any)[0].(map[string]any)["size"] = json.RawMessage(size)
					return mustJSON(t, v)
				}
				return b
			})
			if _, err := ValidateOCIOutput(p, "linux/amd64"); err == nil {
				t.Fatal("accepted invalid size")
			}
		})
	}
}

func TestOCIExactManifestMetadata(t *testing.T) {
	p, want := writeOCILayoutTar(t, "amd64", nil)
	out, err := ValidateOCIOutput(p, "linux/amd64")
	if err != nil {
		t.Fatal(err)
	}
	field := reflect.ValueOf(out).FieldByName("ManifestMediaType")
	if !field.IsValid() || field.String() != "application/vnd.oci.image.manifest.v1+json" {
		t.Fatal("exact validated manifest media type missing")
	}
	f, err := os.Open(p)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	tr := tar.NewReader(f)
	for {
		h, err := tr.Next()
		if err == io.EOF {
			t.Fatal("manifest missing")
		}
		if err != nil {
			t.Fatal(err)
		}
		if h.Name == "blobs/sha256/"+want.manifestDigest {
			if out.ManifestSize != h.Size {
				t.Fatal("manifest size changed")
			}
			break
		}
	}
}

func rewriteOCITar(t *testing.T, p string, mutate func(string, []byte) []byte) {
	t.Helper()
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	tr := tar.NewReader(bytes.NewReader(b))
	var dst bytes.Buffer
	tw := tar.NewWriter(&dst)
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		b, err := io.ReadAll(tr)
		if err != nil {
			t.Fatal(err)
		}
		b = mutate(h.Name, b)
		h.Size = int64(len(b))
		if err := tw.WriteHeader(h); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write(b); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, dst.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
}

func TestOCIAtLimitsAndStreaming(t *testing.T) {
	t.Run("exact JSON budget", func(t *testing.T) {
		p, _ := writeOCILayoutTar(t, "amd64", func(f *ociLayoutFixture) {
			f.manifest.Annotations = map[string]string{"padding": ""}
			f.manifest.Annotations["padding"] = strings.Repeat("x", (4<<20)-len(mustJSON(t, f.manifest)))
		})
		rewriteOCITar(t, p, func(name string, b []byte) []byte {
			if name == "index.json" {
				b = append(b, bytes.Repeat([]byte(" "), (4<<20)-len(b))...)
			}
			return b
		})
		out, err := ValidateOCIOutput(p, "linux/amd64")
		if err != nil {
			t.Fatal(err)
		}
		if out.ManifestSize != 4<<20 {
			t.Fatal("boundary fixture incorrect")
		}
	})
	t.Run("128 layers and 256 blobs", func(t *testing.T) {
		p, _ := writeOCILayoutTar(t, "amd64", func(f *ociLayoutFixture) {
			for i := 0; i < 254; i++ {
				b := []byte{byte(i), byte(i >> 8)}
				f.extraEntries = append(f.extraEntries, tarEntry{name: "blobs/sha256/" + sha256Hex(b), body: b})
				if i < 128 {
					f.manifest.Layers = append(f.manifest.Layers, ociDescriptor{MediaType: "application/vnd.oci.image.layer.v1.tar", Digest: "sha256:" + sha256Hex(b), Size: 2})
				}
			}
		})
		if _, err := ValidateOCIOutput(p, "linux/amd64"); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("stream layer hashing", func(t *testing.T) {
		b := bytes.Repeat([]byte("L"), 17<<20)
		p, _ := writeOCILayoutTar(t, "amd64", func(f *ociLayoutFixture) {
			f.manifest.Layers = []ociDescriptor{{MediaType: "application/vnd.oci.image.layer.v1.tar", Digest: "sha256:" + sha256Hex(b), Size: int64(len(b))}}
			f.extraEntries = append(f.extraEntries, tarEntry{name: "blobs/sha256/" + sha256Hex(b), body: b})
		})
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		if _, err := ValidateOCIOutput(p, "linux/amd64"); err != nil {
			t.Fatal(err)
		}
		runtime.ReadMemStats(&after)
		if after.TotalAlloc-before.TotalAlloc > 8<<20 {
			t.Fatal("scanner allocated layer-sized memory")
		}
	})
	for _, size := range []int64{-1, 107374182401} {
		t.Run(fmt.Sprintf("tar entry size %d", size), func(t *testing.T) {
			var b bytes.Buffer
			tw := tar.NewWriter(&b)
			if err := tw.WriteHeader(&tar.Header{Name: "index.json", Mode: 0600, Size: 0}); err != nil {
				t.Fatal(err)
			}
			header := append([]byte(nil), b.Bytes()[:512]...)
			if size < 0 {
				for i := 124; i < 136; i++ {
					header[i] = 255
				}
			} else {
				copy(header[124:136], fmt.Sprintf("%011o\x00", size))
			}
			for i := 148; i < 156; i++ {
				header[i] = ' '
			}
			sum := 0
			for _, v := range header {
				sum += int(v)
			}
			copy(header[148:156], fmt.Sprintf("%06o\x00 ", sum))
			p := filepath.Join(t.TempDir(), "bad.tar")
			if err := os.WriteFile(p, header, 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := ValidateOCIOutput(p, "linux/amd64"); err == nil {
				t.Fatal("accepted invalid tar entry size")
			}
		})
	}
}

// In-memory fixture helper retained for executor conformance tests. The scanner
// and uploader use descriptorEntry and streaming readers instead.
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
