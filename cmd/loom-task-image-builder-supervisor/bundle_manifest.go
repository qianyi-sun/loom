package main

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"path"
	"strconv"
	"strings"
	"unicode/utf16"
	"unicode/utf8"
)

const maxRegisteredManifestBytes = 4 * 1024 * 1024

// RegisteredBundleObject is the authority's data-only V2 descriptor. URL is a
// transport secret and is deliberately excluded from both content identities.
type RegisteredBundleObject struct {
	RelativePath string `json:"relative_path"`
	SizeBytes    int64  `json:"size_bytes"`
	URL          string `json:"url"`
	SHA256       string `json:"sha256"`
	Mode         string `json:"mode"`
}

func validateRegisteredBundlePath(value string) error {
	if !utf8.ValidString(value) || len(value) > 1024 || value == ".loom-bundle-files.v1.json" ||
		strings.Contains(value, `\`) || validateRelativeBundlePath(value) != nil {
		return errors.New("registered bundle path invalid")
	}
	for _, r := range value {
		if r < 32 || r == 127 {
			return errors.New("registered bundle path invalid")
		}
	}
	return nil
}

// appendRegisteredPath is intentionally not a general-purpose JSON encoder.
// Callers first validate the restricted manifest path domain (valid Unicode,
// no controls, DEL or backslashes). RFC8785 preserves all other Unicode and HTML
// characters; only quotes need escaping. The legacy Python ensure_ascii mode
// encoding instead escapes every non-ASCII scalar, including surrogate pairs.
func appendRegisteredPath(output *strings.Builder, value string, ascii bool) {
	output.WriteByte('"')
	for _, r := range value {
		switch {
		case r == '"':
			output.WriteString(`\"`)
		case ascii && r >= 128:
			if r <= 0xffff {
				fmt.Fprintf(output, `\u%04x`, r)
			} else {
				hi, lo := utf16.EncodeRune(r)
				fmt.Fprintf(output, `\u%04x\u%04x`, hi, lo)
			}
		default:
			output.WriteRune(r)
		}
	}
	output.WriteByte('"')
}

func registeredBundleManifest(files []RegisteredBundleObject, taskChecksum string) ([]byte, []byte, error) {
	if !isDigest(taskChecksum) || len(files) == 0 || len(files) > maxTaskImageBuildBundleFiles {
		return nil, nil, errors.New("registered bundle manifest bounds invalid")
	}
	paths := make(map[string]bool, len(files))
	var total int64
	for i, file := range files {
		if validateRegisteredBundlePath(file.RelativePath) != nil || !isDigest(file.SHA256) ||
			(file.Mode != "0644" && file.Mode != "0755") || file.SizeBytes < 0 ||
			file.SizeBytes > maxTaskImageBuildBundleBytes-total ||
			(i > 0 && files[i-1].RelativePath >= file.RelativePath) {
			return nil, nil, errors.New("registered bundle descriptor invalid")
		}
		total += file.SizeBytes
		paths[file.RelativePath] = true
	}
	for _, file := range files {
		for parent := path.Dir(file.RelativePath); parent != "."; parent = path.Dir(parent) {
			if paths[parent] {
				return nil, nil, errors.New("registered bundle file is also a directory")
			}
		}
	}

	var metadata strings.Builder
	metadata.WriteString(`{"files":{`)
	for i, file := range files {
		if i != 0 {
			metadata.WriteByte(',')
		}
		appendRegisteredPath(&metadata, file.RelativePath, true)
		metadata.WriteString(`:{"mode":"` + file.Mode + `"}`)
	}
	metadata.WriteString(`},"schema_version":1}`)
	if metadata.Len() > maxRegisteredManifestBytes {
		return nil, nil, errors.New("registered bundle mode metadata too large")
	}
	modeBytes := []byte(metadata.String())
	modeDigest := fmt.Sprintf("%x", sha256.Sum256(modeBytes))

	// Closed ASCII field names are emitted in RFC8785 key order. Every integer
	// lies below 2^53 and has an exact decimal representation. Array order is the
	// registered Python codepoint path order, identical to Go's valid UTF8 order.
	var manifest strings.Builder
	manifest.WriteString(`{"bundle_file_metadata_sha256":"` + modeDigest + `","files":[`)
	for i, file := range files {
		if i != 0 {
			manifest.WriteByte(',')
		}
		manifest.WriteString(`{"mode":"` + file.Mode + `","path":`)
		appendRegisteredPath(&manifest, file.RelativePath, false)
		manifest.WriteString(`,"sha256":"` + file.SHA256 + `","size_bytes":`)
		manifest.WriteString(strconv.FormatInt(file.SizeBytes, 10))
		manifest.WriteByte('}')
	}
	manifest.WriteString(`],"schema_version":"loom.task-image-bundle-content.v1","task_checksum":"` + taskChecksum + `"}`)
	if manifest.Len() > maxRegisteredManifestBytes {
		return nil, nil, errors.New("registered bundle content manifest too large")
	}
	return []byte(manifest.String()), modeBytes, nil
}
