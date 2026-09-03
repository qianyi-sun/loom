package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

const taskImageBundleCapabilitySchema = "loom.task-image-bundle-capability/v1"

type TaskImageBundleCapabilityV1 struct {
	Schema         string                  `json:"schema"`
	BaseURL        string                  `json:"base_url"`
	ServerName     string                  `json:"server_name"`
	CAPEM          string                  `json:"ca_pem"`
	MaxFiles       int                     `json:"max_files"`
	MaxBytes       int64                   `json:"max_bytes"`
	Files          []TaskImageBundleFileV1 `json:"files"`
	MetadataSHA256 string                  `json:"metadata_sha256"`
}

type TaskImageBundleFileV1 struct {
	Path      string `json:"path"`
	URLPath   string `json:"url_path"`
	SizeBytes int64  `json:"size_bytes"`
	Mode      uint32 `json:"mode"`
	SHA256    string `json:"sha256"`
}

type FileChecksum struct {
	SHA256    string
	SizeBytes int64
	Mode      uint32
}

func BundleMetadataSHA256(files []TaskImageBundleFileV1) (string, error) {
	values := make([]map[string]any, 0, len(files))
	for _, file := range files {
		if err := validateBundleFileSpec(file); err != nil {
			return "", err
		}
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
		return "", err
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

func HashFileAt(rootFD int, relativePath string) (FileChecksum, error) {
	if rootFD < 0 {
		return FileChecksum{}, errors.New("root descriptor invalid")
	}
	if err := validateRelativeBundlePath(relativePath); err != nil {
		return FileChecksum{}, err
	}
	fd, err := openFileBeneath(rootFD, relativePath)
	if err != nil {
		return FileChecksum{}, err
	}
	defer syscall.Close(fd)

	var statValue syscall.Stat_t
	if err := syscall.Fstat(fd, &statValue); err != nil {
		return FileChecksum{}, err
	}
	if statValue.Mode&syscall.S_IFMT != syscall.S_IFREG {
		return FileChecksum{}, errors.New("bundle file must be regular")
	}
	hashFD, err := dupFDForHash(fd)
	if err != nil {
		return FileChecksum{}, err
	}
	file := os.NewFile(uintptr(hashFD), relativePath)
	if file == nil {
		syscall.Close(hashFD)
		return FileChecksum{}, errors.New("bundle file unavailable")
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return FileChecksum{}, err
	}
	return FileChecksum{
		SHA256:    hex.EncodeToString(hash.Sum(nil)),
		SizeBytes: statValue.Size,
		Mode:      uint32(os.FileMode(statValue.Mode).Perm()),
	}, nil
}

func VerifyBundleFileAt(rootFD int, spec TaskImageBundleFileV1) error {
	if err := validateBundleFileSpec(spec); err != nil {
		return err
	}
	got, err := HashFileAt(rootFD, spec.Path)
	if err != nil {
		return err
	}
	if got.SHA256 != spec.SHA256 {
		return errors.New("bundle file content digest mismatch")
	}
	if got.SizeBytes != spec.SizeBytes {
		return errors.New("bundle file size mismatch")
	}
	if got.Mode != spec.Mode {
		return errors.New("bundle file mode mismatch")
	}
	return nil
}

func validateBundleFileSpec(file TaskImageBundleFileV1) error {
	if err := validateRelativeBundlePath(file.Path); err != nil {
		return err
	}
	if file.URLPath == "" || !strings.HasPrefix(file.URLPath, "/") || strings.Contains(file.URLPath, "://") {
		return errors.New("bundle URL path invalid")
	}
	if strings.Contains(file.URLPath, "\x00") || strings.Contains(file.URLPath, "/../") || strings.HasSuffix(file.URLPath, "/..") {
		return errors.New("bundle URL path invalid")
	}
	if file.SizeBytes < 0 {
		return errors.New("bundle file size invalid")
	}
	if file.Mode&^0o777 != 0 || file.Mode&0o222 != 0 || file.Mode == 0 {
		return errors.New("bundle file mode invalid")
	}
	if !isDigest(file.SHA256) {
		return errors.New("bundle file digest invalid")
	}
	return nil
}

func validateRelativeBundlePath(path string) error {
	if path == "" || filepath.IsAbs(path) || filepath.Clean(path) != path || path == "." {
		return errors.New("bundle path invalid")
	}
	if strings.Contains(path, "\x00") {
		return errors.New("bundle path invalid")
	}
	for _, component := range strings.Split(path, string(os.PathSeparator)) {
		if component == "" || component == "." || component == ".." {
			return errors.New("bundle path invalid")
		}
	}
	return nil
}

func dupFDForHash(fd int) (int, error) {
	duplicated, err := syscall.Dup(fd)
	if err != nil {
		return -1, err
	}
	return duplicated, nil
}
