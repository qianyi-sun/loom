package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
)

const (
	configSchema         = "loom.task-image-builder-supervisor-config/v1"
	memberExecutableMode = 0o555
	configFileMode       = 0o444
	releaseDirectoryMode = 0o555
	maxConfigBytes       = 1 << 20
)

var (
	requiredOwnerUID      = uint32(0)
	loadConfigPreOpenHook func() error
)

type Config struct {
	ReleaseSHA256 string
	CPUArch       string
	Guard         GuardConfig
	Runtime       RuntimeConfig
}

type GuardConfig struct {
	SocketPath        string
	MaxPacketBytes    int
	AckTimeoutSeconds int
}

type RuntimeConfig struct {
	RootlessKit ExecutableMember
	Buildctl    ExecutableMember
	Buildkitd   ExecutableMember
}

type ExecutableMember struct {
	Path   string
	SHA256 string
}

type configDisk struct {
	Schema        string            `json:"schema"`
	ReleaseSHA256 string            `json:"release_sha256"`
	CPUArch       string            `json:"cpu_arch"`
	Guard         guardDiskConfig   `json:"guard"`
	Runtime       runtimeDiskConfig `json:"runtime"`
}

type guardDiskConfig struct {
	SocketPath        string `json:"socket_path"`
	MaxPacketBytes    int    `json:"max_packet_bytes"`
	AckTimeoutSeconds int    `json:"ack_timeout_seconds"`
}

type runtimeDiskConfig struct {
	RootlessKit executableDiskConfig `json:"rootlesskit"`
	Buildctl    executableDiskConfig `json:"buildctl"`
	Buildkitd   executableDiskConfig `json:"buildkitd"`
}

type executableDiskConfig struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}

type fileIdentity struct {
	dev  uint64
	ino  uint64
	uid  uint32
	mode os.FileMode
}

func parseArguments(args []string) (startupOptions, error) {
	if len(args) != 2 || args[0] != "--grant-id" {
		return startupOptions{}, errors.New("supervisor requires exactly --grant-id <canonical-nonzero-uuid>")
	}
	if !isCanonicalNonZeroUUID(args[1]) {
		return startupOptions{}, errors.New("grant id must be canonical non-zero UUID")
	}
	return startupOptions{GrantID: args[1]}, nil
}

func sanitizeEnvironment(environ []string, quotaRoot string) ([]string, error) {
	if !filepath.IsAbs(quotaRoot) || filepath.Clean(quotaRoot) != quotaRoot {
		return nil, errors.New("quota root must be absolute")
	}
	allowed := map[string]bool{
		"SLURM_JOB_ID":       true,
		"SLURM_JOB_UID":      true,
		"SLURM_JOB_GID":      true,
		"SLURM_JOB_USER":     true,
		"SLURM_CLUSTER_NAME": true,
		"SLURMD_NODENAME":    true,
	}
	values := make(map[string]string, len(allowed))
	for _, entry := range environ {
		name, value, found := strings.Cut(entry, "=")
		if !found || name == "" {
			return nil, errors.New("environment entry invalid")
		}
		if name == "HOME" || name == "TMPDIR" || name == "LANG" || name == "TZ" {
			return nil, fmt.Errorf("inherited environment key forbidden: %s", name)
		}
		if !allowed[name] {
			return nil, fmt.Errorf("inherited environment key forbidden: %s", name)
		}
		if _, exists := values[name]; exists {
			return nil, fmt.Errorf("duplicate environment key: %s", name)
		}
		values[name] = value
	}
	result := []string{
		"LANG=C.UTF-8",
		"TZ=UTC",
		"HOME=" + filepath.Join(quotaRoot, "home"),
		"TMPDIR=" + filepath.Join(quotaRoot, "tmp"),
		"SLURM_JOB_ID=" + values["SLURM_JOB_ID"],
		"SLURM_JOB_UID=" + values["SLURM_JOB_UID"],
		"SLURM_JOB_GID=" + values["SLURM_JOB_GID"],
		"SLURM_JOB_USER=" + values["SLURM_JOB_USER"],
		"SLURM_CLUSTER_NAME=" + values["SLURM_CLUSTER_NAME"],
		"SLURMD_NODENAME=" + values["SLURMD_NODENAME"],
	}
	return result, nil
}

func LoadConfig(path string, expectedRelease string) (Config, error) {
	if !isDigest(expectedRelease) {
		return Config{}, errors.New("expected release digest invalid")
	}
	if !filepath.IsAbs(compiledReleaseBasePath) {
		return Config{}, errors.New("compiled release base path invalid")
	}
	identity, payload, err := readVerifiedConfigFile(path)
	if err != nil {
		return Config{}, err
	}
	if identity.uid != requiredOwnerUID || identity.mode.Perm() != configFileMode {
		return Config{}, errors.New("config ownership or mode invalid")
	}
	if err := rejectDuplicateJSONKeys(payload); err != nil {
		return Config{}, err
	}
	var disk configDisk
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&disk); err != nil {
		return Config{}, errors.New("config JSON invalid")
	}
	if err := expectJSONEOF(decoder); err != nil {
		return Config{}, errors.New("config JSON invalid")
	}
	if disk.Schema != configSchema {
		return Config{}, errors.New("config schema invalid")
	}
	if disk.ReleaseSHA256 != expectedRelease {
		return Config{}, errors.New("release digest mismatch")
	}
	if disk.CPUArch != runtime.GOARCH {
		return Config{}, errors.New("cpu architecture mismatch")
	}
	releaseRoot := filepath.Join(compiledReleaseBasePath, expectedRelease)
	if err := verifyDirectory(releaseRoot, releaseDirectoryMode); err != nil {
		return Config{}, err
	}
	cfg := Config{
		ReleaseSHA256: disk.ReleaseSHA256,
		CPUArch:       disk.CPUArch,
		Guard: GuardConfig{
			SocketPath:        disk.Guard.SocketPath,
			MaxPacketBytes:    disk.Guard.MaxPacketBytes,
			AckTimeoutSeconds: disk.Guard.AckTimeoutSeconds,
		},
		Runtime: RuntimeConfig{
			RootlessKit: ExecutableMember{
				Path:   disk.Runtime.RootlessKit.Path,
				SHA256: disk.Runtime.RootlessKit.SHA256,
			},
			Buildctl: ExecutableMember{
				Path:   disk.Runtime.Buildctl.Path,
				SHA256: disk.Runtime.Buildctl.SHA256,
			},
			Buildkitd: ExecutableMember{
				Path:   disk.Runtime.Buildkitd.Path,
				SHA256: disk.Runtime.Buildkitd.SHA256,
			},
		},
	}
	if err := verifyGuardSocketPath(cfg.Guard.SocketPath); err != nil {
		return Config{}, err
	}
	for _, member := range []ExecutableMember{
		cfg.Runtime.RootlessKit,
		cfg.Runtime.Buildctl,
		cfg.Runtime.Buildkitd,
	} {
		if err := verifyExecutableMember(member, releaseRoot); err != nil {
			return Config{}, err
		}
	}
	return cfg, nil
}

func readVerifiedConfigFile(path string) (fileIdentity, []byte, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return fileIdentity{}, nil, errors.New("config path invalid")
	}
	info, err := os.Lstat(path)
	if err != nil {
		return fileIdentity{}, nil, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return fileIdentity{}, nil, errors.New("config path must be regular file")
	}
	before, err := statIdentity(info)
	if err != nil {
		return fileIdentity{}, nil, err
	}
	if loadConfigPreOpenHook != nil {
		if err := loadConfigPreOpenHook(); err != nil {
			return fileIdentity{}, nil, err
		}
	}
	file, err := os.Open(path)
	if err != nil {
		return fileIdentity{}, nil, err
	}
	defer file.Close()

	infoAfter, err := file.Stat()
	if err != nil {
		return fileIdentity{}, nil, err
	}
	after, err := statIdentity(infoAfter)
	if err != nil {
		return fileIdentity{}, nil, err
	}
	if before != after {
		return fileIdentity{}, nil, errors.New("config file changed during open")
	}
	payload, err := io.ReadAll(io.LimitReader(file, maxConfigBytes+1))
	if err != nil {
		return fileIdentity{}, nil, err
	}
	if len(payload) == 0 || len(payload) > maxConfigBytes {
		return fileIdentity{}, nil, errors.New("config payload invalid")
	}
	return after, payload, nil
}

func verifyDirectory(path string, mode os.FileMode) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("release directory invalid")
	}
	identity, err := statIdentity(info)
	if err != nil {
		return err
	}
	if identity.uid != requiredOwnerUID || info.Mode().Perm() != mode {
		return errors.New("release directory ownership or mode invalid")
	}
	return nil
}

func verifyExecutableMember(member ExecutableMember, releaseRoot string) error {
	if !isDigest(member.SHA256) {
		return errors.New("member digest invalid")
	}
	releaseRootFD, err := openVerifiedDirectory(releaseRoot, releaseDirectoryMode)
	if err != nil {
		return err
	}
	defer syscall.Close(releaseRootFD)

	relativePath, err := releaseRelativePath(member.Path, releaseRoot)
	if err != nil {
		return err
	}
	memberFD, err := openFileBeneath(releaseRootFD, relativePath)
	if err != nil {
		return err
	}

	var statValue syscall.Stat_t
	if err := syscall.Fstat(memberFD, &statValue); err != nil {
		syscall.Close(memberFD)
		return err
	}
	mode := os.FileMode(statValue.Mode)
	if statValue.Mode&syscall.S_IFMT != syscall.S_IFREG {
		syscall.Close(memberFD)
		return errors.New("member must be regular file")
	}
	if statValue.Uid != requiredOwnerUID || mode.Perm() != memberExecutableMode {
		syscall.Close(memberFD)
		return errors.New("member ownership or mode invalid")
	}

	file := os.NewFile(uintptr(memberFD), member.Path)
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return err
	}
	if hex.EncodeToString(hash.Sum(nil)) != member.SHA256 {
		return errors.New("member digest mismatch")
	}
	return nil
}

func verifyGuardSocketPath(path string) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return errors.New("guard socket path must be absolute and clean")
	}
	if path != compiledGuardSocketPath {
		return errors.New("guard socket path must match compiled fixed path")
	}
	return nil
}

func verifyContentAddressedPath(path string, releaseRoot string) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return errors.New("path must be absolute and clean")
	}
	prefix := releaseRoot + string(os.PathSeparator)
	if path != releaseRoot && !strings.HasPrefix(path, prefix) {
		return errors.New("path is outside compiled content-addressed release")
	}
	return nil
}

func rejectDuplicateJSONKeys(payload []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	if err := scanJSONValue(decoder); err != nil {
		return errors.New("config JSON invalid")
	}
	if err := expectJSONEOF(decoder); err != nil {
		return errors.New("config JSON invalid")
	}
	return nil
}

func expectJSONEOF(decoder *json.Decoder) error {
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("extra JSON content")
		}
		return err
	}
	return nil
}

func scanJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delim, isDelim := token.(json.Delim)
	if !isDelim {
		return nil
	}
	switch delim {
	case '{':
		seen := map[string]struct{}{}
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return errors.New("object key invalid")
			}
			if _, exists := seen[key]; exists {
				return errors.New("duplicate key")
			}
			seen[key] = struct{}{}
			if err := scanJSONValue(decoder); err != nil {
				return err
			}
		}
		_, err := decoder.Token()
		return err
	case '[':
		for decoder.More() {
			if err := scanJSONValue(decoder); err != nil {
				return err
			}
		}
		_, err := decoder.Token()
		return err
	default:
		return nil
	}
}

func statIdentity(info os.FileInfo) (fileIdentity, error) {
	statValue, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return fileIdentity{}, errors.New("stat identity unavailable")
	}
	return fileIdentity{
		dev:  uint64(statValue.Dev),
		ino:  uint64(statValue.Ino),
		uid:  statValue.Uid,
		mode: info.Mode(),
	}, nil
}

func releaseRelativePath(path string, releaseRoot string) (string, error) {
	if err := verifyContentAddressedPath(path, releaseRoot); err != nil {
		return "", err
	}
	relativePath, err := filepath.Rel(releaseRoot, path)
	if err != nil {
		return "", err
	}
	if relativePath == "." || relativePath == "" {
		return "", errors.New("member path invalid")
	}
	if strings.HasPrefix(relativePath, ".."+string(os.PathSeparator)) || relativePath == ".." {
		return "", errors.New("member path invalid")
	}
	return relativePath, nil
}

func openVerifiedDirectory(path string, mode os.FileMode) (int, error) {
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return -1, err
	}
	var statValue syscall.Stat_t
	if err := syscall.Fstat(fd, &statValue); err != nil {
		syscall.Close(fd)
		return -1, err
	}
	if statValue.Mode&syscall.S_IFMT != syscall.S_IFDIR {
		syscall.Close(fd)
		return -1, errors.New("release directory invalid")
	}
	if statValue.Uid != requiredOwnerUID || os.FileMode(statValue.Mode).Perm() != mode {
		syscall.Close(fd)
		return -1, errors.New("release directory ownership or mode invalid")
	}
	return fd, nil
}

func openFileBeneath(rootFD int, relativePath string) (int, error) {
	components := strings.Split(relativePath, string(os.PathSeparator))
	currentFD := rootFD
	opened := []int{}
	defer func() {
		for _, fd := range opened {
			syscall.Close(fd)
		}
	}()

	for index, component := range components {
		if component == "" || component == "." || component == ".." {
			return -1, errors.New("member path invalid")
		}
		flags := syscall.O_RDONLY | syscall.O_CLOEXEC | syscall.O_NOFOLLOW
		if index < len(components)-1 {
			flags |= syscall.O_DIRECTORY
		}
		nextFD, err := syscall.Openat(currentFD, component, flags, 0)
		if err != nil {
			return -1, err
		}
		if index < len(components)-1 {
			var statValue syscall.Stat_t
			if err := syscall.Fstat(nextFD, &statValue); err != nil {
				syscall.Close(nextFD)
				return -1, err
			}
			if statValue.Mode&syscall.S_IFMT != syscall.S_IFDIR {
				syscall.Close(nextFD)
				return -1, errors.New("member path invalid")
			}
			opened = append(opened, nextFD)
			currentFD = nextFD
			continue
		}
		return nextFD, nil
	}
	return -1, errors.New("member path invalid")
}

func isCanonicalNonZeroUUID(value string) bool {
	if len(value) != 36 {
		return false
	}
	if value == "00000000-0000-0000-0000-000000000000" {
		return false
	}
	for index, r := range value {
		switch index {
		case 8, 13, 18, 23:
			if r != '-' {
				return false
			}
		default:
			if !strings.ContainsRune("0123456789abcdef", r) {
				return false
			}
		}
	}
	return true
}

func isDigest(value string) bool {
	if len(value) != 64 {
		return false
	}
	if value == strings.Repeat("0", 64) {
		return false
	}
	for _, r := range value {
		if !strings.ContainsRune("0123456789abcdef", r) {
			return false
		}
	}
	return true
}
