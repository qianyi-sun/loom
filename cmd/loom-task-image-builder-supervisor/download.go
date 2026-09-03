package main

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"unsafe"
)

const (
	openat2ResolveNoMagicLinks = 0x02
	openat2ResolveNoSymlinks   = 0x04
	openat2ResolveBeneath      = 0x08
	openat2Syscall             = 437
)

type DownloadedBundle struct {
	Root           string
	Files          []TaskImageBundleFileV1
	TotalBytes     int64
	MetadataSHA256 string
}

type openHow struct {
	flags   uint64
	mode    uint64
	resolve uint64
}

func DownloadBundle(ctx context.Context, sealedCapability *SecretBuffer, workspaceDirFD int) (_ *DownloadedBundle, err error) {
	if sealedCapability == nil || sealedCapability.closed {
		return nil, errors.New("bundle capability unavailable")
	}
	if _, err := validateDirectoryDescriptor(workspaceDirFD); err != nil {
		return nil, err
	}
	var capability TaskImageBundleCapabilityV1
	if err := decodeStrictJSON(sealedCapability.data, &capability); err != nil {
		return nil, errors.New("bundle capability invalid")
	}
	if err := validateBundleCapability(capability); err != nil {
		return nil, err
	}

	client, base, err := bundleHTTPClient(capability)
	if err != nil {
		return nil, err
	}
	root, err := pathFromDirectoryFD(workspaceDirFD)
	if err != nil {
		return nil, err
	}

	created := []string{}
	cleanup := func() {
		for index := len(created) - 1; index >= 0; index-- {
			_ = unlinkBundleFile(workspaceDirFD, created[index])
		}
	}
	defer func() {
		if err != nil {
			cleanup()
		}
	}()

	var total int64
	for _, file := range capability.Files {
		if total+file.SizeBytes > capability.MaxBytes {
			return nil, errors.New("bundle aggregate quota exceeded")
		}
		targetURL := *base
		targetURL.Path = file.URLPath
		targetURL.RawPath = ""
		targetURL.RawQuery = ""
		targetURL.Fragment = ""
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL.String(), nil)
		if err != nil {
			return nil, err
		}
		response, err := client.Do(request)
		if err != nil {
			return nil, err
		}
		if response.StatusCode != http.StatusOK {
			response.Body.Close()
			return nil, fmt.Errorf("bundle download status %d", response.StatusCode)
		}
		if response.ContentLength >= 0 && response.ContentLength != file.SizeBytes {
			response.Body.Close()
			return nil, errors.New("bundle content length mismatch")
		}
		if err := createBundleParentDirectories(workspaceDirFD, filepath.Dir(file.Path)); err != nil {
			response.Body.Close()
			return nil, err
		}
		fd, err := createBundleFile(workspaceDirFD, file.Path, file.Mode)
		if err != nil {
			response.Body.Close()
			return nil, err
		}
		created = append(created, file.Path)
		output := os.NewFile(uintptr(fd), file.Path)
		hash := sha256.New()
		limited := io.LimitReader(response.Body, file.SizeBytes+1)
		written, copyErr := io.Copy(io.MultiWriter(output, hash), limited)
		syncErr := output.Sync()
		closeErr := output.Close()
		bodyErr := response.Body.Close()
		if copyErr != nil {
			return nil, copyErr
		}
		if syncErr != nil {
			return nil, syncErr
		}
		if closeErr != nil {
			return nil, closeErr
		}
		if bodyErr != nil {
			return nil, bodyErr
		}
		if written != file.SizeBytes {
			return nil, errors.New("bundle file size mismatch")
		}
		if hex.EncodeToString(hash.Sum(nil)) != file.SHA256 {
			return nil, errors.New("bundle file content digest mismatch")
		}
		if err := VerifyBundleFileAt(workspaceDirFD, file); err != nil {
			return nil, err
		}
		total += written
	}
	if err := fsyncDirectory(workspaceDirFD); err != nil {
		return nil, err
	}
	return &DownloadedBundle{
		Root:           root,
		Files:          append([]TaskImageBundleFileV1(nil), capability.Files...),
		TotalBytes:     total,
		MetadataSHA256: capability.MetadataSHA256,
	}, nil
}

func validateBundleCapability(capability TaskImageBundleCapabilityV1) error {
	if capability.Schema != taskImageBundleCapabilitySchema {
		return errors.New("bundle capability schema invalid")
	}
	if capability.MaxFiles <= 0 || len(capability.Files) == 0 || len(capability.Files) > capability.MaxFiles {
		return errors.New("bundle file count quota exceeded")
	}
	if capability.MaxBytes < 0 {
		return errors.New("bundle aggregate quota invalid")
	}
	var total int64
	seenPaths := map[string]struct{}{}
	seenURLs := map[string]struct{}{}
	for _, file := range capability.Files {
		if err := validateBundleFileSpec(file); err != nil {
			return err
		}
		if _, ok := seenPaths[file.Path]; ok {
			return errors.New("bundle duplicate file path")
		}
		if _, ok := seenURLs[file.URLPath]; ok {
			return errors.New("bundle duplicate URL path")
		}
		seenPaths[file.Path] = struct{}{}
		seenURLs[file.URLPath] = struct{}{}
		if total+file.SizeBytes < total {
			return errors.New("bundle aggregate quota invalid")
		}
		total += file.SizeBytes
	}
	if total > capability.MaxBytes {
		return errors.New("bundle aggregate quota exceeded")
	}
	metadata, err := BundleMetadataSHA256(capability.Files)
	if err != nil {
		return err
	}
	if !isDigest(capability.MetadataSHA256) || metadata != capability.MetadataSHA256 {
		return errors.New("bundle metadata digest mismatch")
	}
	return nil
}

func bundleHTTPClient(capability TaskImageBundleCapabilityV1) (*http.Client, *url.URL, error) {
	base, err := url.Parse(capability.BaseURL)
	if err != nil {
		return nil, nil, err
	}
	if base.Scheme != "https" || base.Host == "" || capability.ServerName == "" || strings.Contains(capability.ServerName, "/") {
		return nil, nil, errors.New("bundle TLS authority invalid")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM([]byte(capability.CAPEM)) {
		return nil, nil, errors.New("bundle CA invalid")
	}
	transport := &http.Transport{
		Proxy: nil,
		TLSClientConfig: &tls.Config{
			MinVersion: tls.VersionTLS13,
			ServerName: capability.ServerName,
			RootCAs:    roots,
		},
	}
	client := &http.Client{
		Transport: transport,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return errors.New("bundle redirects forbidden")
		},
	}
	return client, base, nil
}

func pathFromDirectoryFD(fd int) (string, error) {
	path, err := os.Readlink(fmt.Sprintf("/proc/self/fd/%d", fd))
	if err != nil {
		return "", err
	}
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return "", errors.New("directory descriptor path invalid")
	}
	return path, nil
}

func createBundleParentDirectories(rootFD int, dir string) error {
	if dir == "." || dir == "" {
		return nil
	}
	if err := validateRelativeBundlePath(dir); err != nil {
		return err
	}
	currentFD := rootFD
	opened := []int{}
	defer func() {
		for _, fd := range opened {
			syscall.Close(fd)
		}
	}()
	for _, component := range strings.Split(dir, string(os.PathSeparator)) {
		if err := syscall.Mkdirat(currentFD, component, 0o755); err != nil && !errors.Is(err, syscall.EEXIST) {
			return err
		}
		nextFD, err := syscall.Openat(currentFD, component, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0)
		if err != nil {
			return err
		}
		var statValue syscall.Stat_t
		if err := syscall.Fstat(nextFD, &statValue); err != nil {
			syscall.Close(nextFD)
			return err
		}
		if statValue.Mode&syscall.S_IFMT != syscall.S_IFDIR {
			syscall.Close(nextFD)
			return errors.New("bundle parent path invalid")
		}
		opened = append(opened, nextFD)
		currentFD = nextFD
	}
	return nil
}

func createBundleFile(rootFD int, relativePath string, mode uint32) (int, error) {
	if err := validateRelativeBundlePath(relativePath); err != nil {
		return -1, err
	}
	pointer, err := syscall.BytePtrFromString(relativePath)
	if err != nil {
		return -1, err
	}
	how := openHow{
		flags:   uint64(syscall.O_WRONLY | syscall.O_CREAT | syscall.O_EXCL | syscall.O_CLOEXEC | syscall.O_NOFOLLOW),
		mode:    uint64(mode),
		resolve: openat2ResolveBeneath | openat2ResolveNoSymlinks | openat2ResolveNoMagicLinks,
	}
	fd, _, errno := syscall.Syscall6(openat2Syscall, uintptr(rootFD), uintptr(unsafe.Pointer(pointer)), uintptr(unsafe.Pointer(&how)), unsafe.Sizeof(how), 0, 0)
	if errno != 0 {
		return -1, errno
	}
	if err := syscall.Fchmod(int(fd), mode); err != nil {
		syscall.Close(int(fd))
		return -1, err
	}
	return int(fd), nil
}

func unlinkBundleFile(rootFD int, relativePath string) error {
	if err := validateRelativeBundlePath(relativePath); err != nil {
		return err
	}
	dir, name := filepath.Split(relativePath)
	if dir == "" {
		return syscall.Unlinkat(rootFD, name)
	}
	parentFD, err := openParentDirectory(rootFD, strings.TrimSuffix(dir, string(os.PathSeparator)))
	if err != nil {
		return err
	}
	defer syscall.Close(parentFD)
	return syscall.Unlinkat(parentFD, name)
}

func openParentDirectory(rootFD int, dir string) (int, error) {
	if dir == "" || dir == "." {
		return syscall.Dup(rootFD)
	}
	if err := validateRelativeBundlePath(dir); err != nil {
		return -1, err
	}
	currentFD := rootFD
	opened := []int{}
	for _, component := range strings.Split(dir, string(os.PathSeparator)) {
		nextFD, err := syscall.Openat(currentFD, component, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0)
		if err != nil {
			closeRights(opened)
			return -1, err
		}
		opened = append(opened, nextFD)
		currentFD = nextFD
	}
	for _, fd := range opened[:len(opened)-1] {
		syscall.Close(fd)
	}
	return opened[len(opened)-1], nil
}

func fsyncDirectory(fd int) error {
	duplicated, err := syscall.Dup(fd)
	if err != nil {
		return err
	}
	defer syscall.Close(duplicated)
	return syscall.Fsync(duplicated)
}
