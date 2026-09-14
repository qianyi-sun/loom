package main

import (
	"bytes"
	"crypto/sha256"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"syscall"
)

const maxBundleCABytes = 128 * 1024

func validateBundleConfigFields(payload []byte) error {
	var fields map[string]json.RawMessage
	if json.Unmarshal(payload, &fields) != nil {
		return errors.New("bundle trust configuration invalid")
	}
	for key, value := range fields {
		if !strings.EqualFold(key, "bundle") {
			continue
		}
		if key != "bundle" || requireRegisteredJSONFields(value, "origin", "bucket", "ca") != nil {
			return errors.New("bundle trust configuration fields invalid")
		}
		var bundle map[string]json.RawMessage
		if json.Unmarshal(value, &bundle) != nil || requireRegisteredJSONFields(bundle["ca"], "path", "sha256") != nil {
			return errors.New("bundle trust CA configuration fields invalid")
		}
	}
	return nil
}

func loadBundleDownloadTrust(disk bundleTrustDiskConfig, releaseRoot string) (BundleDownloadTrust, error) {
	trust := BundleDownloadTrust{Origin: disk.Origin, Bucket: disk.Bucket}
	if _, err := registeredBundleOrigin(trust); err != nil {
		return BundleDownloadTrust{}, err
	}
	payload, err := loadReleaseCAPEM(disk.CA, releaseRoot)
	if err != nil {
		return BundleDownloadTrust{}, err
	}
	trust.Roots = x509.NewCertPool()
	if !trust.Roots.AppendCertsFromPEM(payload) {
		return BundleDownloadTrust{}, errors.New("bundle trust CA unavailable")
	}
	return trust, nil
}

// loadReleaseCAPEM returns an owned snapshot of public CA data from one exact
// immutable release member. Bundle and publication trust share this boundary,
// but retain independently configured endpoints and roots.
func loadReleaseCAPEM(member executableDiskConfig, releaseRoot string) ([]byte, error) {
	if !isDigest(member.SHA256) {
		return nil, errors.New("release CA digest invalid")
	}
	payload, err := readBundleReleaseCA(member.Path, releaseRoot)
	if err != nil {
		return nil, err
	}
	if fmt.Sprintf("%x", sha256.Sum256(payload)) != member.SHA256 {
		return nil, errors.New("release CA digest mismatch")
	}
	count := 0
	for remaining := bytes.TrimSpace(payload); len(remaining) > 0; {
		// A CA artifact is public trust data only. Do not silently ignore private
		// keys, unknown PEM blocks or arbitrary bytes around an otherwise valid CA.
		if !bytes.HasPrefix(remaining, []byte("-----BEGIN CERTIFICATE-----")) {
			return nil, errors.New("release CA PEM invalid")
		}
		end := bytes.Index(remaining, []byte("-----END CERTIFICATE-----"))
		if end < 0 {
			return nil, errors.New("release CA PEM invalid")
		}
		end += len("-----END CERTIFICATE-----")
		encoded := remaining[:end]
		// pem.Decode searches past malformed leading blocks; isolate exactly one
		// block so a later valid certificate cannot conceal invalid preceding data.
		if bytes.Count(encoded, []byte("-----BEGIN CERTIFICATE-----")) != 1 {
			return nil, errors.New("release CA PEM invalid")
		}
		block, rest := pem.Decode(encoded)
		if block == nil || block.Type != "CERTIFICATE" || len(block.Headers) != 0 || len(bytes.TrimSpace(rest)) != 0 {
			return nil, errors.New("release CA PEM invalid")
		}
		certificate, err := x509.ParseCertificate(block.Bytes)
		if err != nil || !certificate.IsCA || !certificate.BasicConstraintsValid {
			return nil, errors.New("release CA certificate invalid")
		}
		count++
		remaining = bytes.TrimSpace(remaining[end:])
	}
	if count == 0 {
		return nil, errors.New("release CA unavailable")
	}
	return payload, nil
}

func readBundleReleaseCA(memberPath, releaseRoot string) ([]byte, error) {
	relative, err := releaseRelativePath(memberPath, releaseRoot)
	if err != nil {
		return nil, err
	}
	root, err := openVerifiedDirectory(releaseRoot, releaseDirectoryMode)
	if err != nil {
		return nil, err
	}
	opened := []int{root}
	defer func() { closeRights(opened) }()
	parts := strings.Split(relative, "/")
	parent := root
	for i, part := range parts {
		if part == "" || part == "." || part == ".." {
			return nil, errors.New("bundle trust CA member path invalid")
		}
		flags := syscall.O_RDONLY | syscall.O_CLOEXEC | syscall.O_NOFOLLOW | syscall.O_NONBLOCK
		if i < len(parts)-1 {
			flags |= syscall.O_DIRECTORY
		}
		fd, err := syscall.Openat(parent, part, flags, 0)
		if err != nil {
			return nil, errors.New("bundle trust CA member unavailable")
		}
		opened = append(opened, fd)
		var stat syscall.Stat_t
		if syscall.Fstat(fd, &stat) != nil || stat.Uid != requiredOwnerUID {
			return nil, errors.New("bundle trust CA member ownership invalid")
		}
		if i < len(parts)-1 {
			if stat.Mode&syscall.S_IFMT != syscall.S_IFDIR || stat.Mode&0o7777 != uint32(releaseDirectoryMode) {
				return nil, errors.New("bundle trust CA parent metadata invalid")
			}
			parent = fd
			continue
		}
		if stat.Mode&syscall.S_IFMT != syscall.S_IFREG || stat.Mode&0o7777 != uint32(configFileMode) ||
			stat.Nlink != 1 || stat.Size <= 0 || stat.Size > maxBundleCABytes {
			return nil, errors.New("bundle trust CA member metadata invalid")
		}
		file := os.NewFile(uintptr(fd), memberPath)
		// Transfer ownership to os.File; avoid closing the FD twice after reuse.
		opened[len(opened)-1] = -1
		payload, readErr := io.ReadAll(io.LimitReader(file, maxBundleCABytes+1))
		var after syscall.Stat_t
		statErr := syscall.Fstat(fd, &after)
		closeErr := file.Close()
		if readErr != nil || statErr != nil || closeErr != nil || len(payload) != int(stat.Size) ||
			stat.Dev != after.Dev || stat.Ino != after.Ino || stat.Mode != after.Mode || stat.Uid != after.Uid ||
			stat.Nlink != after.Nlink || stat.Size != after.Size || stat.Mtim != after.Mtim || stat.Ctim != after.Ctim {
			return nil, errors.New("bundle trust CA member changed")
		}
		return payload, nil
	}
	return nil, errors.New("bundle trust CA member missing")
}
