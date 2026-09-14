//go:build linux

package main

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"strings"
	"syscall"
)

// Every directory is pinned by descriptor before traversing the next component.
// O_NOFOLLOW on all components prevents task-controlled symlink races.
func fileParent(path string, create bool) (int, string, error) {
	if !strings.HasPrefix(path, "/") || strings.ContainsRune(path, '\x00') {
		return -1, "", errors.New("absolute path required")
	}
	parts := strings.Split(strings.TrimPrefix(path, "/"), "/")
	for _, p := range parts {
		if p == "" || p == "." || p == ".." {
			return -1, "", errors.New("invalid path component")
		}
	}
	fd, err := syscall.Open("/", syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_CLOEXEC, 0)
	if err != nil {
		return -1, "", err
	}
	for _, part := range parts[:len(parts)-1] {
		next, err := syscall.Openat(fd, part, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
		if create && errors.Is(err, syscall.ENOENT) {
			err = syscall.Mkdirat(fd, part, 0755)
			if err == nil || errors.Is(err, syscall.EEXIST) {
				next, err = syscall.Openat(fd, part, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
			}
		}
		syscall.Close(fd)
		if err != nil {
			return -1, "", err
		}
		fd = next
	}
	return fd, parts[len(parts)-1], nil
}

func readFile(path string) (*os.File, error) {
	dir, name, err := fileParent(path, false)
	if err != nil {
		return nil, err
	}
	defer syscall.Close(dir)
	fd, err := syscall.Openat(dir, name, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK|syscall.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), name)
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() {
		f.Close()
		return nil, errors.New("regular file required")
	}
	return f, nil
}

func writeFile(path string, body io.Reader, mode uint32) error {
	dir, name, err := fileParent(path, true)
	if err != nil {
		return err
	}
	defer syscall.Close(dir)
	// Reject an existing link or special target. Atomic rename below also avoids
	// following a link introduced after this check.
	existing, err := syscall.Openat(dir, name, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK|syscall.O_CLOEXEC, 0)
	if err == nil {
		f := os.NewFile(uintptr(existing), name)
		info, statErr := f.Stat()
		f.Close()
		if statErr != nil || !info.Mode().IsRegular() {
			return errors.New("regular file required")
		}
	} else if !errors.Is(err, syscall.ENOENT) {
		return err
	}
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		return err
	}
	temporary := ".loom-upload-" + hex.EncodeToString(token[:])
	fd, err := syscall.Openat(dir, temporary, syscall.O_WRONLY|syscall.O_CREAT|syscall.O_EXCL|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, mode)
	if err != nil {
		return err
	}
	defer syscall.Unlinkat(dir, temporary)
	f := os.NewFile(uintptr(fd), temporary)
	_, err = io.Copy(f, body)
	if err == nil {
		err = f.Chmod(os.FileMode(mode))
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	return syscall.Renameat(dir, temporary, dir, name)
}
