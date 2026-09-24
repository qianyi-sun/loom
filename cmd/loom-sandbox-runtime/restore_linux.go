//go:build linux

package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"
)

var restoreStageName = regexp.MustCompile(`^\.loom-restore-[a-f0-9]{32}$`)

// The trusted controller validates archives and declarations before extraction.
// This boundary additionally refuses runtime/private roots, including ancestors.
func allowedRestoreRoot(root string) bool {
	if len(root) > 4096 || root == "/" || root == "/tmp" || filepath.Clean(root) != root || !filepath.IsAbs(root) {
		return false
	}
	for _, protected := range []string{"/proc", "/sys", "/dev", "/run", "/var/run", "/loom", "/tests", "/verifier", "/solution", "/opt/verifier", "/opt/verifier-python", "/opt/verifier-assets", "/opt/verifier-tools"} {
		if root == protected || strings.HasPrefix(root, protected+"/") || strings.HasPrefix(protected, root+"/") {
			return false
		}
	}
	return true
}

func openRestoreDirectory(parent int, name string) (*os.File, error) {
	fd, err := syscall.Openat(parent, name, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), name), nil
}

func directoryEntries(directory *os.File) ([]os.DirEntry, error) {
	entries, err := directory.ReadDir(100001)
	if len(entries) > 100000 {
		return nil, errors.New("directory entry limit exceeded")
	}
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	return entries, nil
}

func readDirectoryACL(path, name string) ([]byte, error) {
	size, err := syscall.Getxattr(path, name, nil)
	if errors.Is(err, syscall.ENODATA) || errors.Is(err, syscall.ENOTSUP) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if size > 65536 {
		return nil, errors.New("directory ACL exceeds limit")
	}
	value := make([]byte, size)
	n, err := syscall.Getxattr(path, name, value)
	if err != nil {
		return nil, err
	}
	return value[:n], nil
}

// Promote a fully extracted directory without starting any executable while
// the old tree is incomplete. In particular, the directory may hold ld.so/libc.
// Both directories are pinned; moves and removals cannot follow a replaced
// ancestor. Staging inside the destination avoids cross-device moves and does
// not require write permission on the destination's parent.
func replaceDirectory(root, stageName string) error {
	if !allowedRestoreRoot(root) || !restoreStageName.MatchString(stageName) {
		return errors.New("invalid directory restore request")
	}
	// Ancestors only need search permission; only the destination/stage are
	// enumerated. Linux O_PATH pins ancestors without requiring read access.
	const openPath = 0x200000
	parent, leaf, err := openFileParent(root, false, openPath)
	if err != nil {
		return err
	}
	defer syscall.Close(parent)
	destination, err := openRestoreDirectory(parent, leaf)
	if err != nil {
		return err
	}
	defer destination.Close()
	destinationFD := int(destination.Fd())
	stage, err := openRestoreDirectory(destinationFD, stageName)
	if err != nil {
		return err
	}
	defer stage.Close()
	stageFD := int(stage.Fd())
	info, err := stage.Stat()
	if err != nil {
		return err
	}
	metadata := info.Sys().(*syscall.Stat_t)
	newEntries, err := directoryEntries(stage)
	if err != nil {
		return err
	}
	for _, entry := range newEntries {
		if entry.Name() == stageName {
			return errors.New("staging directory collides with archive entry")
		}
	}
	oldEntries, err := directoryEntries(destination)
	if err != nil {
		return err
	}
	stagePath := fmt.Sprintf("/proc/self/fd/%d", stageFD)
	destinationPath := fmt.Sprintf("/proc/self/fd/%d", destinationFD)
	acls := make(map[string][]byte)
	for _, name := range []string{"system.posix_acl_access", "system.posix_acl_default"} {
		acls[name], err = readDirectoryACL(stagePath, name)
		if err != nil {
			return err
		}
	}
	// Extraction can leave a read-only final directory. Moving its children
	// requires write/search permissions; retain the captured mode and ACLs for
	// the destination while temporarily making only this staging inode writable.
	if err := syscall.Fchmod(stageFD, metadata.Mode&07777|0700); err != nil {
		return err
	}
	// All request/stage validation precedes deletion. The server completes this
	// bounded local operation if the client disconnects; Pod termination remains
	// the authoritative failure/cancellation cleanup. This is not crash-atomic.
	for _, entry := range oldEntries {
		if entry.Name() != stageName {
			if err := os.RemoveAll(filepath.Join(destinationPath, entry.Name())); err != nil {
				return err
			}
		}
	}
	for _, entry := range newEntries {
		if err := syscall.Renameat(stageFD, entry.Name(), destinationFD, entry.Name()); err != nil {
			return err
		}
	}
	if err := os.Remove(filepath.Join(destinationPath, stageName)); err != nil {
		return err
	}
	var oldMetadata syscall.Stat_t
	if err := syscall.Fstat(destinationFD, &oldMetadata); err != nil {
		return err
	}
	if oldMetadata.Uid != metadata.Uid || oldMetadata.Gid != metadata.Gid {
		if err := syscall.Fchown(destinationFD, int(metadata.Uid), int(metadata.Gid)); err != nil {
			return err
		}
	}
	if err := syscall.Fchmod(destinationFD, metadata.Mode&07777); err != nil {
		return err
	}
	for name, value := range acls {
		if value == nil {
			err = syscall.Removexattr(destinationPath, name)
			if errors.Is(err, syscall.ENODATA) || errors.Is(err, syscall.ENOTSUP) {
				err = nil
			}
		} else {
			err = syscall.Setxattr(destinationPath, name, value, 0)
		}
		if err != nil {
			return err
		}
	}
	return os.Chtimes(destinationPath, time.Unix(metadata.Atim.Sec, metadata.Atim.Nsec), time.Unix(metadata.Mtim.Sec, metadata.Mtim.Nsec))
}
