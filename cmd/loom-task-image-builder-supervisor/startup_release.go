package main

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

const supervisorReleaseMember = "bin/loom-task-builder-supervisor"

// The composite release hashes this ELF, so it cannot also be compiled into
// the ELF. Select via the kernel's running-executable identity and then require
// the fixed root-owned config to match. The installer and guard independently
// verify content hashes and grant binding; this is not replacement authority.
func installedSupervisorRelease() (string, error) {
	path, err := os.Readlink("/proc/self/exe")
	if err != nil {
		return "", errors.New("running supervisor path unavailable")
	}
	file, err := os.Open("/proc/self/exe")
	if err != nil {
		return "", errors.New("running supervisor descriptor unavailable")
	}
	defer file.Close()
	base, err := openStartupReleaseBase(compiledReleaseBasePath)
	if err != nil {
		return "", err
	}
	defer syscall.Close(base)
	return verifyInstalledSupervisor(base, compiledReleaseBasePath, path, int(file.Fd()))
}

func openStartupReleaseBase(path string) (int, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || path == "/" {
		return -1, errors.New("startup release base invalid")
	}
	current, err := syscall.Open("/", syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
	if err != nil {
		return -1, err
	}
	var root syscall.Stat_t
	if syscall.Fstat(current, &root) != nil || root.Uid != requiredOwnerUID || root.Mode&0o022 != 0 {
		syscall.Close(current)
		return -1, errors.New("startup root ownership invalid")
	}
	for _, part := range strings.Split(strings.TrimPrefix(path, "/"), "/") {
		next, err := openOwnedStartupDirectoryAt(current, part, false)
		syscall.Close(current)
		if err != nil {
			return -1, err
		}
		current = next
	}
	return current, nil
}

func openOwnedStartupDirectoryAt(parent int, name string, immutable bool) (int, error) {
	invalid := errors.New("startup directory ownership or mode invalid")
	if parent < 0 || name == "" || name == "." || name == ".." || strings.ContainsAny(name, "/\x00") {
		return -1, invalid
	}
	fd, err := syscall.Openat(parent, name, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
	if err != nil {
		return -1, invalid
	}
	var st syscall.Stat_t
	if syscall.Fstat(fd, &st) != nil || st.Mode&syscall.S_IFMT != syscall.S_IFDIR || st.Uid != requiredOwnerUID ||
		st.Mode&0o7022 != 0 || (immutable && st.Mode&0o7777 != uint32(releaseDirectoryMode)) {
		syscall.Close(fd)
		return -1, invalid
	}
	return fd, nil
}

func verifyInstalledSupervisor(baseFD int, base, path string, runningFD int) (string, error) {
	invalid := errors.New("running supervisor does not match installed release")
	if !filepath.IsAbs(base) || filepath.Clean(base) != base || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return "", invalid
	}
	rel, err := filepath.Rel(base, path)
	if err != nil {
		return "", invalid
	}
	parts := strings.Split(rel, "/")
	if len(parts) != 3 || !isDigest(parts[0]) || strings.Join(parts[1:], "/") != supervisorReleaseMember {
		return "", invalid
	}
	var running syscall.Stat_t
	if syscall.Fstat(runningFD, &running) != nil || running.Mode&syscall.S_IFMT != syscall.S_IFREG ||
		running.Mode&0o7777 != uint32(memberExecutableMode) || running.Uid != requiredOwnerUID || running.Nlink != 1 || running.Size <= 0 {
		return "", invalid
	}
	release, err := openOwnedStartupDirectoryAt(baseFD, parts[0], true)
	if err != nil {
		return "", err
	}
	defer syscall.Close(release)
	bin, err := openOwnedStartupDirectoryAt(release, "bin", true)
	if err != nil {
		return "", err
	}
	defer syscall.Close(bin)
	installed, err := syscall.Openat(bin, "loom-task-builder-supervisor", syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC|syscall.O_NONBLOCK, 0)
	if err != nil {
		return "", invalid
	}
	defer syscall.Close(installed)
	var named, after syscall.Stat_t
	if syscall.Fstat(installed, &named) != nil || syscall.Fstat(runningFD, &after) != nil ||
		!sameStartupFile(running, named) || !sameStartupFile(running, after) {
		return "", invalid
	}
	return parts[0], nil
}

func sameStartupFile(a, b syscall.Stat_t) bool {
	return a.Dev == b.Dev && a.Ino == b.Ino && a.Mode == b.Mode && a.Uid == b.Uid && a.Gid == b.Gid &&
		a.Nlink == b.Nlink && a.Size == b.Size && a.Mtim == b.Mtim && a.Ctim == b.Ctim
}
