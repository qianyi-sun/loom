//go:build linux

package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
)

var ErrCloneIntoCgroupUnsupported = errors.New("clone3 into cgroup unsupported")

type Process struct {
	cmd              *exec.Cmd
	PID              int
	ExecutableSHA256 string
	CgroupDevice     uint64
	CgroupInode      uint64
}

var (
	processCommandStarter = func(cmd *exec.Cmd) error { return cmd.Start() }
	processCommandWaiter  = func(cmd *exec.Cmd) error { return cmd.Wait() }
	processCommandKiller  = func(process *os.Process) error { return process.Kill() }
	processCgroupIdentity = liveProcessCgroupIdentity
)

func LaunchInCgroup(ctx context.Context, executable ExecutableMember, argv []string, env []string, cgroupDirFD int) (*Process, error) {
	if err := validateLaunchEnvironment(env); err != nil {
		return nil, err
	}
	if cgroupDirFD < 0 {
		return nil, errors.New("cgroup descriptor invalid")
	}
	cgroupStat, err := validateDirectoryDescriptor(cgroupDirFD)
	if err != nil {
		return nil, err
	}
	cgroupIdentity := identityFromStatValue(cgroupStat)

	executableFD, digest, err := openAndVerifyLaunchExecutable(executable)
	if err != nil {
		return nil, err
	}
	defer syscall.Close(executableFD)

	fdPath := fmt.Sprintf("/proc/self/fd/%d", executableFD)
	cmd := exec.CommandContext(ctx, fdPath, argv...)
	cmd.Args[0] = executable.Path
	cmd.Env = append([]string{}, env...)
	cmd.ExtraFiles = nil
	cmd.SysProcAttr = &syscall.SysProcAttr{
		UseCgroupFD: true,
		CgroupFD:    cgroupDirFD,
	}
	if err := processCommandStarter(cmd); err != nil {
		if errors.Is(err, syscall.ENOSYS) {
			return nil, ErrCloneIntoCgroupUnsupported
		}
		return nil, err
	}
	if cmd.Process == nil || cmd.Process.Pid <= 0 {
		return nil, errors.New("launched process missing pid")
	}
	childIdentity, err := processCgroupIdentity(cmd.Process.Pid)
	if err != nil || childIdentity.dev != cgroupIdentity.dev || childIdentity.ino != cgroupIdentity.ino {
		_ = processCommandKiller(cmd.Process)
		_ = processCommandWaiter(cmd)
		if err != nil {
			return nil, err
		}
		return nil, errors.New("launched process outside requested cgroup")
	}
	return &Process{
		cmd:              cmd,
		PID:              cmd.Process.Pid,
		ExecutableSHA256: digest,
		CgroupDevice:     cgroupIdentity.dev,
		CgroupInode:      cgroupIdentity.ino,
	}, nil
}

func (p *Process) Wait() error {
	if p == nil || p.cmd == nil {
		return nil
	}
	return processCommandWaiter(p.cmd)
}

func (p *Process) Signal(signal os.Signal) error {
	if p == nil || p.cmd == nil || p.cmd.Process == nil {
		return nil
	}
	return p.cmd.Process.Signal(signal)
}

func (p *Process) Kill() error {
	if p == nil || p.cmd == nil || p.cmd.Process == nil {
		return nil
	}
	return processCommandKiller(p.cmd.Process)
}

func (p *Process) Close() error {
	return nil
}

func openAndVerifyLaunchExecutable(member ExecutableMember) (int, string, error) {
	if !filepath.IsAbs(member.Path) || filepath.Clean(member.Path) != member.Path {
		return -1, "", errors.New("executable path invalid")
	}
	if !isDigest(member.SHA256) {
		return -1, "", errors.New("executable digest invalid")
	}
	fd, err := syscall.Open(member.Path, syscall.O_RDONLY|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return -1, "", err
	}
	complete := false
	defer func() {
		if !complete {
			syscall.Close(fd)
		}
	}()
	var statValue syscall.Stat_t
	if err := syscall.Fstat(fd, &statValue); err != nil {
		return -1, "", err
	}
	if statValue.Mode&syscall.S_IFMT != syscall.S_IFREG {
		return -1, "", errors.New("executable must be regular")
	}
	if os.FileMode(statValue.Mode).Perm() != memberExecutableMode {
		return -1, "", errors.New("executable mode invalid")
	}
	hashFD, err := syscall.Dup(fd)
	if err != nil {
		return -1, "", err
	}
	file := os.NewFile(uintptr(hashFD), member.Path)
	hash := sha256.New()
	_, copyErr := io.Copy(hash, file)
	closeErr := file.Close()
	if copyErr != nil {
		return -1, "", copyErr
	}
	if closeErr != nil {
		return -1, "", closeErr
	}
	digest := hex.EncodeToString(hash.Sum(nil))
	if digest != member.SHA256 {
		return -1, "", errors.New("executable digest mismatch")
	}
	complete = true
	return fd, digest, nil
}

func validateLaunchEnvironment(env []string) error {
	seen := map[string]struct{}{}
	for _, entry := range env {
		name, _, found := strings.Cut(entry, "=")
		if !found || name == "" {
			return errors.New("process environment entry invalid")
		}
		if name == "PATH" || name == "HOME" || name == "TMPDIR" {
			return fmt.Errorf("process environment key forbidden: %s", name)
		}
		if _, ok := seen[name]; ok {
			return fmt.Errorf("duplicate process environment key: %s", name)
		}
		seen[name] = struct{}{}
	}
	return nil
}

func liveProcessCgroupIdentity(pid int) (fileIdentity, error) {
	payload, err := os.ReadFile(fmt.Sprintf("/proc/%d/cgroup", pid))
	if err != nil {
		return fileIdentity{}, err
	}
	for _, line := range strings.Split(string(payload), "\n") {
		if !strings.HasPrefix(line, "0::") {
			continue
		}
		cgroupPath := strings.TrimPrefix(line, "0::")
		fullPath := filepath.Join("/sys/fs/cgroup", strings.TrimPrefix(cgroupPath, "/"))
		info, err := os.Stat(fullPath)
		if err != nil {
			return fileIdentity{}, err
		}
		return statIdentity(info)
	}
	return fileIdentity{}, errors.New("process cgroup v2 membership unavailable")
}

func identityFromStatValue(statValue *syscall.Stat_t) fileIdentity {
	return fileIdentity{
		dev:  uint64(statValue.Dev),
		ino:  uint64(statValue.Ino),
		uid:  statValue.Uid,
		mode: os.FileMode(statValue.Mode),
	}
}
