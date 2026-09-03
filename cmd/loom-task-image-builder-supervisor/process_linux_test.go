//go:build linux

package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
)

func TestLaunchUsesCloneIntoCgroupFDAndFdBackedExecutable(t *testing.T) {
	root := t.TempDir()
	executablePath := filepath.Join(root, "runtime", "buildctl")
	writeExecutableFixture(t, executablePath, "#!/bin/sh\nprintf launched\n")
	member := ExecutableMember{Path: executablePath, SHA256: sha256FileHex(t, executablePath)}
	cgroupDir := filepath.Join(root, "cgroup")
	if err := os.Mkdir(cgroupDir, 0o755); err != nil {
		t.Fatalf("Mkdir(%q) error = %v", cgroupDir, err)
	}
	cgroupProcs := filepath.Join(cgroupDir, "cgroup.procs")
	if err := os.WriteFile(cgroupProcs, []byte("original\n"), 0o644); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", cgroupProcs, err)
	}
	cgroupFD := openDirectoryFD(t, cgroupDir)
	defer syscall.Close(cgroupFD)
	cgroupIdentity := identityFromStat(mustFstat(t, cgroupFD))

	var observedPath string
	var observedArgs []string
	var observedEnv []string
	var observedCgroupFD int
	var observedUseCgroupFD bool
	var observedExtraFiles int
	restoreProcessHooks(t)
	processCommandStarter = func(cmd *exec.Cmd) error {
		observedPath = cmd.Path
		observedArgs = append([]string(nil), cmd.Args...)
		observedEnv = append([]string(nil), cmd.Env...)
		observedExtraFiles = len(cmd.ExtraFiles)
		if cmd.SysProcAttr != nil {
			observedUseCgroupFD = cmd.SysProcAttr.UseCgroupFD
			observedCgroupFD = cmd.SysProcAttr.CgroupFD
		}
		cmd.Process, _ = os.FindProcess(999999)
		return nil
	}
	processCgroupIdentity = func(pid int) (fileIdentity, error) {
		if pid != 999999 {
			t.Fatalf("pid = %d, want fake child pid", pid)
		}
		return cgroupIdentity, nil
	}
	processCommandWaiter = func(*exec.Cmd) error { return nil }

	proc, err := LaunchInCgroup(context.Background(), member, []string{"--version"}, []string{"LANG=C.UTF-8"}, cgroupFD)
	if err != nil {
		t.Fatalf("LaunchInCgroup() error = %v", err)
	}
	defer proc.Close()

	if proc.PID != 999999 {
		t.Fatalf("PID = %d, want fake child pid", proc.PID)
	}
	if proc.ExecutableSHA256 != member.SHA256 {
		t.Fatalf("ExecutableSHA256 = %q, want %q", proc.ExecutableSHA256, member.SHA256)
	}
	if !strings.HasPrefix(observedPath, "/proc/self/fd/") {
		t.Fatalf("cmd.Path = %q, want fd-backed executable path", observedPath)
	}
	if !reflect.DeepEqual(observedArgs, []string{member.Path, "--version"}) {
		t.Fatalf("cmd.Args = %#v", observedArgs)
	}
	if !reflect.DeepEqual(observedEnv, []string{"LANG=C.UTF-8"}) {
		t.Fatalf("cmd.Env = %#v", observedEnv)
	}
	if !observedUseCgroupFD || observedCgroupFD != cgroupFD {
		t.Fatalf("cgroup launch = use:%v fd:%d, want UseCgroupFD with fd %d", observedUseCgroupFD, observedCgroupFD, cgroupFD)
	}
	if observedExtraFiles != 0 {
		t.Fatalf("ExtraFiles = %d, want none inherited", observedExtraFiles)
	}
	if got := string(mustReadFile(t, cgroupProcs)); got != "original\n" {
		t.Fatalf("cgroup.procs = %q, want untouched", got)
	}
}

func TestLaunchRejectsExecutableDigestDriftBeforeStarting(t *testing.T) {
	root := t.TempDir()
	executablePath := filepath.Join(root, "runtime", "rootlesskit")
	writeExecutableFixture(t, executablePath, "#!/bin/sh\nexit 0\n")
	member := ExecutableMember{Path: executablePath, SHA256: strings.Repeat("f", 64)}
	cgroupDir := filepath.Join(root, "cgroup")
	if err := os.Mkdir(cgroupDir, 0o755); err != nil {
		t.Fatalf("Mkdir(%q) error = %v", cgroupDir, err)
	}
	cgroupFD := openDirectoryFD(t, cgroupDir)
	defer syscall.Close(cgroupFD)

	calls := 0
	restoreProcessHooks(t)
	processCommandStarter = func(cmd *exec.Cmd) error {
		calls++
		return nil
	}

	if _, err := LaunchInCgroup(context.Background(), member, nil, nil, cgroupFD); err == nil {
		t.Fatal("LaunchInCgroup() succeeded, want digest drift error")
	}
	if calls != 0 {
		t.Fatalf("starter calls = %d, want no process start after digest drift", calls)
	}
}

func TestLaunchTreatsCloneIntoCgroupUnsupportedAsTerminal(t *testing.T) {
	root := t.TempDir()
	executablePath := filepath.Join(root, "runtime", "buildkitd")
	writeExecutableFixture(t, executablePath, "#!/bin/sh\nexit 0\n")
	member := ExecutableMember{Path: executablePath, SHA256: sha256FileHex(t, executablePath)}
	cgroupDir := filepath.Join(root, "cgroup")
	if err := os.Mkdir(cgroupDir, 0o755); err != nil {
		t.Fatalf("Mkdir(%q) error = %v", cgroupDir, err)
	}
	cgroupFD := openDirectoryFD(t, cgroupDir)
	defer syscall.Close(cgroupFD)

	calls := 0
	restoreProcessHooks(t)
	processCommandStarter = func(cmd *exec.Cmd) error {
		if cmd.SysProcAttr == nil || !cmd.SysProcAttr.UseCgroupFD {
			t.Fatal("starter called without UseCgroupFD")
		}
		calls++
		return syscall.ENOSYS
	}

	_, err := LaunchInCgroup(context.Background(), member, nil, nil, cgroupFD)
	if !errors.Is(err, ErrCloneIntoCgroupUnsupported) {
		t.Fatalf("error = %v, want ErrCloneIntoCgroupUnsupported", err)
	}
	if calls != 1 {
		t.Fatalf("starter calls = %d, want one clone3 attempt and no fallback", calls)
	}
}

func TestLaunchRejectsChildOutsideRequestedCgroupAndCleansProcess(t *testing.T) {
	root := t.TempDir()
	executablePath := filepath.Join(root, "runtime", "buildkitd")
	writeExecutableFixture(t, executablePath, "#!/bin/sh\nexit 0\n")
	member := ExecutableMember{Path: executablePath, SHA256: sha256FileHex(t, executablePath)}
	cgroupDir := filepath.Join(root, "cgroup")
	if err := os.Mkdir(cgroupDir, 0o755); err != nil {
		t.Fatalf("Mkdir(%q) error = %v", cgroupDir, err)
	}
	cgroupFD := openDirectoryFD(t, cgroupDir)
	defer syscall.Close(cgroupFD)
	cgroupIdentity := identityFromStat(mustFstat(t, cgroupFD))

	killed := 0
	restoreProcessHooks(t)
	processCommandStarter = func(cmd *exec.Cmd) error {
		cmd.Process, _ = os.FindProcess(999998)
		return nil
	}
	processCgroupIdentity = func(pid int) (fileIdentity, error) {
		return fileIdentity{dev: cgroupIdentity.dev, ino: cgroupIdentity.ino + 1, uid: cgroupIdentity.uid, mode: cgroupIdentity.mode}, nil
	}
	processCommandKiller = func(*os.Process) error {
		killed++
		return nil
	}
	processCommandWaiter = func(*exec.Cmd) error { return nil }

	_, err := LaunchInCgroup(context.Background(), member, nil, nil, cgroupFD)
	if err == nil {
		t.Fatal("LaunchInCgroup() succeeded, want child cgroup mismatch")
	}
	if killed != 1 {
		t.Fatalf("killed = %d, want cleanup kill for mismatched cgroup", killed)
	}
}

func TestLaunchRefusesInheritedPathAndMalformedEnvironmentEntries(t *testing.T) {
	root := t.TempDir()
	executablePath := filepath.Join(root, "runtime", "buildctl")
	writeExecutableFixture(t, executablePath, "#!/bin/sh\nexit 0\n")
	member := ExecutableMember{Path: executablePath, SHA256: sha256FileHex(t, executablePath)}
	cgroupDir := filepath.Join(root, "cgroup")
	if err := os.Mkdir(cgroupDir, 0o755); err != nil {
		t.Fatalf("Mkdir(%q) error = %v", cgroupDir, err)
	}
	cgroupFD := openDirectoryFD(t, cgroupDir)
	defer syscall.Close(cgroupFD)

	for _, env := range [][]string{
		{"PATH=/usr/bin"},
		{"HOME=/tmp"},
		{"MALFORMED"},
	} {
		env := env
		t.Run(strings.Join(env, ","), func(t *testing.T) {
			restoreProcessHooks(t)
			processCommandStarter = func(cmd *exec.Cmd) error {
				t.Fatalf("starter called with invalid env %#v", env)
				return nil
			}
			if _, err := LaunchInCgroup(context.Background(), member, nil, env, cgroupFD); err == nil {
				t.Fatal("LaunchInCgroup() succeeded, want inherited environment rejection")
			}
		})
	}
}

func restoreProcessHooks(t *testing.T) {
	t.Helper()
	previousStarter := processCommandStarter
	previousWaiter := processCommandWaiter
	previousKiller := processCommandKiller
	previousIdentity := processCgroupIdentity
	t.Cleanup(func() {
		processCommandStarter = previousStarter
		processCommandWaiter = previousWaiter
		processCommandKiller = previousKiller
		processCgroupIdentity = previousIdentity
	})
}

func identityFromStat(statValue *syscall.Stat_t) fileIdentity {
	return fileIdentity{
		dev:  uint64(statValue.Dev),
		ino:  uint64(statValue.Ino),
		uid:  statValue.Uid,
		mode: os.FileMode(statValue.Mode),
	}
}

func mustReadFile(t *testing.T, path string) []byte {
	t.Helper()
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%q) error = %v", path, err)
	}
	return payload
}

func TestLaunchRefusesRuntimeMemberSymlink(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "target")
	writeExecutableFixture(t, target, "#!/bin/sh\nexit 0\n")
	link := filepath.Join(root, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Fatalf("Symlink(%q, %q) error = %v", target, link, err)
	}
	sum := sha256.Sum256(mustReadFile(t, target))
	member := ExecutableMember{Path: link, SHA256: hex.EncodeToString(sum[:])}
	cgroupFD := openDirectoryFD(t, root)
	defer syscall.Close(cgroupFD)

	restoreProcessHooks(t)
	processCommandStarter = func(cmd *exec.Cmd) error {
		t.Fatal("starter called for symlink member")
		return nil
	}

	if _, err := LaunchInCgroup(context.Background(), member, nil, nil, cgroupFD); err == nil {
		t.Fatal("LaunchInCgroup() succeeded, want symlink rejection")
	}
}
