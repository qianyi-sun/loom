//go:build linux

package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
)

func TestContextLaunchActuallyInheritsPinnedDirectoryAcrossExec(t *testing.T) {
	root := t.TempDir()
	input := filepath.Join(root, "input")
	if err := os.Mkdir(input, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(input, "data"), []byte("verified input"), 0o644); err != nil {
		t.Fatal(err)
	}
	inputFD := openDirectoryFD(t, input)
	defer syscall.Close(inputFD)
	if err := os.Rename(input, input+".original"); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(input, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(input, "data"), []byte("replacement"), 0o644); err != nil {
		t.Fatal(err)
	}
	// A real ELF exec exercises CLOEXEC and ExtraFiles remapping; a shebang
	// fixture cannot exercise an fd-backed CLOEXEC executable on Linux.
	binary, err := os.ReadFile("/bin/cat")
	if err != nil {
		t.Fatal(err)
	}
	executable := filepath.Join(root, "cat")
	if err := os.WriteFile(executable, binary, 0o555); err != nil {
		t.Fatal(err)
	}
	member := ExecutableMember{Path: executable, SHA256: sha256FileHex(t, executable)}
	cgroupFD := openDirectoryFD(t, root)
	defer syscall.Close(cgroupFD)
	restoreProcessHooks(t)
	var output bytes.Buffer
	var inheritedParentFD int
	processCommandStarter = func(cmd *exec.Cmd) error {
		if cmd.SysProcAttr == nil || !cmd.SysProcAttr.UseCgroupFD || cmd.SysProcAttr.CgroupFD != cgroupFD {
			t.Fatal("context launch lost mandatory cgroup placement")
		}
		if len(cmd.ExtraFiles) != 1 {
			t.Fatal("context launch inherited extra authority")
		}
		inheritedParentFD = int(cmd.ExtraFiles[0].Fd())
		if inheritedParentFD == inputFD {
			t.Fatal("launcher borrowed caller FD without duplication")
		}
		// Only the test substitutes cgroup placement; the production launcher
		// has no fallback. This test proves exec/FD mechanics, not containment.
		cmd.SysProcAttr = nil
		cmd.Stdout = &output
		return cmd.Start()
	}
	processCgroupIdentity = func(int) (fileIdentity, error) { return identityFromStat(mustFstat(t, cgroupFD)), nil }
	process, err := LaunchInCgroupWithContext(context.Background(), member,
		[]string{"/proc/self/fd/3/data"}, []string{"LANG=C.UTF-8"}, cgroupFD, inputFD)
	if err != nil {
		t.Fatal(err)
	}
	if err := process.Wait(); err != nil {
		t.Fatal(err)
	}
	if output.String() != "verified input" {
		t.Fatalf("exec consumed %q", output.String())
	}
	if _, err := validateDirectoryDescriptor(inputFD); err != nil {
		t.Fatal("launcher closed caller FD")
	}
	if _, err := validateDirectoryDescriptor(inheritedParentFD); !errors.Is(err, syscall.EBADF) {
		t.Fatal("launcher leaked its parent-side duplicate")
	}
}

func TestContextLaunchRejectsInvalidInputBeforeStarting(t *testing.T) {
	for _, kind := range []string{"missing", "regular", "shared"} {
		t.Run(kind, func(t *testing.T) {
			restoreProcessHooks(t)
			root := t.TempDir()
			writeExecutableFixture(t, filepath.Join(root, "executable"), "test-only-executable")
			member := ExecutableMember{Path: filepath.Join(root, "executable"), SHA256: sha256FileHex(t, filepath.Join(root, "executable"))}
			cgroupFD := openDirectoryFD(t, root)
			defer syscall.Close(cgroupFD)
			validRoot := t.TempDir()
			if err := os.Chmod(validRoot, 0o700); err != nil {
				t.Fatal(err)
			}
			validFD := openDirectoryFD(t, validRoot)
			defer syscall.Close(validFD)
			processCommandStarter = func(*exec.Cmd) error { return syscall.ENOSYS }
			if _, err := LaunchInCgroupWithContext(context.Background(), member, nil, nil, cgroupFD, validFD); !errors.Is(err, ErrCloneIntoCgroupUnsupported) {
				t.Fatalf("invalid baseline: %v", err)
			}
			processCommandStarter = func(*exec.Cmd) error { t.Fatal("started invalid context"); return nil }
			fd := -1
			if kind == "regular" {
				file, err := os.CreateTemp(t.TempDir(), "regular")
				if err != nil {
					t.Fatal(err)
				}
				defer file.Close()
				fd = int(file.Fd())
			} else if kind == "shared" {
				root := t.TempDir()
				if err := os.Chmod(root, 0o755); err != nil {
					t.Fatal(err)
				}
				fd = openDirectoryFD(t, root)
				defer syscall.Close(fd)
			}
			if _, err := LaunchInCgroupWithContext(context.Background(), member, nil, nil, cgroupFD, fd); err == nil {
				t.Fatal("accepted invalid context")
			}
		})
	}
}
