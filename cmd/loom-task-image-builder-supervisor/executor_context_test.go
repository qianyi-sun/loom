package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestExecutorRegisteredContextHandoffIsSeparateOwnedAndPinned(t *testing.T) {
	fixture := newExecutorFixture(t)
	input := filepath.Join(t.TempDir(), "input")
	if err := os.Mkdir(input, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(input, "Dockerfile"), []byte("FROM scratch\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	fd := openDirectoryFD(t, input)
	defer syscall.Close(fd)
	component := BuildComponent{Name: "sidecar:db", ContextDir: ".", Dockerfile: "Dockerfile"}
	plan := BuildPlan{Architecture: "amd64", Components: []BuildComponent{component}}
	executor, err := NewExecutorWithContext(fixture.config, fixture.capabilities, plan, fd)
	if err != nil {
		t.Fatal(err)
	}
	defer executor.Close(context.Background())
	// The constructor owns both the plan slice and the input FD it will lend.
	plan.Components[0].Dockerfile = "changed"
	if err := os.Rename(input, input+".original"); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(input, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(input, "Dockerfile"), []byte("foreign"), 0o644); err != nil {
		t.Fatal(err)
	}
	restoreExecutorHooks(t)
	previous := executorRunBuildctlWithContext
	t.Cleanup(func() { executorRunBuildctlWithContext = previous })
	executorRunBuildctl = func(context.Context, ExecutableMember, []string, []string, int) error {
		t.Fatal("build used pathname-only launch")
		return nil
	}
	executor.started = true // Startup/containment are covered separately.
	var ownedFD int
	executorRunBuildctlWithContext = func(_ context.Context, _ ExecutableMember, argv, _ []string, cgroupFD, inputFD int) error {
		ownedFD = inputFD
		if inputFD == fd || cgroupFD != fixture.capabilities.BuildEgressFD {
			t.Fatal("descriptor ownership changed")
		}
		if !containsAdjacentArgs(argv, "--local", "context=/proc/self/fd/3") ||
			!containsAdjacentArgs(argv, "--local", "dockerfile=/proc/self/fd/3") {
			t.Fatal("buildctl did not receive child descriptor paths")
		}
		data, err := os.ReadFile(fmt.Sprintf("/proc/self/fd/%d/Dockerfile", inputFD))
		if err != nil || string(data) != "FROM scratch\n" {
			t.Fatal("context followed replacement path")
		}
		if err := os.WriteFile(valueAfterArg(t, argv, "--ref-file"), []byte("solve_1-abc"), 0o600); err != nil {
			t.Fatal(err)
		}
		return os.WriteFile(valueAfterArg(t, argv, "--metadata-file"), baseResolutionFixture("linux/amd64", `[]`), 0o600)
	}
	executorValidateOCIOutput = func(path, platform string) (OCIOutput, error) {
		return OCIOutput{Path: path, TopLevelDigest: baseResolutionTestRoot, FileSHA256: strings.Repeat("b", 64), Architecture: "amd64", OS: "linux"}, nil
	}
	if _, err := executor.Build(context.Background(), component); err != nil {
		t.Fatal(err)
	}
	if err := executor.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := validateDirectoryDescriptor(ownedFD); !errors.Is(err, syscall.EBADF) {
		t.Fatal("executor leaked input FD")
	}
	if _, err := validateDirectoryDescriptor(fd); err != nil {
		t.Fatal("executor closed caller FD")
	}
	if _, err := executor.Build(context.Background(), component); err == nil {
		t.Fatal("closed executor fell back to pathname input")
	}
}

func TestExecutorCloseCancelsAndJoinsBeforeClosingInput(t *testing.T) {
	for _, timeout := range []bool{false, true} {
		t.Run(fmt.Sprint(timeout), func(t *testing.T) {
			fixture := newExecutorFixture(t)
			input := t.TempDir()
			if err := os.Chmod(input, 0o700); err != nil {
				t.Fatal(err)
			}
			fd := openDirectoryFD(t, input)
			defer syscall.Close(fd)
			component := BuildComponent{Name: "task", ContextDir: ".", Dockerfile: "Dockerfile"}
			executor, err := NewExecutorWithContext(fixture.config, fixture.capabilities, BuildPlan{Architecture: "amd64", Components: []BuildComponent{component}}, fd)
			if err != nil {
				t.Fatal(err)
			}
			executor.started = true
			previous := executorRunBuildctlWithContext
			t.Cleanup(func() { executorRunBuildctlWithContext = previous })
			started, cancelled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var ownedFD int
			executorRunBuildctlWithContext = func(ctx context.Context, _ ExecutableMember, _, _ []string, _, inputFD int) error {
				ownedFD = inputFD
				close(started)
				<-ctx.Done()
				close(cancelled)
				<-release
				return ctx.Err()
			}
			built := make(chan error, 1)
			go func() { _, err := executor.Build(context.Background(), component); built <- err }()
			<-started
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			closed := make(chan error, 1)
			go func() { closed <- executor.Close(ctx) }()
			select {
			case <-cancelled:
			case <-time.After(time.Second):
				close(release)
				t.Fatal("Close did not cancel build")
			}
			if _, err := validateDirectoryDescriptor(ownedFD); err != nil {
				t.Fatal("input closed before consumer joined")
			}
			if timeout {
				cancel()
				if err := <-closed; err == nil {
					t.Fatal("incomplete join claimed clean close")
				}
				if _, err := validateDirectoryDescriptor(ownedFD); err != nil {
					t.Fatal("join timeout closed active input")
				}
			} else {
				select {
				case <-closed:
					t.Fatal("Close returned before join")
				case <-time.After(20 * time.Millisecond):
				}
			}
			close(release)
			if err := <-built; err == nil {
				t.Fatal("cancelled build accepted")
			}
			if timeout {
				if err := executor.Close(context.Background()); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := <-closed; err != nil {
					t.Fatal(err)
				}
			}
			if _, err := validateDirectoryDescriptor(ownedFD); !errors.Is(err, syscall.EBADF) {
				t.Fatal("joined input not closed")
			}
		})
	}
}

func TestExecutorRejectsComponentSubstitutionWithinNamedPlan(t *testing.T) {
	_, executor, component := newStartedCaptureExecutor(t)
	executorRunBuildctl = func(context.Context, ExecutableMember, []string, []string, int) error {
		t.Fatal("launched substituted build inputs")
		return nil
	}
	for _, mutate := range []func(*BuildComponent){
		func(c *BuildComponent) { c.ContextDir = "other" },
		func(c *BuildComponent) { c.Dockerfile = "other/Dockerfile" },
	} {
		changed := component
		mutate(&changed)
		if _, err := executor.Build(context.Background(), changed); err == nil {
			t.Fatal("accepted component substitution")
		}
	}
}

func TestExecutorConcurrentCloseHasOneDescriptorCleanupOwner(t *testing.T) {
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	fd := openDirectoryFD(t, root)
	defer syscall.Close(fd)
	for i := 0; i < 50; i++ {
		input, err := duplicatePrivateInputDirectory(fd)
		if err != nil {
			t.Fatal(err)
		}
		ownedFD := int(input.Fd())
		executor := &Executor{input: input, inputRequired: true}
		start := make(chan struct{})
		done := make(chan error, 2)
		for n := 0; n < 2; n++ {
			go func() { <-start; done <- executor.Close(context.Background()) }()
		}
		close(start)
		for n := 0; n < 2; n++ {
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		}
		if _, err := validateDirectoryDescriptor(ownedFD); !errors.Is(err, syscall.EBADF) {
			t.Fatal("concurrent close leaked descriptor")
		}
	}
}
