package main

import (
	"archive/tar"
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestExecutorStartUsesExactRootlessKitAndBuildKitFlagsInBuildEgressCgroup(t *testing.T) {
	fixture := newExecutorFixture(t)
	plan := BuildPlan{
		Architecture: "amd64",
		Components: []BuildComponent{
			{Name: "component-a", ContextDir: "bundle/context", Dockerfile: "bundle/context/Dockerfile"},
		},
	}
	executor, err := NewExecutor(fixture.config, fixture.capabilities, plan)
	if err != nil {
		t.Fatalf("NewExecutor() error = %v", err)
	}

	var launchedExecutable ExecutableMember
	var launchedArgv []string
	var launchedEnv []string
	var launchedCgroupFD int
	readinessProbes := 0
	restoreExecutorHooks(t)
	executorVerifyHostIDMapHelpers = func() error { return nil }
	stubBuildkitCgroupParent(t, fixture, "loom-task5-unit")
	executorLaunchInCgroup = func(ctx context.Context, executable ExecutableMember, argv []string, env []string, cgroupFD int) (*Process, error) {
		launchedExecutable = executable
		launchedArgv = append([]string(nil), argv...)
		launchedEnv = append([]string(nil), env...)
		launchedCgroupFD = cgroupFD
		return &Process{PID: 4242, ExecutableSHA256: executable.SHA256, CgroupInode: fixture.capabilities.BuildEgressInode}, nil
	}
	executorRunBuildctl = func(ctx context.Context, executable ExecutableMember, argv []string, env []string, cgroupFD int) error {
		readinessProbes++
		if executable.Path != fixture.config.Runtime.Buildctl.Path {
			t.Fatalf("readiness executable = %q, want buildctl", executable.Path)
		}
		if cgroupFD != fixture.capabilities.BuildEgressFD {
			t.Fatalf("readiness cgroup fd = %d, want %d", cgroupFD, fixture.capabilities.BuildEgressFD)
		}
		if !reflect.DeepEqual(argv, []string{"--addr", executor.buildkitAddress, "debug", "workers"}) {
			t.Fatalf("readiness argv = %#v", argv)
		}
		return nil
	}

	if err := executor.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if readinessProbes != 1 {
		t.Fatalf("readiness probes = %d, want one fail-closed buildctl probe", readinessProbes)
	}
	if launchedExecutable.Path != fixture.config.Runtime.RootlessKit.Path {
		t.Fatalf("launched executable = %q, want rootlesskit", launchedExecutable.Path)
	}
	if launchedCgroupFD != fixture.capabilities.BuildEgressFD {
		t.Fatalf("launched cgroup fd = %d, want build-egress fd %d", launchedCgroupFD, fixture.capabilities.BuildEgressFD)
	}
	required := []string{
		"--net=slirp4netns",
		"--disable-host-loopback",
		"--ipv6",
		"--slirp4netns-sandbox=auto",
		"--slirp4netns-seccomp=auto",
		"--slirp4netns-binary=" + fixture.config.Runtime.Slirp4netns.Path,
		"--",
		fixture.config.Runtime.Buildkitd.Path,
		"--config",
		"--rootless",
		"--oci-worker=true",
		"--oci-worker-rootless",
		"--containerd-worker=false",
		"--cdi-disabled",
		"--oci-worker-snapshotter=fuse-overlayfs",
		"--oci-worker-binary=" + fixture.config.Runtime.BuildkitRunc.Path,
		"--oci-worker-no-process-sandbox=false",
		"--root",
		filepath.Join(fixture.jobRoot, "buildkit-root"),
	}
	for _, want := range required {
		if want == "--config" {
			if valueAfterArg(t, launchedArgv, want) == "" {
				t.Fatalf("rootless/buildkit argv missing config value in %#v", launchedArgv)
			}
			continue
		}
		if !containsString(launchedArgv, want) {
			t.Fatalf("rootless/buildkit argv missing %q in %#v", want, launchedArgv)
		}
	}
	configPath := valueAfterArg(t, launchedArgv, "--config")
	configPayload, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("ReadFile(buildkit config %q) error = %v", configPath, err)
	}
	if string(configPayload) != "[worker.oci]\ndefaultCgroupParent = \"loom-task5-unit\"\n" {
		t.Fatalf("buildkit config = %q", string(configPayload))
	}
	forbidden := []string{
		"security.insecure",
		"network.host",
		"docker.sock",
		"containerd.sock",
		"--allow-insecure-entitlement",
		"--oci-worker-no-process-sandbox=true",
	}
	for _, fragment := range forbidden {
		if strings.Contains(strings.Join(launchedArgv, " "), fragment) {
			t.Fatalf("rootless/buildkit argv contains forbidden fragment %q: %#v", fragment, launchedArgv)
		}
	}
	if len(launchedEnv) == 0 {
		t.Fatal("Start() launched with empty fixed environment")
	}
	envMap := map[string]string{}
	for _, entry := range launchedEnv {
		if strings.HasPrefix(entry, "PATH=") || strings.HasPrefix(entry, "DOCKER_HOST=") || strings.HasPrefix(entry, "CONTAINERD_ADDRESS=") {
			t.Fatalf("Start() inherited forbidden environment entry %q", entry)
		}
		name, value, found := strings.Cut(entry, "=")
		if found {
			envMap[name] = value
		}
	}
	if envMap["BUILDKIT_FUSE_OVERLAYFS_BINARY"] != fixture.config.Runtime.FuseOverlayFS.Path {
		t.Fatalf("BUILDKIT_FUSE_OVERLAYFS_BINARY = %q, want exact content-addressed fuse-overlayfs path %q", envMap["BUILDKIT_FUSE_OVERLAYFS_BINARY"], fixture.config.Runtime.FuseOverlayFS.Path)
	}
}

func TestExecutorRejectsPolicyEscapeMutations(t *testing.T) {
	fixture := newExecutorFixture(t)
	valid := BuildPlan{
		Architecture: "amd64",
		Components: []BuildComponent{
			{Name: "component-a", ContextDir: "bundle/context", Dockerfile: "bundle/context/Dockerfile"},
		},
	}
	tests := []struct {
		name   string
		mutate func(*BuildPlan)
	}{
		{name: "unsupported architecture", mutate: func(plan *BuildPlan) { plan.Architecture = "ppc64le" }},
		{name: "remote frontend", mutate: func(plan *BuildPlan) { plan.Frontend = "gateway.v0" }},
		{name: "insecure entitlement", mutate: func(plan *BuildPlan) { plan.AllowInsecureEntitlements = true }},
		{name: "host network", mutate: func(plan *BuildPlan) { plan.NetworkMode = "host" }},
		{name: "device", mutate: func(plan *BuildPlan) { plan.Devices = []string{"/dev/kvm"} }},
		{name: "cdi", mutate: func(plan *BuildPlan) { plan.CDIDevices = []string{"vendor.com/device=all"} }},
		{name: "ssh", mutate: func(plan *BuildPlan) { plan.SSHForwarding = []string{"default"} }},
		{name: "cache import", mutate: func(plan *BuildPlan) { plan.CacheImports = []string{"type=registry,ref=example.invalid/cache"} }},
		{name: "cache export", mutate: func(plan *BuildPlan) { plan.CacheExports = []string{"type=registry,ref=example.invalid/cache"} }},
		{name: "arbitrary bind", mutate: func(plan *BuildPlan) { plan.Binds = []string{"/var/run/docker.sock:/var/run/docker.sock"} }},
		{name: "path escape context", mutate: func(plan *BuildPlan) { plan.Components[0].ContextDir = "../context" }},
		{name: "path escape dockerfile", mutate: func(plan *BuildPlan) { plan.Components[0].Dockerfile = "/etc/passwd" }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan := valid
			plan.Components = append([]BuildComponent(nil), valid.Components...)
			tt.mutate(&plan)
			if _, err := NewExecutor(fixture.config, fixture.capabilities, plan); err == nil {
				t.Fatal("NewExecutor() succeeded, want policy rejection")
			}
		})
	}
}

func TestExecutorBuildCallsPinnedBuildctlWithBuiltinDockerfileAndOCIOutputBelowJob(t *testing.T) {
	fixture := newExecutorFixture(t)
	component := BuildComponent{Name: "component-a", ContextDir: "bundle/context", Dockerfile: "bundle/context/Dockerfile"}
	executor, err := NewExecutor(fixture.config, fixture.capabilities, BuildPlan{
		Architecture: "amd64",
		Components:   []BuildComponent{component},
	})
	if err != nil {
		t.Fatalf("NewExecutor() error = %v", err)
	}

	restoreExecutorHooks(t)
	executorVerifyHostIDMapHelpers = func() error { return nil }
	stubBuildkitCgroupParent(t, fixture, "loom-task5-unit")
	executorLaunchInCgroup = func(ctx context.Context, executable ExecutableMember, argv []string, env []string, cgroupFD int) (*Process, error) {
		return &Process{PID: 4242, ExecutableSHA256: executable.SHA256, CgroupInode: fixture.capabilities.BuildEgressInode}, nil
	}
	executorRunBuildctl = func(context.Context, ExecutableMember, []string, []string, int) error { return nil }
	if err := executor.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	var ranExecutable ExecutableMember
	var ranArgv []string
	var ranCgroupFD int
	executorRunBuildctl = func(ctx context.Context, executable ExecutableMember, argv []string, env []string, cgroupFD int) error {
		ranExecutable = executable
		ranArgv = append([]string(nil), argv...)
		ranCgroupFD = cgroupFD
		return nil
	}
	executorValidateOCIOutput = func(path string, platform string) (OCIOutput, error) {
		if !strings.HasPrefix(path, fixture.jobRoot+string(os.PathSeparator)) {
			t.Fatalf("OCI output path %q outside job root %q", path, fixture.jobRoot)
		}
		if platform != "linux/amd64" {
			t.Fatalf("platform = %q, want linux/amd64", platform)
		}
		return OCIOutput{Path: path, TopLevelDigest: "sha256:" + strings.Repeat("a", 64), FileSHA256: strings.Repeat("b", 64), Architecture: "amd64", OS: "linux"}, nil
	}

	output, err := executor.Build(context.Background(), component)
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	if output.TopLevelDigest != "sha256:"+strings.Repeat("a", 64) {
		t.Fatalf("TopLevelDigest = %q", output.TopLevelDigest)
	}
	if ranExecutable.Path != fixture.config.Runtime.Buildctl.Path {
		t.Fatalf("ran executable = %q, want buildctl", ranExecutable.Path)
	}
	if ranCgroupFD != fixture.capabilities.BuildEgressFD {
		t.Fatalf("build cgroup fd = %d, want build-egress fd %d", ranCgroupFD, fixture.capabilities.BuildEgressFD)
	}
	required := []string{
		"--addr", executor.buildkitAddress,
		"build",
		"--no-cache",
		"--frontend", "dockerfile.v0",
		"--local", "context=" + filepath.Join(fixture.jobRoot, component.ContextDir),
		"--local", "dockerfile=" + filepath.Join(fixture.jobRoot, filepath.Dir(component.Dockerfile)),
		"--opt", "filename=" + filepath.Base(component.Dockerfile),
		"--opt", "platform=linux/amd64",
		"--output", "type=oci,dest=" + filepath.Join(fixture.jobRoot, "oci", component.Name+".tar"),
	}
	if !reflect.DeepEqual(ranArgv, required) {
		t.Fatalf("buildctl argv = %#v, want %#v", ranArgv, required)
	}
	forbidden := []string{"--import-cache", "--export-cache", "--ssh", "--allow", "network.host", "security.insecure"}
	for _, fragment := range forbidden {
		if strings.Contains(strings.Join(ranArgv, " "), fragment) {
			t.Fatalf("buildctl argv contains forbidden %q: %#v", fragment, ranArgv)
		}
	}
}

func TestExecutorCloseTerminatesDaemonAndRejectsSurvivors(t *testing.T) {
	fixture := newExecutorFixture(t)
	executor, err := NewExecutor(fixture.config, fixture.capabilities, BuildPlan{
		Architecture: "amd64",
		Components: []BuildComponent{
			{Name: "component-a", ContextDir: "bundle/context", Dockerfile: "bundle/context/Dockerfile"},
		},
	})
	if err != nil {
		t.Fatalf("NewExecutor() error = %v", err)
	}

	terminated := false
	cgroupChildrenCleaned := false
	restoreExecutorHooks(t)
	executorVerifyHostIDMapHelpers = func() error { return nil }
	stubBuildkitCgroupParent(t, fixture, "loom-task5-unit")
	executorLaunchInCgroup = func(ctx context.Context, executable ExecutableMember, argv []string, env []string, cgroupFD int) (*Process, error) {
		return &Process{PID: 4343, ExecutableSHA256: executable.SHA256, CgroupInode: fixture.capabilities.BuildEgressInode}, nil
	}
	executorRunBuildctl = func(context.Context, ExecutableMember, []string, []string, int) error { return nil }
	executorSignalProcess = func(process *Process, signal os.Signal) error {
		if signal == syscall.SIGTERM {
			terminated = true
		}
		return nil
	}
	executorWaitProcess = func(*Process) error { return commandExitStatus(t, 1) }
	executorCgroupEmpty = func(fd int) (bool, error) {
		return terminated && fd == fixture.capabilities.BuildEgressFD, nil
	}
	executorCleanupCgroup = func(fd int) error {
		if fd != fixture.capabilities.BuildEgressFD {
			t.Fatalf("cleanup cgroup fd = %d, want %d", fd, fixture.capabilities.BuildEgressFD)
		}
		cgroupChildrenCleaned = true
		return nil
	}

	if err := executor.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if err := executor.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if !terminated {
		t.Fatal("Close() did not send SIGTERM")
	}
	if !cgroupChildrenCleaned {
		t.Fatal("Close() did not remove empty child cgroups")
	}

	executor, err = NewExecutor(fixture.config, fixture.capabilities, BuildPlan{
		Architecture: "amd64",
		Components: []BuildComponent{
			{Name: "component-a", ContextDir: "bundle/context", Dockerfile: "bundle/context/Dockerfile"},
		},
	})
	if err != nil {
		t.Fatalf("NewExecutor() error = %v", err)
	}
	executorVerifyHostIDMapHelpers = func() error { return nil }
	stubBuildkitCgroupParent(t, fixture, "loom-task5-unit")
	executorLaunchInCgroup = func(ctx context.Context, executable ExecutableMember, argv []string, env []string, cgroupFD int) (*Process, error) {
		return &Process{PID: 4444, ExecutableSHA256: executable.SHA256, CgroupInode: fixture.capabilities.BuildEgressInode}, nil
	}
	executorRunBuildctl = func(context.Context, ExecutableMember, []string, []string, int) error { return nil }
	executorWaitProcess = func(*Process) error { return nil }
	executorCgroupEmpty = func(fd int) (bool, error) { return false, nil }
	executorShutdownTimeout = time.Millisecond
	executorShutdownPoll = time.Millisecond
	if err := executor.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if err := executor.Close(context.Background()); err == nil {
		t.Fatal("Close() succeeded, want surviving process cleanup failure")
	}
}

func TestExecutorCloseRejectsUnexpectedDaemonExitStatus(t *testing.T) {
	fixture := newExecutorFixture(t)
	executor, err := NewExecutor(fixture.config, fixture.capabilities, BuildPlan{
		Architecture: "amd64",
		Components: []BuildComponent{
			{Name: "component-a", ContextDir: "bundle/context", Dockerfile: "bundle/context/Dockerfile"},
		},
	})
	if err != nil {
		t.Fatalf("NewExecutor() error = %v", err)
	}

	restoreExecutorHooks(t)
	executorVerifyHostIDMapHelpers = func() error { return nil }
	stubBuildkitCgroupParent(t, fixture, "loom-task5-unit")
	executorLaunchInCgroup = func(ctx context.Context, executable ExecutableMember, argv []string, env []string, cgroupFD int) (*Process, error) {
		return &Process{PID: 4445, ExecutableSHA256: executable.SHA256, CgroupInode: fixture.capabilities.BuildEgressInode}, nil
	}
	executorRunBuildctl = func(context.Context, ExecutableMember, []string, []string, int) error { return nil }
	executorSignalProcess = func(*Process, os.Signal) error { return nil }
	executorWaitProcess = func(*Process) error { return commandExitStatus(t, 2) }

	if err := executor.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if err := executor.Close(context.Background()); err == nil {
		t.Fatal("Close() accepted unexpected daemon exit status")
	}
}

func TestExecutorStartFailsClosedWhenDaemonReadinessNeverArrives(t *testing.T) {
	fixture := newExecutorFixture(t)
	executor, err := NewExecutor(fixture.config, fixture.capabilities, BuildPlan{
		Architecture: "amd64",
		Components: []BuildComponent{
			{Name: "component-a", ContextDir: "bundle/context", Dockerfile: "bundle/context/Dockerfile"},
		},
	})
	if err != nil {
		t.Fatalf("NewExecutor() error = %v", err)
	}

	signals := []os.Signal{}
	restoreExecutorHooks(t)
	executorVerifyHostIDMapHelpers = func() error { return nil }
	stubBuildkitCgroupParent(t, fixture, "loom-task5-unit")
	executorLaunchInCgroup = func(ctx context.Context, executable ExecutableMember, argv []string, env []string, cgroupFD int) (*Process, error) {
		return &Process{PID: 4545, ExecutableSHA256: executable.SHA256, CgroupInode: fixture.capabilities.BuildEgressInode}, nil
	}
	executorRunBuildctl = func(context.Context, ExecutableMember, []string, []string, int) error {
		return errors.New("daemon unavailable")
	}
	executorProcessAlive = func(*Process) bool { return false }
	executorSignalProcess = func(process *Process, signal os.Signal) error {
		signals = append(signals, signal)
		return nil
	}
	executorWaitProcess = func(*Process) error { return nil }
	executorCgroupEmpty = func(fd int) (bool, error) { return true, nil }
	executorReadinessTimeout = time.Millisecond
	executorReadinessPoll = time.Millisecond

	if err := executor.Start(context.Background()); err == nil {
		t.Fatal("Start() succeeded, want readiness failure")
	}
	if executor.started || executor.daemon != nil {
		t.Fatal("Start() left daemon marked started after readiness failure")
	}
	if len(signals) == 0 || signals[0] != syscall.SIGTERM {
		t.Fatalf("signals = %#v, want SIGTERM cleanup on readiness failure", signals)
	}
}

func TestExecutorStartFailureRemovesPartialStateBeforeLaunch(t *testing.T) {
	fixture := newExecutorFixture(t)
	executor, err := NewExecutor(fixture.config, fixture.capabilities, BuildPlan{
		Architecture: "amd64",
		Components: []BuildComponent{
			{Name: "component-a", ContextDir: "bundle/context", Dockerfile: "bundle/context/Dockerfile"},
		},
	})
	if err != nil {
		t.Fatalf("NewExecutor() error = %v", err)
	}

	restoreExecutorHooks(t)
	executorVerifyHostIDMapHelpers = func() error { return errors.New("idmap helper preflight failed") }

	if err := executor.Start(context.Background()); err == nil {
		t.Fatal("Start() succeeded, want preflight failure")
	}
	if executor.started || executor.daemon != nil {
		t.Fatal("Start() left daemon state after preflight failure")
	}
	assertExecutorStateRemoved(t, fixture.jobRoot)
}

func TestExecutorCloseAggregatesErrorsAndStillCleansState(t *testing.T) {
	fixture := newExecutorFixture(t)
	executor, err := NewExecutor(fixture.config, fixture.capabilities, BuildPlan{
		Architecture: "amd64",
		Components: []BuildComponent{
			{Name: "component-a", ContextDir: "bundle/context", Dockerfile: "bundle/context/Dockerfile"},
		},
	})
	if err != nil {
		t.Fatalf("NewExecutor() error = %v", err)
	}

	restoreExecutorHooks(t)
	executorVerifyHostIDMapHelpers = func() error { return nil }
	stubBuildkitCgroupParent(t, fixture, "loom-task5-unit")
	executorLaunchInCgroup = func(ctx context.Context, executable ExecutableMember, argv []string, env []string, cgroupFD int) (*Process, error) {
		return &Process{PID: 4646, ExecutableSHA256: executable.SHA256, CgroupInode: fixture.capabilities.BuildEgressInode}, nil
	}
	executorRunBuildctl = func(context.Context, ExecutableMember, []string, []string, int) error { return nil }

	signalCalls := 0
	waitCalls := 0
	cgroupCalls := 0
	cleanupCalls := 0
	executorSignalProcess = func(*Process, os.Signal) error {
		signalCalls++
		return errors.New("signal failed")
	}
	executorWaitProcess = func(*Process) error {
		waitCalls++
		return errors.New("wait failed")
	}
	executorCgroupEmpty = func(int) (bool, error) {
		cgroupCalls++
		return false, errors.New("cgroup empty check failed")
	}
	executorCleanupCgroup = func(int) error {
		cleanupCalls++
		return errors.New("child cgroup cleanup failed")
	}

	if err := executor.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	err = executor.Close(context.Background())
	if err == nil {
		t.Fatal("Close() succeeded, want aggregated cleanup errors")
	}
	for _, want := range []string{"signal failed", "wait failed", "cgroup empty check failed", "child cgroup cleanup failed"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("Close() error %q missing %q", err.Error(), want)
		}
	}
	if signalCalls != 1 || waitCalls != 1 || cgroupCalls != 1 || cleanupCalls != 1 {
		t.Fatalf("cleanup calls signal=%d wait=%d cgroup=%d cleanup=%d, want all once", signalCalls, waitCalls, cgroupCalls, cleanupCalls)
	}
	if executor.started || executor.daemon != nil {
		t.Fatal("Close() left executor marked started after cleanup failure")
	}
	assertExecutorStateRemoved(t, fixture.jobRoot)
}

func TestExecutorBuildFailureRemovesPartialOCIOutput(t *testing.T) {
	fixture := newExecutorFixture(t)
	component := BuildComponent{Name: "component-a", ContextDir: "bundle/context", Dockerfile: "bundle/context/Dockerfile"}
	executor, err := NewExecutor(fixture.config, fixture.capabilities, BuildPlan{
		Architecture: "amd64",
		Components:   []BuildComponent{component},
	})
	if err != nil {
		t.Fatalf("NewExecutor() error = %v", err)
	}

	restoreExecutorHooks(t)
	executorVerifyHostIDMapHelpers = func() error { return nil }
	stubBuildkitCgroupParent(t, fixture, "loom-task5-unit")
	executorLaunchInCgroup = func(ctx context.Context, executable ExecutableMember, argv []string, env []string, cgroupFD int) (*Process, error) {
		return &Process{PID: 4747, ExecutableSHA256: executable.SHA256, CgroupInode: fixture.capabilities.BuildEgressInode}, nil
	}
	executorRunBuildctl = func(context.Context, ExecutableMember, []string, []string, int) error { return nil }
	if err := executor.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	outputPath := filepath.Join(fixture.jobRoot, "oci", component.Name+".tar")
	executorRunBuildctl = func(context.Context, ExecutableMember, []string, []string, int) error {
		if err := os.WriteFile(outputPath, []byte("partial build output\n"), 0o600); err != nil {
			t.Fatalf("WriteFile(%q) error = %v", outputPath, err)
		}
		return errors.New("buildctl failed")
	}
	if _, err := executor.Build(context.Background(), component); err == nil {
		t.Fatal("Build() succeeded, want buildctl failure")
	}
	if _, err := os.Stat(outputPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("partial OCI output survived buildctl failure: %v", err)
	}

	executorRunBuildctl = func(context.Context, ExecutableMember, []string, []string, int) error {
		if err := os.WriteFile(outputPath, []byte("invalid oci tar\n"), 0o600); err != nil {
			t.Fatalf("WriteFile(%q) error = %v", outputPath, err)
		}
		return nil
	}
	executorValidateOCIOutput = func(string, string) (OCIOutput, error) {
		return OCIOutput{}, errors.New("oci validation failed")
	}
	if _, err := executor.Build(context.Background(), component); err == nil {
		t.Fatal("Build() succeeded, want OCI validation failure")
	}
	if _, err := os.Stat(outputPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("partial OCI output survived validation failure: %v", err)
	}
}

func TestBuildkitSocketPathEnforcesLinuxSocketLimit(t *testing.T) {
	suffix := string(os.PathSeparator) + filepath.Join("buildkit", "buildkitd.sock")
	rootLen := maxBuildkitUnixSocketPathBytes - len(suffix)
	if rootLen <= 1 {
		t.Fatalf("invalid socket path test geometry: rootLen=%d suffix=%q", rootLen, suffix)
	}
	boundaryRoot := string(os.PathSeparator) + strings.Repeat("a", rootLen-1)
	path, err := buildkitSocketPath(boundaryRoot)
	if err != nil {
		t.Fatalf("buildkitSocketPath(boundary) error = %v", err)
	}
	if len(path) != maxBuildkitUnixSocketPathBytes {
		t.Fatalf("socket path length = %d, want %d", len(path), maxBuildkitUnixSocketPathBytes)
	}
	if _, err := buildkitSocketPath(boundaryRoot + "x"); err == nil {
		t.Fatal("buildkitSocketPath accepted overlength unix socket path")
	}
}

func TestNewExecutorRejectsOverlengthBuildkitSocketPathBeforeLaunch(t *testing.T) {
	fixture := newExecutorFixture(t)
	longJobRoot := filepath.Join(fixture.root, strings.Repeat("j", maxBuildkitUnixSocketPathBytes))
	if err := os.Mkdir(longJobRoot, 0o700); err != nil {
		t.Fatalf("Mkdir(%q) error = %v", longJobRoot, err)
	}
	longJobFD := openDirectoryFD(t, longJobRoot)
	defer syscall.Close(longJobFD)
	longJobStat := mustFstat(t, longJobFD)
	caps := *fixture.capabilities
	caps.JobDirectoryFD = longJobFD
	caps.JobDirectoryDevice = uint64(longJobStat.Dev)
	caps.JobDirectoryInode = uint64(longJobStat.Ino)

	if _, err := NewExecutor(fixture.config, &caps, BuildPlan{
		Architecture: "amd64",
		Components: []BuildComponent{
			{Name: "component-a", ContextDir: "bundle/context", Dockerfile: "bundle/context/Dockerfile"},
		},
	}); err == nil {
		t.Fatal("NewExecutor accepted overlength BuildKit unix socket path")
	}
}

func TestExecutorWriteBuildkitConfigUsesExclusiveNoFollowCreation(t *testing.T) {
	fixture := newExecutorFixture(t)
	executor := &Executor{jobRoot: fixture.jobRoot, capabilities: fixture.capabilities}
	buildkitDir := filepath.Join(fixture.jobRoot, "buildkit")
	if err := os.Mkdir(buildkitDir, 0o700); err != nil {
		t.Fatalf("Mkdir(%q) error = %v", buildkitDir, err)
	}
	configPath := filepath.Join(buildkitDir, "buildkitd.toml")
	original := []byte("do-not-overwrite\n")
	if err := os.WriteFile(configPath, original, 0o600); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", configPath, err)
	}
	if _, err := executor.writeBuildkitConfig("loom-task5-unit"); err == nil {
		t.Fatal("writeBuildkitConfig accepted existing config file")
	}
	if got := mustReadFile(t, configPath); !bytes.Equal(got, original) {
		t.Fatalf("existing config overwritten: %q", got)
	}

	if err := os.Remove(configPath); err != nil {
		t.Fatalf("Remove(%q) error = %v", configPath, err)
	}
	outside := filepath.Join(fixture.root, "outside-buildkitd.toml")
	if err := os.WriteFile(outside, []byte("outside\n"), 0o600); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", outside, err)
	}
	if err := os.Symlink(outside, configPath); err != nil {
		t.Fatalf("Symlink(%q, %q) error = %v", outside, configPath, err)
	}
	if _, err := executor.writeBuildkitConfig("loom-task5-unit"); err == nil {
		t.Fatal("writeBuildkitConfig followed symlink config path")
	}
	if got := string(mustReadFile(t, outside)); got != "outside\n" {
		t.Fatalf("outside target overwritten through symlink: %q", got)
	}
}

func TestNativeFixtureArchitectureAllowsNativeLinuxAMD64AndARM64(t *testing.T) {
	tests := []struct {
		goos   string
		goarch string
		want   bool
	}{
		{goos: "linux", goarch: "amd64", want: true},
		{goos: "linux", goarch: "arm64", want: true},
		{goos: "linux", goarch: "ppc64le", want: false},
		{goos: "darwin", goarch: "arm64", want: false},
	}
	for _, tt := range tests {
		if got := nativeFixtureArchitectureSupported(tt.goos, tt.goarch); got != tt.want {
			t.Fatalf("nativeFixtureArchitectureSupported(%q, %q) = %v, want %v", tt.goos, tt.goarch, got, tt.want)
		}
	}
}

func TestExecutorCloseLeavesBorrowedCapabilityFDsOpen(t *testing.T) {
	fixture := newExecutorFixture(t)
	executor, err := NewExecutor(fixture.config, fixture.capabilities, BuildPlan{
		Architecture: "amd64",
		Components: []BuildComponent{
			{Name: "component-a", ContextDir: "bundle/context", Dockerfile: "bundle/context/Dockerfile"},
		},
	})
	if err != nil {
		t.Fatalf("NewExecutor() error = %v", err)
	}

	restoreExecutorHooks(t)
	executorVerifyHostIDMapHelpers = func() error { return nil }
	stubBuildkitCgroupParent(t, fixture, "loom-task5-unit")
	executorLaunchInCgroup = func(ctx context.Context, executable ExecutableMember, argv []string, env []string, cgroupFD int) (*Process, error) {
		return &Process{PID: 4848, ExecutableSHA256: executable.SHA256, CgroupInode: fixture.capabilities.BuildEgressInode}, nil
	}
	executorRunBuildctl = func(context.Context, ExecutableMember, []string, []string, int) error { return nil }
	executorSignalProcess = func(*Process, os.Signal) error { return nil }
	executorWaitProcess = func(*Process) error { return nil }
	executorCgroupEmpty = func(int) (bool, error) { return true, nil }
	executorCleanupCgroup = func(int) error { return nil }

	if err := executor.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if err := executor.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if _, err := validateDirectoryDescriptor(fixture.capabilities.JobDirectoryFD); err != nil {
		t.Fatalf("borrowed job fd was closed by executor: %v", err)
	}
	if _, err := validateDirectoryDescriptor(fixture.capabilities.BuildEgressFD); err != nil {
		t.Fatalf("borrowed cgroup fd was closed by executor: %v", err)
	}
}

func TestExecutorHostIDMapHelpersUseFixedRootOwnedSetIDPaths(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("root is required to create root-owned setid helper fixtures")
	}
	if hostNewuidmapPath != "/usr/bin/newuidmap" || hostNewgidmapPath != "/usr/bin/newgidmap" {
		t.Fatalf("idmap helper paths changed: uid=%q gid=%q", hostNewuidmapPath, hostNewgidmapPath)
	}
	if hostNsenterPath != "/usr/bin/nsenter" || hostIPPath != "/usr/bin/ip" {
		t.Fatalf("network helper paths changed: nsenter=%q ip=%q", hostNsenterPath, hostIPPath)
	}
	root := t.TempDir()
	uidmap := filepath.Join(root, "usr", "bin", "newuidmap")
	gidmap := filepath.Join(root, "usr", "bin", "newgidmap")
	nsenter := filepath.Join(root, "usr", "bin", "nsenter")
	ip := filepath.Join(root, "usr", "bin", "ip")
	writeExecutableFixture(t, uidmap, "#!/bin/sh\nexit 0\n")
	writeExecutableFixture(t, gidmap, "#!/bin/sh\nexit 0\n")
	writeExecutableFixture(t, nsenter, "#!/bin/sh\nexit 0\n")
	writeExecutableFixture(t, ip, "#!/bin/sh\nexit 0\n")
	for _, path := range []string{uidmap, gidmap, nsenter, ip} {
		if err := os.Chown(path, 0, 0); err != nil {
			t.Fatalf("Chown(%q) error = %v", path, err)
		}
	}
	if err := os.Chmod(uidmap, 0o755|os.ModeSetuid); err != nil {
		t.Fatalf("Chmod(%q) error = %v", uidmap, err)
	}
	if err := os.Chmod(gidmap, 0o755|os.ModeSetuid); err != nil {
		t.Fatalf("Chmod(%q) error = %v", gidmap, err)
	}

	if err := verifyHostIDMapHelper(uidmap, syscall.S_ISUID); err != nil {
		t.Fatalf("verifyHostIDMapHelper(newuidmap) error = %v", err)
	}
	if err := verifyHostIDMapHelper(gidmap, syscall.S_ISUID); err != nil {
		t.Fatalf("verifyHostIDMapHelper(newgidmap) error = %v", err)
	}
	if err := verifyHostIDMapHelper(nsenter, 0); err != nil {
		t.Fatalf("verifyHostIDMapHelper(nsenter) error = %v", err)
	}
	if err := verifyHostIDMapHelper(ip, 0); err != nil {
		t.Fatalf("verifyHostIDMapHelper(ip) error = %v", err)
	}
	if err := verifyHostIDMapHelper(filepath.Join(root, "usr", "bin", "newuidmap"), syscall.S_ISGID); err == nil {
		t.Fatal("verifyHostIDMapHelper() accepted uid helper without required setgid bit")
	}
	if err := os.Chmod(nsenter, 0o644); err != nil {
		t.Fatalf("Chmod(%q) error = %v", nsenter, err)
	}
	if err := verifyHostIDMapHelper(nsenter, 0); err == nil {
		t.Fatal("verifyHostIDMapHelper() accepted non-executable nsenter helper")
	}
}

func TestNativeBuildFixtureExecutesRootlessBuildKitInExactCgroup(t *testing.T) {
	if !nativeFixtureArchitectureSupported(runtime.GOOS, runtime.GOARCH) {
		t.Skip("native rootless BuildKit fixture is only asserted on native linux/amd64 and linux/arm64")
	}
	runtimeRoot := os.Getenv("LOOM_TASK_IMAGE_BUILDER_NATIVE_RUNTIME")
	if runtimeRoot == "" {
		t.Skip("LOOM_TASK_IMAGE_BUILDER_NATIVE_RUNTIME not set; exact seven-member rootless runtime fixture unavailable")
	}
	runtimeRoot = filepath.Clean(runtimeRoot)
	runtimeMembers := loadNativeRuntimeMembers(t, runtimeRoot, runtime.GOARCH)
	requireNativeRootlessPrerequisites(t)

	jobParent, err := os.MkdirTemp("/tmp", "lt5-")
	if err != nil {
		t.Fatalf("MkdirTemp(/tmp/lt5-) error = %v", err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(jobParent); err != nil {
			t.Fatalf("RemoveAll(%q) error = %v", jobParent, err)
		}
	})
	jobRoot := filepath.Join(jobParent, "job")
	if err := os.Mkdir(jobRoot, 0o700); err != nil {
		t.Fatalf("Mkdir(%q) error = %v", jobRoot, err)
	}
	component := BuildComponent{Name: "component-a", ContextDir: "bundle/context", Dockerfile: "bundle/context/Dockerfile"}
	writeNativeBuildContext(t, jobRoot, component)

	cgroupPath, cgroupFD := createNativeTestCgroup(t)
	jobFD := openDirectoryFD(t, jobRoot)
	t.Cleanup(func() {
		syscall.Close(jobFD)
	})
	jobStat := mustFstat(t, jobFD)
	cgroupStat := mustFstat(t, cgroupFD)
	cfg := Config{
		ReleaseSHA256: strings.Repeat("a", 64),
		CPUArch:       runtime.GOARCH,
		Runtime: RuntimeConfig{
			Buildctl:      runtimeMembers["buildctl"],
			Buildkitd:     runtimeMembers["buildkitd"],
			BuildkitRunc:  runtimeMembers["buildkit-runc"],
			RootlessKit:   runtimeMembers["rootlesskit"],
			RootlessCtl:   runtimeMembers["rootlessctl"],
			Slirp4netns:   runtimeMembers["slirp4netns"],
			FuseOverlayFS: runtimeMembers["fuse-overlayfs"],
		},
	}
	executor, err := NewExecutor(cfg, &AllocationCapabilities{
		JobDirectoryFD:     jobFD,
		JobDirectoryDevice: uint64(jobStat.Dev),
		JobDirectoryInode:  uint64(jobStat.Ino),
		BuildEgressFD:      cgroupFD,
		BuildEgressDevice:  uint64(cgroupStat.Dev),
		BuildEgressInode:   uint64(cgroupStat.Ino),
	}, BuildPlan{
		Architecture: runtime.GOARCH,
		Components:   []BuildComponent{component},
	})
	if err != nil {
		t.Fatalf("NewExecutor() error = %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if err := executor.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	executorStarted := true
	t.Cleanup(func() {
		if executorStarted {
			_ = executor.Close(context.Background())
		}
	})
	assertNoForbiddenHostSocketFDs(t, cgroupPath)

	buildDone := make(chan nativeBuildResult, 1)
	go func() {
		output, err := executor.Build(ctx, component)
		buildDone <- nativeBuildResult{output: output, err: err}
	}()

	observed, completedResult := waitForNativeProcessProof(t, cgroupPath, runtimeRoot, buildDone)
	for _, process := range observed {
		if !processInCgroupTree(process, cgroupPath) {
			t.Fatalf("process %d (%s) escaped cgroup tree %q: %q", process.pid, process.cmdline, cgroupPath, process.cgroupPath)
		}
	}
	assertNoForbiddenHostSocketFDs(t, cgroupPath)

	var result nativeBuildResult
	if completedResult != nil {
		result = *completedResult
	} else {
		result = <-buildDone
	}
	if result.err != nil {
		t.Fatalf("Build() error = %v", result.err)
	}
	if result.output.TopLevelDigest == "" || result.output.FileSHA256 == "" || result.output.Architecture != runtime.GOARCH {
		t.Fatalf("unexpected OCI output: %#v", result.output)
	}
	assertOCIOutputContainsNativeRunProof(t, result.output.Path)
	assertNoForbiddenHostSocketFDs(t, cgroupPath)

	if err := executor.Close(ctx); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	executorStarted = false
	assertCgroupTreeEmpty(t, cgroupPath)
	assertNoChildCgroups(t, cgroupPath)
	assertNoMountsBelow(t, jobRoot)
	assertExecutorStateRemoved(t, jobRoot)
}

type nativeBuildResult struct {
	output OCIOutput
	err    error
}

type nativeRuntimeManifest struct {
	Architectures map[string]struct {
		Members map[string]string `json:"members"`
	} `json:"architectures"`
}

func nativeFixtureArchitectureSupported(goos string, goarch string) bool {
	return goos == "linux" && (goarch == "amd64" || goarch == "arm64")
}

func loadNativeRuntimeMembers(t *testing.T, runtimeRoot string, arch string) map[string]ExecutableMember {
	t.Helper()
	expected := nativeRuntimeManifestHashes(t, arch)
	entries, err := os.ReadDir(runtimeRoot)
	if err != nil {
		t.Fatalf("ReadDir(%q) error = %v", runtimeRoot, err)
	}
	if len(entries) != len(expected) {
		t.Fatalf("native runtime entry count = %d, want exact seven-member runtime", len(entries))
	}
	members := map[string]ExecutableMember{}
	for name, wantSHA := range expected {
		path := filepath.Join(runtimeRoot, name)
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("native runtime member %s unavailable: %v", name, err)
		}
		if !info.Mode().IsRegular() || info.Mode().Perm() != 0o555 {
			t.Fatalf("native runtime member %s mode/type = %s, want regular 0555", name, info.Mode())
		}
		gotSHA := sha256FileHex(t, path)
		if gotSHA != wantSHA {
			t.Fatalf("native runtime member %s sha256 = %s, want %s", name, gotSHA, wantSHA)
		}
		members[name] = ExecutableMember{Path: path, SHA256: gotSHA}
	}
	for _, entry := range entries {
		if _, ok := expected[entry.Name()]; !ok {
			t.Fatalf("unexpected native runtime member %q", entry.Name())
		}
	}
	return members
}

func nativeRuntimeManifestHashes(t *testing.T, arch string) map[string]string {
	t.Helper()
	payload := mustReadRepoFile(t, filepath.Join("deploy", "task-image-builder", "rootless-runtime-v2.json"))
	var manifest nativeRuntimeManifest
	if err := json.Unmarshal(payload, &manifest); err != nil {
		t.Fatalf("Unmarshal(rootless-runtime-v2.json) error = %v", err)
	}
	entry, ok := manifest.Architectures[arch]
	if !ok {
		t.Fatalf("manifest missing architecture %s", arch)
	}
	return entry.Members
}

func mustReadRepoFile(t *testing.T, relativePath string) []byte {
	t.Helper()
	for _, prefix := range []string{".", "..", "../..", "../../.."} {
		path := filepath.Join(prefix, relativePath)
		payload, err := os.ReadFile(path)
		if err == nil {
			return payload
		}
		if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("ReadFile(%q) error = %v", path, err)
		}
	}
	t.Fatalf("repository file %q not found from test working directory", relativePath)
	return nil
}

func requireNativeRootlessPrerequisites(t *testing.T) {
	t.Helper()
	if os.Geteuid() != 0 {
		t.Fatal("native fixture requires a disposable privileged root container")
	}
	if err := verifyHostIDMapHelpers(); err != nil {
		t.Fatalf("native fixture missing fixed host idmap helper prerequisite: %v", err)
	}
	if !subIDRangeExists(t, "/etc/subuid", "root") {
		t.Fatal("native fixture missing root subordinate uid range in /etc/subuid")
	}
	if !subIDRangeExists(t, "/etc/subgid", "root") {
		t.Fatal("native fixture missing root subordinate gid range in /etc/subgid")
	}
	info, err := os.Stat("/dev/fuse")
	if err != nil {
		t.Fatalf("/dev/fuse unavailable in native fixture: %v", err)
	}
	if info.Mode()&os.ModeCharDevice == 0 {
		t.Fatalf("/dev/fuse mode = %s, want character device", info.Mode())
	}
	var stat syscall.Statfs_t
	if err := syscall.Statfs("/sys/fs/cgroup", &stat); err != nil {
		t.Fatalf("Statfs(/sys/fs/cgroup) error = %v", err)
	}
	const cgroup2SuperMagic = 0x63677270
	if stat.Type != cgroup2SuperMagic {
		t.Fatalf("/sys/fs/cgroup filesystem magic = %#x, want cgroup v2", stat.Type)
	}
}

func subIDRangeExists(t *testing.T, path string, user string) bool {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("Open(%q) error = %v", path, err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	prefix := user + ":"
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if !strings.HasPrefix(line, prefix) {
			continue
		}
		parts := strings.Split(line, ":")
		if len(parts) != 3 {
			continue
		}
		length, err := strconv.Atoi(parts[2])
		if err == nil && length >= 65536 {
			return true
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("Scan(%q) error = %v", path, err)
	}
	return false
}

func writeNativeBuildContext(t *testing.T, jobRoot string, component BuildComponent) {
	t.Helper()
	contextDir := filepath.Join(jobRoot, component.ContextDir)
	if err := os.MkdirAll(contextDir, 0o700); err != nil {
		t.Fatalf("MkdirAll(%q) error = %v", contextDir, err)
	}
	proofBinary := buildNativeRunProofBinary(t)
	proofPayload, err := os.ReadFile(proofBinary)
	if err != nil {
		t.Fatalf("ReadFile(%q) error = %v", proofBinary, err)
	}
	if err := os.WriteFile(filepath.Join(contextDir, "proof-run"), proofPayload, 0o555); err != nil {
		t.Fatalf("WriteFile(proof-run) error = %v", err)
	}
	dockerfile := "FROM scratch\nCOPY proof-run /proof-run\nRUN [\"/proof-run\"]\n"
	if err := os.WriteFile(filepath.Join(jobRoot, component.Dockerfile), []byte(dockerfile), 0o400); err != nil {
		t.Fatalf("WriteFile(Dockerfile) error = %v", err)
	}
}

func buildNativeRunProofBinary(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	sourcePath := filepath.Join(root, "main.go")
	source := `package main

import (
	"os"
	"time"
)

func main() {
	if err := os.WriteFile("/native-run-proof", []byte("native-rootless-run-proof\n"), 0644); err != nil {
		panic(err)
	}
	time.Sleep(20 * time.Second)
}
`
	if err := os.WriteFile(sourcePath, []byte(source), 0o600); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", sourcePath, err)
	}
	outputPath := filepath.Join(root, "proof-run")
	cmd := exec.Command("go", "build", "-trimpath", "-buildvcs=false", "-o", outputPath, sourcePath)
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0", "GOOS=linux", "GOARCH="+runtime.GOARCH)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("go build proof-run error = %v\n%s", err, output)
	}
	return outputPath
}

func createNativeTestCgroup(t *testing.T) (string, int) {
	t.Helper()
	base := currentCgroupDirectory(t)
	path := filepath.Join(base, fmt.Sprintf("loom-task5-%d-%d", os.Getpid(), time.Now().UnixNano()))
	if err := os.Mkdir(path, 0o755); err != nil {
		t.Fatalf("Mkdir(%q) error = %v", path, err)
	}
	fd := openDirectoryFD(t, path)
	t.Cleanup(func() {
		syscall.Close(fd)
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("Remove(%q) error = %v", path, err)
		}
	})
	return path, fd
}

func currentCgroupDirectory(t *testing.T) string {
	t.Helper()
	payload, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		t.Fatalf("ReadFile(/proc/self/cgroup) error = %v", err)
	}
	for _, line := range strings.Split(string(payload), "\n") {
		if !strings.HasPrefix(line, "0::") {
			continue
		}
		relative := strings.TrimPrefix(line, "0::")
		path := filepath.Join("/sys/fs/cgroup", strings.TrimPrefix(relative, "/"))
		if info, err := os.Stat(path); err != nil || !info.IsDir() {
			t.Fatalf("current cgroup directory %q invalid: %v", path, err)
		}
		return path
	}
	t.Fatal("current cgroup v2 membership unavailable")
	return ""
}

type nativeProcess struct {
	pid        int
	exe        string
	cmdline    string
	cgroupPath string
	fdTargets  []string
}

func waitForNativeProcessProof(t *testing.T, cgroupPath string, runtimeRoot string, buildDone <-chan nativeBuildResult) ([]nativeProcess, *nativeBuildResult) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	var last []nativeProcess
	observed := map[string]nativeProcess{}
	for time.Now().Before(deadline) {
		processes := collectNativeProcesses(t, cgroupPath)
		processes = append(processes, collectNativeProofProcesses(t, runtimeRoot)...)
		last = processes
		for _, process := range processes {
			if nativeProofProcessRelevant(process, runtimeRoot) {
				observed[nativeProcessKey(process)] = process
			}
		}
		observedProcesses := nativeProcessMapValues(observed)
		if nativeProcessProofSatisfied(observedProcesses, runtimeRoot) {
			return observedProcesses, nil
		}
		select {
		case result := <-buildDone:
			if nativeProcessProofSatisfied(observedProcesses, runtimeRoot) {
				return observedProcesses, &result
			}
			t.Fatalf("build completed before full process proof was observed: output=%#v err=%v observed=%#v last=%#v", result.output, result.err, observedProcesses, last)
		default:
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for rootlesskit/buildkit/helper/RUN processes in cgroup %q; observed=%#v last=%#v", cgroupPath, nativeProcessMapValues(observed), last)
	return nil, nil
}

func nativeProcessProofSatisfied(processes []nativeProcess, runtimeRoot string) bool {
	wantExe := map[string]string{
		"rootlesskit":    filepath.Join(runtimeRoot, "rootlesskit"),
		"buildkitd":      filepath.Join(runtimeRoot, "buildkitd"),
		"slirp4netns":    filepath.Join(runtimeRoot, "slirp4netns"),
		"fuse-overlayfs": filepath.Join(runtimeRoot, "fuse-overlayfs"),
	}
	seen := map[string]bool{}
	for _, process := range processes {
		for name, exe := range wantExe {
			if process.exe == exe {
				seen[name] = true
			}
		}
		if filepath.Base(process.exe) == "proof-run" || strings.Contains(process.cmdline, "/proof-run") {
			seen["proof-run"] = true
		}
	}
	return seen["rootlesskit"] && seen["buildkitd"] && seen["slirp4netns"] && seen["fuse-overlayfs"] && seen["proof-run"]
}

func nativeProofProcessRelevant(process nativeProcess, runtimeRoot string) bool {
	for _, name := range []string{"rootlesskit", "buildkitd", "slirp4netns", "fuse-overlayfs"} {
		if process.exe == filepath.Join(runtimeRoot, name) {
			return true
		}
	}
	return filepath.Base(process.exe) == "proof-run" || strings.Contains(process.cmdline, "/proof-run")
}

func nativeProcessKey(process nativeProcess) string {
	return fmt.Sprintf("%d:%s:%s", process.pid, process.exe, process.cmdline)
}

func nativeProcessMapValues(processes map[string]nativeProcess) []nativeProcess {
	values := make([]nativeProcess, 0, len(processes))
	for _, process := range processes {
		values = append(values, process)
	}
	sort.Slice(values, func(i, j int) bool {
		if values[i].pid != values[j].pid {
			return values[i].pid < values[j].pid
		}
		return values[i].cmdline < values[j].cmdline
	})
	return values
}

func collectNativeProcesses(t *testing.T, cgroupPath string) []nativeProcess {
	t.Helper()
	pids := map[int]struct{}{}
	err := filepath.WalkDir(cgroupPath, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || entry.Name() != "cgroup.procs" {
			return nil
		}
		payload, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, field := range strings.Fields(string(payload)) {
			pid, err := strconv.Atoi(field)
			if err == nil && pid > 0 {
				pids[pid] = struct{}{}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("WalkDir(%q) error = %v", cgroupPath, err)
	}
	processes := make([]nativeProcess, 0, len(pids))
	for pid := range pids {
		process, ok := readNativeProcess(pid)
		if ok {
			processes = append(processes, process)
		}
	}
	sort.Slice(processes, func(i, j int) bool { return processes[i].pid < processes[j].pid })
	return processes
}

func collectNativeProofProcesses(t *testing.T, runtimeRoot string) []nativeProcess {
	t.Helper()
	entries, err := os.ReadDir("/proc")
	if err != nil {
		t.Fatalf("ReadDir(/proc) error = %v", err)
	}
	var processes []nativeProcess
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid <= 0 {
			continue
		}
		process, ok := readNativeProcess(pid)
		if ok && nativeProofProcessRelevant(process, runtimeRoot) {
			processes = append(processes, process)
		}
	}
	sort.Slice(processes, func(i, j int) bool { return processes[i].pid < processes[j].pid })
	return processes
}

func readNativeProcess(pid int) (nativeProcess, bool) {
	exe, err := os.Readlink(fmt.Sprintf("/proc/%d/exe", pid))
	if err != nil {
		return nativeProcess{}, false
	}
	cmdlinePayload, err := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", pid))
	if err != nil {
		return nativeProcess{}, false
	}
	cgroupPath, err := processCgroupPath(pid)
	if err != nil {
		return nativeProcess{}, false
	}
	fdTargets := []string{}
	fdDir := fmt.Sprintf("/proc/%d/fd", pid)
	entries, err := os.ReadDir(fdDir)
	if err == nil {
		for _, entry := range entries {
			target, err := os.Readlink(filepath.Join(fdDir, entry.Name()))
			if err == nil {
				fdTargets = append(fdTargets, target)
			}
		}
	}
	return nativeProcess{
		pid:        pid,
		exe:        exe,
		cmdline:    strings.ReplaceAll(string(bytes.TrimRight(cmdlinePayload, "\x00")), "\x00", " "),
		cgroupPath: cgroupPath,
		fdTargets:  fdTargets,
	}, true
}

func processCgroupPath(pid int) (string, error) {
	payload, err := os.ReadFile(fmt.Sprintf("/proc/%d/cgroup", pid))
	if err != nil {
		return "", err
	}
	for _, line := range strings.Split(string(payload), "\n") {
		if !strings.HasPrefix(line, "0::") {
			continue
		}
		relative := strings.TrimPrefix(line, "0::")
		return filepath.Join("/sys/fs/cgroup", strings.TrimPrefix(relative, "/")), nil
	}
	return "", errors.New("process cgroup v2 membership unavailable")
}

func processInCgroupTree(process nativeProcess, cgroupPath string) bool {
	rel, err := filepath.Rel(cgroupPath, process.cgroupPath)
	return err == nil && (rel == "." || rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator)))
}

func assertNoForbiddenHostSocketFDs(t *testing.T, cgroupPath string) {
	t.Helper()
	forbidden := map[string]struct{}{
		"/run/docker.sock":                    {},
		"/var/run/docker.sock":                {},
		"/run/containerd/containerd.sock":     {},
		"/var/run/containerd/containerd.sock": {},
	}
	unixSockets := procNetUnixPaths(t)
	for _, process := range collectNativeProcesses(t, cgroupPath) {
		for _, target := range process.fdTargets {
			if _, ok := forbidden[target]; ok {
				t.Fatalf("process %d opened forbidden host socket %q", process.pid, target)
			}
			if inode, ok := socketInode(target); ok {
				if path, found := unixSockets[inode]; found {
					if _, forbiddenPath := forbidden[path]; forbiddenPath {
						t.Fatalf("process %d opened forbidden host socket inode %s path %q", process.pid, inode, path)
					}
				}
			}
		}
	}
}

func procNetUnixPaths(t *testing.T) map[string]string {
	t.Helper()
	payload, err := os.ReadFile("/proc/net/unix")
	if err != nil {
		t.Fatalf("ReadFile(/proc/net/unix) error = %v", err)
	}
	result := map[string]string{}
	for _, line := range strings.Split(string(payload), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 8 && fields[0] != "Num" {
			result[fields[6]] = fields[7]
		}
	}
	return result
}

func socketInode(target string) (string, bool) {
	if !strings.HasPrefix(target, "socket:[") || !strings.HasSuffix(target, "]") {
		return "", false
	}
	return strings.TrimSuffix(strings.TrimPrefix(target, "socket:["), "]"), true
}

func assertOCIOutputContainsNativeRunProof(t *testing.T, path string) {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("Open(%q) error = %v", path, err)
	}
	defer file.Close()
	entries := map[string][]byte{}
	outer := tar.NewReader(file)
	for {
		header, err := outer.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("OCI tar read error = %v", err)
		}
		name, err := validateTarEntry(header)
		if err != nil {
			t.Fatalf("OCI tar entry invalid: %v", err)
		}
		if header.Typeflag != tar.TypeReg {
			continue
		}
		payload, err := io.ReadAll(outer)
		if err != nil {
			t.Fatalf("OCI blob read error = %v", err)
		}
		entries[name] = payload
	}
	var index ociIndex
	if err := decodeStrictJSON(entries["index.json"], &index); err != nil {
		t.Fatalf("decode OCI index error = %v", err)
	}
	if len(index.Manifests) != 1 {
		t.Fatalf("OCI index manifest count = %d, want one", len(index.Manifests))
	}
	manifestPayload, err := descriptorPayload(entries, index.Manifests[0])
	if err != nil {
		t.Fatalf("OCI manifest descriptor error = %v", err)
	}
	var manifest ociManifest
	if err := decodeStrictJSON(manifestPayload, &manifest); err != nil {
		t.Fatalf("decode OCI manifest error = %v", err)
	}
	for _, layer := range manifest.Layers {
		payload, err := descriptorPayload(entries, layer)
		if err != nil {
			t.Fatalf("OCI layer descriptor error = %v", err)
		}
		if layerPayloadContainsNativeRunProof(payload) {
			return
		}
	}
	t.Fatal("OCI output does not contain file produced by native RUN")
}

func layerPayloadContainsNativeRunProof(payload []byte) bool {
	reader := io.Reader(bytes.NewReader(payload))
	if gzipReader, err := gzip.NewReader(bytes.NewReader(payload)); err == nil {
		defer gzipReader.Close()
		reader = gzipReader
	}
	tarReader := tar.NewReader(reader)
	for {
		header, err := tarReader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil || header.Typeflag != tar.TypeReg {
			continue
		}
		name := strings.TrimPrefix(header.Name, "./")
		if filepath.Clean(name) != "native-run-proof" {
			continue
		}
		entryPayload, err := io.ReadAll(tarReader)
		if err == nil && string(entryPayload) == "native-rootless-run-proof\n" {
			return true
		}
	}
	return false
}

func assertCgroupTreeEmpty(t *testing.T, cgroupPath string) {
	t.Helper()
	for _, process := range collectNativeProcesses(t, cgroupPath) {
		t.Fatalf("cgroup process survived cleanup: pid=%d exe=%q cmdline=%q cgroup=%q", process.pid, process.exe, process.cmdline, process.cgroupPath)
	}
}

func assertNoChildCgroups(t *testing.T, cgroupPath string) {
	t.Helper()
	entries, err := os.ReadDir(cgroupPath)
	if err != nil {
		t.Fatalf("ReadDir(%q) error = %v", cgroupPath, err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			t.Fatalf("child cgroup %q survived cleanup", filepath.Join(cgroupPath, entry.Name()))
		}
	}
}

func assertNoMountsBelow(t *testing.T, root string) {
	t.Helper()
	payload, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		t.Fatalf("ReadFile(/proc/self/mountinfo) error = %v", err)
	}
	for _, line := range strings.Split(string(payload), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 5 {
			continue
		}
		mountPoint := decodeMountInfoPath(fields[4])
		if mountPoint == root || strings.HasPrefix(mountPoint, root+string(os.PathSeparator)) {
			t.Fatalf("mount survived below job root: %s", line)
		}
	}
}

func decodeMountInfoPath(path string) string {
	replacer := strings.NewReplacer(`\040`, " ", `\011`, "\t", `\012`, "\n", `\134`, `\`)
	return replacer.Replace(path)
}

func assertExecutorStateRemoved(t *testing.T, jobRoot string) {
	t.Helper()
	for _, name := range []string{"buildkit", "buildkit-root", "rootlesskit", "tmp", "home"} {
		path := filepath.Join(jobRoot, name)
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("executor state path %q survived cleanup: %v", path, err)
		}
	}
}

type executorFixture struct {
	root         string
	jobRoot      string
	buildRoot    string
	config       Config
	capabilities *AllocationCapabilities
}

func newExecutorFixture(t *testing.T) executorFixture {
	t.Helper()
	root, err := os.MkdirTemp("/tmp", "lt5.")
	if err != nil {
		t.Fatalf("MkdirTemp(/tmp, lt5.) error = %v", err)
	}
	t.Cleanup(func() {
		os.RemoveAll(root)
	})
	jobRoot := filepath.Join(root, "job")
	buildRoot := filepath.Join(root, "build-egress")
	if err := os.Mkdir(jobRoot, 0o755); err != nil {
		t.Fatalf("Mkdir(%q) error = %v", jobRoot, err)
	}
	if err := os.Mkdir(buildRoot, 0o755); err != nil {
		t.Fatalf("Mkdir(%q) error = %v", buildRoot, err)
	}
	runtimeRoot := filepath.Join(root, "runtime")
	members := map[string]ExecutableMember{}
	for _, name := range []string{"buildctl", "buildkitd", "buildkit-runc", "rootlesskit", "rootlessctl", "slirp4netns", "fuse-overlayfs"} {
		path := filepath.Join(runtimeRoot, name)
		writeExecutableFixture(t, path, "#!/bin/sh\nexit 0\n")
		members[name] = ExecutableMember{Path: path, SHA256: sha256FileHex(t, path)}
	}
	jobFD := openDirectoryFD(t, jobRoot)
	buildFD := openDirectoryFD(t, buildRoot)
	t.Cleanup(func() {
		syscall.Close(jobFD)
		syscall.Close(buildFD)
	})
	jobStat := mustFstat(t, jobFD)
	buildStat := mustFstat(t, buildFD)
	config := Config{
		ReleaseSHA256: strings.Repeat("a", 64),
		CPUArch:       runtime.GOARCH,
		Runtime: RuntimeConfig{
			Buildctl:      members["buildctl"],
			Buildkitd:     members["buildkitd"],
			BuildkitRunc:  members["buildkit-runc"],
			RootlessKit:   members["rootlesskit"],
			RootlessCtl:   members["rootlessctl"],
			Slirp4netns:   members["slirp4netns"],
			FuseOverlayFS: members["fuse-overlayfs"],
		},
	}
	return executorFixture{
		root:      root,
		jobRoot:   jobRoot,
		buildRoot: buildRoot,
		config:    config,
		capabilities: &AllocationCapabilities{
			JobDirectoryFD:     jobFD,
			JobDirectoryDevice: uint64(jobStat.Dev),
			JobDirectoryInode:  uint64(jobStat.Ino),
			BuildEgressFD:      buildFD,
			BuildEgressDevice:  uint64(buildStat.Dev),
			BuildEgressInode:   uint64(buildStat.Ino),
		},
	}
}

func restoreExecutorHooks(t *testing.T) {
	t.Helper()
	previousHostHelpers := executorVerifyHostIDMapHelpers
	previousBuildkitCgroupParent := executorBuildkitCgroupParent
	previousLaunch := executorLaunchInCgroup
	previousRunBuildctl := executorRunBuildctl
	previousValidate := executorValidateOCIOutput
	previousSignal := executorSignalProcess
	previousWait := executorWaitProcess
	previousEmpty := executorCgroupEmpty
	previousCleanupCgroup := executorCleanupCgroup
	previousAlive := executorProcessAlive
	previousReadinessTimeout := executorReadinessTimeout
	previousReadinessPoll := executorReadinessPoll
	previousShutdownTimeout := executorShutdownTimeout
	previousShutdownPoll := executorShutdownPoll
	t.Cleanup(func() {
		executorVerifyHostIDMapHelpers = previousHostHelpers
		executorBuildkitCgroupParent = previousBuildkitCgroupParent
		executorLaunchInCgroup = previousLaunch
		executorRunBuildctl = previousRunBuildctl
		executorValidateOCIOutput = previousValidate
		executorSignalProcess = previousSignal
		executorWaitProcess = previousWait
		executorCgroupEmpty = previousEmpty
		executorCleanupCgroup = previousCleanupCgroup
		executorProcessAlive = previousAlive
		executorReadinessTimeout = previousReadinessTimeout
		executorReadinessPoll = previousReadinessPoll
		executorShutdownTimeout = previousShutdownTimeout
		executorShutdownPoll = previousShutdownPoll
	})
}

func stubBuildkitCgroupParent(t *testing.T, fixture executorFixture, parent string) {
	t.Helper()
	executorBuildkitCgroupParent = func(fd int) (string, error) {
		if fd != fixture.capabilities.BuildEgressFD {
			t.Fatalf("buildkit cgroup parent fd = %d, want %d", fd, fixture.capabilities.BuildEgressFD)
		}
		return parent, nil
	}
}

func valueAfterArg(t *testing.T, argv []string, flag string) string {
	t.Helper()
	for i, value := range argv {
		if value == flag {
			if i+1 >= len(argv) {
				t.Fatalf("argv flag %q missing value: %#v", flag, argv)
			}
			return argv[i+1]
		}
	}
	return ""
}

func commandExitStatus(t *testing.T, status int) error {
	t.Helper()
	cmd := exec.Command("/bin/sh", "-c", fmt.Sprintf("exit %d", status))
	err := cmd.Run()
	if err == nil {
		t.Fatalf("command unexpectedly succeeded for exit status %d", status)
	}
	return err
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
