package main

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"syscall"
	"testing"
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
	restoreExecutorHooks(t)
	executorLaunchInCgroup = func(ctx context.Context, executable ExecutableMember, argv []string, env []string, cgroupFD int) (*Process, error) {
		launchedExecutable = executable
		launchedArgv = append([]string(nil), argv...)
		launchedEnv = append([]string(nil), env...)
		launchedCgroupFD = cgroupFD
		return &Process{PID: 4242, ExecutableSHA256: executable.SHA256, CgroupInode: fixture.capabilities.BuildEgressInode}, nil
	}

	if err := executor.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
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
		"--rootless",
		"--oci-worker=true",
		"--oci-worker-rootless",
		"--containerd-worker=false",
		"--oci-worker-snapshotter=fuse-overlayfs",
		"--oci-worker-binary=" + fixture.config.Runtime.BuildkitRunc.Path,
		"--oci-worker-no-process-sandbox=false",
		"--root",
		filepath.Join(fixture.jobRoot, "buildkit-root"),
	}
	for _, want := range required {
		if !containsString(launchedArgv, want) {
			t.Fatalf("rootless/buildkit argv missing %q in %#v", want, launchedArgv)
		}
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
	for _, entry := range launchedEnv {
		if strings.HasPrefix(entry, "PATH=") || strings.HasPrefix(entry, "DOCKER_HOST=") || strings.HasPrefix(entry, "CONTAINERD_ADDRESS=") {
			t.Fatalf("Start() inherited forbidden environment entry %q", entry)
		}
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
	executorLaunchInCgroup = func(ctx context.Context, executable ExecutableMember, argv []string, env []string, cgroupFD int) (*Process, error) {
		return &Process{PID: 4242, ExecutableSHA256: executable.SHA256, CgroupInode: fixture.capabilities.BuildEgressInode}, nil
	}
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
	restoreExecutorHooks(t)
	executorLaunchInCgroup = func(ctx context.Context, executable ExecutableMember, argv []string, env []string, cgroupFD int) (*Process, error) {
		return &Process{PID: 4343, ExecutableSHA256: executable.SHA256, CgroupInode: fixture.capabilities.BuildEgressInode}, nil
	}
	executorSignalProcess = func(process *Process, signal os.Signal) error {
		if signal == syscall.SIGTERM {
			terminated = true
		}
		return nil
	}
	executorWaitProcess = func(*Process) error { return nil }
	executorCgroupEmpty = func(fd int) (bool, error) {
		return terminated && fd == fixture.capabilities.BuildEgressFD, nil
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

	executor, err = NewExecutor(fixture.config, fixture.capabilities, BuildPlan{
		Architecture: "amd64",
		Components: []BuildComponent{
			{Name: "component-a", ContextDir: "bundle/context", Dockerfile: "bundle/context/Dockerfile"},
		},
	})
	if err != nil {
		t.Fatalf("NewExecutor() error = %v", err)
	}
	executorLaunchInCgroup = func(ctx context.Context, executable ExecutableMember, argv []string, env []string, cgroupFD int) (*Process, error) {
		return &Process{PID: 4444, ExecutableSHA256: executable.SHA256, CgroupInode: fixture.capabilities.BuildEgressInode}, nil
	}
	executorCgroupEmpty = func(fd int) (bool, error) { return false, nil }
	if err := executor.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if err := executor.Close(context.Background()); err == nil {
		t.Fatal("Close() succeeded, want surviving process cleanup failure")
	}
}

func TestNativeBuildFixtureRequiresExactRuntimeHelpersAndHostPrerequisites(t *testing.T) {
	runtimeRoot := os.Getenv("LOOM_TASK_IMAGE_BUILDER_NATIVE_RUNTIME")
	if runtimeRoot == "" {
		t.Skip("LOOM_TASK_IMAGE_BUILDER_NATIVE_RUNTIME not set; exact seven-member rootless runtime fixture unavailable")
	}
	for _, member := range []string{"buildctl", "buildkitd", "buildkit-runc", "rootlesskit", "rootlessctl", "slirp4netns", "fuse-overlayfs"} {
		info, err := os.Stat(filepath.Join(runtimeRoot, member))
		if err != nil {
			t.Fatalf("native runtime helper %s unavailable: %v", member, err)
		}
		if info.Mode().Perm() != 0o555 {
			t.Fatalf("native runtime helper %s mode = %#o, want 0555", member, info.Mode().Perm())
		}
	}
	if _, err := exec.LookPath("newuidmap"); err != nil {
		t.Skip("newuidmap unavailable in pinned Go fixture; rootless BuildKit native execution requires subuid mapping helper")
	}
	if _, err := exec.LookPath("newgidmap"); err != nil {
		t.Skip("newgidmap unavailable in pinned Go fixture; rootless BuildKit native execution requires subgid mapping helper")
	}
	if _, err := os.Stat("/dev/fuse"); err != nil {
		t.Skipf("/dev/fuse unavailable in pinned Go fixture: %v", err)
	}
	if os.Geteuid() == 0 {
		t.Skip("pinned Go fixture runs tests as root; rootless BuildKit native execution requires a non-root user with subuid/subgid ranges")
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
	root := t.TempDir()
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
	previousLaunch := executorLaunchInCgroup
	previousRunBuildctl := executorRunBuildctl
	previousValidate := executorValidateOCIOutput
	previousSignal := executorSignalProcess
	previousWait := executorWaitProcess
	previousEmpty := executorCgroupEmpty
	t.Cleanup(func() {
		executorLaunchInCgroup = previousLaunch
		executorRunBuildctl = previousRunBuildctl
		executorValidateOCIOutput = previousValidate
		executorSignalProcess = previousSignal
		executorWaitProcess = previousWait
		executorCgroupEmpty = previousEmpty
	})
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
