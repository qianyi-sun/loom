package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"
)

type BuildPlan struct {
	Architecture              string
	Frontend                  string
	NetworkMode               string
	AllowInsecureEntitlements bool
	Binds                     []string
	Devices                   []string
	CDIDevices                []string
	SSHForwarding             []string
	CacheImports              []string
	CacheExports              []string
	Components                []BuildComponent
}

type BuildComponent struct {
	Name       string
	ContextDir string
	Dockerfile string
}

type Executor struct {
	config          Config
	capabilities    *AllocationCapabilities
	plan            BuildPlan
	jobRoot         string
	buildkitAddress string
	daemon          *Process
	started         bool
}

var (
	executorLaunchInCgroup = LaunchInCgroup
	executorRunBuildctl    = func(ctx context.Context, executable ExecutableMember, argv []string, env []string, cgroupFD int) error {
		process, err := LaunchInCgroup(ctx, executable, argv, env, cgroupFD)
		if err != nil {
			return err
		}
		defer process.Close()
		return process.Wait()
	}
	executorValidateOCIOutput = ValidateOCIOutput
	executorSignalProcess     = func(process *Process, signal os.Signal) error { return process.Signal(signal) }
	executorWaitProcess       = func(process *Process) error { return process.Wait() }
	executorCgroupEmpty       = cgroupEmpty
)

func NewExecutor(cfg Config, caps *AllocationCapabilities, plan BuildPlan) (*Executor, error) {
	if caps == nil || caps.JobDirectoryFD < 0 || caps.BuildEgressFD < 0 {
		return nil, errors.New("executor capabilities invalid")
	}
	jobStat, err := validateDirectoryDescriptor(caps.JobDirectoryFD)
	if err != nil {
		return nil, err
	}
	buildStat, err := validateDirectoryDescriptor(caps.BuildEgressFD)
	if err != nil {
		return nil, err
	}
	if uint64(jobStat.Dev) != caps.JobDirectoryDevice || uint64(jobStat.Ino) != caps.JobDirectoryInode {
		return nil, errors.New("job directory identity mismatch")
	}
	if uint64(buildStat.Dev) != caps.BuildEgressDevice || uint64(buildStat.Ino) != caps.BuildEgressInode {
		return nil, errors.New("build egress cgroup identity mismatch")
	}
	if err := validateRuntimeMembers(cfg.Runtime); err != nil {
		return nil, err
	}
	if err := validateBuildPlan(plan); err != nil {
		return nil, err
	}
	jobRoot, err := pathFromDirectoryFD(caps.JobDirectoryFD)
	if err != nil {
		return nil, err
	}
	address := "unix://" + filepath.Join(jobRoot, "buildkit", "buildkitd.sock")
	return &Executor{
		config:          cfg,
		capabilities:    caps,
		plan:            plan,
		jobRoot:         jobRoot,
		buildkitAddress: address,
	}, nil
}

func (e *Executor) Start(ctx context.Context) error {
	if e == nil {
		return errors.New("executor unavailable")
	}
	if e.started {
		return errors.New("executor already started")
	}
	for _, dir := range []string{
		filepath.Join(e.jobRoot, "buildkit"),
		filepath.Join(e.jobRoot, "buildkit-root"),
		filepath.Join(e.jobRoot, "rootlesskit"),
		filepath.Join(e.jobRoot, "tmp"),
		filepath.Join(e.jobRoot, "home"),
	} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return err
		}
	}
	env := []string{
		"LANG=C.UTF-8",
		"TZ=UTC",
		"XDG_RUNTIME_DIR=" + filepath.Join(e.jobRoot, "rootlesskit"),
		"BUILDKIT_HOST=" + e.buildkitAddress,
	}
	argv := []string{
		"--net=slirp4netns",
		"--disable-host-loopback",
		"--ipv6",
		"--slirp4netns-sandbox=auto",
		"--slirp4netns-seccomp=auto",
		"--slirp4netns-binary=" + e.config.Runtime.Slirp4netns.Path,
		"--state-dir=" + filepath.Join(e.jobRoot, "rootlesskit"),
		"--copy-up=/etc",
		"--propagation=rslave",
		"--",
		e.config.Runtime.Buildkitd.Path,
		"--rootless",
		"--oci-worker=true",
		"--oci-worker-rootless",
		"--containerd-worker=false",
		"--oci-worker-snapshotter=fuse-overlayfs",
		"--oci-worker-binary=" + e.config.Runtime.BuildkitRunc.Path,
		"--oci-worker-no-process-sandbox=false",
		"--root",
		filepath.Join(e.jobRoot, "buildkit-root"),
		"--addr",
		e.buildkitAddress,
	}
	process, err := executorLaunchInCgroup(ctx, e.config.Runtime.RootlessKit, argv, env, e.capabilities.BuildEgressFD)
	if err != nil {
		return err
	}
	e.daemon = process
	e.started = true
	return nil
}

func (e *Executor) Build(ctx context.Context, component BuildComponent) (OCIOutput, error) {
	if e == nil || !e.started {
		return OCIOutput{}, errors.New("executor not started")
	}
	if err := validateBuildComponent(component); err != nil {
		return OCIOutput{}, err
	}
	if !e.planContainsComponent(component.Name) {
		return OCIOutput{}, errors.New("build component not in plan")
	}
	outputDir := filepath.Join(e.jobRoot, "oci")
	if err := os.MkdirAll(outputDir, 0o700); err != nil {
		return OCIOutput{}, err
	}
	outputPath := filepath.Join(outputDir, component.Name+".tar")
	platform := "linux/" + e.plan.Architecture
	argv := []string{
		"--addr", e.buildkitAddress,
		"build",
		"--no-cache",
		"--frontend", "dockerfile.v0",
		"--local", "context=" + filepath.Join(e.jobRoot, component.ContextDir),
		"--local", "dockerfile=" + filepath.Join(e.jobRoot, filepath.Dir(component.Dockerfile)),
		"--opt", "filename=" + filepath.Base(component.Dockerfile),
		"--opt", "platform=" + platform,
		"--output", "type=oci,dest=" + outputPath,
	}
	env := []string{"LANG=C.UTF-8", "TZ=UTC", "BUILDKIT_HOST=" + e.buildkitAddress}
	if err := executorRunBuildctl(ctx, e.config.Runtime.Buildctl, argv, env, e.capabilities.BuildEgressFD); err != nil {
		return OCIOutput{}, err
	}
	return executorValidateOCIOutput(outputPath, platform)
}

func (e *Executor) Close(ctx context.Context) error {
	if e == nil || e.daemon == nil {
		return nil
	}
	if err := executorSignalProcess(e.daemon, syscall.SIGTERM); err != nil {
		return err
	}
	done := make(chan error, 1)
	go func() {
		done <- executorWaitProcess(e.daemon)
	}()
	select {
	case err := <-done:
		if err != nil {
			return err
		}
	case <-time.After(5 * time.Second):
		_ = e.daemon.Kill()
		select {
		case err := <-done:
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	case <-ctx.Done():
		_ = e.daemon.Kill()
		return ctx.Err()
	}
	empty, err := executorCgroupEmpty(e.capabilities.BuildEgressFD)
	if err != nil {
		return err
	}
	if !empty {
		return errors.New("build egress cgroup not empty after cleanup")
	}
	e.daemon = nil
	e.started = false
	return nil
}

func validateRuntimeMembers(runtimeCfg RuntimeConfig) error {
	for name, member := range map[string]ExecutableMember{
		"buildctl":       runtimeCfg.Buildctl,
		"buildkitd":      runtimeCfg.Buildkitd,
		"buildkit-runc":  runtimeCfg.BuildkitRunc,
		"rootlesskit":    runtimeCfg.RootlessKit,
		"rootlessctl":    runtimeCfg.RootlessCtl,
		"slirp4netns":    runtimeCfg.Slirp4netns,
		"fuse-overlayfs": runtimeCfg.FuseOverlayFS,
	} {
		if !filepath.IsAbs(member.Path) || filepath.Clean(member.Path) != member.Path || filepath.Base(member.Path) != name {
			return fmt.Errorf("runtime member %s path invalid", name)
		}
		if !isDigest(member.SHA256) {
			return fmt.Errorf("runtime member %s digest invalid", name)
		}
	}
	return nil
}

func validateBuildPlan(plan BuildPlan) error {
	if plan.Architecture != runtime.GOARCH || (plan.Architecture != "amd64" && plan.Architecture != "arm64") {
		return errors.New("build architecture is not native or supported")
	}
	if plan.Frontend != "" && plan.Frontend != "dockerfile.v0" {
		return errors.New("remote dockerfile frontend forbidden")
	}
	if plan.NetworkMode != "" && plan.NetworkMode != "sandbox" {
		return errors.New("host networking forbidden")
	}
	if plan.AllowInsecureEntitlements {
		return errors.New("insecure entitlements forbidden")
	}
	if len(plan.Binds) != 0 || len(plan.Devices) != 0 || len(plan.CDIDevices) != 0 || len(plan.SSHForwarding) != 0 || len(plan.CacheImports) != 0 || len(plan.CacheExports) != 0 {
		return errors.New("build escape authority forbidden")
	}
	if len(plan.Components) == 0 {
		return errors.New("build plan has no components")
	}
	seen := map[string]struct{}{}
	for _, component := range plan.Components {
		if err := validateBuildComponent(component); err != nil {
			return err
		}
		if _, ok := seen[component.Name]; ok {
			return errors.New("duplicate build component")
		}
		seen[component.Name] = struct{}{}
	}
	return nil
}

func validateBuildComponent(component BuildComponent) error {
	if component.Name == "" {
		return errors.New("build component name invalid")
	}
	for _, r := range component.Name {
		if !(r == '-' || r == '_' || r == '.' || r >= '0' && r <= '9' || r >= 'a' && r <= 'z') {
			return errors.New("build component name invalid")
		}
	}
	if err := validateRelativeBundlePath(component.ContextDir); err != nil {
		return err
	}
	if err := validateRelativeBundlePath(component.Dockerfile); err != nil {
		return err
	}
	return nil
}

func (e *Executor) planContainsComponent(name string) bool {
	for _, component := range e.plan.Components {
		if component.Name == name {
			return true
		}
	}
	return false
}

func cgroupEmpty(fd int) (bool, error) {
	root, err := pathFromDirectoryFD(fd)
	if err != nil {
		return false, err
	}
	payload, err := os.ReadFile(filepath.Join(root, "cgroup.procs"))
	if errors.Is(err, os.ErrNotExist) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	return strings.TrimSpace(string(payload)) == "", nil
}
