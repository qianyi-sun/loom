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
	"runtime"
	"sort"
	"strconv"
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
	config Config
	// capabilities contains borrowed guard-transferred descriptors. Executor uses
	// these FDs for exact placement/cleanup but never closes caller-owned rights.
	capabilities    *AllocationCapabilities
	plan            BuildPlan
	jobRoot         string
	buildkitAddress string
	daemon          *Process
	started         bool
}

const (
	buildkitFuseOverlayFSBinaryEnv = "BUILDKIT_FUSE_OVERLAYFS_BINARY"
	maxBuildkitUnixSocketPathBytes = 104
	hostNewuidmapPath              = "/usr/bin/newuidmap"
	hostNewgidmapPath              = "/usr/bin/newgidmap"
	hostNsenterPath                = "/usr/bin/nsenter"
	hostIPPath                     = "/usr/bin/ip"
	cgroup2SuperMagic              = 0x63677270
)

var (
	executorVerifyHostIDMapHelpers = verifyHostIDMapHelpers
	executorBuildkitCgroupParent   = buildkitCgroupParent
	executorLaunchInCgroup         = LaunchInCgroup
	executorRunBuildctl            = func(ctx context.Context, executable ExecutableMember, argv []string, env []string, cgroupFD int) error {
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
	executorCleanupCgroup     = cleanupCgroupChildren
	executorProcessAlive      = processAlive
	executorReadinessTimeout  = 30 * time.Second
	executorReadinessPoll     = 100 * time.Millisecond
	executorShutdownTimeout   = 5 * time.Second
	executorShutdownPoll      = 50 * time.Millisecond
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
	socketPath, err := buildkitSocketPath(jobRoot)
	if err != nil {
		return nil, err
	}
	address := "unix://" + socketPath
	return &Executor{
		config:          cfg,
		capabilities:    caps,
		plan:            plan,
		jobRoot:         jobRoot,
		buildkitAddress: address,
	}, nil
}

func (e *Executor) Start(ctx context.Context) (err error) {
	if e == nil {
		return errors.New("executor unavailable")
	}
	if e.started {
		return errors.New("executor already started")
	}
	defer func() {
		if err == nil {
			return
		}
		if cleanupErr := e.cleanupState(); cleanupErr != nil {
			err = errors.Join(err, fmt.Errorf("cleanup partial executor state: %w", cleanupErr))
		}
		e.daemon = nil
		e.started = false
	}()
	if err := e.prepareStateDirs(); err != nil {
		return err
	}
	if err := executorVerifyHostIDMapHelpers(); err != nil {
		return err
	}
	if err := verifyExecutableFile(e.config.Runtime.FuseOverlayFS); err != nil {
		return fmt.Errorf("fuse-overlayfs runtime member invalid: %w", err)
	}
	cgroupParent, err := executorBuildkitCgroupParent(e.capabilities.BuildEgressFD)
	if err != nil {
		return err
	}
	buildkitConfigPath, err := e.writeBuildkitConfig(cgroupParent)
	if err != nil {
		return err
	}
	env := []string{
		"LANG=C.UTF-8",
		"TZ=UTC",
		"XDG_RUNTIME_DIR=" + filepath.Join(e.jobRoot, "rootlesskit"),
		"BUILDKIT_HOST=" + e.buildkitAddress,
		buildkitFuseOverlayFSBinaryEnv + "=" + e.config.Runtime.FuseOverlayFS.Path,
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
		"--config",
		buildkitConfigPath,
		"--rootless",
		"--oci-worker=true",
		"--oci-worker-rootless",
		"--containerd-worker=false",
		"--cdi-disabled",
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
	if err := e.waitForBuildkitReady(ctx); err != nil {
		stopErr := e.stopDaemon(ctx)
		e.daemon = nil
		e.started = false
		if stopErr != nil {
			return fmt.Errorf("buildkit daemon readiness failed: %w; cleanup failed: %v", err, stopErr)
		}
		return fmt.Errorf("buildkit daemon readiness failed: %w", err)
	}
	e.started = true
	return nil
}

func (e *Executor) Build(ctx context.Context, component BuildComponent) (_ OCIOutput, err error) {
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
	cleanupOutput := true
	defer func() {
		if cleanupOutput {
			if cleanupErr := removePartialOCIOutput(outputPath); cleanupErr != nil {
				err = errors.Join(err, fmt.Errorf("cleanup partial OCI output: %w", cleanupErr))
			}
		}
	}()
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
	output, err := executorValidateOCIOutput(outputPath, platform)
	if err != nil {
		return OCIOutput{}, err
	}
	cleanupOutput = false
	return output, nil
}

func (e *Executor) Close(ctx context.Context) error {
	if e == nil || e.daemon == nil {
		return nil
	}
	return e.stopDaemon(ctx)
}

func (e *Executor) waitForBuildkitReady(ctx context.Context) error {
	deadline := time.Now().Add(executorReadinessTimeout)
	var lastErr error
	for {
		probeCtx, cancel := context.WithTimeout(ctx, executorReadinessPoll)
		err := executorRunBuildctl(probeCtx, e.config.Runtime.Buildctl, []string{"--addr", e.buildkitAddress, "debug", "workers"}, []string{
			"LANG=C.UTF-8",
			"TZ=UTC",
			"BUILDKIT_HOST=" + e.buildkitAddress,
		}, e.capabilities.BuildEgressFD)
		cancel()
		if err == nil {
			return nil
		}
		lastErr = err
		if !executorProcessAlive(e.daemon) {
			return fmt.Errorf("daemon exited before readiness: %w", lastErr)
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("daemon did not become ready before deadline: %w", lastErr)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(executorReadinessPoll):
		}
	}
}

func (e *Executor) stopDaemon(ctx context.Context) error {
	var errs []error
	if e.daemon != nil {
		if err := executorSignalProcess(e.daemon, syscall.SIGTERM); err != nil {
			errs = append(errs, fmt.Errorf("signal daemon: %w", err))
		}
		waitErr := e.waitForDaemonExit(ctx)
		if !daemonExitAllowedAfterStop(waitErr) {
			errs = append(errs, fmt.Errorf("wait daemon: %w", waitErr))
		}
	}
	if err := e.waitForCgroupEmpty(ctx); err != nil {
		errs = append(errs, fmt.Errorf("wait cgroup empty: %w", err))
	}
	if err := executorCleanupCgroup(e.capabilities.BuildEgressFD); err != nil {
		errs = append(errs, fmt.Errorf("cleanup cgroup children: %w", err))
	}
	if err := e.cleanupState(); err != nil {
		errs = append(errs, fmt.Errorf("cleanup executor state: %w", err))
	}
	e.daemon = nil
	e.started = false
	return errors.Join(errs...)
}

func (e *Executor) waitForDaemonExit(ctx context.Context) error {
	done := make(chan error, 1)
	go func() {
		done <- executorWaitProcess(e.daemon)
	}()
	select {
	case err := <-done:
		return err
	case <-time.After(executorShutdownTimeout):
		killErr := e.daemon.Kill()
		select {
		case err := <-done:
			return errors.Join(killErr, err)
		case <-ctx.Done():
			return errors.Join(killErr, ctx.Err())
		}
	case <-ctx.Done():
		killErr := e.daemon.Kill()
		return errors.Join(killErr, ctx.Err())
	}
}

func daemonExitAllowed(err error) bool {
	if err == nil {
		return true
	}
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		return false
	}
	status, ok := exitErr.Sys().(syscall.WaitStatus)
	return ok && status.Signaled() && (status.Signal() == syscall.SIGTERM || status.Signal() == syscall.SIGKILL)
}

func daemonExitAllowedAfterStop(err error) bool {
	if daemonExitAllowed(err) {
		return true
	}
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		return false
	}
	status, ok := exitErr.Sys().(syscall.WaitStatus)
	return ok && status.Exited() && status.ExitStatus() == 1
}

func processAlive(process *Process) bool {
	if process == nil || process.PID <= 0 {
		return false
	}
	err := executorSignalProcess(process, syscall.Signal(0))
	return err == nil || errors.Is(err, syscall.EPERM)
}

func (e *Executor) waitForCgroupEmpty(ctx context.Context) error {
	deadline := time.Now().Add(executorShutdownTimeout)
	for {
		empty, err := executorCgroupEmpty(e.capabilities.BuildEgressFD)
		if err != nil {
			return err
		}
		if empty {
			return nil
		}
		if time.Now().After(deadline) {
			return errors.New("build egress cgroup not empty after cleanup")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(executorShutdownPoll):
		}
	}
}

func cleanupCgroupChildren(fd int) error {
	root, err := pathFromDirectoryFD(fd)
	if err != nil {
		return err
	}
	var children []string
	if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path != root && entry.IsDir() {
			children = append(children, path)
		}
		return nil
	}); err != nil {
		return err
	}
	sort.Slice(children, func(i, j int) bool {
		if len(children[i]) != len(children[j]) {
			return len(children[i]) > len(children[j])
		}
		return children[i] > children[j]
	})
	for _, child := range children {
		if err := os.Remove(child); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

func (e *Executor) cleanupState() error {
	for _, dir := range executorStateDirs(e.jobRoot) {
		mounts, err := mountPointsBelow(dir)
		if err != nil {
			return err
		}
		if len(mounts) != 0 {
			return fmt.Errorf("mounts survived below executor state %q: %s", dir, strings.Join(mounts, ", "))
		}
		if err := os.RemoveAll(dir); err != nil {
			return err
		}
		if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
			if err == nil {
				return fmt.Errorf("executor state path %q survived cleanup", dir)
			}
			return err
		}
	}
	return nil
}

func executorStateDirs(jobRoot string) []string {
	return []string{
		filepath.Join(jobRoot, "buildkit"),
		filepath.Join(jobRoot, "buildkit-root"),
		filepath.Join(jobRoot, "rootlesskit"),
		filepath.Join(jobRoot, "tmp"),
		filepath.Join(jobRoot, "home"),
	}
}

func mountPointsBelow(root string) ([]string, error) {
	payload, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		return nil, err
	}
	var matches []string
	for _, line := range strings.Split(string(payload), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 5 {
			continue
		}
		mountPoint := decodeMountInfoField(fields[4])
		if mountPoint == root || strings.HasPrefix(mountPoint, root+string(os.PathSeparator)) {
			matches = append(matches, mountPoint)
		}
	}
	return matches, nil
}

func decodeMountInfoField(path string) string {
	return strings.NewReplacer(`\040`, " ", `\011`, "\t", `\012`, "\n", `\134`, `\`).Replace(path)
}

func (e *Executor) prepareStateDirs() error {
	for _, name := range []string{"buildkit", "buildkit-root", "rootlesskit", "tmp", "home"} {
		if err := e.ensureJobSubdirectory(name); err != nil {
			return err
		}
	}
	return nil
}

func (e *Executor) ensureJobSubdirectory(name string) error {
	if name == "" || strings.ContainsRune(name, os.PathSeparator) || name == "." || name == ".." {
		return errors.New("executor state directory name invalid")
	}
	if err := syscall.Mkdirat(e.capabilities.JobDirectoryFD, name, 0o700); err != nil && !errors.Is(err, syscall.EEXIST) {
		return err
	}
	fd, err := syscall.Openat(e.capabilities.JobDirectoryFD, name, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	defer syscall.Close(fd)
	var statValue syscall.Stat_t
	if err := syscall.Fstat(fd, &statValue); err != nil {
		return err
	}
	if statValue.Mode&syscall.S_IFMT != syscall.S_IFDIR {
		return errors.New("executor state path is not a directory")
	}
	if os.FileMode(statValue.Mode).Perm() != 0o700 {
		return errors.New("executor state directory mode invalid")
	}
	return nil
}

func buildkitSocketPath(jobRoot string) (string, error) {
	if !filepath.IsAbs(jobRoot) || filepath.Clean(jobRoot) != jobRoot {
		return "", errors.New("job root path invalid")
	}
	socketPath := filepath.Join(jobRoot, "buildkit", "buildkitd.sock")
	if len(socketPath) > maxBuildkitUnixSocketPathBytes {
		return "", fmt.Errorf("buildkit unix socket path length %d exceeds linux limit %d", len(socketPath), maxBuildkitUnixSocketPathBytes)
	}
	return socketPath, nil
}

func (e *Executor) writeBuildkitConfig(cgroupParent string) (string, error) {
	if err := validateBuildkitCgroupParent(cgroupParent); err != nil {
		return "", err
	}
	configPath := filepath.Join(e.jobRoot, "buildkit", "buildkitd.toml")
	payload := []byte("[worker.oci]\ndefaultCgroupParent = " + strconv.Quote(cgroupParent) + "\n")
	buildkitDirFD, err := syscall.Openat(e.capabilities.JobDirectoryFD, "buildkit", syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return "", err
	}
	defer syscall.Close(buildkitDirFD)
	fd, err := createBundleFile(buildkitDirFD, "buildkitd.toml", 0o600)
	if err != nil {
		return "", err
	}
	complete := false
	defer func() {
		if !complete {
			_ = unlinkBundleFile(buildkitDirFD, "buildkitd.toml")
		}
	}()
	file := os.NewFile(uintptr(fd), configPath)
	if file == nil {
		syscall.Close(fd)
		return "", errors.New("buildkit config file unavailable")
	}
	written, writeErr := file.Write(payload)
	syncErr := file.Sync()
	closeErr := file.Close()
	if writeErr != nil {
		return "", writeErr
	}
	if written != len(payload) {
		return "", io.ErrShortWrite
	}
	if syncErr != nil {
		return "", syncErr
	}
	if closeErr != nil {
		return "", closeErr
	}
	if err := fsyncDirectory(buildkitDirFD); err != nil {
		return "", err
	}
	complete = true
	return configPath, nil
}

func removePartialOCIOutput(path string) error {
	if path == "" {
		return nil
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func buildkitCgroupParent(cgroupFD int) (string, error) {
	cgroupPath, err := pathFromDirectoryFD(cgroupFD)
	if err != nil {
		return "", err
	}
	var stat syscall.Statfs_t
	if err := syscall.Statfs(cgroupPath, &stat); err != nil {
		return "", err
	}
	if stat.Type != cgroup2SuperMagic {
		return "", errors.New("build egress descriptor is not cgroup v2")
	}
	relative, err := filepath.Rel("/sys/fs/cgroup", cgroupPath)
	if err != nil {
		return "", err
	}
	relative = filepath.ToSlash(relative)
	if err := validateBuildkitCgroupParent(relative); err != nil {
		return "", err
	}
	return relative, nil
}

func validateBuildkitCgroupParent(cgroupParent string) error {
	if cgroupParent == "" || filepath.IsAbs(cgroupParent) || filepath.Clean(cgroupParent) != cgroupParent || cgroupParent == "." {
		return errors.New("buildkit cgroup parent invalid")
	}
	for _, segment := range strings.Split(cgroupParent, "/") {
		if segment == "" || segment == "." || segment == ".." {
			return errors.New("buildkit cgroup parent invalid")
		}
	}
	for _, r := range cgroupParent {
		if r == '/' || r == ':' || r == '_' || r == '-' || r == '.' || r == '@' || r >= '0' && r <= '9' || r >= 'A' && r <= 'Z' || r >= 'a' && r <= 'z' {
			continue
		}
		return errors.New("buildkit cgroup parent contains invalid character")
	}
	return nil
}

func verifyHostIDMapHelpers() error {
	if err := verifyHostIDMapHelper(hostNewuidmapPath, syscall.S_ISUID); err != nil {
		return fmt.Errorf("%s invalid: %w", hostNewuidmapPath, err)
	}
	if err := verifyHostIDMapHelper(hostNewgidmapPath, syscall.S_ISUID); err != nil {
		return fmt.Errorf("%s invalid: %w", hostNewgidmapPath, err)
	}
	if err := verifyHostIDMapHelper(hostNsenterPath, 0); err != nil {
		return fmt.Errorf("%s invalid: %w", hostNsenterPath, err)
	}
	if err := verifyHostIDMapHelper(hostIPPath, 0); err != nil {
		return fmt.Errorf("%s invalid: %w", hostIPPath, err)
	}
	return nil
}

func verifyHostIDMapHelper(path string, requiredSetID uint32) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return errors.New("host helper path invalid")
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("host helper must be a regular non-symlink file")
	}
	statValue, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return errors.New("host helper stat identity unavailable")
	}
	if statValue.Uid != 0 {
		return errors.New("host helper must be root-owned")
	}
	if requiredSetID != 0 && statValue.Mode&requiredSetID == 0 {
		return errors.New("host helper missing required setid bit")
	}
	if info.Mode().Perm()&0o111 == 0 {
		return errors.New("host helper must be executable")
	}
	if os.FileMode(statValue.Mode).Perm()&0o022 != 0 {
		return errors.New("host helper must not be group or world writable")
	}
	return nil
}

func verifyExecutableFile(member ExecutableMember) error {
	if !filepath.IsAbs(member.Path) || filepath.Clean(member.Path) != member.Path {
		return errors.New("executable path invalid")
	}
	if !isDigest(member.SHA256) {
		return errors.New("executable digest invalid")
	}
	info, err := os.Lstat(member.Path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("executable must be a regular non-symlink file")
	}
	if info.Mode().Perm() != memberExecutableMode {
		return errors.New("executable mode invalid")
	}
	file, err := os.Open(member.Path)
	if err != nil {
		return err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return err
	}
	if hex.EncodeToString(hash.Sum(nil)) != member.SHA256 {
		return errors.New("executable digest mismatch")
	}
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
	return cgroupTreeEmpty(root)
}

func cgroupTreeEmpty(root string) (bool, error) {
	empty := true
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || entry.Name() != "cgroup.procs" {
			return nil
		}
		payload, err := os.ReadFile(path)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if strings.TrimSpace(string(payload)) != "" {
			empty = false
			return filepath.SkipAll
		}
		return nil
	})
	return empty, err
}
