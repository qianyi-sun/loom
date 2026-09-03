package main

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestArgumentsAcceptExactCanonicalGrantID(t *testing.T) {
	const grantID = "11111111-1111-4111-8111-111111111111"

	options, err := parseArguments([]string{"--grant-id", grantID})
	if err != nil {
		t.Fatalf("parseArguments() error = %v", err)
	}
	if options.GrantID != grantID {
		t.Fatalf("GrantID = %q, want %q", options.GrantID, grantID)
	}
}

func TestArgumentsRejectUnknownOrMissingAuthority(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		args []string
	}{
		{name: "unknown flag", args: []string{"--grant-id", "11111111-1111-4111-8111-111111111111", "--socket", "/tmp/x"}},
		{name: "missing value", args: []string{"--grant-id"}},
		{name: "wrong flag", args: []string{"--grant", "11111111-1111-4111-8111-111111111111"}},
	}
	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if _, err := parseArguments(tt.args); err == nil {
				t.Fatal("parseArguments() succeeded, want error")
			}
		})
	}
}

func TestArgumentsRejectZeroAndNonCanonicalGrantIDs(t *testing.T) {
	t.Parallel()

	for _, grantID := range []string{
		"00000000-0000-0000-0000-000000000000",
		"11111111-1111-4111-8111-111111111111 ",
		"11111111-1111-4111-8111-11111111111",
		strings.ToUpper("aaaaaaaa-1111-4111-8111-111111111111"),
	} {
		grantID := grantID
		t.Run(grantID, func(t *testing.T) {
			t.Parallel()
			if _, err := parseArguments([]string{"--grant-id", grantID}); err == nil {
				t.Fatal("parseArguments() succeeded, want error")
			}
		})
	}
}

func TestArgumentsRejectInheritedEnvironmentAuthority(t *testing.T) {
	t.Parallel()

	if _, err := sanitizeEnvironment([]string{"UNSAFE_KEY=value"}, "/job/root"); err == nil {
		t.Fatal("sanitizeEnvironment() succeeded, want error")
	}
	if _, err := sanitizeEnvironment([]string{"HOME=/tmp/unsafe"}, "/job/root"); err == nil {
		t.Fatal("sanitizeEnvironment() accepted inherited HOME")
	}
	if _, err := sanitizeEnvironment([]string{"TMPDIR=/tmp/unsafe"}, "/job/root"); err == nil {
		t.Fatal("sanitizeEnvironment() accepted inherited TMPDIR")
	}
}

func TestArgumentsConstructFixedEnvironmentBelowQuotaDirectory(t *testing.T) {
	t.Parallel()

	env, err := sanitizeEnvironment([]string{
		"SLURM_JOB_ID=12345",
		"SLURM_JOB_UID=993",
		"SLURM_JOB_GID=980",
		"SLURM_JOB_USER=loom-builder",
		"SLURM_CLUSTER_NAME=gb10",
		"SLURMD_NODENAME=trt-gb10-1",
	}, "/quota/root")
	if err != nil {
		t.Fatalf("sanitizeEnvironment() error = %v", err)
	}

	got := map[string]string{}
	for _, entry := range env {
		name, value, found := strings.Cut(entry, "=")
		if !found {
			t.Fatalf("environment entry %q missing '='", entry)
		}
		got[name] = value
	}
	if got["HOME"] != "/quota/root/home" {
		t.Fatalf("HOME = %q, want /quota/root/home", got["HOME"])
	}
	if got["TMPDIR"] != "/quota/root/tmp" {
		t.Fatalf("TMPDIR = %q, want /quota/root/tmp", got["TMPDIR"])
	}
	if got["LANG"] != "C.UTF-8" {
		t.Fatalf("LANG = %q, want C.UTF-8", got["LANG"])
	}
	if got["TZ"] != "UTC" {
		t.Fatalf("TZ = %q, want UTC", got["TZ"])
	}
	if got["SLURM_JOB_ID"] != "12345" {
		t.Fatalf("SLURM_JOB_ID = %q, want 12345", got["SLURM_JOB_ID"])
	}
	if len(got) != 10 {
		t.Fatalf("len(env) = %d, want 10; env = %#v", len(got), got)
	}
}

func TestConfigLoadAcceptsExactVerifiedRelease(t *testing.T) {
	root := t.TempDir()
	release := strings.Repeat("a", 64)
	useTestConfigPolicy(t, root)
	paths := makeReleaseTree(t, root, release)
	configPath := writeConfigFixture(t, root, release, runtime.GOARCH, paths, nil)

	cfg, err := LoadConfig(configPath, release)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	if cfg.ReleaseSHA256 != release {
		t.Fatalf("ReleaseSHA256 = %q, want %q", cfg.ReleaseSHA256, release)
	}
	if cfg.Guard.SocketPath != paths.guardSocket {
		t.Fatalf("Guard.SocketPath = %q, want %q", cfg.Guard.SocketPath, paths.guardSocket)
	}
	if cfg.Runtime.RootlessKit.Path != paths.rootlesskit {
		t.Fatalf("Runtime.RootlessKit.Path = %q, want %q", cfg.Runtime.RootlessKit.Path, paths.rootlesskit)
	}
}

func TestConfigRejectsUnknownAndDuplicateFields(t *testing.T) {
	root := t.TempDir()
	release := strings.Repeat("b", 64)
	useTestConfigPolicy(t, root)
	paths := makeReleaseTree(t, root, release)
	configPath := writeConfigFixture(t, root, release, runtime.GOARCH, paths, []byte(`{
  "schema":"loom.task-image-builder-supervisor-config/v1",
  "release_sha256":"`+release+`",
  "release_sha256":"`+release+`",
  "cpu_arch":"`+runtime.GOARCH+`",
  "guard":{"socket_path":"`+paths.guardSocket+`","max_packet_bytes":4096,"ack_timeout_seconds":5,"unknown":1},
  "runtime":{"rootlesskit":{"path":"`+paths.rootlesskit+`","sha256":"`+sha256FileHex(t, paths.rootlesskit)+`"},"buildctl":{"path":"`+paths.buildctl+`","sha256":"`+sha256FileHex(t, paths.buildctl)+`"},"buildkitd":{"path":"`+paths.buildkitd+`","sha256":"`+sha256FileHex(t, paths.buildkitd)+`"}}
}`))

	if _, err := LoadConfig(configPath, release); err == nil {
		t.Fatal("LoadConfig() succeeded, want error")
	}
}

func TestConfigRejectsTrailingJSONGarbage(t *testing.T) {
	root := t.TempDir()
	release := strings.Repeat("1", 64)
	useTestConfigPolicy(t, root)
	paths := makeReleaseTree(t, root, release)
	configPath := writeConfigFixture(t, root, release, runtime.GOARCH, paths, nil)

	payload, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("ReadFile(%q) error = %v", configPath, err)
	}
	if err := os.WriteFile(configPath, append(payload, []byte("\n{}")...), 0o444); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", configPath, err)
	}

	if _, err := LoadConfig(configPath, release); err == nil {
		t.Fatal("LoadConfig() succeeded, want trailing-document error")
	}
}

func TestConfigRejectsSymlinkWritableChangedInodeWrongReleaseWrongArchAndNonContentAddressedPaths(t *testing.T) {
	root := t.TempDir()
	release := strings.Repeat("c", 64)
	useTestConfigPolicy(t, root)
	makeReleaseTree(t, root, release)

	tests := []struct {
		name       string
		mutate     func(string)
		wantErrSub string
	}{
		{
			name: "symlink config",
			mutate: func(configPath string) {
				target := configPath + ".real"
				if err := os.Rename(configPath, target); err != nil {
					t.Fatalf("Rename() error = %v", err)
				}
				if err := os.Symlink(target, configPath); err != nil {
					t.Fatalf("Symlink() error = %v", err)
				}
			},
		},
		{
			name: "writable config",
			mutate: func(configPath string) {
				if err := os.Chmod(configPath, 0o644); err != nil {
					t.Fatalf("Chmod() error = %v", err)
				}
			},
		},
		{
			name: "changed inode",
			mutate: func(configPath string) {
				loadConfigPreOpenHook = func() error {
					payload, err := os.ReadFile(configPath)
					if err != nil {
						return err
					}
					tmp := configPath + ".swap"
					if err := os.WriteFile(tmp, payload, 0o444); err != nil {
						return err
					}
					return os.Rename(tmp, configPath)
				}
			},
		},
		{
			name: "wrong release hash",
			mutate: func(configPath string) {
				overwriteConfigValue(t, configPath, release, strings.Repeat("d", 64))
			},
		},
		{
			name: "wrong arch",
			mutate: func(configPath string) {
				replaceFileText(t, configPath, `"cpu_arch":"`+runtime.GOARCH+`"`, `"cpu_arch":"mips64"`)
			},
		},
		{
			name: "non content addressed path",
			mutate: func(configPath string) {
				replaceFileText(
					t,
					configPath,
					filepath.Join("releases", release, "bin", "rootlesskit"),
					filepath.Join("bin", "rootlesskit"),
				)
			},
		},
		{
			name: "writable release directory",
			mutate: func(configPath string) {
				sandbox := filepath.Dir(configPath)
				if err := os.Chmod(filepath.Join(sandbox, "releases", release), 0o755); err != nil {
					t.Fatalf("Chmod() error = %v", err)
				}
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			sandbox := t.TempDir()
			useTestConfigPolicy(t, sandbox)
			localPaths := makeReleaseTree(t, sandbox, release)
			configPath := writeConfigFixture(t, sandbox, release, runtime.GOARCH, localPaths, nil)
			tt.mutate(configPath)
			t.Cleanup(func() { loadConfigPreOpenHook = nil })
			if _, err := LoadConfig(configPath, release); err == nil {
				t.Fatal("LoadConfig() succeeded, want error")
			}
		})
	}
}

func TestConfigAllowsFixedGuardSocketOutsideRelease(t *testing.T) {
	root := t.TempDir()
	release := strings.Repeat("2", 64)
	useTestConfigPolicy(t, root)
	paths := makeReleaseTree(t, root, release)
	paths.guardSocket = filepath.Join(root, "var", "run", "loom-task-image-builder", "guard.sock")
	if err := os.MkdirAll(filepath.Dir(paths.guardSocket), 0o755); err != nil {
		t.Fatalf("MkdirAll(%q) error = %v", filepath.Dir(paths.guardSocket), err)
	}
	configPath := writeConfigFixture(t, root, release, runtime.GOARCH, paths, nil)

	cfg, err := LoadConfig(configPath, release)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	if cfg.Guard.SocketPath != paths.guardSocket {
		t.Fatalf("Guard.SocketPath = %q, want %q", cfg.Guard.SocketPath, paths.guardSocket)
	}
}

func TestConfigRejectsRelativeGuardSocketPath(t *testing.T) {
	root := t.TempDir()
	release := strings.Repeat("3", 64)
	useTestConfigPolicy(t, root)
	paths := makeReleaseTree(t, root, release)
	paths.guardSocket = "relative/guard.sock"
	configPath := writeConfigFixture(t, root, release, runtime.GOARCH, paths, nil)

	if _, err := LoadConfig(configPath, release); err == nil {
		t.Fatal("LoadConfig() succeeded, want guard socket path error")
	}
}

func TestConfigRejectsDigestConfusionBetweenManifestAndELFMembers(t *testing.T) {
	root := t.TempDir()
	release := strings.Repeat("e", 64)
	useTestConfigPolicy(t, root)
	paths := makeReleaseTree(t, root, release)
	configPath := writeConfigFixture(t, root, release, runtime.GOARCH, paths, nil)

	replaceFileText(
		t,
		configPath,
		`"sha256":"`+sha256FileHex(t, paths.rootlesskit)+`"`,
		`"sha256":"`+release+`"`,
	)

	if _, err := LoadConfig(configPath, release); err == nil {
		t.Fatal("LoadConfig() succeeded, want error")
	}
}

func TestConfigRejectsExecutableMemberWithIntermediateSymlink(t *testing.T) {
	root := t.TempDir()
	release := strings.Repeat("4", 64)
	useTestConfigPolicy(t, root)
	paths := makeReleaseTree(t, root, release)
	releaseRoot := filepath.Join(root, "releases", release)
	binDir := filepath.Join(releaseRoot, "bin")
	outsideBin := filepath.Join(root, "outside-bin")
	if err := os.Mkdir(outsideBin, 0o755); err != nil {
		t.Fatalf("Mkdir(%q) error = %v", outsideBin, err)
	}
	outsideRootlesskit := filepath.Join(outsideBin, "rootlesskit")
	writeExecutableFixture(t, outsideRootlesskit, "#!/bin/sh\necho outside\n")
	if err := os.Chmod(releaseRoot, 0o755); err != nil {
		t.Fatalf("Chmod(%q) error = %v", releaseRoot, err)
	}
	if err := os.RemoveAll(binDir); err != nil {
		t.Fatalf("RemoveAll(%q) error = %v", binDir, err)
	}
	if err := os.Symlink(outsideBin, binDir); err != nil {
		t.Fatalf("Symlink(%q, %q) error = %v", outsideBin, binDir, err)
	}
	if err := os.Chmod(releaseRoot, 0o555); err != nil {
		t.Fatalf("Chmod(%q) error = %v", releaseRoot, err)
	}
	paths.rootlesskit = filepath.Join(binDir, "rootlesskit")
	configPath := writeConfigFixture(t, root, release, runtime.GOARCH, paths, nil)
	replaceFileText(
		t,
		configPath,
		`"sha256":"`+sha256FileHex(t, paths.rootlesskit)+`"`,
		`"sha256":"`+sha256FileHex(t, outsideRootlesskit)+`"`,
	)

	if _, err := LoadConfig(configPath, release); err == nil {
		t.Fatal("LoadConfig() succeeded, want intermediate-symlink rejection")
	}
}

type releasePaths struct {
	guardSocket string
	rootlesskit string
	buildctl    string
	buildkitd   string
}

func makeReleaseTree(t *testing.T, root string, release string) releasePaths {
	t.Helper()

	releaseRoot := filepath.Join(root, "releases", release)
	binDir := filepath.Join(releaseRoot, "bin")
	runtimeDir := filepath.Join(releaseRoot, "runtime")
	runDir := filepath.Join(releaseRoot, "run")
	for _, dir := range []string{
		filepath.Join(root, "releases"),
		releaseRoot,
		binDir,
		runtimeDir,
		runDir,
	} {
		if err := os.Mkdir(dir, 0o755); err != nil {
			t.Fatalf("Mkdir(%q) error = %v", dir, err)
		}
	}

	writeExecutableFixture(t, filepath.Join(binDir, "rootlesskit"), "#!/bin/sh\nexit 0\n")
	writeExecutableFixture(t, filepath.Join(runtimeDir, "buildctl"), "buildctl\n")
	writeExecutableFixture(t, filepath.Join(runtimeDir, "buildkitd"), "buildkitd\n")
	writeExecutableFixture(t, filepath.Join(runDir, "guard.sock"), "socket-placeholder\n")
	for _, dir := range []string{binDir, runtimeDir, runDir, releaseRoot} {
		if err := os.Chmod(dir, 0o555); err != nil {
			t.Fatalf("Chmod(%q) error = %v", dir, err)
		}
	}

	return releasePaths{
		guardSocket: filepath.Join(runDir, "guard.sock"),
		rootlesskit: filepath.Join(binDir, "rootlesskit"),
		buildctl:    filepath.Join(runtimeDir, "buildctl"),
		buildkitd:   filepath.Join(runtimeDir, "buildkitd"),
	}
}

func writeExecutableFixture(t *testing.T, path string, body string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(body), 0o555); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", path, err)
	}
}

func writeConfigFixture(t *testing.T, root string, release string, arch string, paths releasePaths, raw []byte) string {
	t.Helper()
	configPath := filepath.Join(root, "supervisor-config.json")
	if raw == nil {
		raw = []byte(`{"schema":"loom.task-image-builder-supervisor-config/v1","release_sha256":"` + release + `","cpu_arch":"` + arch + `","guard":{"socket_path":"` + paths.guardSocket + `","max_packet_bytes":4096,"ack_timeout_seconds":5},"runtime":{"rootlesskit":{"path":"` + paths.rootlesskit + `","sha256":"` + sha256FileHex(t, paths.rootlesskit) + `"},"buildctl":{"path":"` + paths.buildctl + `","sha256":"` + sha256FileHex(t, paths.buildctl) + `"},"buildkitd":{"path":"` + paths.buildkitd + `","sha256":"` + sha256FileHex(t, paths.buildkitd) + `"}}}`)
	}
	if err := os.WriteFile(configPath, raw, 0o444); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", configPath, err)
	}
	return configPath
}

func sha256FileHex(t *testing.T, path string) string {
	t.Helper()
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%q) error = %v", path, err)
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func replaceFileText(t *testing.T, path string, old string, new string) {
	t.Helper()
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%q) error = %v", path, err)
	}
	updated := strings.ReplaceAll(string(payload), old, new)
	if err := os.WriteFile(path, []byte(updated), 0o444); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", path, err)
	}
}

func overwriteConfigValue(t *testing.T, path string, old string, new string) {
	t.Helper()
	replaceFileText(t, path, `"release_sha256":"`+old+`"`, `"release_sha256":"`+new+`"`)
}

func useTestConfigPolicy(t *testing.T, root string) {
	t.Helper()

	previousUID := requiredOwnerUID
	previousBase := compiledReleaseBasePath
	requiredOwnerUID = uint32(os.Geteuid())
	compiledReleaseBasePath = filepath.Join(root, "releases")
	t.Cleanup(func() {
		requiredOwnerUID = previousUID
		compiledReleaseBasePath = previousBase
		loadConfigPreOpenHook = nil
	})
}
