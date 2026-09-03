package main

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestRunRejectsInheritedEnvironmentAuthority(t *testing.T) {
	root := t.TempDir()
	release := strings.Repeat("f", 64)
	useTestConfigPolicy(t, root)
	paths := makeReleaseTree(t, root, release)
	configPath := writeConfigFixture(t, root, release, runtime.GOARCH, paths, nil)

	previousConfigPath := compiledConfigPath
	previousRelease := compiledReleaseSHA256
	compiledConfigPath = configPath
	compiledReleaseSHA256 = release
	t.Cleanup(func() {
		compiledConfigPath = previousConfigPath
		compiledReleaseSHA256 = previousRelease
	})

	err := run([]string{"--grant-id", "11111111-1111-4111-8111-111111111111"}, []string{"UNSAFE_KEY=value"})
	if err == nil {
		t.Fatal("run() succeeded, want inherited environment rejection")
	}
}

func TestRunConstructsEnvironmentFromProjectedQuotaDirectory(t *testing.T) {
	root := t.TempDir()
	release := strings.Repeat("0", 63) + "1"
	useTestConfigPolicy(t, root)
	paths := makeReleaseTree(t, root, release)
	configPath := writeConfigFixture(t, root, release, runtime.GOARCH, paths, nil)
	jobRoot := filepath.Join(root, "projected-job")
	if err := os.Mkdir(jobRoot, 0o755); err != nil {
		t.Fatalf("Mkdir(%q) error = %v", jobRoot, err)
	}
	buildRoot := filepath.Join(root, "projected-egress")
	if err := os.Mkdir(buildRoot, 0o755); err != nil {
		t.Fatalf("Mkdir(%q) error = %v", buildRoot, err)
	}

	previousConfigPath := compiledConfigPath
	previousRelease := compiledReleaseSHA256
	previousFactory := guardClientFactory
	previousApply := applyProcessEnvironment
	compiledConfigPath = configPath
	compiledReleaseSHA256 = release
	var applied []string
	guardClientFactory = func(cfg Config) supervisorProjectClient {
		return supervisorProjectClientFunc(func(ctx context.Context, grantID string) (*AllocationCapabilities, error) {
			bootstrapFD := createMemfdFixture(t, "bootstrap", []byte(`{"bootstrap_token":"sentinel-secret-text"}`), requiredMemfdSeals, true)
			bootstrap, err := NewSecretBuffer(bootstrapFD, maxSecretBytes)
			if err != nil {
				t.Fatalf("NewSecretBuffer() error = %v", err)
			}
			jobFD := openDirectoryFD(t, jobRoot)
			jobStat := mustFstat(t, jobFD)
			buildFD := openDirectoryFD(t, buildRoot)
			buildStat := mustFstat(t, buildFD)
			return &AllocationCapabilities{
				Bootstrap:          bootstrap,
				JobDirectoryFD:     jobFD,
				JobDirectoryDevice: uint64(jobStat.Dev),
				JobDirectoryInode:  uint64(jobStat.Ino),
				BuildEgressFD:      buildFD,
				BuildEgressDevice:  uint64(buildStat.Dev),
				BuildEgressInode:   uint64(buildStat.Ino),
			}, nil
		})
	}
	applyProcessEnvironment = func(env []string) error {
		applied = append([]string(nil), env...)
		return nil
	}
	t.Cleanup(func() {
		compiledConfigPath = previousConfigPath
		compiledReleaseSHA256 = previousRelease
		guardClientFactory = previousFactory
		applyProcessEnvironment = previousApply
	})

	err := run([]string{"--grant-id", "11111111-1111-4111-8111-111111111111"}, []string{
		"SLURM_JOB_ID=12345",
		"SLURM_JOB_UID=993",
		"SLURM_JOB_GID=980",
		"SLURM_JOB_USER=loom-builder",
		"SLURM_CLUSTER_NAME=gb10",
		"SLURMD_NODENAME=trt-gb10-1",
	})
	if err != nil {
		t.Fatalf("run() error = %v", err)
	}

	got := map[string]string{}
	for _, entry := range applied {
		name, value, found := strings.Cut(entry, "=")
		if !found {
			t.Fatalf("invalid environment entry %q", entry)
		}
		got[name] = value
	}
	if got["HOME"] != filepath.Join(jobRoot, "home") {
		t.Fatalf("HOME = %q, want %q", got["HOME"], filepath.Join(jobRoot, "home"))
	}
	if got["TMPDIR"] != filepath.Join(jobRoot, "tmp") {
		t.Fatalf("TMPDIR = %q, want %q", got["TMPDIR"], filepath.Join(jobRoot, "tmp"))
	}
	if got["HOME"] == filepath.Join(filepath.Dir(paths.guardSocket), "home") {
		t.Fatalf("HOME derived from guard socket path: %q", got["HOME"])
	}
	if got["TZ"] != "UTC" || got["LANG"] != "C.UTF-8" {
		t.Fatalf("fixed environment missing: %#v", got)
	}
}

type supervisorProjectClientFunc func(context.Context, string) (*AllocationCapabilities, error)

func (fn supervisorProjectClientFunc) Project(ctx context.Context, grantID string) (*AllocationCapabilities, error) {
	return fn(ctx, grantID)
}
