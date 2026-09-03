package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type startupOptions struct {
	GrantID string
}

type supervisorProjectClient interface {
	Project(context.Context, string) (*AllocationCapabilities, error)
}

var (
	compiledConfigPath      = "/etc/loom-task-image-builder/supervisor-config.json"
	compiledReleaseSHA256   = ""
	compiledReleaseBasePath = "/opt/loom-task-image-builder-provider/releases"
	guardClientFactory      = func(cfg Config) supervisorProjectClient {
		return NewGuardClient(cfg.Guard.SocketPath, cfg.Guard.MaxPacketBytes, time.Duration(cfg.Guard.AckTimeoutSeconds)*time.Second)
	}
	applyProcessEnvironment = replaceProcessEnvironment
)

func main() {
	if err := run(os.Args[1:], os.Environ()); err != nil {
		os.Exit(1)
	}
}

func run(args []string, environ []string) error {
	options, err := parseArguments(args)
	if err != nil {
		return err
	}
	if compiledReleaseSHA256 == "" {
		return errors.New("supervisor release digest not compiled")
	}
	cfg, err := LoadConfig(compiledConfigPath, compiledReleaseSHA256)
	if err != nil {
		return err
	}
	client := guardClientFactory(cfg)
	caps, err := client.Project(context.Background(), options.GrantID)
	if err != nil {
		return err
	}
	defer caps.Close()

	quotaRoot, err := quotaRootFromDirectoryFD(caps.JobDirectoryFD)
	if err != nil {
		return err
	}
	sanitized, err := sanitizeEnvironment(environ, quotaRoot)
	if err != nil {
		return err
	}
	return applyProcessEnvironment(sanitized)
}

func quotaRootFromDirectoryFD(fd int) (string, error) {
	if fd < 0 {
		return "", errors.New("job directory descriptor invalid")
	}
	if _, err := validateDirectoryDescriptor(fd); err != nil {
		return "", err
	}
	path, err := os.Readlink(fmt.Sprintf("/proc/self/fd/%d", fd))
	if err != nil {
		return "", err
	}
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return "", errors.New("job directory path invalid")
	}
	return path, nil
}

func replaceProcessEnvironment(environ []string) error {
	os.Clearenv()
	for _, entry := range environ {
		name, value, found := strings.Cut(entry, "=")
		if !found || name == "" {
			return errors.New("environment entry invalid")
		}
		if err := os.Setenv(name, value); err != nil {
			return err
		}
	}
	return nil
}
