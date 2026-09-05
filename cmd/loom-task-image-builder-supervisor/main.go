package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

type startupOptions struct {
	GrantID string
}

var (
	compiledConfigPath      = "/etc/loom-task-image-builder/supervisor-config.json"
	compiledGuardSocketPath = "/run/loom-task-image-builder-guard/guard.sock"
	compiledReleaseSHA256   = ""
	compiledReleaseBasePath = "/opt/loom-task-image-builder-provider/releases"
	guardClientFactory      = func(cfg Config) TaskImageGuard {
		return NewGuardClient(cfg.Guard.SocketPath, cfg.Guard.MaxPacketBytes, time.Duration(cfg.Guard.AckTimeoutSeconds)*time.Second)
	}
	applyProcessEnvironment = replaceProcessEnvironment
	// Phase 2D1 production composition stays registry-inert; tests inject the
	// opt-in registry handoff directly without adding config discovery here.
	productionPublicationHandoff = DisabledPublicationHandoff{}
	productionSupervisorNewExec  = func(cfg Config, caps *AllocationCapabilities, plan BuildPlan) (BuildExecutor, error) {
		return NewExecutor(cfg, caps, plan)
	}
	productionSupervisorDownload = realBundleDownloader{}
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
	ctx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()
	supervisor := productionOrchestrator(options.GrantID, cfg)
	supervisor.Guard = guardClientFactory(cfg)
	supervisor.PostProject = func(ctx context.Context, caps *AllocationCapabilities) error {
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
	return supervisor.Run(ctx)
}

func productionOrchestrator(grantID string, cfg Config) *Orchestrator {
	return &Orchestrator{
		GrantID:      grantID,
		Config:       cfg,
		Guard:        NewGuardClient(cfg.Guard.SocketPath, cfg.Guard.MaxPacketBytes, time.Duration(cfg.Guard.AckTimeoutSeconds)*time.Second),
		NewExecutor:  productionSupervisorNewExec,
		Download:     productionSupervisorDownload,
		Handoff:      productionPublicationHandoff,
		CleanupGrace: time.Duration(cfg.Guard.AckTimeoutSeconds) * time.Second,
	}
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
