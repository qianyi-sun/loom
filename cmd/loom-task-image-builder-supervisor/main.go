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

type supervisorProjectClient interface {
	Project(context.Context, string) (*AllocationCapabilities, error)
}

var (
	compiledConfigPath      = "/etc/loom-task-image-builder/supervisor-config.json"
	compiledGuardSocketPath = "/run/loom-task-image-builder-guard/guard.sock"
	compiledReleaseSHA256   = ""
	compiledReleaseBasePath = "/opt/loom-task-image-builder-provider/releases"
	guardClientFactory      = func(cfg Config) supervisorProjectClient {
		return NewGuardClient(cfg.Guard.SocketPath, cfg.Guard.MaxPacketBytes, time.Duration(cfg.Guard.AckTimeoutSeconds)*time.Second)
	}
	applyProcessEnvironment      = replaceProcessEnvironment
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
	client := guardClientFactory(cfg)
	ctx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()
	caps, err := client.Project(ctx, options.GrantID)
	if err != nil {
		return err
	}
	defer caps.Close()
	if finisher, ok := client.(interface {
		Finish(context.Context, string, string, map[string]int) error
	}); ok {
		operationID, idErr := newUUID()
		if idErr != nil {
			return idErr
		}
		defer func() {
			finishCtx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.Guard.AckTimeoutSeconds)*time.Second)
			defer cancel()
			_ = finisher.Finish(finishCtx, options.GrantID, operationID, map[string]int{
				"descendant_processes": 0,
				"mounts":               0,
				"sockets":              0,
				"open_files":           0,
			})
		}()
	}

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
