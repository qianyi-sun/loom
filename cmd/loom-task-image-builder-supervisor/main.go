package main

import (
	"errors"
	"os"
)

type startupOptions struct {
	GrantID string
}

var (
	compiledConfigPath      = "/etc/loom-task-image-builder/supervisor-config.json"
	compiledReleaseSHA256   = ""
	compiledReleaseBasePath = "/opt/loom-task-image-builder-provider/releases"
)

func main() {
	if err := run(os.Args[1:], os.Environ()); err != nil {
		os.Exit(1)
	}
}

func run(args []string, environ []string) error {
	if _, err := parseArguments(args); err != nil {
		return err
	}
	if compiledReleaseSHA256 == "" {
		return errors.New("supervisor release digest not compiled")
	}
	_, _ = environ, args
	_, err := LoadConfig(compiledConfigPath, compiledReleaseSHA256)
	return err
}
