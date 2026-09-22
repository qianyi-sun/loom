//go:build !linux

package main

import "context"

type pausedProcesses map[int]string

func pauseProcesses(context.Context) (pausedProcesses, error) { return nil, errCleanupPIDNamespace }
func resumeProcesses(pausedProcesses) error                   { return errCleanupPIDNamespace }
