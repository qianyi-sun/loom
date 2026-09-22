//go:build linux

package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// The start-time identity prevents a later, reused PID from receiving CONT.
type pausedProcesses map[int]string

func processStartTime(pid int) (string, error) {
	data, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "stat"))
	if err != nil {
		return "", err
	}
	end := strings.LastIndexByte(string(data), ')')
	if end < 0 {
		return "", errCleanupProcRead
	}
	fields := strings.Fields(string(data[end+1:]))
	if len(fields) < 20 {
		return "", errCleanupProcRead
	}
	return fields[19], nil
}

func resumeProcesses(paused pausedProcesses) error {
	if os.Getpid() != 1 {
		return errCleanupPIDNamespace
	}
	var failure error
	for pid, identity := range paused {
		current, err := processStartTime(pid)
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ESRCH) {
			continue
		}
		if err != nil {
			failure = errCleanupProcRead
			continue
		}
		if current != identity {
			continue
		}
		if err := syscall.Kill(pid, syscall.SIGCONT); err != nil && !errors.Is(err, syscall.ESRCH) {
			failure = errCleanupProcRead
		}
	}
	return failure
}

func pauseProcesses(parent context.Context) (paused pausedProcesses, failure error) {
	if os.Getpid() != 1 {
		return nil, errCleanupPIDNamespace
	}
	paused = pausedProcesses{}
	defer func() {
		if failure != nil {
			_ = resumeProcesses(paused)
		}
	}()
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	for {
		entries, err := os.ReadDir("/proc")
		if err != nil {
			return paused, errCleanupProcRead
		}
		settled := true
		for _, entry := range entries {
			pid, err := strconv.Atoi(entry.Name())
			if err != nil || pid <= 1 {
				continue
			}
			state, err := sandboxProcessState(filepath.Join("/proc", entry.Name()), os.Geteuid())
			if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ESRCH) {
				continue
			}
			if err != nil {
				return paused, err
			}
			if state == "" || state == "Z" || state == "T" || state == "t" {
				continue
			}
			identity, err := processStartTime(pid)
			if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ESRCH) {
				continue
			}
			if err != nil {
				return paused, errCleanupProcRead
			}
			if err := syscall.Kill(pid, syscall.SIGSTOP); err != nil {
				if errors.Is(err, syscall.ESRCH) {
					continue
				}
				return paused, errCleanupProcRead
			}
			paused[pid] = identity
			settled = false
		}
		if settled {
			return paused, nil
		}
		select {
		case <-ctx.Done():
			return paused, ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
}
