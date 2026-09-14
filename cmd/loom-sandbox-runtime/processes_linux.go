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

func stopProcesses(ctx context.Context) error {
	// The runtime must be PID 1 of its own container. Never run this operation
	// from a host process or a Pod that shares the trusted agent's PID namespace.
	if os.Getpid() != 1 {
		return errCleanupPIDNamespace
	}
	deadline, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	for {
		entries, err := os.ReadDir("/proc")
		if err != nil {
			return errCleanupProcRead
		}
		alive := false
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
				return err
			}
			if state == "Z" {
				// Reap orphan descendants adopted by this PID 1. Active execs are
				// complete before this explicit snapshot boundary is invoked.
				var state syscall.WaitStatus
				_, _ = syscall.Wait4(pid, &state, syscall.WNOHANG, nil)
				continue
			}
			alive = true
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}
		if !alive {
			return nil
		}
		select {
		case <-deadline.Done():
			return deadline.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// Read State and effective UID from one proc status read. A proc directory's
// getattr can succeed with root ownership after its task has been reaped.
func sandboxProcessState(directory string, expectedUID int) (string, error) {
	status, err := os.ReadFile(filepath.Join(directory, "status"))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ESRCH) {
			return "", err
		}
		return "", errCleanupProcRead
	}
	state := ""
	var effectiveUID uint64
	haveUID := false
	for _, line := range strings.Split(string(status), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		switch fields[0] {
		case "State:":
			if len(fields) < 2 || len(fields[1]) != 1 {
				return "", errCleanupProcRead
			}
			state = fields[1]
		case "Uid:":
			// Linux reports real, effective, saved-set and filesystem UIDs.
			if len(fields) != 5 {
				return "", errCleanupProcRead
			}
			effectiveUID, err = strconv.ParseUint(fields[2], 10, 32)
			if err != nil {
				return "", errCleanupProcRead
			}
			haveUID = true
		}
	}
	if state == "" || !haveUID {
		return "", errCleanupProcRead
	}
	if effectiveUID != uint64(expectedUID) {
		return "", errCleanupProcessOwner
	}
	return state, nil
}
