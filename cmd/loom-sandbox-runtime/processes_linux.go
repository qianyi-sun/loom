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
		return errors.New("sandbox runtime must be PID 1")
	}
	deadline, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	for {
		entries, err := os.ReadDir("/proc")
		if err != nil {
			return err
		}
		alive := false
		for _, entry := range entries {
			pid, err := strconv.Atoi(entry.Name())
			if err != nil || pid <= 1 {
				continue
			}
			info, err := os.Stat(filepath.Join("/proc", entry.Name()))
			if err != nil {
				continue
			}
			owner, ok := info.Sys().(*syscall.Stat_t)
			if !ok || int(owner.Uid) != os.Geteuid() {
				return errors.New("unexpected sandbox process owner")
			}
			status, err := os.ReadFile(filepath.Join("/proc", entry.Name(), "status"))
			if err != nil {
				continue
			}
			if strings.Contains(string(status), "State:\tZ") {
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
