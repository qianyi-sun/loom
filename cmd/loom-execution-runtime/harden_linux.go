//go:build linux

package main

import (
	"fmt"
	"io"
	"os"
	"syscall"
)

const prSetDumpable = 4

func hardenRuntimeIdentity() error {
	_, _, errno := syscall.Syscall6(syscall.SYS_PRCTL, prSetDumpable, 0, 0, 0, 0, 0)
	if errno != 0 {
		return fmt.Errorf("prctl(PR_SET_DUMPABLE): %w", errno)
	}
	return nil
}

func readRegularOutputFile(path string) ([]byte, error) {
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		_ = syscall.Close(fd)
		return nil, fmt.Errorf("open output file")
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("output is not a regular file")
	}
	return io.ReadAll(file)
}
