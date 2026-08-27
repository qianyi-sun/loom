//go:build !linux

package main

import (
	"fmt"
	"os"
)

func hardenRuntimeIdentity() error {
	return nil
}

func readRegularOutputFile(path string) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("output is not a regular file")
	}
	return os.ReadFile(path)
}
