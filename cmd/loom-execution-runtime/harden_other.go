//go:build !linux

package main

import (
	"fmt"
	"io"
	"os"
)

func hardenRuntimeIdentity() error {
	return nil
}

func openRegularOutputFile(path string) (*os.File, os.FileInfo, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, nil, fmt.Errorf("output is not a regular file")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	return file, info, nil
}

func readRegularOutputFile(path string) ([]byte, error) {
	file, _, err := openRegularOutputFile(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return io.ReadAll(file)
}
