//go:build !linux

package main

import (
	"context"
	"errors"
	"io"
	"os"
)

func stopProcesses(context.Context) error {
	return errors.New("sandbox process cleanup requires Linux PID 1")
}

func readFile(string) (*os.File, error) {
	return nil, errors.New("secure file transfers require Linux")
}
func writeFile(string, io.Reader, uint32) error {
	return errors.New("secure file transfers require Linux")
}
