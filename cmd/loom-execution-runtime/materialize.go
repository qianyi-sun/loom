package main

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"path/filepath"
)

func materialize(arguments []string) error {
	flags := flag.NewFlagSet("materialize", flag.ContinueOnError)
	encodedPlan := flags.String("encoded-plan", "", "base64-encoded immutable plan")
	runtimeDestination := flags.String("runtime-dest", "/loom/runtime/loom-execution-runtime", "runtime binary destination")
	planDestination := flags.String("plan-dest", "/loom/runtime/execution-plan.json", "plan destination")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if len(*encodedPlan) == 0 || len(*encodedPlan) > 512*1024 {
		return fmt.Errorf("encoded plan size is invalid")
	}
	payload, err := base64.RawURLEncoding.DecodeString(*encodedPlan)
	if err != nil {
		return fmt.Errorf("decode encoded plan: %w", err)
	}
	p, err := decodePlan(payload)
	if err != nil {
		return err
	}
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	binary, err := os.ReadFile(executable)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(binary)
	actual := "sha256:" + hex.EncodeToString(digest[:])
	if actual != p.RuntimeBinarySHA256 {
		return fmt.Errorf("runtime binary digest mismatch")
	}
	for _, destination := range []string{*runtimeDestination, *planDestination} {
		if !filepath.IsAbs(destination) || filepath.Clean(destination) != destination {
			return fmt.Errorf("materialization destination must be a clean absolute path")
		}
		if err := secureDirectory(filepath.Dir(destination)); err != nil {
			return err
		}
	}
	if err := writeExclusive(*runtimeDestination, binary, 0o555); err != nil {
		return err
	}
	if err := writeExclusive(*planDestination, payload, 0o444); err != nil {
		_ = os.Remove(*runtimeDestination)
		return err
	}
	return nil
}

func writeExclusive(path string, payload []byte, mode os.FileMode) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, mode)
	if err != nil {
		return err
	}
	if _, err := file.Write(payload); err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return err
	}
	return file.Close()
}
