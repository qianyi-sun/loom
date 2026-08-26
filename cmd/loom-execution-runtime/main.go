// Command loom-execution-runtime is PID 1 inside one Kubernetes attempt container.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "materialize" {
		if err := materialize(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		return
	}
	planPath := flag.String("plan", "/etc/loom/execution-plan.json", "immutable execution plan")
	workspace := flag.String("workspace", "/workspace", "bounded workspace volume")
	outputRoot := flag.String("output-root", "/loom/output", "bounded evidence volume")
	terminationMessage := flag.String(
		"termination-message",
		"/loom/output/termination-message",
		"bounded Kubernetes termination summary",
	)
	flag.Parse()

	p, err := loadPlan(*planPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
	cleanOutput := filepath.Clean(*outputRoot)
	cleanTerminationMessage := filepath.Clean(*terminationMessage)
	if !isWithin(cleanOutput, cleanTerminationMessage) {
		fmt.Fprintln(os.Stderr, "termination message must be inside output root")
		os.Exit(2)
	}
	result, runErr := runPlan(ctx, p, filepath.Clean(*workspace), cleanOutput)
	if err := writeResult(filepath.Join(cleanOutput, "result.json"), result); err != nil {
		fmt.Fprintln(os.Stderr, "write result:", err)
		os.Exit(3)
	}
	if err := writeTerminationSummary(cleanTerminationMessage, result); err != nil {
		fmt.Fprintln(os.Stderr, "write termination summary:", err)
		os.Exit(3)
	}
	if runErr != nil {
		fmt.Fprintln(os.Stderr, runErr)
		os.Exit(1)
	}
}
