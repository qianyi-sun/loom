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
	"time"
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
	broker, err := workloadBrokerFromEnvironment()
	if err != nil {
		fmt.Fprintln(os.Stderr, "initialize workload broker:", err)
		os.Exit(2)
	}
	if err := hardenRuntimeIdentity(); err != nil {
		fmt.Fprintln(os.Stderr, "harden workload broker identity:", err)
		os.Exit(2)
	}
	if p.TaskInput != nil {
		inputContext, stopInput := context.WithTimeout(ctx, 10*time.Minute)
		err = broker.materializeInputs(inputContext, p, filepath.Clean(*workspace))
		stopInput()
		if err != nil {
			fmt.Fprintln(os.Stderr, "materialize task input:", err)
			os.Exit(2)
		}
	}
	proxyURL, stopProxy, err := broker.startProxy(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "start workload proxy:", err)
		os.Exit(2)
	}
	defer func() { _ = stopProxy() }()
	result, runErr := runPlan(
		ctx,
		p,
		filepath.Clean(*workspace),
		cleanOutput,
		trustedGatewayEnvironment(proxyURL),
	)
	captureErr := captureDeclaredOutputs(p, filepath.Clean(*workspace), cleanOutput, &result)
	if captureErr != nil {
		result.FinishedAt = time.Now().UTC()
		fmt.Fprintln(os.Stderr, "capture complete trial bundle:", captureErr)
	}
	if err := writeResult(filepath.Join(cleanOutput, "result.json"), result); err != nil {
		fmt.Fprintln(os.Stderr, "write result:", err)
		os.Exit(3)
	}
	commitContext, stopCommit := context.WithTimeout(context.Background(), 5*time.Minute)
	evidence, commitErr := broker.commitOutputs(commitContext, cleanOutput, broker.outputRequestID())
	stopCommit()
	if commitErr != nil {
		// A successful command without durable output is a runtime failure. Rewrite
		// the local semantic result before publishing the bounded termination summary.
		if result.Status == "succeeded" {
			result.Status = "runtime_error"
			result.PartialEvidence = true
			result.FinishedAt = time.Now().UTC()
			if err := writeResult(filepath.Join(cleanOutput, "result.json"), result); err != nil {
				fmt.Fprintln(os.Stderr, "rewrite failed output result:", err)
				os.Exit(3)
			}
		}
		fmt.Fprintln(os.Stderr, "commit runtime output:", commitErr)
	}
	var committed *outputCommitEvidence
	if commitErr == nil {
		committed = &evidence
	}
	if err := writeTerminationSummary(cleanTerminationMessage, result, committed); err != nil {
		fmt.Fprintln(os.Stderr, "write termination summary:", err)
		os.Exit(3)
	}
	if commitErr != nil {
		os.Exit(3)
	}
	if runErr != nil {
		fmt.Fprintln(os.Stderr, runErr)
		os.Exit(1)
	}
	if captureErr != nil {
		os.Exit(1)
	}
}
