package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func testSandbox(t *testing.T, root, role string) (*atomic.Int32, func()) {
	t.Helper()
	dir := filepath.Join(root, role)
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("unix", filepath.Join(dir, "sandbox.sock"))
	if err != nil {
		t.Fatal(err)
	}
	state := &atomic.Int32{}
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/health" {
			t.Errorf("unexpected route %s", r.URL.Path)
		}
		switch state.Load() {
		case 1:
			_, _ = fmt.Fprintf(w, `{"ready":true,"instance_id":%q}`, strings.Repeat("b", 32))
		case 2:
			http.Error(w, "private transport body should not escape", 503)
		case 3:
			<-r.Context().Done()
		case 4:
			state.CompareAndSwap(4, 0)
			<-r.Context().Done()
		default:
			_, _ = fmt.Fprintf(w, `{"ready":true,"instance_id":%q}`, strings.Repeat("a", 32))
		}
	})}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { _ = server.Close() })
	return state, func() { _ = server.Close() }
}

func sandboxTestRoot(t *testing.T) string {
	t.Helper()
	root, err := os.MkdirTemp("/tmp", "loom-health-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	return root
}

func TestSandboxMonitorPinsBothPrivateRolesAndBoundsLoss(t *testing.T) {
	for _, role := range []string{"task-sandbox", "verifier-sandbox"} {
		for _, kind := range []string{"restart", "unready", "stalled", "dead"} {
			t.Run(role+"/"+kind, func(t *testing.T) {
				root := sandboxTestRoot(t)
				task, closeTask := testSandbox(t, root, "task-sandbox")
				verifier, closeVerifier := testSandbox(t, root, "verifier-sandbox")
				p := plan{Sidecars: []sidecar{{RoleName: "task-sandbox", PrivateSandbox: true}, {RoleName: "verifier-sandbox", PrivateSandbox: true}, {RoleName: "unmonitored"}}}
				ctx, stop := monitorSandboxes(context.Background(), p, root, 10*time.Millisecond, 50*time.Millisecond)
				defer stop()
				state, closeServer := task, closeTask
				if role == "verifier-sandbox" {
					state, closeServer = verifier, closeVerifier
				}
				started := time.Now()
				switch kind {
				case "restart":
					state.Store(1)
				case "unready":
					state.Store(2)
				case "stalled":
					state.Store(3)
				case "dead":
					closeServer()
				}
				select {
				case <-ctx.Done():
				case <-time.After(500 * time.Millisecond):
					t.Fatal("private sandbox loss did not abort execution")
				}
				if kind == "stalled" && time.Since(started) < 3*50*time.Millisecond {
					t.Fatal("one slow probe was treated as proof of loss")
				}
				if !errors.Is(context.Cause(ctx), errSandboxLost) || !strings.Contains(context.Cause(ctx).Error(), role) || strings.Contains(context.Cause(ctx).Error(), "private transport") {
					t.Fatalf("incorrect or unsafe cause: %v", context.Cause(ctx))
				}
				// Recovery never revives this Trial or permits reconnecting to a new process.
				state.Store(0)
				if ctx.Err() == nil {
					t.Fatal("lost execution was revived")
				}
			})
		}
	}
}

func TestSandboxLossStopsAgentAndNeverRunsVerifier(t *testing.T) {
	workspace, output := t.TempDir(), t.TempDir()
	p := timeoutHandoffPlan(workspace, "124")
	p.Main.TimeoutSeconds = 10
	p.Sidecars = []sidecar{{RoleName: "task-sandbox", PrivateSandbox: true}}
	root := sandboxTestRoot(t)
	state, _ := testSandbox(t, root, "task-sandbox")
	ctx, stop := monitorSandboxes(context.Background(), p, root, 10*time.Millisecond, 50*time.Millisecond)
	defer stop()
	time.AfterFunc(100*time.Millisecond, func() { state.Store(1) })
	start := time.Now()
	result, err := runPlan(ctx, p, workspace, output, nil)
	if time.Since(start) > 2*time.Second {
		t.Fatal("agent termination exceeded bounded grace")
	}
	if !errors.Is(err, errSandboxLost) || result.Status != "runtime_error" || result.FailureReason != "sandbox_lost" || !result.PartialEvidence {
		t.Fatalf("loss misclassified: %#v %v", result, err)
	}
	if len(result.Phases) != 1 || result.Phases[0].TimedOut {
		t.Fatalf("sandbox loss became timeout/handoff: %#v", result.Phases)
	}
	if _, err := os.Stat(filepath.Join(workspace, "verified")); !os.IsNotExist(err) {
		t.Fatalf("verifier ran: %v", err)
	}
	if result.Phases[0].Stdout.Path == "" || result.Phases[0].Stderr.Path == "" {
		t.Fatal("partial stream evidence lost")
	}
}

func TestMissingSandboxFailsBeforeAgentStarts(t *testing.T) {
	workspace, output := t.TempDir(), t.TempDir()
	p := testPlan(workspace, phase{Role: "agent", Argv: []string{"/bin/sh", "-c", "touch started"}, WorkingDirectory: workspace, TimeoutSeconds: 10})
	p.Sidecars = []sidecar{{RoleName: "task-sandbox", PrivateSandbox: true}}
	ctx, stop := monitorSandboxes(context.Background(), p, sandboxTestRoot(t), 10*time.Millisecond, 50*time.Millisecond)
	defer stop()
	result, err := runPlan(ctx, p, workspace, output, nil)
	if !errors.Is(err, errSandboxLost) || result.FailureReason != "sandbox_lost" || len(result.Phases) != 0 {
		t.Fatalf("missing sandbox did not fail closed: %#v %v", result, err)
	}
	if _, err := os.Stat(filepath.Join(workspace, "started")); !os.IsNotExist(err) {
		t.Fatalf("agent ran: %v", err)
	}
}

func TestSandboxMonitorAllowsOneDelayedProbeToRecover(t *testing.T) {
	root := sandboxTestRoot(t)
	state, _ := testSandbox(t, root, "task-sandbox")
	p := plan{Sidecars: []sidecar{{RoleName: "task-sandbox", PrivateSandbox: true}}}
	ctx, stop := monitorSandboxes(context.Background(), p, root, 10*time.Millisecond, 50*time.Millisecond)
	defer stop()
	// Healthy responses between delays must reset the failure counter.
	for i := 0; i < sandboxHealthFailureThreshold; i++ {
		state.Store(4)
		select {
		case <-ctx.Done():
			t.Fatalf("single slow health check killed healthy sandbox: %v", context.Cause(ctx))
		case <-time.After(150 * time.Millisecond):
		}
		if state.Load() != 0 {
			t.Fatal("transient probe was not exercised")
		}
	}
}

func TestStoppingSandboxMonitorDoesNotTurnSuccessIntoLoss(t *testing.T) {
	root := sandboxTestRoot(t)
	state, _ := testSandbox(t, root, "task-sandbox")
	p := plan{Sidecars: []sidecar{{RoleName: "task-sandbox", PrivateSandbox: true}}}
	ctx, stop := monitorSandboxes(context.Background(), p, root, 10*time.Millisecond, 50*time.Millisecond)
	state.Store(3)
	time.Sleep(20 * time.Millisecond)
	stop()
	if ctx.Err() != nil {
		t.Fatalf("monitor shutdown changed successful execution: %v", context.Cause(ctx))
	}
}

func TestSandboxMonitorAllowsInitialDelayedProbe(t *testing.T) {
	root := sandboxTestRoot(t)
	state, _ := testSandbox(t, root, "task-sandbox")
	state.Store(4)
	p := plan{Sidecars: []sidecar{{RoleName: "task-sandbox", PrivateSandbox: true}}}
	ctx, stop := monitorSandboxes(context.Background(), p, root, 10*time.Millisecond, 50*time.Millisecond)
	defer stop()
	if ctx.Err() != nil {
		t.Fatalf("initial transient health failure killed sandbox: %v", context.Cause(ctx))
	}
	if state.Load() != 0 {
		t.Fatal("initial delayed probe not exercised")
	}
}
