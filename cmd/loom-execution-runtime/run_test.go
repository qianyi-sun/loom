package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func testPlan(workspace string, agent phase) plan {
	return plan{
		SchemaVersion: "loom.execution-runtime-plan.v1", CandidateSHA: strings.Repeat("1", 40),
		TaskRevisionSHA256:    "sha256:" + strings.Repeat("2", 64),
		CommandIdentitySHA256: "sha256:" + strings.Repeat("6", 64), ExecutionRole: "attempt",
		ExecutionClassID: "linux-amd64-cpu-pod-v1", Composition: "init_payload",
		TaskImageRef:        "registry/task@sha256:" + strings.Repeat("3", 64),
		RuntimeImageRef:     "registry/runtime@sha256:" + strings.Repeat("4", 64),
		RuntimeBinarySHA256: "sha256:" + strings.Repeat("5", 64),
		RunAsUser:           65532, RunAsGroup: 65532, FSGroup: 65532,
		TaskResources: resources{CPUMillis: 100, MemoryMiB: 128, EphemeralStorageMiB: 128},
		WorkspaceMiB:  128, RuntimeVolumeMiB: 32, TerminationGraceSec: 1,
		Main: agent, VerifierExecution: "skipped", Sidecars: []sidecar{},
		MaxLogBytesPerStream: 32, MaxArtifactBytes: 1024,
	}
}

func TestRunPlanCapturesBoundedEvidenceAndAtomicResult(t *testing.T) {
	workspace, output := t.TempDir(), t.TempDir()
	agent := phase{
		Role: "agent", Argv: []string{"/bin/sh", "-c", "printf 'abcdefghijklmnopqrstuvwxyz0123456789'; printf 'error' >&2"},
		WorkingDirectory: workspace, TimeoutSeconds: 5, Environment: map[string]string{"LOOM_PHASE": "agent"},
	}
	result, err := runPlan(context.Background(), testPlan(workspace, agent), workspace, output)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "succeeded" || len(result.Phases) != 1 || !result.Phases[0].Stdout.Truncated {
		t.Fatalf("unexpected result: %#v", result)
	}
	if result.Phases[0].Stdout.BytesSeen != 36 || result.Phases[0].Stdout.BytesSaved != 32 {
		t.Fatalf("unexpected stdout evidence: %#v", result.Phases[0].Stdout)
	}
	path := filepath.Join(output, "result.json")
	if err := writeResult(path, result); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf("temporary result remains: %v", err)
	}
	summaryPath := filepath.Join(output, "termination-message")
	if err := writeTerminationSummary(summaryPath, result); err != nil {
		t.Fatal(err)
	}
	var summary terminationSummary
	payload, err := os.ReadFile(summaryPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(payload, &summary); err != nil {
		t.Fatal(err)
	}
	if summary.Status != "succeeded" || summary.ResultPath != "result.json" {
		t.Fatalf("unexpected termination summary: %#v", summary)
	}
}

func TestRunPlanTimeoutTerminatesProcessGroupAndRetainsPartialEvidence(t *testing.T) {
	workspace, output := t.TempDir(), t.TempDir()
	agent := phase{
		Role: "agent", Argv: []string{"/bin/sh", "-c", "trap 'exit 0' TERM; while :; do sleep 1; done"},
		WorkingDirectory: workspace, TimeoutSeconds: 1,
	}
	started := time.Now()
	result, err := runPlan(context.Background(), testPlan(workspace, agent), workspace, output)
	if err == nil || result.Status != "timed_out" || !result.PartialEvidence {
		t.Fatalf("timeout did not fail closed: result=%#v err=%v", result, err)
	}
	if time.Since(started) > 4*time.Second {
		t.Fatalf("termination exceeded bounded grace: %s", time.Since(started))
	}
}

func TestSeparateVerifierFailureHasDistinctDurableStatusAndRoles(t *testing.T) {
	workspace, output := t.TempDir(), t.TempDir()
	p := testPlan(workspace, phase{
		Role: "verifier", Argv: []string{"/bin/sh", "-c", "printf verifier-failed >&2; exit 7"},
		WorkingDirectory: workspace, TimeoutSeconds: 5,
	})
	p.ExecutionRole = "verifier"
	result, err := runPlan(context.Background(), p, workspace, output)
	if err == nil || result.Status != "verifier_error" || !result.PartialEvidence {
		t.Fatalf("verifier failure was not retained: result=%#v err=%v", result, err)
	}
	if strings.Join(result.ContainerRoles, ",") != "execution,verifier" {
		t.Fatalf("verifier roles are incomplete: %#v", result.ContainerRoles)
	}
	if result.Phases[0].ExitCode != 7 {
		t.Fatalf("verifier exit status was not retained: %#v", result.Phases[0])
	}
}

func TestLoadPlanRejectsUnknownAndMutableFields(t *testing.T) {
	path := filepath.Join(t.TempDir(), "plan.json")
	payload := `{"schema_version":"loom.execution-runtime-plan.v1","unknown":true}`
	if err := os.WriteFile(path, []byte(payload), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadPlan(path); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("unknown field accepted: %v", err)
	}
}

func TestPlanValidationMatchesBoundedWorkspaceAndSidecarContract(t *testing.T) {
	p := testPlan("/workspaceevil", phase{
		Role: "agent", Argv: []string{"/bin/true"}, WorkingDirectory: "/workspaceevil", TimeoutSeconds: 1,
	})
	if err := p.validate(); err == nil {
		t.Fatal("workspace prefix escape was accepted")
	}
	p = testPlan("/workspace", phase{
		Role: "agent", Argv: []string{"/bin/true"}, WorkingDirectory: "/workspace", TimeoutSeconds: 1,
	})
	p.Sidecars = []sidecar{{
		RoleName: "verifier", ImageRef: "registry/sidecar@sha256:" + strings.Repeat("7", 64),
		Argv: []string{"/bin/true"}, Resources: resources{CPUMillis: 1, MemoryMiB: 1, EphemeralStorageMiB: 1},
		StartupProbe:   probe{Kind: "exec", TimeoutSeconds: 1, PeriodSeconds: 1, FailureThreshold: 1, Argv: []string{"/bin/true"}},
		ReadinessProbe: probe{Kind: "exec", TimeoutSeconds: 1, PeriodSeconds: 1, FailureThreshold: 1, Argv: []string{"/bin/true"}},
	}}
	if err := p.validate(); err == nil {
		t.Fatal("reserved sidecar role was accepted")
	}
}

func TestMaterializeCopiesOnlyDigestVerifiedRuntimeAndPlan(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	binary, err := os.ReadFile(executable)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(binary)
	workspace := "/workspace"
	p := testPlan(workspace, phase{
		Role: "agent", Argv: []string{"/bin/true"}, WorkingDirectory: workspace, TimeoutSeconds: 1,
	})
	p.RuntimeBinarySHA256 = "sha256:" + hex.EncodeToString(digest[:])
	payload, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	destination := t.TempDir()
	runtimePath := filepath.Join(destination, "runtime")
	planPath := filepath.Join(destination, "plan.json")
	err = materialize([]string{
		"--encoded-plan", base64.RawURLEncoding.EncodeToString(payload),
		"--runtime-dest", runtimePath, "--plan-dest", planPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(runtimePath); err != nil || info.Mode().Perm() != 0o555 {
		t.Fatalf("runtime mode mismatch: info=%v err=%v", info, err)
	}
	if err := materialize([]string{
		"--encoded-plan", base64.RawURLEncoding.EncodeToString(payload),
		"--runtime-dest", runtimePath, "--plan-dest", planPath,
	}); err == nil {
		t.Fatal("materialize overwrote an existing runtime")
	}
	materializedPlan, err := os.ReadFile(planPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(materializedPlan) != string(payload) {
		t.Fatal("materialized plan bytes changed")
	}
}
