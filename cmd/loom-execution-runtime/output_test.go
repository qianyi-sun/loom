package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func completeOutputPlan(workspace string) plan {
	p := testPlan(workspace, phase{
		Role: "agent", Argv: []string{"/usr/bin/true"},
		WorkingDirectory: workspace, TimeoutSeconds: 5,
	})
	p.OutputDeclarations = []outputDeclaration{
		{SourcePath: "answer.txt", RelativePath: "artifacts/answer.txt", Kind: "task_artifact", Required: true},
		{SourcePath: ".loom/agent/trajectory.jsonl", RelativePath: "trajectory/events.jsonl", Kind: "trajectory", Required: true},
		{SourcePath: ".loom/agent/usage.json", RelativePath: "accounting/usage.json", Kind: "usage", Required: true},
		{SourcePath: ".loom/verifier/output.json", RelativePath: "verifier/output.json", Kind: "verifier", Required: true},
	}
	p.VerifierExecution = "in_attempt"
	p.Verifier = &phase{
		Role: "verifier", Argv: []string{"/usr/bin/true"},
		WorkingDirectory: workspace, TimeoutSeconds: 5,
	}
	return p
}

func writeWorkspaceOutput(t *testing.T, workspace, relative, body string) {
	t.Helper()
	path := filepath.Join(workspace, filepath.FromSlash(relative))
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestCaptureDeclaredOutputsBuildsCompleteTrialBundle(t *testing.T) {
	workspace, output := t.TempDir(), t.TempDir()
	p := completeOutputPlan(workspace)
	writeWorkspaceOutput(t, workspace, "answer.txt", "42\n")
	writeWorkspaceOutput(t, workspace, ".loom/agent/trajectory.jsonl", "{\"turn\":0}\n")
	writeWorkspaceOutput(t, workspace, ".loom/agent/usage.json", "{\"call_count\":1}\n")
	writeWorkspaceOutput(t, workspace, ".loom/verifier/output.json", "{\"rewards\":{\"passed\":1}}\n")

	result, err := runPlan(context.Background(), p, workspace, output, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := captureDeclaredOutputs(p, workspace, output, &result); err != nil {
		t.Fatal(err)
	}
	if result.Status != "succeeded" || result.PartialEvidence || len(result.Outputs) != 4 {
		t.Fatalf("complete bundle did not retain success: %#v", result)
	}
	if result.VerifierRewards["passed"] != 1 {
		t.Fatalf("verifier rewards were not projected: %#v", result.VerifierRewards)
	}
	for _, path := range []string{
		"artifacts/answer.txt", "trajectory/events.jsonl", "accounting/usage.json", "verifier/output.json",
	} {
		if _, err := os.Stat(filepath.Join(output, filepath.FromSlash(path))); err != nil {
			t.Fatalf("bundle output %s is absent: %v", path, err)
		}
	}
}

func TestCaptureDeclaredOutputsFailsClosedOnMissingTrajectory(t *testing.T) {
	workspace, output := t.TempDir(), t.TempDir()
	p := completeOutputPlan(workspace)
	writeWorkspaceOutput(t, workspace, "answer.txt", "42\n")
	writeWorkspaceOutput(t, workspace, ".loom/agent/usage.json", "{\"call_count\":1}\n")
	writeWorkspaceOutput(t, workspace, ".loom/verifier/output.json", "{\"rewards\":{\"passed\":1}}\n")

	result, err := runPlan(context.Background(), p, workspace, output, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := captureDeclaredOutputs(p, workspace, output, &result); err == nil {
		t.Fatal("missing trajectory was accepted")
	}
	if result.Status != "trajectory_flush_failed" || !result.PartialEvidence {
		t.Fatalf("missing trajectory did not fail closed: %#v", result)
	}
}

func TestCaptureDeclaredOutputsRejectsWorkspaceSymlink(t *testing.T) {
	workspace, output := t.TempDir(), t.TempDir()
	p := completeOutputPlan(workspace)
	target := filepath.Join(t.TempDir(), "outside.txt")
	if err := os.WriteFile(target, []byte("outside"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(workspace, "answer.txt")); err != nil {
		t.Fatal(err)
	}

	result := resultManifest{Status: "succeeded"}
	if err := captureDeclaredOutputs(p, workspace, output, &result); err == nil {
		t.Fatal("workspace symlink was accepted")
	}
	if result.Status != "artifact_upload_failed" {
		t.Fatalf("symlink failure was misclassified: %#v", result)
	}
}
