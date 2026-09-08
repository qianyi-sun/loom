package main

import (
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

// The Python fixture supplies real authority/DB/worker/registry-read boundaries.
// Node containment, bundle download, build and registry upload are explicit
// doubles. No session, candidate, status or receipt authority is fabricated here.
func TestGoPublicationHTTPOrchestratorHelper(t *testing.T) {
	if os.Getenv("LOOM_GO_HTTP_HELPER") != "1" {
		t.Skip("driven by Python HTTP composition fixture")
	}
	useTestProtocolPolicy(t)
	previousArch := runtimeGOARCH
	runtimeGOARCH = func() string { return "arm64" }
	t.Cleanup(func() { runtimeGOARCH = previousArch })
	size, err := strconv.ParseInt(os.Getenv("LOOM_GO_HTTP_ROOT_SIZE"), 10, 64)
	if err != nil {
		t.Fatal("fixture size missing")
	}
	output := OCIOutput{Path: "/fixture/not-read.tar", TopLevelDigest: os.Getenv("LOOM_GO_HTTP_ROOT"), ManifestSize: size,
		ManifestMediaType: ociManifestMediaType, FileSHA256: strings.Repeat("b", 64), SizeBytes: 4096, OS: "linux", Architecture: "arm64"}
	uploader := publicationUploadFunc(func(ctx context.Context, output OCIOutput, source RegistryUploadCredentialSource) (UploadedManifest, error) {
		credential, err := source.Next(ctx, nil)
		if err != nil {
			return UploadedManifest{}, err
		}
		defer func() { source.Close(credential) }()
		// Guard attestations use whole-second observations. Wait for a strictly
		// later observation before exercising real source-driven renewal.
		timer := time.NewTimer(time.Until(time.Now().UTC().Truncate(time.Second).Add(2 * time.Second)))
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return UploadedManifest{}, ctx.Err()
		case <-timer.C:
		}
		next, err := source.Next(ctx, credential)
		if err != nil {
			return UploadedManifest{}, err
		}
		credential = next
		manifest := UploadedManifest{Repository: credential.Repository, Digest: output.TopLevelDigest, MediaType: output.ManifestMediaType, Size: output.ManifestSize}
		if err := source.UploadSucceeded(ctx, manifest, credential); err != nil {
			return UploadedManifest{}, err
		}
		return manifest, nil
	})
	handoff := &RegistryPublicationHandoff{uploader: uploader, expectation: PublicationRegistryExpectation{
		RegistryOrigin: os.Getenv("LOOM_GO_HTTP_REGISTRY"), RegistryService: "registry.test", RegistryIssuer: "loom-task-image-authority", RegistryKeyID: os.Getenv("LOOM_GO_HTTP_REGISTRY_KEY")}}
	executor := &httpFixtureExecutor{output: output}
	var outcomes []BuildOutcome
	o := Orchestrator{GrantID: "11111111-1111-1111-1111-111111111111", Config: Config{CPUArch: "arm64"},
		Guard: NewGuardClient(os.Getenv("LOOM_GO_HTTP_SOCKET"), 32768, 5*time.Second), Clock: realClock{}, Handoff: handoff,
		IdleGrace: time.Millisecond, CleanupGrace: 5 * time.Second,
		NewExecutor:   func(Config, *AllocationCapabilities, BuildPlan) (BuildExecutor, error) { return executor, nil },
		Download:      fakeBundleDownloader(func(context.Context, *SecretBuffer, int) (*DownloadedBundle, error) { return &DownloadedBundle{}, nil }),
		RecordOutcome: func(outcome BuildOutcome) { outcomes = append(outcomes, outcome) }}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := o.Run(ctx); err != nil {
		t.Fatalf("HTTP orchestrator failed: %v", err)
	}
	if !executor.closed || len(outcomes) != 1 || outcomes[0].Status != BuildOutcomeBuilt {
		t.Fatal("HTTP orchestrator did not complete and clean up")
	}
}

type httpFixtureExecutor struct {
	output OCIOutput
	closed bool
}

func (e *httpFixtureExecutor) Start(context.Context) error { return nil }
func (e *httpFixtureExecutor) Close(context.Context) error { e.closed = true; return nil }
func (e *httpFixtureExecutor) Build(_ context.Context, component BuildComponent) (BuildResult, error) {
	if component.Name != "task" {
		return BuildResult{}, errors.New("unexpected fixture component")
	}
	return BuildResult{Output: e.output, BaseResolution: testBaseResolutionEvidence("http-solve", "linux/arm64", e.output.TopLevelDigest)}, nil
}
