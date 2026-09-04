package main

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
)

const (
	testGrantID           = "11111111-1111-4111-8111-111111111111"
	testSessionID         = "22222222-2222-4222-8222-222222222222"
	testMaterializationID = "33333333-3333-4333-8333-333333333333"
	testAttemptID         = "44444444-4444-4444-8444-444444444444"
)

var testNow = time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)

// Break caught: a happy run that skips or reorders any authority/executor phase.
func TestOrchestratorBuiltHandoffReleasesAndFinishesInOrder(t *testing.T) {
	h := newOrchestratorHarness(t)

	err := h.orchestrator().Run(context.Background())
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	h.wantEvents([]string{
		"project",
		"exchange",
		"renew",
		"claim",
		"bundle",
		"download",
		"start",
		"executor_start",
		"build:task",
		"build:sidecar:cache",
		"handoff",
		"release",
		"executor_close",
		"caps_close",
		"finish",
	})
	if len(h.handoff.accepted) != 1 {
		t.Fatalf("handoff accepted %d sets, want 1", len(h.handoff.accepted))
	}
	gotComponents := h.handoff.accepted[0].Components
	if len(gotComponents) != 2 || gotComponents[0].Name != "task" || gotComponents[1].Name != "sidecar:cache" {
		t.Fatalf("handoff components = %#v", gotComponents)
	}
	if h.guard.failureKinds != nil {
		t.Fatalf("Fail called on happy path: %#v", h.guard.failureKinds)
	}
	h.wantOutcome(BuildOutcomeBuilt, "built")
}

// Break caught: idle claim exits without acquiring bundle/start/build authority.
func TestOrchestratorIdleClaimExitsAfterOneBoundedIdleGrace(t *testing.T) {
	h := newOrchestratorHarness(t)
	h.guard.claimAvailable = false

	err := h.orchestrator().Run(context.Background())
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	h.wantEvents([]string{"project", "exchange", "renew", "claim", "caps_close", "finish"})
	h.wantOutcome(BuildOutcomeCancelled, "idle")
}

// Break caught: failed renewal fences all later output and performs only bounded cleanup.
func TestOrchestratorRenewalFailureFencesLaterOutput(t *testing.T) {
	h := newOrchestratorHarness(t)
	h.guard.renewErr = errors.New("renew transport down")

	err := h.orchestrator().Run(context.Background())
	if err == nil {
		t.Fatal("Run() succeeded, want renewal failure")
	}

	h.wantEvents([]string{"project", "exchange", "renew", "caps_close", "finish"})
	if h.handoff.accepted != nil {
		t.Fatalf("handoff ran after failed renewal: %#v", h.handoff.accepted)
	}
	h.wantOutcome(BuildOutcomeLeaseLost, "renew_failed")
}

// Break caught: a lease lost after local OCI output is never reported as built.
func TestOrchestratorLeaseLossAfterOutputFailsInsteadOfPublishing(t *testing.T) {
	h := newOrchestratorHarness(t)
	h.guard.heartbeatErr = errors.New("lease heartbeat rejected")
	h.executor.afterBuild = func(component string) {
		if component == "task" {
			h.clock.now = h.clock.now.Add(31 * time.Second)
		}
	}

	err := h.orchestrator().Run(context.Background())
	if err == nil {
		t.Fatal("Run() succeeded, want lease lost")
	}

	h.wantEvents([]string{
		"project",
		"exchange",
		"renew",
		"claim",
		"bundle",
		"download",
		"start",
		"executor_start",
		"build:task",
		"heartbeat",
		"fail:containment",
		"executor_close",
		"caps_close",
		"finish",
	})
	if h.handoff.accepted != nil {
		t.Fatalf("handoff ran after lease loss: %#v", h.handoff.accepted)
	}
	h.wantOutcome(BuildOutcomeLeaseLost, "heartbeat_failed")
}

// Break caught: deterministic build failures call guard fail, clean up once, and skip handoff/release.
func TestOrchestratorDeterministicBuildFailureFailsLeaseAndCleansOnce(t *testing.T) {
	h := newOrchestratorHarness(t)
	h.executor.buildErr["task"] = DeterministicBuildError{Reason: "dockerfile_rejected"}

	err := h.orchestrator().Run(context.Background())
	if err == nil {
		t.Fatal("Run() succeeded, want deterministic build failure")
	}

	h.wantEvents([]string{
		"project",
		"exchange",
		"renew",
		"claim",
		"bundle",
		"download",
		"start",
		"executor_start",
		"build:task",
		"fail:deterministic",
		"executor_close",
		"caps_close",
		"finish",
	})
	if got := h.executor.closeCalls; got != 1 {
		t.Fatalf("executor Close calls = %d, want 1", got)
	}
	h.wantOutcome(BuildOutcomeDeterministicFailure, "dockerfile_rejected")
}

// Break caught: SIGINT/SIGTERM cancellation does not publish partial output and does finish.
func TestOrchestratorSignalCancellationTerminatesActiveExecutorAndFinishes(t *testing.T) {
	h := newOrchestratorHarness(t)
	ctx, cancel := context.WithCancel(context.Background())
	h.executor.afterBuild = func(component string) {
		if component == "task" {
			cancel()
		}
	}

	err := h.orchestrator().Run(ctx)
	if err == nil {
		t.Fatal("Run() succeeded, want cancellation")
	}

	h.wantEvents([]string{
		"project",
		"exchange",
		"renew",
		"claim",
		"bundle",
		"download",
		"start",
		"executor_start",
		"build:task",
		"fail:containment",
		"executor_close",
		"caps_close",
		"finish",
	})
	if h.handoff.accepted != nil {
		t.Fatalf("handoff accepted partial output: %#v", h.handoff.accepted)
	}
	h.wantOutcome(BuildOutcomeCancelled, "cancelled")
}

// Break caught: BuildOutcome accidentally carries raw logs, URLs, tokens, Dockerfile text, or environment.
func TestBuildOutcomeWireRedactsForbiddenMaterialAndBoundsStatus(t *testing.T) {
	outcome := BuildOutcome{
		Status:    BuildOutcomeBuilt,
		Reason:    "built",
		Component: "task",
		Components: []BuiltComponent{
			{
				Name: "task",
				Output: OCIOutput{
					Path:           "/tmp/work/oci/0000.tar",
					TopLevelDigest: "sha256:" + strings.Repeat("a", 64),
					FileSHA256:     strings.Repeat("b", 64),
					SizeBytes:      123,
					OS:             "linux",
					Architecture:   "arm64",
				},
			},
		},
		Cleanup: map[string]int{"descendant_processes": 0, "open_files": 0},
	}

	wire, err := outcome.MarshalJSON()
	if err != nil {
		t.Fatalf("MarshalJSON() error = %v", err)
	}
	for _, forbidden := range []string{"http://", "https://", "loom_tib", "Dockerfile", "ENV=", "/tmp/work"} {
		if strings.Contains(string(wire), forbidden) {
			t.Fatalf("outcome leaked %q in %s", forbidden, wire)
		}
	}

	outcome.Status = "published"
	if _, err := outcome.MarshalJSON(); err == nil {
		t.Fatal("MarshalJSON() accepted unbounded status")
	}
	outcome.Status = BuildOutcomeBuilt
	outcome.Reason = strings.Repeat("x", 65)
	if _, err := outcome.MarshalJSON(); err == nil {
		t.Fatal("MarshalJSON() accepted unbounded reason")
	}
}

func newOrchestratorHarness(t *testing.T) *orchestratorHarness {
	t.Helper()
	h := &orchestratorHarness{}
	h.clock = &fakeClock{now: testNow}
	h.guard = &fakeOrchestratorGuard{
		h:              h,
		claimAvailable: true,
		sessionExpires: testNow.Add(10 * time.Minute),
		leaseExpires:   testNow.Add(90 * time.Second),
	}
	h.executor = &fakeOrchestratorExecutor{
		h:        h,
		buildErr: map[string]error{},
		outputs: map[string]OCIOutput{
			"task": {
				Path:           "/tmp/secret-path/task.tar",
				TopLevelDigest: "sha256:" + strings.Repeat("1", 64),
				FileSHA256:     strings.Repeat("2", 64),
				SizeBytes:      101,
				OS:             "linux",
				Architecture:   "arm64",
			},
			"sidecar:cache": {
				Path:           "/tmp/secret-path/cache.tar",
				TopLevelDigest: "sha256:" + strings.Repeat("3", 64),
				FileSHA256:     strings.Repeat("4", 64),
				SizeBytes:      202,
				OS:             "linux",
				Architecture:   "arm64",
			},
		},
	}
	h.handoff = &fakePublicationHandoff{h: h}
	h.downloader = fakeBundleDownloader(func(ctx context.Context, secret *SecretBuffer, fd int) (*DownloadedBundle, error) {
		h.events = append(h.events, "download")
		return &DownloadedBundle{}, nil
	})
	h.outcomes = nil
	return h
}

type orchestratorHarness struct {
	events     []string
	clock      *fakeClock
	guard      *fakeOrchestratorGuard
	executor   *fakeOrchestratorExecutor
	handoff    *fakePublicationHandoff
	downloader BundleDownloader
	outcomes   []BuildOutcome
}

func (h *orchestratorHarness) orchestrator() *Orchestrator {
	return &Orchestrator{
		GrantID:      testGrantID,
		Config:       Config{CPUArch: "arm64"},
		Guard:        h.guard,
		Clock:        h.clock,
		NewExecutor:  func(Config, *AllocationCapabilities, BuildPlan) (BuildExecutor, error) { return h.executor, nil },
		Download:     h.downloader,
		Handoff:      h.handoff,
		IdleGrace:    time.Millisecond,
		CleanupGrace: time.Second,
		RecordOutcome: func(outcome BuildOutcome) {
			h.outcomes = append(h.outcomes, outcome)
		},
	}
}

func (h *orchestratorHarness) wantEvents(want []string) {
	if !reflect.DeepEqual(h.events, want) {
		panic(fmt.Sprintf("events = %#v, want %#v", h.events, want))
	}
}

func (h *orchestratorHarness) wantOutcome(status BuildOutcomeStatus, reason string) {
	if len(h.outcomes) != 1 {
		panic(fmt.Sprintf("recorded %d outcomes, want 1: %#v", len(h.outcomes), h.outcomes))
	}
	if h.outcomes[0].Status != status || h.outcomes[0].Reason != reason {
		panic(fmt.Sprintf("outcome = %#v, want status=%q reason=%q", h.outcomes[0], status, reason))
	}
}

type fakeClock struct {
	now time.Time
}

func (c *fakeClock) Now() time.Time { return c.now }

type fakeOrchestratorGuard struct {
	h              *orchestratorHarness
	claimAvailable bool
	renewErr       error
	heartbeatErr   error
	sessionExpires time.Time
	leaseExpires   time.Time
	failureKinds   []string
}

func (g *fakeOrchestratorGuard) Project(ctx context.Context, grantID string) (*AllocationCapabilities, error) {
	g.h.events = append(g.h.events, "project")
	return &AllocationCapabilities{
		Bootstrap:      &SecretBuffer{data: []byte(`{"bootstrap":"redacted"}`)},
		ProofSHA256:    strings.Repeat("7", 64),
		JobDirectoryFD: -1,
		BuildEgressFD:  -1,
		closeHook: func() {
			g.h.events = append(g.h.events, "caps_close")
		},
	}, nil
}

func (g *fakeOrchestratorGuard) Exchange(ctx context.Context, grantID string, exchangeID string, proofSHA256 string, bootstrap *SecretBuffer) (*SessionEnvelope, error) {
	g.h.events = append(g.h.events, "exchange")
	return testSession(1, g.sessionExpires), nil
}

func (g *fakeOrchestratorGuard) Renew(ctx context.Context, grantID string, operationID string, current *SecretBuffer) (*SessionEnvelope, error) {
	g.h.events = append(g.h.events, "renew")
	if g.renewErr != nil {
		return nil, g.renewErr
	}
	return testSession(2, g.sessionExpires.Add(10*time.Minute)), nil
}

func (g *fakeOrchestratorGuard) Claim(ctx context.Context, grantID string, operationID string, current *SecretBuffer) (*SecretBuffer, bool, error) {
	g.h.events = append(g.h.events, "claim")
	if !g.claimAvailable {
		return nil, false, nil
	}
	return &SecretBuffer{data: []byte(testClaimJSON())}, true, nil
}

func (g *fakeOrchestratorGuard) Bundle(ctx context.Context, grantID string, operationID string, materializationID string, attemptID string, leaseEpoch int, current *SecretBuffer) (*SecretBuffer, error) {
	g.h.events = append(g.h.events, "bundle")
	return &SecretBuffer{data: []byte(`{"schema":"loom.task-image-bundle-capability/v1","redacted":true}`)}, nil
}

func (g *fakeOrchestratorGuard) Start(ctx context.Context, grantID string, operationID string, materializationID string, attemptID string, leaseEpoch int, current *SecretBuffer) (*LeaseResponse, error) {
	g.h.events = append(g.h.events, "start")
	return testLease("start", g.leaseExpires), nil
}

func (g *fakeOrchestratorGuard) Heartbeat(ctx context.Context, grantID string, operationID string, materializationID string, attemptID string, leaseEpoch int, current *SecretBuffer) (*LeaseResponse, error) {
	g.h.events = append(g.h.events, "heartbeat")
	if g.heartbeatErr != nil {
		return nil, g.heartbeatErr
	}
	return testLease("heartbeat", g.leaseExpires.Add(90*time.Second)), nil
}

func (g *fakeOrchestratorGuard) Release(ctx context.Context, grantID string, operationID string, materializationID string, attemptID string, leaseEpoch int, current *SecretBuffer) (*LeaseResponse, error) {
	g.h.events = append(g.h.events, "release")
	return testLease("release", time.Time{}), nil
}

func (g *fakeOrchestratorGuard) Fail(ctx context.Context, grantID string, operationID string, materializationID string, attemptID string, leaseEpoch int, failureKind string, current *SecretBuffer) (*LeaseResponse, error) {
	g.h.events = append(g.h.events, "fail:"+failureKind)
	g.failureKinds = append(g.failureKinds, failureKind)
	return testLease("fail", time.Time{}), nil
}

func (g *fakeOrchestratorGuard) Finish(ctx context.Context, grantID string, operationID string, cleanup map[string]int) error {
	g.h.events = append(g.h.events, "finish")
	return nil
}

func testSession(generation int, expires time.Time) *SessionEnvelope {
	return &SessionEnvelope{
		Secret:                &SecretBuffer{data: []byte(`{"session":"redacted"}`)},
		GrantID:               testGrantID,
		SessionID:             testSessionID,
		Generation:            generation,
		AttestationGeneration: generation,
		AttestationSHA256:     strings.Repeat("a", 64),
		IssuedAt:              testNow,
		ExpiresAt:             expires,
	}
}

func testLease(operation string, expires time.Time) *LeaseResponse {
	var leaseExpiresAt *time.Time
	if !expires.IsZero() {
		leaseExpiresAt = &expires
	}
	return &LeaseResponse{
		Operation:                 operation,
		GrantID:                   testGrantID,
		OperationID:               "55555555-5555-4555-8555-555555555555",
		MaterializationID:         testMaterializationID,
		AttemptID:                 testAttemptID,
		LeaseEpoch:                1,
		State:                     "running",
		DeterministicFailureCount: 0,
		LeaseExpiresAt:            leaseExpiresAt,
	}
}

func testClaimJSON() string {
	return fmt.Sprintf(`{
		"schema_version":"loom.task-image-materialization-claim.v1",
		"claim_id":"66666666-6666-4666-8666-666666666666",
		"materialization_id":%q,
		"attempt_id":%q,
		"lease_epoch":1,
		"state":"claimed",
		"deterministic_failure_count":0,
		"lease_expires_at":"2026-09-03T12:01:30Z",
		"plan":{
			"schema_version":"loom.task-image-build-plan.v1",
			"grant_id":%q,
			"session_id":%q,
			"session_generation":2,
			"materialization_id":%q,
			"builder_id":"rootless:22222222222242228222222222222222",
			"task_id":"phase2c/session-bound",
			"task_checksum":"%s",
			"cpu_arch":"arm64",
			"platform":"linux/arm64",
			"bundle_bucket":"loom-bundles",
			"bundle_prefix":"phase2c/session-bound/",
			"bundle_file_metadata_sha256":"%s",
			"bundle_file_limit":2000,
			"bundle_byte_limit":536870912,
			"build_timeout_seconds":600,
			"authorization_expires_at":"2026-09-03T12:10:00Z",
			"components":[
				{"name":"task","dockerfile_path":"environment/Dockerfile","context_path":".","oci_output_path":"oci/0000.tar"},
				{"name":"sidecar:cache","dockerfile_path":"sidecars/cache/Dockerfile","context_path":"sidecars/cache","oci_output_path":"oci/0001.tar"}
			]
		}
	}`, testMaterializationID, testAttemptID, testGrantID, testSessionID, testMaterializationID, strings.Repeat("5", 64), strings.Repeat("6", 64))
}

type fakeOrchestratorExecutor struct {
	h          *orchestratorHarness
	outputs    map[string]OCIOutput
	buildErr   map[string]error
	closeCalls int
	afterBuild func(string)
}

func (e *fakeOrchestratorExecutor) Start(ctx context.Context) error {
	e.h.events = append(e.h.events, "executor_start")
	return nil
}

func (e *fakeOrchestratorExecutor) Build(ctx context.Context, component BuildComponent) (OCIOutput, error) {
	e.h.events = append(e.h.events, "build:"+component.Name)
	if err := e.buildErr[component.Name]; err != nil {
		return OCIOutput{}, err
	}
	if e.afterBuild != nil {
		e.afterBuild(component.Name)
	}
	return e.outputs[component.Name], nil
}

func (e *fakeOrchestratorExecutor) Close(ctx context.Context) error {
	e.h.events = append(e.h.events, "executor_close")
	e.closeCalls++
	return nil
}

type fakePublicationHandoff struct {
	h        *orchestratorHarness
	accepted []BuiltComponentSet
	err      error
}

func (h *fakePublicationHandoff) Accept(ctx context.Context, set BuiltComponentSet) error {
	h.h.events = append(h.h.events, "handoff")
	if h.err != nil {
		return h.err
	}
	h.accepted = append(h.accepted, set)
	return nil
}

type fakeBundleDownloader func(context.Context, *SecretBuffer, int) (*DownloadedBundle, error)

func (fn fakeBundleDownloader) DownloadBundle(ctx context.Context, secret *SecretBuffer, fd int) (*DownloadedBundle, error) {
	return fn(ctx, secret, fd)
}
