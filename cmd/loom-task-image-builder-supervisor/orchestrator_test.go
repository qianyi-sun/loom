package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"sync"
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

// Break caught: a happy run skips/reorders a phase, renews before schedule, or overlaps claims.
func TestOrchestratorBuiltHandoffReleasesAndFinishesInOrder(t *testing.T) {
	h := newOrchestratorHarness(t)

	err := h.orchestrator().Run(context.Background())
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	h.wantEvents(t, []string{
		"project", "exchange",
		"claim", "bundle", "download", "start", "executor_start", "build:task", "build:sidecar:cache", "handoff", "release", "executor_close",
		"claim", "idle_wait", "claim", "caps_close", "finish",
	})
	if h.guard.renewCalls != 0 {
		t.Fatalf("renew calls = %d, want 0 before schedule", h.guard.renewCalls)
	}
	if len(h.handoff.accepted) != 1 {
		t.Fatalf("handoff accepted %d sets, want 1", len(h.handoff.accepted))
	}
	h.wantOutcome(t, BuildOutcomeBuilt, "built")
}

// Break caught: unavailable work exits immediately instead of retrying once after bounded IdleGrace.
func TestOrchestratorIdleClaimExitsAfterBoundedIdleGrace(t *testing.T) {
	h := newOrchestratorHarness(t)
	h.guard.claimAvailability = []bool{false, false}

	err := h.orchestrator().Run(context.Background())
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	h.wantEvents(t, []string{"project", "exchange", "claim", "idle_wait", "claim", "caps_close", "finish"})
	h.wantOutcome(t, BuildOutcomeCancelled, "idle")
}

// Break caught: one completed claim ends the allocation instead of processing the next serial claim.
func TestOrchestratorProcessesTwoSequentialClaimsBeforeIdleExit(t *testing.T) {
	h := newOrchestratorHarness(t)
	h.guard.claimAvailability = []bool{true, true, false, false}

	err := h.orchestrator().Run(context.Background())
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	h.wantEvents(t, []string{
		"project", "exchange",
		"claim", "bundle", "download", "start", "executor_start", "build:task", "build:sidecar:cache", "handoff", "release", "executor_close",
		"claim", "bundle", "download", "start", "executor_start", "build:task", "build:sidecar:cache", "handoff", "release", "executor_close",
		"claim", "idle_wait", "claim", "caps_close", "finish",
	})
	if len(h.handoff.accepted) != 2 {
		t.Fatalf("accepted sets = %d, want 2", len(h.handoff.accepted))
	}
}

// Break caught: exchange hashes bootstrap secret bytes when the guard omitted the projected proof digest.
func TestOrchestratorRejectsProjectionWithoutProofDigest(t *testing.T) {
	h := newOrchestratorHarness(t)
	h.guard.proofSHA256 = ""

	err := h.orchestrator().Run(context.Background())
	if err == nil {
		t.Fatal("Run() succeeded, want missing proof rejection")
	}
	h.wantEvents(t, []string{"project", "caps_close", "finish"})
	h.wantOutcome(t, BuildOutcomeContainmentFailure, "project_proof_missing")
}

// Break caught: failed renewal does not cancel the active long-running build and fence later output.
func TestOrchestratorRenewalFailureCancelsActiveBuildAndFencesOutput(t *testing.T) {
	h := newOrchestratorHarness(t)
	h.guard.sessionExpires = testNow.Add(2 * time.Minute)
	h.guard.leaseExpires = testNow.Add(10 * time.Minute)
	h.guard.claimMutation.AuthorizationExpiresAt = "2026-09-03T12:02:00Z"
	h.guard.renewErr = errors.New("transport down with token loom_tibs_SECRET")
	buildStarted := make(chan struct{})
	h.executor.blockBuild = func(ctx context.Context, component string) (OCIOutput, error) {
		close(buildStarted)
		<-ctx.Done()
		return OCIOutput{}, ctx.Err()
	}

	done := make(chan error, 1)
	go func() { done <- h.orchestrator().Run(context.Background()) }()
	select {
	case <-buildStarted:
	case <-time.After(time.Second):
		t.Fatal("build did not start")
	}
	h.clock.advance(80 * time.Second)

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Run() succeeded, want renewal failure")
		}
	case <-time.After(time.Second):
		t.Fatal("Run() did not cancel active build after renewal failure")
	}
	h.wantEvents(t, []string{
		"project", "exchange", "claim", "bundle", "download", "start", "executor_start", "build:task",
		"renew", "fail:containment", "executor_close", "caps_close", "finish",
	})
	if h.handoff.accepted != nil {
		t.Fatalf("handoff ran after renewal failure: %#v", h.handoff.accepted)
	}
	h.wantOutcome(t, BuildOutcomeLeaseLost, "renew_failed")
}

// Break caught: session renewal waits until two-thirds of the remaining lifetime has elapsed.
func TestRenewalAtUsesEarlierOneThirdTargetOrSafetyMargin(t *testing.T) {
	for _, tc := range []struct {
		name    string
		expires time.Time
		want    time.Time
	}{
		{name: "long lifetime uses one third from now", expires: testNow.Add(90 * time.Second), want: testNow.Add(30 * time.Second)},
		{name: "short lifetime still uses one third from now", expires: testNow.Add(30 * time.Second), want: testNow.Add(10 * time.Second)},
		{name: "expired renews immediately", expires: testNow.Add(-time.Second), want: testNow},
		{name: "sub safety margin renews immediately through past target", expires: testNow.Add(9 * time.Second), want: testNow.Add(-6 * time.Second)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := renewalAt(testNow, tc.expires); !got.Equal(tc.want) {
				t.Fatalf("renewalAt() = %v, want %v", got, tc.want)
			}
		})
	}
}

// Break caught: SIGINT/SIGTERM cancellation waits forever for a Build that ignores context cancellation.
func TestOrchestratorSignalCancellationClosesNonCooperativeExecutorAndFailsWithFreshContext(t *testing.T) {
	h := newOrchestratorHarness(t)
	h.guard.rejectCanceledFailContext = true
	ctx, cancel := context.WithCancel(context.Background())
	buildStarted := make(chan struct{})
	h.executor.blockBuild = func(ctx context.Context, component string) (OCIOutput, error) {
		close(buildStarted)
		<-h.executor.closed()
		return h.executor.outputs[component], nil
	}

	done := make(chan error, 1)
	go func() { done <- h.orchestrator().Run(ctx) }()
	select {
	case <-buildStarted:
	case <-time.After(time.Second):
		t.Fatal("build did not start")
	}
	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Run() succeeded, want cancellation failure")
		}
		if strings.Contains(err.Error(), "fail_failed") {
			t.Fatalf("Run() used canceled context for Fail: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run() waited for non-cooperative Build instead of closing executor")
	}
	h.wantEvents(t, []string{
		"project", "exchange", "claim", "bundle", "download", "start", "executor_start", "build:task",
		"fail:containment", "executor_close", "caps_close", "finish",
	})
	if h.handoff.accepted != nil {
		t.Fatalf("handoff accepted output after cancellation: %#v", h.handoff.accepted)
	}
	if got := h.executor.closeCalls; got != 1 {
		t.Fatalf("executor Close calls = %d, want 1", got)
	}
	h.wantOutcome(t, BuildOutcomeCancelled, "cancelled")
}

// Break caught: renewal loss waits forever for a Build that ignores context cancellation.
func TestOrchestratorRenewalFailureClosesNonCooperativeExecutorBeforeFencing(t *testing.T) {
	h := newOrchestratorHarness(t)
	h.guard.sessionExpires = testNow.Add(2 * time.Minute)
	h.guard.leaseExpires = testNow.Add(10 * time.Minute)
	h.guard.claimMutation.AuthorizationExpiresAt = "2026-09-03T12:02:00Z"
	h.guard.renewErr = errors.New("transport down with token loom_tibs_SECRET")
	buildStarted := make(chan struct{})
	h.executor.blockBuild = func(ctx context.Context, component string) (OCIOutput, error) {
		close(buildStarted)
		<-h.executor.closed()
		return h.executor.outputs[component], nil
	}

	done := make(chan error, 1)
	go func() { done <- h.orchestrator().Run(context.Background()) }()
	select {
	case <-buildStarted:
	case <-time.After(time.Second):
		t.Fatal("build did not start")
	}
	h.clock.advance(40 * time.Second)

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Run() succeeded, want renewal failure")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run() waited for non-cooperative Build after renewal loss")
	}
	h.wantEvents(t, []string{
		"project", "exchange", "claim", "bundle", "download", "start", "executor_start", "build:task",
		"renew", "fail:containment", "executor_close", "caps_close", "finish",
	})
	if h.handoff.accepted != nil {
		t.Fatalf("handoff accepted output after renewal loss: %#v", h.handoff.accepted)
	}
	if got := h.executor.closeCalls; got != 1 {
		t.Fatalf("executor Close calls = %d, want 1", got)
	}
	h.wantOutcome(t, BuildOutcomeLeaseLost, "renew_failed")
}

// Break caught: heartbeat loss waits forever for a Build that ignores context cancellation.
func TestOrchestratorHeartbeatFailureClosesNonCooperativeExecutorBeforeFencing(t *testing.T) {
	h := newOrchestratorHarness(t)
	h.guard.heartbeatErr = errors.New("lease rejected")
	h.guard.sessionExpires = testNow.Add(10 * time.Minute)
	buildStarted := make(chan struct{})
	h.executor.blockBuild = func(ctx context.Context, component string) (OCIOutput, error) {
		close(buildStarted)
		<-h.executor.closed()
		return h.executor.outputs[component], nil
	}

	done := make(chan error, 1)
	go func() { done <- h.orchestrator().Run(context.Background()) }()
	select {
	case <-buildStarted:
	case <-time.After(time.Second):
		t.Fatal("build did not start")
	}
	h.clock.advance(31 * time.Second)

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Run() succeeded, want heartbeat failure")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run() waited for non-cooperative Build after heartbeat loss")
	}
	h.wantEvents(t, []string{
		"project", "exchange", "claim", "bundle", "download", "start", "executor_start", "build:task",
		"heartbeat", "fail:containment", "executor_close", "caps_close", "finish",
	})
	if h.handoff.accepted != nil {
		t.Fatalf("handoff accepted output after heartbeat loss: %#v", h.handoff.accepted)
	}
	if got := h.executor.closeCalls; got != 1 {
		t.Fatalf("executor Close calls = %d, want 1", got)
	}
	h.wantOutcome(t, BuildOutcomeLeaseLost, "heartbeat_failed")
}

// Break caught: authority-derived build timeout is parsed but not enforced during execution.
func TestOrchestratorBuildTimeoutClosesExecutorFailsLeaseAndRejectsLateOutput(t *testing.T) {
	h := newOrchestratorHarness(t)
	h.guard.claimMutation.BuildTimeoutSeconds = 2
	h.guard.claimMutation.AuthorizationExpiresAt = "2026-09-03T12:10:00Z"
	buildStarted := make(chan struct{})
	h.executor.blockBuild = func(ctx context.Context, component string) (OCIOutput, error) {
		close(buildStarted)
		<-h.executor.closed()
		return h.executor.outputs[component], nil
	}

	done := make(chan error, 1)
	go func() { done <- h.orchestrator().Run(context.Background()) }()
	select {
	case <-buildStarted:
	case <-time.After(time.Second):
		t.Fatal("build did not start")
	}
	h.clock.advance(3 * time.Second)

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Run() succeeded, want build timeout")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run() did not enforce authority-derived build timeout")
	}
	h.wantEvents(t, []string{
		"project", "exchange", "claim", "bundle", "download", "start", "executor_start", "build:task",
		"fail:containment", "executor_close", "caps_close", "finish",
	})
	if h.handoff.accepted != nil {
		t.Fatalf("handoff accepted late output after build timeout: %#v", h.handoff.accepted)
	}
	h.wantOutcome(t, BuildOutcomeTransientFailure, "build_timeout")
}

// Break caught: lease lost after local output is reported as built.
func TestOrchestratorLeaseLossAfterOutputFailsInsteadOfPublishing(t *testing.T) {
	h := newOrchestratorHarness(t)
	h.guard.heartbeatErr = errors.New("heartbeat rejected")
	h.executor.afterBuild = func(component string) {
		if component == "task" {
			h.clock.advance(31 * time.Second)
		}
	}

	err := h.orchestrator().Run(context.Background())
	if err == nil {
		t.Fatal("Run() succeeded, want lease lost")
	}

	h.wantEvents(t, []string{
		"project", "exchange", "claim", "bundle", "download", "start", "executor_start", "build:task",
		"heartbeat", "fail:containment", "executor_close", "caps_close", "finish",
	})
	if h.handoff.accepted != nil {
		t.Fatalf("handoff ran after lease loss: %#v", h.handoff.accepted)
	}
	h.wantOutcome(t, BuildOutcomeLeaseLost, "heartbeat_failed")
}

// Break caught: deterministic build failures call guard Fail, clean up once, and skip handoff/release.
func TestOrchestratorDeterministicBuildFailureFailsLeaseAndCleansOnce(t *testing.T) {
	h := newOrchestratorHarness(t)
	h.executor.buildErr["task"] = DeterministicBuildError{Reason: "dockerfile_rejected", Err: errors.New("raw Dockerfile secret text")}

	err := h.orchestrator().Run(context.Background())
	if err == nil {
		t.Fatal("Run() succeeded, want deterministic build failure")
	}

	h.wantEvents(t, []string{
		"project", "exchange", "claim", "bundle", "download", "start", "executor_start", "build:task",
		"fail:deterministic", "executor_close", "caps_close", "finish",
	})
	if got := h.executor.closeCalls; got != 1 {
		t.Fatalf("executor Close calls = %d, want 1", got)
	}
	h.wantOutcome(t, BuildOutcomeDeterministicFailure, "dockerfile_rejected")
}

// Break caught: SIGINT/SIGTERM cancellation publishes partial output instead of failing/releasing bounded cleanup.
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

	h.wantEvents(t, []string{
		"project", "exchange", "claim", "bundle", "download", "start", "executor_start", "build:task",
		"fail:containment", "executor_close", "caps_close", "finish",
	})
	if h.handoff.accepted != nil {
		t.Fatalf("handoff accepted partial output: %#v", h.handoff.accepted)
	}
	h.wantOutcome(t, BuildOutcomeCancelled, "cancelled")
}

// Break caught: publication failures revoke containment or leave BuildKit open
// while releasing retryable infrastructure work.
func TestOrchestratorPublicationRetryableFailuresCloseExecutorThenReleaseWithoutFail(t *testing.T) {
	for _, tc := range []struct {
		name   string
		err    error
		reason string
	}{
		{name: "disabled phase", err: ErrPublicationPhaseUnavailable, reason: "publication_phase_unavailable"},
		{name: "verification unavailable", err: ErrPublicationVerificationUnavailable, reason: "publication_verification_unavailable"},
		{name: "registry failure", err: errors.New("registry PRIVATE-TOKEN upload failed"), reason: "publication_failed"},
		{name: "candidate failure", err: errors.New("candidate PRIVATE-TOKEN ambiguous"), reason: "publication_failed"},
		{name: "credential failure", err: errors.New("credential PRIVATE-TOKEN unavailable"), reason: "publication_failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newOrchestratorHarness(t)
			h.handoff.err = tc.err

			err := h.orchestrator().Run(context.Background())
			if err == nil {
				t.Fatal("Run() succeeded, want publication retryable failure")
			}
			if strings.Contains(err.Error(), "PRIVATE-TOKEN") {
				t.Fatalf("Run() leaked raw publication error: %v", err)
			}

			h.wantEvents(t, []string{
				"project", "exchange", "claim", "bundle", "download", "start", "executor_start",
				"build:task", "build:sidecar:cache", "handoff", "executor_close", "release", "caps_close", "finish",
			})
			if h.guard.failureKinds != nil {
				t.Fatalf("Fail called for retryable publication failure: %#v", h.guard.failureKinds)
			}
			if !reflect.DeepEqual(h.guard.releaseDeterministicFailureCounts, []int{0}) {
				t.Fatalf("release deterministic failure counts = %#v, want unchanged zero count", h.guard.releaseDeterministicFailureCounts)
			}
			h.wantOutcome(t, BuildOutcomeTransientFailure, tc.reason)
		})
	}
}

// Break caught: the orchestrator calls a credentialed handoff without binding it
// to the guarded session/claim authority frozen by the successful build.
func TestOrchestratorInjectsGuardedPublicationCredentialSourceIntoCredentialedHandoff(t *testing.T) {
	h := newOrchestratorHarness(t)
	capture := &captureCredentialedPublicationHandoff{
		expectation: PublicationRegistryExpectation{
			RegistryOrigin:  "https://registry.example",
			RegistryService: "registry.example",
			RegistryIssuer:  "loom-task-image",
			RegistryKeyID:   strings.Repeat("A", 43),
		},
		events: &h.events,
		err:    ErrPublicationVerificationUnavailable,
	}
	h.overrideHandoff = capture

	err := h.orchestrator().Run(context.Background())
	if err == nil {
		t.Fatal("Run() succeeded, want verification-unavailable retry")
	}
	if capture.source == nil {
		t.Fatal("credentialed handoff did not receive a PublicationCredentialSource")
	}
	if capture.source.guard != h.guard {
		t.Fatalf("credential source guard = %#v, want orchestrator guard", capture.source.guard)
	}
	wantBinding := PublicationAttemptBinding{
		GrantID:           testGrantID,
		MaterializationID: testMaterializationID,
		AttemptID:         testAttemptID,
		LeaseEpoch:        1,
		BuilderID:         "rootless:22222222222242228222222222222222",
		CPUArch:           "arm64",
		Platform:          "linux/arm64",
		RegistryOrigin:    "https://registry.example",
		RegistryService:   "registry.example",
		RegistryIssuer:    "loom-task-image",
		RegistryKeyID:     strings.Repeat("A", 43),
	}
	if capture.source.binding != wantBinding {
		t.Fatalf("credential source binding = %#v, want %#v", capture.source.binding, wantBinding)
	}
	if len(capture.set.Components) != 2 || capture.set.Components[0].Name != "task" || capture.set.Components[1].Name != "sidecar:cache" {
		t.Fatalf("credentialed handoff received non-canonical built set: %#v", capture.set.Components)
	}
	h.wantEvents(t, []string{
		"project", "exchange", "claim", "bundle", "download", "start", "executor_start",
		"build:task", "build:sidecar:cache", "credentialed_handoff", "executor_close", "release", "caps_close", "finish",
	})
	h.wantOutcome(t, BuildOutcomeTransientFailure, "publication_verification_unavailable")
}

// Break caught: the registry handoff resorts attacker-controlled output,
// records candidates before manifest acknowledgement, returns success in D1, or
// leaks credential buffers after renewal.
func TestRegistryPublicationHandoffRecordsCandidatesInCanonicalOrderAndReturnsVerificationUnavailable(t *testing.T) {
	set := handoffBuiltSet()
	var events []string
	guard := newHandoffCredentialGuard(t, &events)
	source := NewPublicationCredentialSource(NewSessionManager(testGrantID, testSession(1, time.Now().Add(time.Minute)), guard), guard, validPublicationAttemptBinding())
	handoff := newTestRegistryPublicationHandoff(&handoffUploader{
		events:          &events,
		renewComponents: map[string]bool{"task": true},
	})

	err := handoff.AcceptWithCredentials(context.Background(), set, source)
	if !errors.Is(err, ErrPublicationVerificationUnavailable) {
		t.Fatalf("AcceptWithCredentials() error = %v, want ErrPublicationVerificationUnavailable", err)
	}

	wantEvents := []string{
		"credential:task:1",
		"renew",
		"heartbeat",
		"credential:task:2",
		"manifest-ack:task",
		"candidate:task",
		"credential:sidecar:db:1",
		"manifest-ack:sidecar:db",
		"candidate:sidecar:db",
	}
	if !reflect.DeepEqual(events, wantEvents) {
		t.Fatalf("events = %#v, want %#v", events, wantEvents)
	}
	if len(guard.candidateRequests) != 2 {
		t.Fatalf("candidate requests = %d, want 2", len(guard.candidateRequests))
	}
	if got := []string{guard.candidateRequests[0].Component, guard.candidateRequests[1].Component}; !reflect.DeepEqual(got, []string{"task", "sidecar:db"}) {
		t.Fatalf("candidate components = %#v, want canonical task then sidecar", got)
	}
	guard.wantAllSecretsClosed(t)
}

// Break caught: the upload adapter records a candidate for manifest evidence
// that does not match the frozen built component/repository.
func TestRegistryPublicationHandoffRejectsManifestMismatchBeforeCandidateRecord(t *testing.T) {
	set := handoffBuiltSet()
	var events []string
	guard := newHandoffCredentialGuard(t, &events)
	source := NewPublicationCredentialSource(NewSessionManager(testGrantID, testSession(1, time.Now().Add(time.Minute)), guard), guard, validPublicationAttemptBinding())
	handoff := newTestRegistryPublicationHandoff(&handoffUploader{
		events: &events,
		mutateManifest: func(manifest *UploadedManifest) {
			manifest.Repository = "loom-task-image-attempts/arm64/44444444-4444-4444-8444-444444444444/attacker"
		},
	})

	err := handoff.AcceptWithCredentials(context.Background(), set, source)
	if err == nil {
		t.Fatal("AcceptWithCredentials() succeeded, want manifest mismatch rejection")
	}
	if len(guard.candidateRequests) != 0 {
		t.Fatalf("candidate requests = %#v, want none after manifest mismatch", guard.candidateRequests)
	}
	if !reflect.DeepEqual(events, []string{"credential:task:1", "manifest-ack:task"}) {
		t.Fatalf("events = %#v, want credential then manifest acknowledgement only", events)
	}
	guard.wantAllSecretsClosed(t)
}

// Break caught: the real registry handoff is assembled from ambient production
// configuration instead of a validated policy directly supplied by tests.
func TestRegistryPublicationHandoffConstructorBindsValidatedPolicyExpectation(t *testing.T) {
	server, ca := uploadTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	policy, err := NewRegistryUploadPolicy(server.URL, "test-registry", ca, "example.com")
	if err != nil {
		t.Fatalf("NewRegistryUploadPolicy() error = %v", err)
	}

	handoff, err := NewRegistryPublicationHandoff(policy, "loom-task-image", strings.Repeat("A", 43))
	if err != nil {
		t.Fatalf("NewRegistryPublicationHandoff() error = %v", err)
	}
	if got := handoff.PublicationRegistryExpectation(); got != (PublicationRegistryExpectation{
		RegistryOrigin:  server.URL,
		RegistryService: "test-registry",
		RegistryIssuer:  "loom-task-image",
		RegistryKeyID:   strings.Repeat("A", 43),
	}) {
		t.Fatalf("PublicationRegistryExpectation() = %#v", got)
	}
	if _, err := NewRegistryPublicationHandoff(policy, "issuer with spaces", strings.Repeat("A", 43)); err == nil {
		t.Fatal("NewRegistryPublicationHandoff() accepted invalid issuer")
	}
}

// Break caught: a later upload failure erases the first component's inert
// candidate side effect or records a candidate for the failed component.
func TestRegistryPublicationHandoffSecondComponentFailureKeepsOnlyFirstCandidateAndClosesSecrets(t *testing.T) {
	set := handoffBuiltSet()
	var events []string
	guard := newHandoffCredentialGuard(t, &events)
	source := NewPublicationCredentialSource(NewSessionManager(testGrantID, testSession(1, time.Now().Add(time.Minute)), guard), guard, validPublicationAttemptBinding())
	handoff := newTestRegistryPublicationHandoff(&handoffUploader{
		events:              &events,
		failBeforeManifest:  "sidecar:db",
		failWithPrivateText: "registry PRIVATE-TOKEN connection reset",
	})

	err := handoff.AcceptWithCredentials(context.Background(), set, source)
	if err == nil {
		t.Fatal("AcceptWithCredentials() succeeded, want second component failure")
	}
	if errors.Is(err, ErrPublicationVerificationUnavailable) {
		t.Fatalf("AcceptWithCredentials() returned verification sentinel after failed component: %v", err)
	}

	wantEvents := []string{
		"credential:task:1",
		"manifest-ack:task",
		"candidate:task",
		"credential:sidecar:db:1",
		"upload-failed:sidecar:db",
	}
	if !reflect.DeepEqual(events, wantEvents) {
		t.Fatalf("events = %#v, want %#v", events, wantEvents)
	}
	if len(guard.candidateRequests) != 1 || guard.candidateRequests[0].Component != "task" {
		t.Fatalf("candidate side effects = %#v, want only first component", guard.candidateRequests)
	}
	guard.wantAllSecretsClosed(t)
}

// Break caught: context cancellation during publication continues to the next
// component or converts a canceled partial handoff into verification success.
func TestRegistryPublicationHandoffContextCancellationStopsBeforeNextComponent(t *testing.T) {
	set := handoffBuiltSet()
	var events []string
	guard := newHandoffCredentialGuard(t, &events)
	source := NewPublicationCredentialSource(NewSessionManager(testGrantID, testSession(1, time.Now().Add(time.Minute)), guard), guard, validPublicationAttemptBinding())
	ctx, cancel := context.WithCancel(context.Background())
	handoff := newTestRegistryPublicationHandoff(&handoffUploader{
		events:              &events,
		cancelAfterManifest: cancel,
	})

	err := handoff.AcceptWithCredentials(ctx, set, source)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("AcceptWithCredentials() error = %v, want context.Canceled", err)
	}

	wantEvents := []string{
		"credential:task:1",
		"manifest-ack:task",
		"candidate:task",
	}
	if !reflect.DeepEqual(events, wantEvents) {
		t.Fatalf("events = %#v, want %#v", events, wantEvents)
	}
	if len(guard.candidateRequests) != 1 || guard.candidateRequests[0].Component != "task" {
		t.Fatalf("candidate side effects = %#v, want only first component", guard.candidateRequests)
	}
	guard.wantAllSecretsClosed(t)
}

// Break caught: ambiguous candidate transport failure retries with a fresh
// semantic operation instead of replaying the exact same candidate request.
func TestPublicationCredentialSourceRecordReplaysAmbiguousCandidateWithSameOperation(t *testing.T) {
	set := handoffBuiltSet()
	var events []string
	guard := newHandoffCredentialGuard(t, &events)
	guard.failFirstCandidate = errors.New("candidate PRIVATE-TOKEN response lost")
	source := NewPublicationCredentialSource(NewSessionManager(testGrantID, testSession(1, time.Now().Add(time.Minute)), guard), guard, validPublicationAttemptBinding())
	credential, err := source.Next(context.Background(), set, "task", nil)
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	defer source.Close(credential)

	ack, err := source.Record(context.Background(), set, credential, set.Components[0])
	if err != nil {
		t.Fatalf("Record() error = %v", err)
	}
	if ack == nil {
		t.Fatal("Record() returned nil acknowledgement")
	}
	if len(guard.candidateRequests) != 2 {
		t.Fatalf("candidate requests = %d, want original plus exact replay", len(guard.candidateRequests))
	}
	if guard.candidateRequests[0] != guard.candidateRequests[1] {
		t.Fatalf("candidate replay request drifted:\nfirst=%#v\nsecond=%#v", guard.candidateRequests[0], guard.candidateRequests[1])
	}
	if guard.candidateRequests[0].OperationID == "" || guard.candidateRequests[0].OperationID != ack.OperationID {
		t.Fatalf("replay operation id = %q, ack operation id = %q", guard.candidateRequests[0].OperationID, ack.OperationID)
	}
	if got := strings.Join(events, ","); got != "credential:task:1,candidate:task,candidate:task" {
		t.Fatalf("events = %s, want one credential and exact candidate replay", got)
	}
}

// Break caught: cleanup ambiguity is discarded and caller exits zero.
func TestOrchestratorReturnsCleanupAmbiguity(t *testing.T) {
	h := newOrchestratorHarness(t)
	h.executor.closeErr = errors.New("cgroup still populated")

	err := h.orchestrator().Run(context.Background())
	if err == nil {
		t.Fatal("Run() succeeded, want cleanup ambiguity")
	}
	if !strings.Contains(err.Error(), "cleanup_ambiguous") {
		t.Fatalf("Run() error = %v, want cleanup_ambiguous", err)
	}
}

// Break caught: Fail/Release/Finish errors are dropped as best-effort cleanup.
func TestOrchestratorReturnsGuardCleanupAmbiguity(t *testing.T) {
	for _, tc := range []struct {
		name string
		set  func(*fakeOrchestratorGuard)
	}{
		{name: "fail", set: func(g *fakeOrchestratorGuard) { g.failErr = errors.New("fail rejected") }},
		{name: "release", set: func(g *fakeOrchestratorGuard) { g.releaseErr = errors.New("release rejected") }},
		{name: "finish", set: func(g *fakeOrchestratorGuard) { g.finishErr = errors.New("finish rejected") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newOrchestratorHarness(t)
			tc.set(h.guard)
			if tc.name == "fail" {
				h.executor.buildErr["task"] = DeterministicBuildError{Reason: "dockerfile_rejected"}
			}
			err := h.orchestrator().Run(context.Background())
			if err == nil {
				t.Fatal("Run() succeeded, want cleanup ambiguity")
			}
			if !strings.Contains(err.Error(), "cleanup_ambiguous") {
				t.Fatalf("Run() error = %v, want cleanup_ambiguous", err)
			}
		})
	}
}

// Break caught: claim plan/session/request fields can be mutated without rejection.
func TestOrchestratorRejectsClaimPlanBindingMutations(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*claimMutation)
	}{
		{name: "claim id", mutate: func(m *claimMutation) { m.ClaimID = "99999999-9999-4999-8999-999999999999" }},
		{name: "plan grant", mutate: func(m *claimMutation) { m.PlanGrantID = "99999999-9999-4999-8999-999999999999" }},
		{name: "plan session", mutate: func(m *claimMutation) { m.PlanSessionID = "99999999-9999-4999-8999-999999999999" }},
		{name: "plan generation", mutate: func(m *claimMutation) { m.PlanGeneration = 99 }},
		{name: "plan materialization", mutate: func(m *claimMutation) { m.PlanMaterializationID = "99999999-9999-4999-8999-999999999999" }},
		{name: "builder id", mutate: func(m *claimMutation) { m.BuilderID = "rootless:99999999999949998999999999999999" }},
		{name: "bad digest", mutate: func(m *claimMutation) { m.TaskChecksum = strings.Repeat("0", 64) }},
		{name: "bad limits", mutate: func(m *claimMutation) { m.BundleFileLimit = 2001 }},
		{name: "bad timeout", mutate: func(m *claimMutation) { m.BuildTimeoutSeconds = 7201 }},
		{name: "component order", mutate: func(m *claimMutation) {
			m.Components[0].Name, m.Components[1].Name = m.Components[1].Name, m.Components[0].Name
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newOrchestratorHarness(t)
			tc.mutate(&h.guard.claimMutation)
			err := h.orchestrator().Run(context.Background())
			if err == nil {
				t.Fatal("Run() succeeded, want claim binding rejection")
			}
			h.wantOutcome(t, BuildOutcomeContainmentFailure, "claim_invalid")
		})
	}
}

// Break caught: sealed build claim JSON accepts duplicate keys and lets the decoder choose an ambiguous value.
func TestParseBuildClaimRejectsDuplicateKeys(t *testing.T) {
	mutation := defaultClaimMutation()
	mutation.AuthorizationExpiresAt = "2026-09-03T12:01:00Z"
	payload := strings.Replace(
		testClaimJSON(mutation),
		`"state":"claimed"`,
		`"state":"running","state":"claimed"`,
		1,
	)
	_, err := parseBuildClaim(&SecretBuffer{data: []byte(payload)}, claimBinding{
		GrantID:           testGrantID,
		ClaimID:           mutation.ClaimID,
		SessionID:         testSessionID,
		SessionGeneration: 1,
		ConfigCPUArch:     "arm64",
		Now:               testNow,
		SessionExpiresAt:  testNow.Add(time.Minute),
	})
	if err == nil || err.Error() != "claim JSON invalid" {
		t.Fatalf("parseBuildClaim() error = %v, want duplicate key rejection", err)
	}
}

// Break caught: a valid authority claim is rejected when its materialization lease extends beyond the short session used to claim it.
func TestParseBuildClaimAllowsLeasePastSessionWhenPlanAuthorizationIsSessionBound(t *testing.T) {
	mutation := defaultClaimMutation()
	mutation.AuthorizationExpiresAt = "2026-09-03T12:01:00Z"
	claim, err := parseBuildClaim(&SecretBuffer{data: []byte(testClaimJSON(mutation))}, claimBinding{
		GrantID:           testGrantID,
		ClaimID:           mutation.ClaimID,
		SessionID:         testSessionID,
		SessionGeneration: 1,
		ConfigCPUArch:     "arm64",
		Now:               testNow,
		SessionExpiresAt:  testNow.Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("parseBuildClaim() error = %v", err)
	}
	if claim.LeaseExpiresAt != testNow.Add(90*time.Second) {
		t.Fatalf("lease expiry = %v, want %v", claim.LeaseExpiresAt, testNow.Add(90*time.Second))
	}
}

// Break caught: publication credential renewal derives builder_id from a renewed
// session instead of preserving the original frozen claim builder binding.
func TestParseBuildClaimPreservesFrozenBuilderIDInBuildPlan(t *testing.T) {
	mutation := defaultClaimMutation()
	claim, err := parseBuildClaim(&SecretBuffer{data: []byte(testClaimJSON(mutation))}, claimBinding{
		GrantID:           testGrantID,
		ClaimID:           mutation.ClaimID,
		SessionID:         testSessionID,
		SessionGeneration: 1,
		ConfigCPUArch:     "arm64",
		Now:               testNow,
		SessionExpiresAt:  testNow.Add(10 * time.Minute),
	})
	if err != nil {
		t.Fatalf("parseBuildClaim() error = %v", err)
	}
	if claim.Plan.BuilderID != mutation.BuilderID {
		t.Fatalf("Plan.BuilderID = %q, want frozen claim builder_id %q", claim.Plan.BuilderID, mutation.BuilderID)
	}
}

// Break caught: BuildOutcome carries raw logs, URLs, tokens, Dockerfile text, or environment.
func TestBuildOutcomeWireRedactsForbiddenMaterialAndBoundsStatus(t *testing.T) {
	outcome := BuildOutcome{
		Status:    BuildOutcomeBuilt,
		Reason:    "built",
		Component: "task",
		Components: []BuiltComponent{{
			Name: "task",
			Output: OCIOutput{
				Path:           "/tmp/work/oci/0000.tar",
				TopLevelDigest: "sha256:" + strings.Repeat("a", 64),
				FileSHA256:     strings.Repeat("b", 64),
				SizeBytes:      123,
				OS:             "linux",
				Architecture:   "arm64",
			},
		}},
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
	h.clock = newManualClock(testNow)
	h.guard = &fakeOrchestratorGuard{
		h:                 h,
		claimAvailable:    true,
		claimAvailability: []bool{true, false, false},
		proofSHA256:       strings.Repeat("7", 64),
		sessionExpires:    testNow.Add(10 * time.Minute),
		leaseExpires:      testNow.Add(90 * time.Second),
	}
	h.guard.claimMutation = defaultClaimMutation()
	h.executor = &fakeOrchestratorExecutor{
		h:        h,
		buildErr: map[string]error{},
		closeCh:  make(chan struct{}),
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
	return h
}

type orchestratorHarness struct {
	events          []string
	clock           *manualClock
	guard           *fakeOrchestratorGuard
	executor        *fakeOrchestratorExecutor
	handoff         *fakePublicationHandoff
	overrideHandoff PublicationHandoff
	downloader      BundleDownloader
	outcomes        []BuildOutcome
}

func (h *orchestratorHarness) orchestrator() *Orchestrator {
	handoff := h.overrideHandoff
	if handoff == nil {
		handoff = h.handoff
	}
	return &Orchestrator{
		GrantID:      testGrantID,
		Config:       Config{CPUArch: "arm64"},
		Guard:        h.guard,
		Clock:        h.clock,
		NewExecutor:  func(Config, *AllocationCapabilities, BuildPlan) (BuildExecutor, error) { return h.executor, nil },
		Download:     h.downloader,
		Handoff:      handoff,
		IdleGrace:    time.Millisecond,
		CleanupGrace: time.Second,
		RecordOutcome: func(outcome BuildOutcome) {
			h.outcomes = append(h.outcomes, outcome)
		},
	}
}

func (h *orchestratorHarness) wantEvents(t *testing.T, want []string) {
	t.Helper()
	if !reflect.DeepEqual(h.events, want) {
		t.Fatalf("events = %#v, want %#v", h.events, want)
	}
}

func (h *orchestratorHarness) wantOutcome(t *testing.T, status BuildOutcomeStatus, reason string) {
	t.Helper()
	if len(h.outcomes) == 0 {
		t.Fatalf("recorded no outcomes, want status=%q reason=%q", status, reason)
	}
	got := h.outcomes[0]
	if got.Status != status || got.Reason != reason {
		t.Fatalf("outcome = %#v, want status=%q reason=%q", got, status, reason)
	}
	wire, err := got.MarshalJSON()
	if err != nil {
		t.Fatalf("outcome MarshalJSON() error = %v", err)
	}
	for _, forbidden := range []string{"loom_tib", "Dockerfile", "http://", "https://", "raw", "secret"} {
		if strings.Contains(string(wire), forbidden) {
			t.Fatalf("outcome leaked %q in %s", forbidden, wire)
		}
	}
}

type manualClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*manualTimer
}

type manualTimer struct {
	due time.Time
	ch  chan time.Time
}

func newManualClock(now time.Time) *manualClock { return &manualClock{now: now} }

func (c *manualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *manualClock) NewTimer(d time.Duration) Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	timer := &manualTimer{due: c.now.Add(d), ch: make(chan time.Time, 1)}
	c.timers = append(c.timers, timer)
	if d <= time.Millisecond {
		timer.ch <- c.now
	}
	return timer
}

func (c *manualClock) advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	now := c.now
	var due []*manualTimer
	for _, timer := range c.timers {
		if !now.Before(timer.due) {
			due = append(due, timer)
		}
	}
	c.mu.Unlock()
	for _, timer := range due {
		select {
		case timer.ch <- now:
		default:
		}
	}
}

func (t *manualTimer) C() <-chan time.Time { return t.ch }

func (t *manualTimer) Stop() bool { return true }

type fakeOrchestratorGuard struct {
	h                                 *orchestratorHarness
	claimAvailable                    bool
	claimAvailability                 []bool
	claimMutation                     claimMutation
	renewErr                          error
	renewCalls                        int
	heartbeatErr                      error
	releaseErr                        error
	failErr                           error
	finishErr                         error
	sessionExpires                    time.Time
	leaseExpires                      time.Time
	proofSHA256                       string
	failureKinds                      []string
	finishIDs                         []string
	releaseDeterministicFailureCounts []int

	rejectCanceledFailContext bool
}

func (g *fakeOrchestratorGuard) Project(ctx context.Context, grantID string) (*AllocationCapabilities, error) {
	g.h.events = append(g.h.events, "project")
	return &AllocationCapabilities{
		Bootstrap:      &SecretBuffer{data: []byte(`{"bootstrap":"redacted"}`)},
		ProofSHA256:    g.proofSHA256,
		JobDirectoryFD: -1,
		BuildEgressFD:  -1,
		closeHook: func() {
			g.h.events = append(g.h.events, "caps_close")
		},
	}, nil
}

func (g *fakeOrchestratorGuard) Exchange(ctx context.Context, grantID string, exchangeID string, proofSHA256 string, bootstrap *SecretBuffer) (*SessionEnvelope, error) {
	g.h.events = append(g.h.events, "exchange")
	if proofSHA256 != g.proofSHA256 {
		return nil, errors.New("wrong proof")
	}
	return testSession(1, g.sessionExpires), nil
}

func (g *fakeOrchestratorGuard) Renew(ctx context.Context, grantID string, operationID string, current *SecretBuffer) (*SessionEnvelope, error) {
	g.h.events = append(g.h.events, "renew")
	g.renewCalls++
	if g.renewErr != nil {
		return nil, g.renewErr
	}
	return testSession(2, g.sessionExpires.Add(10*time.Minute)), nil
}

func (g *fakeOrchestratorGuard) Claim(ctx context.Context, grantID string, operationID string, current *SecretBuffer) (*SecretBuffer, bool, error) {
	g.h.events = append(g.h.events, "claim")
	available := g.claimAvailable
	if len(g.claimAvailability) > 0 {
		available = g.claimAvailability[0]
		g.claimAvailability = g.claimAvailability[1:]
	}
	if !available {
		return nil, false, nil
	}
	mutation := g.claimMutation
	if mutation.ClaimID == defaultClaimMutation().ClaimID {
		mutation.ClaimID = operationID
	}
	return &SecretBuffer{data: []byte(testClaimJSON(mutation))}, true, nil
}

func (g *fakeOrchestratorGuard) recordIdleWait() {
	g.h.events = append(g.h.events, "idle_wait")
}

func (g *fakeOrchestratorGuard) Bundle(ctx context.Context, grantID string, operationID string, materializationID string, attemptID string, leaseEpoch int, current *SecretBuffer) (*SecretBuffer, error) {
	g.h.events = append(g.h.events, "bundle")
	return &SecretBuffer{data: []byte(`{"schema":"loom.task-image-bundle-capability/v1","redacted":true}`)}, nil
}

func (g *fakeOrchestratorGuard) RegistryCredential(context.Context, RegistryCredentialRequest, *SecretBuffer) (*SecretBuffer, error) {
	return nil, errors.New("registry credential not configured")
}

func (g *fakeOrchestratorGuard) PublicationCandidate(context.Context, PublicationCandidateRequest, *SecretBuffer) (*PublicationCandidateAcknowledgement, error) {
	return nil, errors.New("publication candidate not configured")
}

func (g *fakeOrchestratorGuard) Start(ctx context.Context, grantID string, operationID string, materializationID string, attemptID string, leaseEpoch int, current *SecretBuffer) (*LeaseResponse, error) {
	g.h.events = append(g.h.events, "start")
	return testLease("start", operationID, g.leaseExpires), nil
}

func (g *fakeOrchestratorGuard) Heartbeat(ctx context.Context, grantID string, operationID string, materializationID string, attemptID string, leaseEpoch int, current *SecretBuffer) (*LeaseResponse, error) {
	g.h.events = append(g.h.events, "heartbeat")
	if g.heartbeatErr != nil {
		return nil, g.heartbeatErr
	}
	return testLease("heartbeat", operationID, g.leaseExpires.Add(90*time.Second)), nil
}

func (g *fakeOrchestratorGuard) Release(ctx context.Context, grantID string, operationID string, materializationID string, attemptID string, leaseEpoch int, current *SecretBuffer) (*LeaseResponse, error) {
	g.h.events = append(g.h.events, "release")
	if g.releaseErr != nil {
		return nil, g.releaseErr
	}
	lease := testLease("release", operationID, time.Time{})
	g.releaseDeterministicFailureCounts = append(g.releaseDeterministicFailureCounts, lease.DeterministicFailureCount)
	return lease, nil
}

func (g *fakeOrchestratorGuard) Fail(ctx context.Context, grantID string, operationID string, materializationID string, attemptID string, leaseEpoch int, failureKind string, current *SecretBuffer) (*LeaseResponse, error) {
	g.h.events = append(g.h.events, "fail:"+failureKind)
	g.failureKinds = append(g.failureKinds, failureKind)
	if g.rejectCanceledFailContext && ctx.Err() != nil {
		return nil, errors.New("fail used canceled context")
	}
	if g.failErr != nil {
		return nil, g.failErr
	}
	return testLease("fail", operationID, time.Time{}), nil
}

func (g *fakeOrchestratorGuard) Finish(ctx context.Context, grantID string, operationID string, cleanup map[string]int) error {
	g.h.events = append(g.h.events, "finish")
	g.finishIDs = append(g.finishIDs, operationID)
	return g.finishErr
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

func testLease(operation, operationID string, expires time.Time) *LeaseResponse {
	var leaseExpiresAt *time.Time
	if !expires.IsZero() {
		leaseExpiresAt = &expires
	}
	state := "running"
	if operation == "release" || operation == "fail" {
		state = "queued"
	}
	return &LeaseResponse{
		Operation:                 operation,
		GrantID:                   testGrantID,
		OperationID:               operationID,
		MaterializationID:         testMaterializationID,
		AttemptID:                 testAttemptID,
		LeaseEpoch:                1,
		State:                     state,
		DeterministicFailureCount: 0,
		LeaseExpiresAt:            leaseExpiresAt,
	}
}

type claimMutation struct {
	ClaimID                  string
	MaterializationID        string
	AttemptID                string
	LeaseEpoch               int
	PlanGrantID              string
	PlanSessionID            string
	PlanGeneration           int
	PlanMaterializationID    string
	BuilderID                string
	TaskChecksum             string
	BundleFileMetadataSHA256 string
	BundleFileLimit          int
	BundleByteLimit          int64
	BuildTimeoutSeconds      float64
	AuthorizationExpiresAt   string
	CPUArch                  string
	Platform                 string
	Components               []buildComponentWire
}

func defaultClaimMutation() claimMutation {
	return claimMutation{
		ClaimID:                  "66666666-6666-4666-8666-666666666666",
		MaterializationID:        testMaterializationID,
		AttemptID:                testAttemptID,
		LeaseEpoch:               1,
		PlanGrantID:              testGrantID,
		PlanSessionID:            testSessionID,
		PlanGeneration:           1,
		PlanMaterializationID:    testMaterializationID,
		BuilderID:                "rootless:22222222222242228222222222222222",
		TaskChecksum:             strings.Repeat("5", 64),
		BundleFileMetadataSHA256: strings.Repeat("6", 64),
		BundleFileLimit:          2000,
		BundleByteLimit:          536870912,
		BuildTimeoutSeconds:      600,
		AuthorizationExpiresAt:   "2026-09-03T12:10:00Z",
		CPUArch:                  "arm64",
		Platform:                 "linux/arm64",
		Components: []buildComponentWire{
			{Name: "task", Dockerfile: "environment/Dockerfile", ContextPath: ".", OCIOutputPath: "oci/0000.tar"},
			{Name: "sidecar:cache", Dockerfile: "sidecars/cache/Dockerfile", ContextPath: "sidecars/cache", OCIOutputPath: "oci/0001.tar"},
		},
	}
}

func testClaimJSON(m claimMutation) string {
	componentJSON := make([]string, 0, len(m.Components))
	for _, component := range m.Components {
		componentJSON = append(componentJSON, fmt.Sprintf(`{"name":%q,"dockerfile_path":%q,"context_path":%q,"oci_output_path":%q}`, component.Name, component.Dockerfile, component.ContextPath, component.OCIOutputPath))
	}
	return fmt.Sprintf(`{
		"schema_version":"loom.task-image-materialization-claim.v1",
		"claim_id":%q,
		"materialization_id":%q,
		"attempt_id":%q,
		"lease_epoch":%d,
		"state":"claimed",
		"deterministic_failure_count":0,
		"lease_expires_at":"2026-09-03T12:01:30Z",
		"plan":{
			"schema_version":"loom.task-image-build-plan.v1",
			"grant_id":%q,
			"session_id":%q,
			"session_generation":%d,
			"materialization_id":%q,
			"builder_id":%q,
			"task_id":"phase2c/session-bound",
			"task_checksum":%q,
			"cpu_arch":%q,
			"platform":%q,
			"bundle_bucket":"loom-bundles",
			"bundle_prefix":"phase2c/session-bound/",
			"bundle_file_metadata_sha256":%q,
			"bundle_file_limit":%d,
			"bundle_byte_limit":%d,
			"build_timeout_seconds":%g,
			"authorization_expires_at":%q,
			"components":[%s]
		}
	}`, m.ClaimID, m.MaterializationID, m.AttemptID, m.LeaseEpoch, m.PlanGrantID, m.PlanSessionID, m.PlanGeneration, m.PlanMaterializationID, m.BuilderID, m.TaskChecksum, m.CPUArch, m.Platform, m.BundleFileMetadataSHA256, m.BundleFileLimit, m.BundleByteLimit, m.BuildTimeoutSeconds, m.AuthorizationExpiresAt, strings.Join(componentJSON, ","))
}

type fakeOrchestratorExecutor struct {
	h          *orchestratorHarness
	outputs    map[string]OCIOutput
	buildErr   map[string]error
	closeErr   error
	closeCalls int
	closeInit  sync.Once
	closeOnce  sync.Once
	closeCh    chan struct{}
	afterBuild func(string)
	blockBuild func(context.Context, string) (OCIOutput, error)
}

func (e *fakeOrchestratorExecutor) Start(ctx context.Context) error {
	e.h.events = append(e.h.events, "executor_start")
	return nil
}

func (e *fakeOrchestratorExecutor) Build(ctx context.Context, component BuildComponent) (OCIOutput, error) {
	e.h.events = append(e.h.events, "build:"+component.Name)
	if e.blockBuild != nil {
		return e.blockBuild(ctx, component.Name)
	}
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
	closeCh := e.ensureCloseCh()
	e.closeOnce.Do(func() {
		close(closeCh)
	})
	return e.closeErr
}

func (e *fakeOrchestratorExecutor) closed() <-chan struct{} {
	return e.ensureCloseCh()
}

func (e *fakeOrchestratorExecutor) ensureCloseCh() chan struct{} {
	e.closeInit.Do(func() {
		if e.closeCh == nil {
			e.closeCh = make(chan struct{})
		}
	})
	return e.closeCh
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

type captureCredentialedPublicationHandoff struct {
	expectation PublicationRegistryExpectation
	events      *[]string
	err         error
	set         BuiltComponentSet
	source      *PublicationCredentialSource
}

func (h *captureCredentialedPublicationHandoff) Accept(context.Context, BuiltComponentSet) error {
	return errors.New("plain handoff called without publication credential source")
}

func (h *captureCredentialedPublicationHandoff) PublicationRegistryExpectation() PublicationRegistryExpectation {
	return h.expectation
}

func (h *captureCredentialedPublicationHandoff) AcceptWithCredentials(ctx context.Context, set BuiltComponentSet, source *PublicationCredentialSource) error {
	h.set = set
	h.source = source
	*h.events = append(*h.events, "credentialed_handoff")
	return h.err
}

type handoffUploader struct {
	events              *[]string
	renewComponents     map[string]bool
	failBeforeManifest  string
	failWithPrivateText string
	cancelAfterManifest func()
	mutateManifest      func(*UploadedManifest)
}

func newTestRegistryPublicationHandoff(u registryPublicationUploader) *RegistryPublicationHandoff {
	return &RegistryPublicationHandoff{
		uploader: u,
		expectation: PublicationRegistryExpectation{
			RegistryOrigin:  "https://registry.example",
			RegistryService: "registry.example",
			RegistryIssuer:  "loom-task-image",
			RegistryKeyID:   strings.Repeat("A", 43),
		},
	}
}

func (u *handoffUploader) Upload(ctx context.Context, output OCIOutput, source RegistryUploadCredentialSource) (UploadedManifest, error) {
	if err := ctx.Err(); err != nil {
		return UploadedManifest{}, err
	}
	credential, err := source.Next(ctx, nil)
	if err != nil {
		return UploadedManifest{}, err
	}
	defer func() {
		if credential != nil {
			source.Close(credential)
		}
	}()
	if u.renewComponents[credential.Component] {
		next, err := source.Next(ctx, credential)
		if err != nil {
			return UploadedManifest{}, err
		}
		credential = next
	}
	if credential.Component == u.failBeforeManifest {
		*u.events = append(*u.events, "upload-failed:"+credential.Component)
		message := u.failWithPrivateText
		if message == "" {
			message = "registry upload failed"
		}
		return UploadedManifest{}, errors.New(message)
	}
	manifest := UploadedManifest{
		Repository: credential.Repository,
		Digest:     output.TopLevelDigest,
		MediaType:  output.ManifestMediaType,
		Size:       output.ManifestSize,
	}
	*u.events = append(*u.events, "manifest-ack:"+credential.Component)
	if u.mutateManifest != nil {
		u.mutateManifest(&manifest)
	}
	if err := source.UploadSucceeded(ctx, manifest, credential); err != nil {
		return UploadedManifest{}, err
	}
	if u.cancelAfterManifest != nil {
		u.cancelAfterManifest()
	}
	return manifest, nil
}

func handoffBuiltSet() BuiltComponentSet {
	return BuiltComponentSet{
		GrantID:           testGrantID,
		MaterializationID: testMaterializationID,
		AttemptID:         testAttemptID,
		LeaseEpoch:        1,
		Components: []BuiltComponent{
			{
				Name: "task",
				Output: OCIOutput{
					Path:              "/tmp/not-used/task.tar",
					TopLevelDigest:    "sha256:" + strings.Repeat("a", 64),
					ManifestSize:      321,
					ManifestMediaType: ociManifestMediaType,
					FileSHA256:        strings.Repeat("b", 64),
					SizeBytes:         5678,
					OS:                "linux",
					Architecture:      "arm64",
				},
			},
			{
				Name: "sidecar:db",
				Output: OCIOutput{
					Path:              "/tmp/not-used/db.tar",
					TopLevelDigest:    "sha256:" + strings.Repeat("c", 64),
					ManifestSize:      654,
					ManifestMediaType: ociManifestMediaType,
					FileSHA256:        strings.Repeat("d", 64),
					SizeBytes:         8765,
					OS:                "linux",
					Architecture:      "arm64",
				},
			},
		},
	}
}

type handoffCredentialGuard struct {
	t                  *testing.T
	events             *[]string
	sessionGeneration  int
	credentialCounters map[string]int
	secrets            []*SecretBuffer
	lastHeartbeat      string
	failFirstCandidate error
	candidateRequests  []PublicationCandidateRequest
}

func newHandoffCredentialGuard(t *testing.T, events *[]string) *handoffCredentialGuard {
	t.Helper()
	return &handoffCredentialGuard{
		t:                  t,
		events:             events,
		sessionGeneration:  1,
		credentialCounters: map[string]int{},
	}
}

func (g *handoffCredentialGuard) Renew(context.Context, string, string, *SecretBuffer) (*SessionEnvelope, error) {
	*g.events = append(*g.events, "renew")
	g.sessionGeneration++
	return testSession(g.sessionGeneration, time.Now().Add(time.Minute)), nil
}

func (g *handoffCredentialGuard) Heartbeat(ctx context.Context, grantID string, operationID string, materializationID string, attemptID string, leaseEpoch int, current *SecretBuffer) (*LeaseResponse, error) {
	*g.events = append(*g.events, "heartbeat")
	g.lastHeartbeat = operationID
	return testLease("heartbeat", operationID, time.Now().Add(time.Minute)), nil
}

func (g *handoffCredentialGuard) RegistryCredential(ctx context.Context, request RegistryCredentialRequest, current *SecretBuffer) (*SecretBuffer, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	g.credentialCounters[request.Component]++
	generation := g.credentialCounters[request.Component]
	*g.events = append(*g.events, fmt.Sprintf("credential:%s:%d", request.Component, generation))
	repo, err := publicationRepository("arm64", testAttemptID, request.Component)
	if err != nil {
		g.t.Fatalf("publicationRepository() error = %v", err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	mutation := registryCredentialMutation{
		CredentialID:          uuidWithTail(0x7000 + len(g.secrets) + 1),
		RequestID:             request.OperationID,
		Component:             request.Component,
		Repository:            repo,
		Generation:            generation,
		SessionGeneration:     g.sessionGeneration,
		AttestationGeneration: g.sessionGeneration,
		IssuedAt:              now.Format(time.RFC3339),
		ExpiresAt:             now.Add(45 * time.Second).Format(time.RFC3339),
	}
	if generation > 1 {
		mutation.PredecessorCredentialIDJSON = fmt.Sprintf("%q", request.PredecessorCredentialID)
		mutation.PredecessorGenerationJSON = fmt.Sprintf("%d", request.PredecessorGeneration)
		mutation.HeartbeatOperationIDJSON = fmt.Sprintf("%q", g.lastHeartbeat)
	}
	secret := &SecretBuffer{data: []byte(validRegistryCredentialJSON(mutation))}
	g.secrets = append(g.secrets, secret)
	return secret, nil
}

func (g *handoffCredentialGuard) PublicationCandidate(ctx context.Context, request PublicationCandidateRequest, current *SecretBuffer) (*PublicationCandidateAcknowledgement, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	g.candidateRequests = append(g.candidateRequests, request)
	*g.events = append(*g.events, "candidate:"+request.Component)
	if g.failFirstCandidate != nil && len(g.candidateRequests) == 1 {
		return nil, g.failFirstCandidate
	}
	return &PublicationCandidateAcknowledgement{
		CandidateID:             uuidWithTail(0x9000 + len(g.candidateRequests)),
		OperationID:             request.OperationID,
		CredentialID:            request.CredentialID,
		CredentialGeneration:    request.CredentialGeneration,
		GrantID:                 request.GrantID,
		SessionID:               request.SessionID,
		SessionGeneration:       request.SessionGeneration,
		MaterializationID:       request.MaterializationID,
		AttemptID:               request.AttemptID,
		AttemptNumber:           request.AttemptNumber,
		LeaseEpoch:              request.LeaseEpoch,
		BuilderID:               request.BuilderID,
		Component:               request.Component,
		ManifestDigest:          request.ManifestDigest,
		ManifestSize:            request.ManifestSize,
		OCIFileSHA256:           request.OCIFileSHA256,
		OCIFileSize:             request.OCIFileSize,
		Platform:                request.Platform,
		RecordedAt:              testNow,
		AuthorityResponseSHA256: strings.Repeat("c", 64),
	}, nil
}

func (g *handoffCredentialGuard) wantAllSecretsClosed(t *testing.T) {
	t.Helper()
	if len(g.secrets) == 0 {
		t.Fatal("no credential secrets were issued")
	}
	for i, secret := range g.secrets {
		if secret == nil || !secret.closed {
			t.Fatalf("credential secret %d closed = %v, want true", i, secret != nil && secret.closed)
		}
		for offset, value := range secret.data {
			if value != 0 {
				t.Fatalf("credential secret %d byte %d = %d, want zeroized", i, offset, value)
			}
		}
	}
}

func uuidWithTail(tail int) string {
	return fmt.Sprintf("77777777-7777-4777-8777-%012x", tail)
}
