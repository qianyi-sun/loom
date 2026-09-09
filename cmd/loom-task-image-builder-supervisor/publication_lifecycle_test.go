package main

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

type publicationTestClock struct {
	*manualClock
	armed chan time.Duration
}

func (c *publicationTestClock) NewTimer(d time.Duration) Timer {
	timer := c.manualClock.NewTimer(d)
	c.armed <- d
	return timer
}

type publicationLifecycleTestGuard struct {
	mu           sync.Mutex
	clock        Clock
	bindings     []publicationStatusBinding
	operations   []string
	states       []string
	heartbeatErr error
	renewals     int
	heartbeats   int
	mutate       func(*publicationStatus)
	statusHook   func()
}

func (g *publicationLifecycleTestGuard) Renew(_ context.Context, _ string, _ string, current *SecretBuffer) (*SessionEnvelope, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if current == nil || current.closed {
		return nil, errors.New("missing session")
	}
	g.renewals++
	return testSession(g.renewals+1, g.clock.Now().Add(time.Minute)), nil
}

func (g *publicationLifecycleTestGuard) Heartbeat(_ context.Context, grant, operation, materialization, attempt string, epoch int, _ *SecretBuffer) (*LeaseResponse, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.heartbeats++
	if g.heartbeatErr != nil {
		return nil, g.heartbeatErr
	}
	expires := g.clock.Now().Add(time.Minute)
	return &LeaseResponse{Operation: "heartbeat", GrantID: grant, OperationID: operation, MaterializationID: materialization, AttemptID: attempt, LeaseEpoch: epoch, State: "running", LeaseExpiresAt: &expires}, nil
}

func (g *publicationLifecycleTestGuard) PublicationSubmit(_ context.Context, binding publicationStatusBinding, _ *SecretBuffer) (*publicationStatus, error) {
	return g.status("submit", binding)
}
func (g *publicationLifecycleTestGuard) PublicationPoll(_ context.Context, binding publicationStatusBinding, _ *SecretBuffer) (*publicationStatus, error) {
	return g.status("poll", binding)
}
func (g *publicationLifecycleTestGuard) status(operation string, binding publicationStatusBinding) (*publicationStatus, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.statusHook != nil {
		g.statusHook()
	}
	g.bindings = append(g.bindings, binding)
	g.operations = append(g.operations, operation)
	state := "completed"
	if len(g.states) > 0 {
		state = g.states[0]
		g.states = g.states[1:]
	}
	if state == "transport" {
		return nil, errors.New("sentinel-private-authority")
	}
	status := &publicationStatus{GrantID: binding.GrantID, OperationID: binding.OperationID, MaterializationID: binding.MaterializationID,
		AttemptID: binding.AttemptID, LeaseEpoch: binding.LeaseEpoch, State: state, SnapshotSHA256: strings.Repeat("a", 64), CandidateSetSHA256: binding.CandidateSetSHA256, ComponentCount: binding.ComponentCount}
	if state == "completed" {
		status.Receipt = &publicationReceipt{OperationID: binding.OperationID, MaterializationID: binding.MaterializationID, AttemptID: binding.AttemptID,
			LeaseEpoch: binding.LeaseEpoch, WorkerGeneration: 1, SnapshotSHA256: status.SnapshotSHA256, CandidateSetSHA256: binding.CandidateSetSHA256,
			PublicationSetSHA256: strings.Repeat("b", 64), ComponentCount: binding.ComponentCount, CompletedAt: g.clock.Now().Truncate(time.Second)}
	}
	if state == "failed" {
		status.FailureCode = "integrity"
	}
	if g.mutate != nil {
		g.mutate(status)
	}
	return status, nil
}

func TestPublicationLifecycleMaintainsLivenessWhileUploadAndVerificationAreSlow(t *testing.T) {
	for _, phase := range []string{"upload", "verification"} {
		t.Run(phase, func(t *testing.T) {
			p, guard, clock := publicationLifecycleFixture(t)
			if phase == "verification" {
				guard.states = make([]string, 80)
				for i := range guard.states {
					guard.states[i] = "running"
				}
			} else {
				original := p.upload
				p.upload = func(ctx context.Context) ([]PublicationCandidateV2Acknowledgement, error) {
					timer := clock.NewTimer(80 * time.Second)
					defer timer.Stop()
					select {
					case <-timer.C():
						return original(ctx)
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				}
			}
			done := make(chan error, 1)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go func() {
				receipt, err := p.run(ctx)
				if err == nil && receipt == nil {
					err = errors.New("missing receipt")
				}
				done <- err
			}()
			for {
				select {
				case err := <-done:
					if err != nil {
						t.Fatal(err)
					}
					if guard.renewals < 3 || guard.heartbeats < 3 {
						t.Fatalf("liveness absent: renewals=%d heartbeats=%d", guard.renewals, guard.heartbeats)
					}
					return
				case d := <-clock.armed:
					if d <= 20*time.Second {
						clock.advance(d)
					}
				case <-time.After(2 * time.Second):
					t.Fatal("slow phase stalled")
				}
			}
		})
	}
}

func TestPublicationLifecycleHeartbeatConflictRequiresExactCompletedPoll(t *testing.T) {
	for _, terminal := range []string{"completed", "running", "transport", "wrong-receipt"} {
		t.Run(terminal, func(t *testing.T) {
			p, guard, clock := publicationLifecycleFixture(t)
			guard.heartbeatErr = errors.New("lease cleared or authority unavailable")
			guard.states = []string{"queued", terminal}
			guard.statusHook = func() {
				if len(guard.bindings) == 1 {
					return
				}
				clock.advance(20 * time.Second)
			}
			if terminal == "wrong-receipt" {
				guard.states[1] = "completed"
				guard.mutate = func(s *publicationStatus) {
					if s.Receipt != nil {
						s.Receipt.AttemptID = uuidWithTail(999)
					}
				}
			}
			receipt, err := drivePublication(t, p, clock)
			if (err == nil && receipt != nil) != (terminal == "completed") {
				t.Fatalf("terminal=%s receipt=%v err=%v", terminal, receipt, err)
			}
			if guard.heartbeats != 1 || len(guard.bindings) != 2 || guard.operations[1] != "poll" {
				t.Fatalf("completion race not exercised: heartbeats=%d operations=%v", guard.heartbeats, guard.operations)
			}
		})
	}
}

func TestPublicationLifecycleCancellationAndDeadlineJoinUpload(t *testing.T) {
	for _, condition := range []string{"cancel", "deadline"} {
		t.Run(condition, func(t *testing.T) {
			p, guard, clock := publicationLifecycleFixture(t)
			started, cancelled, allowExit := make(chan struct{}), make(chan struct{}), make(chan struct{})
			p.upload = func(ctx context.Context) ([]PublicationCandidateV2Acknowledgement, error) {
				close(started)
				<-ctx.Done()
				close(cancelled)
				<-allowExit
				return nil, ctx.Err()
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { _, err := p.run(ctx); done <- err }()
			<-started
			if condition == "cancel" {
				cancel()
			} else {
				clock.advance(time.Hour)
			}
			select {
			case <-cancelled:
			case <-time.After(2 * time.Second):
				close(allowExit)
				t.Fatal("upload not cancelled")
			}
			select {
			case <-done:
				close(allowExit)
				t.Fatal("returned before upload joined")
			default:
			}
			close(allowExit)
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("cancelled upload succeeded")
				}
			case <-time.After(2 * time.Second):
				t.Fatal("upload did not join")
			}
			if len(guard.bindings) != 0 {
				t.Fatal("cancelled upload submitted")
			}
		})
	}
}

func TestPublicationLifecycleRejectsWrongManagerGrantBeforeUpload(t *testing.T) {
	p, guard, clock := publicationLifecycleFixture(t)
	other := testSession(1, testNow.Add(time.Minute))
	other.GrantID = uuidWithTail(999)
	p.session = NewSessionManager(other.GrantID, other, guard)
	defer p.session.Close()
	uploaded := false
	p.upload = func(context.Context) ([]PublicationCandidateV2Acknowledgement, error) {
		uploaded = true
		return nil, errors.New("should not upload")
	}
	if receipt, err := drivePublication(t, p, clock); err == nil || receipt != nil || uploaded {
		t.Fatal("wrong current grant reached upload")
	}
}

func publicationLifecycleFixture(t *testing.T) (*publicationLifecycle, *publicationLifecycleTestGuard, *publicationTestClock) {
	t.Helper()
	clock := &publicationTestClock{manualClock: newManualClock(testNow), armed: make(chan time.Duration, 100)}
	guard := &publicationLifecycleTestGuard{clock: clock}
	initial := testSession(1, testNow.Add(time.Minute))
	manager := NewSessionManager(testGrantID, initial, guard)
	t.Cleanup(manager.Close)
	set := handoffBuiltSet()
	acks := make([]PublicationCandidateV2Acknowledgement, 0, len(set.Components))
	for index, component := range set.Components {
		ack := candidateAcknowledgement(PublicationCandidateRequest{GrantID: set.GrantID, OperationID: uuidWithTail(100 + index),
			CredentialID: uuidWithTail(200 + index), CredentialGeneration: 1, SessionID: initial.SessionID, SessionGeneration: 1,
			MaterializationID: set.MaterializationID, AttemptID: set.AttemptID, AttemptNumber: 1, LeaseEpoch: set.LeaseEpoch,
			BuilderID: validPublicationAttemptBinding().BuilderID, Component: component.Name, ManifestDigest: component.Output.TopLevelDigest,
			ManifestSize: component.Output.ManifestSize, OCIFileSHA256: component.Output.FileSHA256, OCIFileSize: component.Output.SizeBytes, Platform: "linux/arm64"})
		ack.CandidateID = uuidWithTail(300 + index)
		acks = append(acks, PublicationCandidateV2Acknowledgement{PublicationCandidateAcknowledgement: *ack, BaseResolution: component.BaseResolution})
	}
	return &publicationLifecycle{clock: clock, session: manager, guard: guard, set: set, builderID: validPublicationAttemptBinding().BuilderID,
		leaseExpiresAt: testNow.Add(time.Minute), timeout: time.Hour,
		upload: func(context.Context) ([]PublicationCandidateV2Acknowledgement, error) { return acks, nil }}, guard, clock
}

func drivePublication(t *testing.T, lifecycle *publicationLifecycle, clock *publicationTestClock) (*publicationReceipt, error) {
	t.Helper()
	type result struct {
		receipt *publicationReceipt
		err     error
	}
	done := make(chan result, 1)
	go func() { receipt, err := lifecycle.run(context.Background()); done <- result{receipt, err} }()
	for {
		select {
		case got := <-done:
			return got.receipt, got.err
		case duration := <-clock.armed:
			if duration <= 10*time.Second {
				clock.advance(duration)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("publication controller did not finish")
			return nil, nil
		}
	}
}

func TestPublicationLifecyclePinsFullSetThroughQueuedRunningCompletion(t *testing.T) {
	lifecycle, guard, clock := publicationLifecycleFixture(t)
	guard.states = []string{"queued", "running", "completed"}
	receipt, err := drivePublication(t, lifecycle, clock)
	if err != nil || receipt == nil {
		t.Fatalf("receipt=%#v err=%v", receipt, err)
	}
	if len(guard.bindings) != 3 || guard.operations[0] != "submit" || guard.operations[1] != "poll" || guard.operations[2] != "poll" {
		t.Fatal("unexpected publication sequence")
	}
	first := guard.bindings[0]
	if first.PinnedSnapshotSHA256 != "" {
		t.Fatal("first snapshot was invented")
	}
	if first.ComponentCount != 2 || first.CandidateSetSHA256 != receipt.CandidateSetSHA256 {
		t.Fatal("complete identity binding lost")
	}
	for _, binding := range guard.bindings[1:] {
		if binding.OperationID != first.OperationID || binding.PinnedSnapshotSHA256 != receipt.SnapshotSHA256 {
			t.Fatal("operation or snapshot drift")
		}
	}
}

func TestPublicationLifecycleRetriesAmbiguousSubmitWithSameOperation(t *testing.T) {
	lifecycle, guard, clock := publicationLifecycleFixture(t)
	guard.states = []string{"transport", "queued", "completed"}
	if receipt, err := drivePublication(t, lifecycle, clock); err != nil || receipt == nil {
		t.Fatalf("receipt=%#v err=%v", receipt, err)
	}
	if len(guard.bindings) != 3 || guard.operations[0] != "submit" || guard.operations[1] != "submit" || guard.bindings[0].OperationID != guard.bindings[1].OperationID {
		t.Fatal("ambiguous submit changed identity")
	}
}

func TestPublicationLifecycleRejectsPartialAndSubstitutedAcknowledgementsBeforeSubmit(t *testing.T) {
	for _, condition := range []string{"partial", "duplicate", "wrong-root", "wrong-evidence"} {
		t.Run(condition, func(t *testing.T) {
			lifecycle, guard, clock := publicationLifecycleFixture(t)
			original := lifecycle.upload
			lifecycle.upload = func(ctx context.Context) ([]PublicationCandidateV2Acknowledgement, error) {
				acks, err := original(ctx)
				switch condition {
				case "partial":
					acks = acks[:1]
				case "duplicate":
					acks[1].CandidateID = acks[0].CandidateID
				case "wrong-root":
					acks[0].ManifestDigest = "sha256:" + strings.Repeat("f", 64)
				case "wrong-evidence":
					acks[0].BaseResolution = BaseResolutionEvidence{}
				}
				return acks, err
			}
			if receipt, err := drivePublication(t, lifecycle, clock); err == nil || receipt != nil {
				t.Fatal("invalid upload input completed")
			}
			if len(guard.bindings) != 0 {
				t.Fatal("partial or substituted set reached authority")
			}
		})
	}
}

func TestPublicationLifecycleRevalidatesTerminalAdapterBindings(t *testing.T) {
	for _, condition := range []string{"missing-receipt", "wrong-candidate-set", "wrong-receipt-count", "failed"} {
		t.Run(condition, func(t *testing.T) {
			lifecycle, guard, clock := publicationLifecycleFixture(t)
			guard.mutate = func(status *publicationStatus) {
				switch condition {
				case "missing-receipt":
					status.Receipt = nil
				case "wrong-candidate-set":
					status.CandidateSetSHA256 = strings.Repeat("f", 64)
				case "wrong-receipt-count":
					if status.Receipt != nil {
						status.Receipt.ComponentCount = 1
					}
				case "failed":
					status.State = "failed"
					status.Receipt = nil
					status.FailureCode = "integrity"
				}
			}
			if receipt, err := drivePublication(t, lifecycle, clock); err == nil || receipt != nil {
				t.Fatal("inexact or failed status accepted")
			}
		})
	}
}
