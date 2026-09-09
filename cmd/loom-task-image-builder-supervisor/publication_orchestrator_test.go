package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	"testing"
	"time"
)

type publicationOrchestratorGuard struct {
	*fakeOrchestratorGuard
	credentials *handoffCredentialGuard
	publication *publicationLifecycleTestGuard
	lastSession *SessionEnvelope
}

func (g *publicationOrchestratorGuard) Renew(ctx context.Context, grant, operation string, current *SecretBuffer) (*SessionEnvelope, error) {
	next, err := g.credentials.Renew(ctx, grant, operation, current)
	g.lastSession = next
	return next, err
}

func (g *publicationOrchestratorGuard) Heartbeat(ctx context.Context, grant, operation, materialization, attempt string, epoch int, current *SecretBuffer) (*LeaseResponse, error) {
	return g.credentials.Heartbeat(ctx, grant, operation, materialization, attempt, epoch, current)
}

func (g *publicationOrchestratorGuard) RegistryCredential(ctx context.Context, request RegistryCredentialRequest, current *SecretBuffer) (*SecretBuffer, error) {
	return g.credentials.RegistryCredential(ctx, request, current)
}

func (g *publicationOrchestratorGuard) PublicationCandidateV2(ctx context.Context, request PublicationCandidateV2Request, current *SecretBuffer) (*PublicationCandidateV2Acknowledgement, error) {
	return g.credentials.PublicationCandidateV2(ctx, request, current)
}

func (g *publicationOrchestratorGuard) PublicationSubmit(ctx context.Context, binding publicationStatusBinding, current *SecretBuffer) (*publicationStatus, error) {
	return g.publication.PublicationSubmit(ctx, binding, current)
}

func (g *publicationOrchestratorGuard) PublicationPoll(ctx context.Context, binding publicationStatusBinding, current *SecretBuffer) (*publicationStatus, error) {
	return g.publication.PublicationPoll(ctx, binding, current)
}

func TestPublicationOrchestratorUsesReceiptInsteadOfReleasingCompletedLease(t *testing.T) {
	for _, terminal := range []string{"completed", "failed"} {
		t.Run(terminal, func(t *testing.T) {
			h := newOrchestratorHarness(t)
			var uploadEvents []string
			guard := &publicationOrchestratorGuard{fakeOrchestratorGuard: h.guard, credentials: newHandoffCredentialGuard(t, &uploadEvents), publication: &publicationLifecycleTestGuard{clock: h.clock, states: []string{terminal}}}
			h.executor.baseResolutions = make(map[string]BaseResolutionEvidence)
			for name, output := range h.executor.outputs {
				output.ManifestSize, output.ManifestMediaType = 100, ociManifestMediaType
				h.executor.outputs[name] = output
				h.executor.baseResolutions[name] = testBaseResolutionEvidence("solve-test", "linux/arm64", output.TopLevelDigest)
			}
			h.overrideHandoff = newTestRegistryPublicationHandoff(&handoffUploader{events: &uploadEvents, renewComponents: map[string]bool{"task": true}})
			o := h.orchestrator()
			o.Guard = guard
			err := o.Run(context.Background())
			if (err == nil) != (terminal == "completed") {
				t.Fatalf("terminal=%s err=%v", terminal, err)
			}
			if len(guard.publication.bindings) != 1 {
				t.Fatal("actual registry handoff did not submit complete publication")
			}
			releases, failures := 0, 0
			for _, event := range h.events {
				if event == "release" {
					releases++
				}
				if event == "fail" {
					failures++
				}
			}
			wantReleases := 0
			if terminal == "failed" {
				wantReleases = 1
			}
			if releases != wantReleases || failures != 0 {
				t.Fatalf("releases=%d failures=%d events=%v", releases, failures, h.events)
			}
			if guard.lastSession == nil || !guard.lastSession.Secret.closed {
				t.Fatal("refreshed successor session leaked")
			}
			guard.credentials.wantAllSecretsClosed(t)
			if terminal == "completed" {
				h.wantOutcome(t, BuildOutcomeBuilt, "built")
			} else {
				h.wantOutcome(t, BuildOutcomeTransientFailure, "publication_failed")
			}
		})
	}
}

func TestPublicationOrchestratorNextClaimBindsActualCurrentSuccessor(t *testing.T) {
	h := newOrchestratorHarness(t)
	old := testSession(1, testNow.Add(time.Minute))
	manager := NewSessionManager(testGrantID, old, h.guard)
	defer manager.Close()
	if _, err := manager.Renew(context.Background()); err != nil {
		t.Fatal(err)
	}
	s := &orchestratorState{o: h.orchestrator(), ctx: context.Background(), clock: h.clock, session: old, sessionManager: manager}
	claim, _, err := s.claim(uuidWithTail(999))
	if claim != nil {
		defer claim.Close()
	}
	if err != nil {
		t.Fatal(err)
	}
	if s.session.Generation != 2 || s.session.Secret.closed {
		t.Fatal("next claim still bound to stale original session")
	}
}

func TestPublicationOrchestratorRenewSchedulingUsesManagerExpiry(t *testing.T) {
	h := newOrchestratorHarness(t)
	old := testSession(1, testNow.Add(time.Second))
	manager := NewSessionManager(testGrantID, old, h.guard)
	defer manager.Close()
	if _, err := manager.Renew(context.Background()); err != nil {
		t.Fatal(err)
	}
	s := &orchestratorState{o: h.orchestrator(), ctx: context.Background(), clock: h.clock, session: old, sessionManager: manager}
	if err := s.renewIfDue(); err != nil {
		t.Fatal(err)
	}
	if h.guard.renewCalls != 1 {
		t.Fatal("stale cached expiry triggered redundant renewal")
	}
}

func TestPublicationLifecycleOwnsIndependentSidecarOnlyHash(t *testing.T) {
	p, _, clock := publicationLifecycleFixture(t)
	p.set.Components = p.set.Components[1:]
	original := p.upload
	p.upload = func(ctx context.Context) ([]PublicationCandidateV2Acknowledgement, error) {
		acks, err := original(ctx)
		return acks[1:], err
	}
	receipt, err := drivePublication(t, p, clock)
	canonical := fmt.Sprintf(`{"components":[{"candidate_id":%q,"component":"sidecar:db"}],"schema":"loom.task-image-publication-candidate-set/v1"}`, uuidWithTail(301))
	wantHash := fmt.Sprintf("%x", sha256.Sum256([]byte(canonical)))
	if err != nil || receipt == nil || receipt.ComponentCount != 1 || receipt.CandidateSetSHA256 != wantHash {
		t.Fatalf("sidecar-only receipt=%v err=%v", receipt, err)
	}
}
