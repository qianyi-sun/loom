package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"syscall"
	"testing"
	"time"
)

type registeredPrebuildGuard struct {
	TaskImageGuard
	capability *SecretBuffer
	calls      int
	bundle     func(context.Context) (*SecretBuffer, error)
}

func (g *registeredPrebuildGuard) Bundle(ctx context.Context, _, _, _, _ string, _ int, secret *SecretBuffer) (*SecretBuffer, error) {
	g.calls++
	if !strings.Contains(string(secret.data), "current-issuance") {
		panic("bundle did not use owned current session")
	}
	if g.bundle != nil {
		return g.bundle(ctx)
	}
	return g.capability, nil
}

func registeredPreparationFixture(t *testing.T) (*orchestratorState, *registeredPrebuildGuard, RegisteredBundlePlan) {
	t.Helper()
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for _, file := range registeredManifestVector() {
			if strings.HasSuffix(r.URL.Path, "/"+file.RelativePath) {
				_, _ = w.Write(make([]byte, file.SizeBytes))
				return
			}
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(server.Close)
	secret, plan, session, trust := registeredDownloadFixture(t, server)
	current := mustSessionEnvelope(t, session.Generation, "current-issuance")
	current.GrantID, current.SessionID, current.Generation = plan.GrantID, session.SessionID, session.Generation
	current.IssuedAt, current.ExpiresAt = time.Now(), session.ExpiresAt
	manager := NewSessionManager(plan.GrantID, current, &stubSessionClient{})
	t.Cleanup(manager.Close)
	guard := &registeredPrebuildGuard{capability: secret}
	fd := openDirectoryFD(t, t.TempDir())
	t.Cleanup(func() { syscall.Close(fd) })
	expiry := time.Now().Add(time.Minute)
	claim := &buildClaim{MaterializationID: plan.MaterializationID, AttemptID: testAttemptID, LeaseEpoch: 1,
		RegisteredBundle: &plan, LeaseExpiresAtPtr: &expiry,
		Plan: BuildPlan{BuilderID: "rootless:original-claim-provenance", BuildTimeout: time.Minute}}
	state := &orchestratorState{ctx: context.Background(), clock: realClock{}, sessionManager: manager, claimData: claim,
		o:    &Orchestrator{GrantID: plan.GrantID, Guard: guard, Config: Config{Bundle: &trust}},
		caps: &AllocationCapabilities{JobDirectoryFD: fd}}
	return state, guard, plan
}

func TestRegisteredPreparationComposesCurrentSessionWithRealTLSDownloader(t *testing.T) {
	state, guard, plan := registeredPreparationFixture(t)
	got, err := state.prepareRegisteredBundle()
	if err != nil {
		t.Fatal(err)
	}
	defer got.Close()
	if guard.calls != 1 || got.ManifestSHA256 != plan.ManifestSHA256 || !guard.capability.closed {
		t.Fatal("preparation lost content or secret ownership")
	}
	if state.sessionManager.Generation() != 3 {
		t.Fatal("preparation rewrote current session")
	}
	if state.claimData.Plan.BuilderID != "rootless:original-claim-provenance" {
		t.Fatal("current issuance rewrote original builder identity")
	}
	contextFD, err := got.DupDirectoryFD()
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.Close(contextFD)
	for _, file := range registeredManifestVector() {
		actual, err := HashFileAt(contextFD, file.RelativePath)
		if err != nil || actual.SHA256 != file.SHA256 {
			t.Fatal("preparation did not verify downloaded content")
		}
	}
}

func TestRegisteredPreparationRetriesOnlyBoundedSuccessorIssuanceRaces(t *testing.T) {
	for _, kind := range []string{"changed_once", "always_changed", "unchanged_error"} {
		t.Run(kind, func(t *testing.T) {
			state, guard, _ := registeredPreparationFixture(t)
			manager := state.sessionManager
			var wire registeredBundleWire
			if err := json.Unmarshal(guard.capability.data, &wire); err != nil {
				t.Fatal(err)
			}
			generation := 3
			manager.client = &stubSessionClient{renew: func(context.Context, string, string, *SecretBuffer) (*SessionEnvelope, error) {
				generation++
				next := mustSessionEnvelope(t, generation, "current-issuance")
				next.GrantID, next.SessionID = wire.GrantID, uuidWithTail(200+generation)
				next.IssuedAt, next.ExpiresAt = time.Now(), time.Now().Add(time.Minute)
				return next, nil
			}}
			var issued []*SecretBuffer
			guard.bundle = func(ctx context.Context) (*SecretBuffer, error) {
				if kind == "unchanged_error" {
					return nil, errors.New("transient storage error")
				}
				if kind == "always_changed" || guard.calls == 1 {
					if _, err := manager.Renew(ctx); err != nil {
						return nil, err
					}
					// A stale successful response must be destroyed, too.
					copy := mustSecretBuffer(t, guard.capability.data)
					issued = append(issued, copy)
					return copy, nil
				}
				if err := manager.WithCurrentEnvelope(func(envelope *SessionEnvelope, _ *SecretBuffer) error {
					wire.SessionID, wire.SessionGeneration = envelope.SessionID, envelope.Generation
					return nil
				}); err != nil {
					return nil, err
				}
				payload, err := json.Marshal(wire)
				if err != nil {
					return nil, err
				}
				copy := mustSecretBuffer(t, payload)
				issued = append(issued, copy)
				return copy, nil
			}
			got, err := state.prepareRegisteredBundle()
			if kind == "changed_once" {
				if err != nil || got == nil || guard.calls != 2 {
					t.Fatalf("successor retry failed: calls=%d err=%v", guard.calls, err)
				}
				if err := got.Close(); err != nil {
					t.Fatal(err)
				}
			} else {
				want := 1
				if kind == "always_changed" {
					want = 3
				}
				if err == nil || got != nil || guard.calls != want {
					t.Fatalf("retry bound changed: calls=%d err=%v", guard.calls, err)
				}
			}
			for _, secret := range issued {
				if !secret.closed {
					t.Fatal("discarded issuance secret survived")
				}
			}
		})
	}
}
