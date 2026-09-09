package main

import (
	"context"
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
	sessionID string
	calls int
}

func (g *registeredPrebuildGuard) Bundle(_ context.Context, _, _, _, _ string, _ int, secret *SecretBuffer) (*SecretBuffer, error) {
	g.calls++
	if !strings.Contains(string(secret.data), "current-issuance") { panic("bundle did not use owned current session") }
	return g.capability, nil
}

func TestRegisteredPreparationComposesCurrentSessionWithRealTLSDownloader(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for _, file := range registeredManifestVector() {
			if strings.HasSuffix(r.URL.Path, "/"+file.RelativePath) { _, _ = w.Write(make([]byte, file.SizeBytes)); return }
		}
		http.NotFound(w, r)
	}))
	defer server.Close()
	secret, plan, session, trust := registeredDownloadFixture(t, server)
	current := mustSessionEnvelope(t, session.Generation, "current-issuance")
	current.GrantID, current.SessionID, current.Generation = plan.GrantID, session.SessionID, session.Generation
	current.IssuedAt, current.ExpiresAt = time.Now(), session.ExpiresAt
	manager := NewSessionManager(plan.GrantID, current, &stubSessionClient{})
	defer manager.Close()
	guard := &registeredPrebuildGuard{capability: secret}
	fd := openDirectoryFD(t, t.TempDir())
	defer syscall.Close(fd)
	expiry := time.Now().Add(time.Minute)
	claim := &buildClaim{MaterializationID: plan.MaterializationID, AttemptID: testAttemptID, LeaseEpoch: 1,
		RegisteredBundle: &plan, LeaseExpiresAtPtr: &expiry,
		Plan: BuildPlan{BuilderID: "rootless:original-claim-provenance", BuildTimeout: time.Minute}}
	state := &orchestratorState{ctx: context.Background(), clock: realClock{}, sessionManager: manager, claimData: claim,
		o: &Orchestrator{GrantID: plan.GrantID, Guard: guard, Config: Config{Bundle: &trust}},
		caps: &AllocationCapabilities{JobDirectoryFD: fd}}
	got, err := state.prepareRegisteredBundle()
	if err != nil { t.Fatal(err) }
	defer got.Close()
	if guard.calls != 1 || got.ManifestSHA256 != plan.ManifestSHA256 || !secret.closed { t.Fatal("preparation lost content or secret ownership") }
	if manager.Generation() != session.Generation { t.Fatal("preparation rewrote current session") }
	if claim.Plan.BuilderID != "rootless:original-claim-provenance" { t.Fatal("current issuance rewrote original builder identity") }
	contextFD, err := got.DupDirectoryFD()
	if err != nil { t.Fatal(err) }
	defer syscall.Close(contextFD)
	for _, file := range registeredManifestVector() {
		actual, err := HashFileAt(contextFD, file.RelativePath)
		if err != nil || actual.SHA256 != file.SHA256 { t.Fatal("preparation did not verify downloaded content") }
	}
}
