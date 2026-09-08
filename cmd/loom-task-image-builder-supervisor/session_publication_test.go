package main

import (
	"context"
	"testing"
)

// Credential refresh can renew through SessionManager without assigning the
// orchestrator's cached envelope. Cleanup must erase the manager-owned successor.
func TestOrchestratorClosesCurrentSessionAfterPublicationSourceRenewal(t *testing.T) {
	initial := mustSessionEnvelope(t, 1, "sentinel-pre-upload")
	successor := mustSessionEnvelope(t, 2, "sentinel-upload-successor")
	defer successor.Secret.Close()
	manager := NewSessionManager(initial.GrantID, initial, &stubSessionClient{
		renew: func(context.Context, string, string, *SecretBuffer) (*SessionEnvelope, error) {
			return successor, nil
		},
	})
	state := &orchestratorState{session: initial, sessionManager: manager}
	var currentBytes []byte
	err := manager.RenewWithCurrent(context.Background(), func(_ *SessionEnvelope, secret *SecretBuffer) error {
		currentBytes = secret.data
		return nil
	})
	if err != nil { t.Fatal(err) }
	state.closeSecrets()
	if !successor.Secret.closed { t.Fatal("current publication session was not closed") }
	for _, value := range currentBytes {
		if value != 0 { t.Fatal("current publication session bytes were not erased") }
	}
	state.closeSecrets()
	if err := manager.WithCurrent(func(*SecretBuffer) error { t.Fatal("closed session lent to caller"); return nil }); err == nil {
		t.Fatal("closed manager still admits session operations")
	}
}
