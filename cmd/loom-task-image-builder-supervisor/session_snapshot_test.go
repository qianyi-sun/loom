package main

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestSessionSnapshotAllowsRenewalAndOwnsSecretUntilCallbackJoins(t *testing.T) {
	current := mustSessionEnvelope(t, 1, "snapshot-original")
	next := mustSessionEnvelope(t, 2, "snapshot-successor")
	manager := NewSessionManager(current.GrantID, current, &stubSessionClient{renew: func(context.Context, string, string, *SecretBuffer) (*SessionEnvelope, error) { return next, nil }})
	defer manager.Close()
	entered, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	var copied *SecretBuffer
	go func() {
		done <- manager.WithSnapshot(func(envelope *SessionEnvelope, secret *SecretBuffer) error {
			copied = secret
			if envelope == current || secret == current.Secret || envelope.Secret != secret || envelope.Generation != 1 {
				t.Error("snapshot aliases current authority")
			}
			close(entered)
			<-release
			if !strings.Contains(string(secret.data), "snapshot-original") {
				t.Error("renewal destroyed active owned snapshot")
			}
			return nil
		})
	}()
	<-entered
	renewed := make(chan error, 1)
	go func() { _, err := manager.Renew(context.Background()); renewed <- err }()
	select {
	case err := <-renewed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		close(release)
		<-done
		t.Fatal("snapshot held session mutex during I/O")
	}
	if err := manager.WithSnapshot(func(*SessionEnvelope, *SecretBuffer) error { t.Error("unbounded concurrent snapshots"); return nil }); err == nil {
		t.Error("second active snapshot accepted")
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if !copied.closed || strings.Trim(string(copied.data), "\x00") != "" {
		t.Fatal("returned snapshot secret not destroyed")
	}
	if err := manager.WithSnapshot(func(envelope *SessionEnvelope, secret *SecretBuffer) error {
		if envelope.Generation != 2 || !strings.Contains(string(secret.data), "snapshot-successor") {
			t.Error("later snapshot missed successor")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestSessionSnapshotRefusesClosedOrOversizedSecret(t *testing.T) {
	for _, kind := range []string{"closed", "oversized"} {
		t.Run(kind, func(t *testing.T) {
			current := mustSessionEnvelope(t, 1, "snapshot-original")
			manager := NewSessionManager(current.GrantID, current, &stubSessionClient{})
			defer manager.Close()
			if kind == "closed" {
				manager.Close()
			} else {
				current.Secret.Close()
				current.Secret = &SecretBuffer{data: make([]byte, maxSecretBytes+1)}
			}
			if err := manager.WithSnapshot(func(*SessionEnvelope, *SecretBuffer) error { t.Error("invalid snapshot lent"); return nil }); err == nil {
				t.Fatal("invalid snapshot accepted")
			}
		})
	}
}
