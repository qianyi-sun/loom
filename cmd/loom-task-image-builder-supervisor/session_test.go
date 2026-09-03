package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestSessionManagerWithCurrentLendsSecretOnlyWithinCallback(t *testing.T) {
	current := mustSessionEnvelope(t, 1, "sentinel-current")
	manager := NewSessionManager("11111111-1111-4111-8111-111111111111", current, &stubSessionClient{})

	var seen string
	err := manager.WithCurrent(func(buffer *SecretBuffer) error {
		seen = string(buffer.data)
		return nil
	})
	if err != nil {
		t.Fatalf("WithCurrent() error = %v", err)
	}
	if !strings.Contains(seen, "sentinel-current") {
		t.Fatalf("callback did not observe current session: %q", seen)
	}
	if manager.Generation() != 1 {
		t.Fatalf("Generation() = %d, want 1", manager.Generation())
	}
	if !manager.ExpiresAt().Equal(time.Date(2026, 9, 3, 0, 10, 0, 0, time.UTC)) {
		t.Fatalf("ExpiresAt() = %s", manager.ExpiresAt())
	}
}

func TestSessionManagerRenewAtomicallySwapsAndDestroysSupersededToken(t *testing.T) {
	current := mustSessionEnvelope(t, 1, "sentinel-current")
	next := mustSessionEnvelope(t, 2, "sentinel-next")
	client := &stubSessionClient{
		renew: func(ctx context.Context, grantID string, operationID string, current *SecretBuffer) (*SessionEnvelope, error) {
			if grantID != "11111111-1111-4111-8111-111111111111" {
				t.Fatalf("grantID = %q", grantID)
			}
			if operationID == "" {
				t.Fatal("operationID is empty")
			}
			if !strings.Contains(string(current.data), "sentinel-current") {
				t.Fatalf("current session = %q", string(current.data))
			}
			return next, nil
		},
	}
	manager := NewSessionManager("11111111-1111-4111-8111-111111111111", current, client)

	oldBytes := current.Secret.data
	renewed, err := manager.Renew(context.Background())
	if err != nil {
		t.Fatalf("Renew() error = %v", err)
	}
	if renewed.Generation != 2 {
		t.Fatalf("renewed.Generation = %d, want 2", renewed.Generation)
	}
	if manager.Generation() != 2 {
		t.Fatalf("Generation() = %d, want 2", manager.Generation())
	}
	for index, value := range oldBytes {
		if value != 0 {
			t.Fatalf("old session byte %d = %d, want zero", index, value)
		}
	}
}

func TestSessionManagerRenewRejectsInvalidNextGenerationWithoutReplacingCurrent(t *testing.T) {
	current := mustSessionEnvelope(t, 2, "sentinel-current")
	invalid := mustSessionEnvelope(t, 2, "sentinel-invalid")
	client := &stubSessionClient{
		renew: func(context.Context, string, string, *SecretBuffer) (*SessionEnvelope, error) {
			return invalid, nil
		},
	}
	manager := NewSessionManager("11111111-1111-4111-8111-111111111111", current, client)

	if _, err := manager.Renew(context.Background()); err == nil {
		t.Fatal("Renew() succeeded, want error")
	}
	if manager.Generation() != 2 {
		t.Fatalf("Generation() = %d, want 2", manager.Generation())
	}
	if !strings.Contains(string(current.Secret.data), "sentinel-current") {
		t.Fatalf("current session changed: %q", string(current.Secret.data))
	}
}

func TestSessionManagerRenewPropagatesErrorsAndKeepsCurrentSession(t *testing.T) {
	current := mustSessionEnvelope(t, 3, "sentinel-current")
	client := &stubSessionClient{
		renew: func(context.Context, string, string, *SecretBuffer) (*SessionEnvelope, error) {
			return nil, errors.New("boom")
		},
	}
	manager := NewSessionManager("11111111-1111-4111-8111-111111111111", current, client)

	if _, err := manager.Renew(context.Background()); err == nil {
		t.Fatal("Renew() succeeded, want error")
	}
	if manager.Generation() != 3 {
		t.Fatalf("Generation() = %d, want 3", manager.Generation())
	}
}

type stubSessionClient struct {
	renew func(context.Context, string, string, *SecretBuffer) (*SessionEnvelope, error)
}

func (s *stubSessionClient) Renew(ctx context.Context, grantID string, operationID string, current *SecretBuffer) (*SessionEnvelope, error) {
	if s.renew == nil {
		return nil, errors.New("renew not configured")
	}
	return s.renew(ctx, grantID, operationID, current)
}

func mustSessionEnvelope(t *testing.T, generation int, token string) *SessionEnvelope {
	t.Helper()
	fd := createMemfdFixture(t, "session-envelope", []byte(fmt.Sprintf(`{"schema_version":2,"grant_id":"11111111-1111-4111-8111-111111111111","session_id":"33333333-3333-4333-8333-333333333333","purpose":"production","shadow_campaign_id":null,"pool_id":"staging-gb10-task-image","cpu_arch":"%s","session_token":"%s","generation":%d,"attestation_generation":%d,"attestation_sha256":"%s","issued_at":"2026-09-03T00:00:00Z","expires_at":"2026-09-03T00:10:00Z"}`, runtimeSessionArch(), token, generation, generation, strings.Repeat("a", 64))), requiredMemfdSeals, true)
	buffer, err := NewSecretBuffer(fd, 64*1024)
	if err != nil {
		t.Fatalf("NewSecretBuffer() error = %v", err)
	}
	session, err := parseSessionEnvelope(buffer)
	if err != nil {
		t.Fatalf("parseSessionEnvelope() error = %v", err)
	}
	return session
}
