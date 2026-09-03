package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
	"unsafe"
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

func TestSessionEnvelopeRejectsTrailingJSONGarbage(t *testing.T) {
	fd := createMemfdFixture(t, "session-envelope", []byte(`{"schema_version":2,"grant_id":"11111111-1111-4111-8111-111111111111","session_id":"33333333-3333-4333-8333-333333333333","purpose":"production","shadow_campaign_id":null,"pool_id":"staging-gb10-task-image","cpu_arch":"`+runtimeSessionArch()+`","session_token":"sentinel-secret-text","generation":1,"attestation_generation":1,"attestation_sha256":"`+strings.Repeat("a", 64)+`","issued_at":"2026-09-03T00:00:00Z","expires_at":"2026-09-03T00:10:00Z"}{}`), requiredMemfdSeals, true)
	buffer, err := NewSecretBuffer(fd, 64*1024)
	if err != nil {
		t.Fatalf("NewSecretBuffer() error = %v", err)
	}
	defer buffer.Close()

	if _, err := parseSessionEnvelope(buffer); err == nil {
		t.Fatal("parseSessionEnvelope() succeeded, want trailing-document error")
	}
}

func TestSessionEnvelopeScannerKeepsTokenSliceInsideLockedBuffer(t *testing.T) {
	buffer := mustSecretBuffer(t, []byte(`{"schema_version":2,"grant_id":"11111111-1111-4111-8111-111111111111","session_id":"33333333-3333-4333-8333-333333333333","purpose":"production","shadow_campaign_id":null,"pool_id":"staging-gb10-task-image","cpu_arch":"`+runtimeSessionArch()+`","session_token":"sentinel-secret-text","generation":1,"attestation_generation":1,"attestation_sha256":"`+strings.Repeat("a", 64)+`","issued_at":"2026-09-03T00:00:00Z","expires_at":"2026-09-03T00:10:00Z"}`))
	defer buffer.Close()

	fields, err := parseSessionEnvelopeFields(buffer.data)
	if err != nil {
		t.Fatalf("parseSessionEnvelopeFields() error = %v", err)
	}

	base := uintptr(unsafe.Pointer(unsafe.SliceData(buffer.data)))
	limit := base + uintptr(len(buffer.data))
	tokenPtr := reflect.ValueOf(fields.sessionToken).Pointer()
	if tokenPtr < base || tokenPtr >= limit {
		t.Fatalf("session token slice ptr %#x outside locked buffer [%#x, %#x)", tokenPtr, base, limit)
	}
	if got := string(fields.sessionToken); got != `"sentinel-secret-text"` {
		t.Fatalf("sessionToken = %q", got)
	}
}

func TestSessionEnvelopeRejectsUnknownDuplicateOrMissingFields(t *testing.T) {
	for _, payload := range [][]byte{
		[]byte(`{"schema_version":2,"grant_id":"11111111-1111-4111-8111-111111111111","session_id":"33333333-3333-4333-8333-333333333333","purpose":"production","shadow_campaign_id":null,"pool_id":"staging-gb10-task-image","cpu_arch":"` + runtimeSessionArch() + `","generation":1,"attestation_generation":1,"attestation_sha256":"` + strings.Repeat("a", 64) + `","issued_at":"2026-09-03T00:00:00Z","expires_at":"2026-09-03T00:10:00Z"}`),
		[]byte(`{"schema_version":2,"grant_id":"11111111-1111-4111-8111-111111111111","session_id":"33333333-3333-4333-8333-333333333333","purpose":"production","purpose":"production","shadow_campaign_id":null,"pool_id":"staging-gb10-task-image","cpu_arch":"` + runtimeSessionArch() + `","session_token":"sentinel-secret-text","generation":1,"attestation_generation":1,"attestation_sha256":"` + strings.Repeat("a", 64) + `","issued_at":"2026-09-03T00:00:00Z","expires_at":"2026-09-03T00:10:00Z"}`),
		[]byte(`{"schema_version":2,"grant_id":"11111111-1111-4111-8111-111111111111","session_id":"33333333-3333-4333-8333-333333333333","purpose":"production","shadow_campaign_id":null,"pool_id":"staging-gb10-task-image","cpu_arch":"` + runtimeSessionArch() + `","session_token":"sentinel-secret-text","generation":1,"attestation_generation":1,"attestation_sha256":"` + strings.Repeat("a", 64) + `","issued_at":"2026-09-03T00:00:00Z","expires_at":"2026-09-03T00:10:00Z","extra":true}`),
	} {
		buffer := mustSecretBuffer(t, payload)
		if _, err := parseSessionEnvelope(buffer); err == nil {
			t.Fatalf("parseSessionEnvelope(%s) succeeded, want error", payload)
		}
		buffer.Close()
	}
}

func TestLeaseResponseUnmarshalRejectsUnknownFieldsAndWrongSchema(t *testing.T) {
	for _, payload := range [][]byte{
		[]byte(`{"schema":"wrong","operation":"start","response_id":"22222222-2222-4222-8222-222222222222","grant_id":"11111111-1111-4111-8111-111111111111","operation_id":"33333333-3333-4333-8333-333333333333","materialization_id":"44444444-4444-4444-8444-444444444444","attempt_id":"55555555-5555-4555-8555-555555555555","lease_epoch":7,"state":"active","deterministic_failure_count":0}`),
		[]byte(`{"schema":"` + localSchema + `","operation":"start","response_id":"22222222-2222-4222-8222-222222222222","grant_id":"11111111-1111-4111-8111-111111111111","operation_id":"33333333-3333-4333-8333-333333333333","materialization_id":"44444444-4444-4444-8444-444444444444","attempt_id":"55555555-5555-4555-8555-555555555555","lease_epoch":7,"state":"active","deterministic_failure_count":0,"unexpected":true}`),
	} {
		var response LeaseResponse
		if err := json.Unmarshal(payload, &response); err == nil {
			t.Fatalf("json.Unmarshal(%s) succeeded, want error", payload)
		}
	}
}

func mustSecretBuffer(t *testing.T, payload []byte) *SecretBuffer {
	t.Helper()
	fd := createMemfdFixture(t, "session-envelope", payload, requiredMemfdSeals, true)
	buffer, err := NewSecretBuffer(fd, 64*1024)
	if err != nil {
		t.Fatalf("NewSecretBuffer() error = %v", err)
	}
	return buffer
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
