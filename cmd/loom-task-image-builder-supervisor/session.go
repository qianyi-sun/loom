package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"runtime"
	"sync"
	"time"
)

type SessionEnvelope struct {
	Secret                     *SecretBuffer
	GrantID                    string
	SessionID                  string
	Generation                 int
	AttestationGeneration      int
	AttestationSHA256          string
	IssuedAt                   time.Time
	ExpiresAt                  time.Time
	SessionPublicBindingSHA256 string
}

type sessionRenewer interface {
	Renew(context.Context, string, string, *SecretBuffer) (*SessionEnvelope, error)
}

type SessionManager struct {
	mu      sync.Mutex
	grantID string
	client  sessionRenewer
	current *SessionEnvelope
}

func NewSessionManager(grantID string, current *SessionEnvelope, client sessionRenewer) *SessionManager {
	return &SessionManager{
		grantID: grantID,
		client:  client,
		current: current,
	}
}

func (m *SessionManager) WithCurrent(fn func(*SecretBuffer) error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.current == nil || m.current.Secret == nil || m.current.Secret.closed {
		return errors.New("current session unavailable")
	}
	return fn(m.current.Secret)
}

func (m *SessionManager) Renew(ctx context.Context) (*SessionEnvelope, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.current == nil || m.current.Secret == nil || m.current.Secret.closed {
		return nil, errors.New("current session unavailable")
	}
	operationID, err := newUUID()
	if err != nil {
		return nil, err
	}
	next, err := m.client.Renew(ctx, m.grantID, operationID, m.current.Secret)
	if err != nil {
		return nil, err
	}
	if next == nil || next.Secret == nil {
		return nil, errors.New("renewed session missing")
	}
	if next.GrantID != m.grantID || next.Generation != m.current.Generation+1 || !next.ExpiresAt.After(next.IssuedAt) {
		next.Secret.Close()
		return nil, errors.New("renewed session invalid")
	}
	old := m.current
	m.current = next
	old.Secret.Close()
	return next, nil
}

func (m *SessionManager) Generation() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.current == nil {
		return 0
	}
	return m.current.Generation
}

func (m *SessionManager) ExpiresAt() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.current == nil {
		return time.Time{}
	}
	return m.current.ExpiresAt
}

func parseSessionEnvelope(buffer *SecretBuffer) (*SessionEnvelope, error) {
	if buffer == nil || buffer.closed {
		return nil, errors.New("session buffer unavailable")
	}
	var session struct {
		SchemaVersion         int             `json:"schema_version"`
		GrantID               string          `json:"grant_id"`
		SessionID             string          `json:"session_id"`
		Purpose               string          `json:"purpose"`
		ShadowCampaignID      *string         `json:"shadow_campaign_id"`
		PoolID                string          `json:"pool_id"`
		CPUArch               string          `json:"cpu_arch"`
		SessionToken          json.RawMessage `json:"session_token"`
		Generation            int             `json:"generation"`
		AttestationGeneration int             `json:"attestation_generation"`
		AttestationSHA256     string          `json:"attestation_sha256"`
		IssuedAt              string          `json:"issued_at"`
		ExpiresAt             string          `json:"expires_at"`
	}
	if err := decodeStrictJSON(buffer.data, &session); err != nil {
		return nil, err
	}
	defer zeroBytes(session.SessionToken)
	if session.SchemaVersion != 2 || !isCanonicalNonZeroUUID(session.GrantID) || !isCanonicalNonZeroUUID(session.SessionID) || session.CPUArch != runtime.GOARCH || session.Generation <= 0 || session.AttestationGeneration <= 0 || !isDigest(session.AttestationSHA256) || !isNonEmptyJSONStringLiteral(session.SessionToken) {
		return nil, errors.New("session payload invalid")
	}
	issuedAt, err := time.Parse(time.RFC3339, session.IssuedAt)
	if err != nil {
		return nil, err
	}
	expiresAt, err := time.Parse(time.RFC3339, session.ExpiresAt)
	if err != nil {
		return nil, err
	}
	if !expiresAt.After(issuedAt) {
		return nil, errors.New("session expiry invalid")
	}
	return &SessionEnvelope{
		Secret:                buffer,
		GrantID:               session.GrantID,
		SessionID:             session.SessionID,
		Generation:            session.Generation,
		AttestationGeneration: session.AttestationGeneration,
		AttestationSHA256:     session.AttestationSHA256,
		IssuedAt:              issuedAt,
		ExpiresAt:             expiresAt,
	}, nil
}

func newUUID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	raw[6] = (raw[6] & 0x0f) | 0x40
	raw[8] = (raw[8] & 0x3f) | 0x80
	value := make([]byte, 36)
	hex.Encode(value[0:8], raw[0:4])
	value[8] = '-'
	hex.Encode(value[9:13], raw[4:6])
	value[13] = '-'
	hex.Encode(value[14:18], raw[6:8])
	value[18] = '-'
	hex.Encode(value[19:23], raw[8:10])
	value[23] = '-'
	hex.Encode(value[24:36], raw[10:16])
	return string(value), nil
}

func (l *LeaseResponse) UnmarshalJSON(payload []byte) error {
	var wire struct {
		Schema                    string  `json:"schema"`
		Operation                 string  `json:"operation"`
		ResponseID                string  `json:"response_id"`
		GrantID                   string  `json:"grant_id"`
		OperationID               string  `json:"operation_id"`
		MaterializationID         string  `json:"materialization_id"`
		AttemptID                 string  `json:"attempt_id"`
		LeaseEpoch                int     `json:"lease_epoch"`
		State                     string  `json:"state"`
		DeterministicFailureCount int     `json:"deterministic_failure_count"`
		LeaseExpiresAt            *string `json:"lease_expires_at"`
	}
	if err := decodeStrictJSON(payload, &wire); err != nil {
		return err
	}
	if wire.Schema != localSchema {
		return errors.New("lease response schema invalid")
	}
	var expiry *time.Time
	if wire.LeaseExpiresAt != nil {
		value, err := time.Parse(time.RFC3339, *wire.LeaseExpiresAt)
		if err != nil {
			return err
		}
		expiry = &value
	}
	*l = LeaseResponse{
		Operation:                 wire.Operation,
		ResponseID:                wire.ResponseID,
		GrantID:                   wire.GrantID,
		OperationID:               wire.OperationID,
		MaterializationID:         wire.MaterializationID,
		AttemptID:                 wire.AttemptID,
		LeaseEpoch:                wire.LeaseEpoch,
		State:                     wire.State,
		DeterministicFailureCount: wire.DeterministicFailureCount,
		LeaseExpiresAt:            expiry,
	}
	return nil
}

func isNonEmptyJSONStringLiteral(value []byte) bool {
	return len(value) > 2 && value[0] == '"' && value[len(value)-1] == '"'
}
