package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"runtime"
	"strconv"
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
	session, err := parseSessionEnvelopeFields(buffer.data)
	if err != nil {
		return nil, err
	}
	if session.schemaVersion != 2 || !isCanonicalNonZeroUUID(session.grantID) || !isCanonicalNonZeroUUID(session.sessionID) || session.cpuArch != runtime.GOARCH || session.generation <= 0 || session.attestationGeneration <= 0 || !isDigest(session.attestationSHA256) || !isNonEmptyJSONStringLiteral(session.sessionToken) {
		return nil, errors.New("session payload invalid")
	}
	issuedAt, err := time.Parse(time.RFC3339, session.issuedAt)
	if err != nil {
		return nil, err
	}
	expiresAt, err := time.Parse(time.RFC3339, session.expiresAt)
	if err != nil {
		return nil, err
	}
	if !expiresAt.After(issuedAt) {
		return nil, errors.New("session expiry invalid")
	}
	return &SessionEnvelope{
		Secret:                buffer,
		GrantID:               session.grantID,
		SessionID:             session.sessionID,
		Generation:            session.generation,
		AttestationGeneration: session.attestationGeneration,
		AttestationSHA256:     session.attestationSHA256,
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

type sessionEnvelopeFields struct {
	schemaVersion         int
	grantID               string
	sessionID             string
	purpose               string
	shadowCampaignID      *string
	poolID                string
	cpuArch               string
	sessionToken          []byte
	generation            int
	attestationGeneration int
	attestationSHA256     string
	issuedAt              string
	expiresAt             string
}

func parseSessionEnvelopeFields(payload []byte) (sessionEnvelopeFields, error) {
	if !json.Valid(payload) {
		return sessionEnvelopeFields{}, errors.New("session payload invalid")
	}
	values, err := scanJSONObjectFields(payload)
	if err != nil {
		return sessionEnvelopeFields{}, err
	}
	required := []string{
		"schema_version",
		"grant_id",
		"session_id",
		"purpose",
		"shadow_campaign_id",
		"pool_id",
		"cpu_arch",
		"session_token",
		"generation",
		"attestation_generation",
		"attestation_sha256",
		"issued_at",
		"expires_at",
	}
	if len(values) != len(required) {
		return sessionEnvelopeFields{}, errors.New("session payload invalid")
	}
	for _, key := range required {
		if _, ok := values[key]; !ok {
			return sessionEnvelopeFields{}, errors.New("session payload invalid")
		}
	}

	schemaVersion, err := decodeJSONInt(values["schema_version"])
	if err != nil {
		return sessionEnvelopeFields{}, errors.New("session payload invalid")
	}
	grantID, err := decodeJSONString(values["grant_id"])
	if err != nil {
		return sessionEnvelopeFields{}, errors.New("session payload invalid")
	}
	sessionID, err := decodeJSONString(values["session_id"])
	if err != nil {
		return sessionEnvelopeFields{}, errors.New("session payload invalid")
	}
	purpose, err := decodeJSONString(values["purpose"])
	if err != nil {
		return sessionEnvelopeFields{}, errors.New("session payload invalid")
	}
	shadowCampaignID, err := decodeOptionalJSONString(values["shadow_campaign_id"])
	if err != nil {
		return sessionEnvelopeFields{}, errors.New("session payload invalid")
	}
	poolID, err := decodeJSONString(values["pool_id"])
	if err != nil {
		return sessionEnvelopeFields{}, errors.New("session payload invalid")
	}
	cpuArch, err := decodeJSONString(values["cpu_arch"])
	if err != nil {
		return sessionEnvelopeFields{}, errors.New("session payload invalid")
	}
	generation, err := decodeJSONInt(values["generation"])
	if err != nil {
		return sessionEnvelopeFields{}, errors.New("session payload invalid")
	}
	attestationGeneration, err := decodeJSONInt(values["attestation_generation"])
	if err != nil {
		return sessionEnvelopeFields{}, errors.New("session payload invalid")
	}
	attestationSHA256, err := decodeJSONString(values["attestation_sha256"])
	if err != nil {
		return sessionEnvelopeFields{}, errors.New("session payload invalid")
	}
	issuedAt, err := decodeJSONString(values["issued_at"])
	if err != nil {
		return sessionEnvelopeFields{}, errors.New("session payload invalid")
	}
	expiresAt, err := decodeJSONString(values["expires_at"])
	if err != nil {
		return sessionEnvelopeFields{}, errors.New("session payload invalid")
	}

	return sessionEnvelopeFields{
		schemaVersion:         schemaVersion,
		grantID:               grantID,
		sessionID:             sessionID,
		purpose:               purpose,
		shadowCampaignID:      shadowCampaignID,
		poolID:                poolID,
		cpuArch:               cpuArch,
		sessionToken:          values["session_token"],
		generation:            generation,
		attestationGeneration: attestationGeneration,
		attestationSHA256:     attestationSHA256,
		issuedAt:              issuedAt,
		expiresAt:             expiresAt,
	}, nil
}

func scanJSONObjectFields(payload []byte) (map[string][]byte, error) {
	position := skipJSONWhitespace(payload, 0)
	if position >= len(payload) || payload[position] != '{' {
		return nil, errors.New("session payload invalid")
	}
	position++
	fields := make(map[string][]byte, 13)
	for {
		position = skipJSONWhitespace(payload, position)
		if position >= len(payload) {
			return nil, errors.New("session payload invalid")
		}
		if payload[position] == '}' {
			position++
			break
		}
		keyStart := position
		keyEnd, err := consumeJSONString(payload, keyStart)
		if err != nil {
			return nil, errors.New("session payload invalid")
		}
		key, err := decodeJSONString(payload[keyStart:keyEnd])
		if err != nil {
			return nil, errors.New("session payload invalid")
		}
		if _, exists := fields[key]; exists {
			return nil, errors.New("session payload invalid")
		}
		position = skipJSONWhitespace(payload, keyEnd)
		if position >= len(payload) || payload[position] != ':' {
			return nil, errors.New("session payload invalid")
		}
		position++
		position = skipJSONWhitespace(payload, position)
		valueStart := position
		valueEnd, err := consumeJSONValue(payload, valueStart)
		if err != nil {
			return nil, errors.New("session payload invalid")
		}
		fields[key] = payload[valueStart:valueEnd]
		position = skipJSONWhitespace(payload, valueEnd)
		if position >= len(payload) {
			return nil, errors.New("session payload invalid")
		}
		switch payload[position] {
		case ',':
			position++
		case '}':
			position++
			goto done
		default:
			return nil, errors.New("session payload invalid")
		}
	}
done:
	position = skipJSONWhitespace(payload, position)
	if position != len(payload) {
		return nil, errors.New("session payload invalid")
	}
	return fields, nil
}

func consumeJSONValue(payload []byte, position int) (int, error) {
	if position >= len(payload) {
		return 0, errors.New("json value invalid")
	}
	switch payload[position] {
	case '"':
		return consumeJSONString(payload, position)
	case '{':
		return consumeDelimitedJSON(payload, position, '{', '}')
	case '[':
		return consumeDelimitedJSON(payload, position, '[', ']')
	case 't':
		return consumeJSONLiteral(payload, position, "true")
	case 'f':
		return consumeJSONLiteral(payload, position, "false")
	case 'n':
		return consumeJSONLiteral(payload, position, "null")
	default:
		return consumeJSONNumber(payload, position)
	}
}

func consumeDelimitedJSON(payload []byte, position int, open byte, close byte) (int, error) {
	depth := 0
	index := position
	for index < len(payload) {
		switch payload[index] {
		case '"':
			next, err := consumeJSONString(payload, index)
			if err != nil {
				return 0, err
			}
			index = next
			continue
		case open:
			depth++
		case close:
			depth--
			if depth == 0 {
				return index + 1, nil
			}
		}
		index++
	}
	return 0, errors.New("json delimiter invalid")
}

func consumeJSONString(payload []byte, position int) (int, error) {
	if position >= len(payload) || payload[position] != '"' {
		return 0, errors.New("json string invalid")
	}
	for index := position + 1; index < len(payload); index++ {
		switch payload[index] {
		case '\\':
			index++
			if index >= len(payload) {
				return 0, errors.New("json escape invalid")
			}
		case '"':
			return index + 1, nil
		}
	}
	return 0, errors.New("json string invalid")
}

func consumeJSONLiteral(payload []byte, position int, literal string) (int, error) {
	if !bytes.HasPrefix(payload[position:], []byte(literal)) {
		return 0, errors.New("json literal invalid")
	}
	return position + len(literal), nil
}

func consumeJSONNumber(payload []byte, position int) (int, error) {
	index := position
	if payload[index] == '-' {
		index++
	}
	if index >= len(payload) {
		return 0, errors.New("json number invalid")
	}
	if payload[index] == '0' {
		index++
	} else {
		if payload[index] < '1' || payload[index] > '9' {
			return 0, errors.New("json number invalid")
		}
		for index < len(payload) && payload[index] >= '0' && payload[index] <= '9' {
			index++
		}
	}
	if index < len(payload) && payload[index] == '.' {
		index++
		if index >= len(payload) || payload[index] < '0' || payload[index] > '9' {
			return 0, errors.New("json number invalid")
		}
		for index < len(payload) && payload[index] >= '0' && payload[index] <= '9' {
			index++
		}
	}
	if index < len(payload) && (payload[index] == 'e' || payload[index] == 'E') {
		index++
		if index < len(payload) && (payload[index] == '+' || payload[index] == '-') {
			index++
		}
		if index >= len(payload) || payload[index] < '0' || payload[index] > '9' {
			return 0, errors.New("json number invalid")
		}
		for index < len(payload) && payload[index] >= '0' && payload[index] <= '9' {
			index++
		}
	}
	return index, nil
}

func skipJSONWhitespace(payload []byte, position int) int {
	for position < len(payload) {
		switch payload[position] {
		case ' ', '\t', '\r', '\n':
			position++
		default:
			return position
		}
	}
	return position
}

func decodeJSONString(payload []byte) (string, error) {
	var value string
	if err := json.Unmarshal(payload, &value); err != nil {
		return "", err
	}
	return value, nil
}

func decodeOptionalJSONString(payload []byte) (*string, error) {
	if bytes.Equal(payload, []byte("null")) {
		return nil, nil
	}
	value, err := decodeJSONString(payload)
	if err != nil {
		return nil, err
	}
	return &value, nil
}

func decodeJSONInt(payload []byte) (int, error) {
	value, err := strconv.ParseInt(string(payload), 10, 64)
	if err != nil {
		return 0, err
	}
	return int(value), nil
}
