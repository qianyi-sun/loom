package main

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
	"unsafe"
)

// Break caught: credential parsing copies the bearer token out of the locked
// buffer, omits authority bindings, or fails to zeroize the owned secret.
func TestRegistryCredentialParsesStrictBindingAndTokenAliasesLockedBuffer(t *testing.T) {
	secret := mustSecretBuffer(t, []byte(validRegistryCredentialJSON(registryCredentialMutation{})))
	tokenSpan := append([]byte(nil), []byte("header.payload.signature")...)

	credential, err := ParseRegistryCredential(secret, validRegistryCredentialBinding())
	if err != nil {
		t.Fatalf("ParseRegistryCredential() error = %v", err)
	}
	if got := string(credential.BearerToken); got != string(tokenSpan) {
		t.Fatalf("BearerToken = %q, want %q", got, string(tokenSpan))
	}
	base := uintptr(unsafe.Pointer(unsafe.SliceData(secret.data)))
	limit := base + uintptr(len(secret.data))
	tokenPtr := reflect.ValueOf(credential.BearerToken).Pointer()
	if tokenPtr < base || tokenPtr >= limit {
		t.Fatalf("bearer token ptr %#x outside locked buffer [%#x, %#x)", tokenPtr, base, limit)
	}
	if credential.ID != "77777777-7777-4777-8777-777777777777" || credential.Generation != 1 {
		t.Fatalf("credential identity = %s/%d", credential.ID, credential.Generation)
	}
	credential.Close()
	for index, value := range secret.data {
		if value != 0 {
			t.Fatalf("secret byte %d = %d, want zero after Close", index, value)
		}
	}
}

// Break caught: credential parsing accepts a changed authority binding, malformed
// bearer token, wrong action tuple, invalid predecessor pair, or unsafe lifetime.
func TestRegistryCredentialRejectsBindingMutations(t *testing.T) {
	tests := []struct {
		name   string
		mutate registryCredentialMutation
	}{
		{name: "request id", mutate: registryCredentialMutation{RequestID: "99999999-9999-4999-8999-999999999999"}},
		{name: "grant id", mutate: registryCredentialMutation{GrantID: "99999999-9999-4999-8999-999999999999"}},
		{name: "session id", mutate: registryCredentialMutation{SessionID: "99999999-9999-4999-8999-999999999999"}},
		{name: "attestation sha", mutate: registryCredentialMutation{AttestationSHA256: strings.Repeat("9", 64)}},
		{name: "component", mutate: registryCredentialMutation{Component: "sidecar:cache"}},
		{name: "repository", mutate: registryCredentialMutation{Repository: "loom-task-image-attempts/arm64/44444444-4444-4444-8444-444444444444/sidecar-sha256-" + strings.Repeat("9", 64)}},
		{name: "origin", mutate: registryCredentialMutation{RegistryOrigin: "https://registry.invalid"}},
		{name: "service", mutate: registryCredentialMutation{RegistryService: "registry.invalid"}},
		{name: "issuer", mutate: registryCredentialMutation{RegistryIssuer: "issuer.invalid"}},
		{name: "key id", mutate: registryCredentialMutation{RegistryKeyID: strings.Repeat("Z", 43)}},
		{name: "token grammar", mutate: registryCredentialMutation{BearerToken: "not-a-jwt"}},
		{name: "action order", mutate: registryCredentialMutation{ActionsJSON: `["push","pull"]`}},
		{name: "action content", mutate: registryCredentialMutation{ActionsJSON: `["pull","delete"]`}},
		{name: "generation", mutate: registryCredentialMutation{Generation: 2}},
		{name: "predecessor id without generation", mutate: registryCredentialMutation{PredecessorCredentialIDJSON: `"88888888-8888-4888-8888-888888888888"`}},
		{name: "platform", mutate: registryCredentialMutation{Platform: "linux/amd64"}},
		{name: "issue time", mutate: registryCredentialMutation{IssuedAt: "2026-09-03T12:00:00.123Z"}},
		{name: "expiry interval", mutate: registryCredentialMutation{ExpiresAt: "2026-09-03T12:02:00Z"}},
		{name: "unknown field", mutate: registryCredentialMutation{Extra: `,"unexpected":true`}},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			secret := mustSecretBuffer(t, []byte(validRegistryCredentialJSON(tt.mutate)))
			credential, err := ParseRegistryCredential(secret, validRegistryCredentialBinding())
			if err == nil {
				credential.Close()
				t.Fatal("ParseRegistryCredential() succeeded, want binding rejection")
			}
			secret.Close()
		})
	}
}

// Break caught: renewal requests the successor credential before both session
// renewal and a successful same-attempt heartbeat, or closes the predecessor on
// failure.
func TestPublicationCredentialSourceRenewsAfterHeartbeatAndKeepsPredecessorOnFailure(t *testing.T) {
	for _, tc := range []struct {
		name          string
		heartbeatErr  error
		credentialErr error
		wantErr       bool
		wantClosed    bool
		wantEvents    string
	}{
		{name: "success closes predecessor after successor parse", wantClosed: true, wantEvents: "renew,heartbeat,registry-credential"},
		{name: "heartbeat failure keeps predecessor", heartbeatErr: errors.New("heartbeat failed"), wantErr: true, wantEvents: "renew,heartbeat"},
		{name: "credential failure keeps predecessor", credentialErr: errors.New("credential failed"), wantErr: true, wantEvents: "renew,heartbeat,registry-credential"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			guard := &credentialSourceGuard{heartbeatErr: tc.heartbeatErr, credentialErr: tc.credentialErr}
			current := testSession(1, testNow.Add(10*time.Minute))
			manager := NewSessionManager(testGrantID, current, guard)
			source := NewPublicationCredentialSource(manager, guard, validPublicationAttemptBinding())
			predecessorSecret := &SecretBuffer{data: []byte(validRegistryCredentialJSON(registryCredentialMutation{Generation: 1}))}
			predecessor, err := ParseRegistryCredential(predecessorSecret, validRegistryCredentialBinding())
			if err != nil {
				t.Fatalf("predecessor parse error = %v", err)
			}

			successor, err := source.Next(context.Background(), testBuiltSet(), "task", predecessor)
			if tc.wantErr {
				if err == nil {
					if successor != nil {
						successor.Close()
					}
					t.Fatal("Next() succeeded, want error")
				}
			} else if err != nil {
				t.Fatalf("Next() error = %v", err)
			}
			if got := strings.Join(guard.events, ","); got != tc.wantEvents {
				t.Fatalf("events = %s, want %s", got, tc.wantEvents)
			}
			if predecessorSecret.closed != tc.wantClosed {
				t.Fatalf("predecessor closed = %v, want %v", predecessorSecret.closed, tc.wantClosed)
			}
			if successor != nil {
				successor.Close()
			}
			if !predecessorSecret.closed {
				predecessor.Close()
			}
			if current.Secret != nil && !current.Secret.closed {
				current.Secret.Close()
			}
		})
	}
}

// Break caught: generation-one acquisition performs a renewal heartbeat or sends
// a non-null predecessor pair.
func TestPublicationCredentialSourceGetsGenerationOneDirectly(t *testing.T) {
	guard := &credentialSourceGuard{}
	manager := NewSessionManager(testGrantID, testSession(1, testNow.Add(10*time.Minute)), guard)
	source := NewPublicationCredentialSource(manager, guard, validPublicationAttemptBinding())

	credential, err := source.Next(context.Background(), testBuiltSet(), "task", nil)
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	defer credential.Close()
	if credential.Generation != 1 {
		t.Fatalf("Generation = %d, want 1", credential.Generation)
	}
	if got := strings.Join(guard.events, ","); got != "registry-credential" {
		t.Fatalf("events = %s, want registry-credential only", got)
	}
	if !guard.sawNullPredecessor {
		t.Fatal("registry credential request did not use empty/zero generation-one predecessor")
	}
}

// Break caught: candidate recording derives mutable authority from the response
// or accepts acknowledgement drift in attempt number/builder/platform evidence.
func TestPublicationCredentialSourceRecordValidatesCandidateBinding(t *testing.T) {
	guard := &credentialSourceGuard{}
	source := NewPublicationCredentialSource(NewSessionManager(testGrantID, testSession(2, testNow.Add(10*time.Minute)), guard), guard, validPublicationAttemptBinding())
	credential, err := ParseRegistryCredential(mustSecretBuffer(t, []byte(validRegistryCredentialJSON(registryCredentialMutation{Generation: 1}))), validRegistryCredentialBinding())
	if err != nil {
		t.Fatalf("credential parse error = %v", err)
	}
	defer credential.Close()

	ack, err := source.Record(context.Background(), testBuiltSet(), credential, BuiltComponent{
		Name: "task",
		Output: OCIOutput{
			TopLevelDigest: "sha256:" + strings.Repeat("a", 64),
			FileSHA256:     strings.Repeat("b", 64),
			SizeBytes:      5678,
			OS:             "linux",
			Architecture:   "arm64",
		},
	})
	if err != nil {
		t.Fatalf("Record() error = %v", err)
	}
	if ack == nil || ack.AttemptNumber != 11 || ack.BuilderID != "rootless:22222222222242228222222222222222" {
		t.Fatalf("ack = %#v, want frozen attempt binding", ack)
	}
	if got := strings.Join(guard.events, ","); got != "publication-candidate" {
		t.Fatalf("events = %s, want publication-candidate", got)
	}
}

type registryCredentialMutation struct {
	CredentialID                string
	RequestID                   string
	GrantID                     string
	SessionID                   string
	SessionGeneration           int
	AttestationGeneration       int
	AttestationSHA256           string
	MaterializationID           string
	AttemptID                   string
	AttemptNumber               int
	LeaseEpoch                  int
	BuilderID                   string
	CPUArch                     string
	Platform                    string
	Component                   string
	Generation                  int
	PredecessorCredentialIDJSON string
	PredecessorGenerationJSON   string
	HeartbeatOperationIDJSON    string
	RegistryOrigin              string
	RegistryService             string
	RegistryIssuer              string
	Repository                  string
	ActionsJSON                 string
	RegistryKeyID               string
	BearerToken                 string
	IssuedAt                    string
	ExpiresAt                   string
	Extra                       string
}

func validRegistryCredentialJSON(m registryCredentialMutation) string {
	value := registryCredentialMutation{
		CredentialID:                "77777777-7777-4777-8777-777777777777",
		RequestID:                   "22222222-2222-4222-8222-222222222222",
		GrantID:                     testGrantID,
		SessionID:                   testSessionID,
		SessionGeneration:           1,
		AttestationGeneration:       1,
		AttestationSHA256:           strings.Repeat("a", 64),
		MaterializationID:           testMaterializationID,
		AttemptID:                   testAttemptID,
		AttemptNumber:               11,
		LeaseEpoch:                  1,
		BuilderID:                   "rootless:22222222222242228222222222222222",
		CPUArch:                     "arm64",
		Platform:                    "linux/arm64",
		Component:                   "task",
		Generation:                  1,
		PredecessorCredentialIDJSON: "null",
		PredecessorGenerationJSON:   "null",
		HeartbeatOperationIDJSON:    "null",
		RegistryOrigin:              "https://registry.example",
		RegistryService:             "registry.example",
		RegistryIssuer:              "loom-task-image",
		Repository:                  "loom-task-image-attempts/arm64/" + testAttemptID + "/task",
		ActionsJSON:                 `["pull","push"]`,
		RegistryKeyID:               strings.Repeat("A", 43),
		BearerToken:                 "header.payload.signature",
		IssuedAt:                    "2026-09-03T12:00:00Z",
		ExpiresAt:                   "2026-09-03T12:00:30Z",
	}
	if m.CredentialID != "" {
		value.CredentialID = m.CredentialID
	}
	if m.RequestID != "" {
		value.RequestID = m.RequestID
	}
	if m.GrantID != "" {
		value.GrantID = m.GrantID
	}
	if m.SessionID != "" {
		value.SessionID = m.SessionID
	}
	if m.SessionGeneration != 0 {
		value.SessionGeneration = m.SessionGeneration
	}
	if m.AttestationGeneration != 0 {
		value.AttestationGeneration = m.AttestationGeneration
	}
	if m.AttestationSHA256 != "" {
		value.AttestationSHA256 = m.AttestationSHA256
	}
	if m.Component != "" {
		value.Component = m.Component
	}
	if m.Repository != "" {
		value.Repository = m.Repository
	}
	if m.RegistryOrigin != "" {
		value.RegistryOrigin = m.RegistryOrigin
	}
	if m.RegistryService != "" {
		value.RegistryService = m.RegistryService
	}
	if m.RegistryIssuer != "" {
		value.RegistryIssuer = m.RegistryIssuer
	}
	if m.RegistryKeyID != "" {
		value.RegistryKeyID = m.RegistryKeyID
	}
	if m.BearerToken != "" {
		value.BearerToken = m.BearerToken
	}
	if m.ActionsJSON != "" {
		value.ActionsJSON = m.ActionsJSON
	}
	if m.Generation != 0 {
		value.Generation = m.Generation
	}
	if m.PredecessorCredentialIDJSON != "" {
		value.PredecessorCredentialIDJSON = m.PredecessorCredentialIDJSON
	}
	if m.PredecessorGenerationJSON != "" {
		value.PredecessorGenerationJSON = m.PredecessorGenerationJSON
	}
	if m.HeartbeatOperationIDJSON != "" {
		value.HeartbeatOperationIDJSON = m.HeartbeatOperationIDJSON
	}
	if m.Platform != "" {
		value.Platform = m.Platform
	}
	if m.IssuedAt != "" {
		value.IssuedAt = m.IssuedAt
	}
	if m.ExpiresAt != "" {
		value.ExpiresAt = m.ExpiresAt
	}
	return fmt.Sprintf(`{"schema_version":1,"credential_id":%q,"request_id":%q,"grant_id":%q,"session_id":%q,"session_generation":%d,"attestation_generation":%d,"attestation_sha256":%q,"materialization_id":%q,"attempt_id":%q,"attempt_number":%d,"lease_epoch":%d,"builder_id":%q,"purpose":"production","shadow_campaign_id":null,"cpu_arch":%q,"platform":%q,"component":%q,"generation":%d,"predecessor_credential_id":%s,"predecessor_generation":%s,"lease_heartbeat_operation_id":%s,"registry_origin":%q,"registry_service":%q,"registry_issuer":%q,"repository":%q,"actions":%s,"registry_key_id":%q,"bearer_token":%q,"issued_at":%q,"expires_at":%q%s}`,
		value.CredentialID, value.RequestID, value.GrantID, value.SessionID, value.SessionGeneration, value.AttestationGeneration, value.AttestationSHA256, value.MaterializationID, value.AttemptID, value.AttemptNumber, value.LeaseEpoch, value.BuilderID, value.CPUArch, value.Platform, value.Component, value.Generation, value.PredecessorCredentialIDJSON, value.PredecessorGenerationJSON, value.HeartbeatOperationIDJSON, value.RegistryOrigin, value.RegistryService, value.RegistryIssuer, value.Repository, value.ActionsJSON, value.RegistryKeyID, value.BearerToken, value.IssuedAt, value.ExpiresAt, m.Extra)
}

func validRegistryCredentialBinding() RegistryCredentialBinding {
	return RegistryCredentialBinding{
		RequestID:             "22222222-2222-4222-8222-222222222222",
		GrantID:               testGrantID,
		SessionID:             testSessionID,
		SessionGeneration:     1,
		AttestationGeneration: 1,
		AttestationSHA256:     strings.Repeat("a", 64),
		MaterializationID:     testMaterializationID,
		AttemptID:             testAttemptID,
		AttemptNumber:         11,
		LeaseEpoch:            1,
		BuilderID:             "rootless:22222222222242228222222222222222",
		CPUArch:               "arm64",
		Platform:              "linux/arm64",
		Component:             "task",
		Generation:            1,
		RegistryOrigin:        "https://registry.example",
		RegistryService:       "registry.example",
		RegistryIssuer:        "loom-task-image",
		RegistryKeyID:         strings.Repeat("A", 43),
		Now:                   testNow,
	}
}

func validPublicationAttemptBinding() PublicationAttemptBinding {
	return PublicationAttemptBinding{
		GrantID:           testGrantID,
		MaterializationID: testMaterializationID,
		AttemptID:         testAttemptID,
		AttemptNumber:     11,
		LeaseEpoch:        1,
		BuilderID:         "rootless:22222222222242228222222222222222",
		CPUArch:           "arm64",
		Platform:          "linux/arm64",
		RegistryOrigin:    "https://registry.example",
		RegistryService:   "registry.example",
		RegistryIssuer:    "loom-task-image",
		RegistryKeyID:     strings.Repeat("A", 43),
	}
}

func testBuiltSet() BuiltComponentSet {
	return BuiltComponentSet{
		GrantID:           testGrantID,
		MaterializationID: testMaterializationID,
		AttemptID:         testAttemptID,
		LeaseEpoch:        1,
		Components:        []BuiltComponent{{Name: "task"}},
	}
}

type credentialSourceGuard struct {
	events             []string
	heartbeatErr       error
	credentialErr      error
	lastHeartbeat      string
	sawNullPredecessor bool
}

func (g *credentialSourceGuard) Renew(context.Context, string, string, *SecretBuffer) (*SessionEnvelope, error) {
	g.events = append(g.events, "renew")
	return testSession(2, testNow.Add(15*time.Minute)), nil
}

func (g *credentialSourceGuard) Heartbeat(ctx context.Context, grantID string, operationID string, materializationID string, attemptID string, leaseEpoch int, current *SecretBuffer) (*LeaseResponse, error) {
	g.events = append(g.events, "heartbeat")
	if g.heartbeatErr != nil {
		return nil, g.heartbeatErr
	}
	g.lastHeartbeat = operationID
	return testLease("heartbeat", operationID, testNow.Add(2*time.Minute)), nil
}

func (g *credentialSourceGuard) RegistryCredential(ctx context.Context, request RegistryCredentialRequest, current *SecretBuffer) (*SecretBuffer, error) {
	g.events = append(g.events, "registry-credential")
	if g.credentialErr != nil {
		return nil, g.credentialErr
	}
	if request.PredecessorCredentialID == "" && request.PredecessorGeneration == 0 {
		g.sawNullPredecessor = true
		return &SecretBuffer{data: []byte(validRegistryCredentialJSON(registryCredentialMutation{
			RequestID: request.OperationID,
		}))}, nil
	}
	if request.PredecessorCredentialID != "77777777-7777-4777-8777-777777777777" || request.PredecessorGeneration != 1 {
		return nil, errors.New("predecessor binding drift")
	}
	return &SecretBuffer{data: []byte(validRegistryCredentialJSON(registryCredentialMutation{
		CredentialID:                "88888888-8888-4888-8888-888888888888",
		RequestID:                   request.OperationID,
		SessionGeneration:           2,
		AttestationGeneration:       2,
		Generation:                  2,
		PredecessorCredentialIDJSON: `"77777777-7777-4777-8777-777777777777"`,
		PredecessorGenerationJSON:   "1",
		HeartbeatOperationIDJSON:    fmt.Sprintf("%q", g.lastHeartbeat),
	}))}, nil
}

func (g *credentialSourceGuard) PublicationCandidate(ctx context.Context, request PublicationCandidateRequest, current *SecretBuffer) (*PublicationCandidateAcknowledgement, error) {
	g.events = append(g.events, "publication-candidate")
	return &PublicationCandidateAcknowledgement{
		CandidateID:             "99999999-9999-4999-8999-999999999999",
		OperationID:             request.OperationID,
		CredentialID:            request.CredentialID,
		CredentialGeneration:    request.CredentialGeneration,
		GrantID:                 request.GrantID,
		SessionID:               request.SessionID,
		SessionGeneration:       request.SessionGeneration,
		MaterializationID:       request.MaterializationID,
		AttemptID:               request.AttemptID,
		AttemptNumber:           request.AttemptNumber,
		LeaseEpoch:              request.LeaseEpoch,
		BuilderID:               request.BuilderID,
		Component:               request.Component,
		ManifestDigest:          request.ManifestDigest,
		ManifestSize:            request.ManifestSize,
		OCIFileSHA256:           request.OCIFileSHA256,
		OCIFileSize:             request.OCIFileSize,
		Platform:                request.Platform,
		RecordedAt:              testNow,
		AuthorityResponseSHA256: strings.Repeat("c", 64),
	}, nil
}
