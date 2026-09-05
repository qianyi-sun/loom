package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"time"
)

const maxRegistryCredentialLifetime = 45 * time.Second

var (
	componentPattern        = regexp.MustCompile(`^(?:task|sidecar:[A-Za-z0-9][A-Za-z0-9_.-]{0,127})$`)
	builderIDPattern        = regexp.MustCompile(`^rootless:[0-9a-f]{32}$`)
	registryIdentityPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_.:-]{0,127}$`)
	registryKeyIDPattern    = regexp.MustCompile(`^[A-Za-z0-9_-]{43}$`)
	bearerTokenPattern      = regexp.MustCompile(`^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$`)
)

type RegistryCredentialBinding struct {
	RequestID                 string
	GrantID                   string
	SessionID                 string
	SessionGeneration         int
	AttestationGeneration     int
	AttestationSHA256         string
	MaterializationID         string
	AttemptID                 string
	AttemptNumber             int
	LeaseEpoch                int
	BuilderID                 string
	CPUArch                   string
	Platform                  string
	Component                 string
	Generation                int
	PredecessorCredentialID   string
	PredecessorGeneration     int
	LeaseHeartbeatOperationID string
	RegistryOrigin            string
	RegistryService           string
	RegistryIssuer            string
	RegistryKeyID             string
	Now                       time.Time
}

type RegistryCredential struct {
	secret                    *SecretBuffer
	ID                        string
	RequestID                 string
	GrantID                   string
	SessionID                 string
	SessionGeneration         int
	AttestationGeneration     int
	AttestationSHA256         string
	MaterializationID         string
	AttemptID                 string
	AttemptNumber             int
	LeaseEpoch                int
	BuilderID                 string
	CPUArch                   string
	Platform                  string
	Component                 string
	Generation                int
	PredecessorCredentialID   string
	PredecessorGeneration     int
	LeaseHeartbeatOperationID string
	Repository                string
	RegistryOrigin            string
	RegistryService           string
	RegistryIssuer            string
	RegistryKeyID             string
	BearerToken               []byte
	IssuedAt                  time.Time
	ExpiresAt                 time.Time
}

func (c *RegistryCredential) Close() {
	if c == nil {
		return
	}
	if c.secret != nil {
		c.secret.Close()
	}
	c.BearerToken = nil
}

func ParseRegistryCredential(secret *SecretBuffer, binding RegistryCredentialBinding) (*RegistryCredential, error) {
	if secret == nil || secret.closed {
		return nil, errors.New("registry credential secret unavailable")
	}
	values, err := scanJSONObjectFields(secret.data)
	if err != nil || len(values) != 31 {
		return nil, errors.New("registry credential JSON invalid")
	}
	required := []string{
		"schema_version", "credential_id", "request_id", "grant_id", "session_id",
		"session_generation", "attestation_generation", "attestation_sha256",
		"materialization_id", "attempt_id", "attempt_number", "lease_epoch",
		"builder_id", "purpose", "shadow_campaign_id", "cpu_arch", "platform",
		"component", "generation", "predecessor_credential_id", "predecessor_generation",
		"lease_heartbeat_operation_id", "registry_origin", "registry_service",
		"registry_issuer", "repository", "actions", "registry_key_id", "bearer_token",
		"issued_at", "expires_at",
	}
	for _, key := range required {
		if _, ok := values[key]; !ok {
			return nil, errors.New("registry credential JSON invalid")
		}
	}
	schemaVersion, err := decodeJSONInt(values["schema_version"])
	if err != nil || schemaVersion != 1 {
		return nil, errors.New("registry credential invalid")
	}
	credentialID, err := decodeRequiredString(values, "credential_id")
	if err != nil {
		return nil, errors.New("registry credential invalid")
	}
	requestID, err := decodeRequiredString(values, "request_id")
	if err != nil {
		return nil, errors.New("registry credential invalid")
	}
	grantID, err := decodeRequiredString(values, "grant_id")
	if err != nil {
		return nil, errors.New("registry credential invalid")
	}
	sessionID, err := decodeRequiredString(values, "session_id")
	if err != nil {
		return nil, errors.New("registry credential invalid")
	}
	sessionGeneration, err := decodeJSONInt(values["session_generation"])
	if err != nil {
		return nil, errors.New("registry credential invalid")
	}
	attestationGeneration, err := decodeJSONInt(values["attestation_generation"])
	if err != nil {
		return nil, errors.New("registry credential invalid")
	}
	attestationSHA256, err := decodeRequiredString(values, "attestation_sha256")
	if err != nil {
		return nil, errors.New("registry credential invalid")
	}
	materializationID, err := decodeRequiredString(values, "materialization_id")
	if err != nil {
		return nil, errors.New("registry credential invalid")
	}
	attemptID, err := decodeRequiredString(values, "attempt_id")
	if err != nil {
		return nil, errors.New("registry credential invalid")
	}
	attemptNumber, err := decodeJSONInt(values["attempt_number"])
	if err != nil {
		return nil, errors.New("registry credential invalid")
	}
	leaseEpoch, err := decodeJSONInt(values["lease_epoch"])
	if err != nil {
		return nil, errors.New("registry credential invalid")
	}
	builderID, err := decodeRequiredString(values, "builder_id")
	if err != nil {
		return nil, errors.New("registry credential invalid")
	}
	purpose, err := decodeRequiredString(values, "purpose")
	if err != nil {
		return nil, errors.New("registry credential invalid")
	}
	cpuArch, err := decodeRequiredString(values, "cpu_arch")
	if err != nil {
		return nil, errors.New("registry credential invalid")
	}
	platform, err := decodeRequiredString(values, "platform")
	if err != nil {
		return nil, errors.New("registry credential invalid")
	}
	component, err := decodeRequiredString(values, "component")
	if err != nil {
		return nil, errors.New("registry credential invalid")
	}
	generation, err := decodeJSONInt(values["generation"])
	if err != nil {
		return nil, errors.New("registry credential invalid")
	}
	predecessorID, predecessorIDSet, err := decodeOptionalCredentialString(values["predecessor_credential_id"])
	if err != nil {
		return nil, errors.New("registry credential invalid")
	}
	predecessorGeneration, predecessorGenerationSet, err := decodeOptionalCredentialInt(values["predecessor_generation"])
	if err != nil {
		return nil, errors.New("registry credential invalid")
	}
	heartbeatID, heartbeatIDSet, err := decodeOptionalCredentialString(values["lease_heartbeat_operation_id"])
	if err != nil {
		return nil, errors.New("registry credential invalid")
	}
	registryOrigin, err := decodeRequiredString(values, "registry_origin")
	if err != nil {
		return nil, errors.New("registry credential invalid")
	}
	registryService, err := decodeRequiredString(values, "registry_service")
	if err != nil {
		return nil, errors.New("registry credential invalid")
	}
	registryIssuer, err := decodeRequiredString(values, "registry_issuer")
	if err != nil {
		return nil, errors.New("registry credential invalid")
	}
	repository, err := decodeRequiredString(values, "repository")
	if err != nil {
		return nil, errors.New("registry credential invalid")
	}
	registryKeyID, err := decodeRequiredString(values, "registry_key_id")
	if err != nil {
		return nil, errors.New("registry credential invalid")
	}
	issuedRaw, err := decodeRequiredString(values, "issued_at")
	if err != nil {
		return nil, errors.New("registry credential invalid")
	}
	expiresRaw, err := decodeRequiredString(values, "expires_at")
	if err != nil {
		return nil, errors.New("registry credential invalid")
	}
	issuedAt, err := time.Parse(time.RFC3339, issuedRaw)
	if err != nil {
		return nil, errors.New("registry credential time invalid")
	}
	expiresAt, err := time.Parse(time.RFC3339, expiresRaw)
	if err != nil {
		return nil, errors.New("registry credential time invalid")
	}
	var actions []string
	if err := json.Unmarshal(values["actions"], &actions); err != nil {
		return nil, errors.New("registry credential actions invalid")
	}
	tokenLiteral := values["bearer_token"]
	token, err := decodeJSONString(tokenLiteral)
	if err != nil || !isNonEmptyJSONStringLiteral(tokenLiteral) {
		return nil, errors.New("registry credential token invalid")
	}
	tokenBytes := tokenLiteral[1 : len(tokenLiteral)-1]
	expectedRepository, err := publicationRepository(binding.CPUArch, binding.AttemptID, binding.Component)
	if err != nil {
		return nil, err
	}
	if credentialID == predecessorID ||
		requestID != binding.RequestID ||
		grantID != binding.GrantID ||
		sessionID != binding.SessionID ||
		sessionGeneration != binding.SessionGeneration ||
		attestationGeneration != binding.AttestationGeneration ||
		attestationSHA256 != binding.AttestationSHA256 ||
		materializationID != binding.MaterializationID ||
		attemptID != binding.AttemptID ||
		attemptNumber != binding.AttemptNumber ||
		leaseEpoch != binding.LeaseEpoch ||
		builderID != binding.BuilderID ||
		purpose != "production" ||
		!bytes.Equal(values["shadow_campaign_id"], []byte("null")) ||
		cpuArch != binding.CPUArch ||
		platform != binding.Platform ||
		component != binding.Component ||
		generation != binding.Generation ||
		predecessorID != binding.PredecessorCredentialID ||
		predecessorGeneration != binding.PredecessorGeneration ||
		heartbeatID != binding.LeaseHeartbeatOperationID ||
		registryOrigin != binding.RegistryOrigin ||
		registryService != binding.RegistryService ||
		registryIssuer != binding.RegistryIssuer ||
		registryKeyID != binding.RegistryKeyID ||
		repository != expectedRepository ||
		len(actions) != 2 || actions[0] != "pull" || actions[1] != "push" ||
		!isCanonicalNonZeroUUID(credentialID) ||
		!isCanonicalNonZeroUUID(requestID) ||
		!isCanonicalNonZeroUUID(grantID) ||
		!isCanonicalNonZeroUUID(sessionID) ||
		!isCanonicalNonZeroUUID(materializationID) ||
		!isCanonicalNonZeroUUID(attemptID) ||
		sessionGeneration <= 0 ||
		attestationGeneration <= 0 ||
		attemptNumber <= 0 ||
		leaseEpoch <= 0 ||
		generation <= 0 || generation > 512 ||
		!isDigest(attestationSHA256) ||
		!builderIDPattern.MatchString(builderID) ||
		!componentPattern.MatchString(component) ||
		!registryIdentityPattern.MatchString(registryService) ||
		!registryIdentityPattern.MatchString(registryIssuer) ||
		!registryKeyIDPattern.MatchString(registryKeyID) ||
		!bearerTokenPattern.Match(tokenBytes) ||
		token != string(tokenBytes) ||
		!registryOriginValid(registryOrigin) ||
		!platformMatchesCPUArch(platform, cpuArch) ||
		issuedAt.Nanosecond() != 0 ||
		expiresAt.Nanosecond() != 0 ||
		!expiresAt.After(issuedAt) ||
		expiresAt.Sub(issuedAt) > maxRegistryCredentialLifetime {
		return nil, errors.New("registry credential binding invalid")
	}
	if generation == 1 {
		if predecessorIDSet || predecessorGenerationSet || heartbeatIDSet {
			return nil, errors.New("registry credential predecessor invalid")
		}
	} else if !predecessorIDSet || !predecessorGenerationSet || !heartbeatIDSet || predecessorGeneration != generation-1 {
		return nil, errors.New("registry credential predecessor invalid")
	}
	return &RegistryCredential{
		secret:                    secret,
		ID:                        credentialID,
		RequestID:                 requestID,
		GrantID:                   grantID,
		SessionID:                 sessionID,
		SessionGeneration:         sessionGeneration,
		AttestationGeneration:     attestationGeneration,
		AttestationSHA256:         attestationSHA256,
		MaterializationID:         materializationID,
		AttemptID:                 attemptID,
		AttemptNumber:             attemptNumber,
		LeaseEpoch:                leaseEpoch,
		BuilderID:                 builderID,
		CPUArch:                   cpuArch,
		Platform:                  platform,
		Component:                 component,
		Generation:                generation,
		PredecessorCredentialID:   predecessorID,
		PredecessorGeneration:     predecessorGeneration,
		LeaseHeartbeatOperationID: heartbeatID,
		Repository:                repository,
		RegistryOrigin:            registryOrigin,
		RegistryService:           registryService,
		RegistryIssuer:            registryIssuer,
		RegistryKeyID:             registryKeyID,
		BearerToken:               tokenBytes,
		IssuedAt:                  issuedAt,
		ExpiresAt:                 expiresAt,
	}, nil
}

type PublicationAttemptBinding struct {
	GrantID           string
	MaterializationID string
	AttemptID         string
	AttemptNumber     int
	LeaseEpoch        int
	BuilderID         string
	CPUArch           string
	Platform          string
	RegistryOrigin    string
	RegistryService   string
	RegistryIssuer    string
	RegistryKeyID     string
}

type publicationCredentialGuard interface {
	RegistryCredential(context.Context, RegistryCredentialRequest, *SecretBuffer) (*SecretBuffer, error)
	PublicationCandidate(context.Context, PublicationCandidateRequest, *SecretBuffer) (*PublicationCandidateAcknowledgement, error)
	Heartbeat(context.Context, string, string, string, string, int, *SecretBuffer) (*LeaseResponse, error)
}

type PublicationCredentialSource struct {
	session *SessionManager
	guard   publicationCredentialGuard
	binding PublicationAttemptBinding
}

func NewPublicationCredentialSource(session *SessionManager, guard publicationCredentialGuard, binding PublicationAttemptBinding) *PublicationCredentialSource {
	return &PublicationCredentialSource{session: session, guard: guard, binding: binding}
}

func (s *PublicationCredentialSource) Next(ctx context.Context, set BuiltComponentSet, component string, predecessor *RegistryCredential) (*RegistryCredential, error) {
	if s == nil || s.session == nil || s.guard == nil || !s.setMatches(set) || !componentPattern.MatchString(component) {
		return nil, errors.New("publication credential source invalid")
	}
	generation := 1
	var predecessorID string
	var predecessorGeneration int
	var heartbeatID string
	if predecessor != nil {
		generation = predecessor.Generation + 1
		predecessorID = predecessor.ID
		predecessorGeneration = predecessor.Generation
		if _, err := s.session.Renew(ctx); err != nil {
			return nil, err
		}
		var lease *LeaseResponse
		operationID, err := newUUID()
		if err != nil {
			return nil, err
		}
		heartbeatID = operationID
		if err := s.session.WithCurrentEnvelope(func(_ *SessionEnvelope, current *SecretBuffer) error {
			var err error
			lease, err = s.guard.Heartbeat(ctx, s.binding.GrantID, operationID, s.binding.MaterializationID, s.binding.AttemptID, s.binding.LeaseEpoch, current)
			return err
		}); err != nil {
			return nil, err
		}
		if lease == nil || lease.MaterializationID != s.binding.MaterializationID || lease.AttemptID != s.binding.AttemptID || lease.LeaseEpoch != s.binding.LeaseEpoch || lease.OperationID != operationID || (lease.State != "claimed" && lease.State != "running") {
			return nil, errors.New("publication credential heartbeat invalid")
		}
	}
	var parsed *RegistryCredential
	err := s.session.WithCurrentEnvelope(func(session *SessionEnvelope, current *SecretBuffer) error {
		requestID, err := newUUID()
		if err != nil {
			return err
		}
		secret, err := s.guard.RegistryCredential(ctx, RegistryCredentialRequest{
			GrantID:                 s.binding.GrantID,
			OperationID:             requestID,
			MaterializationID:       s.binding.MaterializationID,
			AttemptID:               s.binding.AttemptID,
			LeaseEpoch:              s.binding.LeaseEpoch,
			Component:               component,
			PredecessorCredentialID: predecessorID,
			PredecessorGeneration:   predecessorGeneration,
		}, current)
		if err != nil {
			return err
		}
		binding := RegistryCredentialBinding{
			RequestID:                 requestID,
			GrantID:                   s.binding.GrantID,
			SessionID:                 session.SessionID,
			SessionGeneration:         session.Generation,
			AttestationGeneration:     session.AttestationGeneration,
			AttestationSHA256:         session.AttestationSHA256,
			MaterializationID:         s.binding.MaterializationID,
			AttemptID:                 s.binding.AttemptID,
			AttemptNumber:             s.binding.AttemptNumber,
			LeaseEpoch:                s.binding.LeaseEpoch,
			BuilderID:                 s.binding.BuilderID,
			CPUArch:                   s.binding.CPUArch,
			Platform:                  s.binding.Platform,
			Component:                 component,
			Generation:                generation,
			PredecessorCredentialID:   predecessorID,
			PredecessorGeneration:     predecessorGeneration,
			LeaseHeartbeatOperationID: heartbeatID,
			RegistryOrigin:            s.binding.RegistryOrigin,
			RegistryService:           s.binding.RegistryService,
			RegistryIssuer:            s.binding.RegistryIssuer,
			RegistryKeyID:             s.binding.RegistryKeyID,
			Now:                       time.Now().UTC(),
		}
		parsed, err = ParseRegistryCredential(secret, binding)
		if err != nil {
			secret.Close()
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	if predecessor != nil {
		predecessor.Close()
	}
	return parsed, nil
}

func (s *PublicationCredentialSource) Record(ctx context.Context, set BuiltComponentSet, credential *RegistryCredential, component BuiltComponent) (*PublicationCandidateAcknowledgement, error) {
	if s == nil || s.session == nil || s.guard == nil || credential == nil || !s.setMatches(set) || component.Name != credential.Component {
		return nil, errors.New("publication candidate source invalid")
	}
	var ack *PublicationCandidateAcknowledgement
	err := s.session.WithCurrentEnvelope(func(session *SessionEnvelope, current *SecretBuffer) error {
		operationID, err := newUUID()
		if err != nil {
			return err
		}
		ack, err = s.guard.PublicationCandidate(ctx, PublicationCandidateRequest{
			GrantID:              s.binding.GrantID,
			OperationID:          operationID,
			CredentialID:         credential.ID,
			CredentialGeneration: credential.Generation,
			SessionID:            session.SessionID,
			SessionGeneration:    session.Generation,
			MaterializationID:    s.binding.MaterializationID,
			AttemptID:            s.binding.AttemptID,
			AttemptNumber:        s.binding.AttemptNumber,
			LeaseEpoch:           s.binding.LeaseEpoch,
			BuilderID:            s.binding.BuilderID,
			Component:            component.Name,
			ManifestDigest:       component.Output.TopLevelDigest,
			ManifestSize:         descriptorSize(component.Output),
			OCIFileSHA256:        component.Output.FileSHA256,
			OCIFileSize:          component.Output.SizeBytes,
			Platform:             s.binding.Platform,
		}, current)
		return err
	})
	if err != nil {
		return nil, err
	}
	if err := validateCandidateAcknowledgement(ack, PublicationCandidateRequest{
		GrantID:                 s.binding.GrantID,
		CredentialID:            credential.ID,
		CredentialGeneration:    credential.Generation,
		MaterializationID:       s.binding.MaterializationID,
		AttemptID:               s.binding.AttemptID,
		AttemptNumber:           s.binding.AttemptNumber,
		LeaseEpoch:              s.binding.LeaseEpoch,
		BuilderID:               s.binding.BuilderID,
		Component:               component.Name,
		ManifestDigest:          component.Output.TopLevelDigest,
		ManifestSize:            descriptorSize(component.Output),
		OCIFileSHA256:           component.Output.FileSHA256,
		OCIFileSize:             component.Output.SizeBytes,
		Platform:                s.binding.Platform,
		AuthorityResponseSHA256: "",
	}); err != nil {
		return nil, err
	}
	return ack, nil
}

func (s *PublicationCredentialSource) setMatches(set BuiltComponentSet) bool {
	return set.GrantID == s.binding.GrantID &&
		set.MaterializationID == s.binding.MaterializationID &&
		set.AttemptID == s.binding.AttemptID &&
		set.LeaseEpoch == s.binding.LeaseEpoch
}

func descriptorSize(output OCIOutput) int64 {
	if output.SizeBytes > 0 {
		return output.SizeBytes
	}
	return 1
}

func publicationRepository(cpuArch string, attemptID string, component string) (string, error) {
	if cpuArch != "x86_64" && cpuArch != "arm64" {
		return "", errors.New("registry publication architecture invalid")
	}
	if !isCanonicalNonZeroUUID(attemptID) || !componentPattern.MatchString(component) {
		return "", errors.New("registry publication binding invalid")
	}
	segment := "task"
	if component != "task" {
		sum := sha256.Sum256([]byte(component))
		segment = "sidecar-sha256-" + hex.EncodeToString(sum[:])
	}
	return fmt.Sprintf("loom-task-image-attempts/%s/%s/%s", cpuArch, attemptID, segment), nil
}

func decodeRequiredString(values map[string][]byte, key string) (string, error) {
	return decodeJSONString(values[key])
}

func decodeOptionalCredentialString(payload []byte) (string, bool, error) {
	if bytes.Equal(payload, []byte("null")) {
		return "", false, nil
	}
	value, err := decodeJSONString(payload)
	if err != nil {
		return "", false, err
	}
	if !isCanonicalNonZeroUUID(value) {
		return "", false, errors.New("uuid invalid")
	}
	return value, true, nil
}

func decodeOptionalCredentialInt(payload []byte) (int, bool, error) {
	if bytes.Equal(payload, []byte("null")) {
		return 0, false, nil
	}
	value, err := decodeJSONInt(payload)
	if err != nil || value <= 0 || value > 512 {
		return 0, false, errors.New("generation invalid")
	}
	return value, true, nil
}

func registryOriginValid(value string) bool {
	parsed, err := url.Parse(value)
	return err == nil && parsed.Scheme == "https" && parsed.Host != "" && parsed.User == nil && parsed.RawQuery == "" && parsed.Fragment == ""
}

func platformMatchesCPUArch(platform string, cpuArch string) bool {
	return (cpuArch == "x86_64" && platform == "linux/amd64") || (cpuArch == "arm64" && platform == "linux/arm64")
}

func parsePublicationManifestDigest(value string) error {
	_, err := parseSHA256Descriptor(value)
	return err
}

func validateCandidateAcknowledgement(ack *PublicationCandidateAcknowledgement, request PublicationCandidateRequest) error {
	if ack == nil ||
		ack.GrantID != request.GrantID ||
		ack.CredentialID != request.CredentialID ||
		ack.CredentialGeneration != request.CredentialGeneration ||
		ack.MaterializationID != request.MaterializationID ||
		ack.AttemptID != request.AttemptID ||
		ack.AttemptNumber != request.AttemptNumber ||
		ack.LeaseEpoch != request.LeaseEpoch ||
		ack.BuilderID != request.BuilderID ||
		ack.Component != request.Component ||
		ack.ManifestDigest != request.ManifestDigest ||
		ack.ManifestSize != request.ManifestSize ||
		ack.OCIFileSHA256 != request.OCIFileSHA256 ||
		ack.OCIFileSize != request.OCIFileSize ||
		ack.Platform != request.Platform ||
		!isCanonicalNonZeroUUID(ack.CandidateID) ||
		!isCanonicalNonZeroUUID(ack.OperationID) ||
		!isCanonicalNonZeroUUID(ack.SessionID) ||
		ack.SessionGeneration <= 0 ||
		!isDigest(ack.AuthorityResponseSHA256) {
		return errors.New("publication candidate acknowledgement invalid")
	}
	return nil
}
