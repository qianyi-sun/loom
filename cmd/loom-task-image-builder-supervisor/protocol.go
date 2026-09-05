package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"syscall"
	"time"
)

const maxSecretBytes = 64 * 1024

type GuardClient struct {
	socketPath     string
	maxPacketBytes int
	ackTimeout     time.Duration
}

var requiredGuardUID = uint32(0)

var (
	unixSocket = func(domain int, typ int, proto int) (int, error) {
		return syscall.Socket(domain, typ, proto)
	}
	unixSetsockoptInt = func(fd int, level int, opt int, value int) error {
		return syscall.SetsockoptInt(fd, level, opt, value)
	}
	unixConnect = func(fd int, sa syscall.Sockaddr) error {
		return syscall.Connect(fd, sa)
	}
	unixSendmsgN = func(fd int, payload []byte, oob []byte, to syscall.Sockaddr, flags int) (int, error) {
		return syscall.SendmsgN(fd, payload, oob, to, flags)
	}
	unixRecvmsg = func(fd int, payload []byte, oob []byte, flags int) (int, int, int, syscall.Sockaddr, error) {
		return syscall.Recvmsg(fd, payload, oob, flags)
	}
)

type AllocationCapabilities struct {
	Bootstrap          *SecretBuffer
	ProofSHA256        string
	ReceiptSHA256      string
	JobDirectoryFD     int
	JobDirectoryDevice uint64
	JobDirectoryInode  uint64
	BuildEgressFD      int
	BuildEgressDevice  uint64
	BuildEgressInode   uint64
	closeHook          func()
}

func (a *AllocationCapabilities) Close() {
	if a == nil {
		return
	}
	if a.closeHook != nil {
		a.closeHook()
		a.closeHook = nil
	}
	if a.Bootstrap != nil {
		a.Bootstrap.Close()
	}
	if a.JobDirectoryFD >= 0 {
		syscall.Close(a.JobDirectoryFD)
		a.JobDirectoryFD = -1
	}
	if a.BuildEgressFD >= 0 {
		syscall.Close(a.BuildEgressFD)
		a.BuildEgressFD = -1
	}
}

type LeaseResponse struct {
	Operation                 string
	ResponseID                string
	GrantID                   string
	OperationID               string
	MaterializationID         string
	AttemptID                 string
	LeaseEpoch                int
	State                     string
	DeterministicFailureCount int
	LeaseExpiresAt            *time.Time
}

type RegistryCredentialRequest struct {
	GrantID                 string
	OperationID             string
	MaterializationID       string
	AttemptID               string
	LeaseEpoch              int
	Component               string
	PredecessorCredentialID string
	PredecessorGeneration   int
}

type PublicationCandidateRequest struct {
	GrantID                 string
	OperationID             string
	CredentialID            string
	CredentialGeneration    int
	SessionID               string
	SessionGeneration       int
	MaterializationID       string
	AttemptID               string
	AttemptNumber           int
	LeaseEpoch              int
	BuilderID               string
	Component               string
	ManifestDigest          string
	ManifestSize            int64
	OCIFileSHA256           string
	OCIFileSize             int64
	Platform                string
	AuthorityResponseSHA256 string
}

type PublicationCandidateAcknowledgement struct {
	CandidateID             string
	OperationID             string
	CredentialID            string
	CredentialGeneration    int
	GrantID                 string
	SessionID               string
	SessionGeneration       int
	MaterializationID       string
	AttemptID               string
	AttemptNumber           int
	LeaseEpoch              int
	BuilderID               string
	Component               string
	ManifestDigest          string
	ManifestSize            int64
	OCIFileSHA256           string
	OCIFileSize             int64
	Platform                string
	RecordedAt              time.Time
	AuthorityResponseSHA256 string
}

func NewGuardClient(socketPath string, maxPacketBytes int, ackTimeout time.Duration) *GuardClient {
	return &GuardClient{
		socketPath:     socketPath,
		maxPacketBytes: maxPacketBytes,
		ackTimeout:     ackTimeout,
	}
}

func (c *GuardClient) Project(ctx context.Context, grantID string) (*AllocationCapabilities, error) {
	request := map[string]any{
		"schema":    localSchema,
		"operation": "project",
		"grant_id":  grantID,
	}
	packet, rights, err := c.roundTrip(ctx, request, nil)
	if err != nil {
		return nil, err
	}
	defer closeRights(rights)
	defer packet.Close()

	var response struct {
		Schema                     string `json:"schema"`
		Operation                  string `json:"operation"`
		ResponseID                 string `json:"response_id"`
		GrantID                    string `json:"grant_id"`
		ProofSHA256                string `json:"proof_sha256"`
		ReceiptPublicBindingSHA256 string `json:"receipt_public_binding_sha256"`
		Rights                     []struct {
			Index  int    `json:"index"`
			Kind   string `json:"kind"`
			Role   string `json:"role"`
			Device uint64 `json:"device"`
			Inode  uint64 `json:"inode"`
		} `json:"rights"`
	}
	if err := decodeStrictJSON(packet.payload, &response); err != nil {
		return nil, err
	}
	if response.Schema != localSchema || response.Operation != "projected" || response.GrantID != grantID || !isCanonicalNonZeroUUID(response.ResponseID) {
		return nil, errors.New("project response invalid")
	}
	if len(response.Rights) != 3 || len(rights) != 3 {
		return nil, errors.New("project rights invalid")
	}
	if response.Rights[0].Index != 0 || response.Rights[0].Kind != "sealed_memfd" || response.Rights[0].Role != "bootstrap" {
		return nil, errors.New("project rights invalid")
	}
	if response.Rights[1].Index != 1 || response.Rights[1].Kind != "directory" || response.Rights[1].Role != "job_storage" {
		return nil, errors.New("project rights invalid")
	}
	if response.Rights[2].Index != 2 || response.Rights[2].Kind != "directory" || response.Rights[2].Role != "build_egress" {
		return nil, errors.New("project rights invalid")
	}
	bootstrap, err := NewSecretBuffer(rights[0], maxSecretBytes)
	rights[0] = -1
	if err != nil {
		return nil, err
	}
	jobStat, err := validateDirectoryDescriptor(rights[1])
	if err != nil {
		bootstrap.Close()
		return nil, err
	}
	buildStat, err := validateDirectoryDescriptor(rights[2])
	if err != nil {
		bootstrap.Close()
		return nil, err
	}
	if uint64(jobStat.Dev) != response.Rights[1].Device || uint64(jobStat.Ino) != response.Rights[1].Inode {
		bootstrap.Close()
		return nil, errors.New("projected job directory identity mismatch")
	}
	if uint64(buildStat.Dev) != response.Rights[2].Device || uint64(buildStat.Ino) != response.Rights[2].Inode {
		bootstrap.Close()
		return nil, errors.New("projected build directory identity mismatch")
	}
	caps := &AllocationCapabilities{
		Bootstrap:          bootstrap,
		ProofSHA256:        response.ProofSHA256,
		ReceiptSHA256:      response.ReceiptPublicBindingSHA256,
		JobDirectoryFD:     rights[1],
		JobDirectoryDevice: uint64(jobStat.Dev),
		JobDirectoryInode:  uint64(jobStat.Ino),
		BuildEgressFD:      rights[2],
		BuildEgressDevice:  uint64(buildStat.Dev),
		BuildEgressInode:   uint64(buildStat.Ino),
	}
	rights[1], rights[2] = -1, -1
	if err := c.sendAck(packet.fd, response.ResponseID, packet.deadline); err != nil {
		caps.Close()
		return nil, err
	}
	packet.fd = -1
	return caps, nil
}

func (c *GuardClient) Exchange(ctx context.Context, grantID string, exchangeID string, proofSHA256 string, bootstrap *SecretBuffer) (*SessionEnvelope, error) {
	request := map[string]any{
		"schema":       localSchema,
		"operation":    "exchange",
		"grant_id":     grantID,
		"exchange_id":  exchangeID,
		"proof_sha256": proofSHA256,
	}
	fd, err := bootstrapExchangeMemfd(grantID, exchangeID, proofSHA256, bootstrap)
	if err != nil {
		return nil, err
	}
	packet, rights, err := c.roundTrip(ctx, request, []int{fd})
	syscall.Close(fd)
	if err != nil {
		return nil, err
	}
	defer closeRights(rights)
	defer packet.Close()
	return c.decodeSessionResponse(packet, rights, grantID)
}

func bootstrapExchangeMemfd(grantID string, exchangeID string, proofSHA256 string, bootstrap *SecretBuffer) (int, error) {
	if bootstrap == nil || bootstrap.closed || len(bootstrap.data) == 0 {
		return -1, errors.New("secret unavailable")
	}
	var receipt struct {
		SchemaVersion  int    `json:"schema_version"`
		GrantID        string `json:"grant_id"`
		ProofID        string `json:"proof_id"`
		ProofSHA256    string `json:"proof_sha256"`
		BootstrapToken string `json:"bootstrap_token"`
		IssuedAt       string `json:"issued_at"`
		ExpiresAt      string `json:"expires_at"`
	}
	if err := decodeStrictJSON(bootstrap.data, &receipt); err != nil {
		return -1, err
	}
	if receipt.BootstrapToken == "" {
		return -1, errors.New("bootstrap receipt invalid")
	}
	observedAt := receipt.IssuedAt
	if observedAt == "" {
		observedAt = time.Now().UTC().Format(time.RFC3339)
	} else if parsed, err := time.Parse(time.RFC3339, observedAt); err == nil {
		observedAt = parsed.UTC().Format(time.RFC3339)
	} else {
		return -1, errors.New("bootstrap receipt invalid")
	}
	payload, err := encodeCanonicalJSON(map[string]any{
		"schema_version":  1,
		"exchange_id":     exchangeID,
		"grant_id":        grantID,
		"proof_sha256":    proofSHA256,
		"bootstrap_token": receipt.BootstrapToken,
		"observed_at":     observedAt,
	})
	if err != nil {
		return -1, err
	}
	return createSealedMemfd("bootstrap-exchange", payload, maxSecretBytes)
}

func (c *GuardClient) Renew(ctx context.Context, grantID string, operationID string, current *SecretBuffer) (*SessionEnvelope, error) {
	request := map[string]any{
		"schema":       localSchema,
		"operation":    "renew",
		"grant_id":     grantID,
		"operation_id": operationID,
	}
	fd, err := current.cloneSealedMemfd("session-renew", maxSecretBytes)
	if err != nil {
		return nil, err
	}
	packet, rights, err := c.roundTrip(ctx, request, []int{fd})
	syscall.Close(fd)
	if err != nil {
		return nil, err
	}
	defer closeRights(rights)
	defer packet.Close()
	return c.decodeSessionResponse(packet, rights, grantID)
}

func (c *GuardClient) Claim(ctx context.Context, grantID string, operationID string, current *SecretBuffer) (*SecretBuffer, bool, error) {
	return c.secretOperation(ctx, map[string]any{
		"schema":       localSchema,
		"operation":    "claim",
		"grant_id":     grantID,
		"operation_id": operationID,
	}, current)
}

func (c *GuardClient) Bundle(ctx context.Context, grantID string, operationID string, materializationID string, attemptID string, leaseEpoch int, current *SecretBuffer) (*SecretBuffer, error) {
	secret, _, err := c.secretOperation(ctx, map[string]any{
		"schema":             localSchema,
		"operation":          "bundle",
		"grant_id":           grantID,
		"operation_id":       operationID,
		"materialization_id": materializationID,
		"attempt_id":         attemptID,
		"lease_epoch":        leaseEpoch,
	}, current)
	return secret, err
}

func (c *GuardClient) RegistryCredential(ctx context.Context, request RegistryCredentialRequest, current *SecretBuffer) (*SecretBuffer, error) {
	if (request.PredecessorCredentialID == "") != (request.PredecessorGeneration == 0) {
		return nil, errors.New("registry credential predecessor invalid")
	}
	var predecessorID any
	var predecessorGeneration any
	if request.PredecessorCredentialID != "" || request.PredecessorGeneration != 0 {
		predecessorID = request.PredecessorCredentialID
		predecessorGeneration = request.PredecessorGeneration
	}
	secret, _, err := c.secretOperation(ctx, map[string]any{
		"schema":                    localSchema,
		"operation":                 "registry-credential",
		"grant_id":                  request.GrantID,
		"operation_id":              request.OperationID,
		"materialization_id":        request.MaterializationID,
		"attempt_id":                request.AttemptID,
		"lease_epoch":               request.LeaseEpoch,
		"component":                 request.Component,
		"predecessor_credential_id": predecessorID,
		"predecessor_generation":    predecessorGeneration,
	}, current)
	return secret, err
}

func (c *GuardClient) PublicationCandidate(ctx context.Context, request PublicationCandidateRequest, current *SecretBuffer) (*PublicationCandidateAcknowledgement, error) {
	localRequest := map[string]any{
		"schema":                localSchema,
		"operation":             "publication-candidate",
		"grant_id":              request.GrantID,
		"operation_id":          request.OperationID,
		"materialization_id":    request.MaterializationID,
		"attempt_id":            request.AttemptID,
		"lease_epoch":           request.LeaseEpoch,
		"credential_id":         request.CredentialID,
		"credential_generation": request.CredentialGeneration,
		"component":             request.Component,
		"manifest_digest":       request.ManifestDigest,
		"manifest_size":         request.ManifestSize,
		"oci_file_sha256":       request.OCIFileSHA256,
		"oci_file_size":         request.OCIFileSize,
		"platform":              request.Platform,
	}
	fd, err := current.cloneSealedMemfd("session-publication-candidate", maxSecretBytes)
	if err != nil {
		return nil, err
	}
	packet, rights, err := c.roundTrip(ctx, localRequest, []int{fd})
	syscall.Close(fd)
	if err != nil {
		return nil, err
	}
	defer closeRights(rights)
	defer packet.Close()
	if len(rights) != 0 {
		return nil, errors.New("publication candidate response should not carry rights")
	}
	var response struct {
		Schema                  string `json:"schema"`
		Operation               string `json:"operation"`
		ResponseID              string `json:"response_id"`
		GrantID                 string `json:"grant_id"`
		CandidateID             string `json:"candidate_id"`
		OperationID             string `json:"operation_id"`
		CredentialID            string `json:"credential_id"`
		CredentialGeneration    int    `json:"credential_generation"`
		SessionID               string `json:"session_id"`
		SessionGeneration       int    `json:"session_generation"`
		MaterializationID       string `json:"materialization_id"`
		AttemptID               string `json:"attempt_id"`
		AttemptNumber           int    `json:"attempt_number"`
		LeaseEpoch              int    `json:"lease_epoch"`
		BuilderID               string `json:"builder_id"`
		Component               string `json:"component"`
		ManifestDigest          string `json:"manifest_digest"`
		ManifestSize            int64  `json:"manifest_size"`
		OCIFileSHA256           string `json:"oci_file_sha256"`
		OCIFileSize             int64  `json:"oci_file_size"`
		Platform                string `json:"platform"`
		RecordedAt              string `json:"recorded_at"`
		AuthorityResponseSHA256 string `json:"authority_response_sha256"`
	}
	if err := decodeStrictJSON(packet.payload, &response); err != nil {
		return nil, err
	}
	recordedAt, err := time.Parse(time.RFC3339, response.RecordedAt)
	if err != nil {
		return nil, err
	}
	if response.Schema != localSchema ||
		response.Operation != "publication-candidate" ||
		response.GrantID != request.GrantID ||
		response.OperationID != request.OperationID ||
		response.CredentialID != request.CredentialID ||
		response.CredentialGeneration != request.CredentialGeneration ||
		response.SessionID != request.SessionID ||
		response.SessionGeneration != request.SessionGeneration ||
		response.MaterializationID != request.MaterializationID ||
		response.AttemptID != request.AttemptID ||
		response.AttemptNumber != request.AttemptNumber ||
		response.LeaseEpoch != request.LeaseEpoch ||
		response.BuilderID != request.BuilderID ||
		response.Component != request.Component ||
		response.ManifestDigest != request.ManifestDigest ||
		response.ManifestSize != request.ManifestSize ||
		response.OCIFileSHA256 != request.OCIFileSHA256 ||
		response.OCIFileSize != request.OCIFileSize ||
		response.Platform != request.Platform ||
		(request.AuthorityResponseSHA256 != "" && response.AuthorityResponseSHA256 != request.AuthorityResponseSHA256) ||
		!isCanonicalNonZeroUUID(response.ResponseID) ||
		!isCanonicalNonZeroUUID(response.CandidateID) ||
		!isCanonicalNonZeroUUID(response.OperationID) ||
		!isCanonicalNonZeroUUID(response.CredentialID) ||
		!isCanonicalNonZeroUUID(response.SessionID) ||
		!isCanonicalNonZeroUUID(response.MaterializationID) ||
		!isCanonicalNonZeroUUID(response.AttemptID) ||
		response.CredentialGeneration <= 0 ||
		response.SessionGeneration <= 0 ||
		response.AttemptNumber <= 0 ||
		response.LeaseEpoch <= 0 ||
		!builderIDPattern.MatchString(response.BuilderID) ||
		!componentPattern.MatchString(response.Component) ||
		response.ManifestSize <= 0 ||
		response.OCIFileSize <= 0 ||
		parsePublicationManifestDigest(response.ManifestDigest) != nil ||
		!isDigest(response.OCIFileSHA256) ||
		(response.Platform != "linux/amd64" && response.Platform != "linux/arm64") ||
		!isDigest(response.AuthorityResponseSHA256) {
		return nil, errors.New("publication candidate response invalid")
	}
	if err := c.sendAck(packet.fd, response.ResponseID, packet.deadline); err != nil {
		return nil, err
	}
	packet.fd = -1
	return &PublicationCandidateAcknowledgement{
		CandidateID:             response.CandidateID,
		OperationID:             response.OperationID,
		CredentialID:            response.CredentialID,
		CredentialGeneration:    response.CredentialGeneration,
		GrantID:                 response.GrantID,
		SessionID:               response.SessionID,
		SessionGeneration:       response.SessionGeneration,
		MaterializationID:       response.MaterializationID,
		AttemptID:               response.AttemptID,
		AttemptNumber:           response.AttemptNumber,
		LeaseEpoch:              response.LeaseEpoch,
		BuilderID:               response.BuilderID,
		Component:               response.Component,
		ManifestDigest:          response.ManifestDigest,
		ManifestSize:            response.ManifestSize,
		OCIFileSHA256:           response.OCIFileSHA256,
		OCIFileSize:             response.OCIFileSize,
		Platform:                response.Platform,
		RecordedAt:              recordedAt,
		AuthorityResponseSHA256: response.AuthorityResponseSHA256,
	}, nil
}

func (c *GuardClient) Start(ctx context.Context, grantID string, operationID string, materializationID string, attemptID string, leaseEpoch int, current *SecretBuffer) (*LeaseResponse, error) {
	return c.leaseOperation(ctx, "start", grantID, operationID, materializationID, attemptID, leaseEpoch, "", current)
}

func (c *GuardClient) Heartbeat(ctx context.Context, grantID string, operationID string, materializationID string, attemptID string, leaseEpoch int, current *SecretBuffer) (*LeaseResponse, error) {
	return c.leaseOperation(ctx, "heartbeat", grantID, operationID, materializationID, attemptID, leaseEpoch, "", current)
}

func (c *GuardClient) Release(ctx context.Context, grantID string, operationID string, materializationID string, attemptID string, leaseEpoch int, current *SecretBuffer) (*LeaseResponse, error) {
	return c.leaseOperation(ctx, "release", grantID, operationID, materializationID, attemptID, leaseEpoch, "", current)
}

func (c *GuardClient) Fail(ctx context.Context, grantID string, operationID string, materializationID string, attemptID string, leaseEpoch int, failureKind string, current *SecretBuffer) (*LeaseResponse, error) {
	return c.leaseOperation(ctx, "fail", grantID, operationID, materializationID, attemptID, leaseEpoch, failureKind, current)
}

func (c *GuardClient) Finish(ctx context.Context, grantID string, operationID string, cleanup map[string]int) error {
	request := map[string]any{
		"schema":       localSchema,
		"operation":    "finish",
		"grant_id":     grantID,
		"operation_id": operationID,
		"cleanup":      cleanup,
	}
	packet, rights, err := c.roundTrip(ctx, request, nil)
	if err != nil {
		return err
	}
	defer closeRights(rights)
	defer packet.Close()
	var response struct {
		Schema      string `json:"schema"`
		Operation   string `json:"operation"`
		ResponseID  string `json:"response_id"`
		GrantID     string `json:"grant_id"`
		OperationID string `json:"operation_id"`
	}
	if err := decodeStrictJSON(packet.payload, &response); err != nil {
		return err
	}
	if response.Schema != localSchema || response.Operation != "finishing" || response.GrantID != grantID || response.OperationID != operationID || !isCanonicalNonZeroUUID(response.ResponseID) {
		return errors.New("finish response invalid")
	}
	if err := c.sendAck(packet.fd, response.ResponseID, packet.deadline); err != nil {
		return err
	}
	packet.fd = -1
	return nil
}

func (c *GuardClient) decodeSessionResponse(packet *responsePacket, rights []int, grantID string) (*SessionEnvelope, error) {
	if len(rights) != 1 {
		return nil, errors.New("session response rights invalid")
	}
	var response struct {
		Schema                  string `json:"schema"`
		Operation               string `json:"operation"`
		ResponseID              string `json:"response_id"`
		GrantID                 string `json:"grant_id"`
		SessionID               string `json:"session_id"`
		SessionGeneration       int    `json:"session_generation"`
		SessionPublicBindingSHA string `json:"session_public_binding_sha256"`
	}
	if err := decodeStrictJSON(packet.payload, &response); err != nil {
		return nil, err
	}
	if response.Schema != localSchema || response.Operation != "session" || response.GrantID != grantID || !isCanonicalNonZeroUUID(response.ResponseID) {
		return nil, errors.New("session response invalid")
	}
	buffer, err := NewSecretBuffer(rights[0], maxSecretBytes)
	rights[0] = -1
	if err != nil {
		return nil, err
	}
	session, err := parseSessionEnvelope(buffer)
	if err != nil {
		buffer.Close()
		return nil, err
	}
	if !isCanonicalNonZeroUUID(response.SessionID) || !isDigest(response.SessionPublicBindingSHA) {
		session.Secret.Close()
		return nil, errors.New("session response invalid")
	}
	session.SessionPublicBindingSHA256 = response.SessionPublicBindingSHA
	if session.Generation != response.SessionGeneration || session.SessionID != response.SessionID || session.GrantID != grantID {
		session.Secret.Close()
		return nil, errors.New("session response binding invalid")
	}
	if err := c.sendAck(packet.fd, response.ResponseID, packet.deadline); err != nil {
		session.Secret.Close()
		return nil, err
	}
	packet.fd = -1
	return session, nil
}

type responsePacket struct {
	fd       int
	payload  []byte
	deadline time.Time
}

func (p *responsePacket) Close() {
	if p != nil && p.fd >= 0 {
		syscall.Close(p.fd)
		p.fd = -1
	}
}

func (c *GuardClient) roundTrip(ctx context.Context, request map[string]any, rights []int) (*responsePacket, []int, error) {
	fd, deadline, err := c.connect(ctx)
	if err != nil {
		return nil, nil, err
	}
	if err := sendLocalPacket(fd, deadline, request, rights); err != nil {
		syscall.Close(fd)
		return nil, nil, wrapDeadline(ctx, err)
	}
	payload, receivedRights, credentials, flags, err := receiveLocalPacket(fd, deadline, c.maxPacketBytes)
	if err != nil {
		syscall.Close(fd)
		return nil, nil, wrapDeadline(ctx, err)
	}
	if flags&(syscall.MSG_TRUNC|syscall.MSG_CTRUNC) != 0 {
		closeRights(receivedRights)
		syscall.Close(fd)
		return nil, nil, errors.New("local packet truncated")
	}
	if err := validateGuardPeerCredentials(credentials); err != nil {
		closeRights(receivedRights)
		syscall.Close(fd)
		return nil, nil, err
	}
	return &responsePacket{fd: fd, payload: payload, deadline: deadline}, receivedRights, nil
}

func (c *GuardClient) connect(ctx context.Context) (int, time.Time, error) {
	deadline := time.Now().Add(c.ackTimeout)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}
	for {
		if deadlineExceeded(deadline) {
			return -1, time.Time{}, wrapDeadline(ctx, syscall.EAGAIN)
		}
		fd, err := unixSocket(syscall.AF_UNIX, syscall.SOCK_SEQPACKET|syscall.SOCK_CLOEXEC, 0)
		if err != nil {
			return -1, time.Time{}, err
		}
		if err := unixSetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_PASSCRED, 1); err != nil {
			syscall.Close(fd)
			return -1, time.Time{}, err
		}
		if err := applySocketDeadline(fd, deadline); err != nil {
			syscall.Close(fd)
			return -1, time.Time{}, err
		}
		if err := unixConnect(fd, &syscall.SockaddrUnix{Name: c.socketPath}); err != nil {
			syscall.Close(fd)
			if errors.Is(err, syscall.EINTR) {
				continue
			}
			return -1, time.Time{}, wrapDeadline(ctx, err)
		}
		return fd, deadline, nil
	}
}

func (c *GuardClient) sendAck(fd int, responseID string, deadline time.Time) error {
	if !isCanonicalNonZeroUUID(responseID) {
		return errors.New("ack response id invalid")
	}
	defer syscall.Close(fd)
	return sendLocalPacket(fd, deadline, map[string]any{
		"schema":      localSchema,
		"operation":   "ack",
		"response_id": responseID,
	}, nil)
}

func (c *GuardClient) secretOperation(ctx context.Context, request map[string]any, current *SecretBuffer) (*SecretBuffer, bool, error) {
	grantID, _ := request["grant_id"].(string)
	operation, _ := request["operation"].(string)
	operationID, _ := request["operation_id"].(string)
	materializationID, _ := request["materialization_id"].(string)
	attemptID, _ := request["attempt_id"].(string)
	leaseEpoch, _ := request["lease_epoch"].(int)
	fd, err := current.cloneSealedMemfd("session-operation", maxSecretBytes)
	if err != nil {
		return nil, false, err
	}
	packet, rights, err := c.roundTrip(ctx, request, []int{fd})
	syscall.Close(fd)
	if err != nil {
		return nil, false, err
	}
	defer closeRights(rights)
	defer packet.Close()
	if operation == "claim" && len(rights) == 0 {
		var response struct {
			Schema      string `json:"schema"`
			Operation   string `json:"operation"`
			ResponseID  string `json:"response_id"`
			GrantID     string `json:"grant_id"`
			OperationID string `json:"operation_id"`
			Available   *bool  `json:"available"`
		}
		if err := decodeStrictJSON(packet.payload, &response); err != nil {
			return nil, false, err
		}
		if response.Schema != localSchema || response.Operation != "claim" || response.GrantID != grantID || response.OperationID != operationID || !isCanonicalNonZeroUUID(response.ResponseID) || response.Available == nil || *response.Available {
			return nil, false, errors.New("secret response invalid")
		}
		if err := c.sendAck(packet.fd, response.ResponseID, packet.deadline); err != nil {
			return nil, false, err
		}
		packet.fd = -1
		return nil, false, nil
	}
	if len(rights) != 1 {
		return nil, false, errors.New("secret response rights invalid")
	}
	var payloadSHA256 string
	var responseID string
	switch operation {
	case "claim":
		var response struct {
			Schema        string `json:"schema"`
			Operation     string `json:"operation"`
			ResponseID    string `json:"response_id"`
			GrantID       string `json:"grant_id"`
			OperationID   string `json:"operation_id"`
			PayloadSHA256 string `json:"payload_sha256"`
		}
		if err := decodeStrictJSON(packet.payload, &response); err != nil {
			return nil, false, err
		}
		if response.Schema != localSchema || response.Operation != "claim" || response.GrantID != grantID || response.OperationID != operationID || !isCanonicalNonZeroUUID(response.ResponseID) || !isDigest(response.PayloadSHA256) {
			return nil, false, errors.New("secret response invalid")
		}
		payloadSHA256 = response.PayloadSHA256
		responseID = response.ResponseID
	case "bundle":
		var response struct {
			Schema            string `json:"schema"`
			Operation         string `json:"operation"`
			ResponseID        string `json:"response_id"`
			GrantID           string `json:"grant_id"`
			OperationID       string `json:"operation_id"`
			MaterializationID string `json:"materialization_id"`
			AttemptID         string `json:"attempt_id"`
			LeaseEpoch        int    `json:"lease_epoch"`
			PayloadSHA256     string `json:"payload_sha256"`
		}
		if err := decodeStrictJSON(packet.payload, &response); err != nil {
			return nil, false, err
		}
		if response.Schema != localSchema || response.Operation != "bundle" || response.GrantID != grantID || response.OperationID != operationID || response.MaterializationID != materializationID || response.AttemptID != attemptID || response.LeaseEpoch != leaseEpoch || !isCanonicalNonZeroUUID(response.ResponseID) || !isDigest(response.PayloadSHA256) {
			return nil, false, errors.New("secret response invalid")
		}
		payloadSHA256 = response.PayloadSHA256
		responseID = response.ResponseID
	case "registry-credential":
		component, _ := request["component"].(string)
		var response struct {
			Schema            string `json:"schema"`
			Operation         string `json:"operation"`
			ResponseID        string `json:"response_id"`
			GrantID           string `json:"grant_id"`
			OperationID       string `json:"operation_id"`
			MaterializationID string `json:"materialization_id"`
			AttemptID         string `json:"attempt_id"`
			LeaseEpoch        int    `json:"lease_epoch"`
			Component         string `json:"component"`
			PayloadSHA256     string `json:"payload_sha256"`
		}
		if err := decodeStrictJSON(packet.payload, &response); err != nil {
			return nil, false, err
		}
		if response.Schema != localSchema || response.Operation != "registry-credential" || response.GrantID != grantID || response.OperationID != operationID || response.MaterializationID != materializationID || response.AttemptID != attemptID || response.LeaseEpoch != leaseEpoch || response.Component != component || !isCanonicalNonZeroUUID(response.ResponseID) || !isDigest(response.PayloadSHA256) {
			return nil, false, errors.New("secret response invalid")
		}
		payloadSHA256 = response.PayloadSHA256
		responseID = response.ResponseID
	default:
		return nil, false, errors.New("secret operation invalid")
	}
	buffer, err := NewSecretBuffer(rights[0], 8*1024*1024)
	rights[0] = -1
	if err != nil {
		return nil, false, err
	}
	if secretSHA256(buffer.data) != payloadSHA256 {
		buffer.Close()
		return nil, false, errors.New("secret payload digest mismatch")
	}
	if err := c.sendAck(packet.fd, responseID, packet.deadline); err != nil {
		buffer.Close()
		return nil, false, err
	}
	packet.fd = -1
	return buffer, true, nil
}

func (c *GuardClient) leaseOperation(ctx context.Context, operation string, grantID string, operationID string, materializationID string, attemptID string, leaseEpoch int, failureKind string, current *SecretBuffer) (*LeaseResponse, error) {
	request := map[string]any{
		"schema":             localSchema,
		"operation":          operation,
		"grant_id":           grantID,
		"operation_id":       operationID,
		"materialization_id": materializationID,
		"attempt_id":         attemptID,
		"lease_epoch":        leaseEpoch,
	}
	if failureKind != "" {
		request["failure_kind"] = failureKind
	}
	fd, err := current.cloneSealedMemfd("session-lease", maxSecretBytes)
	if err != nil {
		return nil, err
	}
	packet, rights, err := c.roundTrip(ctx, request, []int{fd})
	syscall.Close(fd)
	if err != nil {
		return nil, err
	}
	defer closeRights(rights)
	defer packet.Close()
	if len(rights) != 0 {
		return nil, errors.New("lease response should not carry rights")
	}
	var response LeaseResponse
	if err := decodeStrictJSON(packet.payload, &response); err != nil {
		return nil, err
	}
	if response.Operation != operation || response.GrantID != grantID || response.OperationID != operationID || response.MaterializationID != materializationID || response.AttemptID != attemptID || response.LeaseEpoch != leaseEpoch || !isCanonicalNonZeroUUID(response.ResponseID) {
		return nil, errors.New("lease response invalid")
	}
	if err := c.sendAck(packet.fd, response.ResponseID, packet.deadline); err != nil {
		return nil, err
	}
	packet.fd = -1
	return &response, nil
}

func sendLocalPacket(fd int, deadline time.Time, request map[string]any, rights []int) error {
	payload, err := encodeCanonicalJSON(request)
	if err != nil {
		return err
	}
	var oob []byte
	if len(rights) > 0 {
		oob = append(oob, syscall.UnixRights(rights...)...)
	}
	written, err := sendmsgNWithRetry(fd, deadline, payload, oob, nil, 0)
	if err != nil {
		return err
	}
	if written != len(payload) {
		return errors.New("short local send")
	}
	return nil
}

func receiveLocalPacket(fd int, deadline time.Time, maximum int) ([]byte, []int, *syscall.Ucred, int, error) {
	payload := make([]byte, maximum)
	oob := make([]byte, syscall.CmsgSpace(4*4)+syscall.CmsgSpace(syscall.SizeofUcred))
	n, oobn, flags, _, err := recvmsgWithRetry(fd, deadline, payload, oob, syscall.MSG_CMSG_CLOEXEC)
	if err != nil {
		return nil, nil, nil, 0, err
	}
	messages, err := syscall.ParseSocketControlMessage(oob[:oobn])
	if err != nil {
		return nil, nil, nil, 0, err
	}
	var rights []int
	var credentials *syscall.Ucred
	for _, message := range messages {
		switch {
		case message.Header.Level == syscall.SOL_SOCKET && message.Header.Type == syscall.SCM_RIGHTS:
			parsed, err := syscall.ParseUnixRights(&message)
			if err != nil {
				closeRights(rights)
				return nil, nil, nil, 0, err
			}
			for _, descriptor := range parsed {
				flags, err := fcntlInt(descriptor, syscall.F_GETFD, 0)
				if err != nil || flags&syscall.FD_CLOEXEC == 0 {
					closeRights(append(rights, parsed...))
					return nil, nil, nil, 0, errors.New("received descriptor missing cloexec")
				}
			}
			rights = append(rights, parsed...)
		case message.Header.Level == syscall.SOL_SOCKET && message.Header.Type == syscall.SCM_CREDENTIALS:
			parsed, err := syscall.ParseUnixCredentials(&message)
			if err != nil {
				closeRights(rights)
				return nil, nil, nil, 0, err
			}
			credentials = parsed
		}
	}
	if n == 0 {
		closeRights(rights)
		return nil, nil, nil, flags, errors.New("empty local packet")
	}
	if credentials == nil {
		closeRights(rights)
		return nil, nil, nil, flags, errors.New("peer credentials missing")
	}
	return payload[:n], rights, credentials, flags, nil
}

func encodeCanonicalJSON(value any) ([]byte, error) {
	return json.Marshal(value)
}

func decodeStrictJSON(payload []byte, target any) error {
	if err := rejectDuplicateJSONKeys(payload); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	return expectJSONEOF(decoder)
}

func closeRights(rights []int) {
	for _, descriptor := range rights {
		if descriptor >= 0 {
			syscall.Close(descriptor)
		}
	}
}

func validateGuardPeerCredentials(credentials *syscall.Ucred) error {
	if credentials == nil || credentials.Pid <= 0 {
		return errors.New("peer credentials missing")
	}
	if credentials.Uid != requiredGuardUID {
		return errors.New("guard peer credentials invalid")
	}
	return nil
}

func validateDirectoryDescriptor(fd int) (*syscall.Stat_t, error) {
	var statValue syscall.Stat_t
	if err := syscall.Fstat(fd, &statValue); err != nil {
		return nil, err
	}
	if statValue.Mode&syscall.S_IFMT != syscall.S_IFDIR {
		return nil, errors.New("received descriptor is not directory")
	}
	return &statValue, nil
}

func applySocketDeadline(fd int, deadline time.Time) error {
	timeout := time.Until(deadline)
	if timeout <= 0 {
		timeout = time.Nanosecond
	}
	timeval := syscall.NsecToTimeval(timeout.Nanoseconds())
	if err := syscall.SetsockoptTimeval(fd, syscall.SOL_SOCKET, syscall.SO_RCVTIMEO, &timeval); err != nil {
		return err
	}
	return syscall.SetsockoptTimeval(fd, syscall.SOL_SOCKET, syscall.SO_SNDTIMEO, &timeval)
}

func wrapDeadline(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EWOULDBLOCK) {
		return context.DeadlineExceeded
	}
	return err
}

func deadlineExceeded(deadline time.Time) bool {
	return !time.Now().Before(deadline)
}

func sendmsgNWithRetry(fd int, deadline time.Time, payload []byte, oob []byte, to syscall.Sockaddr, flags int) (int, error) {
	for {
		if deadlineExceeded(deadline) {
			return 0, syscall.EAGAIN
		}
		if err := applySocketDeadline(fd, deadline); err != nil {
			return 0, err
		}
		written, err := unixSendmsgN(fd, payload, oob, to, flags)
		if !errors.Is(err, syscall.EINTR) {
			return written, err
		}
	}
}

func recvmsgWithRetry(fd int, deadline time.Time, payload []byte, oob []byte, flags int) (int, int, int, syscall.Sockaddr, error) {
	for {
		if deadlineExceeded(deadline) {
			return 0, 0, 0, nil, syscall.EAGAIN
		}
		if err := applySocketDeadline(fd, deadline); err != nil {
			return 0, 0, 0, nil, err
		}
		n, oobn, recvFlags, sa, err := unixRecvmsg(fd, payload, oob, flags)
		if !errors.Is(err, syscall.EINTR) {
			return n, oobn, recvFlags, sa, err
		}
	}
}
