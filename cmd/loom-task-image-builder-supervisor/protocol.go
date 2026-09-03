package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"syscall"
	"time"
)

const maxSecretBytes = 64 * 1024

type GuardClient struct {
	socketPath     string
	maxPacketBytes int
	ackTimeout     time.Duration
}

type AllocationCapabilities struct {
	Bootstrap          *SecretBuffer
	JobDirectoryFD     int
	JobDirectoryDevice uint64
	JobDirectoryInode  uint64
	BuildEgressFD      int
	BuildEgressDevice  uint64
	BuildEgressInode   uint64
}

func (a *AllocationCapabilities) Close() {
	if a == nil {
		return
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
	fd, err := bootstrap.cloneSealedMemfd("bootstrap-exchange", maxSecretBytes)
	if err != nil {
		return nil, err
	}
	packet, rights, err := c.roundTrip(ctx, request, []int{fd})
	syscall.Close(fd)
	if err != nil {
		return nil, err
	}
	defer closeRights(rights)
	return c.decodeSessionResponse(packet, rights, grantID)
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
	var response struct {
		Schema     string `json:"schema"`
		Operation  string `json:"operation"`
		ResponseID string `json:"response_id"`
		GrantID    string `json:"grant_id"`
	}
	if err := decodeStrictJSON(packet.payload, &response); err != nil {
		return err
	}
	if response.Schema != localSchema || response.Operation != "finishing" || response.GrantID != grantID {
		return errors.New("finish response invalid")
	}
	return c.sendAck(packet.fd, response.ResponseID, packet.deadline)
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
	session.SessionPublicBindingSHA256 = response.SessionPublicBindingSHA
	if session.Generation != response.SessionGeneration || session.SessionID != response.SessionID || session.GrantID != grantID {
		session.Secret.Close()
		return nil, errors.New("session response binding invalid")
	}
	if err := c.sendAck(packet.fd, response.ResponseID, packet.deadline); err != nil {
		session.Secret.Close()
		return nil, err
	}
	return session, nil
}

type responsePacket struct {
	fd       int
	payload  []byte
	deadline time.Time
}

func (c *GuardClient) roundTrip(ctx context.Context, request map[string]any, rights []int) (*responsePacket, []int, error) {
	fd, deadline, err := c.connect(ctx)
	if err != nil {
		return nil, nil, err
	}
	if err := sendLocalPacket(fd, request, rights); err != nil {
		syscall.Close(fd)
		return nil, nil, err
	}
	payload, receivedRights, _, flags, err := receiveLocalPacket(fd, c.maxPacketBytes)
	if err != nil {
		syscall.Close(fd)
		return nil, nil, wrapDeadline(ctx, err)
	}
	if flags&(syscall.MSG_TRUNC|syscall.MSG_CTRUNC) != 0 {
		closeRights(receivedRights)
		syscall.Close(fd)
		return nil, nil, errors.New("local packet truncated")
	}
	return &responsePacket{fd: fd, payload: payload, deadline: deadline}, receivedRights, nil
}

func (c *GuardClient) connect(ctx context.Context) (int, time.Time, error) {
	fd, err := syscall.Socket(syscall.AF_UNIX, syscall.SOCK_SEQPACKET|syscall.SOCK_CLOEXEC, 0)
	if err != nil {
		return -1, time.Time{}, err
	}
	if err := syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_PASSCRED, 1); err != nil {
		syscall.Close(fd)
		return -1, time.Time{}, err
	}
	deadline := time.Now().Add(c.ackTimeout)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}
	if err := applySocketDeadline(fd, deadline); err != nil {
		syscall.Close(fd)
		return -1, time.Time{}, err
	}
	if err := syscall.Connect(fd, &syscall.SockaddrUnix{Name: c.socketPath}); err != nil {
		syscall.Close(fd)
		return -1, time.Time{}, wrapDeadline(ctx, err)
	}
	return fd, deadline, nil
}

func (c *GuardClient) sendAck(fd int, responseID string, deadline time.Time) error {
	if !isCanonicalNonZeroUUID(responseID) {
		return errors.New("ack response id invalid")
	}
	if err := applySocketDeadline(fd, deadline); err != nil {
		return err
	}
	defer syscall.Close(fd)
	return sendLocalPacket(fd, map[string]any{
		"schema":      localSchema,
		"operation":   "ack",
		"response_id": responseID,
	}, nil)
}

func (c *GuardClient) secretOperation(ctx context.Context, request map[string]any, current *SecretBuffer) (*SecretBuffer, bool, error) {
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
	var response struct {
		Schema     string `json:"schema"`
		Operation  string `json:"operation"`
		ResponseID string `json:"response_id"`
		GrantID    string `json:"grant_id"`
		Available  bool   `json:"available"`
	}
	if err := decodeStrictJSON(packet.payload, &response); err != nil {
		return nil, false, err
	}
	if response.Schema != localSchema || !isCanonicalNonZeroUUID(response.ResponseID) {
		return nil, false, errors.New("secret response invalid")
	}
	if len(rights) == 0 && response.Operation == "claim" && !response.Available {
		return nil, false, c.sendAck(packet.fd, response.ResponseID, packet.deadline)
	}
	if len(rights) != 1 {
		return nil, false, errors.New("secret response rights invalid")
	}
	buffer, err := NewSecretBuffer(rights[0], 8*1024*1024)
	rights[0] = -1
	if err != nil {
		return nil, false, err
	}
	if err := c.sendAck(packet.fd, response.ResponseID, packet.deadline); err != nil {
		buffer.Close()
		return nil, false, err
	}
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
	if len(rights) != 0 {
		return nil, errors.New("lease response should not carry rights")
	}
	var response LeaseResponse
	if err := decodeStrictJSON(packet.payload, &response); err != nil {
		return nil, err
	}
	if response.Operation != operation || response.GrantID != grantID || !isCanonicalNonZeroUUID(response.ResponseID) {
		return nil, errors.New("lease response invalid")
	}
	if err := c.sendAck(packet.fd, response.ResponseID, packet.deadline); err != nil {
		return nil, err
	}
	return &response, nil
}

func sendLocalPacket(fd int, request map[string]any, rights []int) error {
	payload, err := encodeCanonicalJSON(request)
	if err != nil {
		return err
	}
	credentials := syscall.UnixCredentials(&syscall.Ucred{
		Pid: int32(os.Getpid()),
		Uid: uint32(os.Geteuid()),
		Gid: uint32(os.Getegid()),
	})
	oob := make([]byte, 0, len(credentials)+128)
	oob = append(oob, credentials...)
	if len(rights) > 0 {
		oob = append(oob, syscall.UnixRights(rights...)...)
	}
	written, err := syscall.SendmsgN(fd, payload, oob, nil, 0)
	if err != nil {
		return err
	}
	if written != len(payload) {
		return errors.New("short local send")
	}
	return nil
}

func receiveLocalPacket(fd int, maximum int) ([]byte, []int, *syscall.Ucred, int, error) {
	payload := make([]byte, maximum)
	oob := make([]byte, syscall.CmsgSpace(4*4)+syscall.CmsgSpace(syscall.SizeofUcred))
	n, oobn, flags, _, err := syscall.Recvmsg(fd, payload, oob, syscall.MSG_CMSG_CLOEXEC)
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
	return nil
}

func closeRights(rights []int) {
	for _, descriptor := range rights {
		if descriptor >= 0 {
			syscall.Close(descriptor)
		}
	}
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
