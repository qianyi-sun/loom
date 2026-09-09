package main

import (
	"context"
	"os"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// TestGoPublicationCandidateV2PythonHandoffHelper is executed by the Python
// integration fixture from a prebuilt Go test binary. Ordinary Go suites skip
// it because their pinned container intentionally has no Python runtime.
func TestGoPublicationCandidateV2PythonHandoffHelper(t *testing.T) {
	if os.Getenv("LOOM_GO_V2_HELPER") != "1" {
		t.Skip("cross-language helper is driven by the Python integration fixture")
	}
	useTestProtocolPolicy(t)
	socketPath := os.Getenv("LOOM_GO_V2_SOCKET")
	fd, err := strconv.Atoi(os.Getenv("LOOM_GO_V2_SESSION_FD"))
	if err != nil || socketPath == "" {
		t.Fatal("cross-language helper environment invalid")
	}
	// pass_fds must clear close-on-exec for inheritance; restore the production
	// descriptor invariant immediately after exec and before parsing it.
	syscall.CloseOnExec(fd)
	current, err := NewSecretBuffer(fd, maxSecretBytes)
	if err != nil {
		t.Fatalf("NewSecretBuffer() error = %v", err)
	}
	defer current.Close()
	evidence, err := parseBaseResolutionRecord([]byte(`{"schema":"loom.task-image-base-resolution/v1","solve_ref":"solve-python-handoff","platform":"linux/arm64","output_digest":"sha256:` + strings.Repeat("1", 64) + `","observed_base_digests":["sha256:` + strings.Repeat("3", 64) + `"]}`))
	if err != nil {
		t.Fatal(err)
	}
	request := PublicationCandidateV2Request{
		PublicationCandidateRequest: PublicationCandidateRequest{
			GrantID:                 "11111111-1111-1111-1111-111111111111",
			OperationID:             "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
			CredentialID:            "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee",
			CredentialGeneration:    1,
			SessionID:               "77777777-7777-7777-7777-777777777777",
			SessionGeneration:       1,
			MaterializationID:       "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
			AttemptID:               "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
			AttemptNumber:           2,
			LeaseEpoch:              1,
			BuilderID:               "rootless:" + strings.Repeat("a", 32),
			Component:               "task",
			ManifestDigest:          "sha256:" + strings.Repeat("1", 64),
			ManifestSize:            512,
			OCIFileSHA256:           strings.Repeat("2", 64),
			OCIFileSize:             4096,
			Platform:                "linux/arm64",
			AuthorityResponseSHA256: strings.Repeat("4", 64),
		},
		BaseResolution: evidence,
	}
	ack, err := NewGuardClient(socketPath, 32768, 2*time.Second).PublicationCandidateV2(context.Background(), request, current)
	if err != nil {
		t.Fatalf("PublicationCandidateV2() error = %v", err)
	}
	if ack == nil || ack.BaseResolution != evidence || ack.CandidateID != "ffffffff-ffff-4fff-8fff-ffffffffffff" ||
		ack.OperationID != request.OperationID || ack.AuthorityResponseSHA256 != request.AuthorityResponseSHA256 {
		t.Fatalf("ack = %#v, want exact Python V2 acknowledgement", ack)
	}
}

// This runs against the real Python GuardService under the same required CI
// helper policy as V2 candidate handoff; ordinary pinned Go suites have no Python.
func TestGoPublicationStatusPythonHandoffHelper(t *testing.T) {
	if os.Getenv("LOOM_GO_V2_HELPER") != "1" {
		t.Skip("cross-language helper is driven by the Python integration fixture")
	}
	useTestProtocolPolicy(t)
	socketPath := os.Getenv("LOOM_GO_V2_SOCKET")
	fd, err := strconv.Atoi(os.Getenv("LOOM_GO_V2_SESSION_FD"))
	if err != nil || socketPath == "" {
		t.Fatal("cross-language helper environment invalid")
	}
	syscall.CloseOnExec(fd)
	current, err := NewSecretBuffer(fd, maxSecretBytes)
	if err != nil {
		t.Fatal("current session unavailable")
	}
	defer current.Close()
	binding := publicationStatusBinding{
		GrantID:           "11111111-1111-1111-1111-111111111111",
		OperationID:       "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
		MaterializationID: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
		AttemptID:         "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
		LeaseEpoch:        1, CandidateSetSHA256: strings.Repeat("2", 64), ComponentCount: 128,
	}
	client := NewGuardClient(socketPath, 4096, 2*time.Second)
	submitted, err := client.PublicationSubmit(context.Background(), binding, current)
	if err != nil || submitted == nil || submitted.State != "completed" || submitted.Receipt == nil {
		t.Fatalf("submit failed: %v", err)
	}
	binding.PinnedSnapshotSHA256 = submitted.SnapshotSHA256
	polled, err := client.PublicationPoll(context.Background(), binding, current)
	if err != nil || polled == nil || polled.Receipt == nil || *polled.Receipt != *submitted.Receipt {
		t.Fatalf("poll receipt changed: %v", err)
	}
	if polled.SnapshotSHA256 != strings.Repeat("1", 64) || polled.Receipt.PublicationSetSHA256 != strings.Repeat("3", 64) {
		t.Fatal("status binding differs from Python fixture")
	}
}

// Exercises candidate recording and the composed controller over real sealed
// descriptors/ACKs. Python owns the independently derived status/receipt fixture.
func TestGoPublicationLifecyclePythonHandoffHelper(t *testing.T) {
	if os.Getenv("LOOM_GO_V2_HELPER") != "1" {
		t.Skip("cross-language helper is driven by the Python integration fixture")
	}
	useTestProtocolPolicy(t)
	socketPath := os.Getenv("LOOM_GO_V2_SOCKET")
	fd, err := strconv.Atoi(os.Getenv("LOOM_GO_V2_SESSION_FD"))
	if err != nil || socketPath == "" {
		t.Fatal("cross-language helper environment invalid")
	}
	syscall.CloseOnExec(fd)
	current, err := NewSecretBuffer(fd, maxSecretBytes)
	if err != nil {
		t.Fatal("session unavailable")
	}
	defer current.Close()
	previousArch := runtimeGOARCH
	runtimeGOARCH = func() string { return "arm64" }
	t.Cleanup(func() { runtimeGOARCH = previousArch })
	envelope, err := parseSessionEnvelope(current)
	if err != nil {
		t.Fatal("session invalid")
	}
	client := NewGuardClient(socketPath, 32768, 2*time.Second)
	manager := NewSessionManager(envelope.GrantID, envelope, client)
	defer manager.Close()
	clock := &publicationTestClock{manualClock: newManualClock(envelope.IssuedAt.Add(time.Second)), armed: make(chan time.Duration, 100)}
	set := handoffBuiltSet()
	set.GrantID, set.MaterializationID, set.AttemptID = envelope.GrantID, "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb", "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
	set.Components = set.Components[:1]
	component := set.Components[0]
	builder := "rootless:" + strings.Repeat("a", 32)
	p := &publicationLifecycle{clock: clock, session: manager, guard: client, set: set, builderID: builder, leaseExpiresAt: clock.Now().Add(time.Minute), timeout: time.Hour}
	p.upload = func(ctx context.Context) ([]PublicationCandidateV2Acknowledgement, error) {
		request := PublicationCandidateV2Request{PublicationCandidateRequest: PublicationCandidateRequest{
			GrantID: set.GrantID, OperationID: "dddddddd-dddd-4ddd-8ddd-dddddddddddd", CredentialID: "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee", CredentialGeneration: 1,
			SessionID: envelope.SessionID, SessionGeneration: envelope.Generation, MaterializationID: set.MaterializationID, AttemptID: set.AttemptID, AttemptNumber: 2, LeaseEpoch: 1, BuilderID: builder,
			Component: component.Name, ManifestDigest: component.Output.TopLevelDigest, ManifestSize: component.Output.ManifestSize, OCIFileSHA256: component.Output.FileSHA256, OCIFileSize: component.Output.SizeBytes, Platform: "linux/arm64"}, BaseResolution: component.BaseResolution}
		var ack *PublicationCandidateV2Acknowledgement
		err := manager.WithCurrent(func(secret *SecretBuffer) error {
			var err error
			ack, err = client.PublicationCandidateV2(ctx, request, secret)
			return err
		})
		if err != nil {
			return nil, err
		}
		return []PublicationCandidateV2Acknowledgement{*ack}, nil
	}
	receipt, err := drivePublication(t, p, clock)
	if err != nil || receipt == nil || receipt.ComponentCount != 1 || receipt.PublicationSetSHA256 != strings.Repeat("3", 64) {
		t.Fatalf("lifecycle receipt unavailable: %v", err)
	}
}
