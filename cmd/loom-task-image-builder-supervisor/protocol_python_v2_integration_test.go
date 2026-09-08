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
