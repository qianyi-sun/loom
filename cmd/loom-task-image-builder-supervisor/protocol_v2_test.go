package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"syscall"
	"testing"
	"time"
)

const v2ManifestDigest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func protocolV2Evidence(t *testing.T, count int) BaseResolutionEvidence {
	t.Helper()
	bases := make([]string, 0, count)
	for index := 1; index <= count; index++ {
		bases = append(bases, fmt.Sprintf("sha256:%064x", index))
	}
	payload, err := json.Marshal(map[string]any{
		"schema":                "loom.task-image-base-resolution/v1",
		"solve_ref":             "solve_1-abc",
		"platform":              "linux/arm64",
		"output_digest":         v2ManifestDigest,
		"observed_base_digests": bases,
	})
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := parseBaseResolutionRecord(payload)
	if err != nil {
		t.Fatal(err)
	}
	return evidence
}

func protocolV2Request(evidence BaseResolutionEvidence) PublicationCandidateV2Request {
	return PublicationCandidateV2Request{
		PublicationCandidateRequest: PublicationCandidateRequest{
			GrantID:                 "11111111-1111-4111-8111-111111111111",
			OperationID:             "22222222-2222-4222-8222-222222222222",
			CredentialID:            "66666666-6666-4666-8666-666666666666",
			CredentialGeneration:    3,
			SessionID:               "33333333-3333-4333-8333-333333333333",
			SessionGeneration:       2,
			MaterializationID:       "44444444-4444-4444-8444-444444444444",
			AttemptID:               "55555555-5555-4555-8555-555555555555",
			AttemptNumber:           11,
			LeaseEpoch:              7,
			BuilderID:               "rootless:33333333333343338333333333333333",
			Component:               "task",
			ManifestDigest:          v2ManifestDigest,
			ManifestSize:            1234,
			OCIFileSHA256:           strings.Repeat("b", 64),
			OCIFileSize:             5678,
			Platform:                "linux/arm64",
			AuthorityResponseSHA256: strings.Repeat("c", 64),
		},
		BaseResolution: evidence,
	}
}

func protocolV2Response(t *testing.T, evidenceJSON string) []byte {
	t.Helper()
	if !json.Valid([]byte(evidenceJSON)) {
		t.Fatal("invalid evidence fixture JSON")
	}
	return []byte(`{"schema":"` + localSchema + `","operation":"publication-candidate-v2","response_id":"bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb","grant_id":"11111111-1111-4111-8111-111111111111","candidate_id":"cccccccc-cccc-4ccc-8ccc-cccccccccccc","operation_id":"22222222-2222-4222-8222-222222222222","credential_id":"66666666-6666-4666-8666-666666666666","credential_generation":3,"session_id":"33333333-3333-4333-8333-333333333333","session_generation":2,"materialization_id":"44444444-4444-4444-8444-444444444444","attempt_id":"55555555-5555-4555-8555-555555555555","attempt_number":11,"lease_epoch":7,"builder_id":"rootless:33333333333343338333333333333333","component":"task","manifest_digest":"` + v2ManifestDigest + `","manifest_size":1234,"oci_file_sha256":"` + strings.Repeat("b", 64) + `","oci_file_size":5678,"platform":"linux/arm64","recorded_at":"2026-09-03T12:00:01Z","authority_response_sha256":"` + strings.Repeat("c", 64) + `","base_resolution":` + evidenceJSON + `}`)
}

// Break caught: the V2 client drops evidence, uses the V1 operation, or fails
// to accept semantically equal acknowledgement key order.
func TestGuardClientPublicationCandidateV2HandsOffCompleteEvidence(t *testing.T) {
	useTestProtocolPolicy(t)
	for _, count := range []int{0, 1, 128} {
		t.Run(fmt.Sprintf("observations_%d", count), func(t *testing.T) {
			evidence := protocolV2Evidence(t, count)
			socketPath := testSocketPath(t)
			server := startSeqpacketServer(t, socketPath, func(connFD int) {
				payload, rights, _, flags := receiveSeqpacket(t, connFD, 32768)
				if flags&(syscall.MSG_TRUNC|syscall.MSG_CTRUNC) != 0 || len(rights) != 1 {
					t.Fatalf("request flags=%d rights=%d", flags, len(rights))
				}
				closeRights(rights)
				var request map[string]json.RawMessage
				if err := json.Unmarshal(payload, &request); err != nil {
					t.Fatal(err)
				}
				if string(request["operation"]) != `"publication-candidate-v2"` {
					t.Fatalf("operation = %s", request["operation"])
				}
				if string(request["base_resolution"]) != evidence.JSON() {
					t.Fatalf("base_resolution = %s, want %s", request["base_resolution"], evidence.JSON())
				}
				if count == 128 && len(payload) <= 4096 {
					t.Fatalf("128-observation request size = %d, want >4096", len(payload))
				}
				var record map[string]json.RawMessage
				if err := json.Unmarshal([]byte(evidence.JSON()), &record); err != nil {
					t.Fatal(err)
				}
				reordered := `{"observed_base_digests":` + string(record["observed_base_digests"]) + `,"output_digest":` + string(record["output_digest"]) + `,"platform":` + string(record["platform"]) + `,"solve_ref":` + string(record["solve_ref"]) + `,"schema":` + string(record["schema"]) + `}`
				sendSeqpacket(t, connFD, protocolV2Response(t, reordered), nil)
				ackPayload, ackRights, _, _ := receiveSeqpacket(t, connFD, 32768)
				closeRights(ackRights)
				assertExactJSON(t, ackPayload, `{"operation":"ack","response_id":"bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb","schema":"`+localSchema+`"}`)
			})

			client := NewGuardClient(socketPath, 32768, 2*time.Second)
			current := mustSessionEnvelope(t, 1, "sentinel-current")
			defer current.Secret.Close()
			ack, err := client.PublicationCandidateV2(context.Background(), protocolV2Request(evidence), current.Secret)
			server.Close()
			if err != nil {
				t.Fatalf("PublicationCandidateV2() error = %v", err)
			}
			if ack == nil || ack.CandidateID != "cccccccc-cccc-4ccc-8ccc-cccccccccccc" || ack.BaseResolution != evidence {
				t.Fatalf("ack = %#v, want complete evidence", ack)
			}
		})
	}
}

// Break caught: malformed or substituted V2 evidence is ACKed based on its own
// claims instead of the frozen request evidence.
func TestGuardClientPublicationCandidateV2RejectsInexactAcknowledgementWithoutAck(t *testing.T) {
	useTestProtocolPolicy(t)
	evidence := protocolV2Evidence(t, 1)
	valid := string(protocolV2Response(t, evidence.JSON()))
	tests := map[string]func(*testing.T) ([]byte, []int){
		"missing evidence": func(*testing.T) ([]byte, []int) {
			return []byte(strings.Replace(valid, `,"base_resolution":`+evidence.JSON(), ``, 1)), nil
		},
		"null evidence": func(*testing.T) ([]byte, []int) {
			return []byte(strings.Replace(valid, evidence.JSON(), `null`, 1)), nil
		},
		"unknown evidence field": func(*testing.T) ([]byte, []int) {
			return []byte(strings.Replace(valid, evidence.JSON(), strings.Replace(evidence.JSON(), `}`, `,"unknown":true}`, 1), 1)), nil
		},
		"duplicate evidence field": func(*testing.T) ([]byte, []int) {
			return []byte(strings.Replace(valid, `"solve_ref":"solve_1-abc"`, `"solve_ref":"old","solve_ref":"solve_1-abc"`, 1)), nil
		},
		"substituted solve": func(*testing.T) ([]byte, []int) {
			return []byte(strings.Replace(valid, `solve_1-abc`, `other-solve`, 1)), nil
		},
		"substituted platform": func(*testing.T) ([]byte, []int) {
			return []byte(strings.Replace(valid, `"platform":"linux/arm64"`, `"platform":"linux/amd64"`, 1)), nil
		},
		"substituted root": func(*testing.T) ([]byte, []int) {
			return []byte(strings.Replace(valid, `"output_digest":"`+v2ManifestDigest+`"`, `"output_digest":"sha256:`+strings.Repeat("d", 64)+`"`, 1)), nil
		},
		"substituted observations": func(*testing.T) ([]byte, []int) {
			return []byte(strings.Replace(valid, fmt.Sprintf("sha256:%064x", 1), fmt.Sprintf("sha256:%064x", 2), 1)), nil
		},
		"wrong operation": func(*testing.T) ([]byte, []int) {
			return []byte(strings.Replace(valid, `publication-candidate-v2`, `publication-candidate`, 1)), nil
		},
		"wrong version": func(*testing.T) ([]byte, []int) {
			return []byte(strings.Replace(valid, localSchema, `loom.task-image-builder-guard-local/v2`, 1)), nil
		},
		"unknown envelope field": func(*testing.T) ([]byte, []int) {
			return []byte(strings.Replace(valid, `{`, `{"unexpected":true,`, 1)), nil
		},
		"malformed envelope": func(*testing.T) ([]byte, []int) { return []byte(valid[:len(valid)-1]), nil },
		"duplicate envelope field": func(*testing.T) ([]byte, []int) {
			return []byte(strings.Replace(valid, `"grant_id":`, `"grant_id":"old","grant_id":`, 1)), nil
		},
		"unsolicited rights": func(t *testing.T) ([]byte, []int) {
			return []byte(valid), []int{createMemfdFixture(t, "unexpected-v2-right", []byte(`{"private":"sentinel"}`), requiredMemfdSeals, true)}
		},
	}
	for name, response := range tests {
		t.Run(name, func(t *testing.T) {
			socketPath := testSocketPath(t)
			server := startSeqpacketServer(t, socketPath, func(connFD int) {
				_, requestRights, _, _ := receiveSeqpacket(t, connFD, 32768)
				closeRights(requestRights)
				payload, rights := response(t)
				sendSeqpacket(t, connFD, payload, rights)
				closeRights(rights)
				if tryReceiveSeqpacket(connFD, 32768, 50*time.Millisecond) {
					t.Fatal("invalid V2 acknowledgement received transport ACK")
				}
			})
			current := mustSessionEnvelope(t, 1, "sentinel-current")
			defer current.Secret.Close()
			client := NewGuardClient(socketPath, 32768, time.Second)
			ack, err := client.PublicationCandidateV2(context.Background(), protocolV2Request(evidence), current.Secret)
			server.Close()
			if err == nil || ack != nil {
				t.Fatalf("ack=%#v err=%v, want rejection", ack, err)
			}
		})
	}
}

// Break caught: the client sends invalid/substituted evidence or a packet that
// exceeds its configured outbound cap.
func TestGuardClientPublicationCandidateV2RejectsEvidenceBeforeTransport(t *testing.T) {
	useTestProtocolPolicy(t)
	originalConnect := unixConnect
	connectCalled := false
	unixConnect = func(int, syscall.Sockaddr) error {
		connectCalled = true
		return errors.New("unexpected connection")
	}
	t.Cleanup(func() { unixConnect = originalConnect })
	current := mustSessionEnvelope(t, 1, "sentinel-current")
	defer current.Secret.Close()
	valid := protocolV2Evidence(t, 0)
	for name, request := range map[string]PublicationCandidateV2Request{
		"zero":      protocolV2Request(BaseResolutionEvidence{}),
		"malformed": protocolV2Request(BaseResolutionEvidence{json: `{"schema":"private-malformed"}`}),
		"substituted platform": func() PublicationCandidateV2Request {
			r := protocolV2Request(valid)
			r.Platform = "linux/amd64"
			return r
		}(),
		"substituted root": func() PublicationCandidateV2Request {
			r := protocolV2Request(valid)
			r.ManifestDigest = "sha256:" + strings.Repeat("d", 64)
			return r
		}(),
	} {
		t.Run(name, func(t *testing.T) {
			connectCalled = false
			if ack, err := NewGuardClient("unused", 32768, time.Second).PublicationCandidateV2(context.Background(), request, current.Secret); err == nil || ack != nil {
				t.Fatalf("ack=%#v err=%v, want rejection", ack, err)
			}
			if connectCalled {
				t.Fatal("invalid evidence reached transport")
			}
		})
	}
}

func TestGuardClientPublicationCandidateV2HonorsOutboundPacketCap(t *testing.T) {
	useTestProtocolPolicy(t)
	evidence := protocolV2Evidence(t, 128)
	originalConnect := unixConnect
	connectCalled := false
	unixConnect = func(int, syscall.Sockaddr) error {
		connectCalled = true
		return errors.New("unexpected connection")
	}
	t.Cleanup(func() { unixConnect = originalConnect })
	current := mustSessionEnvelope(t, 1, "sentinel-current")
	defer current.Secret.Close()
	ack, err := NewGuardClient("unused", 4096, time.Second).PublicationCandidateV2(context.Background(), protocolV2Request(evidence), current.Secret)
	if err == nil || ack != nil {
		t.Fatalf("ack=%#v err=%v, want bounded rejection", ack, err)
	}
	if connectCalled {
		t.Fatal("oversized request reached transport")
	}
}

func TestGuardClientPublicationCandidateV2HonorsInboundPacketCap(t *testing.T) {
	useTestProtocolPolicy(t)
	requestEvidence := protocolV2Evidence(t, 0)
	responseEvidence := protocolV2Evidence(t, 128)
	response := protocolV2Response(t, responseEvidence.JSON())
	if len(response) <= 4096 {
		t.Fatalf("128-observation response size = %d, want >4096", len(response))
	}
	socketPath := testSocketPath(t)
	server := startSeqpacketServer(t, socketPath, func(connFD int) {
		_, rights, _, _ := receiveSeqpacket(t, connFD, 4096)
		closeRights(rights)
		sendSeqpacket(t, connFD, response, nil)
		if tryReceiveSeqpacket(connFD, 4096, 50*time.Millisecond) {
			t.Fatal("truncated response received transport ACK")
		}
	})
	current := mustSessionEnvelope(t, 1, "sentinel-current")
	defer current.Secret.Close()
	ack, err := NewGuardClient(socketPath, 4096, time.Second).PublicationCandidateV2(context.Background(), protocolV2Request(requestEvidence), current.Secret)
	server.Close()
	if err == nil || ack != nil {
		t.Fatalf("ack=%#v err=%v, want bounded rejection", ack, err)
	}
}
