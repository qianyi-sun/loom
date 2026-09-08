package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"syscall"
	"testing"
	"time"
)

func publicationLocalResponse(t *testing.T, operation string, payload string) []byte {
	t.Helper()
	wire, err := json.Marshal(map[string]any{
		"schema": localSchema, "operation": operation,
		"response_id":        "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
		"grant_id":           loadPublicationStatusTestVectors(t).Binding.GrantID,
		"publication_status": json.RawMessage(payload),
	})
	if err != nil {
		t.Fatal(err)
	}
	return wire
}

func callPublicationOperation(ctx context.Context, client *GuardClient, operation string, binding publicationStatusBinding, secret *SecretBuffer) (*publicationStatus, error) {
	if operation == "publication-submit" {
		return client.PublicationSubmit(ctx, binding, secret)
	}
	return client.PublicationPoll(ctx, binding, secret)
}

func TestGuardClientPublicationStatusUsesFixedIDsAndSealedCurrentSession(t *testing.T) {
	useTestProtocolPolicy(t)
	vectors := loadPublicationStatusTestVectors(t)
	for _, operation := range []string{"publication-submit", "publication-poll"} {
		for _, vector := range vectors.Statuses {
			t.Run(operation+"/"+vector.Name, func(t *testing.T) {
				current := mustSessionEnvelope(t, 1, "sentinel-current")
				defer current.Secret.Close()
				socketPath := testSocketPath(t)
				server := startSeqpacketServer(t, socketPath, func(connFD int) {
					payload, rights, _, flags := receiveSeqpacket(t, connFD, 4096)
					if flags&(syscall.MSG_TRUNC|syscall.MSG_CTRUNC) != 0 || len(rights) != 1 {
						t.Fatalf("flags=%d rights=%d", flags, len(rights))
					}
					secret, err := NewSecretBuffer(rights[0], maxSecretBytes)
					if err != nil {
						t.Fatal(err)
					}
					if !bytes.Equal(secret.data, current.Secret.data) {
						t.Fatal("current session lost")
					}
					secret.Close()
					expected, err := json.Marshal(map[string]any{
						"schema": localSchema, "operation": operation, "grant_id": vectors.Binding.GrantID,
						"operation_id": vectors.Binding.OperationID, "materialization_id": vectors.Binding.MaterializationID,
						"attempt_id": vectors.Binding.AttemptID, "lease_epoch": vectors.Binding.LeaseEpoch,
					})
					if err != nil || !bytes.Equal(payload, expected) {
						t.Fatal("request fields differ from fixed IDs-only contract")
					}
					sendSeqpacket(t, connFD, publicationLocalResponse(t, operation, vector.Payload), nil)
					ack, ackRights, _, _ := receiveSeqpacket(t, connFD, 4096)
					closeRights(ackRights)
					assertExactJSON(t, ack, `{"operation":"ack","response_id":"bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb","schema":"`+localSchema+`"}`)
				})
				status, err := callPublicationOperation(context.Background(), NewGuardClient(socketPath, 4096, time.Second), operation, vectors.Binding, current.Secret)
				server.Close()
				if err != nil || status == nil || status.State != vector.Name || status.SnapshotSHA256 != vectors.SnapshotSHA256 {
					t.Fatalf("status=%#v err=%v", status, err)
				}
			})
		}
	}
}

func TestGuardClientPublicationStatusRejectsInvalidRepliesWithoutAck(t *testing.T) {
	useTestProtocolPolicy(t)
	vectors := loadPublicationStatusTestVectors(t)
	for _, operation := range []string{"publication-submit", "publication-poll"} {
		valid := publicationLocalResponse(t, operation, publicationStatusVectorByName(t, "completed").Payload)
		tests := map[string][]byte{
			"null status":           mutatePublicationStatusField(t, valid, "publication_status", json.RawMessage(`null`)),
			"missing status":        mutatePublicationStatusField(t, valid, "publication_status", nil),
			"unknown outer":         mutatePublicationStatusField(t, valid, "sentinel-private", json.RawMessage(`true`)),
			"wrong operation":       mutatePublicationStatusField(t, valid, "operation", json.RawMessage(`"heartbeat"`)),
			"wrong grant":           mutatePublicationStatusField(t, valid, "grant_id", json.RawMessage(`"11111111-1111-4111-8111-111111111111"`)),
			"wrong response id":     mutatePublicationStatusField(t, valid, "response_id", json.RawMessage(`"00000000-0000-0000-0000-000000000000"`)),
			"duplicate outer":       bytes.Replace(valid, []byte(`"operation":`), []byte(`"operation":"bad","operation":`), 1),
			"aliased outer":         bytes.Replace(valid, []byte(`"operation":`), []byte(`"OPERATION":`), 1),
			"changed candidate set": bytes.ReplaceAll(valid, []byte(vectors.Binding.CandidateSetSHA256), []byte(strings.Repeat("b", 64))),
			"invalid receipt":       bytes.Replace(valid, []byte(`"worker_generation":9007199254740991`), []byte(`"worker_generation":0`), 1),
			"noncanonical status":   bytes.Replace(valid, []byte(`"state":"completed"`), []byte(`"state": "completed"`), 1),
			"oversized status":      bytes.Replace(valid, []byte(`"state":"completed"`), []byte(`"state":"`+strings.Repeat("x", 4096)+`"`), 1),
			"unexpected rights":     valid,
		}
		for name, wire := range tests {
			t.Run(operation+"/"+name, func(t *testing.T) {
				socketPath := testSocketPath(t)
				server := startSeqpacketServer(t, socketPath, func(connFD int) {
					_, rights, _, _ := receiveSeqpacket(t, connFD, 32768)
					closeRights(rights)
					var unsolicited []int
					if name == "unexpected rights" {
						unsolicited = []int{createMemfdFixture(t, "unexpected-status-right", []byte(`sentinel-private`), requiredMemfdSeals, true)}
					}
					sendSeqpacket(t, connFD, wire, unsolicited)
					closeRights(unsolicited)
					if tryReceiveSeqpacket(connFD, 32768, 50*time.Millisecond) {
						t.Fatal("invalid response was ACKed")
					}
				})
				current := mustSessionEnvelope(t, 1, "sentinel-current")
				defer current.Secret.Close()
				status, err := callPublicationOperation(context.Background(), NewGuardClient(socketPath, 32768, time.Second), operation, vectors.Binding, current.Secret)
				server.Close()
				if err == nil || status != nil || strings.Contains(err.Error(), "sentinel") {
					t.Fatalf("status=%#v error=%v", status, err)
				}
			})
		}
	}
}

func TestGuardClientPublicationStatusRejectsInvalidBindingBeforeTransport(t *testing.T) {
	useTestProtocolPolicy(t)
	original := unixConnect
	connects := 0
	unixConnect = func(int, syscall.Sockaddr) error { connects++; return errors.New("unexpected connect") }
	t.Cleanup(func() { unixConnect = original })
	binding := loadPublicationStatusTestVectors(t).Binding
	binding.CandidateSetSHA256 = ""
	current := mustSessionEnvelope(t, 1, "sentinel-current")
	defer current.Secret.Close()
	for _, operation := range []string{"publication-submit", "publication-poll"} {
		if status, err := callPublicationOperation(context.Background(), NewGuardClient("unused", 4096, time.Second), operation, binding, current.Secret); err == nil || status != nil {
			t.Fatal("invalid binding accepted")
		}
	}
	if connects != 0 {
		t.Fatal("invalid binding reached transport")
	}
}
