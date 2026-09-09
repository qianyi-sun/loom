package main

import (
	"encoding/json"
	"testing"
	"time"
)

func TestBootstrapExchangePreservesAuthorityReceiptTimestamp(t *testing.T) {
	for _, stamp := range []string{"2026-09-08T12:34:56.123456Z", "2026-09-08T08:34:56.123456-04:00", "2026-09-08T12:34:56Z"} {
		t.Run(stamp, func(t *testing.T) {
			payload, err := json.Marshal(map[string]any{"schema_version": 1, "grant_id": testGrantID, "proof_id": uuidWithTail(1), "proof_sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "bootstrap_token": "loom_tibp_test", "issued_at": stamp, "expires_at": "2026-09-08T12:35:26Z"})
			if err != nil {
				t.Fatal(err)
			}
			bootstrap := &SecretBuffer{data: payload}
			defer bootstrap.Close()
			fd, err := bootstrapExchangeMemfd(testGrantID, uuidWithTail(2), "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", bootstrap)
			if err != nil {
				t.Fatal(err)
			}
			wire, err := NewSecretBuffer(fd, maxSecretBytes)
			if err != nil {
				t.Fatal(err)
			}
			defer wire.Close()
			var request struct {
				ObservedAt string `json:"observed_at"`
			}
			if err := json.Unmarshal(wire.data, &request); err != nil {
				t.Fatal(err)
			}
			issued, err := time.Parse(time.RFC3339Nano, stamp)
			if err != nil {
				t.Fatal(err)
			}
			observed, err := time.Parse(time.RFC3339Nano, request.ObservedAt)
			if err != nil || !observed.Equal(issued) {
				t.Fatalf("exchange observation %q changed receipt timestamp %q", request.ObservedAt, stamp)
			}
		})
	}
}
