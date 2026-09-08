package main

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"testing"
)

type publicationStatusTestVectors struct {
	Schema         string                    `json:"schema"`
	Binding        publicationStatusBinding  `json:"binding"`
	SnapshotSHA256 string                    `json:"snapshot_sha256"`
	Statuses       []publicationStatusVector `json:"statuses"`
}

type publicationStatusVector struct {
	Name     string `json:"name"`
	Payload  string `json:"payload"`
	Expected struct {
		State                string  `json:"state"`
		FailureCode          *string `json:"failure_code"`
		PublicationSetSHA256 *string `json:"publication_set_sha256"`
	} `json:"expected"`
}

func loadPublicationStatusTestVectors(t *testing.T) publicationStatusTestVectors {
	t.Helper()
	payload, err := os.ReadFile("testdata/publication_status_vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var vectors publicationStatusTestVectors
	if err := json.Unmarshal(payload, &vectors); err != nil {
		t.Fatal(err)
	}
	if vectors.Schema != "loom.task-image-publication-status-go-test-vectors/v1" ||
		len(vectors.Statuses) != 4 ||
		vectors.SnapshotSHA256 == "" {
		t.Fatalf("status vectors incomplete: %#v", vectors)
	}
	return vectors
}

func publicationStatusVectorByName(t *testing.T, name string) publicationStatusVector {
	t.Helper()
	for _, vector := range loadPublicationStatusTestVectors(t).Statuses {
		if vector.Name == name {
			return vector
		}
	}
	t.Fatalf("status vector %q missing", name)
	return publicationStatusVector{}
}

func mutatePublicationStatusField(t *testing.T, payload []byte, name string, value json.RawMessage) []byte {
	t.Helper()
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(payload, &fields); err != nil {
		t.Fatal(err)
	}
	if value == nil {
		delete(fields, name)
	} else {
		fields[name] = value
	}
	changed, err := json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	return changed
}

func mutatePublicationStatusReceiptField(t *testing.T, payload []byte, name string, value json.RawMessage) []byte {
	t.Helper()
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(payload, &fields); err != nil {
		t.Fatal(err)
	}
	var receipt map[string]json.RawMessage
	if err := json.Unmarshal(fields["receipt"], &receipt); err != nil {
		t.Fatal(err)
	}
	receipt[name] = value
	encodedReceipt, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	fields["receipt"] = encodedReceipt
	changed, err := json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	return changed
}

func assertPublicationStatusRejected(t *testing.T, payload []byte, binding publicationStatusBinding) {
	t.Helper()
	status, err := parsePublicationStatus(payload, binding)
	if err == nil {
		t.Fatalf("parsePublicationStatus() = %#v, want rejection", status)
	}
	if err.Error() != "publication status invalid" {
		t.Fatalf("parsePublicationStatus() error = %q, want generic error", err)
	}
}

func TestParsePublicationStatusMatchesPythonCanonicalVectors(t *testing.T) {
	vectors := loadPublicationStatusTestVectors(t)
	for _, vector := range vectors.Statuses {
		t.Run(vector.Name, func(t *testing.T) {
			status, err := parsePublicationStatus([]byte(vector.Payload), vectors.Binding)
			if err != nil {
				t.Fatalf("parsePublicationStatus() error = %v", err)
			}
			if status.GrantID != vectors.Binding.GrantID ||
				status.OperationID != vectors.Binding.OperationID ||
				status.MaterializationID != vectors.Binding.MaterializationID ||
				status.AttemptID != vectors.Binding.AttemptID ||
				status.LeaseEpoch != vectors.Binding.LeaseEpoch ||
				status.SnapshotSHA256 != vectors.SnapshotSHA256 ||
				status.CandidateSetSHA256 != vectors.Binding.CandidateSetSHA256 ||
				status.ComponentCount != vectors.Binding.ComponentCount ||
				status.State != vector.Expected.State {
				t.Fatalf("parsed status = %#v, want Python vector %#v", status, vector)
			}
			if vector.Expected.FailureCode == nil {
				if status.FailureCode != "" {
					t.Fatalf("failure code = %q, want omitted", status.FailureCode)
				}
			} else if status.FailureCode != *vector.Expected.FailureCode {
				t.Fatalf("failure code = %q, want %q", status.FailureCode, *vector.Expected.FailureCode)
			}
			if vector.Expected.PublicationSetSHA256 == nil {
				if status.Receipt != nil {
					t.Fatalf("receipt = %#v, want omitted", status.Receipt)
				}
			} else if status.Receipt == nil || status.Receipt.PublicationSetSHA256 != *vector.Expected.PublicationSetSHA256 {
				t.Fatalf("receipt = %#v, want publication set %q", status.Receipt, *vector.Expected.PublicationSetSHA256)
			}
		})
	}
}

// Break caught: polling accepts an initial snapshot but does not bind later
// responses to the authority-derived snapshot established by that response.
func TestParsePublicationStatusPinsInitialSnapshotAndRequiresLaterMatch(t *testing.T) {
	vectors := loadPublicationStatusTestVectors(t)
	queued := publicationStatusVectorByName(t, "queued")
	initial, err := parsePublicationStatus([]byte(queued.Payload), vectors.Binding)
	if err != nil {
		t.Fatal(err)
	}
	if initial.SnapshotSHA256 != vectors.SnapshotSHA256 {
		t.Fatalf("initial snapshot = %q, want %q", initial.SnapshotSHA256, vectors.SnapshotSHA256)
	}

	pinned := vectors.Binding
	pinned.PinnedSnapshotSHA256 = initial.SnapshotSHA256
	running := publicationStatusVectorByName(t, "running")
	if _, err := parsePublicationStatus([]byte(running.Payload), pinned); err != nil {
		t.Fatalf("parsePublicationStatus() pinned match error = %v", err)
	}
	pinned.PinnedSnapshotSHA256 = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	assertPublicationStatusRejected(t, []byte(running.Payload), pinned)
}

func TestParsePublicationStatusRejectsEveryMissingOrNullField(t *testing.T) {
	vectors := loadPublicationStatusTestVectors(t)
	for _, vector := range vectors.Statuses {
		t.Run(vector.Name, func(t *testing.T) {
			payload := []byte(vector.Payload)
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(payload, &fields); err != nil {
				t.Fatal(err)
			}
			for name := range fields {
				for _, mutation := range []string{"missing", "null"} {
					t.Run(name+"_"+mutation, func(t *testing.T) {
						var value json.RawMessage
						if mutation == "null" {
							value = json.RawMessage("null")
						}
						assertPublicationStatusRejected(t, mutatePublicationStatusField(t, payload, name, value), vectors.Binding)
					})
				}
			}
		})
	}
	queued := publicationStatusVectorByName(t, "queued")
	for _, name := range []string{"receipt", "failure_code"} {
		t.Run("queued_optional_"+name+"_null", func(t *testing.T) {
			assertPublicationStatusRejected(t, mutatePublicationStatusField(t, []byte(queued.Payload), name, json.RawMessage("null")), vectors.Binding)
		})
	}
}

func TestParsePublicationStatusRejectsInvalidOrNoncanonicalWire(t *testing.T) {
	vectors := loadPublicationStatusTestVectors(t)
	queued := []byte(publicationStatusVectorByName(t, "queued").Payload)
	tests := map[string][]byte{
		"unknown field":    mutatePublicationStatusField(t, queued, "credential", json.RawMessage(`"must-not-leak"`)),
		"case alias":       bytes.Replace(queued, []byte(`"grant_id"`), []byte(`"Grant_Id"`), 1),
		"duplicate field":  append([]byte(`{"attempt_id":"00000000-0000-0000-0000-0000000001f7",`), queued[1:]...),
		"trailing spacing": append(append([]byte(nil), queued...), ' '),
		"leading spacing":  append([]byte{' '}, queued...),
		"oversize":         append(append([]byte(nil), queued...), bytes.Repeat([]byte{' '}, maxPublicationStatusBytes-len(queued)+1)...),
		"invalid UTF-8":    append(append([]byte(nil), queued...), 0xff),
		"wrong schema":     mutatePublicationStatusField(t, queued, "schema", json.RawMessage(`"loom.task-image-publication-status/V1"`)),
		"unknown state":    mutatePublicationStatusField(t, queued, "state", json.RawMessage(`"waiting"`)),
		"state case":       mutatePublicationStatusField(t, queued, "state", json.RawMessage(`"Queued"`)),
		"zero UUID":        mutatePublicationStatusField(t, queued, "grant_id", json.RawMessage(`"00000000-0000-0000-0000-000000000000"`)),
		"uppercase UUID":   mutatePublicationStatusField(t, queued, "attempt_id", json.RawMessage(`"00000000-0000-0000-0000-0000000001F7"`)),
		"zero digest":      mutatePublicationStatusField(t, queued, "snapshot_sha256", json.RawMessage(`"0000000000000000000000000000000000000000000000000000000000000000"`)),
		"uppercase digest": mutatePublicationStatusField(t, queued, "candidate_set_sha256", json.RawMessage(`"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"`)),
		"fractional lease": mutatePublicationStatusField(t, queued, "lease_epoch", json.RawMessage(`1.5`)),
		"boolean lease":    mutatePublicationStatusField(t, queued, "lease_epoch", json.RawMessage(`true`)),
		"zero lease":       mutatePublicationStatusField(t, queued, "lease_epoch", json.RawMessage(`0`)),
		"unsafe lease":     mutatePublicationStatusField(t, queued, "lease_epoch", json.RawMessage(`9007199254740992`)),
		"fractional count": mutatePublicationStatusField(t, queued, "component_count", json.RawMessage(`1.5`)),
		"boolean count":    mutatePublicationStatusField(t, queued, "component_count", json.RawMessage(`true`)),
		"zero count":       mutatePublicationStatusField(t, queued, "component_count", json.RawMessage(`0`)),
		"oversize count":   mutatePublicationStatusField(t, queued, "component_count", json.RawMessage(`129`)),
	}
	for name, payload := range tests {
		t.Run(name, func(t *testing.T) {
			assertPublicationStatusRejected(t, payload, vectors.Binding)
		})
	}
}

// Break caught: a status for another grant, attempt, lease or incomplete
// candidate set is accepted as the status of the expected publication.
func TestParsePublicationStatusRejectsExpectedBindingDrift(t *testing.T) {
	vectors := loadPublicationStatusTestVectors(t)
	queued := publicationStatusVectorByName(t, "queued")
	tests := map[string]func(*publicationStatusBinding){
		"grant": func(binding *publicationStatusBinding) { binding.GrantID = "00000000-0000-0000-0000-000000000201" },
		"operation": func(binding *publicationStatusBinding) {
			binding.OperationID = "00000000-0000-0000-0000-000000000202"
		},
		"materialization": func(binding *publicationStatusBinding) {
			binding.MaterializationID = "00000000-0000-0000-0000-000000000203"
		},
		"attempt": func(binding *publicationStatusBinding) { binding.AttemptID = "00000000-0000-0000-0000-000000000204" },
		"lease":   func(binding *publicationStatusBinding) { binding.LeaseEpoch-- },
		"pinned snapshot": func(binding *publicationStatusBinding) {
			binding.PinnedSnapshotSHA256 = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
		},
		"candidate identity": func(binding *publicationStatusBinding) {
			binding.CandidateSetSHA256 = "b" + binding.CandidateSetSHA256[1:]
		},
		"partial count": func(binding *publicationStatusBinding) { binding.ComponentCount-- },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			binding := vectors.Binding
			mutate(&binding)
			assertPublicationStatusRejected(t, []byte(queued.Payload), binding)
		})
	}
}

func TestParsePublicationStatusEnforcesTerminalShapeAndFailureCodes(t *testing.T) {
	vectors := loadPublicationStatusTestVectors(t)
	queued := publicationStatusVectorByName(t, "queued")
	failed := publicationStatusVectorByName(t, "failed")
	completed := publicationStatusVectorByName(t, "completed")
	var completedFields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(completed.Payload), &completedFields); err != nil {
		t.Fatal(err)
	}
	receipt := completedFields["receipt"]

	invalidShapes := map[string][]byte{
		"queued failure":       mutatePublicationStatusField(t, []byte(queued.Payload), "failure_code", json.RawMessage(`"integrity"`)),
		"queued receipt":       mutatePublicationStatusField(t, []byte(queued.Payload), "receipt", receipt),
		"failed no code":       mutatePublicationStatusField(t, []byte(failed.Payload), "failure_code", nil),
		"failed receipt":       mutatePublicationStatusField(t, []byte(failed.Payload), "receipt", receipt),
		"completed no receipt": mutatePublicationStatusField(t, []byte(completed.Payload), "receipt", nil),
		"completed failure":    mutatePublicationStatusField(t, []byte(completed.Payload), "failure_code", json.RawMessage(`"deadline"`)),
	}
	for name, payload := range invalidShapes {
		t.Run(name, func(t *testing.T) {
			assertPublicationStatusRejected(t, payload, vectors.Binding)
		})
	}

	for _, code := range []string{"integrity", "authority_lost", "verification_failed", "deadline"} {
		t.Run("safe_"+code, func(t *testing.T) {
			payload := mutatePublicationStatusField(t, []byte(failed.Payload), "failure_code", json.RawMessage(`"`+code+`"`))
			status, err := parsePublicationStatus(payload, vectors.Binding)
			if err != nil || status.FailureCode != code {
				t.Fatalf("parsePublicationStatus() = %#v, %v; want safe code %q", status, err, code)
			}
		})
	}
	for _, code := range []string{"", "Integrity", "authority-lost", "timeout", "verification_failed "} {
		t.Run("unsafe_"+code, func(t *testing.T) {
			payload := mutatePublicationStatusField(t, []byte(failed.Payload), "failure_code", json.RawMessage(`"`+code+`"`))
			assertPublicationStatusRejected(t, payload, vectors.Binding)
		})
	}
}

func TestParsePublicationStatusRejectsReceiptBindingMismatch(t *testing.T) {
	vectors := loadPublicationStatusTestVectors(t)
	completed := []byte(publicationStatusVectorByName(t, "completed").Payload)
	tests := map[string]json.RawMessage{
		"operation_id":         json.RawMessage(`"00000000-0000-0000-0000-000000000301"`),
		"materialization_id":   json.RawMessage(`"00000000-0000-0000-0000-000000000302"`),
		"attempt_id":           json.RawMessage(`"00000000-0000-0000-0000-000000000303"`),
		"lease_epoch":          json.RawMessage(`9007199254740990`),
		"snapshot_sha256":      json.RawMessage(`"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"`),
		"candidate_set_sha256": json.RawMessage(`"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"`),
		"component_count":      json.RawMessage(`127`),
	}
	for name, value := range tests {
		t.Run(name, func(t *testing.T) {
			assertPublicationStatusRejected(t, mutatePublicationStatusReceiptField(t, completed, name, value), vectors.Binding)
		})
	}
}

func TestParsePublicationStatusRejectsChangedCompleteCandidateIdentityHash(t *testing.T) {
	vectors := loadPublicationStatusTestVectors(t)
	identities := append([]publicationCandidateIdentity(nil), loadPublicationReceiptTestVectors(t).CandidateSets[2].Identities...)
	identities[len(identities)-1].CandidateID = "00000000-0000-0000-0000-000000000999"
	changedDigest, err := publicationCandidateSetSHA256(identities)
	if err != nil {
		t.Fatal(err)
	}
	if changedDigest == vectors.Binding.CandidateSetSHA256 {
		t.Fatal("changed complete candidate identity set reused expected hash")
	}

	payload := []byte(publicationStatusVectorByName(t, "completed").Payload)
	payload = mutatePublicationStatusReceiptField(t, payload, "candidate_set_sha256", json.RawMessage(`"`+changedDigest+`"`))
	payload = mutatePublicationStatusField(t, payload, "candidate_set_sha256", json.RawMessage(`"`+changedDigest+`"`))
	assertPublicationStatusRejected(t, payload, vectors.Binding)
}

func TestParsePublicationStatusAcceptsComponentCountBounds(t *testing.T) {
	vectors := loadPublicationStatusTestVectors(t)
	queued := []byte(publicationStatusVectorByName(t, "queued").Payload)
	if _, err := parsePublicationStatus(queued, vectors.Binding); err != nil {
		t.Fatalf("maximum component count rejected: %v", err)
	}
	minimum := mutatePublicationStatusField(t, queued, "component_count", json.RawMessage(`1`))
	binding := vectors.Binding
	binding.ComponentCount = 1
	if _, err := parsePublicationStatus(minimum, binding); err != nil {
		t.Fatalf("minimum component count rejected: %v", err)
	}
}

func TestParsePublicationStatusErrorsNeverIncludeUntrustedPayload(t *testing.T) {
	vectors := loadPublicationStatusTestVectors(t)
	payload := mutatePublicationStatusField(
		t,
		[]byte(publicationStatusVectorByName(t, "queued").Payload),
		"credential",
		json.RawMessage(`"status-secret-sentinel"`),
	)
	_, err := parsePublicationStatus(payload, vectors.Binding)
	if err == nil || strings.Contains(err.Error(), "status-secret-sentinel") {
		t.Fatalf("parsePublicationStatus() error = %v, want generic rejection", err)
	}
}
