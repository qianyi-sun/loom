package main

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"
	"time"
)

type publicationReceiptTestVectors struct {
	Schema        string                              `json:"schema"`
	Receipts      []publicationReceiptTestVector      `json:"receipts"`
	CandidateSets []publicationCandidateSetTestVector `json:"candidate_sets"`
}

type publicationReceiptTestVector struct {
	Name     string                    `json:"name"`
	Payload  string                    `json:"payload"`
	Binding  publicationReceiptBinding `json:"binding"`
	Expected struct {
		WorkerGeneration     int64  `json:"worker_generation"`
		PublicationSetSHA256 string `json:"publication_set_sha256"`
		CompletedAt          string `json:"completed_at"`
	} `json:"expected"`
}

type publicationCandidateSetTestVector struct {
	Name       string                         `json:"name"`
	Identities []publicationCandidateIdentity `json:"identities"`
	SHA256     string                         `json:"sha256"`
}

func loadPublicationReceiptTestVectors(t *testing.T) publicationReceiptTestVectors {
	t.Helper()
	payload, err := os.ReadFile("testdata/publication_receipt_vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var vectors publicationReceiptTestVectors
	if err := json.Unmarshal(payload, &vectors); err != nil {
		t.Fatal(err)
	}
	if vectors.Schema != "loom.task-image-publication-go-test-vectors/v1" || len(vectors.Receipts) == 0 || len(vectors.CandidateSets) != 3 {
		t.Fatalf("receipt vectors incomplete: %#v", vectors)
	}
	return vectors
}

func TestParsePublicationReceiptMatchesPythonCanonicalVector(t *testing.T) {
	vector := loadPublicationReceiptTestVectors(t).Receipts[0]
	receipt, err := parsePublicationReceipt([]byte(vector.Payload), vector.Binding)
	if err != nil {
		t.Fatalf("parsePublicationReceipt() error = %v", err)
	}
	completedAt, err := time.Parse("2006-01-02T15:04:05Z", vector.Expected.CompletedAt)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.OperationID != vector.Binding.OperationID ||
		receipt.MaterializationID != vector.Binding.MaterializationID ||
		receipt.AttemptID != vector.Binding.AttemptID ||
		receipt.LeaseEpoch != vector.Binding.LeaseEpoch ||
		receipt.WorkerGeneration != vector.Expected.WorkerGeneration ||
		receipt.SnapshotSHA256 != vector.Binding.SnapshotSHA256 ||
		receipt.CandidateSetSHA256 != vector.Binding.CandidateSetSHA256 ||
		receipt.PublicationSetSHA256 != vector.Expected.PublicationSetSHA256 ||
		receipt.ComponentCount != vector.Binding.ComponentCount ||
		!receipt.CompletedAt.Equal(completedAt) {
		t.Fatalf("parsed receipt = %#v, want Python vector %#v", receipt, vector)
	}
}

// Break caught: a missing or null authority-bearing field is defaulted and the
// remaining receipt is accepted.
func TestParsePublicationReceiptRejectsEveryMissingOrNullField(t *testing.T) {
	vector := loadPublicationReceiptTestVectors(t).Receipts[0]
	var fields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(vector.Payload), &fields); err != nil {
		t.Fatal(err)
	}
	for name := range fields {
		for _, mutation := range []string{"missing", "null"} {
			t.Run(name+"_"+mutation, func(t *testing.T) {
				changed := make(map[string]json.RawMessage, len(fields))
				for key, value := range fields {
					changed[key] = value
				}
				if mutation == "missing" {
					delete(changed, name)
				} else {
					changed[name] = json.RawMessage("null")
				}
				payload, err := json.Marshal(changed)
				if err != nil {
					t.Fatal(err)
				}
				if receipt, err := parsePublicationReceipt(payload, vector.Binding); err == nil {
					t.Fatalf("parsePublicationReceipt() = %#v, want %s field rejection", receipt, mutation)
				}
			})
		}
	}
}

func TestParsePublicationReceiptRejectsInvalidOrNoncanonicalWire(t *testing.T) {
	vector := loadPublicationReceiptTestVectors(t).Receipts[0]
	payload := []byte(vector.Payload)
	mutateField := func(t *testing.T, name string, value json.RawMessage) []byte {
		t.Helper()
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(payload, &fields); err != nil {
			t.Fatal(err)
		}
		fields[name] = value
		changed, err := json.Marshal(fields)
		if err != nil {
			t.Fatal(err)
		}
		return changed
	}
	tests := map[string][]byte{
		"unknown field":    mutateField(t, "credential", json.RawMessage(`"must-not-leak"`)),
		"case alias":       bytes.Replace(payload, []byte(`"attempt_id"`), []byte(`"Attempt_Id"`), 1),
		"duplicate field":  append([]byte(`{"attempt_id":"00000000-0000-0000-0000-0000000001f7",`), payload[1:]...),
		"spacing":          append(append([]byte(nil), payload...), ' '),
		"oversize":         append(append([]byte(nil), payload...), bytes.Repeat([]byte{' '}, maxPublicationReceiptBytes-len(payload)+1)...),
		"wrong schema":     mutateField(t, "schema", json.RawMessage(`"loom.task-image-publication-receipt/V1"`)),
		"zero UUID":        mutateField(t, "attempt_id", json.RawMessage(`"00000000-0000-0000-0000-000000000000"`)),
		"zero digest":      mutateField(t, "publication_set_sha256", json.RawMessage(`"0000000000000000000000000000000000000000000000000000000000000000"`)),
		"uppercase digest": mutateField(t, "snapshot_sha256", json.RawMessage(`"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"`)),
		"fractional lease": mutateField(t, "lease_epoch", json.RawMessage(`1.5`)),
		"boolean lease":    mutateField(t, "lease_epoch", json.RawMessage(`true`)),
		"zero lease":       mutateField(t, "lease_epoch", json.RawMessage(`0`)),
		"unsafe lease":     mutateField(t, "lease_epoch", json.RawMessage(`9007199254740992`)),
		"zero worker":      mutateField(t, "worker_generation", json.RawMessage(`0`)),
		"unsafe worker":    mutateField(t, "worker_generation", json.RawMessage(`9007199254740992`)),
		"zero count":       mutateField(t, "component_count", json.RawMessage(`0`)),
		"oversize count":   mutateField(t, "component_count", json.RawMessage(`129`)),
		"fractional time":  mutateField(t, "completed_at", json.RawMessage(`"2026-09-08T12:34:56.000Z"`)),
		"offset time":      mutateField(t, "completed_at", json.RawMessage(`"2026-09-08T08:34:56-04:00"`)),
	}
	for name, changed := range tests {
		t.Run(name, func(t *testing.T) {
			if receipt, err := parsePublicationReceipt(changed, vector.Binding); err == nil {
				t.Fatalf("parsePublicationReceipt() = %#v, want rejection", receipt)
			}
		})
	}
}

// Break caught: a valid receipt for another attempt or a partial candidate set
// is treated as the expected completion.
func TestParsePublicationReceiptRejectsExpectedAttemptBindingDrift(t *testing.T) {
	vector := loadPublicationReceiptTestVectors(t).Receipts[0]
	tests := map[string]func(*publicationReceiptBinding){
		"operation": func(binding *publicationReceiptBinding) { binding.OperationID = "00000000-0000-0000-0000-000000000101" },
		"materialization": func(binding *publicationReceiptBinding) {
			binding.MaterializationID = "00000000-0000-0000-0000-000000000102"
		},
		"attempt":  func(binding *publicationReceiptBinding) { binding.AttemptID = "00000000-0000-0000-0000-000000000103" },
		"lease":    func(binding *publicationReceiptBinding) { binding.LeaseEpoch-- },
		"snapshot": func(binding *publicationReceiptBinding) { binding.SnapshotSHA256 = "b" + binding.SnapshotSHA256[1:] },
		"candidate identity": func(binding *publicationReceiptBinding) {
			binding.CandidateSetSHA256 = "b" + binding.CandidateSetSHA256[1:]
		},
		"partial count": func(binding *publicationReceiptBinding) { binding.ComponentCount-- },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			binding := vector.Binding
			mutate(&binding)
			if receipt, err := parsePublicationReceipt([]byte(vector.Payload), binding); err == nil {
				t.Fatalf("parsePublicationReceipt() = %#v, want binding rejection", receipt)
			}
		})
	}
}

func TestPublicationCandidateSetSHA256MatchesPythonVectors(t *testing.T) {
	for _, vector := range loadPublicationReceiptTestVectors(t).CandidateSets {
		t.Run(vector.Name, func(t *testing.T) {
			if vector.Name == "maximum_128" && len(vector.Identities) != 128 {
				t.Fatalf("maximum vector identities = %d, want 128", len(vector.Identities))
			}
			got, err := publicationCandidateSetSHA256(vector.Identities)
			if err != nil {
				t.Fatalf("publicationCandidateSetSHA256() error = %v", err)
			}
			if got != vector.SHA256 {
				t.Fatalf("publicationCandidateSetSHA256() = %q, want Python vector %q", got, vector.SHA256)
			}
		})
	}
}

func TestPublicationCandidateSetSHA256RejectsAmbiguousOrNoncanonicalSet(t *testing.T) {
	vectors := loadPublicationReceiptTestVectors(t).CandidateSets
	withTask := append([]publicationCandidateIdentity(nil), vectors[0].Identities...)
	sidecars := append([]publicationCandidateIdentity(nil), vectors[1].Identities...)
	maximum := append([]publicationCandidateIdentity(nil), vectors[2].Identities...)
	duplicateID := append([]publicationCandidateIdentity(nil), withTask...)
	duplicateID[1].CandidateID = duplicateID[0].CandidateID
	duplicateName := append([]publicationCandidateIdentity(nil), withTask...)
	duplicateName[1].Component = duplicateName[0].Component
	taskAfterSidecar := append([]publicationCandidateIdentity(nil), withTask...)
	taskAfterSidecar[0], taskAfterSidecar[1] = taskAfterSidecar[1], taskAfterSidecar[0]
	unorderedSidecars := append([]publicationCandidateIdentity(nil), sidecars...)
	unorderedSidecars[0], unorderedSidecars[1] = unorderedSidecars[1], unorderedSidecars[0]
	invalidID := append([]publicationCandidateIdentity(nil), withTask...)
	invalidID[0].CandidateID = "00000000-0000-0000-0000-000000000000"
	invalidComponent := append([]publicationCandidateIdentity(nil), withTask...)
	invalidComponent[0].Component = "TASK"
	overCount := append(maximum, withTask[0])

	for name, identities := range map[string][]publicationCandidateIdentity{
		"empty":              nil,
		"duplicate ID":       duplicateID,
		"duplicate name":     duplicateName,
		"task after sidecar": taskAfterSidecar,
		"unordered sidecars": unorderedSidecars,
		"invalid ID":         invalidID,
		"invalid component":  invalidComponent,
		"over count":         overCount,
	} {
		t.Run(name, func(t *testing.T) {
			if digest, err := publicationCandidateSetSHA256(identities); err == nil || digest != "" {
				t.Fatalf("publicationCandidateSetSHA256() = %q, %v; want rejection", digest, err)
			}
		})
	}

	partial, err := publicationCandidateSetSHA256(withTask[:2])
	if err != nil {
		t.Fatal(err)
	}
	if partial == vectors[0].SHA256 {
		t.Fatal("partial candidate set reused complete candidate-set digest")
	}
	changed := append([]publicationCandidateIdentity(nil), withTask...)
	changed[0].CandidateID = "00000000-0000-0000-0000-000000000999"
	changedDigest, err := publicationCandidateSetSHA256(changed)
	if err != nil {
		t.Fatal(err)
	}
	if changedDigest == vectors[0].SHA256 {
		t.Fatal("changed candidate identity reused frozen candidate-set digest")
	}
}
