package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"
	"unicode/utf8"
)

const (
	maxPublicationReceiptBytes       = 2048
	maxPublicationComponents         = 128
	maxJSONSafeInteger         int64 = 1<<53 - 1
)

type publicationReceiptBinding struct {
	OperationID        string `json:"operation_id"`
	MaterializationID  string `json:"materialization_id"`
	AttemptID          string `json:"attempt_id"`
	LeaseEpoch         int64  `json:"lease_epoch"`
	SnapshotSHA256     string `json:"snapshot_sha256"`
	CandidateSetSHA256 string `json:"candidate_set_sha256"`
	ComponentCount     int    `json:"component_count"`
}

type publicationReceipt struct {
	OperationID          string
	MaterializationID    string
	AttemptID            string
	LeaseEpoch           int64
	WorkerGeneration     int64
	SnapshotSHA256       string
	CandidateSetSHA256   string
	PublicationSetSHA256 string
	ComponentCount       int
	CompletedAt          time.Time
}

// Field order is RFC 8785 lexical order for this closed ASCII-only schema.
type publicationReceiptWire struct {
	AttemptID            *string `json:"attempt_id"`
	CandidateSetSHA256   *string `json:"candidate_set_sha256"`
	CompletedAt          *string `json:"completed_at"`
	ComponentCount       *int    `json:"component_count"`
	LeaseEpoch           *int64  `json:"lease_epoch"`
	MaterializationID    *string `json:"materialization_id"`
	OperationID          *string `json:"operation_id"`
	PublicationSetSHA256 *string `json:"publication_set_sha256"`
	Schema               *string `json:"schema"`
	SnapshotSHA256       *string `json:"snapshot_sha256"`
	WorkerGeneration     *int64  `json:"worker_generation"`
}

type publicationCandidateIdentity struct {
	CandidateID string `json:"candidate_id"`
	Component   string `json:"component"`
}

// Field order is RFC 8785 lexical order for this closed ASCII-only schema.
type publicationCandidateSetWire struct {
	Components []publicationCandidateIdentity `json:"components"`
	Schema     string                         `json:"schema"`
}

func parsePublicationReceipt(payload []byte, binding publicationReceiptBinding) (publicationReceipt, error) {
	invalid := func() (publicationReceipt, error) {
		return publicationReceipt{}, errors.New("publication receipt invalid")
	}
	if len(payload) == 0 || len(payload) > maxPublicationReceiptBytes || !utf8.Valid(payload) || !validPublicationReceiptBinding(binding) {
		return invalid()
	}
	var wire publicationReceiptWire
	if err := decodeStrictJSON(payload, &wire); err != nil ||
		wire.AttemptID == nil ||
		wire.CandidateSetSHA256 == nil ||
		wire.CompletedAt == nil ||
		wire.ComponentCount == nil ||
		wire.LeaseEpoch == nil ||
		wire.MaterializationID == nil ||
		wire.OperationID == nil ||
		wire.PublicationSetSHA256 == nil ||
		wire.Schema == nil ||
		wire.SnapshotSHA256 == nil ||
		wire.WorkerGeneration == nil {
		return invalid()
	}
	completedAt, err := parseCanonicalPublicationReceiptTime(*wire.CompletedAt)
	if err != nil ||
		*wire.Schema != "loom.task-image-publication-receipt/v1" ||
		!isCanonicalNonZeroUUID(*wire.OperationID) ||
		!isCanonicalNonZeroUUID(*wire.MaterializationID) ||
		!isCanonicalNonZeroUUID(*wire.AttemptID) ||
		!isPositiveJSONSafeInteger(*wire.LeaseEpoch) ||
		!isPositiveJSONSafeInteger(*wire.WorkerGeneration) ||
		!isDigest(*wire.SnapshotSHA256) ||
		!isDigest(*wire.CandidateSetSHA256) ||
		!isDigest(*wire.PublicationSetSHA256) ||
		*wire.ComponentCount < 1 ||
		*wire.ComponentCount > maxPublicationComponents {
		return invalid()
	}
	canonical, err := json.Marshal(wire)
	if err != nil || !bytes.Equal(canonical, payload) {
		return invalid()
	}
	if *wire.OperationID != binding.OperationID ||
		*wire.MaterializationID != binding.MaterializationID ||
		*wire.AttemptID != binding.AttemptID ||
		*wire.LeaseEpoch != binding.LeaseEpoch ||
		*wire.SnapshotSHA256 != binding.SnapshotSHA256 ||
		*wire.CandidateSetSHA256 != binding.CandidateSetSHA256 ||
		*wire.ComponentCount != binding.ComponentCount {
		return invalid()
	}
	return publicationReceipt{
		OperationID:          *wire.OperationID,
		MaterializationID:    *wire.MaterializationID,
		AttemptID:            *wire.AttemptID,
		LeaseEpoch:           *wire.LeaseEpoch,
		WorkerGeneration:     *wire.WorkerGeneration,
		SnapshotSHA256:       *wire.SnapshotSHA256,
		CandidateSetSHA256:   *wire.CandidateSetSHA256,
		PublicationSetSHA256: *wire.PublicationSetSHA256,
		ComponentCount:       *wire.ComponentCount,
		CompletedAt:          completedAt,
	}, nil
}

func validPublicationReceiptBinding(binding publicationReceiptBinding) bool {
	return isCanonicalNonZeroUUID(binding.OperationID) &&
		isCanonicalNonZeroUUID(binding.MaterializationID) &&
		isCanonicalNonZeroUUID(binding.AttemptID) &&
		isPositiveJSONSafeInteger(binding.LeaseEpoch) &&
		isDigest(binding.SnapshotSHA256) &&
		isDigest(binding.CandidateSetSHA256) &&
		binding.ComponentCount >= 1 &&
		binding.ComponentCount <= maxPublicationComponents
}

func isPositiveJSONSafeInteger(value int64) bool {
	return value > 0 && value <= maxJSONSafeInteger
}

func parseCanonicalPublicationReceiptTime(value string) (time.Time, error) {
	const layout = "2006-01-02T15:04:05Z"
	if len(value) != len(layout) {
		return time.Time{}, errors.New("publication receipt time invalid")
	}
	parsed, err := time.Parse(layout, value)
	if err != nil || parsed.Year() < 1 || parsed.Year() > 9999 || parsed.Format(layout) != value {
		return time.Time{}, errors.New("publication receipt time invalid")
	}
	return parsed, nil
}

func publicationCandidateSetSHA256(identities []publicationCandidateIdentity) (string, error) {
	if len(identities) < 1 || len(identities) > maxPublicationComponents {
		return "", errors.New("publication candidate set invalid")
	}
	owned := make([]publicationCandidateIdentity, len(identities))
	seenIDs := make(map[string]struct{}, len(identities))
	seenComponents := make(map[string]struct{}, len(identities))
	previousSidecar := ""
	for index, identity := range identities {
		if !isCanonicalNonZeroUUID(identity.CandidateID) || !componentPattern.MatchString(identity.Component) {
			return "", errors.New("publication candidate identity invalid")
		}
		if _, exists := seenIDs[identity.CandidateID]; exists {
			return "", errors.New("publication candidate set invalid")
		}
		if _, exists := seenComponents[identity.Component]; exists {
			return "", errors.New("publication candidate set invalid")
		}
		if identity.Component == "task" {
			if index != 0 {
				return "", errors.New("publication candidate set invalid")
			}
		} else {
			if previousSidecar != "" && identity.Component <= previousSidecar {
				return "", errors.New("publication candidate set invalid")
			}
			previousSidecar = identity.Component
		}
		seenIDs[identity.CandidateID] = struct{}{}
		seenComponents[identity.Component] = struct{}{}
		owned[index] = identity
	}
	encoded, err := json.Marshal(publicationCandidateSetWire{
		Components: owned,
		Schema:     "loom.task-image-publication-candidate-set/v1",
	})
	if err != nil {
		return "", errors.New("publication candidate set invalid")
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}
