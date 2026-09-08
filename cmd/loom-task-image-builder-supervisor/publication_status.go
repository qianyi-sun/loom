package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"unicode/utf8"
)

const maxPublicationStatusBytes = 4096

type publicationStatusBinding struct {
	GrantID              string `json:"grant_id"`
	OperationID          string `json:"operation_id"`
	MaterializationID    string `json:"materialization_id"`
	AttemptID            string `json:"attempt_id"`
	LeaseEpoch           int64  `json:"lease_epoch"`
	PinnedSnapshotSHA256 string `json:"pinned_snapshot_sha256"`
	CandidateSetSHA256   string `json:"candidate_set_sha256"`
	ComponentCount       int    `json:"component_count"`
}

type publicationStatus struct {
	GrantID            string
	OperationID        string
	MaterializationID  string
	AttemptID          string
	LeaseEpoch         int64
	State              string
	SnapshotSHA256     string
	CandidateSetSHA256 string
	ComponentCount     int
	Receipt            *publicationReceipt
	FailureCode        string
}

// Field order is RFC 8785 lexical order for this closed ASCII-only schema.
type publicationStatusWire struct {
	AttemptID          *string         `json:"attempt_id"`
	CandidateSetSHA256 *string         `json:"candidate_set_sha256"`
	ComponentCount     *int            `json:"component_count"`
	FailureCode        *string         `json:"failure_code,omitempty"`
	GrantID            *string         `json:"grant_id"`
	LeaseEpoch         *int64          `json:"lease_epoch"`
	MaterializationID  *string         `json:"materialization_id"`
	OperationID        *string         `json:"operation_id"`
	Receipt            json.RawMessage `json:"receipt,omitempty"`
	Schema             *string         `json:"schema"`
	SnapshotSHA256     *string         `json:"snapshot_sha256"`
	State              *string         `json:"state"`
}

func parsePublicationStatus(payload []byte, binding publicationStatusBinding) (publicationStatus, error) {
	invalid := func() (publicationStatus, error) {
		return publicationStatus{}, errors.New("publication status invalid")
	}
	if len(payload) == 0 || len(payload) > maxPublicationStatusBytes || !utf8.Valid(payload) || !validPublicationStatusBinding(binding) {
		return invalid()
	}
	var wire publicationStatusWire
	if err := decodeStrictJSON(payload, &wire); err != nil ||
		wire.AttemptID == nil ||
		wire.CandidateSetSHA256 == nil ||
		wire.ComponentCount == nil ||
		wire.GrantID == nil ||
		wire.LeaseEpoch == nil ||
		wire.MaterializationID == nil ||
		wire.OperationID == nil ||
		wire.Schema == nil ||
		wire.SnapshotSHA256 == nil ||
		wire.State == nil {
		return invalid()
	}
	if *wire.Schema != "loom.task-image-publication-status/v1" ||
		!isCanonicalNonZeroUUID(*wire.GrantID) ||
		!isCanonicalNonZeroUUID(*wire.OperationID) ||
		!isCanonicalNonZeroUUID(*wire.MaterializationID) ||
		!isCanonicalNonZeroUUID(*wire.AttemptID) ||
		!isPositiveJSONSafeInteger(*wire.LeaseEpoch) ||
		!isDigest(*wire.SnapshotSHA256) ||
		!isDigest(*wire.CandidateSetSHA256) ||
		*wire.ComponentCount < 1 ||
		*wire.ComponentCount > maxPublicationComponents ||
		!validPublicationStatusTerminalShape(*wire.State, wire.Receipt, wire.FailureCode) {
		return invalid()
	}
	canonical, err := json.Marshal(wire)
	if err != nil || !bytes.Equal(canonical, payload) {
		return invalid()
	}
	if *wire.GrantID != binding.GrantID ||
		*wire.OperationID != binding.OperationID ||
		*wire.MaterializationID != binding.MaterializationID ||
		*wire.AttemptID != binding.AttemptID ||
		*wire.LeaseEpoch != binding.LeaseEpoch ||
		(binding.PinnedSnapshotSHA256 != "" && *wire.SnapshotSHA256 != binding.PinnedSnapshotSHA256) ||
		*wire.CandidateSetSHA256 != binding.CandidateSetSHA256 ||
		*wire.ComponentCount != binding.ComponentCount {
		return invalid()
	}

	status := publicationStatus{
		GrantID:            *wire.GrantID,
		OperationID:        *wire.OperationID,
		MaterializationID:  *wire.MaterializationID,
		AttemptID:          *wire.AttemptID,
		LeaseEpoch:         *wire.LeaseEpoch,
		State:              *wire.State,
		SnapshotSHA256:     *wire.SnapshotSHA256,
		CandidateSetSHA256: *wire.CandidateSetSHA256,
		ComponentCount:     *wire.ComponentCount,
	}
	if wire.FailureCode != nil {
		status.FailureCode = *wire.FailureCode
	}
	if len(wire.Receipt) != 0 {
		receipt, err := parsePublicationReceipt(wire.Receipt, publicationReceiptBinding{
			OperationID:        status.OperationID,
			MaterializationID:  status.MaterializationID,
			AttemptID:          status.AttemptID,
			LeaseEpoch:         status.LeaseEpoch,
			SnapshotSHA256:     status.SnapshotSHA256,
			CandidateSetSHA256: status.CandidateSetSHA256,
			ComponentCount:     status.ComponentCount,
		})
		if err != nil {
			return invalid()
		}
		status.Receipt = &receipt
	}
	return status, nil
}

func validPublicationStatusBinding(binding publicationStatusBinding) bool {
	return isCanonicalNonZeroUUID(binding.GrantID) &&
		isCanonicalNonZeroUUID(binding.OperationID) &&
		isCanonicalNonZeroUUID(binding.MaterializationID) &&
		isCanonicalNonZeroUUID(binding.AttemptID) &&
		isPositiveJSONSafeInteger(binding.LeaseEpoch) &&
		(binding.PinnedSnapshotSHA256 == "" || isDigest(binding.PinnedSnapshotSHA256)) &&
		isDigest(binding.CandidateSetSHA256) &&
		binding.ComponentCount >= 1 &&
		binding.ComponentCount <= maxPublicationComponents
}

func validPublicationStatusTerminalShape(state string, receipt json.RawMessage, failureCode *string) bool {
	hasReceipt := len(receipt) != 0
	hasFailure := failureCode != nil
	switch state {
	case "queued", "running":
		return !hasReceipt && !hasFailure
	case "failed":
		return !hasReceipt && hasFailure && isPublicationFailureCode(*failureCode)
	case "completed":
		return hasReceipt && !hasFailure
	default:
		return false
	}
}

func isPublicationFailureCode(value string) bool {
	switch value {
	case "integrity", "authority_lost", "verification_failed", "deadline":
		return true
	default:
		return false
	}
}
