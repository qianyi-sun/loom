package main

import (
	"encoding/json"
	"errors"
	"regexp"
	"unicode/utf8"
)

const (
	baseResolutionMetadataKey = "loom.task-image-base-resolution.v1"
	maxBuildMetadataBytes     = 64 * 1024
	maxBaseResolutionBytes    = 16 * 1024
	maxObservedBaseImages     = 128
)

var baseResolutionSolveRef = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$`)
var errBaseResolutionInvalid = errors.New("build base-resolution evidence invalid")

// BaseResolutionEvidence is immutable build evidence, not archive contents or
// publication authority. Keeping its owned representation comparable avoids
// introducing mutable aliases into a frozen component set.
type BaseResolutionEvidence struct {
	json string
}

func (e BaseResolutionEvidence) JSON() string { return e.json }

type baseResolutionRecord struct {
	Schema              string   `json:"schema"`
	SolveRef            string   `json:"solve_ref"`
	Platform            string   `json:"platform"`
	OutputDigest        string   `json:"output_digest"`
	ObservedBaseDigests []string `json:"observed_base_digests"`
}

// parseBaseResolutionRecord is the single exact-record boundary used for both
// exporter capture and guard acknowledgement evidence. It returns an owned,
// normalized representation so callers never retain mutable JSON aliases.
func parseBaseResolutionRecord(payload []byte) (BaseResolutionEvidence, error) {
	invalid := BaseResolutionEvidence{}
	if len(payload) == 0 || len(payload) > maxBaseResolutionBytes || !utf8.Valid(payload) {
		return invalid, errBaseResolutionInvalid
	}
	fields, err := scanJSONObjectFields(payload)
	if err != nil || len(fields) != 5 {
		return invalid, errBaseResolutionInvalid
	}
	for _, key := range []string{"schema", "solve_ref", "platform", "output_digest", "observed_base_digests"} {
		if _, exists := fields[key]; !exists {
			return invalid, errBaseResolutionInvalid
		}
	}
	var record baseResolutionRecord
	if err := json.Unmarshal(payload, &record); err != nil ||
		record.Schema != "loom.task-image-base-resolution/v1" ||
		!baseResolutionSolveRef.MatchString(record.SolveRef) ||
		(record.Platform != "linux/amd64" && record.Platform != "linux/arm64") ||
		parsePublicationManifestDigest(record.OutputDigest) != nil ||
		record.ObservedBaseDigests == nil || len(record.ObservedBaseDigests) > maxObservedBaseImages {
		return invalid, errBaseResolutionInvalid
	}
	for i, digest := range record.ObservedBaseDigests {
		if parsePublicationManifestDigest(digest) != nil || (i > 0 && record.ObservedBaseDigests[i-1] >= digest) {
			return invalid, errBaseResolutionInvalid
		}
	}
	encoded, err := json.Marshal(record)
	if err != nil || len(encoded) > maxBaseResolutionBytes {
		return invalid, errBaseResolutionInvalid
	}
	return BaseResolutionEvidence{json: string(encoded)}, nil
}

// parseBaseResolutionMetadata consumes buildctl's decoded --metadata-file
// response. Expected bindings come from the supervisor's own ref-file, frozen
// platform, and independently validated OCI output, never from this payload.
// Other bounded exporter fields are ignored and never propagated or logged.
func parseBaseResolutionMetadata(payload []byte, solveRef, platform, outputDigest string) (BaseResolutionEvidence, error) {
	invalid := BaseResolutionEvidence{}
	if len(payload) == 0 || len(payload) > maxBuildMetadataBytes || !utf8.Valid(payload) ||
		len(solveRef) > 128 || !baseResolutionSolveRef.MatchString(solveRef) ||
		(platform != "linux/amd64" && platform != "linux/arm64") ||
		parsePublicationManifestDigest(outputDigest) != nil {
		return invalid, errBaseResolutionInvalid
	}
	fields, err := scanJSONObjectFields(payload)
	if err != nil {
		return invalid, errBaseResolutionInvalid
	}
	exporterDigest, err := decodeJSONString(fields["containerimage.digest"])
	if err != nil || exporterDigest != outputDigest {
		return invalid, errBaseResolutionInvalid
	}
	raw := fields[baseResolutionMetadataKey]
	if len(raw) == 0 || len(raw) > maxBaseResolutionBytes {
		return invalid, errBaseResolutionInvalid
	}
	evidence, err := parseBaseResolutionRecord(raw)
	if err != nil {
		return invalid, errBaseResolutionInvalid
	}
	var record baseResolutionRecord
	if err := json.Unmarshal([]byte(evidence.JSON()), &record); err != nil ||
		record.SolveRef != solveRef || record.Platform != platform || record.OutputDigest != outputDigest {
		return invalid, errBaseResolutionInvalid
	}
	return evidence, nil
}
