package main

import (
	"encoding/json"
	"errors"
	"strings"
	"unicode/utf8"
)

func validateRegisteredClaimFields(payload []byte, wire claimWire) error {
	invalid := errors.New("registered claim invalid")
	var fields map[string]json.RawMessage
	if json.Unmarshal(payload, &fields) != nil {
		return invalid
	}
	var planFields map[string]json.RawMessage
	if json.Unmarshal(fields["plan"], &planFields) != nil {
		return invalid
	}
	if wire.Plan.SchemaVersion != "loom.task-image-build-plan.v2" {
		for key := range planFields {
			if strings.EqualFold(key, "bundle_content_manifest_sha256") {
				return invalid
			}
		}
		return nil
	}
	if !utf8.Valid(payload) || rejectRegisteredLoneSurrogates(payload) != nil ||
		len(fields["plan"]) > 64*1024 ||
		requireRegisteredJSONFields(payload, "schema_version", "claim_id", "materialization_id", "attempt_id",
			"lease_epoch", "state", "deterministic_failure_count", "lease_expires_at", "plan") != nil ||
		requireRegisteredJSONFields(fields["plan"], "schema_version", "grant_id", "session_id", "session_generation",
			"materialization_id", "builder_id", "task_id", "task_checksum", "cpu_arch", "platform", "bundle_bucket",
			"bundle_prefix", "bundle_file_metadata_sha256", "bundle_content_manifest_sha256", "bundle_file_limit",
			"bundle_byte_limit", "build_timeout_seconds", "authorization_expires_at", "components") != nil {
		return invalid
	}
	w := wire.Plan
	if !isDigest(w.BundleContentManifestSHA256) || len(w.BundleBucket) < 3 || !registeredBucketPattern.MatchString(w.BundleBucket) ||
		strings.Contains(w.BundleBucket, "..") || strings.HasPrefix(w.BundleBucket, "xn--") || strings.HasSuffix(w.BundleBucket, "-s3alias") ||
		!strings.HasSuffix(w.BundlePrefix, "/"+w.BundleContentManifestSHA256+"/") ||
		validateRelativeBundlePath(strings.TrimSuffix(w.BundlePrefix, "/")) != nil || strings.ContainsAny(w.BundlePrefix, "\\?#") ||
		utf8.RuneCountInString(w.BundlePrefix) > 4096 || w.TaskID == "" || utf8.RuneCountInString(w.TaskID) > 512 || wire.DeterministicFailureCount < 0 ||
		wire.DeterministicFailureCount > 9007199254740991 || wire.LeaseEpoch > 9007199254740991 || w.Generation > 9007199254740991 {
		return invalid
	}
	var components []json.RawMessage
	if json.Unmarshal(planFields["components"], &components) != nil || len(components) != len(w.Components) {
		return invalid
	}
	for i, component := range w.Components {
		if requireRegisteredJSONFields(components[i], "name", "dockerfile_path", "context_path", "oci_output_path") != nil ||
			!componentPattern.MatchString(component.Name) ||
			utf8.RuneCountInString(component.ContextPath) > 4096 || utf8.RuneCountInString(component.Dockerfile) > 4096 ||
			(component.ContextPath != "." && !strings.HasPrefix(component.Dockerfile, component.ContextPath+"/")) {
			return invalid
		}
	}
	return nil
}

func (w buildPlanWire) registeredBundlePlan() *RegisteredBundlePlan {
	if w.SchemaVersion != "loom.task-image-build-plan.v2" {
		return nil
	}
	return &RegisteredBundlePlan{GrantID: w.GrantID, MaterializationID: w.MaterializeID,
		TaskChecksum: w.TaskChecksum, ManifestSHA256: w.BundleContentManifestSHA256, MetadataSHA256: w.BundleFileMetadataSHA256,
		Bucket: w.BundleBucket, Prefix: w.BundlePrefix, FileLimit: w.BundleFileLimit, ByteLimit: w.BundleByteLimit}
}
