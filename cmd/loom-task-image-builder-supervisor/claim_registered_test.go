package main

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func registeredClaimFixture(t *testing.T) (map[string]any, claimBinding) {
	t.Helper()
	mutation := defaultClaimMutation()
	var wire map[string]any
	if err := json.Unmarshal([]byte(testClaimJSON(mutation)), &wire); err != nil { t.Fatal(err) }
	plan := wire["plan"].(map[string]any)
	plan["schema_version"] = "loom.task-image-build-plan.v2"
	plan["bundle_content_manifest_sha256"] = strings.Repeat("7", 64)
	plan["bundle_prefix"] = "bundles/sha256/"+strings.Repeat("7", 64)+"/"
	return wire, claimBinding{GrantID: testGrantID, ClaimID: mutation.ClaimID, SessionID: testSessionID,
		SessionGeneration: 1, ConfigCPUArch: "arm64", Now: testNow, SessionExpiresAt: testNow.Add(10*time.Minute)}
}

func parseRegisteredClaimFixture(t *testing.T, wire map[string]any, binding claimBinding) (*buildClaim, error) {
	t.Helper()
	payload, err := json.Marshal(wire)
	if err != nil { t.Fatal(err) }
	return parseBuildClaim(&SecretBuffer{data: payload}, binding)
}

func TestRegisteredClaimRetainsImmutableDownloadInputsAndExecutorMapping(t *testing.T) {
	wire, binding := registeredClaimFixture(t)
	claim, err := parseRegisteredClaimFixture(t, wire, binding)
	if err != nil { t.Fatal(err) }
	if claim.RegisteredBundle == nil { t.Fatal("strong plan lost download authority") }
	want := RegisteredBundlePlan{GrantID: testGrantID, MaterializationID: testMaterializationID,
		TaskChecksum: strings.Repeat("5", 64), ManifestSHA256: strings.Repeat("7", 64), MetadataSHA256: strings.Repeat("6", 64),
		Bucket: "loom-bundles", Prefix: "bundles/sha256/"+strings.Repeat("7", 64)+"/", FileLimit: 2000, ByteLimit: 536870912}
	if *claim.RegisteredBundle != want { t.Fatal("immutable download binding changed") }
	for _, component := range claim.Plan.Components {
		if err := validateBuildComponent(component); err != nil { t.Fatal(err) }
	}
}

func TestRegisteredClaimRejectsDowngradeMissingAndMutatedInputs(t *testing.T) {
	for _, kind := range []string{"missing", "null", "digest", "prefix", "bucket", "schema", "component", "outside_context", "expired", "unknown", "missing_count"} {
		t.Run(kind, func(t *testing.T) {
			wire, binding := registeredClaimFixture(t)
			if _, err := parseRegisteredClaimFixture(t, wire, binding); err != nil { t.Fatalf("invalid baseline: %v", err) }
			plan := wire["plan"].(map[string]any)
			switch kind {
			case "missing": delete(plan, "bundle_content_manifest_sha256")
			case "null": plan["bundle_content_manifest_sha256"] = nil
			case "digest": plan["bundle_content_manifest_sha256"] = "invalid"
			case "prefix": plan["bundle_prefix"] = "mutable/"
			case "bucket": plan["bundle_bucket"] = "other/path"
			case "schema": plan["schema_version"] = "loom.task-image-build-plan.v1"
			case "component": plan["components"].([]any)[0].(map[string]any)["name"] = "invalid"
			case "outside_context": plan["components"].([]any)[0].(map[string]any)["context_path"] = "other"
			case "expired": plan["authorization_expires_at"] = binding.Now.Format(time.RFC3339)
			case "unknown": plan["remote_frontend"] = "unsafe"
			case "missing_count": delete(wire, "deterministic_failure_count")
			}
			if _, err := parseRegisteredClaimFixture(t, wire, binding); err == nil { t.Fatal("accepted invalid registered claim") }
		})
	}
}

func TestLegacyClaimDoesNotInventRegisteredBundleAuthority(t *testing.T) {
	wire, binding := registeredClaimFixture(t)
	plan := wire["plan"].(map[string]any)
	plan["schema_version"] = "loom.task-image-build-plan.v1"
	delete(plan, "bundle_content_manifest_sha256")
	claim, err := parseRegisteredClaimFixture(t, wire, binding)
	if err != nil { t.Fatal(err) }
	if claim.RegisteredBundle != nil { t.Fatal("legacy plan acquired strong authority") }
}
