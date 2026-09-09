package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"
)

func registeredCapabilityFixture(t *testing.T) (registeredBundleWire, RegisteredBundlePlan, BundleSessionBinding, BundleDownloadTrust, time.Time) {
	t.Helper()
	now := time.Date(2026, 9, 9, 14, 0, 0, 0, time.UTC)
	files := registeredManifestVector()
	manifest, metadata, err := registeredBundleManifest(files, strings.Repeat("4", 64))
	if err != nil { t.Fatal(err) }
	plan := RegisteredBundlePlan{
		GrantID: uuidWithTail(101), MaterializationID: uuidWithTail(102), TaskChecksum: strings.Repeat("4", 64),
		ManifestSHA256: fmt.Sprintf("%x", sha256.Sum256(manifest)), MetadataSHA256: fmt.Sprintf("%x", sha256.Sum256(metadata)),
		Bucket: "loom-bundles", FileLimit: 2000, ByteLimit: maxTaskImageBuildBundleBytes,
	}
	plan.Prefix = "registered/" + plan.ManifestSHA256 + "/"
	session := BundleSessionBinding{SessionID: uuidWithTail(103), Generation: 3, ExpiresAt: now.Add(time.Minute)}
	trust := BundleDownloadTrust{Origin: "https://objects.example:9443", Bucket: plan.Bucket}
	for i := range files {
		location := url.URL{Scheme: "https", Host: "objects.example:9443", Path: "/" + plan.Bucket + "/" + plan.Prefix + files[i].RelativePath}
		location.RawQuery = "X-Amz-Date=20260909T140000Z&X-Amz-Expires=40&X-Amz-Signature=" + strings.Repeat("a", 64)
		files[i].URL = location.String()
	}
	wire := registeredBundleWire{
		SchemaVersion: "loom.task-image-bundle-capability.v2", CapabilityID: uuidWithTail(104),
		GrantID: plan.GrantID, MaterializationID: plan.MaterializationID, SessionID: session.SessionID, SessionGeneration: session.Generation,
		TaskChecksum: plan.TaskChecksum, MetadataSHA256: plan.MetadataSHA256, ManifestSHA256: plan.ManifestSHA256,
		FileCount: len(files), TotalBytes: 6, IssuedAt: now.Format(time.RFC3339), ExpiresAt: now.Add(40*time.Second).Format(time.RFC3339), Objects: files,
	}
	return wire, plan, session, trust, now
}

func TestRegisteredCapabilityBindsFrozenPlanCurrentSessionAndExactSignedTargets(t *testing.T) {
	wire, plan, session, trust, now := registeredCapabilityFixture(t)
	payload, _ := json.Marshal(wire)
	got, err := parseRegisteredBundleCapability(payload, plan, session, trust, now)
	if err != nil { t.Fatal(err) }
	if got.ExpiresAt != now.Add(40*time.Second) || len(got.Objects) != 4 || got.ManifestSHA256 != plan.ManifestSHA256 {
		t.Fatal("registered capability binding lost")
	}
	for i := range wire.Objects {
		if got.Objects[i] != wire.Objects[i] { t.Fatal("signed target or descriptor rewritten") }
	}
}

func TestRegisteredCapabilityRejectsTamperingBeforeDownload(t *testing.T) {
	for _, kind := range []string{"schema", "grant", "materialization", "session", "generation", "manifest", "metadata", "checksum", "hash", "mode", "path", "size", "count", "total", "expired", "future", "long_lifetime", "session_expiry", "origin", "bucket", "prefix", "url_path", "url_origin", "userinfo", "fragment", "url_deadline", "url_future", "url_duplicate_query", "url_size", "quota", "bytes", "duplicate_json", "missing_size", "null_size", "missing_total", "oversized", "downgrade"} {
		t.Run(kind, func(t *testing.T) {
			wire, plan, session, trust, now := registeredCapabilityFixture(t)
			switch kind {
			case "schema": wire.SchemaVersion = "unknown"
			case "grant": wire.GrantID = uuidWithTail(999)
			case "materialization": wire.MaterializationID = uuidWithTail(999)
			case "session": session.SessionID = uuidWithTail(999)
			case "generation": session.Generation++
			case "manifest": wire.ManifestSHA256 = strings.Repeat("6", 64)
			case "metadata": wire.MetadataSHA256 = strings.Repeat("6", 64)
			case "checksum": wire.TaskChecksum = strings.Repeat("6", 64)
			case "hash": wire.Objects[0].SHA256 = strings.Repeat("6", 64)
			case "mode": wire.Objects[0].Mode = "0755"
			case "path": wire.Objects[0].RelativePath = "a-changed"
			case "size": wire.Objects[0].SizeBytes = 1; wire.TotalBytes++
			case "count": wire.FileCount--
			case "total": wire.TotalBytes++
			case "expired": now = now.Add(40*time.Second)
			case "future": wire.IssuedAt = now.Add(time.Second).Format(time.RFC3339)
			case "long_lifetime": wire.IssuedAt = now.Add(-901*time.Second).Format(time.RFC3339)
			case "session_expiry": session.ExpiresAt = now.Add(39*time.Second)
			case "origin": trust.Origin = "https://other.example"
			case "bucket": trust.Bucket = "other-bundles"
			case "prefix": plan.Prefix = "other/" + plan.ManifestSHA256 + "/"
			case "url_path": wire.Objects[0].URL = strings.Replace(wire.Objects[0].URL, "/loom-bundles/", "/other-bundles/", 1)
			case "url_origin": wire.Objects[0].URL = strings.Replace(wire.Objects[0].URL, "objects.example", "other.example", 1)
			case "userinfo": wire.Objects[0].URL = strings.Replace(wire.Objects[0].URL, "https://", "https://user:secret@", 1)
			case "fragment": wire.Objects[0].URL += "#secret"
			case "url_deadline": wire.Objects[0].URL = strings.Replace(wire.Objects[0].URL, "Expires=40", "Expires=41", 1)
			case "url_future": wire.Objects[0].URL = strings.Replace(wire.Objects[0].URL, "T140000Z", "T140001Z", 1)
			case "url_duplicate_query": wire.Objects[0].URL += "&X-Amz-Expires=40"
			case "url_size": wire.Objects[0].URL += "&large=" + strings.Repeat("a", 4096)
			case "quota": plan.FileLimit = 3
			case "bytes": plan.ByteLimit = 5
			case "downgrade": wire.SchemaVersion = "loom.task-image-bundle-capability.v1"
			}
			payload, _ := json.Marshal(wire)
			switch kind {
			case "duplicate_json": payload = append([]byte(`{"total_bytes":6,`), payload[1:]...)
			case "missing_size": payload = []byte(strings.Replace(string(payload), `"size_bytes":0,`, "", 1))
			case "null_size": payload = []byte(strings.Replace(string(payload), `"size_bytes":0`, `"size_bytes":null`, 1))
			case "missing_total": payload = []byte(strings.Replace(string(payload), `"total_bytes":6,`, "", 1))
			case "oversized": payload = append(payload, []byte(strings.Repeat(" ", 8*1024*1024))...)
			}
			if _, err := parseRegisteredBundleCapability(payload, plan, session, trust, now); err == nil {
				t.Fatal("accepted invalid native capability")
			} else if strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "X-Amz-") {
				t.Fatal("capability rejection leaked signed target")
			}
		})
	}
}
