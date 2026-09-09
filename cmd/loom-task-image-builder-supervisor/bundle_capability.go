package main

import (
	"bytes"
	"crypto/sha256"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const maxRegisteredCapabilityBytes = 8 * 1024 * 1024

// RegisteredBundlePlan contains only immutable claim inputs. A successor's
// capability binds its issuance session, not the original claim's bearer.
type RegisteredBundlePlan struct {
	GrantID, MaterializationID, TaskChecksum string
	ManifestSHA256, MetadataSHA256           string
	Bucket, Prefix                           string
	FileLimit                                int
	ByteLimit                                int64
}

type BundleSessionBinding struct {
	SessionID  string
	Generation int
	ExpiresAt  time.Time
}

// BundleDownloadTrust is supplied by trusted release configuration, never by a
// sealed task capability. Roots are consumed only by the downloader transport.
type BundleDownloadTrust struct {
	Origin, Bucket string
	Roots          *x509.CertPool
}

type registeredBundleWire struct {
	SchemaVersion     string                   `json:"schema_version"`
	CapabilityID      string                   `json:"capability_id"`
	GrantID           string                   `json:"grant_id"`
	SessionID         string                   `json:"session_id"`
	SessionGeneration int                      `json:"session_generation"`
	MaterializationID string                   `json:"materialization_id"`
	TaskChecksum      string                   `json:"task_checksum"`
	MetadataSHA256    string                   `json:"bundle_file_metadata_sha256"`
	ManifestSHA256    string                   `json:"bundle_content_manifest_sha256"`
	FileCount         int                      `json:"file_count"`
	TotalBytes        int64                    `json:"total_bytes"`
	IssuedAt          string                   `json:"issued_at"`
	ExpiresAt         string                   `json:"expires_at"`
	Objects           []RegisteredBundleObject `json:"objects"`
}

type RegisteredBundleCapability struct {
	ManifestSHA256 string
	MetadataSHA256 string
	TotalBytes     int64
	ExpiresAt      time.Time
	Objects        []RegisteredBundleObject
}

var registeredBucketPattern = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9.-]{1,61}[a-z0-9])?$`)

func registeredBundleOrigin(trust BundleDownloadTrust) (*url.URL, error) {
	origin, err := url.Parse(trust.Origin)
	if err != nil || origin.Scheme != "https" || origin.Hostname() == "" || origin.User != nil ||
		(origin.Path != "" && origin.Path != "/") || origin.RawQuery != "" || origin.Fragment != "" ||
		!registeredBucketPattern.MatchString(trust.Bucket) || strings.Contains(trust.Bucket, "..") ||
		strings.HasPrefix(trust.Bucket, "xn--") || strings.HasSuffix(trust.Bucket, "-s3alias") {
		return nil, errors.New("registered bundle configured origin invalid")
	}
	return origin, nil
}

func requireRegisteredJSONFields(payload []byte, required ...string) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(payload, &fields); err != nil || len(fields) != len(required) {
		return errors.New("registered bundle fields invalid")
	}
	for _, key := range required {
		value, ok := fields[key]
		if !ok || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return errors.New("registered bundle required field missing")
		}
	}
	return nil
}

// encoding/json replaces escaped lone surrogates with U+FFFD. Python's manifest
// path contract rejects them, so a literal registered U+FFFD filename must not
// make malformed wire text acceptable. Scan escapes without interpreting an
// escaped backslash as the beginning of a second escape. JSON syntax validation
// still belongs to decodeStrictJSON.
func rejectRegisteredLoneSurrogates(payload []byte) error {
	inString := false
	for i := 0; i < len(payload); i++ {
		if payload[i] == '"' {
			inString = !inString
			continue
		}
		if !inString || payload[i] != '\\' {
			continue
		}
		i++
		if i >= len(payload) {
			return errors.New("registered bundle JSON escape invalid")
		}
		if payload[i] != 'u' {
			continue
		}
		if i+4 >= len(payload) {
			return errors.New("registered bundle JSON escape invalid")
		}
		value, err := strconv.ParseUint(string(payload[i+1:i+5]), 16, 16)
		if err != nil {
			return errors.New("registered bundle JSON escape invalid")
		}
		i += 4
		if value >= 0xdc00 && value <= 0xdfff {
			return errors.New("registered bundle lone surrogate invalid")
		}
		if value < 0xd800 || value > 0xdbff {
			continue
		}
		if i+6 >= len(payload) || payload[i+1] != '\\' || payload[i+2] != 'u' {
			return errors.New("registered bundle lone surrogate invalid")
		}
		low, err := strconv.ParseUint(string(payload[i+3:i+7]), 16, 16)
		if err != nil || low < 0xdc00 || low > 0xdfff {
			return errors.New("registered bundle lone surrogate invalid")
		}
		i += 6
	}
	return nil
}

func parseRegisteredBundleCapability(payload []byte, plan RegisteredBundlePlan, session BundleSessionBinding, trust BundleDownloadTrust, now time.Time) (*RegisteredBundleCapability, error) {
	if len(payload) == 0 || len(payload) > maxRegisteredCapabilityBytes || !utf8.Valid(payload) || rejectRegisteredLoneSurrogates(payload) != nil {
		return nil, errors.New("registered bundle capability size or encoding invalid")
	}
	origin, err := registeredBundleOrigin(trust)
	if err != nil {
		return nil, err
	}
	if !isCanonicalNonZeroUUID(plan.GrantID) || !isCanonicalNonZeroUUID(plan.MaterializationID) ||
		!isDigest(plan.TaskChecksum) || !isDigest(plan.MetadataSHA256) || !isDigest(plan.ManifestSHA256) ||
		plan.Bucket != trust.Bucket || plan.FileLimit <= 0 || plan.FileLimit > maxTaskImageBuildBundleFiles ||
		plan.ByteLimit <= 0 || plan.ByteLimit > maxTaskImageBuildBundleBytes ||
		!strings.HasSuffix(plan.Prefix, "/"+plan.ManifestSHA256+"/") ||
		validateRelativeBundlePath(strings.TrimSuffix(plan.Prefix, "/")) != nil ||
		strings.ContainsAny(plan.Prefix, "\\?#") ||
		!isCanonicalNonZeroUUID(session.SessionID) || session.Generation <= 0 || !session.ExpiresAt.After(now) {
		return nil, errors.New("registered bundle expected binding invalid")
	}
	var wire registeredBundleWire
	if decodeStrictJSON(payload, &wire) != nil || requireRegisteredJSONFields(payload,
		"schema_version", "capability_id", "grant_id", "session_id", "session_generation", "materialization_id",
		"task_checksum", "bundle_file_metadata_sha256", "bundle_content_manifest_sha256", "file_count", "total_bytes",
		"issued_at", "expires_at", "objects") != nil {
		return nil, errors.New("registered bundle capability JSON invalid")
	}
	// Go's decoder accepts omitted/null integer fields as zero. Both are invalid
	// even for a legitimate zero-byte file; check exact mandatory object fields.
	var raw struct {
		Objects []json.RawMessage `json:"objects"`
	}
	if json.Unmarshal(payload, &raw) != nil {
		return nil, errors.New("registered bundle objects invalid")
	}
	for _, object := range raw.Objects {
		if requireRegisteredJSONFields(object, "relative_path", "size_bytes", "url", "sha256", "mode") != nil {
			return nil, errors.New("registered bundle object fields invalid")
		}
	}
	issued, issueErr := time.Parse(time.RFC3339Nano, wire.IssuedAt)
	expires, expiryErr := time.Parse(time.RFC3339Nano, wire.ExpiresAt)
	if wire.SchemaVersion != "loom.task-image-bundle-capability.v2" || !isCanonicalNonZeroUUID(wire.CapabilityID) ||
		wire.GrantID != plan.GrantID || wire.MaterializationID != plan.MaterializationID ||
		wire.SessionID != session.SessionID || wire.SessionGeneration != session.Generation ||
		wire.TaskChecksum != plan.TaskChecksum || wire.ManifestSHA256 != plan.ManifestSHA256 || wire.MetadataSHA256 != plan.MetadataSHA256 ||
		wire.FileCount != len(wire.Objects) || wire.FileCount <= 0 || wire.FileCount > plan.FileLimit ||
		wire.TotalBytes < 0 || wire.TotalBytes > plan.ByteLimit ||
		issueErr != nil || expiryErr != nil || issued.After(now) || !expires.After(now) || !expires.After(issued) ||
		expires.Sub(issued) > 15*time.Minute || expires.After(session.ExpiresAt) {
		return nil, errors.New("registered bundle capability binding invalid")
	}
	manifest, metadata, err := registeredBundleManifest(wire.Objects, wire.TaskChecksum)
	if err != nil {
		return nil, err
	}
	if fmt.Sprintf("%x", sha256.Sum256(manifest)) != plan.ManifestSHA256 || fmt.Sprintf("%x", sha256.Sum256(metadata)) != plan.MetadataSHA256 {
		return nil, errors.New("registered bundle content identity mismatch")
	}
	var total int64
	seen := make(map[string]bool, len(wire.Objects))
	for _, object := range wire.Objects {
		total += object.SizeBytes // Manifest validation has already bounded all sizes.
		if seen[object.URL] || validateRegisteredBundleURL(object.URL, origin, "/"+plan.Bucket+"/"+plan.Prefix+object.RelativePath, now, expires) != nil {
			return nil, errors.New("registered bundle signed target invalid")
		}
		seen[object.URL] = true
	}
	if total != wire.TotalBytes {
		return nil, errors.New("registered bundle total bytes mismatch")
	}
	return &RegisteredBundleCapability{ManifestSHA256: plan.ManifestSHA256, MetadataSHA256: plan.MetadataSHA256, TotalBytes: total, ExpiresAt: expires, Objects: wire.Objects}, nil
}

func validateRegisteredBundleURL(value string, origin *url.URL, expectedPath string, now, expires time.Time) error {
	if len(value) == 0 || len(value) > 4096 || strings.ContainsAny(value, "\r\n\t") {
		return errors.New("registered bundle URL invalid")
	}
	for _, char := range value {
		if char < 32 {
			return errors.New("registered bundle URL invalid")
		}
	}
	target, err := url.Parse(value)
	if err != nil || target.Scheme != origin.Scheme || target.Host != origin.Host || target.User != nil ||
		target.Fragment != "" || target.Path != expectedPath || target.RawQuery == "" || strings.Count(target.RawQuery, "&") >= 64 {
		return errors.New("registered bundle URL invalid")
	}
	query, err := url.ParseQuery(target.RawQuery)
	if err != nil {
		return errors.New("registered bundle URL query invalid")
	}
	for _, values := range query {
		if len(values) != 1 {
			return errors.New("registered bundle URL query invalid")
		}
	}
	signed, stampErr := time.Parse("20060102T150405Z", query.Get("X-Amz-Date"))
	seconds, secondsErr := strconv.Atoi(query.Get("X-Amz-Expires"))
	if stampErr != nil || secondsErr != nil || seconds <= 0 || seconds > 900 || signed.After(now) ||
		!signed.Add(time.Duration(seconds)*time.Second).Equal(expires) {
		return errors.New("registered bundle URL deadline invalid")
	}
	return nil
}
