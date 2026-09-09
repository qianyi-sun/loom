package main

import (
	"context"
	"crypto/sha256"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

type nativeOrchestratorGuard struct {
	*fakeOrchestratorGuard
	plan       RegisteredBundlePlan
	capability []byte
	jobFD      int
	mode       string
}

func (g *nativeOrchestratorGuard) Project(ctx context.Context, grant string) (*AllocationCapabilities, error) {
	caps, err := g.fakeOrchestratorGuard.Project(ctx, grant)
	if err != nil {
		return nil, err
	}
	caps.JobDirectoryFD, err = fcntlInt(g.jobFD, syscall.F_DUPFD_CLOEXEC, 0)
	return caps, err
}

func (g *nativeOrchestratorGuard) Claim(ctx context.Context, grant, operation string, secret *SecretBuffer) (*SecretBuffer, bool, error) {
	legacy, available, err := g.fakeOrchestratorGuard.Claim(ctx, grant, operation, secret)
	if err != nil || !available {
		return legacy, available, err
	}
	defer legacy.Close()
	var wire map[string]any
	if err := json.Unmarshal(legacy.data, &wire); err != nil {
		return nil, false, err
	}
	wire["lease_expires_at"] = g.leaseExpires.Format(time.RFC3339)
	plan := wire["plan"].(map[string]any)
	plan["schema_version"], plan["task_checksum"] = "loom.task-image-build-plan.v2", g.plan.TaskChecksum
	plan["bundle_content_manifest_sha256"], plan["bundle_file_metadata_sha256"] = g.plan.ManifestSHA256, g.plan.MetadataSHA256
	plan["bundle_prefix"], plan["authorization_expires_at"] = g.plan.Prefix, g.sessionExpires.Format(time.RFC3339)
	for _, raw := range plan["components"].([]any) {
		component := raw.(map[string]any)
		component["dockerfile_path"], component["context_path"] = "Dockerfile", "."
	}
	payload, err := json.Marshal(wire)
	return &SecretBuffer{data: payload}, true, err
}

func (g *nativeOrchestratorGuard) Bundle(context.Context, string, string, string, string, int, *SecretBuffer) (*SecretBuffer, error) {
	g.h.events = append(g.h.events, "native_bundle")
	return &SecretBuffer{data: append([]byte(nil), g.capability...)}, nil
}

func (g *nativeOrchestratorGuard) Start(ctx context.Context, grant, op, materialization, attempt string, epoch int, secret *SecretBuffer) (*LeaseResponse, error) {
	lease, err := g.fakeOrchestratorGuard.Start(ctx, grant, op, materialization, attempt, epoch, secret)
	if g.mode == "expired_start" {
		expires := g.h.clock.Now().Add(-time.Second)
		lease.LeaseExpiresAt = &expires
	}
	if g.mode == "start_identity" {
		lease.OperationID = testAttemptID
	}
	switch g.mode {
	case "start_operation":
		lease.Operation = "heartbeat"
	case "start_grant":
		lease.GrantID = testAttemptID
	case "start_attempt":
		lease.AttemptID = testGrantID
	case "start_materialization":
		lease.MaterializationID = testGrantID
	case "start_epoch":
		lease.LeaseEpoch++
	case "start_state":
		lease.State = "claimed"
	case "start_nil":
		return nil, nil
	case "start_no_expiry":
		lease.LeaseExpiresAt = nil
	case "start_session_expired":
		g.h.clock.advance(61 * time.Second)
	case "start_clock_regression":
		g.h.clock.advance(-time.Second)
	}
	return lease, err
}

type nativeCheckingExecutor struct {
	*fakeOrchestratorExecutor
	input       *os.File
	t           *testing.T
	retainInput bool
}

func (e *nativeCheckingExecutor) Build(ctx context.Context, component BuildComponent) (BuildResult, error) {
	if _, err := HashFileAt(int(e.input.Fd()), "Dockerfile"); err != nil {
		e.t.Error("verified input removed before build")
	}
	return e.fakeOrchestratorExecutor.Build(ctx, component)
}
func (e *nativeCheckingExecutor) Close(ctx context.Context) error {
	if _, err := HashFileAt(int(e.input.Fd()), "Dockerfile"); err != nil {
		e.t.Error("verified input removed before executor close")
	}
	err := e.fakeOrchestratorExecutor.Close(ctx)
	if err == nil && !e.retainInput {
		err = e.input.Close()
	}
	return err
}

func TestNativeOrchestratorConnectsV2TLSInputToExecutorAndOwnsCleanup(t *testing.T) {
	for _, mode := range []string{"success", "missing_trust", "missing_factory", "download_failure", "expired_start", "start_identity", "close_failure",
		"start_operation", "start_grant", "start_attempt", "start_materialization", "start_epoch", "start_state", "start_nil", "start_no_expiry", "start_session_expired", "start_clock_regression", "build_cancel", "build_unjoined"} {
		t.Run(mode, func(t *testing.T) {
			data := []byte("FROM scratch\n")
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if mode == "download_failure" {
					w.WriteHeader(503)
					return
				}
				if r.URL.RawQuery == "" {
					t.Error("lost signed target")
				}
				_, _ = w.Write(data)
			}))
			defer server.Close()
			h := newOrchestratorHarness(t)
			now := time.Now().UTC().Truncate(time.Second)
			h.clock = newManualClock(now)
			h.guard.sessionExpires, h.guard.leaseExpires = now.Add(time.Minute), now.Add(90*time.Second)
			h.guard.claimAvailability = []bool{true, false, false}
			files := []RegisteredBundleObject{{RelativePath: "Dockerfile", SizeBytes: int64(len(data)), SHA256: fmt.Sprintf("%x", sha256.Sum256(data)), Mode: "0644"}}
			manifest, metadata, err := registeredBundleManifest(files, strings.Repeat("4", 64))
			if err != nil {
				t.Fatal(err)
			}
			plan := RegisteredBundlePlan{GrantID: testGrantID, MaterializationID: testMaterializationID, TaskChecksum: strings.Repeat("4", 64),
				ManifestSHA256: fmt.Sprintf("%x", sha256.Sum256(manifest)), MetadataSHA256: fmt.Sprintf("%x", sha256.Sum256(metadata)),
				Bucket: "loom-bundles", FileLimit: 2000, ByteLimit: maxTaskImageBuildBundleBytes}
			plan.Prefix = "native/" + plan.ManifestSHA256 + "/"
			files[0].URL = server.URL + "/" + plan.Bucket + "/" + plan.Prefix + "Dockerfile?X-Amz-Date=" + now.Format("20060102T150405Z") + "&X-Amz-Expires=40&X-Amz-Signature=" + strings.Repeat("a", 64)
			wire := registeredBundleWire{SchemaVersion: "loom.task-image-bundle-capability.v2", CapabilityID: uuidWithTail(104),
				GrantID: testGrantID, SessionID: testSessionID, SessionGeneration: 1, MaterializationID: testMaterializationID,
				TaskChecksum: plan.TaskChecksum, MetadataSHA256: plan.MetadataSHA256, ManifestSHA256: plan.ManifestSHA256,
				FileCount: 1, TotalBytes: int64(len(data)), IssuedAt: now.Format(time.RFC3339), ExpiresAt: now.Add(40 * time.Second).Format(time.RFC3339), Objects: files}
			payload, err := json.Marshal(wire)
			if err != nil {
				t.Fatal(err)
			}
			job := t.TempDir()
			if err := os.WriteFile(filepath.Join(job, "foreign"), []byte("preserve"), 0o600); err != nil {
				t.Fatal(err)
			}
			jobFD := openDirectoryFD(t, job)
			defer syscall.Close(jobFD)
			guard := &nativeOrchestratorGuard{fakeOrchestratorGuard: h.guard, plan: plan, capability: payload, jobFD: jobFD, mode: mode}
			o := h.orchestrator()
			o.Guard = guard
			roots := x509.NewCertPool()
			roots.AddCert(server.Certificate())
			o.Config.Bundle = &BundleDownloadTrust{Origin: server.URL, Bucket: plan.Bucket, Roots: roots}
			o.NewExecutor = func(Config, *AllocationCapabilities, BuildPlan) (BuildExecutor, error) {
				t.Error("strong claim used legacy executor")
				return nil, errors.New("legacy forbidden")
			}
			o.Download = fakeBundleDownloader(func(context.Context, *SecretBuffer, int) (*DownloadedBundle, error) {
				t.Error("strong claim used legacy download")
				return nil, errors.New("legacy forbidden")
			})
			created := false
			o.NewRegisteredExecutor = func(_ Config, _ *AllocationCapabilities, _ BuildPlan, fd int) (BuildExecutor, error) {
				created = true
				input, err := duplicatePrivateInputDirectory(fd)
				if err != nil {
					return nil, err
				}
				t.Cleanup(func() { input.Close() })
				return &nativeCheckingExecutor{fakeOrchestratorExecutor: h.executor, input: input, t: t, retainInput: mode == "build_unjoined"}, nil
			}
			if mode == "missing_trust" {
				o.Config.Bundle = nil
			}
			if mode == "missing_factory" {
				o.NewRegisteredExecutor = nil
			}
			if mode == "close_failure" {
				h.executor.closeErr = errors.New("unproven cgroup cleanup")
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			release, returned := make(chan struct{}), make(chan struct{})
			if mode == "build_cancel" || mode == "build_unjoined" {
				o.CleanupGrace = 30 * time.Millisecond
				h.executor.blockBuild = func(context.Context, string) (OCIOutput, error) {
					defer close(returned)
					cancel()
					if mode == "build_unjoined" {
						<-release
					} else {
						<-h.executor.closed()
					}
					return OCIOutput{}, context.Canceled
				}
			}
			err = o.Run(ctx)
			if mode == "build_unjoined" {
				defer func() { close(release); <-returned }()
				if !errors.Is(err, errCleanupAmbiguous) {
					t.Fatal("unjoined input consumer accepted clean cleanup")
				}
			}
			if mode == "success" {
				if err != nil || !created {
					t.Fatalf("native composition failed: %v", err)
				}
			} else if err == nil {
				t.Fatal("invalid native preparation accepted")
			}
			if mode != "success" && mode != "close_failure" && mode != "build_cancel" && mode != "build_unjoined" && created {
				t.Fatal("executor started without fresh inputs/start")
			}
			entries, readErr := os.ReadDir(job)
			if readErr != nil {
				t.Fatal(readErr)
			}
			want := 1
			if mode == "close_failure" || mode == "build_unjoined" {
				want = 2
				if mode == "close_failure" {
					h.wantOutcome(t, BuildOutcomeContainmentFailure, "cleanup_failed")
				}
				for _, event := range h.events {
					if event == "finish" {
						t.Fatal("ambiguous native cleanup reported clean finish")
					}
					if mode == "build_unjoined" && event == "caps_close" {
						t.Fatal("closed capabilities borrowed by unjoined build")
					}
				}
			}
			if len(entries) != want {
				t.Fatalf("input cleanup entries=%d want=%d", len(entries), want)
			}
			if mode == "download_failure" {
				h.wantOutcome(t, BuildOutcomeTransientFailure, "bundle_download_failed")
			}
		})
	}
}
