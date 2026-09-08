package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// Break caught: the orchestrator bypasses the typed GuardClient/local-socket adapter or leaks secret payloads in clear JSON.
func TestSupervisorCrossLanguageLocalSocketFlow(t *testing.T) {
	useTestProtocolPolicy(t)
	root := t.TempDir()
	socketPath := filepath.Join(root, "guard.sock")
	jobRoot := filepath.Join(root, "job")
	buildRoot := filepath.Join(root, "egress")
	if err := os.Mkdir(jobRoot, 0o700); err != nil {
		t.Fatalf("Mkdir(job) error = %v", err)
	}
	if err := os.Mkdir(buildRoot, 0o700); err != nil {
		t.Fatalf("Mkdir(egress) error = %v", err)
	}

	server := startSupervisorFlowServer(t, socketPath, jobRoot, buildRoot)
	defer server.close()

	var events []string
	supervisor := &Orchestrator{
		GrantID: testGrantID,
		Config: Config{
			CPUArch: runtime.GOARCH,
		},
		Guard:        NewGuardClient(socketPath, 8*1024*1024, 2*time.Second),
		Clock:        newManualClock(testNow),
		IdleGrace:    time.Millisecond,
		CleanupGrace: time.Second,
		Download: fakeBundleDownloader(func(ctx context.Context, secret *SecretBuffer, fd int) (*DownloadedBundle, error) {
			if fd < 0 {
				t.Fatal("DownloadBundle received invalid workspace descriptor")
			}
			if strings.Contains(string(secret.data), "X-Amz-Signature") {
				t.Fatalf("bundle capability leaked presigned URL secret to integration fake: %s", secret.data)
			}
			events = append(events, "download")
			return &DownloadedBundle{}, nil
		}),
		NewExecutor: func(Config, *AllocationCapabilities, BuildPlan) (BuildExecutor, error) {
			return &fakeOrchestratorExecutor{
				h:        &orchestratorHarness{events: events},
				buildErr: map[string]error{},
				outputs: map[string]OCIOutput{
					"task": {
						Path:           filepath.Join(jobRoot, "oci/0000.tar"),
						TopLevelDigest: "sha256:" + strings.Repeat("a", 64),
						FileSHA256:     strings.Repeat("b", 64),
						SizeBytes:      77,
						OS:             "linux",
						Architecture:   runtime.GOARCH,
					},
					"sidecar:cache": {
						Path:           filepath.Join(jobRoot, "oci/0001.tar"),
						TopLevelDigest: "sha256:" + strings.Repeat("c", 64),
						FileSHA256:     strings.Repeat("d", 64),
						SizeBytes:      88,
						OS:             "linux",
						Architecture:   runtime.GOARCH,
					},
				},
			}, nil
		},
		Handoff: &captureHandoff{accepted: func(set BuiltComponentSet) {
			events = append(events, "handoff")
			if set.MaterializationID != testMaterializationID || len(set.Components) != 2 {
				t.Fatalf("handoff set = %#v", set)
			}
		}},
		RecordOutcome: func(outcome BuildOutcome) {
			wire, err := outcome.MarshalJSON()
			if err != nil {
				t.Fatalf("outcome marshal error = %v", err)
			}
			for _, forbidden := range []string{"sentinel-secret", "X-Amz-Signature", jobRoot} {
				if strings.Contains(string(wire), forbidden) {
					t.Fatalf("outcome leaked %q in %s", forbidden, wire)
				}
			}
		},
	}

	if err := supervisor.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	server.wantOperations([]string{"project", "exchange", "claim", "bundle", "start", "release", "claim", "claim", "finish"})
	server.wantNoOperations([]string{"registry-credential", "publication-candidate", "fail"})
	if !reflect.DeepEqual(events, []string{"download", "handoff"}) {
		t.Fatalf("events = %#v, want download/handoff", events)
	}
}

// Break caught: the supervisor only passes against a fake Go socket and no longer speaks the real guard-service protocol.
func TestSupervisorExternalGuardFlow(t *testing.T) {
	useTestProtocolPolicy(t)
	socketPath := os.Getenv("LOOM_PHASE2C_SOCKET")
	if socketPath == "" {
		t.Skip("LOOM_PHASE2C_SOCKET not set")
	}
	grantID := os.Getenv("LOOM_PHASE2C_GRANT_ID")
	if !isCanonicalNonZeroUUID(grantID) {
		t.Fatalf("LOOM_PHASE2C_GRANT_ID invalid")
	}
	expectedMaterializationID := os.Getenv("LOOM_PHASE2C_MATERIALIZATION_ID")
	if expectedMaterializationID != "" && !isCanonicalNonZeroUUID(expectedMaterializationID) {
		t.Fatalf("LOOM_PHASE2C_MATERIALIZATION_ID invalid")
	}
	goArch := os.Getenv("LOOM_PHASE2C_GOARCH_OVERRIDE")
	if goArch == "" {
		goArch = runtime.GOARCH
	}
	if _, _, err := authorityArchForGo(goArch); err != nil {
		t.Fatalf("LOOM_PHASE2C_GOARCH_OVERRIDE invalid")
	}
	previous := runtimeGOARCH
	runtimeGOARCH = func() string { return goArch }
	t.Cleanup(func() { runtimeGOARCH = previous })

	clock := newManualClock(time.Date(2026, 9, 2, 14, 0, 0, 0, time.UTC))
	var events []string
	var accepted []BuiltComponentSet
	supervisor := &Orchestrator{
		GrantID: grantID,
		Config: Config{
			CPUArch: goArch,
		},
		Guard:        NewGuardClient(socketPath, 8*1024*1024, 2*time.Second),
		Clock:        clock,
		IdleGrace:    time.Millisecond,
		CleanupGrace: time.Second,
		Download: fakeBundleDownloader(func(ctx context.Context, secret *SecretBuffer, fd int) (*DownloadedBundle, error) {
			if fd < 0 {
				t.Fatal("DownloadBundle received invalid workspace descriptor")
			}
			var capability struct {
				SchemaVersion            string `json:"schema_version"`
				CapabilityID             string `json:"capability_id"`
				GrantID                  string `json:"grant_id"`
				SessionID                string `json:"session_id"`
				SessionGeneration        int    `json:"session_generation"`
				MaterializationID        string `json:"materialization_id"`
				TaskChecksum             string `json:"task_checksum"`
				BundleFileMetadataSHA256 string `json:"bundle_file_metadata_sha256"`
				FileCount                int    `json:"file_count"`
				TotalBytes               int64  `json:"total_bytes"`
				IssuedAt                 string `json:"issued_at"`
				ExpiresAt                string `json:"expires_at"`
				Objects                  []struct {
					RelativePath string `json:"relative_path"`
					SizeBytes    int64  `json:"size_bytes"`
					URL          string `json:"url"`
				} `json:"objects"`
			}
			decoder := json.NewDecoder(strings.NewReader(string(secret.data)))
			decoder.DisallowUnknownFields()
			if err := decoder.Decode(&capability); err != nil {
				t.Fatalf("bundle capability JSON invalid")
			}
			if err := expectJSONEOF(decoder); err != nil {
				t.Fatalf("bundle capability JSON invalid")
			}
			if capability.SchemaVersion != "loom.task-image-bundle-capability.v1" ||
				capability.GrantID != grantID ||
				!isCanonicalNonZeroUUID(capability.CapabilityID) ||
				!isCanonicalNonZeroUUID(capability.SessionID) ||
				capability.SessionGeneration <= 0 ||
				!isCanonicalNonZeroUUID(capability.MaterializationID) ||
				(expectedMaterializationID != "" && capability.MaterializationID != expectedMaterializationID) ||
				!isDigest(capability.TaskChecksum) ||
				!isDigest(capability.BundleFileMetadataSHA256) ||
				capability.FileCount != len(capability.Objects) ||
				capability.TotalBytes <= 0 ||
				capability.IssuedAt == "" ||
				capability.ExpiresAt == "" {
				t.Fatalf("bundle capability binding invalid")
			}
			sawSignedURL := false
			for _, object := range capability.Objects {
				if object.RelativePath == "" || object.SizeBytes < 0 || !strings.HasPrefix(object.URL, "https://objects.example/") {
					t.Fatalf("bundle capability object invalid")
				}
				if strings.Contains(object.URL, "X-Amz-Signature=") {
					sawSignedURL = true
				}
			}
			if !sawSignedURL {
				t.Fatalf("bundle capability did not include signed object URLs")
			}
			events = append(events, "download")
			return &DownloadedBundle{}, nil
		}),
		NewExecutor: func(cfg Config, caps *AllocationCapabilities, plan BuildPlan) (BuildExecutor, error) {
			if cfg.CPUArch != goArch ||
				caps == nil ||
				plan.Architecture != goArch ||
				plan.NetworkMode != "sandbox" ||
				len(plan.Components) != 1 ||
				plan.Components[0].Name != "task" ||
				plan.Components[0].Dockerfile != "environment/Dockerfile" ||
				plan.Components[0].ContextDir != "." {
				return nil, errors.New("external build plan invalid")
			}
			return &externalFlowExecutor{
				clock:  clock,
				events: &events,
				output: OCIOutput{
					Path:           "/tmp/phase2c-external/oci/0000.tar",
					TopLevelDigest: "sha256:" + strings.Repeat("a", 64),
					FileSHA256:     strings.Repeat("b", 64),
					SizeBytes:      77,
					OS:             "linux",
					Architecture:   goArch,
				},
			}, nil
		},
		Handoff: &captureHandoff{accepted: func(set BuiltComponentSet) {
			events = append(events, "handoff")
			accepted = append(accepted, set)
		}},
		RecordOutcome: func(outcome BuildOutcome) {
			wire, err := outcome.MarshalJSON()
			if err != nil {
				t.Fatalf("outcome marshal error = %v", err)
			}
			for _, forbidden := range []string{"loom_tib", "X-Amz-Signature", "objects.example", "/tmp/phase2c-external"} {
				if strings.Contains(string(wire), forbidden) {
					t.Fatalf("outcome leaked sensitive detail")
				}
			}
		},
	}

	if err := supervisor.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if !reflect.DeepEqual(events, []string{"download", "executor_start", "build:task", "handoff", "executor_close"}) {
		t.Fatalf("events = %#v, want single external build flow", events)
	}
	if len(accepted) != 1 {
		t.Fatalf("handoff accepted %d sets, want 1", len(accepted))
	}
	set := accepted[0]
	if set.GrantID != grantID ||
		(expectedMaterializationID != "" && set.MaterializationID != expectedMaterializationID) ||
		set.AttemptID == "" ||
		set.LeaseEpoch <= 0 ||
		len(set.Components) != 1 ||
		set.Components[0].Name != "task" {
		t.Fatalf("handoff set binding invalid")
	}
}

type externalFlowExecutor struct {
	clock  *manualClock
	events *[]string
	output OCIOutput
}

func (e *externalFlowExecutor) Start(ctx context.Context) error {
	*e.events = append(*e.events, "executor_start")
	return nil
}

func (e *externalFlowExecutor) Build(ctx context.Context, component BuildComponent) (OCIOutput, error) {
	*e.events = append(*e.events, "build:"+component.Name)
	if component.Name != "task" {
		return OCIOutput{}, errors.New("unexpected component")
	}
	e.clock.advance(31 * time.Second)
	time.Sleep(10 * time.Millisecond)
	return e.output, nil
}

func (e *externalFlowExecutor) Close(ctx context.Context) error {
	*e.events = append(*e.events, "executor_close")
	return nil
}

type supervisorFlowServer struct {
	t          *testing.T
	listenerFD int
	done       chan struct{}
	maxOps     int
	mu         sync.Mutex
	ops        []string
	claims     int
	jobRoot    string
	buildRoot  string
}

func startSupervisorFlowServer(t *testing.T, socketPath string, jobRoot string, buildRoot string) *supervisorFlowServer {
	t.Helper()
	listenerFD, err := syscall.Socket(syscall.AF_UNIX, syscall.SOCK_SEQPACKET|syscall.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatalf("Socket() error = %v", err)
	}
	if err := syscall.Bind(listenerFD, &syscall.SockaddrUnix{Name: socketPath}); err != nil {
		t.Fatalf("Bind() error = %v", err)
	}
	if err := syscall.Listen(listenerFD, 16); err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	server := &supervisorFlowServer{
		t:          t,
		listenerFD: listenerFD,
		done:       make(chan struct{}),
		maxOps:     9,
		jobRoot:    jobRoot,
		buildRoot:  buildRoot,
	}
	go server.serve()
	return server
}

func (s *supervisorFlowServer) close() {
	syscall.Close(s.listenerFD)
	select {
	case <-s.done:
	case <-time.After(time.Second):
	}
}

func (s *supervisorFlowServer) wantOperations(want []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !reflect.DeepEqual(s.ops, want) {
		s.t.Fatalf("guard operations = %#v, want %#v", s.ops, want)
	}
}

func (s *supervisorFlowServer) wantNoOperations(forbidden []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, operation := range s.ops {
		for _, denied := range forbidden {
			if operation == denied {
				s.t.Fatalf("guard operation %q occurred in inert publication flow: %#v", denied, s.ops)
			}
		}
	}
}

func (s *supervisorFlowServer) serve() {
	defer close(s.done)
	for {
		connFD, _, err := syscall.Accept4(s.listenerFD, syscall.SOCK_CLOEXEC)
		if err != nil {
			return
		}
		s.handle(connFD)
		syscall.Close(connFD)
		s.mu.Lock()
		done := len(s.ops) >= s.maxOps
		s.mu.Unlock()
		if done {
			return
		}
	}
}

func (s *supervisorFlowServer) handle(connFD int) {
	if err := syscall.SetsockoptInt(connFD, syscall.SOL_SOCKET, syscall.SO_PASSCRED, 1); err != nil {
		s.t.Errorf("SetsockoptInt() error = %v", err)
		return
	}
	payload, rights, _, _ := receiveSeqpacket(s.t, connFD, 8*1024*1024)
	defer closeRights(rights)
	var request map[string]any
	if err := json.Unmarshal(payload, &request); err != nil {
		s.t.Errorf("request JSON invalid: %v", err)
		return
	}
	operation, _ := request["operation"].(string)
	s.mu.Lock()
	s.ops = append(s.ops, operation)
	s.mu.Unlock()
	for _, forbidden := range []string{"sentinel-secret-bootstrap", "loom_tibs_"} {
		if strings.Contains(string(payload), forbidden) {
			s.t.Errorf("request leaked %q in %s", forbidden, payload)
		}
	}
	switch operation {
	case "project":
		s.respondProject(connFD)
	case "exchange":
		s.respondSession(connFD, 1)
	case "renew":
		s.respondSession(connFD, 2)
	case "claim":
		s.claims++
		if s.claims > 1 {
			s.respondAckOnly(connFD, "claim", map[string]any{
				"operation_id": request["operation_id"],
				"available":    false,
			})
			return
		}
		claim := defaultClaimMutation()
		if runtime.GOARCH == "amd64" {
			claim.CPUArch = "x86_64"
			claim.Platform = "linux/amd64"
		}
		if id, ok := request["operation_id"].(string); ok {
			claim.ClaimID = id
		}
		s.respondSecret(connFD, "claim", []byte(testClaimJSON(claim)), map[string]any{
			"operation_id": request["operation_id"],
		})
	case "bundle":
		s.respondSecret(connFD, "bundle", []byte(`{"schema":"loom.task-image-bundle-capability/v1","objects":[]}`), map[string]any{
			"operation_id":       request["operation_id"],
			"materialization_id": testMaterializationID,
			"attempt_id":         testAttemptID,
			"lease_epoch":        1,
		})
	case "start", "release":
		s.respondLease(connFD, operation, request)
	case "finish":
		s.respondAckOnly(connFD, "finishing", map[string]any{"operation_id": request["operation_id"]})
	default:
		s.t.Errorf("unexpected operation %q", operation)
	}
}

func (s *supervisorFlowServer) respondProject(connFD int) {
	bootstrap := createMemfdFixture(s.t, "bootstrap", []byte(`{"bootstrap_token":"sentinel-secret-bootstrap"}`), requiredMemfdSeals, true)
	jobFD := openDirectoryFD(s.t, s.jobRoot)
	buildFD := openDirectoryFD(s.t, s.buildRoot)
	defer syscall.Close(bootstrap)
	defer syscall.Close(jobFD)
	defer syscall.Close(buildFD)
	jobStat := mustFstat(s.t, jobFD)
	buildStat := mustFstat(s.t, buildFD)
	s.respond(connFD, map[string]any{
		"schema":                        localSchema,
		"operation":                     "projected",
		"response_id":                   "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
		"grant_id":                      testGrantID,
		"proof_sha256":                  strings.Repeat("8", 64),
		"receipt_public_binding_sha256": strings.Repeat("9", 64),
		"rights": []map[string]any{
			{"index": 0, "kind": "sealed_memfd", "role": "bootstrap", "device": 0, "inode": 0},
			{"index": 1, "kind": "directory", "role": "job_storage", "device": uint64(jobStat.Dev), "inode": uint64(jobStat.Ino)},
			{"index": 2, "kind": "directory", "role": "build_egress", "device": uint64(buildStat.Dev), "inode": uint64(buildStat.Ino)},
		},
	}, []int{bootstrap, jobFD, buildFD})
}

func (s *supervisorFlowServer) respondSession(connFD int, generation int) {
	authorityArch, _, err := authorityArchForGo(runtime.GOARCH)
	if err != nil {
		s.t.Errorf("authorityArchForGo() error = %v", err)
		return
	}
	payload := []byte(fmt.Sprintf(`{"schema_version":2,"grant_id":%q,"session_id":%q,"purpose":"production","shadow_campaign_id":null,"pool_id":"staging-gb10-task-image","cpu_arch":%q,"session_token":%q,"generation":%d,"attestation_generation":%d,"attestation_sha256":%q,"issued_at":"2026-09-03T12:00:00Z","expires_at":"2026-09-03T12:10:00Z"}`,
		testGrantID, testSessionID, authorityArch, "loom_tibs_"+strings.Repeat(fmt.Sprintf("%d", generation), 64), generation, generation, strings.Repeat("a", 64)))
	fd := createMemfdFixture(s.t, "session", payload, requiredMemfdSeals, true)
	defer syscall.Close(fd)
	s.respond(connFD, map[string]any{
		"schema":                        localSchema,
		"operation":                     "session",
		"response_id":                   fmt.Sprintf("bbbbbbbb-bbbb-4bbb-8bb%d-bbbbbbbbbbbb", generation),
		"grant_id":                      testGrantID,
		"session_id":                    testSessionID,
		"session_generation":            generation,
		"session_public_binding_sha256": strings.Repeat("a", 64),
	}, []int{fd})
}

func (s *supervisorFlowServer) respondSecret(connFD int, operation string, payload []byte, extra map[string]any) {
	fd := createMemfdFixture(s.t, operation, payload, requiredMemfdSeals, true)
	defer syscall.Close(fd)
	sum := sha256.Sum256(payload)
	response := map[string]any{
		"schema":         localSchema,
		"operation":      operation,
		"response_id":    "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
		"grant_id":       testGrantID,
		"payload_sha256": hex.EncodeToString(sum[:]),
	}
	for key, value := range extra {
		response[key] = value
	}
	s.respond(connFD, response, []int{fd})
}

func (s *supervisorFlowServer) respondLease(connFD int, operation string, request map[string]any) {
	expiry := any(nil)
	state := "queued"
	if operation == "start" {
		expiry = "2026-09-03T12:01:30Z"
		state = "running"
	}
	s.respond(connFD, map[string]any{
		"schema":                      localSchema,
		"operation":                   operation,
		"response_id":                 "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
		"grant_id":                    testGrantID,
		"operation_id":                request["operation_id"],
		"materialization_id":          testMaterializationID,
		"attempt_id":                  testAttemptID,
		"lease_epoch":                 1,
		"state":                       state,
		"deterministic_failure_count": 0,
		"lease_expires_at":            expiry,
	}, nil)
}

func (s *supervisorFlowServer) respondAckOnly(connFD int, operation string, extra map[string]any) {
	response := map[string]any{
		"schema":      localSchema,
		"operation":   operation,
		"response_id": "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee",
		"grant_id":    testGrantID,
	}
	for key, value := range extra {
		response[key] = value
	}
	s.respond(connFD, response, nil)
}

func (s *supervisorFlowServer) respond(connFD int, response map[string]any, rights []int) {
	payload, err := encodeCanonicalJSON(response)
	if err != nil {
		s.t.Errorf("response JSON error = %v", err)
		return
	}
	sendSeqpacket(s.t, connFD, payload, rights)
	ack, ackRights, _, _ := receiveSeqpacket(s.t, connFD, 4096)
	closeRights(ackRights)
	var ackDoc map[string]any
	if err := json.Unmarshal(ack, &ackDoc); err != nil {
		s.t.Errorf("ack JSON invalid: %v", err)
		return
	}
	if ackDoc["operation"] != "ack" || ackDoc["response_id"] != response["response_id"] {
		s.t.Errorf("ack = %s for response %#v", ack, response)
	}
}

type captureHandoff struct {
	accepted func(BuiltComponentSet)
}

func (h *captureHandoff) Accept(ctx context.Context, set BuiltComponentSet) error {
	h.accepted(set)
	return nil
}
