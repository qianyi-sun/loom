package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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
			CPUArch: "arm64",
		},
		Guard:        NewGuardClient(socketPath, 8*1024*1024, time.Second),
		Clock:        &fakeClock{now: testNow},
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
						Architecture:   "arm64",
					},
					"sidecar:cache": {
						Path:           filepath.Join(jobRoot, "oci/0001.tar"),
						TopLevelDigest: "sha256:" + strings.Repeat("c", 64),
						FileSHA256:     strings.Repeat("d", 64),
						SizeBytes:      88,
						OS:             "linux",
						Architecture:   "arm64",
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

	server.wantOperations([]string{"project", "exchange", "renew", "claim", "bundle", "start", "release", "finish"})
	if !reflect.DeepEqual(events, []string{"download", "handoff"}) {
		t.Fatalf("events = %#v, want download/handoff", events)
	}
}

type supervisorFlowServer struct {
	t          *testing.T
	listenerFD int
	done       chan struct{}
	maxOps     int
	mu         sync.Mutex
	ops        []string
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
		maxOps:     8,
		jobRoot:    jobRoot,
		buildRoot:  buildRoot,
	}
	go server.serve()
	return server
}

func (s *supervisorFlowServer) close() {
	syscall.Close(s.listenerFD)
	<-s.done
}

func (s *supervisorFlowServer) wantOperations(want []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !reflect.DeepEqual(s.ops, want) {
		s.t.Fatalf("guard operations = %#v, want %#v", s.ops, want)
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
		s.respondSecret(connFD, "claim", []byte(testClaimJSON()), map[string]any{
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
	payload := []byte(fmt.Sprintf(`{"schema_version":2,"grant_id":%q,"session_id":%q,"purpose":"production","shadow_campaign_id":null,"pool_id":"staging-gb10-task-image","cpu_arch":%q,"session_token":%q,"generation":%d,"attestation_generation":%d,"attestation_sha256":%q,"issued_at":"2026-09-03T12:00:00Z","expires_at":"2026-09-03T12:10:00Z"}`,
		testGrantID, testSessionID, runtime.GOARCH, "loom_tibs_"+strings.Repeat(fmt.Sprintf("%d", generation), 64), generation, generation, strings.Repeat("a", 64)))
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
