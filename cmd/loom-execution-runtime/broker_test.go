package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestLoopbackLedgerBindsPodIdentityAndRejectsCallerScope(t *testing.T) {
	tokenFile := filepath.Join(t.TempDir(), "pod-token")
	if err := os.WriteFile(tokenFile, []byte("pod-bound-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.URL.Path != "/internal/service-execution/llm-calls" || r.URL.RawQuery != "" ||
			r.Header.Get("Authorization") != "Bearer pod-bound-token" ||
			r.Header.Get("X-Loom-Execution-Lease-Id") != "bound-lease" ||
			r.Header.Get("X-Loom-Execution-Generation") != "3" ||
			r.Header.Get("X-Loom-Execution-Role") != "attempt" {
			t.Errorf("ledger request identity was not broker-owned")
		}
		_, _ = w.Write([]byte(`{"trial_id":"bound-trial","team_id":"bound-team","step_id":"agent","items":[]}`))
	}))
	defer server.Close()
	root, _ := url.Parse(server.URL + "/internal/service-execution")
	broker := &workloadBroker{root: root, client: server.Client(), podTokenFile: tokenFile,
		identity: workloadIdentity{LeaseID: "bound-lease", Generation: 3, ExecutionRole: "attempt"}}
	proxy, stop, err := broker.startProxy(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stop() }()
	for _, test := range []struct {
		method, suffix string
		status         int
	}{
		{http.MethodGet, "", http.StatusOK},
		{http.MethodGet, "?trial_id=foreign", http.StatusForbidden},
		{http.MethodPost, "", http.StatusForbidden},
	} {
		request, _ := http.NewRequest(test.method, proxy+"/internal/loom/llm-calls"+test.suffix, nil)
		request.Header.Set("Authorization", "Bearer attacker-token")
		request.Header.Set("X-Loom-Execution-Lease-Id", "foreign-lease")
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		_ = response.Body.Close()
		if response.StatusCode != test.status {
			t.Errorf("%s %s: got %d", test.method, test.suffix, response.StatusCode)
		}
	}
	if calls != 1 {
		t.Errorf("unscoped requests reached broker: %d", calls)
	}
}

func TestLoopbackLedgerDoesNotExposeUpstreamErrorBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "private-upstream-detail", http.StatusForbidden)
	}))
	defer server.Close()
	root, _ := url.Parse(server.URL)
	broker := &workloadBroker{root: root, client: server.Client(), identity: workloadIdentity{ExecutionRole: "attempt"}}
	request := httptest.NewRequest(http.MethodGet, "/internal/loom/llm-calls", nil)
	response := httptest.NewRecorder()
	broker.serveCallLedger(response, request)
	if response.Code != http.StatusBadGateway || strings.Contains(response.Body.String(), "private-upstream-detail") {
		t.Fatal("ledger leaked upstream failure details")
	}
}

func TestModelProxyWaitsBeyondBrokerOperationTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		time.Sleep(80 * time.Millisecond)
		if r.URL.Path == "/v1/responses" {
			http.Error(w, "upstream deadline reached", http.StatusGatewayTimeout)
			return
		}
		_, _ = w.Write([]byte(`{"choices":[]}`))
	}))
	defer server.Close()
	root, _ := url.Parse(server.URL + "/internal/service-execution")
	client := server.Client()
	client.Timeout = 20 * time.Millisecond
	broker := &workloadBroker{root: root, client: client, token: "step-token", expires: time.Now().Add(time.Hour)}
	proxyURL, stop, err := broker.startProxy(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stop() }()
	for path, want := range map[string]int{"/v1/chat/completions": http.StatusOK, "/v1/responses": http.StatusGatewayTimeout} {
		response, err := http.Post(proxyURL+path, "application/json", strings.NewReader("{}"))
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if response.StatusCode != want {
			t.Errorf("%s: got %d %s, want %d", path, response.StatusCode, body, want)
		}
	}
	// Token/input/output operations keep the original finite client budget.
	err = broker.doJSON(context.Background(), http.MethodGet, broker.endpoint("/slow"), nil, nil, nil)
	var timeout interface{ Timeout() bool }
	if !errors.As(err, &timeout) || !timeout.Timeout() {
		t.Fatalf("non-model operation lost its timeout: %v", err)
	}
}

func TestModelProxyPhaseProcessCancellationReachesUpstream(t *testing.T) {
	started, cancelled := make(chan struct{}), make(chan struct{})
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		close(started)
		select {
		case <-r.Context().Done():
			close(cancelled)
		case <-release:
		}
	}))
	defer func() {
		close(release)
		server.Close()
	}()
	root, _ := url.Parse(server.URL + "/internal/service-execution")
	broker := &workloadBroker{root: root, client: server.Client(), token: "step-token", expires: time.Now().Add(time.Hour)}
	proxyURL, stop, err := broker.startProxy(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stop() }()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		select {
		case <-started:
			cancel()
		case <-ctx.Done():
		}
	}()
	workspace := t.TempDir()
	evidence, err := runPhase(ctx, phase{
		Role: "agent", Argv: []string{os.Args[0], "-test.run=^TestModelProxyPhaseHelper$"},
		WorkingDirectory: workspace, TimeoutSeconds: 3,
		Environment: map[string]string{"LOOM_TEST_PROXY_URL": proxyURL},
	}, 1, workspace, t.TempDir(), 4096, 50*time.Millisecond, nil)
	if !errors.Is(err, context.Canceled) || evidence.ExitCode == 0 {
		t.Fatalf("phase process did not stop: evidence=%+v err=%v", evidence, err)
	}
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("upstream model request survived the phase process")
	}
}

func TestModelProxyPhaseHelper(t *testing.T) {
	endpoint := os.Getenv("LOOM_TEST_PROXY_URL")
	if endpoint == "" {
		return
	}
	response, err := http.Post(endpoint+"/v1/chat/completions", "application/json", strings.NewReader("{}"))
	if err != nil {
		os.Exit(2)
	}
	_ = response.Body.Close()
	os.Exit(0)
}

func TestWorkloadBrokerRefreshProxyAndDurableOutputCommit(t *testing.T) {
	leaseID := "0194d739-8bec-7b7b-88f5-62f7cbd42cb3"
	sessionID := "0194d739-8bec-7b7b-88f5-62f7cbd42cb4"
	podTokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(podTokenFile, []byte("pod-0"), 0o600); err != nil {
		t.Fatal(err)
	}
	podCalls := 0
	var mu sync.Mutex
	tokenCalls := 0
	parts := map[string][]byte{}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		if strings.HasPrefix(request.URL.Path, "/internal/service-execution/") {
			if request.Header.Get("Authorization") != fmt.Sprintf("Bearer pod-%d", podCalls) {
				t.Error("broker did not use the current projected Pod token")
				http.Error(writer, "unauthorized", http.StatusUnauthorized)
				return
			}
			podCalls++
			// Atomic replacement models kubelet projected-volume rotation.
			next := podTokenFile + ".next"
			if err := os.WriteFile(next, []byte(fmt.Sprintf("pod-%d", podCalls)), 0o600); err != nil {
				t.Error(err)
			}
			if err := os.Rename(next, podTokenFile); err != nil {
				t.Error(err)
			}
		}
		switch {
		case request.URL.Path == "/internal/service-execution/token":
			tokenCalls++
			if tokenCalls == 1 {
				http.Error(writer, `{"detail":"workload_identity_not_observed"}`, http.StatusServiceUnavailable)
				return
			}
			_ = json.NewEncoder(writer).Encode(map[string]any{
				"schema_version": "loom.service-execution-token.v1",
				"token":          "step-token",
				"expires_at":     time.Now().Add(10 * time.Minute).UTC(),
				"step_jwt_id":    "0194d739-8bec-7b7b-88f5-62f7cbd42cb5",
			})
		case request.URL.Path == "/openai/v1/chat/completions":
			if request.Header.Get("Authorization") != "Bearer step-token" {
				http.Error(writer, "missing step token", http.StatusUnauthorized)
				return
			}
			_ = json.NewEncoder(writer).Encode(map[string]any{"data": []any{}})
		case request.URL.Path == "/internal/service-execution/outputs/prepare":
			var prepared outputPrepare
			if err := json.NewDecoder(request.Body).Decode(&prepared); err != nil {
				t.Fatal(err)
			}
			plans := make([]map[string]any, len(prepared.Files))
			for index, file := range prepared.Files {
				plans[index] = map[string]any{
					"file_index": index, "relative_path": file.RelativePath,
					"expected_max_bytes": 1048576,
				}
			}
			writer.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(writer).Encode(map[string]any{
				"schema_version":    "loom.upload-session-grant.v1",
				"upload_session_id": sessionID, "state": "uploading",
				"upload_token":     strings.Repeat("u", 48),
				"token_expires_at": time.Now().Add(10 * time.Minute).UTC(),
				"files":            plans,
			})
		case request.Method == http.MethodPut && strings.Contains(request.URL.Path, "/parts/"):
			payload, err := io.ReadAll(request.Body)
			if err != nil {
				t.Fatal(err)
			}
			digest := sha256.Sum256(payload)
			sha := "sha256:" + hex.EncodeToString(digest[:])
			if request.Header.Get("X-Loom-Content-SHA256") != sha {
				http.Error(writer, "digest drift", http.StatusConflict)
				return
			}
			segments := strings.Split(request.URL.Path, "/")
			fileIndex, _ := strconv.Atoi(segments[len(segments)-3])
			partNumber, _ := strconv.Atoi(segments[len(segments)-1])
			mu.Lock()
			parts[request.URL.Path] = payload
			mu.Unlock()
			_ = json.NewEncoder(writer).Encode(partReceipt{
				FileIndex: fileIndex, PartNumber: partNumber,
				SizeBytes: int64(len(payload)), SHA256: sha,
			})
		case strings.HasSuffix(request.URL.Path, "/complete"):
			_ = json.NewEncoder(writer).Encode(map[string]any{"state": "uploaded"})
		case strings.HasSuffix(request.URL.Path, "/commit"):
			_ = json.NewEncoder(writer).Encode(map[string]any{
				"upload_session_id": sessionID, "state": "committed", "artifacts": []any{},
				"manifest_sha256":         "sha256:" + strings.Repeat("1", 64),
				"committed_marker_sha256": "sha256:" + strings.Repeat("2", 64),
			})
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()

	root, err := url.Parse(server.URL + "/internal/service-execution")
	if err != nil {
		t.Fatal(err)
	}
	broker := &workloadBroker{
		podTokenFile: podTokenFile,
		root:         root,
		identity:     workloadIdentity{LeaseID: leaseID, Generation: 7, ExecutionRole: "attempt"},
		client:       server.Client(),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	proxyURL, stop, err := broker.startProxy(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stop() }()
	response, err := http.Post( // #nosec G107 -- local test listener
		proxyURL+"/openai/v1/chat/completions", "application/json", strings.NewReader("{}"),
	)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK || tokenCalls != 2 {
		t.Fatalf("proxy did not retry and inject one refreshed token: status=%d calls=%d", response.StatusCode, tokenCalls)
	}
	denied, err := http.Post( // #nosec G107 -- local test listener
		proxyURL+"/internal/service-execution/token", "application/json", strings.NewReader("{}"),
	)
	if err != nil {
		t.Fatal(err)
	}
	_ = denied.Body.Close()
	if denied.StatusCode != http.StatusForbidden || tokenCalls != 2 {
		t.Fatalf("proxy exposed a non-model Gateway route: status=%d calls=%d", denied.StatusCode, tokenCalls)
	}

	output := t.TempDir()
	if err := os.WriteFile(filepath.Join(output, "result.json"), []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(output, "01-agent.stdout"), []byte("hello\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(output, "artifacts"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(output, "artifacts", "answer.txt"), []byte("42\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	evidence, err := broker.commitOutputs(ctx, output, broker.outputRequestID())
	if err != nil {
		t.Fatal(err)
	}
	if evidence.UploadSessionID != sessionID || len(parts) != 3 {
		t.Fatalf("output was not completely committed: evidence=%#v parts=%d", evidence, len(parts))
	}
	if broker.outputRequestID() != broker.outputRequestID() {
		t.Fatal("output idempotency key is not deterministic")
	}
}

func TestInventoryOutputsRejectsSymlink(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(t.TempDir(), "secret")
	if err := os.WriteFile(target, []byte("not runtime output"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "result.json"), []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(root, "attempt.stdout")); err != nil {
		t.Fatal(err)
	}
	if _, err := inventoryOutputs(root); err == nil {
		t.Fatal("expected output symlink to be rejected")
	}
}

func TestProjectedPodIdentityInputsRotateAndEnvironmentIsCleared(t *testing.T) {
	tokenFile := filepath.Join(t.TempDir(), "token")
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		calls++
		if request.Header.Get("Authorization") != fmt.Sprintf("Bearer input-%d", calls) ||
			request.Header.Get("X-Loom-Execution-Lease-Id") != "0194d739-8bec-7b7b-88f5-62f7cbd42cb3" {
			t.Error("input request did not carry current Pod identity and lease")
		}
		_, _ = writer.Write([]byte("input"))
	}))
	defer server.Close()
	t.Setenv("LOOM_EXECUTION_BROKER_URL", server.URL+"/internal/service-execution")
	t.Setenv("LOOM_EXECUTION_LEASE_ID", "0194d739-8bec-7b7b-88f5-62f7cbd42cb3")
	t.Setenv("LOOM_EXECUTION_GENERATION", "1")
	t.Setenv("LOOM_EXECUTION_ROLE", "attempt")
	t.Setenv("LOOM_EXECUTION_POD_TOKEN_FILE", tokenFile)
	broker, err := workloadBrokerFromEnvironment()
	if err != nil {
		t.Fatal(err)
	}
	if os.Getenv("LOOM_EXECUTION_POD_TOKEN_FILE") != "" {
		t.Fatal("Pod token path leaked into child environment")
	}
	for index, path := range []string{"/inputs/manifest", "/inputs/files/0"} {
		next := tokenFile + ".next"
		if err := os.WriteFile(next, []byte(fmt.Sprintf("input-%d\n", index+1)), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(next, tokenFile); err != nil {
			t.Fatal(err)
		}
		response, err := broker.getInput(context.Background(), broker.endpoint(path))
		if err != nil {
			t.Fatal(err)
		}
		_ = response.Body.Close()
	}
	if calls != 2 {
		t.Fatalf("input calls = %d", calls)
	}
}

func TestConfiguredPodIdentityFailsClosedBeforeAnyBrokerRequest(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		calls++
		writer.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	root, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(t.TempDir(), "token")
	broker := &workloadBroker{root: root, client: server.Client(), podTokenFile: file}
	operations := map[string]func() error{
		"token": func() error {
			return broker.doJSON(context.Background(), http.MethodPost, broker.endpoint("/token"), nil, nil, nil)
		},
		"prepare": func() error {
			return broker.doJSON(context.Background(), http.MethodPost, broker.endpoint("/outputs/prepare"), nil, nil, nil)
		},
		"complete": func() error {
			return broker.doJSON(context.Background(), http.MethodPost, broker.endpoint("/outputs/upload/complete"), nil, nil, nil)
		},
		"commit": func() error {
			return broker.doJSON(context.Background(), http.MethodPost, broker.endpoint("/outputs/upload/commit"), nil, nil, nil)
		},
		"input-manifest": func() error {
			_, err := broker.getInputOnce(context.Background(), broker.endpoint("/inputs/manifest"))
			return err
		},
		"input-file": func() error {
			_, err := broker.getInputOnce(context.Background(), broker.endpoint("/inputs/files/0"))
			return err
		},
		"output-part": func() error {
			_, err := broker.putPart(context.Background(), uploadGrant{UploadSessionID: "upload"}, 0, 1, []byte("part"))
			return err
		},
	}
	for _, state := range []string{"missing", "empty", "invalid"} {
		if state == "empty" {
			if err := os.WriteFile(file, nil, 0o600); err != nil {
				t.Fatal(err)
			}
		}
		if state == "invalid" {
			if err := os.WriteFile(file, []byte("secret-value\ninvalid-header"), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		for name, operation := range operations {
			t.Run(state+"/"+name, func(t *testing.T) {
				err := operation()
				if err == nil {
					t.Fatal("configured Pod identity failure was ignored")
				}
				if strings.Contains(err.Error(), "secret-value") {
					t.Fatal("token leaked into error")
				}
			})
		}
	}
	if calls != 0 {
		t.Fatalf("unauthenticated requests sent: %d", calls)
	}
	// An unconfigured legacy deployment retains its existing request behavior.
	broker.podTokenFile = ""
	if err := broker.doJSON(context.Background(), http.MethodPost, broker.endpoint("/token"), nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatal("legacy mode did not send its request")
	}
}
