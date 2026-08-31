package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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

func TestWorkloadBrokerRefreshProxyAndDurableOutputCommit(t *testing.T) {
	leaseID := "0194d739-8bec-7b7b-88f5-62f7cbd42cb3"
	sessionID := "0194d739-8bec-7b7b-88f5-62f7cbd42cb4"
	var mu sync.Mutex
	tokenCalls := 0
	parts := map[string][]byte{}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
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
		root:     root,
		identity: workloadIdentity{LeaseID: leaseID, Generation: 7, ExecutionRole: "attempt"},
		client:   server.Client(),
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
	evidence, err := broker.commitOutputs(ctx, output, broker.outputRequestID())
	if err != nil {
		t.Fatal(err)
	}
	if evidence.UploadSessionID != sessionID || len(parts) != 2 {
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
