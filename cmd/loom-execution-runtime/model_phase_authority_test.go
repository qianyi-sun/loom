package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestSetupAndVerifierCannotReopenModelAuthority(t *testing.T) {
	var modelCalls, ledgerCalls atomic.Int32
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/internal/service-execution/token":
			_ = json.NewEncoder(w).Encode(tokenResponse{SchemaVersion: "loom.service-execution-token.v1", Token: "fixture", ExpiresAt: time.Now().Add(time.Hour)})
		case "/internal/service-execution/llm-calls":
			ledgerCalls.Add(1)
			_, _ = io.WriteString(w, `{"items":[]}`)
		default:
			modelCalls.Add(1)
			_, _ = io.WriteString(w, `{}`)
		}
	}))
	defer gateway.Close()
	root, _ := url.Parse(gateway.URL + "/internal/service-execution")
	broker := &workloadBroker{root: root, client: gateway.Client(), identity: workloadIdentity{ExecutionRole: "attempt"}}
	proxy, stop, err := broker.startProxy(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	for _, role := range []string{"setup", "agent", "verifier"} {
		t.Run(role, func(t *testing.T) {
			workspace := t.TempDir()
			expected := "403"
			if role == "agent" {
				expected = "200"
			}
			evidence, err := runPhase(context.Background(), phase{
				Role: role, Argv: []string{os.Args[0], "-test.run=^TestModelAuthorityPhaseHelper$"}, WorkingDirectory: workspace, TimeoutSeconds: 3,
				Environment: map[string]string{"LOOM_MODEL_AUTHORITY_TEST_PROXY": proxy, "LOOM_MODEL_AUTHORITY_EXPECT_STATUS": expected},
			}, 1, workspace, t.TempDir(), 4096, 50*time.Millisecond, nil, broker.setPhaseDeadline)
			if err != nil || evidence.ExitCode != 0 {
				t.Fatalf("phase %s exposed model authority or lost ledger: %v %+v", role, err, evidence)
			}
		})
	}
	response, err := http.Get(proxy + "/internal/loom/llm-calls")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatal("phase completion blocked final accounting")
	}
	if modelCalls.Load() != 1 || ledgerCalls.Load() != 4 {
		t.Fatalf("unexpected authority: model=%d ledger=%d", modelCalls.Load(), ledgerCalls.Load())
	}
}

func TestModelAuthorityPhaseHelper(t *testing.T) {
	endpoint := os.Getenv("LOOM_MODEL_AUTHORITY_TEST_PROXY")
	if endpoint == "" {
		return
	}
	response, err := http.Post(endpoint+"/v1/chat/completions", "application/json", strings.NewReader("{}"))
	if err != nil {
		os.Exit(2)
	}
	response.Body.Close()
	expected := http.StatusForbidden
	if os.Getenv("LOOM_MODEL_AUTHORITY_EXPECT_STATUS") == "200" {
		expected = http.StatusOK
	}
	if response.StatusCode != expected {
		os.Exit(3)
	}
	response, err = http.Get(endpoint + "/internal/loom/llm-calls")
	if err != nil {
		os.Exit(4)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		os.Exit(5)
	}
	os.Exit(0)
}

func TestAgentPhaseCompletionCancelsRetainedModelRequest(t *testing.T) {
	for _, stage := range []string{"token", "model"} {
		t.Run(stage, func(t *testing.T) {
			started, aborted, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
			gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				isToken := strings.HasSuffix(r.URL.Path, "/token")
				if isToken && stage == "model" {
					_ = json.NewEncoder(w).Encode(tokenResponse{SchemaVersion: "loom.service-execution-token.v1", Token: "fixture", ExpiresAt: time.Now().Add(time.Hour)})
					return
				}
				if !isToken && stage == "token" {
					return
				}
				_, _ = io.Copy(io.Discard, r.Body)
				close(started)
				select {
				case <-r.Context().Done():
					close(aborted)
				case <-time.After(2 * time.Second):
				}
				if isToken {
					_ = json.NewEncoder(w).Encode(tokenResponse{SchemaVersion: "loom.service-execution-token.v1", Token: "fixture", ExpiresAt: time.Now().Add(time.Hour)})
				}
			}))
			defer gateway.Close()
			root, _ := url.Parse(gateway.URL + "/internal/service-execution")
			broker := &workloadBroker{root: root, client: gateway.Client()}
			proxy, stop, err := broker.startProxy(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer stop()
			boundary := func(deadline time.Time) {
				broker.setPhaseDeadline(deadline)
				if deadline.IsZero() {
					return
				}
				go func() {
					defer close(finished)
					response, err := http.Post(proxy+"/v1/chat/completions", "application/json", strings.NewReader("{}"))
					if err == nil {
						response.Body.Close()
					}
				}()
				<-started
			}
			workspace := t.TempDir()
			start := time.Now()
			_, err = runPhase(context.Background(), phase{Role: "agent", Argv: []string{"/bin/true"}, WorkingDirectory: workspace, TimeoutSeconds: 10}, 1, workspace, t.TempDir(), 4096, 50*time.Millisecond, nil, boundary)
			if err != nil {
				t.Fatal(err)
			}
			if time.Since(start) > 300*time.Millisecond {
				t.Error("phase completion waited for a retained model or token request")
			}
			select {
			case <-aborted:
			case <-time.After(300 * time.Millisecond):
				t.Error("finished agent left retained model request active until original deadline")
			}
			<-finished
		})
	}
}
