package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "fixture mode is required")
		os.Exit(2)
	}
	switch os.Args[1] {
	case "idle":
		for {
			time.Sleep(time.Hour)
		}
	case "server":
		if len(os.Args) != 3 {
			fmt.Fprintln(os.Stderr, "server port is required")
			os.Exit(2)
		}
		http.HandleFunc("/", func(response http.ResponseWriter, _ *http.Request) {
			response.WriteHeader(http.StatusNoContent)
		})
		if err := http.ListenAndServe(":"+os.Args[2], nil); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	case "broker":
		http.HandleFunc("/internal/service-execution/outputs/prepare", func(response http.ResponseWriter, request *http.Request) {
			var prepared struct {
				Files []struct {
					RelativePath string `json:"relative_path"`
				} `json:"files"`
			}
			if err := json.NewDecoder(request.Body).Decode(&prepared); err != nil {
				http.Error(response, "invalid prepare", http.StatusBadRequest)
				return
			}
			files := make([]map[string]any, len(prepared.Files))
			for index, file := range prepared.Files {
				files[index] = map[string]any{
					"file_index":    index,
					"relative_path": file.RelativePath,
				}
			}
			response.Header().Set("Content-Type", "application/json")
			response.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(response).Encode(map[string]any{
				"upload_session_id": "0194d739-8bec-7b7b-88f5-62f7cbd42cb3",
				"upload_token":      strings.Repeat("u", 48),
				"token_expires_at":  time.Now().Add(10 * time.Minute).UTC(),
				"files":             files,
			})
		})
		http.HandleFunc("/internal/service-execution/outputs/", func(response http.ResponseWriter, request *http.Request) {
			segments := strings.Split(strings.Trim(request.URL.Path, "/"), "/")
			response.Header().Set("Content-Type", "application/json")
			if request.Method == http.MethodPut && len(segments) >= 8 && segments[len(segments)-2] == "parts" {
				payload, err := io.ReadAll(request.Body)
				if err != nil {
					http.Error(response, "read part", http.StatusBadRequest)
					return
				}
				fileIndex, _ := strconv.Atoi(segments[len(segments)-3])
				partNumber, _ := strconv.Atoi(segments[len(segments)-1])
				digest := sha256.Sum256(payload)
				_ = json.NewEncoder(response).Encode(map[string]any{
					"file_index":  fileIndex,
					"part_number": partNumber,
					"size_bytes":  len(payload),
					"sha256":      "sha256:" + hex.EncodeToString(digest[:]),
				})
				return
			}
			if strings.HasSuffix(request.URL.Path, "/complete") {
				_ = json.NewEncoder(response).Encode(map[string]any{"state": "uploaded"})
				return
			}
			if strings.HasSuffix(request.URL.Path, "/commit") {
				_ = json.NewEncoder(response).Encode(map[string]any{
					"upload_session_id":       "0194d739-8bec-7b7b-88f5-62f7cbd42cb3",
					"manifest_sha256":         "sha256:" + strings.Repeat("1", 64),
					"committed_marker_sha256": "sha256:" + strings.Repeat("2", 64),
				})
				return
			}
			http.NotFound(response, request)
		})
		if err := http.ListenAndServe(":9100", nil); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	case "probe-report":
		if len(os.Args) != 3 {
			fmt.Println("exit:1")
			return
		}
		client := &http.Client{Timeout: 3 * time.Second}
		response, err := client.Get(os.Args[2])
		if err != nil {
			fmt.Println("exit:1")
			return
		}
		_ = response.Body.Close()
		if response.StatusCode < 200 || response.StatusCode >= 400 {
			fmt.Println("exit:1")
			return
		}
		fmt.Println("exit:0")
	case "sidecar":
		http.HandleFunc("/healthz", func(response http.ResponseWriter, _ *http.Request) {
			response.WriteHeader(http.StatusNoContent)
		})
		http.HandleFunc("/readyz", func(response http.ResponseWriter, _ *http.Request) {
			response.WriteHeader(http.StatusNoContent)
		})
		if err := http.ListenAndServe(":8080", nil); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	case "phase":
		if len(os.Args) != 3 {
			fmt.Fprintln(os.Stderr, "fixture phase name is required")
			os.Exit(2)
		}
		if os.Args[2] == "agent" {
			if err := os.WriteFile("/workspace/agent-output.txt", []byte("agent-output\n"), 0o600); err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
		}
		if os.Args[2] == "verifier" {
			payload, err := os.ReadFile("/workspace/agent-output.txt")
			if err != nil || string(payload) != "agent-output\n" {
				fmt.Fprintln(os.Stderr, "verifier did not receive exact agent output")
				os.Exit(1)
			}
		}
		fmt.Printf("fixture-phase=%s\n", os.Args[2])
	default:
		fmt.Fprintln(os.Stderr, "unsupported fixture mode")
		os.Exit(2)
	}
}
