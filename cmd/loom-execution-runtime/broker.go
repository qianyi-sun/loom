package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const uploadPartBytes int64 = 64 * 1024 * 1024

type workloadIdentity struct {
	LeaseID       string `json:"lease_id"`
	Generation    int64  `json:"generation"`
	ExecutionRole string `json:"execution_role"`
}

type workloadBroker struct {
	root     *url.URL
	identity workloadIdentity
	client   *http.Client
	mu       sync.Mutex
	token    string
	expires  time.Time
}

type tokenRequest struct {
	workloadIdentity
	TTLSeconds int `json:"ttl_seconds"`
}

type tokenResponse struct {
	SchemaVersion string    `json:"schema_version"`
	Token         string    `json:"token"`
	ExpiresAt     time.Time `json:"expires_at"`
}

type outputFile struct {
	RelativePath string `json:"relative_path"`
	MediaType    string `json:"media_type"`
	SizeBytes    int64  `json:"size_bytes"`
	SHA256       string `json:"sha256"`
}

type outputPrepare struct {
	workloadIdentity
	SchemaVersion string       `json:"schema_version"`
	RequestID     string       `json:"request_id"`
	Files         []outputFile `json:"files"`
}

type uploadPlanFile struct {
	FileIndex    int    `json:"file_index"`
	RelativePath string `json:"relative_path"`
}

type uploadGrant struct {
	UploadSessionID string           `json:"upload_session_id"`
	UploadToken     string           `json:"upload_token"`
	TokenExpiresAt  time.Time        `json:"token_expires_at"`
	Files           []uploadPlanFile `json:"files"`
}

type partReceipt struct {
	FileIndex  int    `json:"file_index"`
	PartNumber int    `json:"part_number"`
	SizeBytes  int64  `json:"size_bytes"`
	SHA256     string `json:"sha256"`
}

type outputCommitEvidence struct {
	UploadSessionID       string `json:"upload_session_id"`
	ManifestSHA256        string `json:"manifest_sha256"`
	CommittedMarkerSHA256 string `json:"committed_marker_sha256"`
}

func workloadBrokerFromEnvironment() (*workloadBroker, error) {
	rawRoot := strings.TrimRight(os.Getenv("LOOM_EXECUTION_BROKER_URL"), "/")
	root, err := url.Parse(rawRoot)
	if err != nil || root.Scheme == "" || root.Host == "" || (root.Scheme != "http" && root.Scheme != "https") {
		return nil, fmt.Errorf("invalid execution broker URL")
	}
	generation, err := strconv.ParseInt(os.Getenv("LOOM_EXECUTION_GENERATION"), 10, 64)
	role := os.Getenv("LOOM_EXECUTION_ROLE")
	leaseID := os.Getenv("LOOM_EXECUTION_LEASE_ID")
	if err != nil || generation <= 0 || (role != "attempt" && role != "verifier") || len(leaseID) != 36 {
		return nil, fmt.Errorf("invalid execution broker identity")
	}
	for _, name := range []string{
		"LOOM_EXECUTION_BROKER_URL",
		"LOOM_EXECUTION_GENERATION",
		"LOOM_EXECUTION_LEASE_ID",
		"LOOM_EXECUTION_ROLE",
	} {
		if err := os.Unsetenv(name); err != nil {
			return nil, fmt.Errorf("clear execution broker identity: %w", err)
		}
	}
	return &workloadBroker{
		root:     root,
		identity: workloadIdentity{LeaseID: leaseID, Generation: generation, ExecutionRole: role},
		client:   &http.Client{Timeout: 120 * time.Second},
	}, nil
}

func (b *workloadBroker) endpoint(path string) string {
	copyURL := *b.root
	copyURL.Path = strings.TrimRight(b.root.Path, "/") + path
	copyURL.RawQuery = ""
	return copyURL.String()
}

func (b *workloadBroker) gatewayEndpoint(path string, query string) string {
	copyURL := *b.root
	copyURL.Path = strings.TrimSuffix(b.root.Path, "/internal/service-execution") + path
	copyURL.RawQuery = query
	return copyURL.String()
}

func (b *workloadBroker) doJSON(ctx context.Context, method, endpoint string, request any, headers map[string]string, response any) error {
	payload, err := json.Marshal(request)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	for name, value := range headers {
		req.Header.Set(name, value)
	}
	result, err := b.client.Do(req)
	if err != nil {
		return err
	}
	defer result.Body.Close()
	body, err := io.ReadAll(io.LimitReader(result.Body, 1024*1024))
	if err != nil {
		return err
	}
	if result.StatusCode < 200 || result.StatusCode >= 300 {
		return fmt.Errorf("broker HTTP %d: %s", result.StatusCode, strings.TrimSpace(string(body)))
	}
	if response == nil {
		return nil
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	if err := decoder.Decode(response); err != nil {
		return err
	}
	return nil
}

func (b *workloadBroker) currentToken(ctx context.Context) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.token != "" && time.Until(b.expires) > 60*time.Second {
		return b.token, nil
	}
	var response tokenResponse
	if err := retryOperation(ctx, func() error {
		return b.doJSON(ctx, http.MethodPost, b.endpoint("/token"), tokenRequest{
			workloadIdentity: b.identity,
			TTLSeconds:       480,
		}, nil, &response)
	}); err != nil {
		return "", err
	}
	if response.SchemaVersion != "loom.service-execution-token.v1" || response.Token == "" || !response.ExpiresAt.After(time.Now()) {
		return "", fmt.Errorf("broker returned invalid step token")
	}
	b.token, b.expires = response.Token, response.ExpiresAt
	return b.token, nil
}

func (b *workloadBroker) startProxy(ctx context.Context) (string, func() error, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", nil, err
	}
	server := &http.Server{
		ReadHeaderTimeout: 10 * time.Second,
		Handler: http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			if !allowedGatewayRequest(request.Method, request.URL.Path) {
				http.Error(writer, "gateway route unavailable", http.StatusForbidden)
				return
			}
			token, tokenErr := b.currentToken(request.Context())
			if tokenErr != nil {
				http.Error(writer, "workload token unavailable", http.StatusServiceUnavailable)
				return
			}
			upstream, requestErr := http.NewRequestWithContext(
				request.Context(), request.Method,
				b.gatewayEndpoint(request.URL.Path, request.URL.RawQuery), request.Body,
			)
			if requestErr != nil {
				http.Error(writer, "invalid gateway request", http.StatusBadRequest)
				return
			}
			upstream.Header = request.Header.Clone()
			upstream.Header.Set("Authorization", "Bearer "+token)
			upstream.Header.Del("Connection")
			response, requestErr := b.client.Do(upstream)
			if requestErr != nil {
				http.Error(writer, "gateway unavailable", http.StatusBadGateway)
				return
			}
			defer response.Body.Close()
			for name, values := range response.Header {
				if strings.EqualFold(name, "Connection") {
					continue
				}
				for _, value := range values {
					writer.Header().Add(name, value)
				}
			}
			writer.WriteHeader(response.StatusCode)
			_, _ = io.Copy(writer, response.Body)
		}),
	}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
	}()
	go func() { _ = server.Serve(listener) }()
	return "http://" + listener.Addr().String(), func() error {
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return server.Shutdown(shutdown)
	}, nil
}

func allowedGatewayRequest(method, path string) bool {
	if method != http.MethodPost {
		return false
	}
	switch path {
	case "/v1/chat/completions", "/v1/messages", "/v1/responses",
		"/openai/v1/chat/completions", "/openai/v1/responses",
		"/anthropic/v1/messages":
		return true
	default:
		return strings.HasPrefix(path, "/v1beta/models/") ||
			strings.HasPrefix(path, "/google/v1beta/models/")
	}
}

func trustedGatewayEnvironment(proxyURL string) map[string]string {
	return map[string]string{
		"LOOM_GATEWAY_URL":       proxyURL,
		"OPENAI_BASE_URL":        proxyURL + "/v1",
		"ANTHROPIC_BASE_URL":     proxyURL + "/anthropic/v1",
		"GOOGLE_GEMINI_BASE_URL": proxyURL + "/google/v1beta",
		"OPENAI_API_KEY":         "loom_workload_proxy",
		"ANTHROPIC_API_KEY":      "loom_workload_proxy",
		"GOOGLE_API_KEY":         "loom_workload_proxy",
	}
}

func (b *workloadBroker) outputRequestID() string {
	digest := sha256.Sum256([]byte(fmt.Sprintf(
		"loom.service-execution-output.v1\x00%s\x00%d\x00%s",
		b.identity.LeaseID,
		b.identity.Generation,
		b.identity.ExecutionRole,
	)))
	// The idempotency key is deterministic for one immutable execution
	// generation, while still satisfying the API's UUID contract.
	digest[6] = (digest[6] & 0x0f) | 0x50
	digest[8] = (digest[8] & 0x3f) | 0x80
	value := hex.EncodeToString(digest[:16])
	return fmt.Sprintf("%s-%s-%s-%s-%s", value[:8], value[8:12], value[12:16], value[16:20], value[20:])
}

func inventoryOutputs(outputRoot string) ([]outputFile, error) {
	entries, err := os.ReadDir(outputRoot)
	if err != nil {
		return nil, err
	}
	files := make([]outputFile, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || entry.Name() == "termination-message" || strings.HasSuffix(entry.Name(), ".tmp") {
			continue
		}
		path := filepath.Join(outputRoot, entry.Name())
		payload, err := readRegularOutputFile(path)
		if err != nil {
			return nil, err
		}
		digest := sha256.Sum256(payload)
		media := "application/octet-stream"
		if strings.HasSuffix(entry.Name(), ".json") {
			media = "application/json"
		} else if strings.HasSuffix(entry.Name(), ".stdout") || strings.HasSuffix(entry.Name(), ".stderr") {
			media = "text/plain; charset=utf-8"
		}
		files = append(files, outputFile{
			RelativePath: entry.Name(), MediaType: media, SizeBytes: int64(len(payload)),
			SHA256: "sha256:" + hex.EncodeToString(digest[:]),
		})
	}
	sort.Slice(files, func(i, j int) bool {
		if files[i].RelativePath == "result.json" {
			return true
		}
		if files[j].RelativePath == "result.json" {
			return false
		}
		return files[i].RelativePath < files[j].RelativePath
	})
	if len(files) == 0 || files[0].RelativePath != "result.json" {
		return nil, fmt.Errorf("runtime result is missing from output inventory")
	}
	return files, nil
}

func (b *workloadBroker) putPart(ctx context.Context, grant uploadGrant, fileIndex, partNumber int, payload []byte) (partReceipt, error) {
	digest := sha256.Sum256(payload)
	sha := "sha256:" + hex.EncodeToString(digest[:])
	endpoint := b.endpoint(fmt.Sprintf("/outputs/%s/files/%d/parts/%d", grant.UploadSessionID, fileIndex, partNumber))
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, endpoint, bytes.NewReader(payload))
	if err != nil {
		return partReceipt{}, err
	}
	req.ContentLength = int64(len(payload))
	req.Header.Set("X-Loom-Execution-Lease-Id", b.identity.LeaseID)
	req.Header.Set("X-Loom-Execution-Generation", strconv.FormatInt(b.identity.Generation, 10))
	req.Header.Set("X-Loom-Execution-Role", b.identity.ExecutionRole)
	req.Header.Set("X-Loom-Upload-Token", grant.UploadToken)
	req.Header.Set("X-Loom-Content-SHA256", sha)
	response, err := b.client.Do(req)
	if err != nil {
		return partReceipt{}, err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 1024*1024))
	if err != nil {
		return partReceipt{}, err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return partReceipt{}, fmt.Errorf("broker HTTP %d: %s", response.StatusCode, strings.TrimSpace(string(body)))
	}
	var receipt partReceipt
	if err := json.Unmarshal(body, &receipt); err != nil {
		return partReceipt{}, err
	}
	if receipt.FileIndex != fileIndex || receipt.PartNumber != partNumber || receipt.SizeBytes != int64(len(payload)) || receipt.SHA256 != sha {
		return partReceipt{}, fmt.Errorf("upload receipt identity drift")
	}
	return receipt, nil
}

func retryOperation(ctx context.Context, operation func() error) error {
	return retryOperationIf(ctx, operation, func(error) bool { return true })
}

func retryOperationIf(ctx context.Context, operation func() error, retryable func(error) bool) error {
	delay := 100 * time.Millisecond
	var last error
	for attempt := 0; attempt < 12; attempt++ {
		if err := operation(); err == nil {
			return nil
		} else {
			last = err
			if !retryable(err) {
				return err
			}
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		if delay < 5*time.Second {
			delay *= 2
		}
	}
	return last
}

func (b *workloadBroker) commitOutputs(ctx context.Context, outputRoot, requestID string) (outputCommitEvidence, error) {
	files, err := inventoryOutputs(outputRoot)
	if err != nil {
		return outputCommitEvidence{}, err
	}
	request := outputPrepare{
		workloadIdentity: b.identity,
		SchemaVersion:    "loom.service-execution-output-prepare.v1",
		RequestID:        requestID,
		Files:            files,
	}
	var grant uploadGrant
	if err := retryOperation(ctx, func() error {
		return b.doJSON(ctx, http.MethodPost, b.endpoint("/outputs/prepare"), request, nil, &grant)
	}); err != nil {
		return outputCommitEvidence{}, fmt.Errorf("prepare output upload: %w", err)
	}
	if len(grant.Files) != len(files) || grant.UploadSessionID == "" || grant.UploadToken == "" {
		return outputCommitEvidence{}, fmt.Errorf("invalid output upload grant")
	}
	for index, file := range files {
		if grant.Files[index].FileIndex != index || grant.Files[index].RelativePath != file.RelativePath {
			return outputCommitEvidence{}, fmt.Errorf("output upload plan identity drift")
		}
		payload, err := readRegularOutputFile(filepath.Join(outputRoot, file.RelativePath))
		if err != nil {
			return outputCommitEvidence{}, err
		}
		receipts := []partReceipt{}
		for offset, partNumber := int64(0), 1; offset < int64(len(payload)) || (len(payload) == 0 && partNumber == 1); partNumber++ {
			end := offset + uploadPartBytes
			if end > int64(len(payload)) {
				end = int64(len(payload))
			}
			chunk := payload[offset:end]
			var receipt partReceipt
			if err := retryOperation(ctx, func() error {
				var uploadErr error
				receipt, uploadErr = b.putPart(ctx, grant, index, partNumber, chunk)
				return uploadErr
			}); err != nil {
				return outputCommitEvidence{}, fmt.Errorf("upload output part: %w", err)
			}
			receipts = append(receipts, receipt)
			if len(payload) == 0 {
				break
			}
			offset = end
		}
		complete := struct {
			workloadIdentity
			SchemaVersion string        `json:"schema_version"`
			OrderedParts  []partReceipt `json:"ordered_parts"`
		}{b.identity, "loom.service-execution-file-complete.v1", receipts}
		if err := retryOperation(ctx, func() error {
			return b.doJSON(ctx, http.MethodPost,
				b.endpoint(fmt.Sprintf("/outputs/%s/files/%d/complete", grant.UploadSessionID, index)),
				complete, map[string]string{"X-Loom-Upload-Token": grant.UploadToken}, nil)
		}); err != nil {
			return outputCommitEvidence{}, fmt.Errorf("complete output file: %w", err)
		}
	}
	commit := struct {
		workloadIdentity
		SchemaVersion string `json:"schema_version"`
	}{b.identity, "loom.service-execution-output-commit.v1"}
	var evidence outputCommitEvidence
	if err := retryOperation(ctx, func() error {
		return b.doJSON(ctx, http.MethodPost,
			b.endpoint(fmt.Sprintf("/outputs/%s/commit", grant.UploadSessionID)),
			commit, map[string]string{"X-Loom-Upload-Token": grant.UploadToken}, &evidence)
	}); err != nil {
		return outputCommitEvidence{}, fmt.Errorf("commit output: %w", err)
	}
	if evidence.UploadSessionID != grant.UploadSessionID || evidence.ManifestSHA256 == "" || evidence.CommittedMarkerSHA256 == "" {
		return outputCommitEvidence{}, fmt.Errorf("output commit evidence is incomplete")
	}
	return evidence, nil
}
