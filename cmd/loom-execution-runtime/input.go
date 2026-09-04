package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const maxInputManifestBytes = 16 * 1024 * 1024

type brokerHTTPError struct {
	statusCode int
	body       string
}

func (e *brokerHTTPError) Error() string {
	return fmt.Sprintf("broker HTTP %d: %s", e.statusCode, e.body)
}

type taskInputFile struct {
	RelativePath string `json:"relative_path"`
	SizeBytes    int64  `json:"size_bytes"`
	SHA256       string `json:"sha256"`
	Mode         string `json:"mode"`
}

type taskInputManifest struct {
	SchemaVersion      string          `json:"schema_version"`
	TaskRevisionSHA256 string          `json:"task_revision_sha256"`
	Files              []taskInputFile `json:"files"`
}

func (b *workloadBroker) identityHeaders(request *http.Request) {
	request.Header.Set("X-Loom-Execution-Lease-Id", b.identity.LeaseID)
	request.Header.Set("X-Loom-Execution-Generation", strconv.FormatInt(b.identity.Generation, 10))
	request.Header.Set("X-Loom-Execution-Role", b.identity.ExecutionRole)
}

func (b *workloadBroker) getInputOnce(ctx context.Context, endpoint string) (*http.Response, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	b.identityHeaders(request)
	response, err := b.client.Do(request)
	if err != nil {
		return nil, err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		defer response.Body.Close()
		body, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return nil, &brokerHTTPError{
			statusCode: response.StatusCode,
			body:       strings.TrimSpace(string(body)),
		}
	}
	return response, nil
}

func workloadIdentityNotObserved(err error) bool {
	var brokerErr *brokerHTTPError
	if !errors.As(err, &brokerErr) || brokerErr.statusCode != http.StatusServiceUnavailable {
		return false
	}
	var payload struct {
		Detail string `json:"detail"`
	}
	return json.Unmarshal([]byte(brokerErr.body), &payload) == nil &&
		payload.Detail == "workload_identity_not_observed"
}

func (b *workloadBroker) getInput(ctx context.Context, endpoint string) (*http.Response, error) {
	var response *http.Response
	err := retryOperationIf(ctx, func() error {
		var err error
		response, err = b.getInputOnce(ctx, endpoint)
		return err
	}, workloadIdentityNotObserved)
	return response, err
}

func secureInputDirectory(root, directory string) error {
	if directory != root && !isWithin(root, directory) {
		return fmt.Errorf("task input directory escapes workspace")
	}
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return err
	}
	relative, err := filepath.Rel(root, directory)
	if err != nil {
		return err
	}
	current := root
	if err := secureDirectory(current); err != nil {
		return err
	}
	if relative == "." {
		return nil
	}
	for _, component := range strings.Split(relative, string(filepath.Separator)) {
		current = filepath.Join(current, component)
		if err := secureDirectory(current); err != nil {
			return err
		}
	}
	return nil
}

func decodeTaskInputManifest(payload []byte, p plan) (taskInputManifest, error) {
	if p.TaskInput == nil {
		return taskInputManifest{}, fmt.Errorf("task input binding is absent")
	}
	digest := sha256.Sum256(payload)
	if "sha256:"+hex.EncodeToString(digest[:]) != p.TaskInput.ManifestSHA256 {
		return taskInputManifest{}, fmt.Errorf("task input manifest digest mismatch")
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var manifest taskInputManifest
	if err := decoder.Decode(&manifest); err != nil {
		return taskInputManifest{}, err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return taskInputManifest{}, fmt.Errorf("task input manifest contains trailing JSON")
	}
	if manifest.SchemaVersion != "loom.service-execution-input-manifest.v1" ||
		manifest.TaskRevisionSHA256 != p.TaskRevisionSHA256 || len(manifest.Files) != p.TaskInput.FileCount {
		return taskInputManifest{}, fmt.Errorf("task input manifest identity mismatch")
	}
	total := int64(0)
	previous := ""
	for _, file := range manifest.Files {
		clean := filepath.ToSlash(filepath.Clean(file.RelativePath))
		if clean != file.RelativePath || clean == "." || strings.HasPrefix(clean, "/") ||
			strings.HasPrefix(clean, "../") || strings.Contains(clean, "/../") ||
			(file.Mode != "0644" && file.Mode != "0755") || file.SizeBytes < 0 ||
			!sha256Value.MatchString(file.SHA256) || (previous != "" && previous >= file.RelativePath) {
			return taskInputManifest{}, fmt.Errorf("task input file inventory is invalid")
		}
		previous = file.RelativePath
		total += file.SizeBytes
		if total < 0 || total > p.TaskInput.TotalBytes {
			return taskInputManifest{}, fmt.Errorf("task input size exceeds its binding")
		}
	}
	if total != p.TaskInput.TotalBytes {
		return taskInputManifest{}, fmt.Errorf("task input total size mismatch")
	}
	return manifest, nil
}

func (b *workloadBroker) materializeInputs(ctx context.Context, p plan, workspace string) error {
	if p.TaskInput == nil {
		return nil
	}
	if err := secureDirectory(workspace); err != nil {
		return err
	}
	manifestResponse, err := b.getInput(ctx, b.endpoint("/inputs/manifest"))
	if err != nil {
		return err
	}
	payload, readErr := io.ReadAll(io.LimitReader(manifestResponse.Body, maxInputManifestBytes+1))
	closeErr := manifestResponse.Body.Close()
	if readErr != nil {
		return readErr
	}
	if closeErr != nil {
		return closeErr
	}
	if len(payload) > maxInputManifestBytes {
		return fmt.Errorf("task input manifest exceeds 16 MiB")
	}
	manifest, err := decodeTaskInputManifest(payload, p)
	if err != nil {
		return err
	}
	bundleDigest := sha256.New()
	for index, file := range manifest.Files {
		destination := filepath.Join(workspace, filepath.FromSlash(file.RelativePath))
		if !isWithin(workspace, destination) {
			return fmt.Errorf("task input path escapes workspace")
		}
		if err := secureInputDirectory(workspace, filepath.Dir(destination)); err != nil {
			return err
		}
		response, err := b.getInput(ctx, b.endpoint(fmt.Sprintf("/inputs/files/%d", index)))
		if err != nil {
			return err
		}
		mode := os.FileMode(0o644)
		if file.Mode == "0755" {
			mode = 0o755
		}
		output, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, mode)
		if err != nil {
			response.Body.Close()
			return err
		}
		fileDigest := sha256.New()
		bundleDigest.Write([]byte{0})
		bundleDigest.Write([]byte(file.RelativePath))
		bundleDigest.Write([]byte{0})
		written, copyErr := io.Copy(
			io.MultiWriter(output, fileDigest, bundleDigest),
			io.LimitReader(response.Body, file.SizeBytes+1),
		)
		closeBodyErr := response.Body.Close()
		closeFileErr := output.Close()
		if copyErr != nil || closeBodyErr != nil || closeFileErr != nil {
			_ = os.Remove(destination)
			if copyErr != nil {
				return copyErr
			}
			if closeBodyErr != nil {
				return closeBodyErr
			}
			return closeFileErr
		}
		actualSHA := "sha256:" + hex.EncodeToString(fileDigest.Sum(nil))
		if written != file.SizeBytes || actualSHA != file.SHA256 {
			_ = os.Remove(destination)
			return fmt.Errorf("task input file identity mismatch")
		}
	}
	if "sha256:"+hex.EncodeToString(bundleDigest.Sum(nil)) != p.TaskRevisionSHA256 {
		return fmt.Errorf("materialized task bundle checksum mismatch")
	}
	return nil
}
