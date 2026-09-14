package main

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"testing"
)

func TestCleanupFailurePublishesOnlyFixedReason(t *testing.T) {
	cases := []struct {
		err  error
		code string
	}{
		{errCleanupPIDNamespace, "pid_namespace_invalid"},
		{errCleanupProcessOwner, "process_owner_mismatch"},
		{errCleanupProcRead, "process_inspection_failed"},
		{fmt.Errorf("private detail: %w", context.DeadlineExceeded), "cleanup_timeout"},
		{context.Canceled, "cleanup_cancelled"},
		{errors.New("private command/env/URL fixture"), "cleanup_failed"},
	}
	for _, tc := range cases {
		response := httptest.NewRecorder()
		writeCleanupFailure(response, tc.err)
		if response.Code != 409 || response.Header().Get("X-Loom-Sandbox-Error") != tc.code || response.Body.String() != "sandbox process cleanup failed\n" {
			t.Fatalf("unexpected cleanup response for %s: %v", tc.code, response)
		}
	}
}
