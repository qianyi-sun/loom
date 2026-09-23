package main

import (
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestPauseResumeKeepsServiceAliveAndSnapshotStable(t *testing.T) {
	c := testClient(t)
	post := func(endpoint string) int {
		response, err := c.Post("http://sandbox/"+endpoint, "application/json", nil)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, response.Body)
		response.Body.Close()
		return response.StatusCode
	}
	if runtime.GOOS != "linux" || os.Getpid() != 1 {
		if status := post("pause-processes"); status != http.StatusConflict {
			t.Fatalf("pause allowed outside isolated PID 1: %d", status)
		}
		return
	}
	marker := filepath.Join(t.TempDir(), "heartbeat")
	status, result := runExec(t, c, execRequest{Argv: []string{"/bin/sh", "-c",
		"(while true; do echo tick >> '" + marker + "'; sleep .02; done) </dev/null >/dev/null 2>&1 &"}})
	if status != 200 || result.Code != 0 {
		t.Fatal("service did not start")
	}
	defer post("stop-processes")
	deadline := time.Now().Add(2 * time.Second)
	for {
		if info, err := os.Stat(marker); err == nil && info.Size() > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("service not ready")
		}
		time.Sleep(time.Millisecond)
	}
	if status := post("pause-processes"); status != http.StatusNoContent {
		t.Fatalf("pause failed: %d", status)
	}
	before, _ := os.ReadFile(marker)
	time.Sleep(100 * time.Millisecond)
	after, _ := os.ReadFile(marker)
	if string(before) != string(after) {
		t.Fatal("service modified state during snapshot pause")
	}
	status, result = runExec(t, c, execRequest{Argv: []string{"/bin/sh", "-c", "printf archive-ready"}})
	if status != 200 || string(result.Stdout) != "archive-ready" {
		t.Fatal("snapshot RPC unavailable while paused")
	}
	if status := post("resume-processes"); status != http.StatusNoContent {
		t.Fatalf("resume failed: %d", status)
	}
	deadline = time.Now().Add(2 * time.Second)
	for {
		current, _ := os.ReadFile(marker)
		if len(current) > len(after) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("service did not resume")
		}
		time.Sleep(time.Millisecond)
	}
}
