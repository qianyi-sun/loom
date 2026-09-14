package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

func testClient(t *testing.T) *http.Client {
	t.Helper()
	// macOS limits Unix socket path length, including temporary directory prefix.
	dir, err := os.MkdirTemp("/tmp", "loom-rpc-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	path := filepath.Join(dir, "rpc.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: (runtimeServer{1024, 3 * time.Second}).handler()}
	go server.Serve(listener)
	t.Cleanup(func() { server.Close() })
	client := &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", path)
	}}}
	t.Cleanup(client.CloseIdleConnections)
	if err := checkSocket(path); err != nil {
		t.Fatal(err)
	}
	return client
}

func runExec(t *testing.T, client *http.Client, req execRequest) (int, execResult) {
	t.Helper()
	data, _ := json.Marshal(req)
	response, err := client.Post("http://sandbox/exec", "application/json", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var result execResult
	if response.StatusCode == 200 {
		if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
			t.Fatal(err)
		}
	}
	return response.StatusCode, result
}

func TestExecRealProcessAndBounds(t *testing.T) {
	c := testClient(t)
	uid := strconv.Itoa(os.Geteuid())
	status, result := runExec(t, c, execRequest{Argv: []string{"/bin/sh", "-c", `printf "%s:%s" "$VALUE" "$PWD"; printf failure >&2; exit 7`}, Cwd: "/", Env: map[string]string{"VALUE": "hello"}, User: &uid})
	if status != 200 || result.Code != 7 || string(result.Stdout) != "hello:/" || string(result.Stderr) != "failure" {
		t.Fatalf("unexpected execution: %d %#v", status, result)
	}
	other := strconv.Itoa(os.Geteuid() + 1)
	status, _ = runExec(t, c, execRequest{Argv: []string{"true"}, User: &other})
	if status != 400 {
		t.Fatal("accepted UID switch")
	}
	status, result = runExec(t, c, execRequest{Argv: []string{"/bin/sh", "-c", "yes x | head -c 11000000"}})
	if status != 200 || !result.Truncated || len(result.Stdout) != maxOutput {
		t.Fatal("output was not bounded")
	}
	status, _ = runExec(t, c, execRequest{Argv: []string{"true"}, Timeout: 4})
	if status != 400 {
		t.Fatal("accepted excessive timeout")
	}
}

func TestDeadlineAndDisconnectKillChildren(t *testing.T) {
	c := testClient(t)
	for _, disconnect := range []bool{false, true} {
		dir := t.TempDir()
		marker := filepath.Join(dir, "child-survived")
		command := "(sleep 0.4; touch '" + marker + "') & wait"
		if disconnect {
			data, _ := json.Marshal(execRequest{Argv: []string{"/bin/sh", "-c", command}})
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			req, _ := http.NewRequestWithContext(ctx, "POST", "http://sandbox/exec", bytes.NewReader(data))
			_, err := c.Do(req)
			cancel()
			if err == nil {
				t.Fatal("disconnect should cancel HTTP request")
			}
		} else {
			status, result := runExec(t, c, execRequest{Argv: []string{"/bin/sh", "-c", command}, Timeout: .1})
			if status != 200 || result.Code != 124 {
				t.Fatalf("timeout not returned: %d %#v", status, result)
			}
		}
		time.Sleep(500 * time.Millisecond)
		if _, err := os.Stat(marker); !os.IsNotExist(err) {
			t.Fatal("child survived cancelled exec")
		}
	}
}

func TestTransferBoundaries(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("openat transfer boundary requires Linux")
	}
	c := testClient(t)
	dir := t.TempDir()
	request := func(method, path, body string) (int, string) {
		t.Helper()
		req, _ := http.NewRequest(method, "http://sandbox/file", strings.NewReader(body))
		q := req.URL.Query()
		q.Set("path", path)
		req.URL.RawQuery = q.Encode()
		resp, err := c.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		data, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(data)
	}
	path := filepath.Join(dir, "nested", "data")
	if status, _ := request("PUT", path, "payload"); status != 204 {
		t.Fatal(status)
	}
	if status, data := request("GET", path, ""); status != 200 || data != "payload" {
		t.Fatalf("round trip: %d %q", status, data)
	}
	if status, _ := request("PUT", path, strings.Repeat("a", 1025)); status != 413 {
		t.Fatal("oversized upload accepted")
	}
	if _, data := request("GET", path, ""); data != "payload" {
		t.Fatal("failed upload changed existing file")
	}
	large := filepath.Join(dir, "large")
	if err := os.WriteFile(large, bytes.Repeat([]byte("x"), 1025), 0600); err != nil {
		t.Fatal(err)
	}
	if status, _ := request("GET", large, ""); status != 413 {
		t.Fatal("oversized download accepted")
	}
	link := filepath.Join(dir, "link")
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	parentLink := filepath.Join(dir, "parent-link")
	if err := os.Symlink(filepath.Join(dir, "nested"), parentLink); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{link, filepath.Join(parentLink, "data"), dir + "/../escape", "relative"} {
		for _, method := range []string{"GET", "PUT"} {
			if status, _ := request(method, bad, "untrusted"); status != 400 {
				t.Fatalf("accepted %s %s: %d", method, bad, status)
			}
		}
	}
}

func TestStopProcessesPreservesRPCAndRejectsHostProcess(t *testing.T) {
	c := testClient(t)
	if runtime.GOOS != "linux" || os.Getpid() != 1 {
		response, err := c.Post("http://sandbox/stop-processes", "application/json", nil)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != 409 {
			t.Fatal("cleanup allowed outside Linux PID 1")
		}
		return
	}
	marker := filepath.Join(t.TempDir(), "detached-child-survived")
	status, result := runExec(t, c, execRequest{Argv: []string{"/bin/sh", "-c", "(sleep 0.4; touch '" + marker + "') </dev/null >/dev/null 2>&1 &"}})
	if status != 200 || result.Code != 0 {
		t.Fatal("failed to start detached child")
	}
	response, err := c.Post("http://sandbox/stop-processes", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != 204 {
		t.Fatalf("cleanup failed: %d", response.StatusCode)
	}
	time.Sleep(500 * time.Millisecond)
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatal("detached child survived cleanup")
	}
	status, result = runExec(t, c, execRequest{Argv: []string{"/bin/sh", "-c", "printf snapshot-ready"}})
	if status != 200 || string(result.Stdout) != "snapshot-ready" {
		t.Fatal("cleanup stopped RPC server")
	}
}
