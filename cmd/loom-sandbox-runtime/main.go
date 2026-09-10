// loom-sandbox-runtime exposes only the container's execution and file surface.
// Its Unix socket is mounted into the trusted agent, never another sandbox.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const maxOutput = 10 * 1024 * 1024

type runtimeServer struct {
	maxTransfer int64
	maxTimeout  time.Duration
}

type execRequest struct {
	Argv    []string          `json:"argv"`
	Cwd     string            `json:"cwd"`
	Env     map[string]string `json:"env"`
	User    *string           `json:"user"`
	Timeout float64           `json:"timeout_sec"`
}

type execResult struct {
	Code      int     `json:"return_code"`
	Stdout    []byte  `json:"stdout"`
	Stderr    []byte  `json:"stderr"`
	Truncated bool    `json:"truncated"`
	Duration  float64 `json:"duration_sec"`
}

type cappedBuffer struct {
	buffer    bytes.Buffer
	truncated bool
}

func (b *cappedBuffer) Write(p []byte) (int, error) {
	n := len(p)
	remaining := maxOutput - b.buffer.Len()
	if len(p) > remaining {
		p = p[:remaining]
		b.truncated = true
	}
	_, _ = b.buffer.Write(p)
	return n, nil
}

func (s runtimeServer) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"ready":true}`)
	})
	mux.HandleFunc("POST /exec", s.execute)
	mux.HandleFunc("PUT /file", s.upload)
	mux.HandleFunc("GET /file", s.download)
	mux.HandleFunc("POST /stop-processes", func(w http.ResponseWriter, r *http.Request) {
		if err := stopProcesses(r.Context()); err != nil {
			http.Error(w, "sandbox process cleanup failed", 409)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	return mux
}

func (s runtimeServer) execute(w http.ResponseWriter, r *http.Request) {
	var req execRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1024*1024))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil || len(req.Argv) == 0 {
		http.Error(w, "invalid exec request", http.StatusBadRequest)
		return
	}
	if req.User != nil && *req.User != strconv.Itoa(os.Geteuid()) && !(*req.User == "root" && os.Geteuid() == 0) {
		http.Error(w, "exec user must match sandbox UID", http.StatusBadRequest)
		return
	}
	if math.IsNaN(req.Timeout) || math.IsInf(req.Timeout, 0) || req.Timeout < 0 || req.Timeout > s.maxTimeout.Seconds() {
		http.Error(w, "exec timeout outside configured limit", http.StatusBadRequest)
		return
	}
	deadline := s.maxTimeout
	if req.Timeout > 0 {
		deadline = time.Duration(req.Timeout * float64(time.Second))
	}
	ctx, cancel := context.WithTimeout(r.Context(), deadline)
	defer cancel()
	cmd := exec.CommandContext(ctx, req.Argv[0], req.Argv[1:]...)
	cmd.Dir = req.Cwd
	cmd.Env = os.Environ()
	for key, value := range req.Env {
		if key == "" || strings.ContainsAny(key, "=\x00") || strings.ContainsRune(value, '\x00') {
			http.Error(w, "invalid exec environment", http.StatusBadRequest)
			return
		}
		cmd.Env = append(cmd.Env, key+"="+value)
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		if errors.Is(err, syscall.ESRCH) {
			return os.ErrProcessDone
		}
		return err
	}
	cmd.WaitDelay = time.Second
	var stdout, stderr cappedBuffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	started := time.Now()
	err := cmd.Run()
	code := 0
	if err != nil {
		var exitErr *exec.ExitError
		switch {
		case ctx.Err() != nil:
			code = 124
		case errors.Is(err, exec.ErrWaitDelay):
			// A shell exited but left a child holding output pipes open.
			// Bound that execution too; tmux's detached server closes its pipes.
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			code = 124
		case errors.As(err, &exitErr):
			code = exitErr.ExitCode()
		default:
			http.Error(w, "unable to execute sandbox command", http.StatusUnprocessableEntity)
			return
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(execResult{code, stdout.buffer.Bytes(), stderr.buffer.Bytes(), stdout.truncated || stderr.truncated, time.Since(started).Seconds()})
}

func (s runtimeServer) upload(w http.ResponseWriter, r *http.Request) {
	mode := uint64(0644)
	if raw := r.Header.Get("X-File-Mode"); raw != "" {
		var err error
		mode, err = strconv.ParseUint(raw, 8, 32)
		if err != nil || mode > 0777 {
			http.Error(w, "invalid file mode", 400)
			return
		}
	}
	if r.ContentLength > s.maxTransfer {
		http.Error(w, "file too large", 413)
		return
	}
	err := writeFile(r.URL.Query().Get("path"), http.MaxBytesReader(w, r.Body, s.maxTransfer), uint32(mode))
	if err != nil {
		var oversized *http.MaxBytesError
		if errors.As(err, &oversized) {
			http.Error(w, "file too large", 413)
			return
		}
		http.Error(w, "file upload rejected", 400)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s runtimeServer) download(w http.ResponseWriter, r *http.Request) {
	f, err := readFile(r.URL.Query().Get("path"))
	if err != nil {
		http.Error(w, "file download rejected", 400)
		return
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() {
		http.Error(w, "not a regular file", 400)
		return
	}
	if info.Size() > s.maxTransfer {
		http.Error(w, "file too large", 413)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatInt(info.Size(), 10))
	w.Header().Set("X-File-Mode", strconv.FormatUint(uint64(info.Mode().Perm()), 8))
	_, _ = io.CopyN(w, f, info.Size())
}

func checkSocket(path string) error {
	client := &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", path)
	}}}
	resp, err := client.Get("http://sandbox/health")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("sandbox not ready: %d", resp.StatusCode)
	}
	return nil
}

func main() {
	socket := flag.String("socket", "", "Unix socket path in dedicated emptyDir")
	check := flag.String("check-socket", "", "check an existing Unix socket and exit")
	limit := flag.Int64("max-transfer-bytes", 256*1024*1024, "maximum file transfer size")
	timeout := flag.Int("exec-timeout-seconds", 900, "maximum and default exec deadline")
	flag.Parse()
	if *check != "" {
		if err := checkSocket(*check); err != nil {
			log.Fatal("sandbox not ready")
		}
		return
	}
	if *socket == "" || *limit <= 0 || *timeout <= 0 {
		log.Fatal("socket and positive limits required")
	}
	// Do not unlink an unknown existing entry. A fresh Pod uses a fresh emptyDir.
	listener, err := net.Listen("unix", *socket)
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()
	if err := os.Chmod(*socket, 0660); err != nil {
		log.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	server := &http.Server{Handler: (runtimeServer{*limit, time.Duration(*timeout) * time.Second}).handler(), ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second, BaseContext: func(net.Listener) context.Context { return ctx }}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
	}()
	if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}
