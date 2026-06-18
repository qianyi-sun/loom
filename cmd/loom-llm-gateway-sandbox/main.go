// Command loom-llm-gateway-sandbox is the per-node TLS-terminating
// HTTP CONNECT proxy that sandbox containers reach LLM gateways
// through (#78 Phase B).
//
// Lifecycle:
//
//  1. Each per-trial `--internal` docker bridge (#78 Phase A,
//     PR #189) blocks the sandbox from reaching anything off-host.
//  2. The worker (PR-B2, follow-up) attaches THIS singleton to
//     every per-trial bridge so the sandbox CAN reach
//     `loom-sandbox-gateway.local:8443`.
//  3. The sandbox sends `CONNECT <upstream>:<port>` with a
//     step-JWT in `Authorization: Bearer ...` OR `x-api-key`.
//  4. We verify the JWT (HS256, shared signing key with Control
//     Plane) and tunnel the bytes to the upstream URL.
//  5. The upstream is the in-cluster `gateway-router` Service,
//     which fans out to llm-gateway pods.
//
// The binary is intentionally minimal: TLS terminate, JWT verify,
// CONNECT tunnel. No request inspection beyond the JWT header.
// Per the spike (#196), filter shape live in Envoy (egress side)
// for ALL transport-layer policy.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	cfg := parseFlags()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	signingKey, err := os.ReadFile(cfg.JWTSigningKeyFile)
	if err != nil {
		logger.Error("read jwt signing key", "path", cfg.JWTSigningKeyFile, "err", err)
		os.Exit(2)
	}
	// Strip trailing newline if a human edited the file with vim.
	signingKey = trimTrailingNewlines(signingKey)

	tlsConfig, err := loadServerTLS(cfg.TLSCertFile, cfg.TLSKeyFile)
	if err != nil {
		logger.Error("load tls", "err", err)
		os.Exit(2)
	}

	handler := newConnectProxy(connectProxyConfig{
		Upstream:   cfg.UpstreamURL,
		SigningKey: signingKey,
		Logger:     logger,
	})

	server := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           handler,
		TLSConfig:         tlsConfig,
		ReadHeaderTimeout: 10 * time.Second,
		// Per-request deadlines live in the CONNECT handler; the
		// server-level timeouts are intentionally loose because
		// tunnels are long-lived.
	}

	// Graceful shutdown plumbing — SIGTERM unblocks the Wait;
	// in-flight tunnels get 30s to drain before forced close.
	ctx, cancel := signal.NotifyContext(context.Background(),
		syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		logger.Info("listening", "addr", cfg.ListenAddr,
			"upstream", cfg.UpstreamURL)
		// ListenAndServeTLS uses the TLSConfig we set above; cert
		// + key file args are required by the API but ignored when
		// TLSConfig.Certificates is populated.
		err := server.ListenAndServeTLS("", "")
		if !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received")
	case err := <-errCh:
		logger.Error("listen failed", "err", err)
		os.Exit(1)
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(
		context.Background(), 30*time.Second)
	defer shutdownCancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Warn("shutdown error", "err", err)
	}
}

type config struct {
	ListenAddr        string
	UpstreamURL       string
	JWTSigningKeyFile string
	TLSCertFile       string
	TLSKeyFile        string
}

func parseFlags() config {
	c := config{}
	flag.StringVar(&c.ListenAddr, "listen-addr", ":8443",
		"TLS address to bind. Reachable from the per-trial sandbox bridge.")
	flag.StringVar(&c.UpstreamURL, "upstream-url",
		"http://gateway-router:30443",
		"In-cluster URL of the gateway-router service. CONNECT requests forward here.")
	flag.StringVar(&c.JWTSigningKeyFile, "jwt-signing-key-file",
		"/run/loom/jwt-signing-key",
		"Path to the HS256 signing key (shared with Control Plane's LOOM_CP_STEP_JWT_SIGNING_KEY).")
	flag.StringVar(&c.TLSCertFile, "tls-cert-file",
		"/run/loom/loom-sandbox-gateway.crt",
		"Path to the TLS cert (loom-ca-signed, served on loom-sandbox-gateway.local).")
	flag.StringVar(&c.TLSKeyFile, "tls-key-file",
		"/run/loom/loom-sandbox-gateway.key",
		"Path to the TLS private key.")
	flag.Parse()
	if err := c.validate(); err != nil {
		fmt.Fprintln(os.Stderr, "config:", err)
		os.Exit(2)
	}
	return c
}

func (c config) validate() error {
	if c.ListenAddr == "" {
		return errors.New("--listen-addr required")
	}
	if c.UpstreamURL == "" {
		return errors.New("--upstream-url required")
	}
	if c.JWTSigningKeyFile == "" {
		return errors.New("--jwt-signing-key-file required")
	}
	if c.TLSCertFile == "" || c.TLSKeyFile == "" {
		return errors.New("--tls-cert-file and --tls-key-file required")
	}
	return nil
}

func trimTrailingNewlines(b []byte) []byte {
	for len(b) > 0 && (b[len(b)-1] == '\n' || b[len(b)-1] == '\r') {
		b = b[:len(b)-1]
	}
	return b
}
