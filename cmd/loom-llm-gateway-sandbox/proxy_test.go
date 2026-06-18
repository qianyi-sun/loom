// End-to-end tests for the CONNECT proxy handler. Spin up a real
// HTTP server, send CONNECT, verify byte tunneling + JWT enforcement.

package main

import (
	"bufio"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// startProxy starts the handler on a free TLS port and returns the
// addr + a cleanup. Self-signed cert so the test client trusts it
// via a per-test CA pool.
func startProxy(t *testing.T, upstreamURL string) (addr string, caPool *x509.CertPool) {
	t.Helper()

	cert, caPool := selfSignedCert(t)

	handler := newConnectProxy(connectProxyConfig{
		Upstream:   upstreamURL,
		SigningKey: testSigningKey,
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	})

	listener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	})
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	server := &http.Server{
		Handler: handler,
	}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		_ = server.Close()
		_ = listener.Close()
	})

	return listener.Addr().String(), caPool
}

func TestProxy_HappyPath_TunnelsToUpstream(t *testing.T) {
	// Upstream is a plain HTTP server that echoes the path back —
	// we don't actually open a TLS tunnel to it (the spec says
	// CONNECT to gateway-router which speaks plain HTTP in-cluster).
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("upstream_saw:" + r.URL.Path))
	}))
	defer upstream.Close()

	addr, caPool := startProxy(t, upstream.URL)

	// Open a TLS connection to the proxy.
	conn, err := tls.Dial("tcp", addr, &tls.Config{
		RootCAs:    caPool,
		ServerName: "localhost",
	})
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer conn.Close()

	tok := mintTestJWT(t, nil)
	// Send CONNECT with the JWT as Bearer.
	req := "CONNECT example.com:443 HTTP/1.1\r\n" +
		"Host: example.com:443\r\n" +
		"Authorization: Bearer " + tok + "\r\n\r\n"
	if _, err := conn.Write([]byte(req)); err != nil {
		t.Fatalf("write CONNECT: %v", err)
	}

	// Read the proxy's response line.
	reader := bufio.NewReader(conn)
	status, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read CONNECT response: %v", err)
	}
	if !strings.Contains(status, "200") {
		t.Fatalf("expected 200 CONNECT response, got: %q", status)
	}
	// Consume the empty line after the status.
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("read headers: %v", err)
		}
		if line == "\r\n" || line == "\n" {
			break
		}
	}

	// Now send a raw HTTP request through the tunnel — the upstream
	// is HTTP not HTTPS, so we speak HTTP/1.1 directly.
	httpReq := "GET /hello HTTP/1.1\r\nHost: " + upstream.Listener.Addr().String() + "\r\n\r\n"
	if _, err := conn.Write([]byte(httpReq)); err != nil {
		t.Fatalf("write GET: %v", err)
	}

	// Read upstream's response.
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 4096)
	n, err := conn.Read(buf)
	if err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("read upstream response: %v", err)
	}
	body := string(buf[:n])
	if !strings.Contains(body, "upstream_saw:/hello") {
		t.Errorf("upstream didn't see the path; body=%q", body)
	}
}

func TestProxy_RejectsMissingToken(t *testing.T) {
	addr, caPool := startProxy(t, "http://127.0.0.1:9999")
	conn, err := tls.Dial("tcp", addr, &tls.Config{
		RootCAs: caPool, ServerName: "localhost",
	})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	req := "CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\n\r\n"
	conn.Write([]byte(req))
	resp := readStatusLine(t, conn)
	if !strings.Contains(resp, "401") {
		t.Errorf("expected 401, got: %q", resp)
	}
}

func TestProxy_RejectsInvalidToken(t *testing.T) {
	addr, caPool := startProxy(t, "http://127.0.0.1:9999")
	conn, err := tls.Dial("tcp", addr, &tls.Config{
		RootCAs: caPool, ServerName: "localhost",
	})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	req := "CONNECT example.com:443 HTTP/1.1\r\n" +
		"Host: example.com:443\r\n" +
		"Authorization: Bearer loom_step_obviouslygarbage\r\n\r\n"
	conn.Write([]byte(req))
	resp := readStatusLine(t, conn)
	if !strings.Contains(resp, "401") {
		t.Errorf("expected 401, got: %q", resp)
	}
}

func TestProxy_RejectsNonConnect(t *testing.T) {
	addr, caPool := startProxy(t, "http://127.0.0.1:9999")
	conn, err := tls.Dial("tcp", addr, &tls.Config{
		RootCAs: caPool, ServerName: "localhost",
	})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	req := "GET / HTTP/1.1\r\nHost: example.com\r\n\r\n"
	conn.Write([]byte(req))
	resp := readStatusLine(t, conn)
	if !strings.Contains(resp, "405") {
		t.Errorf("expected 405 Method Not Allowed, got: %q", resp)
	}
}

func TestProxy_AcceptsXApiKeyHeader(t *testing.T) {
	// Anthropic dialect uses x-api-key instead of Authorization;
	// both forms must work — sandbox SDK picks based on which
	// provider it's emulating.
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	addr, caPool := startProxy(t, upstream.URL)
	conn, err := tls.Dial("tcp", addr, &tls.Config{
		RootCAs: caPool, ServerName: "localhost",
	})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	tok := mintTestJWT(t, nil)
	req := "CONNECT example.com:443 HTTP/1.1\r\n" +
		"Host: example.com:443\r\n" +
		"x-api-key: " + tok + "\r\n\r\n"
	conn.Write([]byte(req))
	resp := readStatusLine(t, conn)
	if !strings.Contains(resp, "200") {
		t.Errorf("expected 200, got: %q", resp)
	}
}

func readStatusLine(t *testing.T, conn net.Conn) string {
	t.Helper()
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	br := bufio.NewReader(conn)
	line, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("read status: %v", err)
	}
	return line
}
