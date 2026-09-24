package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// Package clients retain metadata and redirect connections while opening their
// parallel downloads. Keep those CONNECT sockets open while filling the budget.
func TestTaskEgressPackageConnectionBudgetAndRecovery(t *testing.T) {
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer ws.CloseNow()
		if _, _, err = ws.Read(r.Context()); err != nil {
			return
		}
		if err = ws.Write(r.Context(), websocket.MessageText, []byte(`{"status":"ready"}`)); err != nil {
			return
		}
		for {
			kind, data, err := ws.Read(r.Context())
			if err != nil {
				return
			}
			if err = ws.Write(r.Context(), kind, data); err != nil {
				return
			}
		}
	}))
	defer gateway.Close()
	root, _ := url.Parse(gateway.URL + "/internal/service-execution")
	tokenFile := filepath.Join(t.TempDir(), "pod-token")
	if err := os.WriteFile(tokenFile, []byte("pod-token"), 0600); err != nil {
		t.Fatal(err)
	}
	broker := &workloadBroker{podTokenFile: tokenFile, root: root, identity: workloadIdentity{LeaseID: "package-lease", Generation: 1, ExecutionRole: "attempt"}, client: gateway.Client()}
	broker.setPhase("agent", time.Now().Add(time.Minute))
	proxy, stop, err := broker.startTaskEgress(context.Background(), &webAllowlist{
		Kind: "web-allowlist", Destinations: []webDestination{
			{Host: "api.example.org", Protocol: "https"},
			{Host: "downloads.example.org", Protocol: "https"},
			{Host: "metadata.example.org", Protocol: "https"},
		},
	}, "sha256:bound", io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	proxyURL, _ := url.Parse(proxy)
	var connections []net.Conn
	defer func() {
		for _, connection := range connections {
			connection.Close()
		}
	}()
	connect := func(host string) (net.Conn, int) {
		t.Helper()
		connection, err := net.DialTimeout("tcp", proxyURL.Host, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if err = connection.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
			t.Fatal(err)
		}
		if _, err = fmt.Fprintf(connection, "CONNECT %s:443 HTTP/1.1\r\nHost: %s:443\r\n\r\n", host, host); err != nil {
			t.Fatal(err)
		}
		reader := bufio.NewReader(connection)
		response, err := http.ReadResponse(reader, &http.Request{Method: http.MethodConnect})
		if err != nil {
			connection.Close()
			t.Fatal(err)
		}
		if response.StatusCode != http.StatusOK {
			response.Body.Close()
			connection.Close()
			return nil, response.StatusCode
		}
		// Exercise retained tunnels, not only successful CONNECT responses.
		if _, err = io.WriteString(connection, "package"); err != nil {
			t.Fatal(err)
		}
		payload := make([]byte, len("package"))
		if _, err = io.ReadFull(reader, payload); err != nil || string(payload) != "package" {
			connection.Close()
			t.Fatalf("tunnel lost package bytes: %q, %v", payload, err)
		}
		return connection, response.StatusCode
	}
	hosts := []string{"metadata.example.org", "api.example.org", "downloads.example.org"}
	for i := 0; i < 32; i++ {
		connection, status := connect(hosts[i%len(hosts)])
		if status != http.StatusOK {
			t.Fatalf("package connection %d rejected with %d", i+1, status)
		}
		connections = append(connections, connection)
	}
	if connection, status := connect("downloads.example.org"); status != http.StatusTooManyRequests {
		if connection != nil {
			connection.Close()
		}
		t.Fatalf("excess connection was not bounded: %d", status)
	}
	connections[0].Close()
	// Closure crosses the proxy and WebSocket goroutines before releasing a seat.
	until := time.Now().Add(5 * time.Second)
	for {
		connection, status := connect("downloads.example.org")
		if status == http.StatusOK {
			connections = append(connections, connection)
			break
		}
		if status != http.StatusTooManyRequests || time.Now().After(until) {
			t.Fatalf("released capacity was not reusable: %d", status)
		}
		time.Sleep(10 * time.Millisecond)
	}
	broker.setPhase("agent", time.Time{})
	for _, connection := range connections[1:] {
		connection.SetReadDeadline(time.Now().Add(time.Second))
		if _, err := connection.Read(make([]byte, 1)); err != io.EOF {
			t.Fatalf("phase completion did not close retained connection: %v", err)
		}
	}
}
