package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestProtocolProjectReturnsValidatedCapabilitiesAndAcknowledgesResponse(t *testing.T) {
	useTestProtocolPolicy(t)
	socketPath := testSocketPath(t)
	server := startSeqpacketServer(t, socketPath, func(connFD int) {
		payload, rights, creds, flags := receiveSeqpacket(t, connFD, 4096)
		if flags&^syscall.MSG_CMSG_CLOEXEC != 0 {
			t.Fatalf("recv flags = %d, want 0", flags)
		}
		if creds == nil || creds.Pid <= 0 {
			t.Fatalf("credentials = %#v, want populated SCM_CREDENTIALS", creds)
		}
		if len(rights) != 0 {
			t.Fatalf("received %d rights, want 0", len(rights))
		}
		assertExactJSON(t, payload, `{"grant_id":"11111111-1111-4111-8111-111111111111","operation":"project","schema":"`+localSchema+`"}`)

		bootstrapFD := createMemfdFixture(t, "bootstrap", []byte(`{"bootstrap_token":"sentinel-secret-text"}`), requiredMemfdSeals, true)
		workspace := openDirectoryFD(t, t.TempDir())
		buildEgress := openDirectoryFD(t, t.TempDir())
		workspaceStat := mustFstat(t, workspace)
		buildStat := mustFstat(t, buildEgress)
		sendSeqpacket(
			t,
			connFD,
			[]byte(fmt.Sprintf(`{"schema":"%s","operation":"projected","response_id":"22222222-2222-4222-8222-222222222222","grant_id":"11111111-1111-4111-8111-111111111111","proof_sha256":"%s","receipt_public_binding_sha256":"%s","rights":[{"index":0,"kind":"sealed_memfd","role":"bootstrap"},{"index":1,"kind":"directory","role":"job_storage","device":%d,"inode":%d},{"index":2,"kind":"directory","role":"build_egress","device":%d,"inode":%d}]}`, localSchema, strings.Repeat("a", 64), strings.Repeat("b", 64), workspaceStat.Dev, workspaceStat.Ino, buildStat.Dev, buildStat.Ino)),
			[]int{bootstrapFD, workspace, buildEgress},
		)
		syscall.Close(bootstrapFD)
		syscall.Close(workspace)
		syscall.Close(buildEgress)

		ackPayload, ackRights, _, ackFlags := receiveSeqpacket(t, connFD, 4096)
		if ackFlags&^syscall.MSG_CMSG_CLOEXEC != 0 {
			t.Fatalf("ack flags = %d, want 0", ackFlags)
		}
		if len(ackRights) != 0 {
			t.Fatalf("ack rights = %d, want 0", len(ackRights))
		}
		assertExactJSON(t, ackPayload, `{"operation":"ack","response_id":"22222222-2222-4222-8222-222222222222","schema":"`+localSchema+`"}`)
	})
	defer server.Close()

	client := NewGuardClient(socketPath, 4096, 2*time.Second)
	caps, err := client.Project(context.Background(), "11111111-1111-4111-8111-111111111111")
	if err != nil {
		t.Fatalf("Project() error = %v", err)
	}
	if got := string(caps.Bootstrap.data); !strings.Contains(got, "sentinel-secret-text") {
		t.Fatalf("bootstrap contents = %q, want sentinel secret", got)
	}
	if caps.JobDirectoryFD < 0 || caps.BuildEgressFD < 0 {
		t.Fatalf("returned directory descriptors invalid: %#v", caps)
	}
	caps.Close()
}

func TestProtocolProjectRejectsDescriptorMutationsAndClosesPartialRights(t *testing.T) {
	useTestProtocolPolicy(t)
	tests := []struct {
		name     string
		response func(*testing.T, int)
	}{
		{
			name: "wrong descriptor count",
			response: func(t *testing.T, connFD int) {
				bootstrapFD := createMemfdFixture(t, "bootstrap", []byte(`{"bootstrap_token":"sentinel-secret-text"}`), requiredMemfdSeals, true)
				workspace := openDirectoryFD(t, t.TempDir())
				sendSeqpacket(t, connFD, []byte(`{"schema":"`+localSchema+`","operation":"projected","response_id":"22222222-2222-4222-8222-222222222222","grant_id":"11111111-1111-4111-8111-111111111111","proof_sha256":"`+strings.Repeat("a", 64)+`","receipt_public_binding_sha256":"`+strings.Repeat("b", 64)+`","rights":[{"index":0,"kind":"sealed_memfd","role":"bootstrap"},{"index":1,"kind":"directory","role":"job_storage","device":1,"inode":1},{"index":2,"kind":"directory","role":"build_egress","device":2,"inode":2}]}`), []int{bootstrapFD, workspace})
				syscall.Close(bootstrapFD)
				syscall.Close(workspace)
			},
		},
		{
			name: "wrong descriptor order",
			response: func(t *testing.T, connFD int) {
				bootstrapFD := createMemfdFixture(t, "bootstrap", []byte(`{"bootstrap_token":"sentinel-secret-text"}`), requiredMemfdSeals, true)
				workspace := openDirectoryFD(t, t.TempDir())
				buildEgress := openDirectoryFD(t, t.TempDir())
				workspaceStat := mustFstat(t, workspace)
				buildStat := mustFstat(t, buildEgress)
				sendSeqpacket(
					t,
					connFD,
					[]byte(fmt.Sprintf(`{"schema":"%s","operation":"projected","response_id":"22222222-2222-4222-8222-222222222222","grant_id":"11111111-1111-4111-8111-111111111111","proof_sha256":"%s","receipt_public_binding_sha256":"%s","rights":[{"index":0,"kind":"sealed_memfd","role":"bootstrap"},{"index":1,"kind":"directory","role":"job_storage","device":%d,"inode":%d},{"index":2,"kind":"directory","role":"build_egress","device":%d,"inode":%d}]}`, localSchema, strings.Repeat("a", 64), strings.Repeat("b", 64), workspaceStat.Dev, workspaceStat.Ino, buildStat.Dev, buildStat.Ino)),
					[]int{workspace, bootstrapFD, buildEgress},
				)
				syscall.Close(bootstrapFD)
				syscall.Close(workspace)
				syscall.Close(buildEgress)
			},
		},
		{
			name: "mismatched inode metadata",
			response: func(t *testing.T, connFD int) {
				bootstrapFD := createMemfdFixture(t, "bootstrap", []byte(`{"bootstrap_token":"sentinel-secret-text"}`), requiredMemfdSeals, true)
				workspace := openDirectoryFD(t, t.TempDir())
				buildEgress := openDirectoryFD(t, t.TempDir())
				workspaceStat := mustFstat(t, workspace)
				buildStat := mustFstat(t, buildEgress)
				sendSeqpacket(
					t,
					connFD,
					[]byte(fmt.Sprintf(`{"schema":"%s","operation":"projected","response_id":"22222222-2222-4222-8222-222222222222","grant_id":"11111111-1111-4111-8111-111111111111","proof_sha256":"%s","receipt_public_binding_sha256":"%s","rights":[{"index":0,"kind":"sealed_memfd","role":"bootstrap"},{"index":1,"kind":"directory","role":"job_storage","device":%d,"inode":%d},{"index":2,"kind":"directory","role":"build_egress","device":%d,"inode":%d}]}`, localSchema, strings.Repeat("a", 64), strings.Repeat("b", 64), workspaceStat.Dev, workspaceStat.Ino+1, buildStat.Dev, buildStat.Ino)),
					[]int{bootstrapFD, workspace, buildEgress},
				)
				syscall.Close(bootstrapFD)
				syscall.Close(workspace)
				syscall.Close(buildEgress)
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			socketPath := testSocketPath(t)
			before := countOpenFileDescriptors(t)
			server := startSeqpacketServer(t, socketPath, func(connFD int) {
				_, _, _, _ = receiveSeqpacket(t, connFD, 4096)
				tt.response(t, connFD)
			})
			client := NewGuardClient(socketPath, 4096, 2*time.Second)
			_, err := client.Project(context.Background(), "11111111-1111-4111-8111-111111111111")
			if err == nil {
				t.Fatal("Project() succeeded, want error")
			}
			server.Close()
			after := countOpenFileDescriptors(t)
			if after != before {
				t.Fatalf("fd leak detected: before=%d after=%d", before, after)
			}
		})
	}
}

func TestProtocolExchangeSendsSealedSecretDescriptorAndAcknowledgesSessionResponse(t *testing.T) {
	useTestProtocolPolicy(t)
	socketPath := testSocketPath(t)
	server := startSeqpacketServer(t, socketPath, func(connFD int) {
		payload, rights, _, _ := receiveSeqpacket(t, connFD, 4096)
		if len(rights) != 1 {
			t.Fatalf("received %d rights, want 1", len(rights))
		}
		assertExactJSON(t, payload, `{"exchange_id":"22222222-2222-4222-8222-222222222222","grant_id":"11111111-1111-4111-8111-111111111111","operation":"exchange","proof_sha256":"`+strings.Repeat("a", 64)+`","schema":"`+localSchema+`"}`)
		buffer, err := NewSecretBuffer(rights[0], 64*1024)
		if err != nil {
			t.Fatalf("NewSecretBuffer() error = %v", err)
		}
		var exchange struct {
			SchemaVersion  int    `json:"schema_version"`
			ExchangeID     string `json:"exchange_id"`
			GrantID        string `json:"grant_id"`
			ProofSHA256    string `json:"proof_sha256"`
			BootstrapToken string `json:"bootstrap_token"`
			ObservedAt     string `json:"observed_at"`
		}
		if err := decodeStrictJSON(buffer.data, &exchange); err != nil {
			t.Fatalf("exchange descriptor JSON error = %v", err)
		}
		if exchange.SchemaVersion != 1 ||
			exchange.ExchangeID != "22222222-2222-4222-8222-222222222222" ||
			exchange.GrantID != "11111111-1111-4111-8111-111111111111" ||
			exchange.ProofSHA256 != strings.Repeat("a", 64) ||
			exchange.BootstrapToken != "sentinel-secret-text" {
			t.Fatalf("exchange descriptor binding = %#v", exchange)
		}
		if _, err := time.Parse(time.RFC3339, exchange.ObservedAt); err != nil {
			t.Fatalf("exchange observed_at = %q, want RFC3339 timestamp", exchange.ObservedAt)
		}
		buffer.Close()
		sessionFD := createMemfdFixture(t, "session", []byte(`{"schema_version":2,"grant_id":"11111111-1111-4111-8111-111111111111","session_id":"33333333-3333-4333-8333-333333333333","purpose":"production","shadow_campaign_id":null,"pool_id":"staging-gb10-task-image","cpu_arch":"`+runtimeSessionArch()+`","session_token":"sentinel-secret-text","generation":2,"attestation_generation":2,"attestation_sha256":"`+strings.Repeat("b", 64)+`","issued_at":"2026-09-03T00:00:00Z","expires_at":"2026-09-03T00:10:00Z"}`), requiredMemfdSeals, true)
		sendSeqpacket(t, connFD, []byte(`{"schema":"`+localSchema+`","operation":"session","response_id":"44444444-4444-4444-8444-444444444444","grant_id":"11111111-1111-4111-8111-111111111111","session_id":"33333333-3333-4333-8333-333333333333","session_generation":2,"session_public_binding_sha256":"`+strings.Repeat("c", 64)+`"}`), []int{sessionFD})
		syscall.Close(sessionFD)
		ackPayload, _, _, _ := receiveSeqpacket(t, connFD, 4096)
		assertExactJSON(t, ackPayload, `{"operation":"ack","response_id":"44444444-4444-4444-8444-444444444444","schema":"`+localSchema+`"}`)
	})
	defer server.Close()

	client := NewGuardClient(socketPath, 4096, 2*time.Second)
	bootstrapFD := createMemfdFixture(t, "bootstrap", []byte(`{"schema_version":1,"grant_id":"11111111-1111-4111-8111-111111111111","bootstrap_token":"sentinel-secret-text"}`), requiredMemfdSeals, true)
	bootstrap, err := NewSecretBuffer(bootstrapFD, 64*1024)
	if err != nil {
		t.Fatalf("NewSecretBuffer() error = %v", err)
	}
	session, err := client.Exchange(context.Background(), "11111111-1111-4111-8111-111111111111", "22222222-2222-4222-8222-222222222222", strings.Repeat("a", 64), bootstrap)
	if err != nil {
		t.Fatalf("Exchange() error = %v", err)
	}
	if session.Generation != 2 {
		t.Fatalf("Generation = %d, want 2", session.Generation)
	}
	if !session.ExpiresAt.Equal(time.Date(2026, 9, 3, 0, 10, 0, 0, time.UTC)) {
		t.Fatalf("ExpiresAt = %s", session.ExpiresAt)
	}
	bootstrap.Close()
	session.Secret.Close()
}

func TestProtocolUsesAbsoluteDeadlines(t *testing.T) {
	useTestProtocolPolicy(t)
	socketPath := testSocketPath(t)
	server := startSeqpacketServer(t, socketPath, func(connFD int) {
		defer syscall.Close(connFD)
		time.Sleep(250 * time.Millisecond)
	})
	defer server.Close()

	client := NewGuardClient(socketPath, 4096, 2*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err := client.Project(ctx, "11111111-1111-4111-8111-111111111111")
	if err == nil {
		t.Fatal("Project() succeeded, want deadline error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want context deadline exceeded", err)
	}
}

func TestProtocolRetriesInterruptedResponseReceive(t *testing.T) {
	useTestProtocolPolicy(t)
	socketPath := testSocketPath(t)
	server := startSeqpacketServer(t, socketPath, func(connFD int) {
		_, _, _, _ = receiveSeqpacket(t, connFD, 4096)
		sendSeqpacket(t, connFD, []byte(`{"schema":"`+localSchema+`","operation":"finishing","response_id":"22222222-2222-4222-8222-222222222222","grant_id":"11111111-1111-4111-8111-111111111111","operation_id":"33333333-3333-4333-8333-333333333333"}`), nil)
		ackPayload, _, _, _ := receiveSeqpacket(t, connFD, 4096)
		assertExactJSON(t, ackPayload, `{"operation":"ack","response_id":"22222222-2222-4222-8222-222222222222","schema":"`+localSchema+`"}`)
	})
	defer server.Close()

	previousRecvmsg := unixRecvmsg
	defer func() { unixRecvmsg = previousRecvmsg }()
	calls := 0
	unixRecvmsg = func(fd int, payload []byte, oob []byte, flags int) (int, int, int, syscall.Sockaddr, error) {
		calls++
		if calls == 1 {
			return 0, 0, 0, nil, syscall.EINTR
		}
		return previousRecvmsg(fd, payload, oob, flags)
	}

	client := NewGuardClient(socketPath, 4096, 2*time.Second)
	if err := client.Finish(context.Background(), "11111111-1111-4111-8111-111111111111", "33333333-3333-4333-8333-333333333333", map[string]int{
		"descendant_processes": 0,
		"mounts":               0,
		"sockets":              0,
		"open_files":           0,
	}); err != nil {
		t.Fatalf("Finish() error = %v", err)
	}
	if calls < 2 {
		t.Fatalf("recvmsg calls = %d, want retry after EINTR", calls)
	}
}

func TestProtocolRetriesInterruptedConnectWithFreshSocket(t *testing.T) {
	useTestProtocolPolicy(t)
	socketPath := testSocketPath(t)
	server := startSeqpacketServer(t, socketPath, func(connFD int) {
		_, _, _, _ = receiveSeqpacket(t, connFD, 4096)
		sendSeqpacket(t, connFD, []byte(`{"schema":"`+localSchema+`","operation":"finishing","response_id":"22222222-2222-4222-8222-222222222222","grant_id":"11111111-1111-4111-8111-111111111111","operation_id":"33333333-3333-4333-8333-333333333333"}`), nil)
		ackPayload, _, _, _ := receiveSeqpacket(t, connFD, 4096)
		assertExactJSON(t, ackPayload, `{"operation":"ack","response_id":"22222222-2222-4222-8222-222222222222","schema":"`+localSchema+`"}`)
	})
	defer server.Close()

	previousSocket := unixSocket
	previousSetsockopt := unixSetsockoptInt
	previousConnect := unixConnect
	defer func() {
		unixSocket = previousSocket
		unixSetsockoptInt = previousSetsockopt
		unixConnect = previousConnect
	}()
	socketCalls := 0
	connectCalls := 0
	unixSocket = func(domain int, typ int, proto int) (int, error) {
		socketCalls++
		return previousSocket(domain, typ, proto)
	}
	unixSetsockoptInt = func(fd int, level int, opt int, value int) error {
		return previousSetsockopt(fd, level, opt, value)
	}
	unixConnect = func(fd int, sa syscall.Sockaddr) error {
		connectCalls++
		if connectCalls == 1 {
			return syscall.EINTR
		}
		return previousConnect(fd, sa)
	}

	client := NewGuardClient(socketPath, 4096, 2*time.Second)
	if err := client.Finish(context.Background(), "11111111-1111-4111-8111-111111111111", "33333333-3333-4333-8333-333333333333", map[string]int{
		"descendant_processes": 0,
		"mounts":               0,
		"sockets":              0,
		"open_files":           0,
	}); err != nil {
		t.Fatalf("Finish() error = %v", err)
	}
	if connectCalls < 2 {
		t.Fatalf("connect calls = %d, want retry after EINTR", connectCalls)
	}
	if socketCalls < 2 {
		t.Fatalf("socket calls = %d, want fresh socket after interrupted connect", socketCalls)
	}
}

func TestProtocolInterruptedConnectPreservesAbsoluteDeadline(t *testing.T) {
	useTestProtocolPolicy(t)
	socketPath := testSocketPath(t)
	received := make(chan bool, 1)
	server := startSeqpacketServer(t, socketPath, func(connFD int) {
		received <- tryReceiveSeqpacket(connFD, 4096, 250*time.Millisecond)
	})
	defer func() {
		fd, err := syscall.Socket(syscall.AF_UNIX, syscall.SOCK_SEQPACKET|syscall.SOCK_CLOEXEC, 0)
		if err == nil {
			_ = syscall.Connect(fd, &syscall.SockaddrUnix{Name: socketPath})
			syscall.Close(fd)
		}
		server.Close()
	}()

	previousSocket := unixSocket
	previousSetsockopt := unixSetsockoptInt
	previousConnect := unixConnect
	defer func() {
		unixSocket = previousSocket
		unixSetsockoptInt = previousSetsockopt
		unixConnect = previousConnect
	}()
	socketCalls := 0
	connectCalls := 0
	unixSocket = func(domain int, typ int, proto int) (int, error) {
		socketCalls++
		return previousSocket(domain, typ, proto)
	}
	unixSetsockoptInt = func(fd int, level int, opt int, value int) error {
		return previousSetsockopt(fd, level, opt, value)
	}
	unixConnect = func(fd int, sa syscall.Sockaddr) error {
		connectCalls++
		if connectCalls <= 3 {
			time.Sleep(40 * time.Millisecond)
			return syscall.EINTR
		}
		return previousConnect(fd, sa)
	}

	client := NewGuardClient(socketPath, 4096, 500*time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	started := time.Now()

	err := client.Finish(ctx, "11111111-1111-4111-8111-111111111111", "33333333-3333-4333-8333-333333333333", map[string]int{
		"descendant_processes": 0,
		"mounts":               0,
		"sockets":              0,
		"open_files":           0,
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Finish() error = %v, want context deadline exceeded", err)
	}
	if elapsed := time.Since(started); elapsed > 220*time.Millisecond {
		t.Fatalf("Finish() elapsed = %v, want bounded by absolute deadline", elapsed)
	}
	if connectCalls < 3 {
		t.Fatalf("connect calls = %d, want repeated EINTR retries before deadline", connectCalls)
	}
	if socketCalls < 3 {
		t.Fatalf("socket calls = %d, want fresh sockets for interrupted connect retries", socketCalls)
	}
	select {
	case got := <-received:
		if got {
			t.Fatal("request was sent after connect deadline expired")
		}
	case <-time.After(300 * time.Millisecond):
	}
}

func TestProtocolInterruptedRequestSendPreservesAbsoluteDeadline(t *testing.T) {
	useTestProtocolPolicy(t)
	socketPath := testSocketPath(t)
	received := make(chan bool, 1)
	server := startSeqpacketServer(t, socketPath, func(connFD int) {
		received <- tryReceiveSeqpacket(connFD, 4096, 250*time.Millisecond)
	})
	defer server.Close()

	previousSendmsg := unixSendmsgN
	defer func() { unixSendmsgN = previousSendmsg }()
	calls := 0
	unixSendmsgN = func(fd int, payload []byte, oob []byte, to syscall.Sockaddr, flags int) (int, error) {
		calls++
		if calls <= 3 {
			time.Sleep(40 * time.Millisecond)
			return 0, syscall.EINTR
		}
		return previousSendmsg(fd, payload, oob, to, flags)
	}

	client := NewGuardClient(socketPath, 4096, 500*time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	started := time.Now()

	err := client.Finish(ctx, "11111111-1111-4111-8111-111111111111", "33333333-3333-4333-8333-333333333333", map[string]int{
		"descendant_processes": 0,
		"mounts":               0,
		"sockets":              0,
		"open_files":           0,
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Finish() error = %v, want context deadline exceeded", err)
	}
	if elapsed := time.Since(started); elapsed > 220*time.Millisecond {
		t.Fatalf("Finish() elapsed = %v, want bounded by absolute deadline", elapsed)
	}
	if got := <-received; got {
		t.Fatal("request was sent after deadline expired")
	}
}

func TestProtocolInterruptedResponseReceivePreservesAbsoluteDeadline(t *testing.T) {
	useTestProtocolPolicy(t)
	socketPath := testSocketPath(t)
	server := startSeqpacketServer(t, socketPath, func(connFD int) {
		_, _, _, _ = receiveSeqpacket(t, connFD, 4096)
		time.Sleep(300 * time.Millisecond)
	})
	defer server.Close()

	previousRecvmsg := unixRecvmsg
	defer func() { unixRecvmsg = previousRecvmsg }()
	calls := 0
	unixRecvmsg = func(fd int, payload []byte, oob []byte, flags int) (int, int, int, syscall.Sockaddr, error) {
		calls++
		if calls <= 3 {
			time.Sleep(40 * time.Millisecond)
			return 0, 0, 0, nil, syscall.EINTR
		}
		return previousRecvmsg(fd, payload, oob, flags)
	}

	client := NewGuardClient(socketPath, 4096, 500*time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	started := time.Now()

	err := client.Finish(ctx, "11111111-1111-4111-8111-111111111111", "33333333-3333-4333-8333-333333333333", map[string]int{
		"descendant_processes": 0,
		"mounts":               0,
		"sockets":              0,
		"open_files":           0,
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Finish() error = %v, want context deadline exceeded", err)
	}
	if elapsed := time.Since(started); elapsed > 220*time.Millisecond {
		t.Fatalf("Finish() elapsed = %v, want bounded by absolute deadline", elapsed)
	}
	if calls < 3 {
		t.Fatalf("recvmsg calls = %d, want repeated EINTR retries before deadline", calls)
	}
}

func TestProtocolRetriesInterruptedExchangeSendWithoutDuplicatingRights(t *testing.T) {
	useTestProtocolPolicy(t)
	socketPath := testSocketPath(t)
	server := startSeqpacketServer(t, socketPath, func(connFD int) {
		payload, rights, _, _ := receiveSeqpacket(t, connFD, 4096)
		if len(rights) != 1 {
			t.Fatalf("received %d rights, want 1", len(rights))
		}
		closeRights(rights)
		assertExactJSON(t, payload, `{"exchange_id":"22222222-2222-4222-8222-222222222222","grant_id":"11111111-1111-4111-8111-111111111111","operation":"exchange","proof_sha256":"`+strings.Repeat("a", 64)+`","schema":"`+localSchema+`"}`)
		if tryReceiveSeqpacket(connFD, 4096, 50*time.Millisecond) {
			t.Fatal("duplicate request packet received after send retry")
		}
		sessionFD := createMemfdFixture(t, "session", []byte(`{"schema_version":2,"grant_id":"11111111-1111-4111-8111-111111111111","session_id":"33333333-3333-4333-8333-333333333333","purpose":"production","shadow_campaign_id":null,"pool_id":"staging-gb10-task-image","cpu_arch":"`+runtimeSessionArch()+`","session_token":"sentinel-secret-text","generation":2,"attestation_generation":2,"attestation_sha256":"`+strings.Repeat("b", 64)+`","issued_at":"2026-09-03T00:00:00Z","expires_at":"2026-09-03T00:10:00Z"}`), requiredMemfdSeals, true)
		sendSeqpacket(t, connFD, []byte(`{"schema":"`+localSchema+`","operation":"session","response_id":"44444444-4444-4444-8444-444444444444","grant_id":"11111111-1111-4111-8111-111111111111","session_id":"33333333-3333-4333-8333-333333333333","session_generation":2,"session_public_binding_sha256":"`+strings.Repeat("c", 64)+`"}`), []int{sessionFD})
		syscall.Close(sessionFD)
		ackPayload, _, _, _ := receiveSeqpacket(t, connFD, 4096)
		assertExactJSON(t, ackPayload, `{"operation":"ack","response_id":"44444444-4444-4444-8444-444444444444","schema":"`+localSchema+`"}`)
	})
	defer server.Close()

	previousSendmsg := unixSendmsgN
	defer func() { unixSendmsgN = previousSendmsg }()
	calls := 0
	unixSendmsgN = func(fd int, payload []byte, oob []byte, to syscall.Sockaddr, flags int) (int, error) {
		calls++
		if calls == 1 {
			return 0, syscall.EINTR
		}
		return previousSendmsg(fd, payload, oob, to, flags)
	}

	client := NewGuardClient(socketPath, 4096, 2*time.Second)
	bootstrapFD := createMemfdFixture(t, "bootstrap", []byte(`{"schema_version":1,"grant_id":"11111111-1111-4111-8111-111111111111","bootstrap_token":"sentinel-secret-text"}`), requiredMemfdSeals, true)
	bootstrap, err := NewSecretBuffer(bootstrapFD, 64*1024)
	if err != nil {
		t.Fatalf("NewSecretBuffer() error = %v", err)
	}
	defer bootstrap.Close()
	session, err := client.Exchange(context.Background(), "11111111-1111-4111-8111-111111111111", "22222222-2222-4222-8222-222222222222", strings.Repeat("a", 64), bootstrap)
	if err != nil {
		t.Fatalf("Exchange() error = %v", err)
	}
	session.Secret.Close()
	if calls < 2 {
		t.Fatalf("sendmsg calls = %d, want retry after EINTR", calls)
	}
}

func TestProtocolRejectsTruncatedOrInvalidResponses(t *testing.T) {
	t.Parallel()
	useTestProtocolPolicy(t)

	tests := []struct {
		name    string
		payload string
		rights  []int
	}{
		{
			name:    "truncated",
			payload: `{"schema":"` + localSchema + `","operation":"projected","response_id":"22222222-2222-4222-8222-222222222222","grant_id":"11111111-1111-4111-8111-111111111111","proof_sha256":"` + strings.Repeat("a", 5000) + `"}`,
		},
		{
			name:    "invalid schema",
			payload: `{"schema":"invalid","operation":"projected","response_id":"22222222-2222-4222-8222-222222222222","grant_id":"11111111-1111-4111-8111-111111111111","proof_sha256":"` + strings.Repeat("a", 64) + `","receipt_public_binding_sha256":"` + strings.Repeat("b", 64) + `","rights":[]}`,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			socketPath := testSocketPath(t)
			server := startSeqpacketServer(t, socketPath, func(connFD int) {
				_, _, _, _ = receiveSeqpacket(t, connFD, 4096)
				sendSeqpacket(t, connFD, []byte(tt.payload), tt.rights)
			})
			defer server.Close()

			client := NewGuardClient(socketPath, 4096, 2*time.Second)
			if _, err := client.Project(context.Background(), "11111111-1111-4111-8111-111111111111"); err == nil {
				t.Fatal("Project() succeeded, want error")
			}
		})
	}
}

func TestProtocolRejectsNonRootPeerCredentials(t *testing.T) {
	useTestProtocolPolicy(t)
	requiredGuardUID = uint32(os.Geteuid() + 1)

	socketPath := testSocketPath(t)
	server := startSeqpacketServer(t, socketPath, func(connFD int) {
		_, _, _, _ = receiveSeqpacket(t, connFD, 4096)
		sendSeqpacket(t, connFD, []byte(`{"schema":"`+localSchema+`","operation":"finishing","response_id":"22222222-2222-4222-8222-222222222222","grant_id":"11111111-1111-4111-8111-111111111111"}`), nil)
	})
	defer server.Close()

	client := NewGuardClient(socketPath, 4096, 2*time.Second)
	err := client.Finish(context.Background(), "11111111-1111-4111-8111-111111111111", "33333333-3333-4333-8333-333333333333", map[string]int{"tmp": 1})
	if err == nil {
		t.Fatal("Finish() succeeded, want peer credential rejection")
	}
}

func TestProtocolRejectsTrailingJSONGarbage(t *testing.T) {
	useTestProtocolPolicy(t)
	socketPath := testSocketPath(t)
	server := startSeqpacketServer(t, socketPath, func(connFD int) {
		_, _, _, _ = receiveSeqpacket(t, connFD, 4096)
		sendSeqpacket(t, connFD, []byte(`{"schema":"`+localSchema+`","operation":"finishing","response_id":"22222222-2222-4222-8222-222222222222","grant_id":"11111111-1111-4111-8111-111111111111"}{}`), nil)
	})
	defer server.Close()

	client := NewGuardClient(socketPath, 4096, 2*time.Second)
	err := client.Finish(context.Background(), "11111111-1111-4111-8111-111111111111", "33333333-3333-4333-8333-333333333333", map[string]int{"tmp": 1})
	if err == nil {
		t.Fatal("Finish() succeeded, want trailing JSON rejection")
	}
}

func TestProtocolLeaseResponsesBindRequestIdentity(t *testing.T) {
	useTestProtocolPolicy(t)
	socketPath := testSocketPath(t)
	server := startSeqpacketServer(t, socketPath, func(connFD int) {
		_, rights, _, _ := receiveSeqpacket(t, connFD, 4096)
		closeRights(rights)
		sendSeqpacket(t, connFD, []byte(`{"schema":"`+localSchema+`","operation":"start","response_id":"22222222-2222-4222-8222-222222222222","grant_id":"11111111-1111-4111-8111-111111111111","operation_id":"33333333-3333-4333-8333-333333333334","materialization_id":"44444444-4444-4444-8444-444444444444","attempt_id":"55555555-5555-4555-8555-555555555555","lease_epoch":7,"state":"active","deterministic_failure_count":0,"lease_expires_at":"2026-09-03T00:10:00Z"}`), nil)
	})
	defer server.Close()

	client := NewGuardClient(socketPath, 4096, 2*time.Second)
	current := mustSessionEnvelope(t, 1, "sentinel-current")
	defer current.Secret.Close()
	if _, err := client.Start(context.Background(), "11111111-1111-4111-8111-111111111111", "33333333-3333-4333-8333-333333333333", "44444444-4444-4444-8444-444444444444", "55555555-5555-4555-8555-555555555555", 7, current.Secret); err == nil {
		t.Fatal("Start() succeeded, want request-binding error")
	}
}

func TestProtocolSecretResponsesBindRequestIdentity(t *testing.T) {
	useTestProtocolPolicy(t)
	socketPath := testSocketPath(t)
	server := startSeqpacketServer(t, socketPath, func(connFD int) {
		_, rights, _, _ := receiveSeqpacket(t, connFD, 4096)
		closeRights(rights)
		secretFD := createMemfdFixture(t, "bundle", []byte(`{"bundle_token":"sentinel-secret-text"}`), requiredMemfdSeals, true)
		sendSeqpacket(t, connFD, []byte(`{"schema":"`+localSchema+`","operation":"bundle","response_id":"22222222-2222-4222-8222-222222222222","grant_id":"11111111-1111-4111-8111-111111111111","operation_id":"33333333-3333-4333-8333-333333333333","materialization_id":"44444444-4444-4444-8444-444444444444","attempt_id":"55555555-5555-4555-8555-555555555556","lease_epoch":7,"available":true}`), []int{secretFD})
		syscall.Close(secretFD)
	})
	defer server.Close()

	client := NewGuardClient(socketPath, 4096, 2*time.Second)
	current := mustSessionEnvelope(t, 1, "sentinel-current")
	defer current.Secret.Close()
	if _, err := client.Bundle(context.Background(), "11111111-1111-4111-8111-111111111111", "33333333-3333-4333-8333-333333333333", "44444444-4444-4444-8444-444444444444", "55555555-5555-4555-8555-555555555555", 7, current.Secret); err == nil {
		t.Fatal("Bundle() succeeded, want request-binding error")
	}
}

func TestProtocolClaimAndBundleAcceptRealGuardSecretResponses(t *testing.T) {
	useTestProtocolPolicy(t)

	tests := []struct {
		name      string
		call      func(*testing.T, *GuardClient, *SecretBuffer) error
		response  string
		payload   []byte
		wantAckID string
	}{
		{
			name: "claim available",
			call: func(t *testing.T, client *GuardClient, current *SecretBuffer) error {
				secret, available, err := client.Claim(context.Background(), "11111111-1111-4111-8111-111111111111", "22222222-2222-4222-8222-222222222222", current)
				if err != nil {
					return err
				}
				defer secret.Close()
				if !available {
					t.Fatal("available = false, want true")
				}
				return nil
			},
			response:  `{"schema":"` + localSchema + `","operation":"claim","response_id":"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa","grant_id":"11111111-1111-4111-8111-111111111111","operation_id":"22222222-2222-4222-8222-222222222222","payload_sha256":"%s"}`,
			payload:   []byte(`{"claim_token":"sentinel-secret-text"}`),
			wantAckID: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
		},
		{
			name: "claim unavailable",
			call: func(t *testing.T, client *GuardClient, current *SecretBuffer) error {
				secret, available, err := client.Claim(context.Background(), "11111111-1111-4111-8111-111111111111", "22222222-2222-4222-8222-222222222222", current)
				if err != nil {
					return err
				}
				if secret != nil || available {
					t.Fatalf("got secret=%v available=%v, want nil/false", secret, available)
				}
				return nil
			},
			response:  `{"schema":"` + localSchema + `","operation":"claim","response_id":"bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb","grant_id":"11111111-1111-4111-8111-111111111111","operation_id":"22222222-2222-4222-8222-222222222222","available":false}`,
			wantAckID: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
		},
		{
			name: "bundle",
			call: func(t *testing.T, client *GuardClient, current *SecretBuffer) error {
				secret, err := client.Bundle(context.Background(), "11111111-1111-4111-8111-111111111111", "22222222-2222-4222-8222-222222222222", "33333333-3333-4333-8333-333333333333", "44444444-4444-4444-8444-444444444444", 9, current)
				if err != nil {
					return err
				}
				defer secret.Close()
				return nil
			},
			response:  `{"schema":"` + localSchema + `","operation":"bundle","response_id":"cccccccc-cccc-4ccc-8ccc-cccccccccccc","grant_id":"11111111-1111-4111-8111-111111111111","operation_id":"22222222-2222-4222-8222-222222222222","materialization_id":"33333333-3333-4333-8333-333333333333","attempt_id":"44444444-4444-4444-8444-444444444444","lease_epoch":9,"payload_sha256":"%s"}`,
			payload:   []byte(`{"bundle_token":"sentinel-secret-text"}`),
			wantAckID: "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			socketPath := testSocketPath(t)
			server := startSeqpacketServer(t, socketPath, func(connFD int) {
				_, rights, _, _ := receiveSeqpacket(t, connFD, 4096)
				closeRights(rights)
				var rightsToSend []int
				response := tt.response
				if tt.payload != nil {
					fd := createMemfdFixture(t, tt.name, tt.payload, requiredMemfdSeals, true)
					rightsToSend = []int{fd}
					sum := sha256.Sum256(tt.payload)
					response = fmt.Sprintf(response, fmt.Sprintf("%x", sum[:]))
					sendSeqpacket(t, connFD, []byte(response), rightsToSend)
					syscall.Close(fd)
				} else {
					sendSeqpacket(t, connFD, []byte(response), nil)
				}
				ackPayload, _, _, _ := receiveSeqpacket(t, connFD, 4096)
				assertExactJSON(t, ackPayload, `{"operation":"ack","response_id":"`+tt.wantAckID+`","schema":"`+localSchema+`"}`)
			})
			defer server.Close()

			client := NewGuardClient(socketPath, 4096, 2*time.Second)
			current := mustSessionEnvelope(t, 1, "sentinel-current")
			defer current.Secret.Close()
			if err := tt.call(t, client, current.Secret); err != nil {
				t.Fatalf("call() error = %v", err)
			}
		})
	}
}

// Break caught: registry credential requests drift from the local guard's fixed
// component/predecessor packet or skip the one sealed response descriptor ACK.
func TestGuardClientRegistryCredentialSendsCanonicalPacketAndReturnsSecret(t *testing.T) {
	useTestProtocolPolicy(t)
	socketPath := testSocketPath(t)
	secretPayload := []byte(`{"schema_version":"loom.task-image-registry-credential.v1","bearer_token":"header.payload.signature"}`)
	sum := sha256.Sum256(secretPayload)
	server := startSeqpacketServer(t, socketPath, func(connFD int) {
		payload, rights, creds, flags := receiveSeqpacket(t, connFD, 4096)
		if flags&^syscall.MSG_CMSG_CLOEXEC != 0 {
			t.Fatalf("recv flags = %d, want only MSG_CMSG_CLOEXEC", flags)
		}
		if creds == nil || creds.Pid <= 0 {
			t.Fatalf("credentials = %#v, want populated SCM_CREDENTIALS", creds)
		}
		if len(rights) != 1 {
			t.Fatalf("received %d rights, want one current-session memfd", len(rights))
		}
		current, err := NewSecretBuffer(rights[0], maxSecretBytes)
		if err != nil {
			t.Fatalf("current session descriptor invalid: %v", err)
		}
		if !strings.Contains(string(current.data), "sentinel-current") {
			t.Fatalf("current session descriptor = %q, want cloned current session", string(current.data))
		}
		current.Close()
		assertExactJSON(t, payload, `{"attempt_id":"55555555-5555-4555-8555-555555555555","component":"task","grant_id":"11111111-1111-4111-8111-111111111111","lease_epoch":7,"materialization_id":"44444444-4444-4444-8444-444444444444","operation":"registry-credential","operation_id":"22222222-2222-4222-8222-222222222222","predecessor_credential_id":null,"predecessor_generation":null,"schema":"`+localSchema+`"}`)

		secretFD := createMemfdFixture(t, "registry-credential", secretPayload, requiredMemfdSeals, true)
		sendSeqpacket(t, connFD, []byte(`{"schema":"`+localSchema+`","operation":"registry-credential","response_id":"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa","grant_id":"11111111-1111-4111-8111-111111111111","operation_id":"22222222-2222-4222-8222-222222222222","materialization_id":"44444444-4444-4444-8444-444444444444","attempt_id":"55555555-5555-4555-8555-555555555555","lease_epoch":7,"component":"task","payload_sha256":"`+fmt.Sprintf("%x", sum[:])+`"}`), []int{secretFD})
		syscall.Close(secretFD)
		ackPayload, ackRights, _, _ := receiveSeqpacket(t, connFD, 4096)
		if len(ackRights) != 0 {
			t.Fatalf("ack rights = %d, want none", len(ackRights))
		}
		assertExactJSON(t, ackPayload, `{"operation":"ack","response_id":"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa","schema":"`+localSchema+`"}`)
	})
	defer server.Close()

	client := NewGuardClient(socketPath, 4096, 2*time.Second)
	current := mustSessionEnvelope(t, 1, "sentinel-current")
	defer current.Secret.Close()
	secret, err := client.RegistryCredential(context.Background(), RegistryCredentialRequest{
		GrantID:           "11111111-1111-4111-8111-111111111111",
		OperationID:       "22222222-2222-4222-8222-222222222222",
		MaterializationID: "44444444-4444-4444-8444-444444444444",
		AttemptID:         "55555555-5555-4555-8555-555555555555",
		LeaseEpoch:        7,
		Component:         "task",
	}, current.Secret)
	if err != nil {
		t.Fatalf("RegistryCredential() error = %v", err)
	}
	defer secret.Close()
	if string(secret.data) != string(secretPayload) {
		t.Fatalf("secret payload = %q, want %q", string(secret.data), string(secretPayload))
	}
}

// Break caught: publication candidate acknowledgements accept rights or omit
// one of the authority-confirmed bindings before ACKing the guard.
func TestGuardClientPublicationCandidateValidatesBindingAndRejectsRights(t *testing.T) {
	useTestProtocolPolicy(t)
	tests := []struct {
		name      string
		response  string
		rights    func(*testing.T) []int
		wantError bool
	}{
		{
			name:     "valid acknowledgement",
			response: `{"schema":"` + localSchema + `","operation":"publication-candidate","response_id":"bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb","grant_id":"11111111-1111-4111-8111-111111111111","candidate_id":"cccccccc-cccc-4ccc-8ccc-cccccccccccc","operation_id":"22222222-2222-4222-8222-222222222222","credential_id":"66666666-6666-4666-8666-666666666666","credential_generation":3,"session_id":"33333333-3333-4333-8333-333333333333","session_generation":2,"materialization_id":"44444444-4444-4444-8444-444444444444","attempt_id":"55555555-5555-4555-8555-555555555555","attempt_number":11,"lease_epoch":7,"builder_id":"rootless:33333333333343338333333333333333","component":"task","manifest_digest":"sha256:` + strings.Repeat("a", 64) + `","manifest_size":1234,"oci_file_sha256":"` + strings.Repeat("b", 64) + `","oci_file_size":5678,"platform":"linux/arm64","recorded_at":"2026-09-03T12:00:01Z","authority_response_sha256":"` + strings.Repeat("c", 64) + `"}`,
		},
		{
			name:     "extra rights rejected",
			response: `{"schema":"` + localSchema + `","operation":"publication-candidate","response_id":"bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb","grant_id":"11111111-1111-4111-8111-111111111111","candidate_id":"cccccccc-cccc-4ccc-8ccc-cccccccccccc","operation_id":"22222222-2222-4222-8222-222222222222","credential_id":"66666666-6666-4666-8666-666666666666","credential_generation":3,"session_id":"33333333-3333-4333-8333-333333333333","session_generation":2,"materialization_id":"44444444-4444-4444-8444-444444444444","attempt_id":"55555555-5555-4555-8555-555555555555","attempt_number":11,"lease_epoch":7,"builder_id":"rootless:33333333333343338333333333333333","component":"task","manifest_digest":"sha256:` + strings.Repeat("a", 64) + `","manifest_size":1234,"oci_file_sha256":"` + strings.Repeat("b", 64) + `","oci_file_size":5678,"platform":"linux/arm64","recorded_at":"2026-09-03T12:00:01Z","authority_response_sha256":"` + strings.Repeat("c", 64) + `"}`,
			rights: func(t *testing.T) []int {
				return []int{createMemfdFixture(t, "unexpected-candidate-right", []byte(`{"right":"unexpected"}`), requiredMemfdSeals, true)}
			},
			wantError: true,
		},
		{
			name:      "binding drift rejected",
			response:  `{"schema":"` + localSchema + `","operation":"publication-candidate","response_id":"bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb","grant_id":"11111111-1111-4111-8111-111111111111","candidate_id":"cccccccc-cccc-4ccc-8ccc-cccccccccccc","operation_id":"22222222-2222-4222-8222-222222222222","credential_id":"66666666-6666-4666-8666-666666666666","credential_generation":3,"session_id":"33333333-3333-4333-8333-333333333333","session_generation":2,"materialization_id":"44444444-4444-4444-8444-444444444444","attempt_id":"55555555-5555-4555-8555-555555555555","attempt_number":11,"lease_epoch":7,"builder_id":"rootless:33333333333343338333333333333333","component":"sidecar:cache","manifest_digest":"sha256:` + strings.Repeat("a", 64) + `","manifest_size":1234,"oci_file_sha256":"` + strings.Repeat("b", 64) + `","oci_file_size":5678,"platform":"linux/arm64","recorded_at":"2026-09-03T12:00:01Z","authority_response_sha256":"` + strings.Repeat("c", 64) + `"}`,
			wantError: true,
		},
		{
			name:      "unknown response field rejected",
			response:  `{"schema":"` + localSchema + `","operation":"publication-candidate","response_id":"bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb","grant_id":"11111111-1111-4111-8111-111111111111","candidate_id":"cccccccc-cccc-4ccc-8ccc-cccccccccccc","operation_id":"22222222-2222-4222-8222-222222222222","credential_id":"66666666-6666-4666-8666-666666666666","credential_generation":3,"session_id":"33333333-3333-4333-8333-333333333333","session_generation":2,"materialization_id":"44444444-4444-4444-8444-444444444444","attempt_id":"55555555-5555-4555-8555-555555555555","attempt_number":11,"lease_epoch":7,"builder_id":"rootless:33333333333343338333333333333333","component":"task","manifest_digest":"sha256:` + strings.Repeat("a", 64) + `","manifest_size":1234,"oci_file_sha256":"` + strings.Repeat("b", 64) + `","oci_file_size":5678,"platform":"linux/arm64","recorded_at":"2026-09-03T12:00:01Z","authority_response_sha256":"` + strings.Repeat("c", 64) + `","unexpected":true}`,
			wantError: true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			socketPath := testSocketPath(t)
			server := startSeqpacketServer(t, socketPath, func(connFD int) {
				payload, rights, _, _ := receiveSeqpacket(t, connFD, 4096)
				if len(rights) != 1 {
					t.Fatalf("received %d rights, want one current-session memfd", len(rights))
				}
				closeRights(rights)
				assertExactJSON(t, payload, `{"attempt_id":"55555555-5555-4555-8555-555555555555","component":"task","credential_generation":3,"credential_id":"66666666-6666-4666-8666-666666666666","grant_id":"11111111-1111-4111-8111-111111111111","lease_epoch":7,"manifest_digest":"sha256:`+strings.Repeat("a", 64)+`","manifest_size":1234,"materialization_id":"44444444-4444-4444-8444-444444444444","oci_file_sha256":"`+strings.Repeat("b", 64)+`","oci_file_size":5678,"operation":"publication-candidate","operation_id":"22222222-2222-4222-8222-222222222222","platform":"linux/arm64","schema":"`+localSchema+`"}`)
				rightsToSend := []int(nil)
				if tt.rights != nil {
					rightsToSend = tt.rights(t)
					defer closeRights(rightsToSend)
				}
				sendSeqpacket(t, connFD, []byte(tt.response), rightsToSend)
				if !tt.wantError {
					ackPayload, _, _, _ := receiveSeqpacket(t, connFD, 4096)
					assertExactJSON(t, ackPayload, `{"operation":"ack","response_id":"bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb","schema":"`+localSchema+`"}`)
				}
			})

			client := NewGuardClient(socketPath, 4096, 2*time.Second)
			current := mustSessionEnvelope(t, 1, "sentinel-current")
			defer current.Secret.Close()
			ack, err := client.PublicationCandidate(context.Background(), PublicationCandidateRequest{
				GrantID:                 "11111111-1111-4111-8111-111111111111",
				OperationID:             "22222222-2222-4222-8222-222222222222",
				CredentialID:            "66666666-6666-4666-8666-666666666666",
				CredentialGeneration:    3,
				SessionID:               "33333333-3333-4333-8333-333333333333",
				SessionGeneration:       2,
				MaterializationID:       "44444444-4444-4444-8444-444444444444",
				AttemptID:               "55555555-5555-4555-8555-555555555555",
				AttemptNumber:           11,
				LeaseEpoch:              7,
				BuilderID:               "rootless:33333333333343338333333333333333",
				Component:               "task",
				ManifestDigest:          "sha256:" + strings.Repeat("a", 64),
				ManifestSize:            1234,
				OCIFileSHA256:           strings.Repeat("b", 64),
				OCIFileSize:             5678,
				Platform:                "linux/arm64",
				AuthorityResponseSHA256: strings.Repeat("c", 64),
			}, current.Secret)
			server.Close()
			if tt.wantError {
				if err == nil {
					t.Fatal("PublicationCandidate() succeeded, want error")
				}
				return
			}
			if err != nil {
				t.Fatalf("PublicationCandidate() error = %v", err)
			}
			if ack == nil || ack.CandidateID != "cccccccc-cccc-4ccc-8ccc-cccccccccccc" || ack.AttemptNumber != 11 || ack.AuthorityResponseSHA256 != strings.Repeat("c", 64) {
				t.Fatalf("ack = %#v, want strict candidate binding", ack)
			}
		})
	}
}

func TestProtocolSecretResponsesRejectPayloadDigestMismatch(t *testing.T) {
	useTestProtocolPolicy(t)
	socketPath := testSocketPath(t)
	server := startSeqpacketServer(t, socketPath, func(connFD int) {
		_, rights, _, _ := receiveSeqpacket(t, connFD, 4096)
		closeRights(rights)
		fd := createMemfdFixture(t, "bundle", []byte(`{"bundle_token":"sentinel-secret-text"}`), requiredMemfdSeals, true)
		sendSeqpacket(t, connFD, []byte(`{"schema":"`+localSchema+`","operation":"bundle","response_id":"cccccccc-cccc-4ccc-8ccc-cccccccccccc","grant_id":"11111111-1111-4111-8111-111111111111","operation_id":"22222222-2222-4222-8222-222222222222","materialization_id":"33333333-3333-4333-8333-333333333333","attempt_id":"44444444-4444-4444-8444-444444444444","lease_epoch":9,"payload_sha256":"`+strings.Repeat("d", 64)+`"}`), []int{fd})
		syscall.Close(fd)
	})
	defer server.Close()

	client := NewGuardClient(socketPath, 4096, 2*time.Second)
	current := mustSessionEnvelope(t, 1, "sentinel-current")
	defer current.Secret.Close()
	if _, err := client.Bundle(context.Background(), "11111111-1111-4111-8111-111111111111", "22222222-2222-4222-8222-222222222222", "33333333-3333-4333-8333-333333333333", "44444444-4444-4444-8444-444444444444", 9, current.Secret); err == nil {
		t.Fatal("Bundle() succeeded, want payload digest error")
	}
}

func TestProtocolFinishAcceptsAndBindsRealGuardResponse(t *testing.T) {
	useTestProtocolPolicy(t)
	socketPath := testSocketPath(t)
	server := startSeqpacketServer(t, socketPath, func(connFD int) {
		_, _, _, _ = receiveSeqpacket(t, connFD, 4096)
		sendSeqpacket(t, connFD, []byte(`{"schema":"`+localSchema+`","operation":"finishing","response_id":"dddddddd-dddd-4ddd-8ddd-dddddddddddd","grant_id":"11111111-1111-4111-8111-111111111111","operation_id":"22222222-2222-4222-8222-222222222222"}`), nil)
		ackPayload, _, _, _ := receiveSeqpacket(t, connFD, 4096)
		assertExactJSON(t, ackPayload, `{"operation":"ack","response_id":"dddddddd-dddd-4ddd-8ddd-dddddddddddd","schema":"`+localSchema+`"}`)
	})
	defer server.Close()

	client := NewGuardClient(socketPath, 4096, 2*time.Second)
	if err := client.Finish(context.Background(), "11111111-1111-4111-8111-111111111111", "22222222-2222-4222-8222-222222222222", map[string]int{"tmp": 1}); err != nil {
		t.Fatalf("Finish() error = %v", err)
	}
}

func TestProtocolClosesSocketOnInvalidResponses(t *testing.T) {
	useTestProtocolPolicy(t)
	tests := []struct {
		name    string
		run     func(*testing.T, *GuardClient) error
		respond func(*testing.T, int)
	}{
		{
			name: "project invalid",
			run: func(t *testing.T, client *GuardClient) error {
				_, err := client.Project(context.Background(), "11111111-1111-4111-8111-111111111111")
				return err
			},
			respond: func(t *testing.T, connFD int) {
				_, _, _, _ = receiveSeqpacket(t, connFD, 4096)
				sendSeqpacket(t, connFD, []byte(`{"schema":"bad","operation":"projected","response_id":"22222222-2222-4222-8222-222222222222","grant_id":"11111111-1111-4111-8111-111111111111","rights":[]}`), nil)
			},
		},
		{
			name: "exchange invalid session",
			run: func(t *testing.T, client *GuardClient) error {
				bootstrapFD := createMemfdFixture(t, "bootstrap", []byte(`{"schema_version":1,"grant_id":"11111111-1111-4111-8111-111111111111","bootstrap_token":"sentinel-secret-text"}`), requiredMemfdSeals, true)
				bootstrap, err := NewSecretBuffer(bootstrapFD, 64*1024)
				if err != nil {
					t.Fatalf("NewSecretBuffer() error = %v", err)
				}
				defer bootstrap.Close()
				_, err = client.Exchange(context.Background(), "11111111-1111-4111-8111-111111111111", "22222222-2222-4222-8222-222222222222", strings.Repeat("a", 64), bootstrap)
				return err
			},
			respond: func(t *testing.T, connFD int) {
				_, rights, _, _ := receiveSeqpacket(t, connFD, 4096)
				closeRights(rights)
				sendSeqpacket(t, connFD, []byte(`{"schema":"bad","operation":"session","response_id":"44444444-4444-4444-8444-444444444444","grant_id":"11111111-1111-4111-8111-111111111111","session_id":"33333333-3333-4333-8333-333333333333","session_generation":2,"session_public_binding_sha256":"`+strings.Repeat("c", 64)+`"}`), nil)
			},
		},
		{
			name: "claim invalid secret",
			run: func(t *testing.T, client *GuardClient) error {
				current := mustSessionEnvelope(t, 1, "sentinel-current")
				defer current.Secret.Close()
				_, _, err := client.Claim(context.Background(), "11111111-1111-4111-8111-111111111111", "22222222-2222-4222-8222-222222222222", current.Secret)
				return err
			},
			respond: func(t *testing.T, connFD int) {
				_, rights, _, _ := receiveSeqpacket(t, connFD, 4096)
				closeRights(rights)
				sendSeqpacket(t, connFD, []byte(`{"schema":"bad","operation":"claim","response_id":"44444444-4444-4444-8444-444444444444","grant_id":"11111111-1111-4111-8111-111111111111","operation_id":"22222222-2222-4222-8222-222222222222","available":false}`), nil)
			},
		},
		{
			name: "lease invalid",
			run: func(t *testing.T, client *GuardClient) error {
				current := mustSessionEnvelope(t, 1, "sentinel-current")
				defer current.Secret.Close()
				_, err := client.Start(context.Background(), "11111111-1111-4111-8111-111111111111", "22222222-2222-4222-8222-222222222222", "33333333-3333-4333-8333-333333333333", "44444444-4444-4444-8444-444444444444", 9, current.Secret)
				return err
			},
			respond: func(t *testing.T, connFD int) {
				_, rights, _, _ := receiveSeqpacket(t, connFD, 4096)
				closeRights(rights)
				sendSeqpacket(t, connFD, []byte(`{"schema":"bad","operation":"start","response_id":"55555555-5555-4555-8555-555555555555","grant_id":"11111111-1111-4111-8111-111111111111","operation_id":"22222222-2222-4222-8222-222222222222","materialization_id":"33333333-3333-4333-8333-333333333333","attempt_id":"44444444-4444-4444-8444-444444444444","lease_epoch":9,"state":"active","deterministic_failure_count":0,"lease_expires_at":"2026-09-03T00:10:00Z"}`), nil)
			},
		},
		{
			name: "finish invalid",
			run: func(t *testing.T, client *GuardClient) error {
				return client.Finish(context.Background(), "11111111-1111-4111-8111-111111111111", "22222222-2222-4222-8222-222222222222", map[string]int{"tmp": 1})
			},
			respond: func(t *testing.T, connFD int) {
				_, _, _, _ = receiveSeqpacket(t, connFD, 4096)
				sendSeqpacket(t, connFD, []byte(`{"schema":"bad","operation":"finishing","response_id":"55555555-5555-4555-8555-555555555555","grant_id":"11111111-1111-4111-8111-111111111111"}`), nil)
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			socketPath := testSocketPath(t)
			before := countOpenFileDescriptors(t)
			server := startSeqpacketServer(t, socketPath, func(connFD int) {
				tt.respond(t, connFD)
			})
			client := NewGuardClient(socketPath, 4096, 2*time.Second)
			if err := tt.run(t, client); err == nil {
				t.Fatal("operation succeeded, want error")
			}
			server.Close()
			after := countOpenFileDescriptors(t)
			if after != before {
				t.Fatalf("fd leak detected: before=%d after=%d", before, after)
			}
		})
	}
}

type seqpacketServer struct {
	listenerFD int
	done       chan struct{}
}

func (s *seqpacketServer) Close() {
	syscall.Close(s.listenerFD)
	<-s.done
}

func startSeqpacketServer(t *testing.T, socketPath string, handler func(int)) *seqpacketServer {
	t.Helper()

	listenerFD, err := syscall.Socket(syscall.AF_UNIX, syscall.SOCK_SEQPACKET|syscall.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatalf("Socket() error = %v", err)
	}
	addr := &syscall.SockaddrUnix{Name: socketPath}
	if err := syscall.Bind(listenerFD, addr); err != nil {
		t.Fatalf("Bind() error = %v", err)
	}
	if err := syscall.Listen(listenerFD, 4); err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		connFD, _, err := syscall.Accept4(listenerFD, syscall.SOCK_CLOEXEC)
		if err != nil {
			return
		}
		defer syscall.Close(connFD)
		if err := syscall.SetsockoptInt(connFD, syscall.SOL_SOCKET, syscall.SO_PASSCRED, 1); err != nil {
			t.Errorf("SetsockoptInt() error = %v", err)
			return
		}
		handler(connFD)
	}()
	return &seqpacketServer{listenerFD: listenerFD, done: done}
}

func receiveSeqpacket(t *testing.T, connFD int, maximum int) ([]byte, []int, *syscall.Ucred, int) {
	t.Helper()
	payload := make([]byte, maximum)
	oob := make([]byte, syscall.CmsgSpace(4*4)+syscall.CmsgSpace(syscall.SizeofUcred))
	n, oobn, flags, _, err := syscall.Recvmsg(connFD, payload, oob, syscall.MSG_CMSG_CLOEXEC)
	if err != nil {
		t.Fatalf("Recvmsg() error = %v", err)
	}
	messages, err := syscall.ParseSocketControlMessage(oob[:oobn])
	if err != nil {
		t.Fatalf("ParseSocketControlMessage() error = %v", err)
	}
	var rights []int
	var creds *syscall.Ucred
	for _, message := range messages {
		switch {
		case message.Header.Level == syscall.SOL_SOCKET && message.Header.Type == syscall.SCM_RIGHTS:
			parsed, err := syscall.ParseUnixRights(&message)
			if err != nil {
				t.Fatalf("ParseUnixRights() error = %v", err)
			}
			rights = append(rights, parsed...)
		case message.Header.Level == syscall.SOL_SOCKET && message.Header.Type == syscall.SCM_CREDENTIALS:
			parsed, err := syscall.ParseUnixCredentials(&message)
			if err != nil {
				t.Fatalf("ParseUnixCredentials() error = %v", err)
			}
			creds = parsed
		}
	}
	return payload[:n], rights, creds, flags
}

func tryReceiveSeqpacket(connFD int, maximum int, timeout time.Duration) bool {
	timeval := syscall.NsecToTimeval(timeout.Nanoseconds())
	if err := syscall.SetsockoptTimeval(connFD, syscall.SOL_SOCKET, syscall.SO_RCVTIMEO, &timeval); err != nil {
		return false
	}
	payload := make([]byte, maximum)
	oob := make([]byte, syscall.CmsgSpace(4*4)+syscall.CmsgSpace(syscall.SizeofUcred))
	n, _, _, _, err := syscall.Recvmsg(connFD, payload, oob, syscall.MSG_CMSG_CLOEXEC)
	return err == nil && n > 0
}

func sendSeqpacket(t *testing.T, connFD int, payload []byte, rights []int) {
	t.Helper()

	var oob []byte
	if len(rights) > 0 {
		oob = syscall.UnixRights(rights...)
	}
	written, err := syscall.SendmsgN(connFD, payload, oob, nil, 0)
	if err != nil {
		t.Fatalf("SendmsgN() error = %v", err)
	}
	if written != len(payload) {
		t.Fatalf("SendmsgN() = %d, want %d", written, len(payload))
	}
}

func assertExactJSON(t *testing.T, payload []byte, expected string) {
	t.Helper()

	var gotValue any
	if err := json.Unmarshal(payload, &gotValue); err != nil {
		t.Fatalf("json.Unmarshal(got) error = %v", err)
	}
	var expectedValue any
	if err := json.Unmarshal([]byte(expected), &expectedValue); err != nil {
		t.Fatalf("json.Unmarshal(expected) error = %v", err)
	}
	if fmt.Sprintf("%#v", gotValue) != fmt.Sprintf("%#v", expectedValue) {
		t.Fatalf("payload = %s, want %s", payload, expected)
	}
}

func openDirectoryFD(t *testing.T, path string) int {
	t.Helper()
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_CLOEXEC, 0)
	if err != nil {
		t.Fatalf("Open(%q) error = %v", path, err)
	}
	return fd
}

func mustFstat(t *testing.T, fd int) *syscall.Stat_t {
	t.Helper()
	var statValue syscall.Stat_t
	if err := syscall.Fstat(fd, &statValue); err != nil {
		t.Fatalf("Fstat() error = %v", err)
	}
	return &statValue
}

func testSocketPath(t *testing.T) string {
	t.Helper()
	return filepath.Join("/tmp", fmt.Sprintf("loom-supervisor-%d.sock", time.Now().UnixNano()))
}

func useTestProtocolPolicy(t *testing.T) {
	t.Helper()

	protocolPolicyMu.Lock()
	previousUID := requiredGuardUID
	requiredGuardUID = uint32(os.Geteuid())
	t.Cleanup(func() {
		requiredGuardUID = previousUID
		protocolPolicyMu.Unlock()
	})
}

var protocolPolicyMu sync.Mutex
