package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"syscall"
	"testing"
)

func TestSecretBufferReadsSealedMemfdAndRedactsFormatting(t *testing.T) {
	fd := createMemfdFixture(t, "secret-buffer", []byte(`{"session_token":"sentinel-secret-text"}`), requiredMemfdSeals, true)

	buffer, err := NewSecretBuffer(fd, 64*1024)
	if err != nil {
		t.Fatalf("NewSecretBuffer() error = %v", err)
	}
	if _, err := fcntlInt(fd, syscall.F_GETFD, 0); err == nil {
		t.Fatal("received memfd remained open")
	}

	if got := string(buffer.data); !strings.Contains(got, "sentinel-secret-text") {
		t.Fatalf("buffer contents = %q, want sentinel text", got)
	}
	if !coreDumpsDisabled() {
		t.Fatal("core dumps were not disabled")
	}
	if dumpable, err := dumpableState(); err == nil && dumpable {
		t.Fatal("process remained dumpable")
	}

	var logBuffer bytes.Buffer
	logger := log.New(&logBuffer, "", 0)
	logger.Print(buffer)
	formatted := strings.Join([]string{
		fmt.Sprintf("%v", buffer),
		fmt.Sprintf("%#v", buffer),
		fmt.Sprintf("%s", buffer),
		logBuffer.String(),
	}, "\n")
	if strings.Contains(formatted, "sentinel-secret-text") {
		t.Fatalf("formatted output leaked secret: %s", formatted)
	}

	wire, err := json.Marshal(struct {
		Secret *SecretBuffer `json:"secret"`
	}{Secret: buffer})
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	if strings.Contains(string(wire), "sentinel-secret-text") {
		t.Fatalf("JSON output leaked secret: %s", string(wire))
	}

	var recovered any
	func() {
		defer func() { recovered = recover() }()
		panic(buffer)
	}()
	if strings.Contains(fmt.Sprint(recovered), "sentinel-secret-text") {
		t.Fatalf("panic output leaked secret: %v", recovered)
	}

	buffer.Close()
}

func TestSecretBufferRejectsInvalidDescriptors(t *testing.T) {
	tempFile, err := os.CreateTemp(t.TempDir(), "secret-buffer")
	if err != nil {
		t.Fatalf("CreateTemp() error = %v", err)
	}
	defer tempFile.Close()

	unsealed := createMemfdFixture(t, "unsealed", []byte("secret"), 0, true)
	noCloexec := createMemfdFixture(t, "no-cloexec", []byte("secret"), requiredMemfdSeals, false)

	tests := []struct {
		name string
		fd   int
	}{
		{name: "linked file", fd: int(tempFile.Fd())},
		{name: "unsealed memfd", fd: unsealed},
		{name: "missing cloexec", fd: noCloexec},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			received := dupWithoutCloexec(t, tt.fd)
			if tt.name != "missing cloexec" {
				syscall.Close(received)
				received = dupFileDescriptor(t, tt.fd)
			}
			_, err := NewSecretBuffer(received, 4096)
			if err == nil {
				t.Fatal("NewSecretBuffer() succeeded, want error")
			}
			if _, err := fcntlInt(received, syscall.F_GETFD, 0); err == nil {
				t.Fatal("received descriptor remained open after error")
			}
		})
	}

	syscall.Close(unsealed)
	syscall.Close(noCloexec)
}

func dupWithoutCloexec(t interface {
	Helper()
	Fatalf(string, ...any)
}, fd int) int {
	t.Helper()
	duplicated, err := syscall.Dup(fd)
	if err != nil {
		t.Fatalf("Dup() error = %v", err)
	}
	return duplicated
}

func TestSecretBufferCloseZeroesAndUnlocksBytes(t *testing.T) {
	fd := createMemfdFixture(t, "close-zero", []byte("sentinel-secret-text"), requiredMemfdSeals, true)
	buffer, err := NewSecretBuffer(fd, 4096)
	if err != nil {
		t.Fatalf("NewSecretBuffer() error = %v", err)
	}

	data := buffer.data
	if len(data) == 0 {
		t.Fatal("buffer data is empty")
	}
	buffer.Close()
	for index, value := range data {
		if value != 0 {
			t.Fatalf("data[%d] = %d, want 0", index, value)
		}
	}
	if !buffer.closed {
		t.Fatal("buffer not marked closed")
	}
}
