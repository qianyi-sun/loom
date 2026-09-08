package main

import (
	"syscall"
	"testing"
	"time"
)

// Reuse the socket number after ACK closes it, before the caller's deferred
// packet.Close. A stale owner must never close the new descriptor.
func TestGuardClientFixAckFailureDoesNotCloseReusedFD(t *testing.T) {
	for _, name := range []string{"send failure", "invalid response id", "success"} {
		t.Run(name, func(t *testing.T) {
			sockets, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_SEQPACKET, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer syscall.Close(sockets[1])
			packet := &responsePacket{fd: sockets[0], deadline: time.Now().Add(time.Second)}
			defer packet.Close()
			id := testGrantID
			if name == "send failure" {
				if err := syscall.Shutdown(sockets[1], syscall.SHUT_RDWR); err != nil {
					t.Fatal(err)
				}
			}
			if name == "invalid response id" {
				id = "invalid"
			}
			err = (&GuardClient{}).ackPacket(packet, id)
			if (err == nil) != (name == "success") {
				t.Fatalf("ACK error=%v", err)
			}
			var stat syscall.Stat_t
			if err := syscall.Fstat(sockets[0], &stat); err != syscall.EBADF {
				t.Fatalf("ACK did not close original fd: %v", err)
			}
			replacement, err := syscall.Open("/dev/null", syscall.O_RDONLY|syscall.O_CLOEXEC, 0)
			if err != nil {
				t.Fatal(err)
			}
			if replacement != sockets[0] {
				if err := syscall.Dup2(replacement, sockets[0]); err != nil {
					t.Fatal(err)
				}
				syscall.Close(replacement)
			}
			defer syscall.Close(sockets[0])
			packet.Close()
			if err := syscall.Fstat(sockets[0], &stat); err != nil {
				t.Fatalf("deferred packet close destroyed reused fd: %v", err)
			}
		})
	}
}
