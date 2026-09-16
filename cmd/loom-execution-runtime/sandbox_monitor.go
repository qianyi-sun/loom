package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"syscall"
	"time"
)

var errSandboxLost = errors.New("private sandbox lost")

// A busy sandbox may briefly miss a local health deadline. Only consecutive
// failures establish sustained unresponsiveness; a vanished Unix listener or
// a changed process identity is direct loss evidence and needs no grace.
const sandboxHealthFailureThreshold = 3

// Native sidecars have passed their startup probes before the controller starts.
// Pin each process incarnation and never reconnect a Trial to an empty restarted
// sandbox. Polling is local and bounded; the actuator retains native exit details.
func monitorPrivateSandboxes(parent context.Context, p plan) (context.Context, func()) {
	return monitorSandboxes(parent, p, "/loom/sandboxes", 2*time.Second, 2*time.Second)
}

func monitorSandboxes(parent context.Context, p plan, socketRoot string, interval, probeTimeout time.Duration) (context.Context, func()) {
	execution, fail := context.WithCancelCause(parent)
	watch, stop := context.WithCancel(parent)
	type watchedSandbox struct {
		role, instance string
		client         *http.Client
		failures       int
	}
	sandboxes := []watchedSandbox{}
	for _, item := range p.Sidecars {
		if !item.PrivateSandbox {
			continue
		}
		socket := filepath.Join(socketRoot, item.RoleName, "sandbox.sock")
		transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", socket)
		}}
		client := &http.Client{Transport: transport, Timeout: probeTimeout}
		instance, err := initialSandboxInstance(watch, client, interval)
		sandboxes = append(sandboxes, watchedSandbox{role: item.RoleName, instance: instance, client: client})
		if err != nil {
			fail(fmt.Errorf("%w: %s unavailable", errSandboxLost, item.RoleName))
			break
		}
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() {
			for _, sandbox := range sandboxes {
				sandbox.client.CloseIdleConnections()
			}
		}()
		if execution.Err() != nil || len(sandboxes) == 0 {
			return
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-watch.Done():
				return
			case <-ticker.C:
			}
			for i := range sandboxes {
				sandbox := &sandboxes[i]
				instance, err := sandboxInstance(watch, sandbox.client)
				if watch.Err() != nil {
					return
				}
				if err != nil {
					sandbox.failures++
					if confirmedSandboxConnectionLoss(err) || sandbox.failures >= sandboxHealthFailureThreshold {
						fail(fmt.Errorf("%w: %s unavailable", errSandboxLost, sandbox.role))
						return
					}
					continue
				}
				sandbox.failures = 0
				if instance != sandbox.instance {
					fail(fmt.Errorf("%w: %s restarted", errSandboxLost, sandbox.role))
					return
				}
			}
		}
	}()
	return execution, func() { stop(); <-done }
}

func sandboxInstance(ctx context.Context, client *http.Client) (string, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://sandbox/health", nil)
	if err != nil {
		return "", err
	}
	response, err := client.Do(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return "", errors.New("sandbox not ready")
	}
	var health struct {
		Ready      bool   `json:"ready"`
		InstanceID string `json:"instance_id"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 1024)).Decode(&health); err != nil {
		return "", err
	}
	decoded, err := hex.DecodeString(health.InstanceID)
	if !health.Ready || err != nil || len(decoded) != 16 {
		return "", errors.New("sandbox health invalid")
	}
	return health.InstanceID, nil
}

func confirmedSandboxConnectionLoss(err error) bool {
	if errors.Is(err, syscall.ENOENT) || errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) {
		return true
	}
	// EOF from the HTTP transport means the server connection vanished. An
	// incomplete JSON response alone is only a failed health probe.
	var transportError *url.Error
	return errors.As(err, &transportError) && errors.Is(transportError.Err, io.EOF)
}

func initialSandboxInstance(ctx context.Context, client *http.Client, interval time.Duration) (string, error) {
	for failures := 1; ; failures++ {
		instance, err := sandboxInstance(ctx, client)
		if err == nil || ctx.Err() != nil || confirmedSandboxConnectionLoss(err) || failures >= sandboxHealthFailureThreshold {
			return instance, err
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return "", ctx.Err()
		case <-timer.C:
		}
	}
}
