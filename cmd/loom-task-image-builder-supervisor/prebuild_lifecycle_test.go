package main

import (
	"context"
	"errors"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"
)

type prebuildTestGuard struct {
	TaskImageGuard
	clock         *manualClock
	heartbeats    chan struct{}
	failHeartbeat bool
}

func (g *prebuildTestGuard) Heartbeat(_ context.Context, grant, op, materialization, attempt string, epoch int, _ *SecretBuffer) (*LeaseResponse, error) {
	if g.failHeartbeat {
		return nil, errors.New("heartbeat unavailable")
	}
	expires := g.clock.Now().Add(time.Minute)
	g.heartbeats <- struct{}{}
	return &LeaseResponse{Operation: "heartbeat", OperationID: op, GrantID: grant, MaterializationID: materialization,
		AttemptID: attempt, LeaseEpoch: epoch, State: "claimed", LeaseExpiresAt: &expires}, nil
}

func prebuildFixture(t *testing.T) (*prebuildLifecycle, *manualClock, chan struct{}, *prebuildTestGuard, int) {
	t.Helper()
	clock := newManualClock(testNow)
	current := mustSessionEnvelope(t, 1, "prebuild-original")
	current.IssuedAt, current.ExpiresAt = clock.Now(), clock.Now().Add(time.Minute)
	renewed := make(chan struct{}, 4)
	client := &stubSessionClient{renew: func(context.Context, string, string, *SecretBuffer) (*SessionEnvelope, error) {
		next := mustSessionEnvelope(t, 2, "prebuild-successor")
		next.IssuedAt, next.ExpiresAt = clock.Now(), clock.Now().Add(time.Minute)
		renewed <- struct{}{}
		return next, nil
	}}
	manager := NewSessionManager(current.GrantID, current, client)
	t.Cleanup(manager.Close)
	guard := &prebuildTestGuard{clock: clock, heartbeats: make(chan struct{}, 4)}
	fd := openDirectoryFD(t, t.TempDir())
	t.Cleanup(func() { syscall.Close(fd) })
	return &prebuildLifecycle{clock: clock, session: manager, guard: guard, grantID: current.GrantID,
		materializationID: testMaterializationID, attemptID: testAttemptID, leaseEpoch: 1,
		leaseExpiresAt: clock.Now().Add(time.Minute), timeout: 2 * time.Minute}, clock, renewed, guard, fd
}

func waitForPrebuildControlTimer(t *testing.T, clock *manualClock) {
	t.Helper()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		clock.mu.Lock()
		// The phase timer is registered before fetch starts. Wait for the
		// separate control timer too: fetch entry alone does not prove that
		// run has finished computing and registering its relative wait.
		armed := len(clock.timers) >= 2
		clock.mu.Unlock()
		if armed {
			return
		}
		select {
		case <-tick.C:
		case <-deadline.C:
			t.Fatal("prebuild control timer was not registered")
		}
	}
}

func TestPrebuildRenewsAndHeartbeatsDuringSnapshotIO(t *testing.T) {
	p, clock, renewed, guard, fd := prebuildFixture(t)
	entered, release := make(chan struct{}), make(chan struct{})
	p.fetch = func(ctx context.Context) (*DownloadedRegisteredBundle, error) {
		err := p.session.WithSnapshot(func(_ *SessionEnvelope, secret *SecretBuffer) error {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
			if !strings.Contains(string(secret.data), "prebuild-original") {
				return errors.New("snapshot destroyed during renewal")
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		bundle, err := createRegisteredBundleDirectory(fd)
		if err == nil {
			bundle.ExpiresAt = clock.Now().Add(time.Minute)
		}
		return bundle, err
	}
	done := make(chan error, 1)
	go func() {
		bundle, err := p.run(context.Background())
		if bundle != nil {
			err = errors.Join(err, bundle.Close())
		}
		done <- err
	}()
	<-entered
	waitForPrebuildControlTimer(t, clock)
	clock.advance(20 * time.Second)
	select {
	case <-renewed:
	case <-time.After(time.Second):
		close(release)
		<-done
		t.Fatal("renewal blocked by bundle I/O")
	}
	select {
	case <-guard.heartbeats:
	case <-time.After(time.Second):
		close(release)
		<-done
		t.Fatal("heartbeat blocked by bundle I/O")
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestPrebuildCancellationJoinsFetchAndCleansRacingSuccess(t *testing.T) {
	p, clock, _, _, fd := prebuildFixture(t)
	entered, cancelled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	p.fetch = func(ctx context.Context) (*DownloadedRegisteredBundle, error) {
		bundle, err := createRegisteredBundleDirectory(fd)
		if err != nil {
			return nil, err
		}
		bundle.ExpiresAt = clock.Now().Add(time.Minute)
		close(entered)
		<-ctx.Done()
		close(cancelled)
		<-release
		return bundle, nil // A completed fetch racing with cancellation.
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := p.run(ctx); done <- err }()
	<-entered
	cancel()
	<-cancelled
	select {
	case <-done:
		t.Fatal("returned before input owner joined")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-done; err == nil {
		t.Fatal("accepted cancelled prebuild")
	}
	path, err := pathFromDirectoryFD(fd)
	if err != nil {
		t.Fatal(err)
	}
	if entries, err := os.ReadDir(path); err != nil || len(entries) != 0 {
		t.Fatal("cancelled prebuild leaked input")
	}
}

func TestPrebuildRejectsHeartbeatLossAndClockRegression(t *testing.T) {
	for _, kind := range []string{"heartbeat", "clock", "timeout"} {
		t.Run(kind, func(t *testing.T) {
			p, clock, _, guard, _ := prebuildFixture(t)
			entered, joined := make(chan struct{}), make(chan struct{})
			p.fetch = func(ctx context.Context) (*DownloadedRegisteredBundle, error) {
				close(entered)
				<-ctx.Done()
				close(joined)
				return nil, ctx.Err()
			}
			guard.failHeartbeat = kind == "heartbeat"
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { _, err := p.run(ctx); done <- err }()
			<-entered
			waitForPrebuildControlTimer(t, clock)
			if kind == "clock" {
				clock.advance(-time.Second)
				clock.advance(0)
			} else if kind == "timeout" {
				clock.advance(3 * time.Minute)
			} else {
				clock.advance(20 * time.Second)
			}
			// Regression has no expired synthetic timer; wake the clock's next
			// wait explicitly to model a scheduler tick without advancing time.
			if kind == "clock" {
				clock.mu.Lock()
				for _, timer := range clock.timers {
					select {
					case timer.ch <- clock.now:
					default:
					}
				}
				clock.mu.Unlock()
			}
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("accepted lost prebuild authority")
				}
			case <-time.After(time.Second):
				t.Fatal("prebuild did not terminate")
			}
			select {
			case <-joined:
			default:
				t.Fatal("failed prebuild did not join fetch")
			}
		})
	}
}

func TestPrebuildPreservesCleanupAmbiguityWithoutTransportSecrets(t *testing.T) {
	p, _, _, _, _ := prebuildFixture(t)
	p.fetch = func(context.Context) (*DownloadedRegisteredBundle, error) {
		return nil, errors.Join(errCleanupAmbiguous, errors.New("secret transport target"))
	}
	if _, err := p.run(context.Background()); !errors.Is(err, errCleanupAmbiguous) || strings.Contains(err.Error(), "secret transport") {
		t.Fatal("prebuild erased cleanup ambiguity or exposed transport details")
	}
}
