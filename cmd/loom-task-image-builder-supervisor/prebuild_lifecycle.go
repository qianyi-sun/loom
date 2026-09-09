package main

import (
	"context"
	"errors"
	"time"
)

type prebuildHeartbeatGuard interface {
	Heartbeat(context.Context, string, string, string, string, int, *SecretBuffer) (*LeaseResponse, error)
}

// One control owner renews the current session and lease while a single fetch
// owns issuance/download. Fetch must honor cancellation and bounded I/O; every
// exit joins it before returning or disposing a racing successful input tree.
type prebuildLifecycle struct {
	clock                                 Clock
	session                               *SessionManager
	guard                                 prebuildHeartbeatGuard
	grantID, materializationID, attemptID string
	leaseEpoch                            int
	leaseExpiresAt                        time.Time
	timeout                               time.Duration
	fetch                                 func(context.Context) (*DownloadedRegisteredBundle, error)
}

func (p *prebuildLifecycle) run(parent context.Context) (_ *DownloadedRegisteredBundle, err error) {
	invalid := errors.New("registered prebuild unavailable")
	if p.clock == nil || p.session == nil || p.guard == nil || p.fetch == nil ||
		p.timeout <= 0 || p.timeout > 10*time.Minute || !isCanonicalNonZeroUUID(p.grantID) ||
		!isCanonicalNonZeroUUID(p.materializationID) || !isCanonicalNonZeroUUID(p.attemptID) || p.leaseEpoch <= 0 || p.session.grantID != p.grantID {
		return nil, invalid
	}
	now := p.clock.Now()
	expiry := p.session.ExpiresAt()
	if parent.Err() != nil || !expiry.After(now) || !p.leaseExpiresAt.After(now) {
		return nil, invalid
	}
	ctx, cancel := context.WithTimeout(parent, p.timeout)
	deadline, previous := now.Add(p.timeout), now
	phaseTimer := p.clock.NewTimer(p.timeout)
	defer phaseTimer.Stop()
	type result struct {
		bundle *DownloadedRegisteredBundle
		err    error
	}
	downloaded := make(chan result, 1)
	joined := make(chan struct{})
	go func() {
		defer close(joined)
		bundle, err := p.fetch(ctx)
		downloaded <- result{bundle, err}
	}()
	var received *DownloadedRegisteredBundle
	defer func() {
		cancel()
		<-joined
		if err != nil {
			if received == nil {
				select {
				case got := <-downloaded:
					received = got.bundle
				default:
				}
			}
			if received != nil {
				err = errors.Join(err, received.Close())
			}
		}
	}()
	renewDue, heartbeatDue := renewalAt(now, expiry), heartbeatAt(now, &p.leaseExpiresAt)
	for {
		now = p.clock.Now()
		if ctx.Err() != nil || now.Before(previous) || !now.Before(deadline) || !p.leaseExpiresAt.After(now) {
			return nil, invalid
		}
		previous = now
		currentExpiry := p.session.ExpiresAt()
		if !currentExpiry.Equal(expiry) {
			expiry = currentExpiry
			renewDue = renewalAt(now, expiry)
		}
		if !expiry.After(now) {
			return nil, invalid
		}
		if !now.Before(renewDue) {
			opCtx, stop := context.WithTimeout(ctx, publicationOperationTimeout)
			_, renewErr := p.session.Renew(opCtx)
			stop()
			if renewErr != nil {
				return nil, invalid
			}
			expiry = p.session.ExpiresAt()
			renewDue = renewalAt(p.clock.Now(), expiry)
			if !renewDue.After(p.clock.Now()) {
				return nil, invalid
			}
			continue
		}
		if !now.Before(heartbeatDue) {
			if p.heartbeat(ctx) != nil {
				return nil, invalid
			}
			heartbeatDue = heartbeatAt(p.clock.Now(), &p.leaseExpiresAt)
			continue
		}
		if received != nil {
			if !received.ExpiresAt.After(now) {
				return nil, invalid
			}
			return received, nil
		}
		next := renewDue
		if heartbeatDue.Before(next) {
			next = heartbeatDue
		}
		timer := p.clock.NewTimer(durationUntil(now, next))
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, invalid
		case <-phaseTimer.C():
			timer.Stop()
			return nil, invalid
		case <-timer.C():
		case got := <-downloaded:
			received = got.bundle
			if got.err != nil || received == nil {
				timer.Stop()
				if errors.Is(got.err, errCleanupAmbiguous) {
					return nil, errors.Join(invalid, errCleanupAmbiguous)
				}
				return nil, invalid
			}
		}
		timer.Stop()
	}
}

func (p *prebuildLifecycle) heartbeat(ctx context.Context) error {
	operation, err := newUUID()
	if err != nil {
		return err
	}
	opCtx, stop := context.WithTimeout(ctx, publicationOperationTimeout)
	defer stop()
	return p.session.WithCurrent(func(secret *SecretBuffer) error {
		lease, err := p.guard.Heartbeat(opCtx, p.grantID, operation, p.materializationID, p.attemptID, p.leaseEpoch, secret)
		if err != nil {
			return err
		}
		if lease == nil || lease.Operation != "heartbeat" || lease.OperationID != operation || lease.GrantID != p.grantID ||
			lease.MaterializationID != p.materializationID || lease.AttemptID != p.attemptID || lease.LeaseEpoch != p.leaseEpoch ||
			(lease.State != "claimed" && lease.State != "running") || lease.LeaseExpiresAt == nil || !lease.LeaseExpiresAt.After(p.clock.Now()) {
			return errors.New("prebuild heartbeat invalid")
		}
		p.leaseExpiresAt = *lease.LeaseExpiresAt
		return nil
	})
}
