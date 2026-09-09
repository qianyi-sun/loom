package main

import (
	"context"
	"errors"
	"time"
)

// prepareRegisteredBundle composes actual capability issuance and verified
// download with prebuild liveness. It does not start an executor or extend the
// returned capability's deadline. Native runClaim remains closed until build
// cancellation joins context consumers before executor/input cleanup.
func (s *orchestratorState) prepareRegisteredBundle() (*DownloadedRegisteredBundle, error) {
	invalid := errors.New("registered bundle preparation unavailable")
	if s.o == nil || s.o.Config.Bundle == nil || s.o.Guard == nil || s.sessionManager == nil || s.clock == nil ||
		s.caps == nil || s.caps.JobDirectoryFD < 0 || s.claimData == nil || s.claimData.RegisteredBundle == nil || s.claimData.LeaseExpiresAtPtr == nil {
		return nil, invalid
	}
	claim := *s.claimData
	plan := *claim.RegisteredBundle
	trust := *s.o.Config.Bundle
	if plan.GrantID != s.o.GrantID || plan.MaterializationID != claim.MaterializationID || trust.Roots == nil || plan.Bucket != trust.Bucket {
		return nil, invalid
	}
	trust.Roots = trust.Roots.Clone()
	p := &prebuildLifecycle{clock: s.clock, session: s.sessionManager, guard: s.o.Guard,
		grantID: plan.GrantID, materializationID: claim.MaterializationID, attemptID: claim.AttemptID,
		leaseEpoch: claim.LeaseEpoch, leaseExpiresAt: *claim.LeaseExpiresAtPtr, timeout: 10 * time.Minute}
	p.fetch = func(ctx context.Context) (*DownloadedRegisteredBundle, error) {
		// A renewal may supersede an issuance already in flight. Only that
		// bounded race gets a fresh operation/session retry, not arbitrary I/O.
		for attempt := 0; attempt < 3; attempt++ {
			if ctx.Err() != nil {
				return nil, invalid
			}
			operation, err := newUUID()
			if err != nil {
				return nil, invalid
			}
			var capability *SecretBuffer
			var binding BundleSessionBinding
			err = s.sessionManager.WithSnapshot(func(envelope *SessionEnvelope, secret *SecretBuffer) error {
				binding = BundleSessionBinding{SessionID: envelope.SessionID, Generation: envelope.Generation, ExpiresAt: envelope.ExpiresAt}
				if envelope.GrantID != plan.GrantID || !binding.ExpiresAt.After(s.clock.Now()) {
					return invalid
				}
				opCtx, stop := context.WithTimeout(ctx, 45*time.Second)
				defer stop()
				var issueErr error
				capability, issueErr = s.o.Guard.Bundle(opCtx, plan.GrantID, operation, claim.MaterializationID, claim.AttemptID, claim.LeaseEpoch, secret)
				return issueErr
			})
			if binding.Generation != 0 && binding.Generation != s.sessionManager.Generation() {
				if capability != nil {
					capability.Close()
				}
				continue
			}
			if err != nil || capability == nil {
				if capability != nil {
					capability.Close()
				}
				return nil, invalid
			}
			defer capability.Close()
			return DownloadRegisteredBundle(ctx, capability, s.caps.JobDirectoryFD, plan, binding, trust, s.clock.Now)
		}
		return nil, invalid
	}
	bundle, err := p.run(s.ctx)
	if err != nil {
		return nil, err
	}
	// The liveness owner has joined; publish its final lease/session observation
	// to the sequential orchestrator, without rewriting frozen claim provenance.
	expires := p.leaseExpiresAt
	s.claimData.LeaseExpiresAtPtr = &expires
	if err := s.sessionManager.WithCurrentEnvelope(func(envelope *SessionEnvelope, _ *SecretBuffer) error {
		s.session = envelope
		return nil
	}); err != nil {
		return nil, errors.Join(invalid, bundle.Close())
	}
	return bundle, nil
}
