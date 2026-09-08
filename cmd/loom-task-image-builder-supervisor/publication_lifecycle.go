package main

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

const publicationOperationTimeout = 5 * time.Second

type publicationLifecycleGuard interface {
	Heartbeat(context.Context, string, string, string, string, int, *SecretBuffer) (*LeaseResponse, error)
	PublicationSubmit(context.Context, publicationStatusBinding, *SecretBuffer) (*publicationStatus, error)
	PublicationPoll(context.Context, publicationStatusBinding, *SecretBuffer) (*publicationStatus, error)
}

// One owner schedules liveness and verification. Only upload runs concurrently;
// it is always cancelled and joined before the caller may destroy its resources.
type publicationLifecycle struct {
	clock          Clock
	session        *SessionManager
	guard          publicationLifecycleGuard
	set            BuiltComponentSet
	builderID      string
	leaseExpiresAt time.Time
	timeout        time.Duration
	upload         func(context.Context) ([]PublicationCandidateV2Acknowledgement, error)
}

func (p *publicationLifecycle) run(parent context.Context) (*publicationReceipt, error) {
	invalid := errors.New("publication unavailable")
	if p.clock == nil || p.session == nil || p.guard == nil || p.upload == nil || p.timeout <= 0 || p.timeout > 2*time.Hour || p.builderID == "" || validatePublicationBuiltSet(p.set) != nil {
		return nil, invalid
	}
	set := p.set
	set.Components = append([]BuiltComponent(nil), p.set.Components...)
	now := p.clock.Now()
	var expiry time.Time
	if err := p.session.WithCurrentEnvelope(func(envelope *SessionEnvelope, _ *SecretBuffer) error {
		if envelope.GrantID != set.GrantID || p.session.grantID != set.GrantID {
			return invalid
		}
		expiry = envelope.ExpiresAt
		return nil
	}); err != nil {
		return nil, invalid
	}
	if !expiry.After(now) || !p.leaseExpiresAt.After(now) || parent.Err() != nil {
		return nil, invalid
	}
	ctx, cancel := context.WithTimeout(parent, p.timeout)
	defer cancel()
	deadline := now.Add(p.timeout)
	phaseTimer := p.clock.NewTimer(p.timeout)
	defer phaseTimer.Stop()
	type uploadResult struct {
		acks []PublicationCandidateV2Acknowledgement
		err  error
	}
	uploaded := make(chan uploadResult, 1)
	joined := make(chan struct{})
	go func() {
		defer close(joined)
		acks, err := p.upload(ctx)
		uploaded <- uploadResult{acks, err}
	}()
	defer func() { cancel(); <-joined }()

	renewAt := renewalAt(now, expiry)
	heartbeatDue := heartbeatAt(now, &p.leaseExpiresAt)
	var binding publicationStatusBinding
	var pollAt time.Time
	submitted := false
	failures := 0
	for {
		now = p.clock.Now()
		if ctx.Err() != nil || !now.Before(deadline) {
			return nil, invalid
		}
		// Credential refresh can install a successor during upload. Schedule from
		// that actual expiry, never from the original cached claim envelope.
		currentExpiry := p.session.ExpiresAt()
		if !currentExpiry.Equal(expiry) {
			expiry = currentExpiry
			renewAt = renewalAt(now, expiry)
		}
		if !expiry.After(now) {
			return nil, invalid
		}
		if !now.Before(renewAt) {
			opCtx, stop := context.WithTimeout(ctx, publicationOperationTimeout)
			_, err := p.session.Renew(opCtx)
			stop()
			if err != nil {
				return nil, invalid
			}
			expiry = p.session.ExpiresAt()
			renewAt = renewalAt(p.clock.Now(), expiry)
			if !renewAt.After(p.clock.Now()) {
				return nil, invalid
			}
			continue
		}
		if !now.Before(heartbeatDue) {
			if !p.leaseExpiresAt.After(now) || p.heartbeat(ctx, set) != nil {
				// Completion clears the lease. A heartbeat failure is not proof of
				// completion: only an authenticated exact terminal poll can succeed.
				if submitted {
					status, err := p.status(ctx, binding, false)
					if err == nil && status.State == "completed" {
						return status.Receipt, nil
					}
				}
				return nil, invalid
			}
			heartbeatDue = heartbeatAt(p.clock.Now(), &p.leaseExpiresAt)
			continue
		}
		if !pollAt.IsZero() && !now.Before(pollAt) {
			// Set before sending: even a lost submit reply may have committed.
			submit := binding.PinnedSnapshotSHA256 == ""
			submitted = true
			status, err := p.status(ctx, binding, submit)
			if err != nil {
				failures++
				if failures >= 3 {
					return nil, invalid
				}
				pollAt = p.clock.Now().Add(time.Duration(failures) * time.Second)
				continue
			}
			failures = 0
			binding.PinnedSnapshotSHA256 = status.SnapshotSHA256
			switch status.State {
			case "completed":
				return status.Receipt, nil
			case "failed":
				return nil, invalid
			}
			pollAt = p.clock.Now().Add(time.Second)
			continue
		}
		wake := renewAt
		if heartbeatDue.Before(wake) {
			wake = heartbeatDue
		}
		if !pollAt.IsZero() && pollAt.Before(wake) {
			wake = pollAt
		}
		timer := p.clock.NewTimer(durationUntil(now, wake))
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, invalid
		case <-phaseTimer.C():
			timer.Stop()
			return nil, invalid
		case result := <-uploaded:
			timer.Stop()
			uploaded = nil
			if result.err != nil {
				return nil, invalid
			}
			hash, err := publicationUploadedSetHash(set, p.builderID, result.acks)
			if err != nil {
				return nil, invalid
			}
			operation, err := newUUID()
			if err != nil {
				return nil, invalid
			}
			binding = publicationStatusBinding{GrantID: set.GrantID, OperationID: operation, MaterializationID: set.MaterializationID, AttemptID: set.AttemptID, LeaseEpoch: int64(set.LeaseEpoch), CandidateSetSHA256: hash, ComponentCount: len(set.Components)}
			pollAt = p.clock.Now()
		case <-timer.C():
			timer.Stop()
		}
	}
}

func (p *publicationLifecycle) heartbeat(ctx context.Context, set BuiltComponentSet) error {
	operation, err := newUUID()
	if err != nil {
		return err
	}
	opCtx, stop := context.WithTimeout(ctx, publicationOperationTimeout)
	defer stop()
	return p.session.WithCurrent(func(current *SecretBuffer) error {
		lease, err := p.guard.Heartbeat(opCtx, set.GrantID, operation, set.MaterializationID, set.AttemptID, set.LeaseEpoch, current)
		if err != nil {
			return err
		}
		if lease == nil || lease.Operation != "heartbeat" || lease.OperationID != operation || lease.GrantID != set.GrantID || lease.MaterializationID != set.MaterializationID || lease.AttemptID != set.AttemptID || lease.LeaseEpoch != set.LeaseEpoch || lease.State != "running" || lease.LeaseExpiresAt == nil || !lease.LeaseExpiresAt.After(p.clock.Now()) {
			return errors.New("publication heartbeat invalid")
		}
		p.leaseExpiresAt = *lease.LeaseExpiresAt
		return nil
	})
}

func (p *publicationLifecycle) status(ctx context.Context, binding publicationStatusBinding, submit bool) (*publicationStatus, error) {
	opCtx, stop := context.WithTimeout(ctx, publicationOperationTimeout)
	defer stop()
	var status *publicationStatus
	err := p.session.WithCurrent(func(current *SecretBuffer) error {
		var err error
		if submit {
			status, err = p.guard.PublicationSubmit(opCtx, binding, current)
		} else {
			status, err = p.guard.PublicationPoll(opCtx, binding, current)
		}
		return err
	})
	if err != nil {
		return nil, errors.New("publication status unavailable")
	}
	return validatePublicationStatusValue(status, binding)
}

// Reuse the closed wire validators for typed adapters too. This creates owned
// values and cannot upgrade a malformed receipt into authority.
func validatePublicationStatusValue(status *publicationStatus, binding publicationStatusBinding) (*publicationStatus, error) {
	if status == nil {
		return nil, errors.New("publication status invalid")
	}
	schema := "loom.task-image-publication-status/v1"
	wire := publicationStatusWire{AttemptID: &status.AttemptID, CandidateSetSHA256: &status.CandidateSetSHA256, ComponentCount: &status.ComponentCount, GrantID: &status.GrantID, LeaseEpoch: &status.LeaseEpoch, MaterializationID: &status.MaterializationID, OperationID: &status.OperationID, Schema: &schema, SnapshotSHA256: &status.SnapshotSHA256, State: &status.State}
	if status.FailureCode != "" {
		wire.FailureCode = &status.FailureCode
	}
	if r := status.Receipt; r != nil {
		// Formatting must not silently normalize a fractional/non-UTC adapter time.
		if r.CompletedAt.Nanosecond() != 0 {
			return nil, errors.New("publication receipt time invalid")
		}
		_, offset := r.CompletedAt.Zone()
		if offset != 0 {
			return nil, errors.New("publication receipt time invalid")
		}
		timestamp := r.CompletedAt.Format("2006-01-02T15:04:05Z")
		receiptSchema := "loom.task-image-publication-receipt/v1"
		receipt := publicationReceiptWire{AttemptID: &r.AttemptID, CandidateSetSHA256: &r.CandidateSetSHA256, CompletedAt: &timestamp, ComponentCount: &r.ComponentCount, LeaseEpoch: &r.LeaseEpoch, MaterializationID: &r.MaterializationID, OperationID: &r.OperationID, PublicationSetSHA256: &r.PublicationSetSHA256, Schema: &receiptSchema, SnapshotSHA256: &r.SnapshotSHA256, WorkerGeneration: &r.WorkerGeneration}
		encoded, err := json.Marshal(receipt)
		if err != nil {
			return nil, errors.New("publication receipt invalid")
		}
		wire.Receipt = encoded
	}
	encoded, err := json.Marshal(wire)
	if err != nil {
		return nil, errors.New("publication status invalid")
	}
	validated, err := parsePublicationStatus(encoded, binding)
	if err != nil {
		return nil, err
	}
	return &validated, nil
}

func publicationUploadedSetHash(set BuiltComponentSet, builderID string, acks []PublicationCandidateV2Acknowledgement) (string, error) {
	invalid := errors.New("publication upload set invalid")
	if len(acks) != len(set.Components) || len(acks) == 0 {
		return "", invalid
	}
	identities := make([]publicationCandidateIdentity, len(acks))
	for i, ack := range acks {
		component := set.Components[i]
		platform := component.Output.OS + "/" + component.Output.Architecture
		if !baseResolutionMatchesComponent(component, platform) || !isCanonicalNonZeroUUID(ack.CredentialID) || ack.CredentialGeneration < 1 || ack.CredentialGeneration > 512 || ack.AttemptNumber < 1 || ack.AttemptNumber != acks[0].AttemptNumber {
			return "", invalid
		}
		request := PublicationCandidateV2Request{PublicationCandidateRequest: PublicationCandidateRequest{
			GrantID: set.GrantID, OperationID: ack.OperationID, CredentialID: ack.CredentialID, CredentialGeneration: ack.CredentialGeneration, SessionID: ack.SessionID, SessionGeneration: ack.SessionGeneration,
			MaterializationID: set.MaterializationID, AttemptID: set.AttemptID, AttemptNumber: ack.AttemptNumber, LeaseEpoch: set.LeaseEpoch, BuilderID: builderID, Component: component.Name,
			ManifestDigest: component.Output.TopLevelDigest, ManifestSize: component.Output.ManifestSize, OCIFileSHA256: component.Output.FileSHA256, OCIFileSize: component.Output.SizeBytes, Platform: platform}, BaseResolution: component.BaseResolution}
		if validateCandidateV2Acknowledgement(&ack, request) != nil {
			return "", invalid
		}
		identities[i] = publicationCandidateIdentity{CandidateID: ack.CandidateID, Component: ack.Component}
	}
	return publicationCandidateSetSHA256(identities)
}
