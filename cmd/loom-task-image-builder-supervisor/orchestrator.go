package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

type TaskImageGuard interface {
	Project(context.Context, string) (*AllocationCapabilities, error)
	Exchange(context.Context, string, string, string, *SecretBuffer) (*SessionEnvelope, error)
	Renew(context.Context, string, string, *SecretBuffer) (*SessionEnvelope, error)
	Claim(context.Context, string, string, *SecretBuffer) (*SecretBuffer, bool, error)
	Bundle(context.Context, string, string, string, string, int, *SecretBuffer) (*SecretBuffer, error)
	Start(context.Context, string, string, string, string, int, *SecretBuffer) (*LeaseResponse, error)
	Heartbeat(context.Context, string, string, string, string, int, *SecretBuffer) (*LeaseResponse, error)
	Release(context.Context, string, string, string, string, int, *SecretBuffer) (*LeaseResponse, error)
	Fail(context.Context, string, string, string, string, int, string, *SecretBuffer) (*LeaseResponse, error)
	Finish(context.Context, string, string, map[string]int) error
}

type BuildExecutor interface {
	Start(context.Context) error
	Build(context.Context, BuildComponent) (OCIOutput, error)
	Close(context.Context) error
}

type BundleDownloader interface {
	DownloadBundle(context.Context, *SecretBuffer, int) (*DownloadedBundle, error)
}

type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now().UTC() }

type ExecutorFactory func(Config, *AllocationCapabilities, BuildPlan) (BuildExecutor, error)

type Orchestrator struct {
	GrantID       string
	Config        Config
	Guard         TaskImageGuard
	Clock         Clock
	NewExecutor   ExecutorFactory
	Download      BundleDownloader
	Handoff       PublicationHandoff
	IdleGrace     time.Duration
	CleanupGrace  time.Duration
	RecordOutcome func(BuildOutcome)
}

func (o *Orchestrator) Run(ctx context.Context) (err error) {
	if err := o.validate(); err != nil {
		return err
	}
	clock := o.Clock
	if clock == nil {
		clock = realClock{}
	}
	cleanupGrace := o.CleanupGrace
	if cleanupGrace <= 0 {
		cleanupGrace = 5 * time.Second
	}
	state := &orchestratorState{
		o:            o,
		ctx:          ctx,
		clock:        clock,
		cleanupGrace: cleanupGrace,
		cleanup:      map[string]int{"descendant_processes": 0, "mounts": 0, "sockets": 0, "open_files": 0},
	}
	defer func() {
		cleanupErr := state.cleanupAll()
		if finishErr := state.finish(); finishErr != nil {
			cleanupErr = errors.Join(cleanupErr, finishErr)
		}
		state.closeSecrets()
		if state.outcomeRecorded == false {
			state.record(BuildOutcomeCancelled, "cancelled", "")
		}
		if cleanupErr != nil {
			err = errors.Join(err, cleanupErr)
		}
	}()

	state.caps, err = o.Guard.Project(ctx, o.GrantID)
	if err != nil {
		state.record(BuildOutcomeTransientFailure, "project_failed", "")
		return err
	}
	state.pushCleanup(func(context.Context) error {
		state.caps.Close()
		return nil
	})

	exchangeID, err := newUUID()
	if err != nil {
		state.record(BuildOutcomeTransientFailure, "uuid_failed", "")
		return err
	}
	state.operationID = exchangeID
	proofSHA256 := state.caps.ProofSHA256
	if proofSHA256 == "" {
		sum := sha256.Sum256(state.caps.Bootstrap.data)
		proofSHA256 = hex.EncodeToString(sum[:])
	}
	state.session, err = o.Guard.Exchange(ctx, o.GrantID, exchangeID, proofSHA256, state.caps.Bootstrap)
	if err != nil {
		state.record(BuildOutcomeTransientFailure, "exchange_failed", "")
		return err
	}
	if err := state.renew(); err != nil {
		state.fenced = true
		state.record(BuildOutcomeLeaseLost, "renew_failed", "")
		return err
	}

	claimID, err := newUUID()
	if err != nil {
		state.record(BuildOutcomeTransientFailure, "uuid_failed", "")
		return err
	}
	state.operationID = claimID
	claimSecret, available, err := o.Guard.Claim(ctx, o.GrantID, claimID, state.session.Secret)
	if err != nil {
		state.record(BuildOutcomeTransientFailure, "claim_failed", "")
		return err
	}
	if !available {
		state.record(BuildOutcomeCancelled, "idle", "")
		return nil
	}
	defer claimSecret.Close()

	claim, err := parseBuildClaim(claimSecret)
	if err != nil {
		state.record(BuildOutcomeContainmentFailure, "claim_invalid", "")
		return err
	}
	state.claim = claim
	state.operationID = claimID

	bundleID, err := newUUID()
	if err != nil {
		state.record(BuildOutcomeTransientFailure, "uuid_failed", "")
		return err
	}
	bundleSecret, err := o.Guard.Bundle(ctx, o.GrantID, bundleID, claim.MaterializationID, claim.AttemptID, claim.LeaseEpoch, state.session.Secret)
	if err != nil {
		state.record(BuildOutcomeTransientFailure, "bundle_failed", claim.firstComponent())
		return err
	}
	defer bundleSecret.Close()
	if _, err := o.Download.DownloadBundle(ctx, bundleSecret, state.caps.JobDirectoryFD); err != nil {
		state.record(BuildOutcomeDeterministicFailure, "bundle_download_failed", claim.firstComponent())
		return err
	}

	startID, err := newUUID()
	if err != nil {
		state.record(BuildOutcomeTransientFailure, "uuid_failed", "")
		return err
	}
	state.operationID = startID
	lease, err := o.Guard.Start(ctx, o.GrantID, startID, claim.MaterializationID, claim.AttemptID, claim.LeaseEpoch, state.session.Secret)
	if err != nil {
		state.record(BuildOutcomeLeaseLost, "start_failed", claim.firstComponent())
		return err
	}
	if err := claim.validateLease(lease); err != nil {
		state.record(BuildOutcomeLeaseLost, "start_invalid", claim.firstComponent())
		return err
	}
	state.lease = lease
	state.leaseHeartbeatAt = heartbeatAt(clock.Now(), lease.LeaseExpiresAt)

	executor, err := o.NewExecutor(o.Config, state.caps, claim.Plan)
	if err != nil {
		state.record(BuildOutcomeContainmentFailure, "executor_create_failed", claim.firstComponent())
		_ = state.failLease("containment")
		return err
	}
	state.executor = executor
	state.pushCleanup(executor.Close)
	if err := executor.Start(ctx); err != nil {
		state.record(BuildOutcomeContainmentFailure, "executor_start_failed", claim.firstComponent())
		_ = state.failLease("containment")
		return err
	}

	for _, component := range claim.Plan.Components {
		if err := ctx.Err(); err != nil {
			state.record(BuildOutcomeCancelled, "cancelled", component.Name)
			_ = state.failLease("containment")
			return err
		}
		output, err := executor.Build(ctx, component)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				state.record(BuildOutcomeCancelled, "cancelled", component.Name)
				_ = state.failLease("containment")
				return err
			}
			var deterministic DeterministicBuildError
			if errors.As(err, &deterministic) {
				reason := deterministic.Reason
				if reason == "" {
					reason = "build_failed"
				}
				state.record(BuildOutcomeDeterministicFailure, reason, component.Name)
				_ = state.failLease("deterministic")
				return err
			}
			state.record(BuildOutcomeTransientFailure, "build_failed", component.Name)
			_ = state.failLease("containment")
			return err
		}
		state.built = append(state.built, BuiltComponent{Name: component.Name, Output: output})
		if err := ctx.Err(); err != nil {
			state.record(BuildOutcomeCancelled, "cancelled", component.Name)
			_ = state.failLease("containment")
			return err
		}
		if state.heartbeatDue() {
			if err := state.heartbeat(); err != nil {
				state.fenced = true
				state.record(BuildOutcomeLeaseLost, "heartbeat_failed", component.Name)
				_ = state.failLease("containment")
				return err
			}
		}
	}

	set := BuiltComponentSet{
		GrantID:           o.GrantID,
		MaterializationID: claim.MaterializationID,
		AttemptID:         claim.AttemptID,
		LeaseEpoch:        claim.LeaseEpoch,
		Components:        append([]BuiltComponent(nil), state.built...),
	}
	if state.fenced {
		state.record(BuildOutcomeLeaseLost, "authority_fenced", "")
		return errors.New("authority fenced")
	}
	if err := o.Handoff.Accept(ctx, set); err != nil {
		if errors.Is(err, ErrPublicationPhaseUnavailable) {
			state.record(BuildOutcomeDeterministicFailure, "publication_phase_unavailable", "")
			if releaseErr := state.releaseLease(); releaseErr != nil {
				return errors.Join(err, releaseErr)
			}
			return err
		}
		state.record(BuildOutcomeTransientFailure, "publication_failed", "")
		_ = state.failLease("containment")
		return err
	}
	if err := state.releaseLease(); err != nil {
		state.record(BuildOutcomeLeaseLost, "release_failed", "")
		return err
	}
	state.recordBuilt()
	return nil
}

func (o *Orchestrator) validate() error {
	if o == nil {
		return errors.New("orchestrator unavailable")
	}
	if !isCanonicalNonZeroUUID(o.GrantID) {
		return errors.New("orchestrator grant id invalid")
	}
	if o.Guard == nil || o.NewExecutor == nil || o.Download == nil || o.Handoff == nil {
		return errors.New("orchestrator dependencies missing")
	}
	return nil
}

type orchestratorState struct {
	o                *Orchestrator
	ctx              context.Context
	clock            Clock
	cleanupGrace     time.Duration
	caps             *AllocationCapabilities
	session          *SessionEnvelope
	claim            *buildClaim
	lease            *LeaseResponse
	leaseHeartbeatAt time.Time
	operationID      string
	executor         BuildExecutor
	built            []BuiltComponent
	cleanup          map[string]int
	cleanupStack     []func(context.Context) error
	fenced           bool
	outcomeRecorded  bool
}

func (s *orchestratorState) renew() error {
	if s.session == nil || s.session.Secret == nil {
		return errors.New("session unavailable")
	}
	renewID, err := newUUID()
	if err != nil {
		return err
	}
	next, err := s.o.Guard.Renew(s.ctx, s.o.GrantID, renewID, s.session.Secret)
	if err != nil {
		return err
	}
	if next == nil || next.Secret == nil || next.GrantID != s.o.GrantID || next.Generation <= s.session.Generation {
		if next != nil && next.Secret != nil {
			next.Secret.Close()
		}
		return errors.New("renewed session invalid")
	}
	old := s.session
	s.session = next
	old.Secret.Close()
	return nil
}

func (s *orchestratorState) heartbeatDue() bool {
	return !s.leaseHeartbeatAt.IsZero() && !s.clock.Now().Before(s.leaseHeartbeatAt)
}

func (s *orchestratorState) heartbeat() error {
	if s.claim == nil {
		return errors.New("claim unavailable")
	}
	operationID, err := newUUID()
	if err != nil {
		return err
	}
	lease, err := s.o.Guard.Heartbeat(s.ctx, s.o.GrantID, operationID, s.claim.MaterializationID, s.claim.AttemptID, s.claim.LeaseEpoch, s.session.Secret)
	if err != nil {
		return err
	}
	if err := s.claim.validateLease(lease); err != nil {
		return err
	}
	s.lease = lease
	s.leaseHeartbeatAt = heartbeatAt(s.clock.Now(), lease.LeaseExpiresAt)
	return nil
}

func (s *orchestratorState) releaseLease() error {
	if s.claim == nil {
		return nil
	}
	operationID, err := newUUID()
	if err != nil {
		return err
	}
	_, err = s.o.Guard.Release(s.ctx, s.o.GrantID, operationID, s.claim.MaterializationID, s.claim.AttemptID, s.claim.LeaseEpoch, s.session.Secret)
	return err
}

func (s *orchestratorState) failLease(kind string) error {
	if s.claim == nil || s.session == nil || s.session.Secret == nil {
		return nil
	}
	operationID, err := newUUID()
	if err != nil {
		return err
	}
	_, err = s.o.Guard.Fail(s.ctx, s.o.GrantID, operationID, s.claim.MaterializationID, s.claim.AttemptID, s.claim.LeaseEpoch, kind, s.session.Secret)
	return err
}

func (s *orchestratorState) pushCleanup(fn func(context.Context) error) {
	if fn != nil {
		s.cleanupStack = append(s.cleanupStack, fn)
	}
}

func (s *orchestratorState) cleanupAll() error {
	var errs []error
	for i := len(s.cleanupStack) - 1; i >= 0; i-- {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), s.cleanupGrace)
		if err := s.cleanupStack[i](cleanupCtx); err != nil {
			errs = append(errs, err)
		}
		cancel()
	}
	s.cleanupStack = nil
	return errors.Join(errs...)
}

func (s *orchestratorState) finish() error {
	if s.operationID == "" {
		return nil
	}
	finishCtx, cancel := context.WithTimeout(context.Background(), s.cleanupGrace)
	defer cancel()
	return s.o.Guard.Finish(finishCtx, s.o.GrantID, s.operationID, s.cleanup)
}

func (s *orchestratorState) closeSecrets() {
	if s.session != nil && s.session.Secret != nil {
		s.session.Secret.Close()
	}
	if s.caps != nil && s.caps.Bootstrap != nil {
		s.caps.Bootstrap.Close()
	}
}

func (s *orchestratorState) record(status BuildOutcomeStatus, reason string, component string) {
	if s.outcomeRecorded {
		return
	}
	s.outcomeRecorded = true
	if s.o.RecordOutcome != nil {
		s.o.RecordOutcome(BuildOutcome{
			Status:     status,
			Reason:     boundedReason(reason),
			Component:  component,
			Components: append([]BuiltComponent(nil), s.built...),
			Cleanup:    copyCounters(s.cleanup),
		})
	}
}

func (s *orchestratorState) recordBuilt() {
	if s.outcomeRecorded {
		return
	}
	s.outcomeRecorded = true
	if s.o.RecordOutcome != nil {
		s.o.RecordOutcome(BuildOutcome{
			Status:     BuildOutcomeBuilt,
			Reason:     "built",
			Components: append([]BuiltComponent(nil), s.built...),
			Cleanup:    copyCounters(s.cleanup),
		})
	}
}

func copyCounters(in map[string]int) map[string]int {
	out := make(map[string]int, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func boundedReason(reason string) string {
	if reasonCodePattern.MatchString(reason) {
		return reason
	}
	return "operation_failed"
}

func heartbeatAt(now time.Time, expires *time.Time) time.Time {
	if expires == nil || !expires.After(now) {
		return time.Time{}
	}
	return now.Add(expires.Sub(now) / 3)
}

func renewalAt(now time.Time, expires time.Time) time.Time {
	if !expires.After(now) {
		return now
	}
	remaining := expires.Sub(now)
	beforeOneThirdRemaining := expires.Add(-(remaining / 3))
	beforeFifteenSeconds := expires.Add(-15 * time.Second)
	if beforeOneThirdRemaining.Before(beforeFifteenSeconds) {
		return beforeOneThirdRemaining
	}
	return beforeFifteenSeconds
}

type buildClaim struct {
	MaterializationID string
	AttemptID         string
	LeaseEpoch        int
	LeaseExpiresAt    time.Time
	Plan              BuildPlan
}

func (c buildClaim) firstComponent() string {
	if len(c.Plan.Components) == 0 {
		return ""
	}
	return c.Plan.Components[0].Name
}

func (c buildClaim) validateLease(lease *LeaseResponse) error {
	if lease == nil || lease.MaterializationID != c.MaterializationID || lease.AttemptID != c.AttemptID || lease.LeaseEpoch != c.LeaseEpoch {
		return errors.New("lease response binding invalid")
	}
	if lease.State != "claimed" && lease.State != "running" {
		return errors.New("lease response state invalid")
	}
	if lease.LeaseExpiresAt == nil {
		return errors.New("lease expiry missing")
	}
	return nil
}

type claimWire struct {
	SchemaVersion             string        `json:"schema_version"`
	ClaimID                   string        `json:"claim_id"`
	MaterializationID         string        `json:"materialization_id"`
	AttemptID                 string        `json:"attempt_id"`
	LeaseEpoch                int           `json:"lease_epoch"`
	State                     string        `json:"state"`
	DeterministicFailureCount int           `json:"deterministic_failure_count"`
	LeaseExpiresAt            string        `json:"lease_expires_at"`
	Plan                      buildPlanWire `json:"plan"`
}

type buildPlanWire struct {
	SchemaVersion            string               `json:"schema_version"`
	GrantID                  string               `json:"grant_id"`
	SessionID                string               `json:"session_id"`
	Generation               int                  `json:"session_generation"`
	MaterializeID            string               `json:"materialization_id"`
	BuilderID                string               `json:"builder_id"`
	TaskID                   string               `json:"task_id"`
	TaskChecksum             string               `json:"task_checksum"`
	CPUArch                  string               `json:"cpu_arch"`
	Platform                 string               `json:"platform"`
	BundleBucket             string               `json:"bundle_bucket"`
	BundlePrefix             string               `json:"bundle_prefix"`
	BundleFileMetadataSHA256 string               `json:"bundle_file_metadata_sha256"`
	BundleFileLimit          int                  `json:"bundle_file_limit"`
	BundleByteLimit          int64                `json:"bundle_byte_limit"`
	BuildTimeoutSeconds      float64              `json:"build_timeout_seconds"`
	AuthorizationExpiresAt   string               `json:"authorization_expires_at"`
	Components               []buildComponentWire `json:"components"`
}

type buildComponentWire struct {
	Name          string `json:"name"`
	Dockerfile    string `json:"dockerfile_path"`
	ContextPath   string `json:"context_path"`
	OCIOutputPath string `json:"oci_output_path"`
}

func parseBuildClaim(secret *SecretBuffer) (*buildClaim, error) {
	if secret == nil || secret.closed {
		return nil, errors.New("claim secret unavailable")
	}
	var wire claimWire
	decoder := json.NewDecoder(strings.NewReader(string(secret.data)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&wire); err != nil {
		return nil, errors.New("claim JSON invalid")
	}
	if err := expectJSONEOF(decoder); err != nil {
		return nil, errors.New("claim JSON invalid")
	}
	if wire.SchemaVersion != "loom.task-image-materialization-claim.v1" || !isCanonicalNonZeroUUID(wire.MaterializationID) || !isCanonicalNonZeroUUID(wire.AttemptID) || wire.LeaseEpoch <= 0 || wire.State != "claimed" && wire.State != "running" {
		return nil, errors.New("claim response invalid")
	}
	expires, err := time.Parse(time.RFC3339, wire.LeaseExpiresAt)
	if err != nil {
		return nil, err
	}
	plan, err := wire.Plan.buildPlan(wire.MaterializationID)
	if err != nil {
		return nil, err
	}
	return &buildClaim{
		MaterializationID: wire.MaterializationID,
		AttemptID:         wire.AttemptID,
		LeaseEpoch:        wire.LeaseEpoch,
		LeaseExpiresAt:    expires,
		Plan:              plan,
	}, nil
}

func (w buildPlanWire) buildPlan(materializationID string) (BuildPlan, error) {
	if w.SchemaVersion != "loom.task-image-build-plan.v1" || w.MaterializeID != materializationID || len(w.Components) == 0 {
		return BuildPlan{}, errors.New("build plan invalid")
	}
	architecture := ""
	switch w.Platform {
	case "linux/amd64":
		architecture = "amd64"
	case "linux/arm64":
		architecture = "arm64"
	default:
		return BuildPlan{}, errors.New("build plan platform invalid")
	}
	components := make([]BuildComponent, 0, len(w.Components))
	for index, component := range w.Components {
		if component.Name == "" || component.Dockerfile == "" || component.ContextPath == "" || component.OCIOutputPath != fmt.Sprintf("oci/%04d.tar", index) {
			return BuildPlan{}, errors.New("build plan component invalid")
		}
		components = append(components, BuildComponent{
			Name:       component.Name,
			ContextDir: component.ContextPath,
			Dockerfile: component.Dockerfile,
		})
	}
	return BuildPlan{
		Architecture: architecture,
		Frontend:     "dockerfile.v0",
		NetworkMode:  "default",
		Components:   components,
	}, nil
}

type realBundleDownloader struct{}

func (realBundleDownloader) DownloadBundle(ctx context.Context, secret *SecretBuffer, fd int) (*DownloadedBundle, error) {
	return DownloadBundle(ctx, secret, fd)
}

type executorAdapter struct{}

func (executorAdapter) New(cfg Config, caps *AllocationCapabilities, plan BuildPlan) (BuildExecutor, error) {
	return NewExecutor(cfg, caps, plan)
}

type DeterministicBuildError struct {
	Reason string
	Err    error
}

func (e DeterministicBuildError) Error() string {
	if e.Err != nil {
		return e.Err.Error()
	}
	if e.Reason != "" {
		return e.Reason
	}
	return "deterministic build failure"
}

func (e DeterministicBuildError) Unwrap() error { return e.Err }

var ErrPublicationPhaseUnavailable = errors.New("publication_phase_unavailable")

type DisabledPublicationHandoff struct{}

func (DisabledPublicationHandoff) Accept(context.Context, BuiltComponentSet) error {
	return ErrPublicationPhaseUnavailable
}
