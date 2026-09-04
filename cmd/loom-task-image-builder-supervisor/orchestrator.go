package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

const (
	maxTaskImageBuildBundleFiles       = 2000
	maxTaskImageBuildBundleBytes int64 = 512 * 1024 * 1024
	maxTaskImageBuildSeconds           = 2 * 60 * 60
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

type Timer interface {
	C() <-chan time.Time
	Stop() bool
}

type Clock interface {
	Now() time.Time
	NewTimer(time.Duration) Timer
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now().UTC() }

func (realClock) NewTimer(d time.Duration) Timer {
	if d < 0 {
		d = 0
	}
	return realTimer{timer: time.NewTimer(d)}
}

type realTimer struct{ timer *time.Timer }

func (t realTimer) C() <-chan time.Time { return t.timer.C }

func (t realTimer) Stop() bool { return t.timer.Stop() }

type ExecutorFactory func(Config, *AllocationCapabilities, BuildPlan) (BuildExecutor, error)

type Orchestrator struct {
	GrantID       string
	Config        Config
	Guard         TaskImageGuard
	Clock         Clock
	NewExecutor   ExecutorFactory
	Download      BundleDownloader
	Handoff       PublicationHandoff
	PostProject   func(context.Context, *AllocationCapabilities) error
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
		if !state.outcomeRecorded {
			state.record(BuildOutcomeCancelled, "cancelled", "")
		}
		if cleanupErr != nil {
			err = errors.Join(err, cleanupErr)
		}
	}()

	state.caps, err = o.Guard.Project(ctx, o.GrantID)
	if err != nil {
		state.record(BuildOutcomeTransientFailure, "project_failed", "")
		return errors.Join(safeError("project_failed"), err)
	}
	state.pushCleanup(func(context.Context) error {
		state.caps.Close()
		return nil
	})
	state.finishID, err = newUUID()
	if err != nil {
		state.record(BuildOutcomeTransientFailure, "uuid_failed", "")
		return safeError("uuid_failed")
	}
	if !isDigest(state.caps.ProofSHA256) {
		state.record(BuildOutcomeContainmentFailure, "project_proof_missing", "")
		return safeError("project_proof_missing")
	}
	if o.PostProject != nil {
		if err := o.PostProject(ctx, state.caps); err != nil {
			state.record(BuildOutcomeContainmentFailure, "environment_failed", "")
			return safeError("environment_failed")
		}
	}

	exchangeID, err := newUUID()
	if err != nil {
		state.record(BuildOutcomeTransientFailure, "uuid_failed", "")
		return safeError("uuid_failed")
	}
	session, err := o.Guard.Exchange(ctx, o.GrantID, exchangeID, state.caps.ProofSHA256, state.caps.Bootstrap)
	if err != nil {
		state.record(BuildOutcomeTransientFailure, "exchange_failed", "")
		return safeError("exchange_failed")
	}
	state.sessionManager = NewSessionManager(o.GrantID, session, o.Guard)
	state.session = session

	for {
		if err := state.renewIfDue(); err != nil {
			state.fenced = true
			state.record(BuildOutcomeLeaseLost, "renew_failed", "")
			return errors.Join(safeError("renew_failed"), errCleanupAmbiguous)
		}
		claimID, err := newUUID()
		if err != nil {
			state.record(BuildOutcomeTransientFailure, "uuid_failed", "")
			return safeError("uuid_failed")
		}
		claimSecret, available, err := state.claim(claimID)
		if err != nil {
			state.record(BuildOutcomeTransientFailure, "claim_failed", "")
			return safeError("claim_failed")
		}
		if !available {
			if state.idleOnce {
				state.record(BuildOutcomeCancelled, "idle", "")
				return nil
			}
			state.idleOnce = true
			state.o.RecordIdleWait()
			if err := state.waitIdle(); err != nil {
				state.record(BuildOutcomeCancelled, "cancelled", "")
				return err
			}
			continue
		}
		state.idleOnce = false
		err = state.runClaim(claimID, claimSecret)
		claimSecret.Close()
		if err != nil {
			return err
		}
	}
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

func (o *Orchestrator) RecordIdleWait() {
	type idleRecorder interface{ recordIdleWait() }
	if recorder, ok := any(o.Guard).(idleRecorder); ok {
		recorder.recordIdleWait()
	}
}

type orchestratorState struct {
	o               *Orchestrator
	ctx             context.Context
	clock           Clock
	cleanupGrace    time.Duration
	caps            *AllocationCapabilities
	sessionManager  *SessionManager
	session         *SessionEnvelope
	claimData       *buildClaim
	finishID        string
	executor        BuildExecutor
	built           []BuiltComponent
	cleanup         map[string]int
	cleanupStack    []func(context.Context) error
	fenced          bool
	idleOnce        bool
	outcomeRecorded bool
}

func (s *orchestratorState) claim(claimID string) (*SecretBuffer, bool, error) {
	var claimSecret *SecretBuffer
	var available bool
	err := s.sessionManager.WithCurrent(func(current *SecretBuffer) error {
		var err error
		claimSecret, available, err = s.o.Guard.Claim(s.ctx, s.o.GrantID, claimID, current)
		return err
	})
	return claimSecret, available, err
}

func (s *orchestratorState) runClaim(claimID string, claimSecret *SecretBuffer) (err error) {
	claim, err := parseBuildClaim(claimSecret, claimBinding{
		GrantID:           s.o.GrantID,
		ClaimID:           claimID,
		SessionID:         s.session.SessionID,
		SessionGeneration: s.session.Generation,
		ConfigCPUArch:     s.o.Config.CPUArch,
		Now:               s.clock.Now(),
		SessionExpiresAt:  s.session.ExpiresAt,
	})
	if err != nil {
		s.record(BuildOutcomeContainmentFailure, "claim_invalid", "")
		return safeError("claim_invalid")
	}
	s.claimData = claim
	s.built = nil
	bundleID, err := newUUID()
	if err != nil {
		s.record(BuildOutcomeTransientFailure, "uuid_failed", "")
		return safeError("uuid_failed")
	}
	var bundleSecret *SecretBuffer
	if err := s.sessionManager.WithCurrent(func(current *SecretBuffer) error {
		var err error
		bundleSecret, err = s.o.Guard.Bundle(s.ctx, s.o.GrantID, bundleID, claim.MaterializationID, claim.AttemptID, claim.LeaseEpoch, current)
		return err
	}); err != nil {
		s.record(BuildOutcomeTransientFailure, "bundle_failed", claim.firstComponent())
		return safeError("bundle_failed")
	}
	defer bundleSecret.Close()
	if _, err := s.o.Download.DownloadBundle(s.ctx, bundleSecret, s.caps.JobDirectoryFD); err != nil {
		s.record(BuildOutcomeDeterministicFailure, "bundle_download_failed", claim.firstComponent())
		return safeError("bundle_download_failed")
	}

	startID, err := newUUID()
	if err != nil {
		s.record(BuildOutcomeTransientFailure, "uuid_failed", "")
		return safeError("uuid_failed")
	}
	var lease *LeaseResponse
	if err := s.sessionManager.WithCurrent(func(current *SecretBuffer) error {
		var err error
		lease, err = s.o.Guard.Start(s.ctx, s.o.GrantID, startID, claim.MaterializationID, claim.AttemptID, claim.LeaseEpoch, current)
		return err
	}); err != nil {
		s.record(BuildOutcomeLeaseLost, "start_failed", claim.firstComponent())
		return errors.Join(safeError("start_failed"), err)
	}
	if err := claim.validateLease(lease); err != nil {
		s.record(BuildOutcomeLeaseLost, "start_invalid", claim.firstComponent())
		return safeError("start_invalid")
	}

	executor, err := s.o.NewExecutor(s.o.Config, s.caps, claim.Plan)
	if err != nil {
		s.record(BuildOutcomeContainmentFailure, "executor_create_failed", claim.firstComponent())
		return errors.Join(safeError("executor_create_failed"), s.failLease("containment"))
	}
	s.executor = executor
	defer func() {
		if cleanupErr := s.closeExecutor(executor); cleanupErr != nil {
			err = errors.Join(err, cleanupErr)
		}
		s.executor = nil
	}()
	if err := executor.Start(s.ctx); err != nil {
		s.record(BuildOutcomeContainmentFailure, "executor_start_failed", claim.firstComponent())
		return errors.Join(safeError("executor_start_failed"), s.failLease("containment"))
	}
	if err := s.buildComponents(lease); err != nil {
		return err
	}

	set := BuiltComponentSet{
		GrantID:           s.o.GrantID,
		MaterializationID: claim.MaterializationID,
		AttemptID:         claim.AttemptID,
		LeaseEpoch:        claim.LeaseEpoch,
		Components:        append([]BuiltComponent(nil), s.built...),
	}
	if err := s.o.Handoff.Accept(s.ctx, set); err != nil {
		if errors.Is(err, ErrPublicationPhaseUnavailable) {
			s.record(BuildOutcomeTransientFailure, "publication_phase_unavailable", "")
			return errors.Join(ErrPublicationPhaseUnavailable, s.releaseLease())
		}
		s.record(BuildOutcomeTransientFailure, "publication_failed", "")
		return errors.Join(safeError("publication_failed"), s.failLease("containment"))
	}
	if err := s.releaseLease(); err != nil {
		s.record(BuildOutcomeLeaseLost, "release_failed", "")
		return err
	}
	s.recordBuilt()
	return nil
}

func (s *orchestratorState) closeExecutor(executor BuildExecutor) error {
	if executor == nil {
		return nil
	}
	cleanupCtx, cancel := context.WithTimeout(context.Background(), s.cleanupGrace)
	defer cancel()
	if err := executor.Close(cleanupCtx); err != nil {
		return errors.Join(errCleanupAmbiguous, safeError("cleanup_failed"))
	}
	return nil
}

type buildResult struct {
	component BuildComponent
	output    OCIOutput
	err       error
}

func (s *orchestratorState) buildComponents(lease *LeaseResponse) error {
	leaseTimer := s.clock.NewTimer(durationUntil(s.clock.Now(), heartbeatAt(s.clock.Now(), lease.LeaseExpiresAt)))
	defer leaseTimer.Stop()
	renewTimer := s.clock.NewTimer(durationUntil(s.clock.Now(), renewalAt(s.clock.Now(), s.session.ExpiresAt)))
	defer renewTimer.Stop()
	for _, component := range s.claimData.Plan.Components {
		buildCtx, cancelBuild := context.WithCancel(s.ctx)
		result := make(chan buildResult, 1)
		go func(component BuildComponent) {
			output, err := s.executor.Build(buildCtx, component)
			result <- buildResult{component: component, output: output, err: err}
		}(component)
		for {
			select {
			case <-s.ctx.Done():
				cancelBuild()
				<-result
				s.record(BuildOutcomeCancelled, "cancelled", component.Name)
				return errors.Join(s.ctx.Err(), s.failLease("containment"))
			case <-renewTimer.C():
				if err := s.renew(); err != nil {
					cancelBuild()
					<-result
					s.fenced = true
					s.record(BuildOutcomeLeaseLost, "renew_failed", component.Name)
					return errors.Join(safeError("renew_failed"), err, s.failLease("containment"))
				}
				renewTimer.Stop()
				renewTimer = s.clock.NewTimer(durationUntil(s.clock.Now(), renewalAt(s.clock.Now(), s.session.ExpiresAt)))
			case <-leaseTimer.C():
				if err := s.heartbeat(); err != nil {
					cancelBuild()
					<-result
					s.fenced = true
					s.record(BuildOutcomeLeaseLost, "heartbeat_failed", component.Name)
					return errors.Join(safeError("heartbeat_failed"), err, s.failLease("containment"))
				}
				leaseTimer.Stop()
				leaseTimer = s.clock.NewTimer(durationUntil(s.clock.Now(), heartbeatAt(s.clock.Now(), s.claimData.LeaseExpiresAtPtr)))
			case got := <-result:
				cancelBuild()
				if got.err != nil {
					return s.handleBuildError(got)
				}
				if s.fenced {
					s.record(BuildOutcomeLeaseLost, "authority_fenced", got.component.Name)
					return safeError("authority_fenced")
				}
				s.built = append(s.built, BuiltComponent{Name: got.component.Name, Output: got.output})
				goto nextComponent
			}
		}
	nextComponent:
	}
	return nil
}

func (s *orchestratorState) handleBuildError(got buildResult) error {
	if errors.Is(got.err, context.Canceled) || errors.Is(got.err, context.DeadlineExceeded) {
		s.record(BuildOutcomeCancelled, "cancelled", got.component.Name)
		return errors.Join(context.Canceled, s.failLease("containment"))
	}
	var deterministic DeterministicBuildError
	if errors.As(got.err, &deterministic) {
		reason := deterministic.Reason
		if reason == "" {
			reason = "build_failed"
		}
		s.record(BuildOutcomeDeterministicFailure, reason, got.component.Name)
		return errors.Join(safeError(reason), s.failLease("deterministic"))
	}
	s.record(BuildOutcomeTransientFailure, "build_failed", got.component.Name)
	return errors.Join(safeError("build_failed"), s.failLease("containment"))
}

func (s *orchestratorState) waitIdle() error {
	timer := s.clock.NewTimer(s.o.IdleGrace)
	defer timer.Stop()
	select {
	case <-timer.C():
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

func (s *orchestratorState) renewIfDue() error {
	if !s.clock.Now().Before(renewalAt(s.clock.Now(), s.session.ExpiresAt)) {
		return s.renew()
	}
	return nil
}

func (s *orchestratorState) renew() error {
	next, err := s.sessionManager.Renew(s.ctx)
	if err != nil {
		return errCleanupAmbiguous
	}
	s.session = next
	return nil
}

func (s *orchestratorState) heartbeat() error {
	operationID, err := newUUID()
	if err != nil {
		return errCleanupAmbiguous
	}
	var lease *LeaseResponse
	if err := s.sessionManager.WithCurrent(func(current *SecretBuffer) error {
		var err error
		lease, err = s.o.Guard.Heartbeat(s.ctx, s.o.GrantID, operationID, s.claimData.MaterializationID, s.claimData.AttemptID, s.claimData.LeaseEpoch, current)
		return err
	}); err != nil {
		return errCleanupAmbiguous
	}
	if err := s.claimData.validateLease(lease); err != nil {
		return errCleanupAmbiguous
	}
	s.claimData.LeaseExpiresAtPtr = lease.LeaseExpiresAt
	return nil
}

func (s *orchestratorState) releaseLease() error {
	if s.claimData == nil {
		return nil
	}
	operationID, err := newUUID()
	if err != nil {
		return errCleanupAmbiguous
	}
	var releaseErr error
	err = s.sessionManager.WithCurrent(func(current *SecretBuffer) error {
		_, releaseErr = s.o.Guard.Release(s.ctx, s.o.GrantID, operationID, s.claimData.MaterializationID, s.claimData.AttemptID, s.claimData.LeaseEpoch, current)
		return releaseErr
	})
	if err != nil {
		return errors.Join(errCleanupAmbiguous, safeError("release_failed"))
	}
	return nil
}

func (s *orchestratorState) failLease(kind string) error {
	if s.claimData == nil || s.sessionManager == nil {
		return nil
	}
	operationID, err := newUUID()
	if err != nil {
		return errCleanupAmbiguous
	}
	err = s.sessionManager.WithCurrent(func(current *SecretBuffer) error {
		_, err := s.o.Guard.Fail(s.ctx, s.o.GrantID, operationID, s.claimData.MaterializationID, s.claimData.AttemptID, s.claimData.LeaseEpoch, kind, current)
		return err
	})
	if err != nil {
		return errors.Join(errCleanupAmbiguous, safeError("fail_failed"))
	}
	return nil
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
			errs = append(errs, errors.Join(errCleanupAmbiguous, safeError("cleanup_failed")))
		}
		cancel()
	}
	s.cleanupStack = nil
	s.executor = nil
	return errors.Join(errs...)
}

func (s *orchestratorState) finish() error {
	if s.finishID == "" {
		return nil
	}
	finishCtx, cancel := context.WithTimeout(context.Background(), s.cleanupGrace)
	defer cancel()
	if err := s.o.Guard.Finish(finishCtx, s.o.GrantID, s.finishID, copyCounters(s.cleanup)); err != nil {
		return errors.Join(errCleanupAmbiguous, safeError("finish_failed"))
	}
	return nil
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
	s.record(BuildOutcomeBuilt, "built", "")
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

func safeError(code string) error {
	return errors.New(boundedReason(code))
}

var errCleanupAmbiguous = errors.New("cleanup_ambiguous")

func heartbeatAt(now time.Time, expires *time.Time) time.Time {
	if expires == nil || !expires.After(now) {
		return now
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

func durationUntil(now, at time.Time) time.Duration {
	if at.IsZero() || !at.After(now) {
		return 0
	}
	return at.Sub(now)
}

type buildClaim struct {
	ClaimID           string
	MaterializationID string
	AttemptID         string
	LeaseEpoch        int
	LeaseExpiresAt    time.Time
	LeaseExpiresAtPtr *time.Time
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

type claimBinding struct {
	GrantID           string
	ClaimID           string
	SessionID         string
	SessionGeneration int
	ConfigCPUArch     string
	Now               time.Time
	SessionExpiresAt  time.Time
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

func parseBuildClaim(secret *SecretBuffer, binding claimBinding) (*buildClaim, error) {
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
	if wire.SchemaVersion != "loom.task-image-materialization-claim.v1" ||
		wire.ClaimID != binding.ClaimID ||
		!isCanonicalNonZeroUUID(wire.MaterializationID) ||
		!isCanonicalNonZeroUUID(wire.AttemptID) ||
		wire.LeaseEpoch <= 0 ||
		(wire.State != "claimed" && wire.State != "running") {
		return nil, errors.New("claim response invalid")
	}
	expires, err := time.Parse(time.RFC3339, wire.LeaseExpiresAt)
	if err != nil || !expires.After(binding.Now) {
		return nil, errors.New("claim lease expiry invalid")
	}
	plan, err := wire.Plan.buildPlan(wire.MaterializationID, binding)
	if err != nil {
		return nil, err
	}
	return &buildClaim{
		ClaimID:           wire.ClaimID,
		MaterializationID: wire.MaterializationID,
		AttemptID:         wire.AttemptID,
		LeaseEpoch:        wire.LeaseEpoch,
		LeaseExpiresAt:    expires,
		LeaseExpiresAtPtr: &expires,
		Plan:              plan,
	}, nil
}

func (w buildPlanWire) buildPlan(materializationID string, binding claimBinding) (BuildPlan, error) {
	native, platform, err := authorityArchForGo(binding.ConfigCPUArch)
	if err != nil {
		return BuildPlan{}, err
	}
	expectedBuilderID := "rootless:" + strings.ReplaceAll(binding.SessionID, "-", "")
	if w.SchemaVersion != "loom.task-image-build-plan.v1" ||
		w.GrantID != binding.GrantID ||
		w.SessionID != binding.SessionID ||
		w.Generation != binding.SessionGeneration ||
		w.MaterializeID != materializationID ||
		w.MaterializeID != binding.GrantSafeMaterialization(materializationID) ||
		w.BuilderID != expectedBuilderID ||
		w.CPUArch != native ||
		w.Platform != platform ||
		!isDigest(w.TaskChecksum) ||
		!isDigest(w.BundleFileMetadataSHA256) ||
		w.BundleFileLimit <= 0 ||
		w.BundleFileLimit > maxTaskImageBuildBundleFiles ||
		w.BundleByteLimit <= 0 ||
		w.BundleByteLimit > maxTaskImageBuildBundleBytes ||
		(math.IsNaN(w.BuildTimeoutSeconds) || math.IsInf(w.BuildTimeoutSeconds, 0)) ||
		w.BuildTimeoutSeconds <= 0 ||
		w.BuildTimeoutSeconds > maxTaskImageBuildSeconds ||
		len(w.Components) == 0 ||
		len(w.Components) > 128 {
		return BuildPlan{}, errors.New("build plan invalid")
	}
	authExpires, err := time.Parse(time.RFC3339, w.AuthorizationExpiresAt)
	if err != nil || authExpires.Before(binding.Now) || authExpires.After(binding.SessionExpiresAt) {
		return BuildPlan{}, errors.New("build plan authorization invalid")
	}
	components := make([]BuildComponent, 0, len(w.Components))
	names := make([]string, 0, len(w.Components))
	for index, component := range w.Components {
		if component.Name == "" ||
			component.Dockerfile == "" ||
			component.ContextPath == "" ||
			component.OCIOutputPath != fmt.Sprintf("oci/%04d.tar", index) ||
			validateRelativeBundlePath(component.Dockerfile) != nil ||
			(component.ContextPath != "." && validateRelativeBundlePath(component.ContextPath) != nil) {
			return BuildPlan{}, errors.New("build plan component invalid")
		}
		names = append(names, component.Name)
		components = append(components, BuildComponent{
			Name:       component.Name,
			ContextDir: component.ContextPath,
			Dockerfile: component.Dockerfile,
		})
	}
	sortedNames := append([]string(nil), names...)
	sort.Slice(sortedNames, func(i, j int) bool {
		return (sortedNames[i] != "task") == (sortedNames[j] != "task") && sortedNames[i] < sortedNames[j] || sortedNames[i] == "task"
	})
	for i := range names {
		if names[i] != sortedNames[i] || (i > 0 && names[i] == names[i-1]) {
			return BuildPlan{}, errors.New("build plan component order invalid")
		}
	}
	architecture := "arm64"
	if binding.ConfigCPUArch == "amd64" {
		architecture = "amd64"
	}
	return BuildPlan{
		Architecture: architecture,
		Frontend:     "dockerfile.v0",
		NetworkMode:  "sandbox",
		Components:   components,
	}, nil
}

func (binding claimBinding) GrantSafeMaterialization(materializationID string) string {
	return materializationID
}

func authorityArchForGo(goArch string) (string, string, error) {
	switch goArch {
	case "amd64":
		return "x86_64", "linux/amd64", nil
	case "arm64":
		return "arm64", "linux/arm64", nil
	default:
		return "", "", errors.New("unsupported native architecture")
	}
}

type realBundleDownloader struct{}

func (realBundleDownloader) DownloadBundle(ctx context.Context, secret *SecretBuffer, fd int) (*DownloadedBundle, error) {
	return DownloadBundle(ctx, secret, fd)
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
