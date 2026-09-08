package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestRegistryCredentialFixTimesAndJSON(t *testing.T) {
	valid := validRegistryCredentialJSON(registryCredentialMutation{})
	for _, tc := range []struct {
		name, payload string
		now           time.Time
		ok            bool
	}{
		{"issue boundary", valid, testNow, true},
		{"before expiry", valid, testNow.Add(30*time.Second - time.Nanosecond), true},
		{"expired boundary", valid, testNow.Add(30 * time.Second), false},
		{"stale", valid, testNow.Add(time.Minute), false},
		{"future", valid, testNow.Add(-time.Nanosecond), false},
		{"trailing comma", strings.TrimSuffix(valid, "}") + ",}", testNow, false},
		{"duplicate", strings.TrimSuffix(valid, "}") + `,"generation":1}`, testNow, false},
		{"missing", strings.Replace(valid, `"generation":1,`, "", 1), testNow, false},
		{"truncated", valid[:len(valid)-1], testNow, false},
		{"escaped token", strings.Replace(valid, "header.payload.signature", `\u0068eader.payload.signature`, 1), testNow, false},
		{"invalid escape", strings.Replace(valid, "header.payload.signature", `\x00.payload.signature`, 1), testNow, false},
		{"control byte", strings.Replace(valid, "header.payload.signature", "head\x01er.payload.signature", 1), testNow, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			secret := mustSecretBuffer(t, []byte(tc.payload))
			defer secret.Close()
			binding := validRegistryCredentialBinding()
			binding.Now = tc.now
			c, err := ParseRegistryCredential(secret, binding)
			if (err == nil) != tc.ok {
				t.Fatalf("accepted=%v, want %v", err == nil, tc.ok)
			}
			if c != nil {
				token := c.BearerToken
				c.Close()
				c.Close()
				if !bytes.Equal(token, make([]byte, len(token))) || !secret.closed {
					t.Fatal("token not zeroized")
				}
			}
		})
	}
}

// Allocation growth with token length exposes even a temporary decoded string.
func TestRegistryCredentialFixNoTokenCopy(t *testing.T) {
	measure := func(token string) uint64 {
		secret := mustSecretBuffer(t, []byte(validRegistryCredentialJSON(registryCredentialMutation{BearerToken: token})))
		defer secret.Close()
		binding := validRegistryCredentialBinding()
		for i := 0; i < 5; i++ {
			if _, err := ParseRegistryCredential(secret, binding); err != nil {
				t.Fatal(err)
			}
		}
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		for i := 0; i < 20; i++ {
			if _, err := ParseRegistryCredential(secret, binding); err != nil {
				t.Fatal(err)
			}
		}
		runtime.ReadMemStats(&after)
		return (after.TotalAlloc - before.TotalAlloc) / 20
	}
	small := measure("a.b.c")
	large := measure(strings.Repeat("a", 32000) + ".b.c")
	if large > small+8000 {
		t.Fatalf("token-dependent unlocked allocation: small=%d large=%d bytes/parse", small, large)
	}
}

type fixGuard struct {
	credentialSourceGuard
	renewErr, heartbeatFailure, transportErr error
	mutate                                   func(*registryCredentialMutation)
	ackMutate                                func(*PublicationCandidateAcknowledgement)
	renewMutate                              func(*SessionEnvelope)
	heartbeatHook                            func(*SecretBuffer)
	credentialHook                           func(*SecretBuffer)
	predecessor                              *RegistryCredential
	lastSecret                               *SecretBuffer
	request                                  PublicationCandidateRequest
	generation                               int
}

func (g *fixGuard) Renew(_ context.Context, _ string, _ string, _ *SecretBuffer) (*SessionEnvelope, error) {
	g.events = append(g.events, "renew")
	if g.renewErr != nil {
		return nil, g.renewErr
	}
	g.generation++
	s := testSession(g.generation, time.Now().Add(time.Minute))
	if g.renewMutate != nil {
		g.renewMutate(s)
	}
	return s, nil
}
func (g *fixGuard) Heartbeat(_ context.Context, grant, op, mat, attempt string, epoch int, current *SecretBuffer) (*LeaseResponse, error) {
	g.events = append(g.events, "heartbeat")
	if g.heartbeatHook != nil {
		g.heartbeatHook(current)
	}
	if g.predecessor != nil && g.predecessor.secret.closed {
		return nil, errors.New("premature predecessor close")
	}
	g.lastHeartbeat = op
	if g.heartbeatFailure != nil {
		return nil, g.heartbeatFailure
	}
	return testLease("heartbeat", op, time.Now().Add(time.Minute)), nil
}
func (g *fixGuard) RegistryCredential(_ context.Context, r RegistryCredentialRequest, current *SecretBuffer) (*SecretBuffer, error) {
	g.events = append(g.events, "registry-credential")
	if g.credentialHook != nil {
		g.credentialHook(current)
	}
	if g.predecessor != nil && g.predecessor.secret.closed {
		return nil, errors.New("premature predecessor close")
	}
	if g.transportErr != nil {
		return nil, g.transportErr
	}
	now := time.Now().UTC().Truncate(time.Second)
	repo, _ := publicationRepository("arm64", testAttemptID, r.Component)
	m := registryCredentialMutation{Component: r.Component, Repository: repo, RequestID: r.OperationID, SessionGeneration: g.generation, AttestationGeneration: g.generation, IssuedAt: now.Format(time.RFC3339), ExpiresAt: now.Add(45 * time.Second).Format(time.RFC3339)}
	if r.PredecessorGeneration > 0 {
		m.CredentialID = fmt.Sprintf("88888888-8888-4888-8888-%012x", r.PredecessorGeneration+1)
		m.Generation = r.PredecessorGeneration + 1
		m.PredecessorCredentialIDJSON = `"` + r.PredecessorCredentialID + `"`
		m.PredecessorGenerationJSON = strconv.Itoa(r.PredecessorGeneration)
		m.HeartbeatOperationIDJSON = `"` + g.lastHeartbeat + `"`
	}
	if g.mutate != nil {
		g.mutate(&m)
	}
	g.lastSecret = &SecretBuffer{data: []byte(validRegistryCredentialJSON(m))}
	return g.lastSecret, nil
}
func (g *fixGuard) PublicationCandidate(ctx context.Context, r PublicationCandidateRequest, current *SecretBuffer) (*PublicationCandidateAcknowledgement, error) {
	g.request = r
	ack, err := g.credentialSourceGuard.PublicationCandidate(ctx, r, current)
	if g.ackMutate != nil {
		g.ackMutate(ack)
	}
	return ack, err
}
func fixSource(t *testing.T) (*PublicationCredentialSource, *fixGuard, BuiltComponentSet) {
	t.Helper()
	g := &fixGuard{generation: 1}
	s := NewPublicationCredentialSource(NewSessionManager(testGrantID, testSession(1, time.Now().Add(time.Minute)), g), g, validPublicationAttemptBinding())
	t.Cleanup(func() { _ = s.session.WithCurrent(func(b *SecretBuffer) error { b.Close(); return nil }) })
	set := testBuiltSet()
	set.Components[0].Output = OCIOutput{TopLevelDigest: "sha256:" + strings.Repeat("a", 64), FileSHA256: strings.Repeat("b", 64), SizeBytes: 5678, OS: "linux", Architecture: "arm64"}
	// Reflection keeps the RED suite runnable before the metadata field exists.
	f := reflect.ValueOf(&set.Components[0].Output).Elem().FieldByName("ManifestSize")
	if f.IsValid() {
		f.SetInt(321)
	}
	return s, g, set
}
func firstCredential(t *testing.T, s *PublicationCredentialSource, set BuiltComponentSet) *RegistryCredential {
	t.Helper()
	c, err := s.Next(context.Background(), set, "task", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}
func TestPublicationCredentialSourceFixCaptureAttempt(t *testing.T) {
	s, g, set := fixSource(t)

	c, err := s.Next(context.Background(), set, "task", nil)
	if err != nil {
		t.Fatalf("discover attempt: %v", err)
	}
	defer c.Close()
	if c.AttemptNumber != 11 {
		t.Fatal("authority attempt not captured")
	}
	if _, err = s.Record(context.Background(), set, c, set.Components[0]); err != nil {
		t.Fatal(err)
	}
	if g.request.AttemptNumber != 11 {
		t.Fatal("candidate did not retain attempt")
	}
	g.mutate = func(m *registryCredentialMutation) { m.AttemptNumber = 12 }
	next, err := s.Next(context.Background(), set, "task", c)
	if next != nil {
		next.Close()
	}
	if err == nil || c.secret.closed {
		t.Fatal("renewal attempt drift accepted or predecessor closed")
	}
}
func TestPublicationCredentialSourceFixFailures(t *testing.T) {
	for _, name := range []string{"renew", "heartbeat", "transport", "parse", "session liveness", "attestation liveness", "success"} {
		t.Run(name, func(t *testing.T) {
			s, g, set := fixSource(t)
			c := firstCredential(t, s, set)
			token := c.BearerToken
			g.predecessor = c
			g.events = nil
			switch name {
			case "renew":
				g.renewErr = errors.New("renew")
			case "heartbeat":
				g.heartbeatFailure = errors.New("heartbeat")
			case "transport":
				g.transportErr = errors.New("transport")
			case "parse":
				g.mutate = func(m *registryCredentialMutation) { m.BearerToken = "invalid" }
			case "session liveness":
				c.SessionGeneration = 2
			case "attestation liveness":
				g.renewMutate = func(s *SessionEnvelope) { s.AttestationGeneration = 1 }
				g.mutate = func(m *registryCredentialMutation) { m.AttestationGeneration = 1 }
			}
			next, err := s.Next(context.Background(), set, "task", c)
			if next != nil {
				defer next.Close()
			}
			if name == "success" {
				if err != nil {
					t.Fatal(err)
				}
				if !c.secret.closed || !bytes.Equal(token, make([]byte, len(token))) {
					t.Fatal("predecessor not zeroized")
				}
				return
			}
			if err == nil {
				t.Fatal("failure accepted")
			}
			if c.secret.closed || bytes.Equal(token, make([]byte, len(token))) {
				t.Fatal("predecessor lost on failure")
			}
			if name == "parse" && (!g.lastSecret.closed || !bytes.Equal(g.lastSecret.data, make([]byte, len(g.lastSecret.data)))) {
				t.Fatal("failed successor not zeroized")
			}
		})
	}
}
func TestPublicationCredentialSourceFixRejectInputsBeforeEffects(t *testing.T) {
	for _, name := range []string{"absent component", "foreign source", "attempt", "component", "closed", "generation", "limit", "frozen evidence", "record evidence", "missing manifest", "negative manifest"} {
		t.Run(name, func(t *testing.T) {
			s, g, set := fixSource(t)
			c := firstCredential(t, s, set)
			component := "task"
			record := false
			candidate := set.Components[0]
			switch name {
			case "absent component":
				component = "sidecar:absent"
				c = nil
			case "foreign source":
				other, _, otherSet := fixSource(t)
				c = firstCredential(t, other, otherSet)
			case "attempt":
				c.AttemptID = testGrantID
			case "component":
				c.Component = "sidecar:other"
			case "closed":
				c.Close()
			case "generation":
				c.Generation = 2
			case "limit":
				c.Generation = 512
			case "frozen evidence":
				set.Components[0].Output.FileSHA256 = strings.Repeat("c", 64)
			case "record evidence":
				record = true
				candidate.Output.SizeBytes++
			case "missing manifest", "negative manifest":
				record = true
				f := reflect.ValueOf(&candidate.Output).Elem().FieldByName("ManifestSize")
				if f.IsValid() {
					if name == "missing manifest" {
						f.SetInt(0)
					} else {
						f.SetInt(-1)
					}
				}
			}
			g.events = nil
			var err error
			if record {
				_, err = s.Record(context.Background(), set, c, candidate)
			} else {
				var next *RegistryCredential
				next, err = s.Next(context.Background(), set, component, c)
				if next != nil {
					next.Close()
				}
			}
			if err == nil || len(g.events) != 0 {
				t.Fatalf("invalid input caused side effects: err=%v events=%v", err, g.events)
			}
		})
	}
}
func TestPublicationCredentialSourceFixCandidateRequest(t *testing.T) {
	for _, field := range []string{"valid", "OperationID", "SessionID", "SessionGeneration", "AttemptNumber", "BuilderID", "CredentialID", "CredentialGeneration", "GrantID", "MaterializationID", "AttemptID", "LeaseEpoch", "Component", "ManifestDigest", "ManifestSize", "OCIFileSHA256", "OCIFileSize", "Platform"} {
		t.Run(field, func(t *testing.T) {
			s, g, set := fixSource(t)
			c := firstCredential(t, s, set)
			if field != "valid" {
				g.ackMutate = func(a *PublicationCandidateAcknowledgement) {
					v := reflect.ValueOf(a).Elem().FieldByName(field)
					if v.Kind() == reflect.String {
						v.SetString(testGrantID)
					} else {
						v.SetInt(v.Int() + 1)
					}
					if field == "GrantID" {
						v.SetString(testAttemptID)
					}
				}
			}
			_, err := s.Record(context.Background(), set, c, set.Components[0])
			if field == "valid" {
				if err != nil {
					t.Fatal(err)
				}
				if g.request.ManifestSize != 321 || g.request.OCIFileSize != 5678 {
					t.Fatalf("fabricated manifest size: %d archive: %d", g.request.ManifestSize, g.request.OCIFileSize)
				}
			} else if err == nil {
				t.Fatal("ack drift accepted")
			}
		})
	}
}
func TestPublicationCredentialSourceFixSessionCriticalSection(t *testing.T) {
	s, g, set := fixSource(t)
	c := firstCredential(t, s, set)
	g.heartbeatHook = func(current *SecretBuffer) {
		if s.session.mu.TryLock() {
			s.session.mu.Unlock()
			t.Error("heartbeat outside session lock")
		}
	}
	g.credentialHook = func(current *SecretBuffer) {
		if s.session.mu.TryLock() {
			s.session.mu.Unlock()
			t.Error("credential outside session lock")
		}
	}
	// A concurrent Record must wait throughout renewal, heartbeat and issuance.
	entered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	recordDone := make(chan struct{})
	g.renewMutate = func(_ *SessionEnvelope) { close(entered); <-release }
	go func() {
		defer close(done)
		n, err := s.Next(context.Background(), set, "task", c)
		if err != nil {
			t.Error(err)
		}
		if n != nil {
			n.Close()
		}
	}()
	<-entered
	go func() { defer close(recordDone); _, _ = s.Record(context.Background(), set, c, set.Components[0]) }()
	select {
	case <-recordDone:
		t.Error("record passed renewal")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	<-done
	<-recordDone
}

func TestPublicationCredentialSourceFixHeartbeatIssuanceCannotInterleave(t *testing.T) {
	s, g, set := fixSource(t)
	c := firstCredential(t, s, set)
	renewed := make(chan struct{})
	var heartbeatSession *SecretBuffer
	g.heartbeatHook = func(current *SecretBuffer) {
		heartbeatSession = current
		go func() { _, _ = s.session.Renew(context.Background()); close(renewed) }()
		// Let the competing renew block on the session mutex long enough to queue.
		time.Sleep(30 * time.Millisecond)
	}
	g.credentialHook = func(current *SecretBuffer) {
		if current != heartbeatSession || heartbeatSession.closed {
			t.Error("session changed between heartbeat and issuance")
		}
	}
	n, err := s.Next(context.Background(), set, "task", c)
	if n != nil {
		n.Close()
	}
	if err != nil {
		t.Error(err)
	}
	<-renewed
}

func TestPublicationCredentialSourceFixSerializesGenerationOne(t *testing.T) {
	s, g, set := fixSource(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	g.credentialHook = func(_ *SecretBuffer) {
		select {
		case <-entered:
		default:
			close(entered)
			<-release
		}
	}
	results := make(chan error, 2)
	acquire := func() {
		c, err := s.Next(context.Background(), set, "task", nil)
		if c != nil {
			c.Close()
		}
		results <- err
	}
	go acquire()
	<-entered
	go acquire()
	close(release)
	a, b := <-results, <-results
	if (a == nil) == (b == nil) {
		t.Fatalf("want one accepted generation one, errors: %v / %v", a, b)
	}
}

func TestRegistryCredentialFixAttemptBounds(t *testing.T) {
	for _, number := range []string{"0", "-1", "1", "9223372036854775807", "9223372036854775808"} {
		t.Run(number, func(t *testing.T) {
			raw := strings.Replace(validRegistryCredentialJSON(registryCredentialMutation{}), `"attempt_number":11`, `"attempt_number":`+number, 1)
			secret := mustSecretBuffer(t, []byte(raw))
			defer secret.Close()
			b := validRegistryCredentialBinding()
			b.AttemptNumber = 0
			c, err := ParseRegistryCredential(secret, b)
			if c != nil {
				c.Close()
			}
			valid := number == "1" || number == "9223372036854775807"
			if (err == nil) != valid {
				t.Fatalf("accepted=%v want=%v", err == nil, valid)
			}
		})
	}
}
func TestPublicationCredentialSourceFixAttemptCaptureIsAtomicAcrossComponents(t *testing.T) {
	s, g, set := fixSource(t)
	sidecar := set.Components[0]
	sidecar.Name = "sidecar:db"
	set.Components = append(set.Components, sidecar)
	g.mutate = func(m *registryCredentialMutation) { m.AttemptNumber = 12; m.BearerToken = "invalid" }
	if c, err := s.Next(context.Background(), set, "task", nil); err == nil {
		c.Close()
		t.Fatal("accepted bad first response")
	}
	if !g.lastSecret.closed {
		t.Fatal("bad first secret retained")
	}
	g.mutate = func(m *registryCredentialMutation) { m.AttemptNumber = 13 }
	c := firstCredential(t, s, set)
	if c.AttemptNumber != 13 {
		t.Fatal("failed parse poisoned attempt discovery")
	}
	g.mutate = func(m *registryCredentialMutation) { m.AttemptNumber = 14 }
	if other, err := s.Next(context.Background(), set, "sidecar:db", nil); err == nil {
		other.Close()
		t.Fatal("different component changed attempt")
	}
	g.mutate = func(m *registryCredentialMutation) { m.AttemptNumber = 13 }
	other, err := s.Next(context.Background(), set, "sidecar:db", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if _, err := s.Record(context.Background(), set, other, sidecar); err != nil {
		t.Fatal(err)
	}
	if g.request.AttemptNumber != 13 {
		t.Fatal("candidate changed attempt")
	}
}
func TestPublicationCredentialSourceFixRejectsMissingFrozenManifestEvidence(t *testing.T) {
	for _, size := range []int64{0, -1} {
		t.Run(strconv.FormatInt(size, 10), func(t *testing.T) {
			s, g, set := fixSource(t)
			set.Components[0].Output.ManifestSize = size
			c := firstCredential(t, s, set)
			g.events = nil
			if _, err := s.Record(context.Background(), set, c, set.Components[0]); err == nil || len(g.events) != 0 {
				t.Fatalf("missing evidence recorded: %v events=%v", err, g.events)
			}
		})
	}
}
func TestPublicationCredentialSourceFixGenerationLimit(t *testing.T) {
	s, g, set := fixSource(t)
	c := firstCredential(t, s, set)
	for i := 2; i <= 512; i++ {
		next, err := s.Next(context.Background(), set, "task", c)
		if err != nil {
			t.Fatalf("generation %d: %v", i, err)
		}
		if !c.secret.closed {
			t.Fatal("predecessor still open")
		}
		c = next
	}
	defer c.Close()
	g.events = nil
	if next, err := s.Next(context.Background(), set, "task", c); err == nil || len(g.events) != 0 {
		if next != nil {
			next.Close()
		}
		t.Fatalf("generation 513 caused side effects: %v events=%v", err, g.events)
	}
	if c.secret.closed {
		t.Fatal("limit closed predecessor")
	}
	if _, err := s.Record(context.Background(), set, c, set.Components[0]); err != nil {
		t.Fatal(err)
	}
}
func TestPublicationCredentialSourceFixSuccessorMustBeNewerThanPredecessor(t *testing.T) {
	s, g, set := fixSource(t)
	s.session.current.Secret.Close()
	s.session.current = testSession(2, time.Now().Add(time.Minute))
	g.generation = 2
	c := firstCredential(t, s, set)
	// Simulate a stale manager restored independently of the publication source.
	s.session.current.Secret.Close()
	s.session.current = testSession(1, time.Now().Add(time.Minute))
	g.generation = 1
	g.events = nil
	next, err := s.Next(context.Background(), set, "task", c)
	if next != nil {
		next.Close()
	}
	if err == nil || c.secret.closed {
		t.Fatal("non-newer session accepted or predecessor lost")
	}
	if strings.Join(g.events, ",") != "renew" {
		t.Fatalf("stale successor allowed issuance: %v", g.events)
	}
}
