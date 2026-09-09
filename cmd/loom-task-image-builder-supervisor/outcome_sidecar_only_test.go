package main

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

// Break caught: a plan with a prebuilt task image and only Dockerfile-backed
// sidecars is rejected before its first registry upload.
func TestRegistryPublicationHandoffAcceptsCanonicalSidecarOnlyBuiltSet(t *testing.T) {
	set := handoffBuiltSet()
	cache := set.Components[0]
	cache.Name = "sidecar:cache"
	set.Components = []BuiltComponent{cache, set.Components[1]}
	var events []string
	guard := newHandoffCredentialGuard(t, &events)
	source := NewPublicationCredentialSource(
		NewSessionManager(testGrantID, testSession(1, time.Now().Add(time.Minute)), guard),
		guard,
		validPublicationAttemptBinding(),
	)
	handoff := newTestRegistryPublicationHandoff(&handoffUploader{events: &events})

	err := handoff.AcceptWithCredentials(context.Background(), set, source)
	if !errors.Is(err, ErrPublicationVerificationUnavailable) {
		t.Fatalf("AcceptWithCredentials() error = %v, want ErrPublicationVerificationUnavailable", err)
	}
	wantEvents := []string{
		"credential:sidecar:cache:1",
		"manifest-ack:sidecar:cache",
		"candidate:sidecar:cache",
		"credential:sidecar:db:1",
		"manifest-ack:sidecar:db",
		"candidate:sidecar:db",
	}
	if !reflect.DeepEqual(events, wantEvents) {
		t.Fatalf("events = %#v, want canonical sidecar-only publication %#v", events, wantEvents)
	}
	guard.wantAllSecretsClosed(t)
}

func TestValidatePublicationBuiltSetRejectsEmptyDuplicateOrNoncanonicalComponents(t *testing.T) {
	base := handoffBuiltSet()
	task := base.Components[0]
	cache := task
	cache.Name = "sidecar:cache"
	database := base.Components[1]

	for _, tc := range []struct {
		name       string
		components []BuiltComponent
		want       string
	}{
		{name: "empty", components: nil, want: "registry publication set invalid"},
		{name: "duplicate", components: []BuiltComponent{cache, cache}, want: "registry publication component order invalid"},
		{name: "task after sidecar", components: []BuiltComponent{cache, task}, want: "registry publication component order invalid"},
		{name: "unordered sidecars", components: []BuiltComponent{database, cache}, want: "registry publication component order invalid"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			set := base
			set.Components = tc.components
			if err := validatePublicationBuiltSet(set); err == nil || err.Error() != tc.want {
				t.Fatalf("validatePublicationBuiltSet() error = %v, want %q", err, tc.want)
			}
		})
	}
}
