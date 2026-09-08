package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

type publicationUploadFunc func(context.Context, OCIOutput, RegistryUploadCredentialSource) (UploadedManifest, error)

func (f publicationUploadFunc) Upload(ctx context.Context, output OCIOutput, source RegistryUploadCredentialSource) (UploadedManifest, error) {
	return f(ctx, output, source)
}

type retainingCandidateGuard struct {
	*handoffCredentialGuard
	last *PublicationCandidateV2Acknowledgement
	duplicateID bool
}

func (g *retainingCandidateGuard) PublicationCandidateV2(ctx context.Context, request PublicationCandidateV2Request, current *SecretBuffer) (*PublicationCandidateV2Acknowledgement, error) {
	ack, err := g.handoffCredentialGuard.PublicationCandidateV2(ctx, request, current)
	if ack != nil && g.duplicateID { ack.CandidateID = "99999999-9999-4999-8999-999999999999" }
	g.last = ack
	return ack, err
}

func TestPublicationUploadRetainsCompleteOwnedV2Acknowledgements(t *testing.T) {
	for _, sidecarsOnly := range []bool{false, true} {
		t.Run(map[bool]string{false:"task-and-sidecar", true:"sidecar-only"}[sidecarsOnly], func(t *testing.T) {
			set := handoffBuiltSet()
			if sidecarsOnly { set.Components[0].Name = "sidecar:cache" }
			var events []string
			guard := &retainingCandidateGuard{handoffCredentialGuard:newHandoffCredentialGuard(t, &events)}
			manager := NewSessionManager(testGrantID, testSession(1, time.Now().Add(time.Minute)), guard)
			defer manager.Close()
			source := NewPublicationCredentialSource(manager, guard, validPublicationAttemptBinding())
			base := &handoffUploader{events:&events, renewComponents:map[string]bool{set.Components[0].Name:true}}
			uploader := publicationUploadFunc(func(ctx context.Context, output OCIOutput, credentials RegistryUploadCredentialSource) (UploadedManifest, error) {
				manifest, err := base.Upload(ctx, output, credentials)
				if guard.last != nil {
					// Mutate the adapter's returned pointer after its synchronous
					// callback. The upload result must already own a value copy.
					guard.last.CandidateID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
					guard.last.BaseResolution = BaseResolutionEvidence{}
				}
				return manifest, err
			})
			acks, err := newTestRegistryPublicationHandoff(uploader).UploadWithCredentials(context.Background(), set, source)
			if err != nil || len(acks) != len(set.Components) { t.Fatalf("count=%d err=%v", len(acks), err) }
			for index, ack := range acks {
				request := guard.candidateRequests[index]
				if ack.CandidateID != uuidWithTail(0x9000+index+1) || ack.Component != set.Components[index].Name ||
					ack.OperationID != request.OperationID || ack.CredentialID != request.CredentialID ||
					ack.SessionGeneration != request.SessionGeneration || ack.SessionGeneration != 2 ||
					ack.ManifestDigest != set.Components[index].Output.TopLevelDigest ||
					ack.BaseResolution != set.Components[index].BaseResolution || ack.AuthorityResponseSHA256 != strings.Repeat("c",64) {
					t.Fatal("complete validated acknowledgement lost or aliased")
				}
			}
			guard.wantAllSecretsClosed(t)
		})
	}
}

func TestPublicationUploadCannotReturnCompleteSetWithoutEveryDistinctCandidate(t *testing.T) {
	for _, condition := range []string{"missing-callback", "duplicate-id", "second-upload-fails", "cancelled"} {
		t.Run(condition, func(t *testing.T) {
			set := handoffBuiltSet()
			var events []string
			guard := &retainingCandidateGuard{handoffCredentialGuard:newHandoffCredentialGuard(t, &events), duplicateID:condition=="duplicate-id"}
			manager := NewSessionManager(testGrantID, testSession(1, time.Now().Add(time.Minute)), guard)
			defer manager.Close()
			source := NewPublicationCredentialSource(manager, guard, validPublicationAttemptBinding())
			base := &handoffUploader{events:&events}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if condition == "second-upload-fails" { base.failBeforeManifest = "sidecar:db" }
			if condition == "cancelled" { base.cancelAfterManifest = cancel }
			uploader := publicationUploadFunc(func(ctx context.Context, output OCIOutput, credentials RegistryUploadCredentialSource) (UploadedManifest, error) {
				if condition == "missing-callback" { return UploadedManifest{Digest:output.TopLevelDigest, MediaType:output.ManifestMediaType, Size:output.ManifestSize}, nil }
				return base.Upload(ctx, output, credentials)
			})
			acks, err := newTestRegistryPublicationHandoff(uploader).UploadWithCredentials(ctx, set, source)
			if err == nil || acks != nil { t.Fatalf("partial/unacknowledged set returned: count=%d err=%v", len(acks), err) }
			if condition == "cancelled" && !errors.Is(err, context.Canceled) { t.Fatal("cancellation lost") }
			if condition != "missing-callback" { guard.wantAllSecretsClosed(t) }
		})
	}
}
