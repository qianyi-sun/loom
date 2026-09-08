package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
)

type BuildOutcomeStatus string

const (
	BuildOutcomeBuilt                BuildOutcomeStatus = "built"
	BuildOutcomeDeterministicFailure BuildOutcomeStatus = "deterministic_failure"
	BuildOutcomeTransientFailure     BuildOutcomeStatus = "transient_failure"
	BuildOutcomeContainmentFailure   BuildOutcomeStatus = "containment_failure"
	BuildOutcomeLeaseLost            BuildOutcomeStatus = "lease_lost"
	BuildOutcomeCancelled            BuildOutcomeStatus = "cancelled"
)

var reasonCodePattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)

type BuiltComponent struct {
	Name   string
	Output OCIOutput
}

type BuiltComponentSet struct {
	GrantID           string
	MaterializationID string
	AttemptID         string
	LeaseEpoch        int
	Components        []BuiltComponent
}

type BuildOutcome struct {
	Status           BuildOutcomeStatus
	Reason           string
	Component        string
	Components       []BuiltComponent
	Cleanup          map[string]int
	ResourceCounters map[string]int
}

type buildOutcomeWire struct {
	Status           BuildOutcomeStatus   `json:"status"`
	Reason           string               `json:"reason"`
	Component        string               `json:"component,omitempty"`
	Components       []builtComponentWire `json:"components,omitempty"`
	Cleanup          map[string]int       `json:"cleanup,omitempty"`
	ResourceCounters map[string]int       `json:"resource_counters,omitempty"`
}

type builtComponentWire struct {
	Name           string `json:"name"`
	TopLevelDigest string `json:"top_level_digest"`
	FileSHA256     string `json:"file_sha256"`
	SizeBytes      int64  `json:"size_bytes"`
	OS             string `json:"os"`
	Architecture   string `json:"architecture"`
}

func (o BuildOutcome) MarshalJSON() ([]byte, error) {
	if !validBuildOutcomeStatus(o.Status) {
		return nil, fmt.Errorf("build outcome status invalid: %s", o.Status)
	}
	if !reasonCodePattern.MatchString(o.Reason) {
		return nil, errors.New("build outcome reason invalid")
	}
	components := make([]builtComponentWire, 0, len(o.Components))
	for _, component := range o.Components {
		if component.Name == "" || !isDigest(component.Output.FileSHA256) || component.Output.SizeBytes < 0 {
			return nil, errors.New("build outcome component evidence invalid")
		}
		if _, err := parseSHA256Descriptor(component.Output.TopLevelDigest); err != nil {
			return nil, errors.New("build outcome component digest invalid")
		}
		components = append(components, builtComponentWire{
			Name:           component.Name,
			TopLevelDigest: component.Output.TopLevelDigest,
			FileSHA256:     component.Output.FileSHA256,
			SizeBytes:      component.Output.SizeBytes,
			OS:             component.Output.OS,
			Architecture:   component.Output.Architecture,
		})
	}
	return json.Marshal(buildOutcomeWire{
		Status:           o.Status,
		Reason:           o.Reason,
		Component:        o.Component,
		Components:       components,
		Cleanup:          o.Cleanup,
		ResourceCounters: o.ResourceCounters,
	})
}

func validBuildOutcomeStatus(status BuildOutcomeStatus) bool {
	switch status {
	case BuildOutcomeBuilt,
		BuildOutcomeDeterministicFailure,
		BuildOutcomeTransientFailure,
		BuildOutcomeContainmentFailure,
		BuildOutcomeLeaseLost,
		BuildOutcomeCancelled:
		return true
	default:
		return false
	}
}

type PublicationHandoff interface {
	Accept(context.Context, BuiltComponentSet) error
}

type CredentialedPublicationHandoff interface {
	PublicationHandoff
	PublicationRegistryExpectation() PublicationRegistryExpectation
	AcceptWithCredentials(context.Context, BuiltComponentSet, *PublicationCredentialSource) error
}

type PublicationRegistryExpectation struct {
	RegistryOrigin  string
	RegistryService string
	RegistryIssuer  string
	RegistryKeyID   string
}

type registryPublicationUploader interface {
	Upload(context.Context, OCIOutput, RegistryUploadCredentialSource) (UploadedManifest, error)
}

var ErrPublicationVerificationUnavailable = errors.New("publication_verification_unavailable")

type RegistryPublicationHandoff struct {
	uploader    registryPublicationUploader
	expectation PublicationRegistryExpectation
}

func NewRegistryPublicationHandoff(policy RegistryUploadPolicy, registryIssuer string, registryKeyID string) (*RegistryPublicationHandoff, error) {
	uploader, err := NewOCIRegistryUploader(policy)
	if err != nil {
		return nil, err
	}
	if !registryIdentityPattern.MatchString(registryIssuer) || !registryKeyIDPattern.MatchString(registryKeyID) {
		return nil, errors.New("registry publication expectation invalid")
	}
	return &RegistryPublicationHandoff{
		uploader: uploader,
		expectation: PublicationRegistryExpectation{
			RegistryOrigin:  policy.origin.String(),
			RegistryService: policy.service,
			RegistryIssuer:  registryIssuer,
			RegistryKeyID:   registryKeyID,
		},
	}, nil
}

func (h *RegistryPublicationHandoff) PublicationRegistryExpectation() PublicationRegistryExpectation {
	if h == nil {
		return PublicationRegistryExpectation{}
	}
	return h.expectation
}

func (h *RegistryPublicationHandoff) Accept(context.Context, BuiltComponentSet) error {
	return errors.New("registry publication credential source unavailable")
}

func (h *RegistryPublicationHandoff) AcceptWithCredentials(ctx context.Context, set BuiltComponentSet, source *PublicationCredentialSource) error {
	if h == nil || h.uploader == nil || source == nil || ctx == nil {
		return errors.New("registry publication handoff invalid")
	}
	if err := validatePublicationBuiltSet(set); err != nil {
		return err
	}
	for _, component := range set.Components {
		if err := ctx.Err(); err != nil {
			return err
		}
		adapter := publicationUploadCredentialSource{
			set:       set,
			component: component,
			source:    source,
		}
		if _, err := h.uploader.Upload(ctx, component.Output, &adapter); err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			return errors.New("registry publication upload failed")
		}
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	return ErrPublicationVerificationUnavailable
}

type publicationUploadCredentialSource struct {
	set       BuiltComponentSet
	component BuiltComponent
	source    *PublicationCredentialSource
}

func (s *publicationUploadCredentialSource) Next(ctx context.Context, predecessor *RegistryCredential) (*RegistryCredential, error) {
	if s == nil || s.source == nil {
		return nil, errors.New("registry publication credential adapter invalid")
	}
	return s.source.Next(ctx, s.set, s.component.Name, predecessor)
}

func (s *publicationUploadCredentialSource) UploadSucceeded(ctx context.Context, manifest UploadedManifest, credential *RegistryCredential) error {
	if s == nil || s.source == nil || credential == nil {
		return errors.New("registry publication candidate adapter invalid")
	}
	expectedRepository, err := publicationRepositoryForOutput(s.set.AttemptID, s.component)
	if err != nil {
		return err
	}
	if manifest.Repository != expectedRepository ||
		manifest.Digest != s.component.Output.TopLevelDigest ||
		manifest.MediaType != s.component.Output.ManifestMediaType ||
		manifest.Size != s.component.Output.ManifestSize ||
		credential.Repository != expectedRepository ||
		credential.Component != s.component.Name ||
		credential.AttemptID != s.set.AttemptID {
		return errors.New("registry publication manifest acknowledgement invalid")
	}
	_, err = s.source.Record(ctx, s.set, credential, s.component)
	return err
}

func (s *publicationUploadCredentialSource) Close(credential *RegistryCredential) {
	if s == nil || s.source == nil {
		if credential != nil {
			credential.Close()
		}
		return
	}
	s.source.Close(credential)
}

func validatePublicationBuiltSet(set BuiltComponentSet) error {
	if !isCanonicalNonZeroUUID(set.GrantID) ||
		!isCanonicalNonZeroUUID(set.MaterializationID) ||
		!isCanonicalNonZeroUUID(set.AttemptID) ||
		set.LeaseEpoch <= 0 ||
		len(set.Components) == 0 ||
		len(set.Components) > 128 {
		return errors.New("registry publication set invalid")
	}
	previousSidecar := ""
	for index, component := range set.Components {
		if !componentPattern.MatchString(component.Name) {
			return errors.New("registry publication component invalid")
		}
		if index == 0 {
			if component.Name != "task" {
				return errors.New("registry publication component order invalid")
			}
		} else {
			if component.Name == "task" || component.Name <= previousSidecar {
				return errors.New("registry publication component order invalid")
			}
			previousSidecar = component.Name
		}
		if _, err := publicationRepositoryForOutput(set.AttemptID, component); err != nil {
			return err
		}
		if parsePublicationManifestDigest(component.Output.TopLevelDigest) != nil ||
			component.Output.ManifestSize <= 0 ||
			component.Output.ManifestMediaType != ociManifestMediaType ||
			!isDigest(component.Output.FileSHA256) ||
			component.Output.SizeBytes <= 0 {
			return errors.New("registry publication component evidence invalid")
		}
	}
	return nil
}

func publicationRepositoryForOutput(attemptID string, component BuiltComponent) (string, error) {
	cpuArch, err := publicationCPUArchFromOCI(component.Output)
	if err != nil {
		return "", err
	}
	return publicationRepository(cpuArch, attemptID, component.Name)
}

func publicationCPUArchFromOCI(output OCIOutput) (string, error) {
	if output.OS != "linux" {
		return "", errors.New("registry publication platform invalid")
	}
	switch output.Architecture {
	case "amd64":
		return "x86_64", nil
	case "arm64":
		return "arm64", nil
	default:
		return "", errors.New("registry publication architecture invalid")
	}
}
