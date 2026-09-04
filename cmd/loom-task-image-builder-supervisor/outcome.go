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
