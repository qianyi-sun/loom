package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestResourceRequestsStrictDecodePreservesLimitsAndLegacyPlan(t *testing.T) {
	for _, configured := range []bool{false, true} {
		p := isolatedControllerPlan()
		if configured {
			p.ResourceRequests = &executionResourceRequests{
				Controller:  &resources{CPUMillis: 250, MemoryMiB: 512, EphemeralStorageMiB: 1024},
				TaskSandbox: &resources{CPUMillis: 500, MemoryMiB: 1024, EphemeralStorageMiB: 2048},
			}
		}
		body, err := json.Marshal(p)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(body), "resource_requests") != configured {
			t.Fatal("default serialization added requests")
		}
		if configured && strings.Contains(string(body), `"verifier_sandbox"`) {
			t.Fatal("unconfigured request roles must be omitted")
		}
		loaded, err := decodePlan(body)
		if err != nil {
			t.Fatal(err)
		}
		if loaded.TaskResources != p.TaskResources || *loaded.ControllerResources != *p.ControllerResources {
			t.Fatal("request configuration changed limits")
		}
		if configured && (*loaded.ResourceRequests.Controller != *p.ResourceRequests.Controller || loaded.ResourceRequests.VerifierSandbox != nil) {
			t.Fatal("request override was not preserved")
		}
	}
}

func TestResourceRequestsRejectOversizeEmptyAndUnsupportedPlan(t *testing.T) {
	cases := []struct {
		name   string
		change func(*plan)
		reason string
	}{
		{"controller cpu", func(p *plan) { p.ResourceRequests.Controller.CPUMillis = 1001 }, "exceed hard limits"},
		{"task memory", func(p *plan) { p.ResourceRequests.TaskSandbox = &resources{1, 16385, 1} }, "exceed hard limits"},
		{"verifier storage", func(p *plan) { p.ResourceRequests.VerifierSandbox = &resources{1, 1, 10241} }, "exceed hard limits"},
		{"zero", func(p *plan) { p.ResourceRequests.Controller.CPUMillis = 0 }, "invalid resource requests"},
		{"empty", func(p *plan) { p.ResourceRequests = &executionResourceRequests{} }, "at least one"},
		{"unisolated", func(p *plan) { p.ControllerResources = nil; p.Sidecars = nil }, "isolated attempt controller"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := isolatedControllerPlan()
			p.ResourceRequests = &executionResourceRequests{Controller: &resources{250, 512, 1024}}
			c.change(&p)
			body, err := json.Marshal(p)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := decodePlan(body); err == nil || !strings.Contains(err.Error(), c.reason) {
				t.Fatalf("wanted %s, got %v", c.reason, err)
			}
		})
	}
}
