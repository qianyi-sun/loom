package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func timeoutVerifierContractPlan() plan {
	p := isolatedControllerPlan()
	p.ControllerResources = nil
	p.VerifierExecution = "in_attempt"
	p.Verifier = &phase{Role: "verifier", Argv: []string{"/bin/true"}, WorkingDirectory: "/app", TimeoutSeconds: 120}
	p.VerifierAfterAgentTimeout = true
	return p
}

func TestTimeoutVerificationOptInRoundTripAndLegacyPlan(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		p := timeoutVerifierContractPlan()
		p.VerifierAfterAgentTimeout = enabled
		body, err := json.Marshal(p)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(body), `"verifier_after_agent_timeout"`) != enabled {
			t.Fatalf("default serialization changed or opt-in was lost: %s", body)
		}
		loaded, err := decodePlan(body)
		if err != nil {
			t.Fatal(err)
		}
		if loaded.VerifierAfterAgentTimeout != enabled || loaded.Main.TimeoutSeconds != 60 || loaded.Verifier.TimeoutSeconds != 120 {
			t.Fatal("strict plan decoding changed timeout verification or phase deadlines")
		}
	}
}

func TestTimeoutVerificationRejectsUnsafeTopology(t *testing.T) {
	cases := []struct {
		name   string
		change func(*plan)
		reason string
	}{
		{"precomposed", func(p *plan) { p.Composition = "precomposed" }, "isolated attempt controller"},
		{"missing controller", func(p *plan) { p.AgentImageRef = nil }, "agent image reference"},
		{"no sandboxes", func(p *plan) { p.Sidecars = nil }, "isolated attempt controller"},
		{"missing task sandbox", func(p *plan) { p.Sidecars = p.Sidecars[1:] }, "isolated attempt controller"},
		{"missing verifier sandbox", func(p *plan) { p.Sidecars = p.Sidecars[:1] }, "isolated attempt controller"},
		{"public task sandbox", func(p *plan) { p.Sidecars[0].PrivateSandbox = false }, "private mounts"},
		{"public verifier sandbox", func(p *plan) { p.Sidecars[1].PrivateSandbox = false }, "private mounts"},
		{"skipped verifier", func(p *plan) { p.VerifierExecution = "skipped"; p.Verifier = nil }, "in-attempt verifier"},
		{"separate verifier", func(p *plan) { p.VerifierExecution = "separate_execution"; p.Verifier = nil }, "in-attempt verifier"},
		{"missing verifier", func(p *plan) { p.Verifier = nil }, "in-attempt verifier is missing"},
		{"verifier execution unit", func(p *plan) {
			p.ExecutionRole = "verifier"
			p.Main.Role = "verifier"
			p.VerifierExecution = "skipped"
			p.Verifier = nil
		}, "isolated attempt controller"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := timeoutVerifierContractPlan()
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
