package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func preparedTaskPlan() plan {
	p := testPlan("/workspace", phase{
		Role: "agent", Argv: []string{"/bin/true"}, WorkingDirectory: "/workspace", TimeoutSeconds: 1,
	})
	id := "11111111-2222-3333-4444-555555555555"
	agentImage := "registry/agent@sha256:" + strings.Repeat("b", 64)
	p.TaskImageMaterializationID = &id
	p.AgentImageRef = &agentImage
	p.ImageAdmission.Admissions[0].Statement.ImageRef = agentImage
	ready := probe{Kind: "exec", Argv: []string{"/bin/true"}, TimeoutSeconds: 1, PeriodSeconds: 1, FailureThreshold: 1}
	p.Sidecars = []sidecar{{
		RoleName: "task-sandbox", PrivateSandbox: true, ImageRef: p.TaskImageRef,
		Argv: []string{"/bin/true"}, Resources: p.TaskResources, StartupProbe: ready, ReadinessProbe: ready,
	}}
	verifier := p.Sidecars[0]
	verifier.RoleName = "verifier-sandbox"
	p.Sidecars = append(p.Sidecars, verifier)
	return p
}

func TestDecodePlanSupportsPreparedImageAndLegacyOmission(t *testing.T) {
	prepared := preparedTaskPlan()
	legacy := testPlan("/workspace", prepared.Main)
	for name, p := range map[string]plan{"prepared": prepared, "legacy": legacy} {
		t.Run(name, func(t *testing.T) {
			payload, err := json.Marshal(p)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(payload), "task_image_materialization_id") != (name == "prepared") {
				t.Fatalf("unexpected optional materialization field: %s", payload)
			}
			decoded, err := decodePlan(payload)
			if err != nil {
				t.Fatal(err)
			}
			if name == "prepared" && (decoded.TaskImageMaterializationID == nil || *decoded.TaskImageMaterializationID != *p.TaskImageMaterializationID) {
				t.Fatal("materialization identity was lost during decoding")
			}
		})
	}
}

func TestPreparedImageRequiresValidControllerBinding(t *testing.T) {
	for _, invalid := range []string{"", "not-a-uuid", "00000000-0000-0000-0000-000000000000", "11111111-2222-3333-4444-55555555555z"} {
		t.Run("uuid="+invalid, func(t *testing.T) {
			p := preparedTaskPlan()
			p.TaskImageMaterializationID = &invalid
			if err := p.validate(); err == nil || !strings.Contains(err.Error(), "materialization UUID") {
				t.Fatalf("invalid materialization UUID accepted: %v", err)
			}
		})
	}
	for name, mutate := range map[string]func(*plan){
		"missing agent": func(p *plan) { p.AgentImageRef = nil },
		"empty agent":   func(p *plan) { empty := ""; p.AgentImageRef = &empty },
		"verifier role": func(p *plan) { p.ExecutionRole = "verifier"; p.Main.Role = "verifier" },
		"precomposed":   func(p *plan) { p.Composition = "precomposed" },
	} {
		t.Run(name, func(t *testing.T) {
			p := preparedTaskPlan()
			mutate(&p)
			if err := p.validate(); err == nil {
				t.Fatal("invalid prepared-image controller binding accepted")
			}
		})
	}
}

func TestPreparedImageDoesNotExemptTrustedOrOtherSidecarImages(t *testing.T) {
	for name, mutate := range map[string]func(*plan){
		"missing runtime admission": func(p *plan) { p.ImageAdmission.Admissions = p.ImageAdmission.Admissions[:1] },
		"missing agent admission":   func(p *plan) { p.ImageAdmission.Admissions = p.ImageAdmission.Admissions[1:] },
		"missing materialization":   func(p *plan) { p.TaskImageMaterializationID = nil },
		"different private image":   func(p *plan) { p.Sidecars[0].ImageRef = "registry/other@sha256:" + strings.Repeat("c", 64) },
		"same image public sidecar": func(p *plan) { p.Sidecars[0].PrivateSandbox = false; p.Sidecars[0].RoleName = "database" },
		"prepared image as controller": func(p *plan) {
			p.AgentImageRef = &p.TaskImageRef
			p.ImageAdmission.Admissions = p.ImageAdmission.Admissions[1:]
		},
	} {
		t.Run(name, func(t *testing.T) {
			p := preparedTaskPlan()
			mutate(&p)
			if err := p.validate(); err == nil || !strings.Contains(err.Error(), "coverage") {
				t.Fatalf("missing publication admission accepted: %v", err)
			}
		})
	}
	t.Run("different private image with its own admission", func(t *testing.T) {
		p := preparedTaskPlan()
		p.Sidecars[0].ImageRef = "registry/other@sha256:" + strings.Repeat("c", 64)
		admission := p.ImageAdmission.Admissions[0]
		admission.Statement.ImageRef = p.Sidecars[0].ImageRef
		p.ImageAdmission.Admissions = append(p.ImageAdmission.Admissions, admission)
		if err := p.validate(); err != nil {
			t.Fatalf("fully admitted other sandbox rejected: %v", err)
		}
	})
}
