package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func nodeSharePlan() plan {
	p := isolatedControllerPlan()
	p.NodeResourceAllocation = &nodeResourceAllocation{
		Policy: "node-share-v1", TargetID: "pool-a", BaselineSlots: 16,
		UsableNode:   resources{CPUMillis: 16000, MemoryMiB: 245760, EphemeralStorageMiB: 524288},
		DeclaredTask: resources{CPUMillis: 1000, MemoryMiB: 4096, EphemeralStorageMiB: 2048},
	}
	p.TaskResources = resources{CPUMillis: 1000, MemoryMiB: 7168, EphemeralStorageMiB: 15360}
	p.ControllerResources = &resources{CPUMillis: 1000, MemoryMiB: 1024, EphemeralStorageMiB: 2048}
	for i := range p.Sidecars {
		p.Sidecars[i].Resources = p.TaskResources
	}
	task := p.TaskResources
	p.ResourceRequests = &executionResourceRequests{Controller: p.ControllerResources, TaskSandbox: &task, VerifierSandbox: &task}
	return p
}

func TestNodeShareStrictDecodeAndMemoryReservations(t *testing.T) {
	p := nodeSharePlan()
	encoded, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := decodePlan(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if *loaded.NodeResourceAllocation != *p.NodeResourceAllocation || loaded.TaskResources.MemoryMiB != 7168 {
		t.Fatal("node allocation changed during strict runtime decoding")
	}
	for _, mutate := range []func(*plan){
		func(p *plan) { p.ResourceRequests.TaskSandbox.MemoryMiB = 4096 },
		func(p *plan) { p.NodeResourceAllocation.DeclaredTask.MemoryMiB = 12288 },
		func(p *plan) { p.NodeResourceAllocation.BaselineSlots = 32 },
	} {
		bad := nodeSharePlan()
		mutate(&bad)
		if err := bad.validate(); err == nil || !strings.Contains(err.Error(), "node allocation") {
			t.Fatalf("invalid allocation accepted: %v", err)
		}
	}
}
