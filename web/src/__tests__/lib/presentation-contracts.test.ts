import { describe, expect, it } from "vitest";

import { agentReadinessMessage, agentServiceModeReady } from "../../lib/agentReadiness";
import { helpForState, humanizeState } from "../../lib/helpText";
import { modelLabel } from "../../lib/modelLabel";
import { ownershipLabel, ownershipSearchText } from "../../lib/ownership";
import { allowedModelsSummary, providerStatusSummary } from "../../lib/providerDisplay";

describe("presentation contracts", () => {
  it("normalizes known states and provider-specific labels", () => {
    expect(helpForState(" Partial Failed ")).toContain("Some trials failed");
    expect(helpForState(123)).toBeUndefined();
    expect(helpForState({})).toBeUndefined();
    expect(humanizeState("provider", "valid").label).toBe("Ready");
    expect(humanizeState("provider", "invalid").label).toBe("Needs attention");
    expect(humanizeState("provider", "untested").label).toBe("Untested");
    expect(humanizeState("batch", "partial_failed").label).toBe("partial failed");
    expect(humanizeState("generic", null).state).toBe("unknown");
  });

  it("formats scalar and structured model values", () => {
    expect(modelLabel(null)).toBe("—");
    expect(modelLabel(42)).toBe("—");
    expect(modelLabel("")).toBe("—");
    expect(modelLabel("gpt-4o")).toBe("gpt-4o");
    expect(modelLabel({ provider: "openai", name: "gpt-4o" })).toBe("openai/gpt-4o");
    expect(modelLabel({ provider: 7, name: "gpt-4o" })).toBe("gpt-4o");
    expect(modelLabel({ provider: "openai", name: 7 })).toBe("—");
  });

  it("uses the most specific ownership fields and builds search text", () => {
    const complete = {
      submitted_by_user: { id: "u1", username: " Ada ", team_name: "Research" },
      owner_team: { id: "t1", name: "Owner fallback" },
      team_name: "Row fallback",
      team_id: "team-id",
    };
    expect(ownershipLabel(complete)).toBe("Ada / Research");
    expect(ownershipSearchText(complete)).toBe(
      " ada  research owner fallback row fallback team-id",
    );
    expect(ownershipLabel({ submitted_by_user: { id: "u1", username: "Ada" } })).toBe("Ada");
    expect(ownershipLabel({ owner_team: { id: "t1", name: "Owner" } })).toBe("Owner");
    expect(ownershipLabel({ team_name: "Row Team" })).toBe("Row Team");
    expect(ownershipLabel({ team_id: "team-id" })).toBe("team-id");
    expect(ownershipLabel({
      submitted_by_user: { id: "u1", username: "   " },
      team_name: "Whitespace fallback",
    })).toBe("Whitespace fallback");
    expect(ownershipLabel({})).toBe("-");
  });

  it("describes agent and provider readiness fallbacks", () => {
    expect(agentServiceModeReady({ name: "ready" })).toBe(true);
    expect(agentServiceModeReady({ name: "blocked", service_mode_ready: false })).toBe(false);
    expect(agentReadinessMessage({ name: "agent", readiness_message: "Explicit" })).toBe("Explicit");
    expect(agentReadinessMessage({ name: "agent", runtime_contract: { install_hint: "Install it" } })).toBe("Install it");
    expect(agentReadinessMessage({ name: "agent" })).toContain("Agent agent needs");
    expect(agentReadinessMessage({
      name: "agent",
      readiness_message: null,
      runtime_contract: null,
    })).toContain("Agent agent needs");
    expect(providerStatusSummary("valid").label).toBe("Ready");
    expect(providerStatusSummary("invalid").label).toBe("Needs attention");
    expect(providerStatusSummary(null).label).toBe("Untested");
    expect(allowedModelsSummary(undefined).label).toBe("All discovered models");
    expect(allowedModelsSummary(null).label).toBe("All discovered models");
    expect(allowedModelsSummary([]).label).toBe("No allowed models");
    expect(allowedModelsSummary(["one"]).label).toBe("1 allowed model");
    expect(allowedModelsSummary(["one", "two"]).label).toBe("2 allowed models");
  });
});
