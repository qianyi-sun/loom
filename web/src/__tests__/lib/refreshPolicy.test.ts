import { describe, expect, it } from "vitest";

import { calculateRefreshPolicy } from "../../lib/refreshPolicy";

describe("calculateRefreshPolicy", () => {
  it("returns the base interval when visible, focused, and active", () => {
    const decision = calculateRefreshPolicy({
      baseIntervalMs: 5_000,
      visible: true,
      focused: true,
    });
    expect(decision.refetchInterval).toBe(5_000);
    expect(decision.reason).toBe("active");
  });

  it("pauses entirely when manually paused", () => {
    const decision = calculateRefreshPolicy({
      baseIntervalMs: 5_000,
      manualPaused: true,
    });
    expect(decision.refetchInterval).toBe(false);
    expect(decision.reason).toBe("paused");
  });

  it("pauses when hidden if hiddenBehavior=pause (the default)", () => {
    const decision = calculateRefreshPolicy({
      baseIntervalMs: 5_000,
      visible: false,
    });
    expect(decision.refetchInterval).toBe(false);
    expect(decision.reason).toBe("hidden");
  });

  it("slows polling when hidden if hiddenBehavior=slow", () => {
    const decision = calculateRefreshPolicy({
      baseIntervalMs: 5_000,
      visible: false,
      hiddenBehavior: "slow",
    });
    expect(decision.refetchInterval).toBe(20_000);
    expect(decision.reason).toBe("hidden");
  });

  it("slows polling on blur by default", () => {
    const decision = calculateRefreshPolicy({
      baseIntervalMs: 5_000,
      visible: true,
      focused: false,
    });
    expect(decision.refetchInterval).toBe(10_000);
    expect(decision.reason).toBe("blurred");
  });

  it("clamps to maxIntervalMs even when behaviour rules would slow further", () => {
    const decision = calculateRefreshPolicy({
      baseIntervalMs: 5_000,
      visible: false,
      hiddenBehavior: "slow",
      maxIntervalMs: 8_000,
    });
    expect(decision.refetchInterval).toBe(8_000);
  });

  it("respects minIntervalMs even when freshness=stale", () => {
    const decision = calculateRefreshPolicy({
      baseIntervalMs: 5_000,
      minIntervalMs: 3_000,
      freshness: "stale",
    });
    expect(decision.refetchInterval).toBe(3_000);
    expect(decision.reason).toBe("stale");
  });

  it("slows for freshness=fresh", () => {
    const decision = calculateRefreshPolicy({
      baseIntervalMs: 5_000,
      freshness: "fresh",
    });
    expect(decision.refetchInterval).toBe(10_000);
    expect(decision.reason).toBe("fresh");
  });

  it("manualPaused wins over freshness/visibility", () => {
    const decision = calculateRefreshPolicy({
      baseIntervalMs: 5_000,
      manualPaused: true,
      visible: false,
      freshness: "stale",
    });
    expect(decision.refetchInterval).toBe(false);
    expect(decision.reason).toBe("paused");
  });
});
