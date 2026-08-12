import { describe, expect, test } from "vitest";

import {
  bytewiseCompare,
  formatMicrousd,
  pipelineResultPresentation,
  truncateNfcUtf8,
} from "../../lib/pipelinePresentation";

describe("pipeline presentation helpers", () => {
  test("presents pending and terminal results", () => {
    expect(pipelineResultPresentation(null).label).toBe("Pending");
    expect(pipelineResultPresentation("failed").variant).toBe("failed");
  });

  test("formats signed micros exactly", () => {
    expect(formatMicrousd(1_000_001)).toBe("$1.000001");
    expect(formatMicrousd(-42)).toBe("-$0.000042");
  });

  test("compares UTF-8 bytewise including prefixes", () => {
    expect(bytewiseCompare("a", "b")).toBeLessThan(0);
    expect(bytewiseCompare("ab", "a")).toBeGreaterThan(0);
    expect(bytewiseCompare("same", "same")).toBe(0);
  });

  test("normalizes and truncates without splitting Unicode characters", () => {
    expect(truncateNfcUtf8("e\u0301", 8)).toBe("é");
    expect(truncateNfcUtf8("a😀b", 5)).toBe("a😀");
    expect(truncateNfcUtf8("abc", 0)).toBe("");
  });
});
