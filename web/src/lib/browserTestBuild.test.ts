import { describe, expect, it } from "vitest";

import {
  clearBrowserTestRecoveryFault,
  IS_BROWSER_TEST_BUILD,
  shouldTriggerBrowserTestRecoveryFault,
} from "./browserTestBuild";

describe("browser test build marker", () => {
  it("is disabled in the normal production and unit-test build", () => {
    expect(IS_BROWSER_TEST_BUILD).toBe(false);
    expect(
      shouldTriggerBrowserTestRecoveryFault("root-render-once"),
    ).toBe(false);
    expect(() =>
      clearBrowserTestRecoveryFault("root-render-once"),
    ).not.toThrow();
  });
});
