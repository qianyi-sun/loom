import { describe, expect, it } from "vitest";

import { IS_BROWSER_TEST_BUILD } from "./browserTestBuild";

describe("browser test build marker", () => {
  it("is disabled in the normal production and unit-test build", () => {
    expect(IS_BROWSER_TEST_BUILD).toBe(false);
  });
});
