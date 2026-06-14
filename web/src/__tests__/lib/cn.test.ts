import { describe, expect, it } from "vitest";

import { cn } from "../../lib/cn";

describe("cn", () => {
  it("joins simple string arguments", () => {
    expect(cn("a", "b")).toBe("a b");
  });

  it("drops falsy values", () => {
    expect(cn("a", null, undefined, false, "")).toBe("a");
  });

  it("respects conditional objects (clsx semantics)", () => {
    expect(cn("base", { active: true, hidden: false })).toBe("base active");
  });

  it("flattens nested arrays", () => {
    expect(cn(["a", ["b", "c"]])).toBe("a b c");
  });

  it("returns empty string for no truthy classes", () => {
    expect(cn(false, null, undefined)).toBe("");
  });
});
