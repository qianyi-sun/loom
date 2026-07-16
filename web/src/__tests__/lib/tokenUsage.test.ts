import { describe, expect, it } from "vitest";

import { formatTokenUsage } from "../../lib/tokenUsage";

describe("formatTokenUsage", () => {
  it("uses explicit input and output labels instead of P/C abbreviations", () => {
    const text = formatTokenUsage(77, 11);

    expect(text).toBe("Input 77 / Output 11");
    expect(text).not.toContain("P ");
    expect(text).not.toContain("C ");
  });

  it("normalizes missing token counts to zero", () => {
    expect(formatTokenUsage(null, undefined)).toBe("Input 0 / Output 0");
  });
});
