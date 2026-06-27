import { describe, expect, it } from "vitest";

import { formatTokenUsage } from "../../lib/tokenUsage";

describe("formatTokenUsage", () => {
  it("uses explicit input and output labels instead of P/C abbreviations", () => {
    const text = formatTokenUsage(77, 11);

    expect(text).toBe("Input 77 / Output 11");
    expect(text).not.toContain("P ");
    expect(text).not.toContain("C ");
  });
});
