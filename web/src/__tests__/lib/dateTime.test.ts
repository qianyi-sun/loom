import { describe, expect, it } from "vitest";

import { formatLocalDateTime } from "../../lib/dateTime";

describe("formatLocalDateTime", () => {
  it("renders UTC ISO timestamps in the requested local timezone with zone context", () => {
    expect(
      formatLocalDateTime("2026-06-27T03:04:54Z", {
        timeZone: "America/Toronto",
      }),
    ).toBe("2026-06-26 23:04 EDT");
  });

  it("uses the fallback for empty or invalid timestamps", () => {
    expect(formatLocalDateTime(null)).toBe("—");
    expect(formatLocalDateTime("not-a-date", { fallback: "--" })).toBe("--");
  });
});
