import { describe, expect, it } from "vitest";

import { humanizeFailureReason } from "../../lib/humanizeFailureReason";

describe("humanizeFailureReason", () => {
  it("maps known failure codes to human labels", () => {
    expect(humanizeFailureReason("artifact_upload_failed").label).toBe(
      "Artifact upload failed",
    );
  });

  it("preserves unknown codes for diagnostics", () => {
    const out = humanizeFailureReason("custom_runner_failure");

    expect(out.label).toBe("Custom runner failure");
    expect(out.code).toBe("custom_runner_failure");
  });
});
