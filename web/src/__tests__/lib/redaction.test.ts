import { describe, expect, it } from "vitest";

import { redactText, redactValue } from "../../lib/redaction";

describe("redaction helpers", () => {
  it("redacts public-beta secret shapes from display text", () => {
    const text = [
      "Authorization: Bearer loom_api_abcdefghijklmnopqrstuvwxyz012345",
      "provider key sk-proj-abcdefghijklmnopqrstuvwxyz0123456789",
      "http://minio.internal/artifacts/a?X-Amz-Signature=abc123",
      "http://loom-control-plane:8080/trials",
      "loom://provider-connection/team-1/key",
    ].join("\n");

    const redacted = redactText(text);

    expect(redacted).not.toContain("loom_api_abcdefghijklmnopqrstuvwxyz012345");
    expect(redacted).not.toContain("sk-proj-abcdefghijklmnopqrstuvwxyz0123456789");
    expect(redacted).not.toContain("X-Amz-Signature=abc123");
    expect(redacted).not.toContain("minio.internal");
    expect(redacted).not.toContain("loom-control-plane");
    expect(redacted).not.toContain("loom://provider-connection");
    expect(redacted).toContain("[REDACTED]");
  });

  it("redacts nested sensitive values before JSON diagnostics render", () => {
    const redacted = redactValue({
      authorization: "Bearer loom_api_abcdefghijklmnopqrstuvwxyz012345",
      headers: {
        "x-loom-csrf": "loom_csrf_abcdefghijklmnopqrstuvwxyz012345",
      },
      artifact_url: "http://minio:9000/a?X-Amz-Credential=cred",
      safe_note: "finished",
    });

    expect(JSON.stringify(redacted)).not.toContain("loom_api_");
    expect(JSON.stringify(redacted)).not.toContain("loom_csrf_");
    expect(JSON.stringify(redacted)).not.toContain("X-Amz-Credential=cred");
    expect(redacted).toMatchObject({
      authorization: "[REDACTED]",
      headers: {
        "x-loom-csrf": "[REDACTED]",
      },
      safe_note: "finished",
    });
  });
});
