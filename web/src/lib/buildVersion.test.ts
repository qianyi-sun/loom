import { describe, expect, it } from "vitest";

import { frontendUpdateStatus } from "./buildVersion";

describe("frontendUpdateStatus (#2009)", () => {
  it("reports no update when the served revision matches the loaded one", () => {
    const revision = "a".repeat(40);
    const status = frontendUpdateStatus(revision, { revision });
    expect(status.hasNewerBuild).toBe(false);
    expect(status.servedRevision).toBe(revision);
  });

  it("reports an update when the served revision differs from the loaded one", () => {
    const status = frontendUpdateStatus("a".repeat(40), {
      revision: "b".repeat(40),
    });
    expect(status.hasNewerBuild).toBe(true);
    expect(status.servedRevision).toBe("b".repeat(40));
  });

  it("never claims an update when the loaded revision is unknown (local build)", () => {
    const status = frontendUpdateStatus(null, { revision: "b".repeat(40) });
    expect(status.hasNewerBuild).toBe(false);
  });

  it("never claims an update on a failed/unknown served fetch", () => {
    expect(frontendUpdateStatus("a".repeat(40), null).hasNewerBuild).toBe(
      false,
    );
    expect(frontendUpdateStatus("a".repeat(40), undefined).hasNewerBuild).toBe(
      false,
    );
    expect(
      frontendUpdateStatus("a".repeat(40), { revision: null }).hasNewerBuild,
    ).toBe(false);
  });
});
