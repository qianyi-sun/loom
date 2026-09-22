import { describe, expect, it, vi } from "vitest";

import {
  captureManagedLoginProof,
  completeManagedLogin,
  hasManagedLoginProof,
} from "../../lib/managedLogin";

const proof = "loom_env_login_" + "a".repeat(43);

function capture(hash: string, protocol = "https:"): ReturnType<typeof vi.fn> {
  const replaceState = vi.fn();
  captureManagedLoginProof(
    { pathname: "/auth/managed", protocol, hash },
    { state: null, replaceState },
  );
  return replaceState;
}

describe("managed browser proof", () => {
  it("scrubs the fragment synchronously and consumes at most once after explicit action", async () => {
    const scrub = capture("#token=" + proof);
    expect(scrub).toHaveBeenCalledWith(null, "", "/auth/managed");
    expect(hasManagedLoginProof()).toBe(true);
    const received: string[] = [];
    const result = completeManagedLogin(async (token) => {
      expect(hasManagedLoginProof()).toBe(false);
      received.push(token);
    });
    await expect(completeManagedLogin(async () => { throw new Error("must not run"); })).rejects.toThrow("fresh");
    await result;
    expect(received).toEqual([proof]);
  });

  it.each(["", "#token=wrong", "#token=" + proof + "&next=https://foreign.example.com", "#token=" + proof + "&token=" + proof])(
    "removes but never accepts invalid or ambiguous fragment %s",
    async (hash) => {
      expect(capture(hash)).toHaveBeenCalledWith(null, "", "/auth/managed");
      expect(hasManagedLoginProof()).toBe(false);
      await expect(completeManagedLogin(async () => undefined)).rejects.toThrow("fresh");
    },
  );

  it("rejects insecure origins and never replays a proof after failure", async () => {
    capture("#token=" + proof, "http:");
    expect(hasManagedLoginProof()).toBe(false);
    capture("#token=" + proof);
    await expect(completeManagedLogin(async () => { throw new Error("upstream echoed " + proof); })).rejects.toThrow("fresh");
    expect(hasManagedLoginProof()).toBe(false);
  });
});
