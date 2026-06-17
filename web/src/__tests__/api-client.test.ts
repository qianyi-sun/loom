import { beforeEach, describe, expect, it, vi } from "vitest";

import { api, apiFetch, setUnauthorizedHandler } from "../api/client";

describe("apiFetch", () => {
  beforeEach(() => {
    window.localStorage.clear();
    vi.restoreAllMocks();
  });

  it("attaches bearer token from localStorage", async () => {
    window.localStorage.setItem("loom_token", "abc");
    const spy = vi.spyOn(global, "fetch").mockResolvedValue(
      new Response(JSON.stringify({ ok: true }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      }),
    );
    await apiFetch("/api/v1/health");
    expect(spy).toHaveBeenCalled();
    const init = spy.mock.calls[0][1] as RequestInit;
    expect((init.headers as Record<string, string>).Authorization).toBe(
      "Bearer abc",
    );
  });

  it("does NOT attach Authorization header when no token", async () => {
    const spy = vi.spyOn(global, "fetch").mockResolvedValue(
      new Response("{}", {
        status: 200,
        headers: { "Content-Type": "application/json" },
      }),
    );
    await apiFetch("/api/v1/health");
    const init = spy.mock.calls[0][1] as RequestInit;
    expect("Authorization" in (init.headers as object)).toBe(false);
  });

  it("calls unauthorized handler on 401", async () => {
    vi.spyOn(global, "fetch").mockResolvedValue(
      new Response("", { status: 401 }),
    );
    const onAuth = vi.fn();
    setUnauthorizedHandler(onAuth);
    await expect(apiFetch("/api/v1/trials")).rejects.toMatchObject({
      status: 401,
    });
    expect(onAuth).toHaveBeenCalled();
  });

  it("throws ApiError with detail on 403", async () => {
    vi.spyOn(global, "fetch").mockResolvedValue(
      new Response(JSON.stringify({ detail: "nope" }), {
        status: 403,
        headers: { "Content-Type": "application/json" },
      }),
    );
    await expect(apiFetch("/api/v1/trials")).rejects.toMatchObject({
      status: 403,
      detail: "nope",
    });
  });

  it("returns undefined on 204", async () => {
    vi.spyOn(global, "fetch").mockResolvedValue(
      new Response(null, { status: 204 }),
    );
    const result = await apiFetch("/api/v1/tokens/abc12345");
    expect(result).toBeUndefined();
  });

  it("sends admin actor header when approving a team registration", async () => {
    window.localStorage.setItem("loom_token", "loom_admin_secret");
    const spy = vi.spyOn(global, "fetch").mockResolvedValue(
      new Response(JSON.stringify({ registration: { id: "reg-1" } }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      }),
    );

    await api.approveTeamRegistration("reg-1", "ops-owner");

    expect(spy).toHaveBeenCalledWith(
      "/api/v1/admin/team-registrations/reg-1/approve",
      expect.objectContaining({ method: "POST" }),
    );
    const init = spy.mock.calls[0][1] as RequestInit;
    expect((init.headers as Record<string, string>)["X-Loom-Admin-Actor"]).toBe(
      "ops-owner",
    );
  });
});
