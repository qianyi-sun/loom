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

describe("provider connection management endpoints", () => {
  beforeEach(() => {
    window.localStorage.setItem("loom_token", "t");
    vi.stubGlobal("fetch", vi.fn());
  });

  afterEach(() => vi.restoreAllMocks());

  it("getProviderConnection GETs /api/v1/provider-connections/:id", async () => {
    (global.fetch as ReturnType<typeof vi.fn>).mockResolvedValue(
      new Response(JSON.stringify({ id: "abc", name: "x" }), { status: 200 }),
    );
    const { api } = await import("../api/client");
    const result = await api.getProviderConnection("abc");
    expect(global.fetch).toHaveBeenCalledWith(
      "/api/v1/provider-connections/abc",
      expect.objectContaining({}),
    );
    expect(result).toEqual({ id: "abc", name: "x" });
  });

  it("createProviderConnection POSTs to /api/v1/provider-connections", async () => {
    (global.fetch as ReturnType<typeof vi.fn>).mockResolvedValue(
      new Response(JSON.stringify({ id: "new" }), { status: 201 }),
    );
    const { api } = await import("../api/client");
    const payload = {
      name: "n", type: "openai-compatible",
      base_url: "https://example", api_key: "k",
    };
    const result = await api.createProviderConnection(payload);
    expect(global.fetch).toHaveBeenCalledWith(
      "/api/v1/provider-connections",
      expect.objectContaining({ method: "POST", body: JSON.stringify(payload) }),
    );
    expect(result).toEqual({ id: "new" });
  });

  it("updateProviderConnection PATCHes /api/v1/provider-connections/:id", async () => {
    (global.fetch as ReturnType<typeof vi.fn>).mockResolvedValue(
      new Response(JSON.stringify({ id: "abc" }), { status: 200 }),
    );
    const { api } = await import("../api/client");
    const patch = { allowed_models: ["m1"] };
    await api.updateProviderConnection("abc", patch);
    expect(global.fetch).toHaveBeenCalledWith(
      "/api/v1/provider-connections/abc",
      expect.objectContaining({ method: "PATCH", body: JSON.stringify(patch) }),
    );
  });

  it("deleteProviderConnection DELETEs /api/v1/provider-connections/:id", async () => {
    (global.fetch as ReturnType<typeof vi.fn>).mockResolvedValue(
      new Response(null, { status: 204 }),
    );
    const { api } = await import("../api/client");
    await api.deleteProviderConnection("abc");
    expect(global.fetch).toHaveBeenCalledWith(
      "/api/v1/provider-connections/abc",
      expect.objectContaining({ method: "DELETE" }),
    );
  });

  it("testProviderConnection POSTs /api/v1/provider-connections/:id/test", async () => {
    (global.fetch as ReturnType<typeof vi.fn>).mockResolvedValue(
      new Response(JSON.stringify({ status: "valid" }), { status: 200 }),
    );
    const { api } = await import("../api/client");
    const result = await api.testProviderConnection("abc");
    expect(global.fetch).toHaveBeenCalledWith(
      "/api/v1/provider-connections/abc/test",
      expect.objectContaining({ method: "POST" }),
    );
    expect(result).toEqual({ status: "valid" });
  });

  it("listProviderConnectionModels GETs /api/v1/provider-connections/:id/models", async () => {
    (global.fetch as ReturnType<typeof vi.fn>).mockResolvedValue(
      new Response(JSON.stringify({ items: [] }), { status: 200 }),
    );
    const { api } = await import("../api/client");
    await api.listProviderConnectionModels("abc");
    expect(global.fetch).toHaveBeenCalledWith(
      "/api/v1/provider-connections/abc/models",
      expect.objectContaining({}),
    );
  });

  it("addProviderConnectionModel POSTs /api/v1/provider-connections/:id/models", async () => {
    (global.fetch as ReturnType<typeof vi.fn>).mockResolvedValue(
      new Response(JSON.stringify({}), { status: 201 }),
    );
    const { api } = await import("../api/client");
    const model = { model_id: "manual/x" };
    await api.addProviderConnectionModel("abc", model);
    expect(global.fetch).toHaveBeenCalledWith(
      "/api/v1/provider-connections/abc/models",
      expect.objectContaining({ method: "POST", body: JSON.stringify(model) }),
    );
  });

  it("refreshProviderConnectionModels POSTs .../models/refresh", async () => {
    (global.fetch as ReturnType<typeof vi.fn>).mockResolvedValue(
      new Response(JSON.stringify({ added: 0, removed: 0 }), { status: 200 }),
    );
    const { api } = await import("../api/client");
    await api.refreshProviderConnectionModels("abc");
    expect(global.fetch).toHaveBeenCalledWith(
      "/api/v1/provider-connections/abc/models/refresh",
      expect.objectContaining({ method: "POST" }),
    );
  });

  it("hideProviderConnectionModel POSTs .../models/:mid/hide (url-encoded mid)", async () => {
    (global.fetch as ReturnType<typeof vi.fn>).mockResolvedValue(
      new Response(null, { status: 204 }),
    );
    const { api } = await import("../api/client");
    await api.hideProviderConnectionModel("abc", "openai/gpt-4");
    expect(global.fetch).toHaveBeenCalledWith(
      "/api/v1/provider-connections/abc/models/openai%2Fgpt-4/hide",
      expect.objectContaining({ method: "POST" }),
    );
  });

  it("unhideProviderConnectionModel POSTs .../models/:mid/unhide", async () => {
    (global.fetch as ReturnType<typeof vi.fn>).mockResolvedValue(
      new Response(null, { status: 204 }),
    );
    const { api } = await import("../api/client");
    await api.unhideProviderConnectionModel("abc", "openai/gpt-4");
    expect(global.fetch).toHaveBeenCalledWith(
      "/api/v1/provider-connections/abc/models/openai%2Fgpt-4/unhide",
      expect.objectContaining({ method: "POST" }),
    );
  });

  it("addProviderConnectionModel is the new name (createProviderConnectionModel is gone)", async () => {
    const mod = await import("../api/client");
    expect(mod.api.addProviderConnectionModel).toBeDefined();
    expect((mod.api as Record<string, unknown>).createProviderConnectionModel)
      .toBeUndefined();
  });
});
