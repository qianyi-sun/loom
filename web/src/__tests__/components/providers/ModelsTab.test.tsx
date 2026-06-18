import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import ModelsTab from "../../../components/providers/ModelsTab";

function renderTab(models: unknown[]) {
  const fetchMock = vi.fn().mockImplementation((_url: string, _init?: RequestInit) => {
    // All list calls return the same items shape; refresh returns summary counts.
    if (_url.includes("/models/refresh")) {
      return Promise.resolve(new Response(JSON.stringify({
        added: 0,
        refreshed: 0,
        missing: 0,
        items: models,
      }), { status: 200 }));
    }
    if (_url.endsWith("/hide") || _url.endsWith("/unhide")) {
      return Promise.resolve(new Response(null, { status: 204 }));
    }
    if (_init?.method === "POST" && _url.endsWith("/models")) {
      // add manual model
      return Promise.resolve(new Response(JSON.stringify({}), { status: 201 }));
    }
    return Promise.resolve(new Response(JSON.stringify({ items: models }), { status: 200 }));
  });
  vi.stubGlobal("fetch", fetchMock);
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  const result = render(
    <QueryClientProvider client={qc}>
      <ModelsTab id="abc" />
    </QueryClientProvider>,
  );
  return { ...result, fetchMock };
}

describe("ModelsTab", () => {
  beforeEach(() => window.localStorage.setItem("loom_token", "t"));
  afterEach(() => vi.restoreAllMocks());

  it("renders a row per model with hidden state", async () => {
    renderTab([
      { model_id: "gpt-4o", source: "upstream", visible: true, visibility: "default" },
      { model_id: "manual/x", source: "manual", visible: false, visibility: "hidden" },
    ]);
    await waitFor(() => {
      expect(screen.getByText("gpt-4o")).toBeInTheDocument();
      expect(screen.getByText("manual/x")).toBeInTheDocument();
    });
  });

  it("renders hidden rows from the provider models API visibility contract", async () => {
    renderTab([
      {
        model_id: "manual/x",
        source: "manual",
        visible: false,
        hidden_reason: "operator-hidden",
        visibility: "hidden",
      },
    ]);
    await waitFor(() => screen.getByText("manual/x"));
    expect(screen.getByText("hidden")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /^unhide$/i })).toBeInTheDocument();
  });

  it("Refresh button triggers POST .../models/refresh", async () => {
    const { fetchMock } = renderTab([]);
    const user = userEvent.setup();
    await waitFor(() => screen.getByRole("button", { name: /^refresh$/i }));
    await user.click(screen.getByRole("button", { name: /^refresh$/i }));
    await waitFor(() => {
      const refreshCalls = (fetchMock.mock.calls as Array<[string, RequestInit | undefined]>).filter(
        ([url]) => url.endsWith("/models/refresh"),
      );
      expect(refreshCalls.length).toBeGreaterThan(0);
    });
  });

  it("Add manual model opens modal, submitting POSTs", async () => {
    const { fetchMock } = renderTab([]);
    const user = userEvent.setup();
    await waitFor(() => screen.getByRole("button", { name: /add manual model/i }));
    await user.click(screen.getByRole("button", { name: /add manual model/i }));
    await user.type(screen.getByLabelText(/model id/i), "manual/y");
    await user.click(screen.getByRole("button", { name: /^add model$/i }));
    await waitFor(() => {
      const addCalls = (fetchMock.mock.calls as Array<[string, RequestInit | undefined]>).filter(
        ([url, init]) => url === "/api/v1/provider-connections/abc/models" && init?.method === "POST",
      );
      expect(addCalls.length).toBeGreaterThan(0);
    });
  });

  it("Hide button on a non-hidden row POSTs .../hide", async () => {
    const { fetchMock } = renderTab([
      { model_id: "gpt-4o", source: "upstream", visible: true, visibility: "default" },
    ]);
    const user = userEvent.setup();
    await waitFor(() => screen.getByText("gpt-4o"));
    await user.click(screen.getByRole("button", { name: /^hide$/i }));
    await waitFor(() => {
      const hideCalls = (fetchMock.mock.calls as Array<[string, RequestInit | undefined]>).filter(
        ([url]) => url.endsWith("/gpt-4o/hide"),
      );
      expect(hideCalls.length).toBeGreaterThan(0);
    });
  });

  it("Unhide button on a hidden row POSTs .../unhide", async () => {
    const { fetchMock } = renderTab([
      { model_id: "gpt-4o", source: "upstream", visible: false, visibility: "hidden" },
    ]);
    const user = userEvent.setup();
    await waitFor(() => screen.getByText("gpt-4o"));
    await user.click(screen.getByRole("button", { name: /^unhide$/i }));
    await waitFor(() => {
      const unhideCalls = (fetchMock.mock.calls as Array<[string, RequestInit | undefined]>).filter(
        ([url]) => url.endsWith("/gpt-4o/unhide"),
      );
      expect(unhideCalls.length).toBeGreaterThan(0);
    });
  });
});
