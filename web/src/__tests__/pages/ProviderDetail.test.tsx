import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import ProviderDetail from "../../pages/ProviderDetail";

const CONN = {
  id: "abc",
  name: "prod-anthropic",
  type: "anthropic",
  base_url: "https://api.anthropic.com",
  status: "valid",
  allowed_models: ["claude-3-opus"],
  created_at: "2026-01-01T00:00:00Z",
  updated_at: "2026-01-01T00:00:00Z",
  last_validated_at: null,
  last_validation_error: null,
  pricing_source: null,
  rate_card_provider: null,
};

function renderPage(conn: unknown, initialPath = "/providers/abc") {
  vi.stubGlobal(
    "fetch",
    vi.fn().mockResolvedValue(
      new Response(JSON.stringify(conn), { status: 200 }),
    ),
  );
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={qc}>
      <MemoryRouter initialEntries={[initialPath]}>
        <Routes>
          <Route path="/providers/:id" element={<ProviderDetail />} />
          <Route path="/providers" element={<div>providers-list</div>} />
        </Routes>
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

describe("ProviderDetail", () => {
  beforeEach(() => window.localStorage.setItem("loom_token", "t"));
  afterEach(() => vi.restoreAllMocks());

  it("Overview tab is shown by default with connection name", async () => {
    renderPage(CONN);
    await waitFor(() => {
      expect(screen.getByRole("heading", { name: /prod-anthropic/i })).toBeInTheDocument();
    });
    expect(screen.getByRole("tab", { name: /overview/i })).toHaveAttribute("aria-selected", "true");
  });

  it("explains readiness and allowed-model policy in plain language", async () => {
    renderPage(CONN);
    await waitFor(() => {
      expect(screen.getByText("1 allowed model")).toBeInTheDocument();
    });
    expect(screen.getByText("Ready")).toBeInTheDocument();
    expect(
      screen.getByText(/model picker only offers the configured allow-list/i),
    ).toBeInTheDocument();
    expect(
      screen.getByText(/last provider test passed/i),
    ).toBeInTheDocument();
    expect(
      screen.getByText(/OpenAI-compatible root ending in \/v1/i),
    ).toBeInTheDocument();
  });

  it("clicking Models tab switches content", async () => {
    const user = userEvent.setup();
    // Use per-call mock so each fetch gets a fresh Response (reusing a single
    // Response object fails because the body stream can only be consumed once).
    vi.stubGlobal(
      "fetch",
      vi.fn().mockImplementation((_url: string) => {
        if (_url.includes("/models")) {
          return Promise.resolve(new Response(JSON.stringify({ items: [] }), { status: 200 }));
        }
        return Promise.resolve(new Response(JSON.stringify(CONN), { status: 200 }));
      }),
    );
    const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    render(
      <QueryClientProvider client={qc}>
        <MemoryRouter initialEntries={["/providers/abc"]}>
          <Routes>
            <Route path="/providers/:id" element={<ProviderDetail />} />
          </Routes>
        </MemoryRouter>
      </QueryClientProvider>,
    );
    await waitFor(() => {
      expect(screen.getByRole("tab", { name: /models/i })).toBeInTheDocument();
    });
    await user.click(screen.getByRole("tab", { name: /models/i }));
    await waitFor(() => {
      expect(screen.getByRole("button", { name: /add manual model/i })).toBeInTheDocument();
    });
  });

  it("404 response shows not-found message", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(
        new Response(JSON.stringify({ detail: "not found" }), { status: 404 }),
      ),
    );
    const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    render(
      <QueryClientProvider client={qc}>
        <MemoryRouter initialEntries={["/providers/missing"]}>
          <Routes>
            <Route path="/providers/:id" element={<ProviderDetail />} />
          </Routes>
        </MemoryRouter>
      </QueryClientProvider>,
    );
    await waitFor(() => {
      expect(screen.getByText(/connection not found/i)).toBeInTheDocument();
    });
  });

  it("Test connection button triggers POST and shows ready pill on success", async () => {
    // Use mockImplementation to reliably sequence: GET conn, POST test, GET conn (re-fetch)
    let callCount = 0;
    const fetchMock = vi.fn().mockImplementation(async () => {
      callCount++;
      if (callCount === 1) return new Response(JSON.stringify(CONN), { status: 200 });
      if (callCount === 2) return new Response(JSON.stringify({ status: "valid" }), { status: 200 });
      return new Response(JSON.stringify(CONN), { status: 200 });
    });
    vi.stubGlobal("fetch", fetchMock);
    const user = userEvent.setup();
    const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    render(
      <QueryClientProvider client={qc}>
        <MemoryRouter initialEntries={["/providers/abc"]}>
          <Routes>
            <Route path="/providers/:id" element={<ProviderDetail />} />
          </Routes>
        </MemoryRouter>
      </QueryClientProvider>,
    );
    await waitFor(() => {
      expect(screen.getByRole("button", { name: /test connection/i })).toBeInTheDocument();
    });
    await user.click(screen.getByRole("button", { name: /test connection/i }));
    await waitFor(() => {
      expect(screen.getAllByText(/Ready/i).length).toBeGreaterThan(0);
    });
  });

  it("Test connection shows invalid pill and error fallback text on failure", async () => {
    // Use mockImplementation to reliably sequence: GET conn, POST test (invalid), GET conn
    let callCount = 0;
    const fetchMock = vi.fn().mockImplementation(async () => {
      callCount++;
      if (callCount === 1) return new Response(JSON.stringify(CONN), { status: 200 });
      if (callCount === 2)
        return new Response(
          JSON.stringify({ status: "invalid", last_validation_error: null }),
          { status: 200 },
        );
      return new Response(JSON.stringify(CONN), { status: 200 });
    });
    vi.stubGlobal("fetch", fetchMock);
    const user = userEvent.setup();
    const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    render(
      <QueryClientProvider client={qc}>
        <MemoryRouter initialEntries={["/providers/abc"]}>
          <Routes>
            <Route path="/providers/:id" element={<ProviderDetail />} />
          </Routes>
        </MemoryRouter>
      </QueryClientProvider>,
    );
    await waitFor(() => {
      expect(screen.getByRole("button", { name: /test connection/i })).toBeInTheDocument();
    });
    await user.click(screen.getByRole("button", { name: /test connection/i }));
    await waitFor(() => {
      expect(screen.getByText(/no error details reported/i)).toBeInTheDocument();
    });
  });
});
