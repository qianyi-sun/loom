import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { setFrontendConfigForTests } from "../../lib/frontendConfig";
import ProviderCreate from "../../pages/ProviderCreate";

function renderPage(initialPath = "/providers/new") {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={qc}>
      <MemoryRouter initialEntries={[initialPath]}>
        <Routes>
          <Route path="/providers/new" element={<ProviderCreate />} />
          <Route path="/providers/:id" element={<div>detail-page</div>} />
          <Route path="/batches/new" element={<div>new-batch-page</div>} />
        </Routes>
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

describe("ProviderCreate", () => {
  beforeEach(() => window.localStorage.setItem("loom_token", "t"));
  afterEach(() => {
    setFrontendConfigForTests(null);
    vi.restoreAllMocks();
  });

  it("happy path POSTs then redirects to /providers/:id", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(
      new Response(JSON.stringify({ id: "new-id" }), { status: 201 }),
    ));
    const user = userEvent.setup();
    renderPage();
    await user.type(screen.getByLabelText(/name/i), "smoke");
    await user.type(screen.getByLabelText(/base url/i), "https://x");
    await user.type(screen.getByLabelText(/api key/i), "sk-x");
    await user.click(screen.getByRole("button", { name: /create/i }));
    await waitFor(() => {
      expect(screen.getByText(/detail-page/i)).toBeInTheDocument();
    });
  });

  it("shows the two provider setup paths in human language", () => {
    setFrontendConfigForTests({
      environment: "staging",
      environmentLabel: "Staging",
      routePath: "/dev",
      apiBase: "/dev",
      apiRouteBase: `${window.location.origin}/dev/api`,
    });
    renderPage();
    expect(screen.getByText(/third-party api/i)).toBeInTheDocument();
    expect(screen.getByText(/gpu cluster checkpoint/i)).toBeInTheDocument();
    expect(screen.getAllByText(/loom inference deploy slurm/i).length).toBeGreaterThan(0);
    expect(screen.getByText("Hosted API CLI")).toBeInTheDocument();
    expect(
      screen.getByText(/loom auth login --server/i),
    ).toHaveTextContent(`${window.location.origin}/dev`);
    expect(screen.getByText(/loom providers create/)).toHaveTextContent(
      "--api-key env:PROVIDER_API_KEY",
    );
    expect(screen.getByText("Cluster deploy CLI")).toBeInTheDocument();
  });

  it("with ?returnTo=/batches/new redirects there", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(
      new Response(JSON.stringify({ id: "new-id" }), { status: 201 }),
    ));
    const user = userEvent.setup();
    renderPage("/providers/new?returnTo=/batches/new");
    await user.type(screen.getByLabelText(/name/i), "smoke");
    await user.type(screen.getByLabelText(/base url/i), "https://x");
    await user.type(screen.getByLabelText(/api key/i), "sk-x");
    await user.click(screen.getByRole("button", { name: /create/i }));
    await waitFor(() => {
      expect(screen.getByText(/new-batch-page/i)).toBeInTheDocument();
    });
  });

  it("400 from backend shows inline error banner; form not cleared", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(
      new Response(JSON.stringify({ detail: "base_url must be valid URL" }), {
        status: 400,
      }),
    ));
    const user = userEvent.setup();
    renderPage();
    await user.type(screen.getByLabelText(/name/i), "smoke");
    await user.type(screen.getByLabelText(/base url/i), "not-a-url");
    await user.type(screen.getByLabelText(/api key/i), "sk-x");
    await user.click(screen.getByRole("button", { name: /create/i }));
    await waitFor(() => {
      expect(screen.getByText(/base_url must be valid URL/i)).toBeInTheDocument();
    });
    expect((screen.getByLabelText(/name/i) as HTMLInputElement).value).toBe("smoke");
  });
});
