import { screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import Monitor from "../../pages/Monitor";
import { renderWithProviders } from "../../test-utils/renderWithProviders";

function mockMonitorEndpoints(): ReturnType<typeof vi.spyOn> {
  return vi
    .spyOn(globalThis, "fetch")
    .mockImplementation((input: RequestInfo | URL) => {
      const url = typeof input === "string" ? input : String(input);
      if (url.includes("/api/v1/batches")) {
        return Promise.resolve(
          new Response(
            JSON.stringify({
              items: [
                {
                  id: "batch-1",
                  name: "human-readable-batch",
                  state: "submitted",
                  expected_trial_count: 164,
                  created_at: "2026-06-19T20:23:00Z",
                  created_by_token_prefix: "test:web",
                },
              ],
              next_cursor: null,
            }),
            { status: 200, headers: { "Content-Type": "application/json" } },
          ),
        );
      }
      if (url.includes("/api/v1/trials")) {
        return Promise.resolve(
          new Response(JSON.stringify({ items: [], next_cursor: null }), {
            status: 200,
            headers: { "Content-Type": "application/json" },
          }),
        );
      }
      return Promise.reject(new Error(`unexpected fetch ${url}`));
    });
}

describe("Monitor human-readable labels", () => {
  beforeEach(() => {
    window.localStorage.clear();
    window.localStorage.setItem("loom_token", "test-token");
    vi.restoreAllMocks();
  });

  it("explains batch filters and planned trial counts without raw API wording", async () => {
    mockMonitorEndpoints();
    renderWithProviders(<Monitor />, { route: "/monitor?view=batches" });

    expect(await screen.findByText("human-readable-batch")).toBeInTheDocument();
    expect(screen.getByText("Planned trials")).toBeInTheDocument();
    expect(screen.queryByText("Expected")).not.toBeInTheDocument();
    expect(
      screen.getByPlaceholderText("Search batches by name or ID..."),
    ).toBeInTheDocument();
    expect(screen.getByRole("option", { name: "All states" })).toBeInTheDocument();
    expect(
      screen.getByRole("option", { name: "Submitted - waiting for scheduling" }),
    ).toBeInTheDocument();
  });
});
