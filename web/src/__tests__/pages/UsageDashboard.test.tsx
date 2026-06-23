import { screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import UsageDashboard from "../../pages/UsageDashboard";
import { renderWithProviders } from "../../test-utils/renderWithProviders";

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

describe("UsageDashboard", () => {
  beforeEach(() => {
    vi.restoreAllMocks();
  });

  it("shows a CLI equivalent for the selected usage range", async () => {
    vi.spyOn(global, "fetch").mockImplementation(
      async (input: RequestInfo | URL) => {
        const url = String(input);
        if (url.includes("/api/v1/auth/me")) {
          return jsonResponse({
            user: {
              id: "user-1",
              email: "user@example.com",
              display_name: null,
              is_platform_admin: false,
            },
            teams: [{ id: "team-1", name: "Alpha", role: "member" }],
            current_team: { id: "team-1", name: "Alpha", role: "member" },
            role: "member",
            scopes: ["read:own"],
            is_platform_admin: false,
            csrf_token: "csrf-test",
          });
        }
        if (url.includes("/api/v1/usage")) {
          return jsonResponse({
            degraded: false,
            buckets: [
              {
                start_at: "2026-06-01T00:00:00Z",
                trial_count: 2,
                total_cost_usd: 0.25,
                llm_input_tokens: 1000,
                llm_output_tokens: 500,
                trials_currently_succeeded: 1,
                trials_currently_failed: 1,
              },
            ],
          });
        }
        return jsonResponse({ detail: `unhandled ${url}` }, 404);
      },
    );

    renderWithProviders(<UsageDashboard />, { route: "/usage" });

    expect(await screen.findByText("Usage CLI")).toBeInTheDocument();
    expect(screen.getByText(/loom eval usage --start/)).toHaveTextContent("--end");
    expect(await screen.findByText("Estimated LLM cost")).toBeInTheDocument();
  });
});
