import { screen, within } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import App from "../../App";
import { renderWithProviders } from "../../test-utils/renderWithProviders";

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

describe("Layout", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("passes current team context into the authenticated app shell", async () => {
    vi.spyOn(global, "fetch").mockImplementation(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.includes("/api/v1/auth/me")) {
        return jsonResponse({
          user: {
            id: "user-1",
            email: "owner@example.com",
            display_name: "Owner Example",
            is_platform_admin: false,
          },
          teams: [{ id: "team-eai", name: "EAI", role: "owner" }],
          current_team: { id: "team-eai", name: "EAI", role: "owner" },
          role: "owner",
          scopes: ["read:own", "submit", "team:manage"],
          is_platform_admin: false,
          csrf_token: "csrf-owner",
        });
      }
      if (url.endsWith("/api/v1/teams/team-eai")) {
        return jsonResponse({
          id: "team-eai",
          name: "EAI",
          created_at: "2026-06-24T00:00:00Z",
          quota: null,
          members: [],
          user_members: [],
        });
      }
      return jsonResponse({ detail: `unhandled ${url}` }, 404);
    });

    renderWithProviders(<App />, { route: "/settings" });

    const nav = await screen.findByRole("navigation", { name: "Primary" });
    const teamContext = within(nav).getByLabelText("Current team");
    expect(teamContext).toHaveTextContent("EAI");
    expect(teamContext).toHaveTextContent("owner");
  });

  it("gives the signed-out settings page a wide public onboarding shell", async () => {
    vi.spyOn(global, "fetch").mockImplementation(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.includes("/api/v1/auth/me")) {
        return jsonResponse({ detail: "unauthorized" }, 401);
      }
      return jsonResponse({ detail: `unhandled ${url}` }, 404);
    });

    renderWithProviders(<App />, { route: "/settings" });

    expect(await screen.findByRole("heading", { name: "Sign in" })).toBeInTheDocument();
    const shell = screen.getByTestId("public-onboarding-shell");
    expect(shell).toHaveClass("max-w-6xl");
    expect(shell).not.toHaveClass("max-w-md");
  });

  it("keeps invite acceptance reachable before sign-in", async () => {
    vi.spyOn(global, "fetch").mockImplementation(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.includes("/api/v1/auth/me")) {
        return jsonResponse({ detail: "unauthorized" }, 401);
      }
      if (url.includes("/api/v1/invites/lookup")) {
        return jsonResponse({
          team_name: "Public Beta",
          role: "member",
          status: "pending",
          code_prefix: "abc12345",
        });
      }
      return jsonResponse({ detail: `unhandled ${url}` }, 404);
    });

    renderWithProviders(<App />, {
      route: "/invites/accept?code=loom_invite_abc",
    });

    expect(await screen.findByText("Public Beta")).toBeInTheDocument();
    expect(
      screen.queryByRole("heading", { name: "Sign in" }),
    ).not.toBeInTheDocument();
  });
});
