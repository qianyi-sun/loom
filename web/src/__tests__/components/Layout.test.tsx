import { screen } from "@testing-library/react";
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
