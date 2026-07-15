import { screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import RateCardsAdmin from "../../pages/RateCardsAdmin";
import { renderWithProviders } from "../../test-utils/renderWithProviders";

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

const adminMe = {
  user: {
    id: "admin-user",
    username: "Admin",
    email: "admin@example.com",
    display_name: "Admin Example",
    is_platform_admin: true,
  },
  teams: [{ id: "team-1", name: "Ops", role: "platform_admin" }],
  current_team: { id: "team-1", name: "Ops", role: "platform_admin" },
  role: "platform_admin",
  scopes: ["admin:rate_cards"],
  is_platform_admin: true,
  csrf_token: "csrf-admin",
};

describe("RateCardsAdmin", () => {
  beforeEach(() => {
    vi.restoreAllMocks();
  });

  it("shows rate-card entry shape and provider mapping guidance", async () => {
    vi.spyOn(global, "fetch").mockImplementation(
      async (input: RequestInfo | URL) => {
        const url = String(input);
        if (url.includes("/api/v1/auth/me")) {
          return jsonResponse(adminMe);
        }
        if (url.includes("/api/v1/rate-cards")) {
          return jsonResponse({ items: [] });
        }
        return jsonResponse({ detail: `unhandled ${url}` }, 404);
      },
    );

    renderWithProviders(<RateCardsAdmin />, { route: "/rate-cards" });

    expect(await screen.findByText("Rate-card JSON example")).toBeInTheDocument();
    expect(screen.getByText(/"model": "gpt-4o-mini"/)).toBeInTheDocument();
    expect(screen.getByText(/rate_card_provider/)).toBeInTheDocument();
  });
});
