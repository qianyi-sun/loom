import { fireEvent, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
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
    vi.spyOn(globalThis, "fetch").mockImplementation(
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
    expect(
      screen.getByRole("heading", { name: "Published" }).closest(
        '[data-loom-query="rate-cards"]',
      ),
    ).toHaveAttribute("data-loom-query-status", "success");
  });

  it("summarizes populated, empty, and partially specified cards", async () => {
    vi.spyOn(globalThis, "fetch").mockImplementation(
      async (input: RequestInfo | URL) => {
        const url = String(input);
        if (url.includes("/api/v1/auth/me")) return jsonResponse(adminMe);
        return jsonResponse({
          items: [
            { table: { entries: [] } },
            {
              id: "published-card",
              captured_at: "2026-07-16T00:00:00Z",
              table: {
                entries: [
                  {
                    provider: "openai",
                    model: "gpt-4o-mini",
                    input_per_mtok: 1.25,
                    output_per_mtok: 2.5,
                    cache_read_per_mtok: null,
                  },
                ],
              },
            },
          ],
        });
      },
    );

    renderWithProviders(<RateCardsAdmin />);
    expect(await screen.findByText("Rate card 1")).toBeInTheDocument();
    expect(screen.getByText("No model pricing entries are published in this card.")).toBeInTheDocument();
    expect(screen.getByText("published-card")).toBeInTheDocument();
    expect(screen.getByText("$1.25 / 1M tokens")).toBeInTheDocument();
    expect(screen.getByText("$2.50 / 1M tokens")).toBeInTheDocument();
    expect(screen.getAllByText("not set").length).toBeGreaterThan(0);
    expect(screen.getByText(/1 price entry - captured/)).toBeInTheDocument();
  });

  it("validates JSON and publishes an object", async () => {
    const user = userEvent.setup();
    vi.spyOn(globalThis, "fetch").mockImplementation(
      async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = String(input);
        if (url.includes("/api/v1/auth/me")) return jsonResponse(adminMe);
        if (init?.method === "POST") return jsonResponse({ id: "created" }, 201);
        return jsonResponse({ items: [] });
      },
    );
    renderWithProviders(<RateCardsAdmin />);
    const editor = await screen.findByLabelText("Rate card JSON payload");

    fireEvent.change(editor, { target: { value: "[]" } });
    await user.click(screen.getByRole("button", { name: "Publish" }));
    expect(screen.getByText("expected a JSON object")).toBeInTheDocument();

    fireEvent.change(editor, { target: { value: "{" } });
    await user.click(screen.getByRole("button", { name: "Publish" }));
    expect(screen.getByText(/^Expected property name/u)).toBeInTheDocument();

    fireEvent.change(editor, { target: { value: '{"id":"created"}' } });
    await user.click(screen.getByRole("button", { name: "Publish" }));
    expect(await screen.findByText("Rate card published.")).toBeInTheDocument();
  });

  it("hides publishing for non-admin users and surfaces list errors", async () => {
    vi.spyOn(globalThis, "fetch").mockImplementation(
      async (input: RequestInfo | URL) => {
        if (String(input).includes("/api/v1/auth/me")) {
          return jsonResponse({
            ...adminMe,
            user: { ...adminMe.user, is_platform_admin: false },
            role: "owner",
            scopes: [],
            is_platform_admin: false,
          });
        }
        return jsonResponse({ detail: "rate cards unavailable" }, 503);
      },
    );
    renderWithProviders(<RateCardsAdmin />);
    expect(await screen.findByText("rate cards unavailable")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Publish" })).not.toBeInTheDocument();
  });
});
