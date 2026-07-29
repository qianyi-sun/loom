import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import { setFrontendConfigForTests } from "../../lib/frontendConfig";
import App from "../../App";
import Settings from "../../pages/Settings";
import { renderWithProviders } from "../../test-utils/renderWithProviders";

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

const ownerMe = {
  user: {
    id: "user-owner",
    username: "Owner",
    email: "owner@example.com",
    display_name: "Owner Example",
    is_platform_admin: false,
  },
  teams: [
    { id: "team-a", name: "Alpha", role: "owner" },
    { id: "team-b", name: "Beta", role: "member" },
  ],
  current_team: { id: "team-a", name: "Alpha", role: "owner" },
  role: "owner",
  scopes: ["read:own", "submit", "tokens:manage", "providers:manage", "team:manage"],
  is_platform_admin: false,
  csrf_token: "csrf-owner",
};

const betaMe = {
  ...ownerMe,
  current_team: { id: "team-b", name: "Beta", role: "member" },
  role: "member",
  scopes: ["read:own", "submit"],
  csrf_token: "csrf-beta",
};

const adminMe = {
  ...ownerMe,
  user: {
    ...ownerMe.user,
    id: "platform-admin",
    username: "PlatformAdmin",
    is_platform_admin: true,
  },
  is_platform_admin: true,
  csrf_token: "csrf-admin",
};

const teamDetail = {
  id: "team-a",
  name: "Alpha",
  created_at: "2026-06-22T00:00:00Z",
  quota: null,
  members: [],
  user_members: [
    {
      user_id: "user-owner",
      email: "owner@example.com",
      display_name: "Owner Example",
      role: "owner",
      joined_at: "2026-06-20T00:00:00Z",
    },
    {
      user_id: "user-member",
      email: "member@example.com",
      display_name: null,
      role: "member",
      joined_at: "2026-06-21T00:00:00Z",
    },
  ],
};

describe("Settings", () => {
  afterEach(() => {
    setFrontendConfigForTests(null);
    vi.restoreAllMocks();
  });

  it("redirects signed-out visitors to /auth/login", async () => {
    vi.spyOn(globalThis, "fetch").mockImplementation(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.includes("/api/v1/auth/me")) {
        return jsonResponse({ detail: "unauthorized" }, 401);
      }
      if (url.endsWith("/api/v1/auth/public-teams")) {
        return jsonResponse({ items: [] });
      }
      return jsonResponse({ detail: `unhandled ${url}` }, 404);
    });

    // Use App so Layout + route tree perform the legacy /settings redirect.
    renderWithProviders(<App />, { route: "/settings" });
    expect(await screen.findByRole("heading", { name: "Sign in" })).toBeInTheDocument();
  });

  it("summarizes the current team, members, and owner setup actions", async () => {
    const fetchSpy = vi.spyOn(globalThis, "fetch").mockImplementation(
      async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = String(input);
        if (url.includes("/api/v1/auth/me")) {
          return jsonResponse(ownerMe);
        }
        if (url.endsWith("/api/v1/auth/team") && init?.method === "POST") {
          expect(JSON.parse(String(init.body))).toEqual({ team_id: "team-b" });
          return jsonResponse(betaMe);
        }
        if (url.endsWith("/api/v1/teams/team-a")) {
          return jsonResponse(teamDetail);
        }
        if (url.endsWith("/api/v1/teams/team-b")) {
          return jsonResponse({
            ...teamDetail,
            id: "team-b",
            name: "Beta",
            user_members: [],
          });
        }
        if (url.endsWith("/api/v1/tokens")) {
          return jsonResponse({
            items: [
              {
                name: "CLI submit",
                token_hash_prefix: "abc12345",
                type: "team",
                scopes: ["read:own", "submit"],
                team_id: "team-a",
                issued_at: "2026-06-22T00:00:00Z",
                expires_at: "2026-07-22T00:00:00Z",
                revoked_at: null,
                last_used_at: null,
              },
            ],
          });
        }
        return jsonResponse({ detail: `unhandled ${url}` }, 404);
      },
    );

    renderWithProviders(<Settings />, { route: "/settings" });

    expect(await screen.findByRole("heading", { name: "Team Settings" })).toBeInTheDocument();
    expect(screen.getByLabelText("Current team")).toHaveValue("team-a");
    expect(screen.getByText("Owner")).toBeInTheDocument();
    expect((await screen.findAllByText("owner@example.com")).length).toBeGreaterThan(0);
    expect((await screen.findAllByText("member@example.com")).length).toBeGreaterThan(0);
    expect(screen.getByRole("link", { name: "Provider connections" })).toHaveAttribute(
      "href",
      "/providers",
    );
    expect(screen.getByRole("link", { name: "Team access" })).toHaveAttribute(
      "href",
      "/admin/access",
    );
    expect(screen.queryByText(/quota/i)).not.toBeInTheDocument();

    await userEvent.selectOptions(screen.getByLabelText("Current team"), "team-b");

    await waitFor(() => {
      expect(fetchSpy.mock.calls.some(([input, init]) =>
        String(input).endsWith("/api/v1/auth/team") && init?.method === "POST",
      )).toBe(true);
    });
  });

  it("confirms token revoke and scopes pending state to the selected token", async () => {
    let releaseRevoke: ((response: Response) => void) | undefined;
    const revokeResponse = new Promise<Response>((resolve) => {
      releaseRevoke = resolve;
    });
    vi.spyOn(globalThis, "fetch").mockImplementation(
      async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = String(input);
        if (url.includes("/api/v1/auth/me")) return jsonResponse(ownerMe);
        if (url.endsWith("/api/v1/teams/team-a")) return jsonResponse(teamDetail);
        if (
          url.endsWith("/api/v1/tokens/abc12345") &&
          init?.method === "DELETE"
        ) {
          return revokeResponse;
        }
        if (url.endsWith("/api/v1/tokens")) {
          return jsonResponse({
            items: [
              {
                name: "CLI submit",
                token_hash_prefix: "abc12345",
                type: "team",
                scopes: ["read:own", "submit"],
                team_id: "team-a",
                issued_at: "2026-06-22T00:00:00Z",
                expires_at: "2026-07-22T00:00:00Z",
                revoked_at: null,
                last_used_at: null,
              },
              {
                name: "Read only",
                token_hash_prefix: "def67890",
                type: "team",
                scopes: ["read:own"],
                team_id: "team-a",
                issued_at: "2026-06-22T00:00:00Z",
                expires_at: "2026-07-22T00:00:00Z",
                revoked_at: null,
                last_used_at: null,
              },
            ],
          });
        }
        return jsonResponse({ detail: `unhandled ${url}` }, 404);
      },
    );
    const user = userEvent.setup();
    renderWithProviders(<Settings />, { route: "/settings" });

    const firstRow = (await screen.findByText("CLI submit")).closest("tr");
    const otherRow = screen.getByText("Read only").closest("tr");
    expect(firstRow).not.toBeNull();
    expect(otherRow).not.toBeNull();
    await user.click(within(firstRow!).getByRole("button", { name: "Revoke" }));
    expect(
      screen.getByRole("dialog", { name: "Revoke API token" }),
    ).toHaveTextContent("CLI submit (abc12345)");
    await user.click(screen.getByRole("button", { name: "Revoke token" }));

    await waitFor(() => {
      expect(firstRow!.querySelector("button")).toBeDisabled();
      expect(otherRow!.querySelector("button")).toBeEnabled();
    });
    releaseRevoke?.(new Response(null, { status: 204 }));
    await waitFor(() =>
      expect(
        screen.queryByRole("dialog", { name: "Revoke API token" }),
      ).not.toBeInTheDocument(),
    );
  });

  it("routes platform-admin token mutations through audited Team access", async () => {
    vi.spyOn(globalThis, "fetch").mockImplementation(
      async (input: RequestInfo | URL) => {
        const url = String(input);
        if (url.includes("/api/v1/auth/me")) return jsonResponse(adminMe);
        if (url.endsWith("/api/v1/teams/team-a")) return jsonResponse(teamDetail);
        if (url.endsWith("/api/v1/tokens")) {
          return jsonResponse({
            items: [
              {
                name: "Admin-visible token",
                token_hash_prefix: "admin123",
                type: "team",
                scopes: ["read:own"],
                team_id: "team-a",
                issued_at: "2026-06-22T00:00:00Z",
                expires_at: "2027-06-22T00:00:00Z",
                revoked_at: null,
                last_used_at: null,
              },
            ],
          });
        }
        return jsonResponse({ detail: `unhandled ${url}` }, 404);
      },
    );

    renderWithProviders(<Settings />, { route: "/settings" });

    expect(await screen.findByText("Admin-visible token")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Revoke" })).not.toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Create CLI token" })).toHaveAttribute(
      "href",
      "/admin/access",
    );
  });
});
