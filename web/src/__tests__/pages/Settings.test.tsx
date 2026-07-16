import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import { setFrontendConfigForTests } from "../../lib/frontendConfig";
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

  it("shows admin-reviewed onboarding choices and CLI setup guidance when signed out", async () => {
    setFrontendConfigForTests({
      environment: "staging",
      environmentLabel: "Staging",
      routePath: "/dev",
      apiBase: "/dev",
      apiRouteBase: `${window.location.origin}/dev/api`,
    });
    const fetchSpy = vi.spyOn(globalThis, "fetch").mockImplementation(async (
      input: RequestInfo | URL,
      init?: RequestInit,
    ) => {
      const url = String(input);
      if (url.includes("/api/v1/auth/me")) {
        return jsonResponse({ detail: "unauthorized" }, 401);
      }
      if (url.endsWith("/api/v1/auth/public-teams")) {
        return jsonResponse({
          items: [{ id: "team-research", name: "Research" }],
        });
      }
      if (url.endsWith("/api/v1/auth/login") && init?.method === "POST") {
        expect(JSON.parse(String(init.body))).toEqual({
          username: "Mark",
          password: "long-passphrase-1",
        });
        return jsonResponse({
          ...ownerMe,
          user: { ...ownerMe.user, username: "Mark" },
          current_team: { id: "team-research", name: "Research", role: "member" },
          csrf_token: "csrf-login",
        });
      }
      if (
        url.endsWith("/api/v1/auth/registration-requests") &&
        init?.method === "POST"
      ) {
        expect(JSON.parse(String(init.body))).toEqual({
          username: "Mark",
          team_id: "team-research",
          metadata: {},
        });
        return jsonResponse({
          id: "reg-1",
          username: "Mark",
          team: { id: "team-research", name: "Research" },
          status: "pending",
          requested_at: "2026-06-24T00:00:00Z",
          reviewed_at: null,
          reviewed_by_actor: null,
          approved_team_id: null,
        }, 201);
      }
      if (
        url.endsWith("/api/v1/auth/password-reset-requests") &&
        init?.method === "POST"
      ) {
        expect(JSON.parse(String(init.body))).toEqual({ username: "Mark" });
        return jsonResponse({
          id: "reset-1",
          username: "Mark",
          status: "pending",
          requested_at: "2026-06-24T00:00:00Z",
        }, 202);
      }
      return jsonResponse({ detail: `unhandled ${url}` }, 404);
    });

    renderWithProviders(<Settings />, { route: "/settings" });

    expect(await screen.findByRole("heading", { name: "Sign in" })).toBeInTheDocument();
    expect(screen.queryByText(/send a one-time sign-in link/i)).not.toBeInTheDocument();
    expect(screen.queryByText(/check your email/i)).not.toBeInTheDocument();
    expect(screen.queryByText(/invite link/i)).not.toBeInTheDocument();
    expect(screen.queryByText(/one-time login code/i)).not.toBeInTheDocument();
    expect(screen.getByText(/Staging account requests are reviewed by an admin/i)).toBeInTheDocument();
    expect(screen.getAllByText("Request account").length).toBeGreaterThan(0);
    expect(screen.getByText("Forgot password")).toBeInTheDocument();
    expect(screen.getByText("CLI setup")).toBeInTheDocument();
    expect(screen.getByText("First run checklist")).toBeInTheDocument();
    expect(screen.getByText(/export LOOM_PASSWORD=\.\.\./)).toBeInTheDocument();
    expect(screen.getByText(/loom auth whoami/)).toBeInTheDocument();
    const cliCommand = screen.getByText(/loom auth login --server/i);
    expect(cliCommand).toHaveTextContent(`${window.location.origin}/dev`);
    expect(cliCommand).toHaveTextContent("--username USER --password env:LOOM_PASSWORD");
    expect(cliCommand).not.toHaveTextContent("<server-url>");
    expect(cliCommand).not.toHaveTextContent("LOOM_API_TOKEN");
    expect(cliCommand.closest("pre")).toHaveClass("whitespace-pre-wrap");

    expect(await screen.findByRole("option", { name: "Research" })).toBeInTheDocument();
    await userEvent.type(screen.getByLabelText("Requested username"), "Mark");
    await userEvent.selectOptions(screen.getByLabelText("Registration team"), "team-research");
    await userEvent.click(screen.getByRole("button", { name: "Request account" }));

    expect(await screen.findByText(
      "Request submitted. An admin will review it and share a password setup link manually if approved.",
    )).toBeInTheDocument();
    expect(fetchSpy.mock.calls.some(([input, init]) =>
      String(input).endsWith("/api/v1/auth/registration-requests") && init?.method === "POST",
    )).toBe(true);

    await userEvent.type(screen.getByLabelText("Reset username"), "Mark");
    await userEvent.click(screen.getByRole("button", { name: "Request reset" }));

    expect(await screen.findByText(
      "Reset request submitted. An admin will share a reset link if approved.",
    )).toBeInTheDocument();
    expect(fetchSpy.mock.calls.some(([input, init]) =>
      String(input).endsWith("/api/v1/auth/password-reset-requests") &&
        init?.method === "POST",
    )).toBe(true);

    await userEvent.type(screen.getByLabelText("Username"), "Mark");
    await userEvent.type(screen.getByLabelText("Password"), "long-passphrase-1");
    await userEvent.click(screen.getByRole("button", { name: "Sign in" }));

    await waitFor(() => {
      expect(fetchSpy.mock.calls.some(([input, init]) =>
        String(input).endsWith("/api/v1/auth/login") && init?.method === "POST",
      )).toBe(true);
    });
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
});
