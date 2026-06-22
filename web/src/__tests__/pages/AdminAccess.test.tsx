import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import AdminAccess from "../../pages/AdminAccess";
import { renderWithProviders } from "../../test-utils/renderWithProviders";

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

const platformAdminMe = {
  user: {
    id: "admin-user",
    email: "admin@example.com",
    display_name: "Admin Example",
    is_platform_admin: true,
  },
  teams: [{ id: "team-1", name: "latent-team", role: "platform_admin" }],
  current_team: { id: "team-1", name: "latent-team", role: "platform_admin" },
  role: "platform_admin",
  scopes: ["admin:platform"],
  is_platform_admin: true,
  csrf_token: "csrf-admin",
};

const ownerMe = {
  user: {
    id: "owner-user",
    email: "owner@example.com",
    display_name: "Owner Example",
    is_platform_admin: false,
  },
  teams: [{ id: "team-1", name: "latent-team", role: "owner" }],
  current_team: { id: "team-1", name: "latent-team", role: "owner" },
  role: "owner",
  scopes: ["read:own", "submit", "tokens:manage", "providers:manage", "team:manage"],
  is_platform_admin: false,
  csrf_token: "csrf-owner",
};

describe("AdminAccess", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("reviews registrations and reveals approved invite link once", async () => {
    window.localStorage.setItem("loom_token", "loom_admin_secret");
    const fetchSpy = vi.spyOn(global, "fetch").mockImplementation(
      async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = String(input);
        if (url.includes("/api/v1/auth/me")) {
          return jsonResponse(platformAdminMe);
        }
        if (url.includes("/api/v1/admin/team-registrations")) {
          if (init?.method === "POST" && url.endsWith("/approve")) {
            return jsonResponse({
              registration: {
                id: "reg-1",
                name: "latent-team",
                contact_email: "latent@example.com",
                status: "approved",
                requested_at: "2026-06-16T00:00:00Z",
                reviewed_at: "2026-06-16T00:01:00Z",
                reviewed_by_actor: "qianyi",
                approved_team_id: "team-1",
              },
              team: { id: "team-1", name: "latent-team" },
              invite: {
                id: "invite-1",
                team_id: "team-1",
                team_name: "latent-team",
                email: "latent@example.com",
                allowed_domain: null,
                role: "owner",
                status: "pending",
                code_prefix: "abc12345",
                max_uses: 1,
                accepted_uses: 0,
                created_by_actor: "qianyi",
                created_at: "2026-06-16T00:01:00Z",
                expires_at: "2026-06-30T00:01:00Z",
                last_sent_at: "2026-06-16T00:01:00Z",
                accepted_at: null,
                revoked_at: null,
              },
              invite_code: "loom_invite_revealed",
              invite_link: "https://loom.example.com/invites/accept?code=loom_invite_revealed",
            });
          }
          return jsonResponse({
            items: [
              {
                id: "reg-1",
                name: "latent-team",
                contact_email: "latent@example.com",
                status: "pending",
                requested_at: "2026-06-16T00:00:00Z",
                reviewed_at: null,
                reviewed_by_actor: null,
                approved_team_id: null,
              },
            ],
          });
        }
        if (url.includes("/api/v1/admin/audit-events")) {
          return jsonResponse({
            items: [
              {
                id: "audit-1",
                created_at: "2026-06-16T00:01:00Z",
                actor: "qianyi",
                action: "team_registration.approve",
                target_type: "team_registration",
                target_id: "reg-1",
                request_id: null,
                source_ip_hash: null,
                user_agent_hash: null,
                metadata: { team_id: "team-1" },
              },
            ],
            next_cursor: null,
          });
        }
        if (url.includes("/api/v1/invites")) {
          return jsonResponse({
            items: [
              {
                id: "invite-1",
                team_id: "team-1",
                team_name: "latent-team",
                email: "latent@example.com",
                allowed_domain: null,
                role: "owner",
                status: "pending",
                code_prefix: "abc12345",
                max_uses: 1,
                accepted_uses: 0,
                created_by_actor: "qianyi",
                created_at: "2026-06-16T00:01:00Z",
                expires_at: "2026-06-30T00:01:00Z",
                last_sent_at: "2026-06-16T00:01:00Z",
                accepted_at: null,
                revoked_at: null,
              },
            ],
          });
        }
        if (url.endsWith("/api/v1/tokens")) {
          return jsonResponse({ items: [] });
        }
        return jsonResponse({ detail: `unhandled ${url}` }, 404);
      },
    );

    renderWithProviders(<AdminAccess />);

    expect((await screen.findAllByText("latent-team")).length).toBeGreaterThan(0);
    expect(screen.getAllByText("latent@example.com").length).toBeGreaterThan(0);
    expect(screen.getByText("team_registration.approve")).toBeInTheDocument();

    await userEvent.clear(screen.getByLabelText("Admin actor"));
    await userEvent.type(screen.getByLabelText("Admin actor"), "qianyi");
    await userEvent.click(screen.getByRole("button", { name: "Approve" }));

    expect(await screen.findByText(
      "https://loom.example.com/invites/accept?code=loom_invite_revealed",
    )).toBeInTheDocument();
    expect(screen.getByText("Pending invites")).toBeInTheDocument();
    await waitFor(() => {
      const approveCall = fetchSpy.mock.calls.find(([input]) =>
        String(input).endsWith("/reg-1/approve"),
      );
      expect(approveCall).toBeTruthy();
      const init = approveCall?.[1] as RequestInit;
      expect((init.headers as Record<string, string>)["X-Loom-Admin-Actor"]).toBe(
        "qianyi",
      );
    });
  });

  it("manages named API tokens without exposing stored raw secrets", async () => {
    const fetchSpy = vi.spyOn(global, "fetch").mockImplementation(
      async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = String(input);
        if (url.includes("/api/v1/auth/me")) {
          return jsonResponse(platformAdminMe);
        }
        if (url.includes("/api/v1/admin/team-registrations")) {
          return jsonResponse({ items: [] });
        }
        if (url.includes("/api/v1/admin/audit-events")) {
          return jsonResponse({ items: [], next_cursor: null });
        }
        if (url.includes("/api/v1/invites")) {
          return jsonResponse({ items: [] });
        }
        if (url.endsWith("/api/v1/tokens") && (!init?.method || init.method === "GET")) {
          return jsonResponse({
            items: [
              {
                name: "CLI submit",
                token_hash_prefix: "abc12345",
                type: "team",
                scopes: ["read:own", "submit"],
                team_id: "team-1",
                issued_at: "2026-06-22T00:00:00Z",
                expires_at: "2026-07-22T00:00:00Z",
                revoked_at: null,
                last_used_at: null,
                created_by_actor: "user:owner@example.com",
                created_by_user_id: "user-1",
              },
            ],
          });
        }
        if (url.endsWith("/api/v1/tokens") && init?.method === "POST") {
          return jsonResponse({
            token: "loom_api_revealed_create_secret",
            token_hash_prefix: "new12345",
            expires_at: "2026-07-22T00:00:00Z",
            item: {
              name: "Nightly CLI",
              token_hash_prefix: "new12345",
              type: "team",
              scopes: ["read:own", "submit", "providers:manage"],
              team_id: "team-1",
              issued_at: "2026-06-22T00:01:00Z",
              expires_at: "2026-07-22T00:00:00Z",
              revoked_at: null,
              last_used_at: null,
              created_by_actor: "user:owner@example.com",
              created_by_user_id: "user-1",
            },
          }, 201);
        }
        if (url.endsWith("/api/v1/tokens/abc12345/rotate")) {
          return jsonResponse({
            token: "loom_api_revealed_rotate_secret",
            token_hash_prefix: "rot12345",
            expires_at: "2026-07-22T00:00:00Z",
            item: {
              name: "CLI submit",
              token_hash_prefix: "rot12345",
              type: "team",
              scopes: ["read:own", "submit"],
              team_id: "team-1",
              issued_at: "2026-06-22T00:02:00Z",
              expires_at: "2026-07-22T00:00:00Z",
              revoked_at: null,
              last_used_at: null,
              created_by_actor: "user:owner@example.com",
              created_by_user_id: "user-1",
            },
          }, 201);
        }
        if (url.endsWith("/api/v1/tokens/abc12345") && init?.method === "DELETE") {
          return new Response(null, { status: 204 });
        }
        return jsonResponse({ detail: `unhandled ${url}` }, 404);
      },
    );

    renderWithProviders(<AdminAccess />);

    expect(await screen.findByText("API tokens")).toBeInTheDocument();
    expect(await screen.findByText("CLI submit")).toBeInTheDocument();
    expect(screen.getByText("abc12345")).toBeInTheDocument();
    expect(screen.queryByText("loom_api_revealed_create_secret")).not.toBeInTheDocument();

    await userEvent.type(screen.getByLabelText("Token name"), "Nightly CLI");
    await userEvent.click(screen.getByLabelText("Manage provider connections"));
    await userEvent.click(screen.getByRole("button", { name: "Create API token" }));

    expect(await screen.findByText("loom_api_revealed_create_secret")).toBeInTheDocument();
    expect(screen.getByText("CLI setup commands")).toBeInTheDocument();
    expect(screen.getByText("export LOOM_API_TOKEN=loom_api_revealed_create_secret")).toBeInTheDocument();
    expect(screen.getByText("loom auth login --server <server-url> --token env:LOOM_API_TOKEN")).toBeInTheDocument();
    expect(screen.getByText("loom auth whoami")).toBeInTheDocument();
    await waitFor(() => {
      const createCall = fetchSpy.mock.calls.find(([input, init]) =>
        String(input).endsWith("/api/v1/tokens") && init?.method === "POST",
      );
      expect(createCall).toBeTruthy();
      const body = JSON.parse(String(createCall?.[1]?.body));
      expect(body).toEqual({
        name: "Nightly CLI",
        type: "team",
        scopes: ["read:own", "submit", "providers:manage"],
        expires_in_days: 30,
      });
    });

    await userEvent.click(screen.getByRole("button", { name: "Rotate CLI submit" }));
    expect(await screen.findByText("loom_api_revealed_rotate_secret")).toBeInTheDocument();
    await userEvent.click(screen.getByRole("button", { name: "Revoke CLI submit" }));

    await waitFor(() => {
      expect(fetchSpy.mock.calls.some(([input, init]) =>
        String(input).endsWith("/api/v1/tokens/abc12345/rotate") &&
        init?.method === "POST",
      )).toBe(true);
      expect(fetchSpy.mock.calls.some(([input, init]) =>
        String(input).endsWith("/api/v1/tokens/abc12345") &&
        init?.method === "DELETE",
      )).toBe(true);
    });
  });

  it("lets team owners manage invites and tokens without platform-admin registration calls", async () => {
    const fetchSpy = vi.spyOn(global, "fetch").mockImplementation(
      async (input: RequestInfo | URL) => {
        const url = String(input);
        if (url.includes("/api/v1/auth/me")) {
          return jsonResponse(ownerMe);
        }
        if (url.includes("/api/v1/invites")) {
          return jsonResponse({ items: [] });
        }
        if (url.endsWith("/api/v1/tokens")) {
          return jsonResponse({ items: [] });
        }
        if (url.includes("/api/v1/admin/team-registrations")) {
          return jsonResponse({ detail: "platform admin required" }, 403);
        }
        if (url.includes("/api/v1/admin/audit-events")) {
          return jsonResponse({ detail: "platform admin required" }, 403);
        }
        return jsonResponse({ detail: `unhandled ${url}` }, 404);
      },
    );

    renderWithProviders(<AdminAccess />);

    expect(await screen.findByText("API tokens")).toBeInTheDocument();
    expect(screen.getAllByText("Create invite").length).toBeGreaterThan(0);
    expect(screen.queryByText("Pending registrations")).not.toBeInTheDocument();
    expect(screen.queryByText("Admin actor")).not.toBeInTheDocument();

    await waitFor(() => {
      expect(fetchSpy.mock.calls.some(([input]) =>
        String(input).includes("/api/v1/admin/team-registrations"),
      )).toBe(false);
      expect(fetchSpy.mock.calls.some(([input]) =>
        String(input).includes("/api/v1/admin/audit-events"),
      )).toBe(false);
    });
  });
});
