import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

import AdminAccess from "../../pages/AdminAccess";
import { renderWithProviders } from "../../test-utils/renderWithProviders";

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

describe("AdminAccess", () => {
  it("reviews registrations and reveals approved invite link once", async () => {
    window.localStorage.setItem("loom_token", "loom_admin_secret");
    const fetchSpy = vi.spyOn(global, "fetch").mockImplementation(
      async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = String(input);
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
});
