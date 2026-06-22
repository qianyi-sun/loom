import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

import InviteAccept from "../../pages/InviteAccept";
import { renderWithProviders } from "../../test-utils/renderWithProviders";

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

const authMe = {
  user: {
    id: "user-1",
    email: "beta@example.com",
    display_name: null,
    is_platform_admin: false,
  },
  teams: [{ id: "team-1", name: "Invite Alpha", role: "member" }],
  current_team: { id: "team-1", name: "Invite Alpha", role: "member" },
  role: "member",
  scopes: ["read:own", "submit"],
  is_platform_admin: false,
  csrf_token: "csrf-next",
};

describe("InviteAccept", () => {
  it("explains the invite and accepts it with the intended email", async () => {
    const fetchSpy = vi.spyOn(global, "fetch").mockImplementation(
      async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = String(input);
        if (url.includes("/api/v1/auth/me")) {
          return jsonResponse({ detail: "unauthorized" }, 401);
        }
        if (url.includes("/api/v1/invites/lookup")) {
          return jsonResponse({
            team_name: "Invite Alpha",
            role: "member",
            status: "pending",
            code_prefix: "abc12345",
          });
        }
        if (url.includes("/api/v1/invites/accept")) {
          expect(init?.method).toBe("POST");
          expect(JSON.parse(String(init?.body))).toEqual({
            code: "loom_invite_abc",
            email: "beta@example.com",
          });
          return jsonResponse(authMe);
        }
        return jsonResponse({ detail: `unhandled ${url}` }, 404);
      },
    );

    renderWithProviders(<InviteAccept />, {
      route: "/invites/accept?code=loom_invite_abc",
    });

    expect(await screen.findByText("Invite Alpha")).toBeInTheDocument();
    expect(screen.getByText("member")).toBeInTheDocument();

    await userEvent.type(screen.getByLabelText("Invite email"), "beta@example.com");
    await userEvent.click(screen.getByRole("button", { name: "Accept invite" }));

    expect(await screen.findByText("Joined Invite Alpha")).toBeInTheDocument();
    await waitFor(() => {
      expect(fetchSpy).toHaveBeenCalledWith(
        expect.stringContaining("/api/v1/invites/accept"),
        expect.any(Object),
      );
    });
  });

  it.each([
    ["expired", "Invite expired"],
    ["revoked", "Invite revoked"],
    ["accepted", "Invite already used"],
  ])("handles %s invites without accepting", async (status, label) => {
    vi.spyOn(global, "fetch").mockImplementation(
      async (input: RequestInfo | URL) => {
        const url = String(input);
        if (url.includes("/api/v1/auth/me")) {
          return jsonResponse({ detail: "unauthorized" }, 401);
        }
        if (url.includes("/api/v1/invites/lookup")) {
          return jsonResponse({
            team_name: "Invite Alpha",
            role: "member",
            status,
            code_prefix: "abc12345",
          });
        }
        return jsonResponse({ detail: `unhandled ${url}` }, 404);
      },
    );

    renderWithProviders(<InviteAccept />, {
      route: "/invites/accept?code=loom_invite_abc",
    });

    expect((await screen.findAllByText(label)).length).toBeGreaterThan(0);
    expect(
      screen.queryByRole("button", { name: "Accept invite" }),
    ).not.toBeInTheDocument();
  });
});
