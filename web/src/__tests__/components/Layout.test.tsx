import { screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import App from "../../App";
import { setBrowserFailureReporter } from "../../lib/errorReporting";
import { setFrontendConfigForTests } from "../../lib/frontendConfig";
import { renderWithProviders } from "../../test-utils/renderWithProviders";

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

describe("Layout", () => {
  afterEach(() => {
    setBrowserFailureReporter(null);
    setFrontendConfigForTests(null);
    window.history.replaceState({}, "", "/");
    vi.restoreAllMocks();
  });

  it("renders a non-empty accessible shell while the session is loading", () => {
    vi.spyOn(globalThis, "fetch").mockReturnValue(
      new Promise<Response>(() => undefined),
    );

    renderWithProviders(<App />, { route: "/settings" });

    expect(screen.getByRole("status")).toHaveTextContent(
      "Checking your browser session",
    );
    expect(screen.getAllByRole("main")).toHaveLength(1);
    expect(screen.getByRole("main")).toHaveAttribute("id", "main-content");
    expect(screen.getByRole("link", { name: "Skip to main content" })).toHaveAttribute(
      "href",
      "#main-content",
    );
  });

  it.each([
    ["development", "/dev"],
    ["production", "/prod"],
  ] as const)(
    "renders a redacted actionable session failure under %s",
    async (environment, routePath) => {
      const reports: Array<{
        kind: string;
        pathname: string;
        referenceId: string;
      }> = [];
      setBrowserFailureReporter((report) => reports.push(report));
      window.history.replaceState({}, "", `${routePath}/settings?token=raw-secret`);
      setFrontendConfigForTests({
        environment,
        environmentLabel:
          environment === "production" ? "Production" : "Development",
        routePath,
        apiBase: routePath,
        apiRouteBase: `${window.location.origin}${routePath}/api`,
      });
      const response = jsonResponse(
        { detail: "proxy failure token=raw-secret signed_url=https://secret" },
        503,
      );
      const fetchSpy = vi.spyOn(globalThis, "fetch").mockResolvedValue(response);

      renderWithProviders(<App />, { route: "/settings" });

      expect(
        await screen.findByRole("heading", {
          name: "Loom could not verify your session",
        }),
      ).toBeInTheDocument();
      const alert = screen.getByRole("alert");
      expect(alert).toHaveTextContent(
        "browser session service is temporarily unavailable",
      );
      expect(alert).toHaveTextContent(/Support reference: WEB-[0-9A-F]{8}/);
      expect(alert).not.toHaveTextContent("raw-secret");
      expect(alert).not.toHaveTextContent("signed_url");
      expect(screen.getByRole("button", { name: "Retry" })).toBeInTheDocument();
      expect(screen.getByRole("button", { name: "Reload Loom" })).toBeInTheDocument();
      expect(screen.getByRole("link", { name: "Go to Loom home" })).toHaveAttribute(
        "href",
        `${routePath}/`,
      );
      expect(screen.getAllByRole("main")).toHaveLength(1);
      expect(screen.queryByRole("heading", { name: "Sign in" })).not.toBeInTheDocument();
      expect(fetchSpy).toHaveBeenCalledTimes(1);
      expect(response.bodyUsed).toBe(false);

      const referenceId = alert.textContent?.match(/WEB-[0-9A-F]{8}/u)?.[0];
      expect(reports).toEqual([
        expect.objectContaining({
          kind: "auth-session-http",
          pathname: `${routePath}/settings`,
          referenceId,
        }),
      ]);
    },
  );

  it("retries session verification in place and resumes the original route", async () => {
    const fetchSpy = vi.spyOn(globalThis, "fetch").mockImplementation(async (
      input: RequestInfo | URL,
    ) => {
      const url = String(input);
      if (url.includes("/api/v1/auth/me")) {
        const authCalls = fetchSpy.mock.calls.filter(([candidate]) =>
          String(candidate).includes("/api/v1/auth/me"),
        ).length;
        if (authCalls === 1) {
          return jsonResponse({ detail: "proxy failure raw-secret" }, 503);
        }
        return new Response("", { status: 401 });
      }
      if (url.endsWith("/api/v1/auth/public-teams")) {
        return jsonResponse({ items: [] });
      }
      return jsonResponse({ detail: `unhandled ${url}` }, 404);
    });
    const user = userEvent.setup();

    renderWithProviders(<App />, { route: "/settings" });
    expect(
      await screen.findByRole("heading", {
        name: "Loom could not verify your session",
      }),
    ).toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "Retry" }));

    expect(await screen.findByRole("heading", { name: "Sign in" })).toBeInTheDocument();
    expect(screen.queryByText("raw-secret")).not.toBeInTheDocument();
    expect(
      fetchSpy.mock.calls.filter(([candidate]) =>
        String(candidate).includes("/api/v1/auth/me"),
      ),
    ).toHaveLength(2);
  });

  it("fails closed when a sign-in response cannot establish session state", async () => {
    const reports: Array<{ kind: string; referenceId: string }> = [];
    setBrowserFailureReporter((report) => reports.push(report));
    const loginFailure = jsonResponse(
      { detail: "proxy token=raw-secret signed_url=https://secret" },
      503,
    );
    const fetchSpy = vi.spyOn(globalThis, "fetch").mockImplementation(async (
      input: RequestInfo | URL,
      init?: RequestInit,
    ) => {
      const url = String(input);
      if (url.includes("/api/v1/auth/me")) {
        return new Response("", { status: 401 });
      }
      if (url.endsWith("/api/v1/auth/public-teams")) {
        return jsonResponse({ items: [] });
      }
      if (url.endsWith("/api/v1/auth/login") && init?.method === "POST") {
        return loginFailure;
      }
      return jsonResponse({ detail: `unhandled ${url}` }, 404);
    });
    const user = userEvent.setup();
    renderWithProviders(<App />, { route: "/settings" });
    expect(await screen.findByRole("heading", { name: "Sign in" })).toBeInTheDocument();
    await user.type(screen.getByLabelText("Username"), "Owner");
    await user.type(screen.getByLabelText("Password"), "long-passphrase-1");

    await user.click(screen.getByRole("button", { name: "Sign in" }));

    const heading = await screen.findByRole("heading", {
      name: "Loom could not verify your session",
    });
    const alert = screen.getByRole("alert");
    expect(heading).toBeInTheDocument();
    expect(alert).toHaveTextContent(
      "browser session service is temporarily unavailable",
    );
    expect(alert).not.toHaveTextContent("raw-secret");
    expect(alert).not.toHaveTextContent("signed_url");
    expect(loginFailure.bodyUsed).toBe(false);
    expect(screen.queryByRole("heading", { name: "Sign in" })).not.toBeInTheDocument();
    expect(screen.getAllByRole("main")).toHaveLength(1);
    const referenceId = alert.textContent?.match(/WEB-[0-9A-F]{8}/u)?.[0];
    expect(reports).toEqual([
      expect.objectContaining({ kind: "auth-session-http", referenceId }),
    ]);
    expect(
      fetchSpy.mock.calls.filter(([input]) =>
        String(input).endsWith("/api/v1/auth/login"),
      ),
    ).toHaveLength(1);
  });

  it("passes current team context into the authenticated app shell", async () => {
    vi.spyOn(globalThis, "fetch").mockImplementation(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.includes("/api/v1/auth/me")) {
        return jsonResponse({
          user: {
            id: "user-1",
            username: "Owner",
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
    const identityContext = within(nav).getByLabelText("Current user and team");
    expect(identityContext).toHaveTextContent("Owner / EAI");
    expect(identityContext).toHaveTextContent("owner");
    expect(screen.getByRole("main")).toHaveAttribute("id", "main-content");
  });

  it("gives the signed-out settings page a wide public onboarding shell", async () => {
    vi.spyOn(globalThis, "fetch").mockImplementation(async (input: RequestInfo | URL) => {
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
    expect(screen.getByRole("main")).toHaveAttribute("id", "main-content");
  });

  it("keeps invite acceptance reachable before sign-in", async () => {
    vi.spyOn(globalThis, "fetch").mockImplementation(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.includes("/api/v1/auth/me")) {
        return jsonResponse({ detail: "unauthorized" }, 401);
      }
      if (url.includes("/api/v1/invites/lookup")) {
        return jsonResponse({
          team_name: "Staging",
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

    expect(await screen.findByText("Staging")).toBeInTheDocument();
    expect(
      screen.queryByRole("heading", { name: "Sign in" }),
    ).not.toBeInTheDocument();
  });

  it.each([
    ["/auth/setup?token=loom_setup_abc", "/api/v1/auth/setup/lookup", "Set Password"],
    ["/auth/reset?token=loom_reset_abc", "/api/v1/auth/reset/lookup", "Reset Password"],
  ])("keeps %s reachable before sign-in", async (route, lookupPath, heading) => {
    vi.spyOn(globalThis, "fetch").mockImplementation(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.includes("/api/v1/auth/me")) {
        return jsonResponse({ detail: "unauthorized" }, 401);
      }
      if (url.includes(lookupPath)) {
        return jsonResponse({
          username: "Hongjian",
          team: { id: "team-admin", name: "admin" },
          expires_at: "2026-07-01T00:00:00Z",
        });
      }
      return jsonResponse({ detail: `unhandled ${url}` }, 404);
    });

    renderWithProviders(<App />, { route });

    expect(await screen.findByRole("heading", { name: heading })).toBeInTheDocument();
    expect(await screen.findByText("Account: Hongjian / admin")).toBeInTheDocument();
    expect(
      screen.queryByRole("heading", { name: "Sign in" }),
    ).not.toBeInTheDocument();
  });
});
