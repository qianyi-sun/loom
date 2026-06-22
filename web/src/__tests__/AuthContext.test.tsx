import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { AuthProvider } from "../auth/AuthContext";
import { useAuth } from "../auth/useAuth";

const memberMe = {
  user: {
    id: "user-1",
    email: "owner@example.com",
    display_name: "Owner Example",
    is_platform_admin: false,
  },
  teams: [
    { id: "team-a", name: "Alpha", role: "viewer" },
    { id: "team-b", name: "Beta", role: "owner" },
  ],
  current_team: { id: "team-a", name: "Alpha", role: "viewer" },
  role: "viewer",
  scopes: ["read:own"],
  is_platform_admin: false,
  csrf_token: "csrf-initial",
};

function Display(): JSX.Element {
  const {
    me,
    isAuthenticated,
    isLoading,
    isAdmin,
    authError,
    currentTeamId,
    loginStart,
    loginComplete,
    switchTeam,
    logout,
  } = useAuth();
  return (
    <div>
      <span data-testid="loading">{isLoading ? "loading" : "ready"}</span>
      <span data-testid="auth">{isAuthenticated ? "in" : "out"}</span>
      <span data-testid="email">{me?.user.email ?? "none"}</span>
      <span data-testid="team">{currentTeamId ?? "none"}</span>
      <span data-testid="admin">{isAdmin ? "admin" : "not-admin"}</span>
      <span data-testid="err">{authError ?? "no-error"}</span>
      <button onClick={() => void loginStart("owner@example.com")}>start</button>
      <button onClick={() => void loginComplete("login-token")}>complete</button>
      <button onClick={() => void switchTeam("team-b")}>switch</button>
      <button onClick={() => void logout()}>logout</button>
    </div>
  );
}

function withQueryClient(ui: React.ReactNode, qc?: QueryClient): JSX.Element {
  const client = qc ?? new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return <QueryClientProvider client={client}>{ui}</QueryClientProvider>;
}

describe("AuthContext", () => {
  beforeEach(() => {
    window.localStorage.clear();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("loads the browser session from /auth/me on mount", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(
        new Response(JSON.stringify(memberMe), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        }),
      ),
    );
    render(
      withQueryClient(
        <AuthProvider>
          <Display />
        </AuthProvider>,
      ),
    );
    await waitFor(() => expect(screen.getByTestId("auth").textContent).toBe("in"));
    expect(screen.getByTestId("email").textContent).toBe("owner@example.com");
    expect(screen.getByTestId("team").textContent).toBe("team-a");
    expect(window.localStorage.getItem("loom_token")).toBe(null);
  });

  it("treats 401 on mount as a signed-out expired session", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response("", { status: 401 })));
    render(
      withQueryClient(
        <AuthProvider>
          <Display />
        </AuthProvider>,
      ),
    );
    await waitFor(() => expect(screen.getByTestId("loading").textContent).toBe("ready"));
    expect(screen.getByTestId("auth").textContent).toBe("out");
    expect(screen.getByTestId("err").textContent).toBe("no-error");
  });

  it("preserves cached page data when /auth/me fails for a non-auth reason", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(
        new Response(JSON.stringify({ detail: "service unavailable" }), {
          status: 503,
          headers: { "Content-Type": "application/json" },
        }),
      ),
    );
    const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    qc.setQueryData(["tasks"], { items: [{ id: "existing" }] });
    render(
      withQueryClient(
        <AuthProvider>
          <Display />
        </AuthProvider>,
        qc,
      ),
    );

    await waitFor(() => expect(screen.getByTestId("loading").textContent).toBe("ready"));
    expect(screen.getByTestId("auth").textContent).toBe("out");
    expect(screen.getByTestId("err").textContent).toContain("service unavailable");
    expect(qc.getQueryData(["tasks"])).toEqual({ items: [{ id: "existing" }] });
  });

  it("loginComplete stores the returned session state and clears cached team data", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(new Response("", { status: 401 }))
      .mockResolvedValueOnce(
        new Response(JSON.stringify(memberMe), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        }),
      );
    vi.stubGlobal("fetch", fetchMock);
    const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    qc.setQueryData(["batches"], { items: [{ id: "old" }] });
    const user = userEvent.setup();
    render(
      withQueryClient(
        <AuthProvider>
          <Display />
        </AuthProvider>,
        qc,
      ),
    );
    await waitFor(() => expect(screen.getByTestId("auth").textContent).toBe("out"));
    await user.click(screen.getByText("complete"));
    await waitFor(() => expect(screen.getByTestId("auth").textContent).toBe("in"));
    expect(qc.getQueryData(["batches"])).toBeUndefined();
    expect(fetchMock).toHaveBeenLastCalledWith(
      "/api/v1/auth/login/complete",
      expect.objectContaining({ method: "POST", credentials: "include" }),
    );
  });

  it("loginComplete installs the returned CSRF token for the next mutation", async () => {
    const loginMe = { ...memberMe, csrf_token: "csrf-login" };
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(new Response("", { status: 401 }))
      .mockResolvedValueOnce(
        new Response(JSON.stringify(loginMe), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        }),
      )
      .mockResolvedValueOnce(new Response(null, { status: 204 }));
    vi.stubGlobal("fetch", fetchMock);
    const user = userEvent.setup();
    render(withQueryClient(<AuthProvider><Display /></AuthProvider>));

    await waitFor(() => expect(screen.getByTestId("auth").textContent).toBe("out"));
    await user.click(screen.getByText("complete"));
    await waitFor(() => expect(screen.getByTestId("auth").textContent).toBe("in"));
    await user.click(screen.getByText("logout"));

    const logoutInit = fetchMock.mock.calls[2][1] as RequestInit;
    expect((logoutInit.headers as Record<string, string>)["X-Loom-CSRF"]).toBe(
      "csrf-login",
    );
  });

  it("switchTeam updates current team and clears cached data", async () => {
    const switched = {
      ...memberMe,
      current_team: { id: "team-b", name: "Beta", role: "owner" },
      role: "owner",
      csrf_token: "csrf-switched",
    };
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(new Response(JSON.stringify(memberMe), { status: 200 }))
      .mockResolvedValueOnce(new Response(JSON.stringify(switched), { status: 200 }));
    vi.stubGlobal(
      "fetch",
      fetchMock,
    );
    const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    qc.setQueryData(["tokens"], { items: [{ id: "old" }] });
    const user = userEvent.setup();
    render(withQueryClient(<AuthProvider><Display /></AuthProvider>, qc));
    await waitFor(() => expect(screen.getByTestId("team").textContent).toBe("team-a"));
    await user.click(screen.getByText("switch"));
    await waitFor(() => expect(screen.getByTestId("team").textContent).toBe("team-b"));
    expect(qc.getQueryData(["tokens"])).toBeUndefined();
    const switchInit = fetchMock.mock.calls[1][1] as RequestInit;
    expect((switchInit.headers as Record<string, string>)["X-Loom-CSRF"]).toBe(
      "csrf-initial",
    );
  });

  it("logout clears auth state and cache", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(new Response(JSON.stringify(memberMe), { status: 200 }))
      .mockResolvedValueOnce(new Response(null, { status: 204 }));
    vi.stubGlobal(
      "fetch",
      fetchMock,
    );
    const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    qc.setQueryData(["batches"], { items: [{ id: "old" }] });
    const user = userEvent.setup();
    render(withQueryClient(<AuthProvider><Display /></AuthProvider>, qc));
    await waitFor(() => expect(screen.getByTestId("auth").textContent).toBe("in"));
    await user.click(screen.getByText("logout"));
    await waitFor(() => expect(screen.getByTestId("auth").textContent).toBe("out"));
    expect(qc.getQueryData(["batches"])).toBeUndefined();
    const logoutInit = fetchMock.mock.calls[1][1] as RequestInit;
    expect((logoutInit.headers as Record<string, string>)["X-Loom-CSRF"]).toBe(
      "csrf-initial",
    );
  });
});
