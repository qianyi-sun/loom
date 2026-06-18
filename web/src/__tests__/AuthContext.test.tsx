import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { AuthProvider } from "../auth/AuthContext";
import { useAuth } from "../auth/useAuth";
import { apiFetch } from "../api/client";

function Display(): JSX.Element {
  const { token, tokenError, setToken, clearToken } = useAuth();
  return (
    <div>
      <span data-testid="t">{token ?? "none"}</span>
      <span data-testid="err">{tokenError ?? "no-error"}</span>
      <button onClick={() => setToken("xyz")}>set</button>
      <button onClick={() => clearToken()}>clear</button>
    </div>
  );
}

function withQueryClient(ui: React.ReactNode, qc?: QueryClient): JSX.Element {
  const client = qc ?? new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return <QueryClientProvider client={client}>{ui}</QueryClientProvider>;
}

describe("AuthContext", () => {
  beforeEach(() => window.localStorage.clear());

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("reads token from localStorage on mount", () => {
    window.localStorage.setItem("loom_token", "stored");
    render(
      withQueryClient(
        <AuthProvider>
          <Display />
        </AuthProvider>,
      ),
    );
    expect(screen.getByTestId("t").textContent).toBe("stored");
  });

  it("setToken persists to localStorage", async () => {
    // Probe the auth endpoint; stub fetch so the probe doesn't 401.
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(
        new Response(JSON.stringify({ items: [] }), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        }),
      ),
    );
    const user = userEvent.setup();
    render(
      withQueryClient(
        <AuthProvider>
          <Display />
        </AuthProvider>,
      ),
    );
    await user.click(screen.getByText("set"));
    expect(window.localStorage.getItem("loom_token")).toBe("xyz");
    expect(screen.getByTestId("t").textContent).toBe("xyz");
  });

  it("clearToken removes localStorage entry", async () => {
    window.localStorage.setItem("loom_token", "abc");
    const user = userEvent.setup();
    render(
      withQueryClient(
        <AuthProvider>
          <Display />
        </AuthProvider>,
      ),
    );
    await user.click(screen.getByText("clear"));
    expect(window.localStorage.getItem("loom_token")).toBe(null);
    expect(screen.getByTestId("t").textContent).toBe("none");
  });

  it("setToken clears the React Query cache so prior-team data does not leak", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(
        new Response(JSON.stringify({ items: [] }), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        }),
      ),
    );
    const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    qc.setQueryData(["batches"], { items: [{ id: "leftover-batch" }] });
    const user = userEvent.setup();
    render(
      withQueryClient(
        <AuthProvider>
          <Display />
        </AuthProvider>,
        qc,
      ),
    );
    await user.click(screen.getByText("set"));
    // Cache should be cleared synchronously inside setToken.
    expect(qc.getQueryData(["batches"])).toBeUndefined();
  });

  it("clearToken clears the React Query cache", async () => {
    window.localStorage.setItem("loom_token", "abc");
    const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    qc.setQueryData(["batches"], { items: [{ id: "leftover-batch" }] });
    const user = userEvent.setup();
    render(
      withQueryClient(
        <AuthProvider>
          <Display />
        </AuthProvider>,
        qc,
      ),
    );
    await user.click(screen.getByText("clear"));
    expect(qc.getQueryData(["batches"])).toBeUndefined();
  });

  it("setToken with an invalid token surfaces tokenError + clears the stored token", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(
        new Response(JSON.stringify({ detail: "invalid bearer token" }), {
          status: 401,
          headers: { "Content-Type": "application/json" },
        }),
      ),
    );
    const user = userEvent.setup();
    render(
      withQueryClient(
        <AuthProvider>
          <Display />
        </AuthProvider>,
      ),
    );
    await user.click(screen.getByText("set"));
    await waitFor(() => {
      expect(screen.getByTestId("err").textContent).toMatch(/invalid|revoked/i);
    });
    expect(window.localStorage.getItem("loom_token")).toBe(null);
    expect(screen.getByTestId("t").textContent).toBe("none");
  });

  it("keeps the invalid token message visible when a background 401 follows the sign-in probe", async () => {
    let resolveProbe: ((response: Response) => void) | undefined;
    let resolveBackground: ((response: Response) => void) | undefined;
    const unauthorized = () =>
      new Response(JSON.stringify({ detail: "invalid bearer token" }), {
        status: 401,
        headers: { "Content-Type": "application/json" },
      });
    const fetchMock = vi
      .fn()
      .mockImplementationOnce(
        () => new Promise<Response>((resolve) => (resolveProbe = resolve)),
      )
      .mockImplementationOnce(
        () =>
          new Promise<Response>((resolve) => (resolveBackground = resolve)),
      );
    vi.stubGlobal("fetch", fetchMock);

    const user = userEvent.setup();
    render(
      withQueryClient(
        <AuthProvider>
          <Display />
        </AuthProvider>,
      ),
    );

    await user.click(screen.getByText("set"));
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1));

    await act(async () => {
      resolveProbe?.(unauthorized());
    });
    await waitFor(() => {
      expect(screen.getByTestId("err").textContent).toMatch(/invalid|revoked/i);
    });

    const backgroundRequest = apiFetch<unknown>("/api/v1/batches").catch(
      () => undefined,
    );
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2));
    await act(async () => {
      resolveBackground?.(unauthorized());
      await backgroundRequest;
    });

    expect(screen.getByTestId("err").textContent).toMatch(/invalid|revoked/i);
    expect(window.localStorage.getItem("loom_token")).toBe(null);
    expect(screen.getByTestId("t").textContent).toBe("none");
  });

  it("setToken with a non-401 error surfaces tokenError but does NOT clear the token", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(
        new Response(JSON.stringify({ detail: "server exploded" }), {
          status: 500,
          headers: { "Content-Type": "application/json" },
        }),
      ),
    );
    const user = userEvent.setup();
    render(
      withQueryClient(
        <AuthProvider>
          <Display />
        </AuthProvider>,
      ),
    );
    await user.click(screen.getByText("set"));
    await waitFor(() => {
      expect(screen.getByTestId("err").textContent).toMatch(
        /could not verify|server exploded/i,
      );
    });
    // 5xx is not "token is invalid" — keep the token, surface the message.
    expect(window.localStorage.getItem("loom_token")).toBe("xyz");
  });
});
