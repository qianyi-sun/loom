import { act, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import { Route, Routes, useLocation, useNavigate, Link } from "react-router-dom";

import AdminAuditLog from "../../../components/admin/AdminAuditLog";
import { renderWithProviders } from "../../../test-utils/renderWithProviders";

function AuditRouteProbe() {
  const location = useLocation(); const navigate = useNavigate();
  return <><div aria-label="Current URL">{location.pathname}{location.search}</div><Link to="/away">Leave audit</Link><button onClick={() => navigate(-1)}>Browser back</button><Routes><Route path="/admin/access" element={<AdminAuditLog />} /><Route path="/away" element={<p>Away</p>} /></Routes></>;
}

const adminMe = {
  user: {
    id: "admin-user",
    username: "Qianyi",
    email: "admin@example.com",
    display_name: "Admin Example",
    is_platform_admin: true,
  },
  teams: [{ id: "team-1", name: "Admin", role: "platform_admin" }],
  current_team: { id: "team-1", name: "Admin", role: "platform_admin" },
  role: "platform_admin",
  scopes: ["admin:platform"],
  is_platform_admin: true,
  csrf_token: "csrf-admin-test",
};

const auditPageOne = {
  items: [
    {
      id: "audit-1",
      created_at: "2026-07-10T12:00:00Z",
      actor: "qianyi",
      action: "audit.first",
      target_type: "team",
      target_id: "team-1",
      request_id: "staging-admin-browser-request",
      source_ip_hash: null,
      user_agent_hash: null,
      metadata: {},
    },
  ],
  next_cursor: "audit-page-2",
};

const auditPageTwo = {
  items: [
    {
      id: "audit-2",
      created_at: "2026-07-10T11:00:00Z",
      actor: "qianyi",
      action: "audit.second",
      target_type: "token",
      target_id: "prefix-2",
      request_id: null,
      source_ip_hash: null,
      user_agent_hash: null,
      metadata: {},
    },
  ],
  next_cursor: null,
};

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

function mockAuditPages({
  deferSecond = false,
  firstPage = auditPageOne,
  failFirstOnce = false,
}: {
  deferSecond?: boolean;
  firstPage?: Record<string, unknown>;
  failFirstOnce?: boolean;
} = {}) {
  const requests: URL[] = [];
  let firstAttempts = 0;
  let resolveDeferredSecond: ((response: Response) => void) | null = null;
  const secondResponse = deferSecond
    ? new Promise<Response>((resolve) => {
        resolveDeferredSecond = resolve;
      })
    : Promise.resolve(jsonResponse(auditPageTwo));

  vi.spyOn(globalThis, "fetch").mockImplementation(
    (input: RequestInfo | URL) => {
      const url = new URL(String(input), "http://localhost");
      if (url.pathname === "/api/v1/auth/me") {
        return Promise.resolve(jsonResponse(adminMe));
      }
      if (url.pathname === "/api/v1/admin/audit-events") {
        requests.push(url);
        if (url.searchParams.get("cursor") === "audit-page-2") {
          return secondResponse;
        }
        firstAttempts += 1;
        if (failFirstOnce && firstAttempts === 1) {
          return Promise.resolve(
            jsonResponse({ detail: "audit unavailable" }, 503),
          );
        }
        return Promise.resolve(jsonResponse(firstPage));
      }
      return Promise.resolve(jsonResponse({ detail: `unhandled ${url}` }, 404));
    },
  );

  return {
    requests,
    resolveSecond(response: Response): void {
      if (resolveDeferredSecond === null) {
        throw new Error("second audit page is not deferred");
      }
      resolveDeferredSecond(response);
    },
  };
}

describe("AdminAuditLog", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("restores the audit scope and page from a shared URL", async () => {
    const mock = mockAuditPages();
    renderWithProviders(<AdminAuditLog />, { route: "/admin/access?tab=audit&auditScope=all&cursor=audit-page-2&cursor_history=%5Bnull%5D" });
    await screen.findByText("audit.second");
    expect(screen.getByLabelText("Audit scope")).toHaveValue("all");
    expect(mock.requests[0]?.searchParams.get("cursor")).toBe("audit-page-2");
    expect(screen.getByRole("status")).toHaveTextContent("Page 2");
  });

  it("filters all records server-side and resets pagination before requesting a new scope", async () => {
    const mock = mockAuditPages();
    const user = userEvent.setup();
    renderWithProviders(<AdminAuditLog />, { route: "/admin/access?tab=audit&actor=qianyi&action=team&start=2026-07-01&end=2026-07-31" });
    await screen.findByText("audit.first");
    expect(screen.getByLabelText("Actor")).toHaveValue("qianyi");
    expect(mock.requests[0]?.searchParams.get("scope")).toBe("access");
    expect(mock.requests[0]?.searchParams.get("start")).toBe("2026-07-01T00:00:00Z");
    await user.click(screen.getByRole("button", { name: /next page/i }));
    await screen.findByText("audit.second");
    await user.selectOptions(screen.getByLabelText("Audit scope"), "all");
    await waitFor(() => expect(mock.requests.at(-1)?.searchParams.get("scope")).toBe("all"));
    expect(mock.requests.at(-1)?.searchParams.has("cursor")).toBe(false);
    expect(screen.getByRole("status")).toHaveTextContent("Page 1");
  });

  it("traverses forward and backward with loading and terminal states", async () => {
    const mock = mockAuditPages({ deferSecond: true });
    const user = userEvent.setup();
    renderWithProviders(<AdminAuditLog />);

    expect(await screen.findByText("audit.first")).toBeInTheDocument();
    expect(screen.getByText("staging-admin-browser-request")).toBeInTheDocument();
    expect(
      screen.getByRole("heading", { name: "Audit log" }).closest(
        '[data-loom-query="audit-events"]',
      ),
    ).toHaveAttribute("data-loom-query-status", "success");
    expect(screen.getByRole("status")).toHaveTextContent(
      "Page 1, more results available",
    );
    expect(screen.getByRole("button", { name: /previous page/i })).toHaveAttribute(
      "aria-disabled",
      "true",
    );
    const nextButton = screen.getByRole("button", { name: /next page/i });
    await user.click(nextButton);

    expect(await screen.findByRole("status")).toHaveTextContent("Loading page 2");
    expect(nextButton).toHaveFocus();
    expect(screen.getByRole("button", { name: /previous page/i })).toHaveAttribute(
      "aria-disabled",
      "true",
    );
    expect(screen.getByRole("button", { name: /next page/i })).toHaveAttribute(
      "aria-disabled",
      "true",
    );
    await act(async () => {
      mock.resolveSecond(jsonResponse(auditPageTwo));
    });

    expect(await screen.findByText("audit.second")).toBeInTheDocument();
    expect(screen.getByRole("status")).toHaveTextContent(
      "Page 2, end of results",
    );
    expect(screen.getByRole("button", { name: /previous page/i })).toHaveAttribute(
      "aria-disabled",
      "false",
    );
    expect(screen.getByRole("button", { name: /next page/i })).toHaveAttribute(
      "aria-disabled",
      "true",
    );
    expect(screen.getByRole("button", { name: /next page/i })).not.toHaveAttribute(
      "disabled",
    );

    await user.click(screen.getByRole("button", { name: /previous page/i }));
    expect(await screen.findByText("audit.first")).toBeInTheDocument();
    expect(mock.requests.some((url) => url.searchParams.get("cursor") === "audit-page-2"))
      .toBe(true);
  });

  it("renders an explicit empty terminal page", async () => {
    mockAuditPages({ firstPage: { items: [], next_cursor: null } });
    renderWithProviders(<AdminAuditLog />);

    expect(await screen.findByText("No admin audit events.")).toBeInTheDocument();
    expect(screen.getByRole("status")).toHaveTextContent(
      "Page 1, end of results",
    );
  });

  it("renders an error and retries the same cursor", async () => {
    mockAuditPages({ failFirstOnce: true });
    const user = userEvent.setup();
    renderWithProviders(<AdminAuditLog />);

    expect(await screen.findByText("audit unavailable")).toBeInTheDocument();
    expect(screen.getByRole("status")).toHaveTextContent(
      "Page 1 could not be loaded",
    );
    await user.click(screen.getByRole("button", { name: /retry page/i }));

    expect(await screen.findByText("audit.first")).toBeInTheDocument();
    expect(screen.getByRole("status")).toHaveTextContent(
      "Page 1, more results available",
    );
  });
  it("traverses three pages and restores the scoped middle page after leaving and browser back", async () => {
    const user = userEvent.setup();
    vi.spyOn(globalThis, "fetch").mockImplementation(async (input) => {
      const url = new URL(String(input), "http://localhost");
      if (url.pathname.endsWith("/auth/me")) return jsonResponse(adminMe);
      const cursor = url.searchParams.get("cursor");
      const index = cursor === "middle" ? 2 : cursor === "last" ? 3 : 1;
      return jsonResponse({ items: [{ ...auditPageOne.items[0], id: `audit-${index}`, action: `access.page-${index}` }], next_cursor: index === 3 ? null : index === 1 ? "middle" : "last" });
    });
    renderWithProviders(<AuditRouteProbe />, { route: "/admin/access?tab=audit&auditScope=access&actor=qianyi" });
    await screen.findByText("access.page-1");
    expect(screen.getByRole("button", { name: "previous page" })).toHaveAttribute("aria-disabled", "true");
    await user.click(screen.getByRole("button", { name: "next page" }));
    await screen.findByText("access.page-2");
    expect(screen.getByRole("status")).toHaveTextContent("Page 2, more results");
    await user.click(screen.getByRole("link", { name: "Leave audit" }));
    await user.click(screen.getByRole("button", { name: "Browser back" }));
    await screen.findByText("access.page-2");
    expect(screen.getByLabelText("Audit scope")).toHaveValue("access");
    expect(screen.getByLabelText("Actor")).toHaveValue("qianyi");
    expect(screen.getByLabelText("Current URL")).toHaveTextContent("tab=audit");
    expect(screen.getByLabelText("Current URL")).toHaveTextContent("cursor=middle");
    await user.click(screen.getByRole("button", { name: "next page" }));
    await screen.findByText("access.page-3");
    expect(screen.getByRole("status")).toHaveTextContent("Page 3, end of results");
    expect(screen.getByRole("button", { name: "next page" })).toHaveAttribute("aria-disabled", "true");
    await user.click(screen.getByRole("button", { name: "previous page" }));
    await screen.findByText("access.page-2");
  });

});
