import { act, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import AdminAuditLog from "../../../components/admin/AdminAuditLog";
import { renderWithProviders } from "../../../test-utils/renderWithProviders";

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
});
