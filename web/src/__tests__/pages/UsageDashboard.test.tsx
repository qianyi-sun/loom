import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";

import UsageDashboard from "../../pages/UsageDashboard";
import type { FetchMock } from "../../test-utils/fetchMock";
import { renderWithProviders } from "../../test-utils/renderWithProviders";

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

describe("UsageDashboard", () => {
  beforeEach(() => {
    vi.restoreAllMocks();
  });

  function mockUsageDashboard({
    authGate,
    platformAdmin = false,
    usageBody = {
      degraded: false,
      buckets: [
        {
          start_at: "2026-06-01T00:00:00Z",
          trial_count: 2,
          total_cost_usd: 0.25,
          estimated_cost_usd: 0.25,
          cost_status: "estimated",
          cost_currency: "USD",
          pricing_modes: ["priced"],
          llm_input_tokens: 1000,
          llm_output_tokens: 500,
          trials_currently_succeeded: 1,
          trials_currently_failed: 1,
        },
      ],
    },
  }: {
    authGate?: Promise<void>;
    platformAdmin?: boolean;
    usageBody?: Record<string, unknown>;
  } = {}): FetchMock {
    return vi.spyOn(globalThis, "fetch").mockImplementation(
      async (input: RequestInfo | URL) => {
        const url = String(input);
        if (url.includes("/api/v1/auth/me")) {
          if (authGate) await authGate;
          return jsonResponse({
            user: {
              id: "user-1",
              username: platformAdmin ? "PlatformAdmin" : "Member",
              email: "user@example.com",
              display_name: null,
              is_platform_admin: platformAdmin,
            },
            teams: platformAdmin
              ? []
              : [{ id: "team-1", name: "Alpha", role: "member" }],
            current_team: platformAdmin
              ? null
              : { id: "team-1", name: "Alpha", role: "member" },
            role: platformAdmin ? "platform_admin" : "member",
            scopes: platformAdmin ? ["admin:platform"] : ["read:own"],
            is_platform_admin: platformAdmin,
            csrf_token: "csrf-test",
          });
        }
        if (url.includes("/api/v1/admin/teams")) {
          return jsonResponse({
            items: [
              { id: "team-a", name: "EAI" },
              { id: "team-b", name: "Runtime Research" },
            ],
          });
        }
        if (url.includes("/api/v1/usage")) {
          return jsonResponse(usageBody);
        }
        return jsonResponse({ detail: `unhandled ${url}` }, 404);
      },
    );
  }

  it("shows a CLI equivalent for the selected usage range", async () => {
    const fetchMock = mockUsageDashboard();

    renderWithProviders(<UsageDashboard />, { route: "/usage" });

    expect(await screen.findByRole("button", { name: "Export usage query" })).toBeInTheDocument();
    expect(screen.queryByText(/loom eval usage --start/)).not.toBeInTheDocument();
    await userEvent.click(screen.getByRole("button", { name: "Export usage query" }));
    expect(screen.getByText(/loom eval usage --start/)).toHaveTextContent("--end");
    await userEvent.click(screen.getByRole("button", { name: "close" }));
    expect(screen.getByText("Current team")).toBeInTheDocument();
    expect(screen.getByText("Alpha")).toBeInTheDocument();
    expect(
      screen.queryByPlaceholderText("UUID, blank for own team"),
    ).not.toBeInTheDocument();
    expect(await screen.findByText("Estimated LLM cost")).toBeInTheDocument();
    expect(fetchMock.mock.calls.some(([input]) =>
      String(input).includes("/api/v1/admin/teams"),
    )).toBe(false);
  });

  it("lets platform admins filter usage by internal team name", async () => {
    const fetchMock = mockUsageDashboard({ platformAdmin: true });

    renderWithProviders(<UsageDashboard />, { route: "/usage" });

    expect(await screen.findByRole("button", { name: "Export usage query" })).toBeInTheDocument();
    expect(
      await screen.findByRole("option", { name: "Runtime Research" }),
    ).toBeInTheDocument();

    const user = userEvent.setup();
    await user.selectOptions(screen.getByLabelText("Team"), "team-b");

    await waitFor(() => {
      const usageRequests = fetchMock.mock.calls.filter(([input]) =>
        String(input).includes("/api/v1/usage"),
      );
      expect(usageRequests.length).toBeGreaterThan(1);
      const latestUrl = new URL(
        String(usageRequests.at(-1)![0]),
        "http://localhost",
      );
      expect(latestUrl.searchParams.get("team_id")).toBe("team-b");
    });
    await user.click(screen.getByRole("button", { name: "Export usage query" }));
    expect(screen.getByText(/loom eval usage --start/)).toHaveTextContent(
      "--team-id team-b",
    );
  });

  it.each(["day", "week", "month"])("exports %s with the effective admin team scope", async (group) => {
    const fetchMock = mockUsageDashboard({ platformAdmin: true });
    renderWithProviders(<UsageDashboard />, { route: "/usage" });
    const user = userEvent.setup();
    await screen.findByRole("option", { name: "Runtime Research" });
    await user.selectOptions(screen.getByLabelText("Group by"), group);
    for (const team of ["team-b", ""]) {
      await user.selectOptions(screen.getByLabelText("Team"), team);
      await user.click(screen.getByRole("button", { name: "Export usage query" }));
      const command = screen.getByText(/loom eval usage --start/);
      expect(command).toHaveTextContent(`--group-by ${group}`);
      expect(command).toHaveTextContent("--include-batches");
      if (team) expect(command).toHaveTextContent(`--team-id ${team}`);
      else expect(command).not.toHaveTextContent("--team-id");
      const requests = fetchMock.mock.calls.filter(([input]) => String(input).includes("/api/v1/usage"));
      const url = new URL(String(requests.at(-1)![0]), "http://localhost");
      expect(command).toHaveTextContent(`--start ${url.searchParams.get("start")} --end ${url.searchParams.get("end")}`);
      expect(url.searchParams.get("group_by")).toBe(group);
      expect(url.searchParams.get("team_id")).toBe(team || null);
      await user.click(screen.getByRole("button", { name: "close" }));
    }
  });

  it.each(["day", "week", "month"])("exports member %s without admin scope flags", async (group) => {
    mockUsageDashboard();
    renderWithProviders(<UsageDashboard />, { route: "/usage" });
    const user = userEvent.setup();
    await screen.findByText("Alpha");
    await user.selectOptions(screen.getByLabelText("Group by"), group);
    await user.click(screen.getByRole("button", { name: "Export usage query" }));
    const command = screen.getByText(/loom eval usage --start/);
    expect(command).toHaveTextContent(`--group-by ${group}`);
    expect(command).not.toHaveTextContent("--include-batches");
    expect(command).not.toHaveTextContent("--team-id");
  });

  it("waits for session authority before requesting authorization-dependent usage", async () => {
    let releaseAuth!: () => void;
    const authGate = new Promise<void>((resolve) => {
      releaseAuth = resolve;
    });
    const fetchMock = mockUsageDashboard({ platformAdmin: true, authGate });

    renderWithProviders(<UsageDashboard />, { route: "/usage" });

    await waitFor(() => {
      expect(fetchMock.mock.calls.some(([input]) =>
        String(input).includes("/api/v1/auth/me"),
      )).toBe(true);
    });
    expect(fetchMock.mock.calls.some(([input]) =>
      String(input).includes("/api/v1/usage"),
    )).toBe(false);

    releaseAuth();

    expect(await screen.findByText("Estimated LLM cost")).toBeInTheDocument();
    const usageRequests = fetchMock.mock.calls.filter(([input]) =>
      String(input).includes("/api/v1/usage"),
    );
    expect(usageRequests).toHaveLength(1);
    const usageUrl = new URL(String(usageRequests[0]![0]), "http://localhost");
    expect(usageUrl.searchParams.get("include_batches")).toBe("true");
  });

  it("shows token-only cost as not applicable and includes admin batch drilldown", async () => {
    const fetchMock = mockUsageDashboard({
      platformAdmin: true,
      usageBody: {
        degraded: false,
        buckets: [
          {
            start_at: "2026-06-02T00:00:00Z",
            trial_count: 1,
            total_cost_usd: 0,
            estimated_cost_usd: null,
            cost_status: "not_applicable",
            cost_currency: null,
            pricing_modes: ["tokens-only"],
            partial_usage_llm_calls_count: 1,
            missing_usage_llm_calls_count: 0,
            usage_reporting_status: "partial",
            usage_estimate_confidence: "partial",
            llm_input_tokens: 77,
            llm_output_tokens: 11,
            trials_currently_succeeded: 0,
            trials_currently_failed: 1,
            batches: [
              {
                batch_id: "batch-token-only",
                batch_name: "self-deployed token-only batch",
                team_id: "team-1",
                team_name: "Team One",
                trial_count: 1,
                estimated_cost_usd: null,
                cost_status: "not_applicable",
                cost_currency: null,
                pricing_modes: ["tokens-only"],
                partial_usage_llm_calls_count: 1,
                missing_usage_llm_calls_count: 0,
                usage_reporting_status: "partial",
                usage_estimate_confidence: "partial",
                llm_input_tokens: 77,
                llm_output_tokens: 11,
              },
            ],
          },
        ],
      },
    });

    renderWithProviders(<UsageDashboard />, { route: "/usage" });

    expect(await screen.findByText("self-deployed token-only batch"))
      .toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Tokens per bucket" })).toBeInTheDocument();
    expect(screen.queryByRole("heading", { name: "Estimated LLM cost per bucket" })).not.toBeInTheDocument();
    const breakdown = screen.getByText("self-deployed token-only batch").closest("details");
    expect(breakdown).not.toHaveAttribute("open");
    await userEvent.click(screen.getByText("Batch breakdown (1)"));
    expect(breakdown).toHaveAttribute("open");
    expect(screen.getByText(/Mixed combines priced and token-only calls/)).toHaveTextContent("price_unknown means a matching price is unavailable");
    expect(screen.getAllByText("n/a").length).toBeGreaterThan(0);
    expect(screen.getAllByText("not_applicable").length).toBeGreaterThan(0);
    expect(screen.getAllByText("partial").length).toBeGreaterThan(0);

    await waitFor(() => {
      const usageRequests = fetchMock.mock.calls.filter(([input]) =>
        String(input).includes("/api/v1/usage"),
      );
      const latestUrl = new URL(
        String(usageRequests.at(-1)![0]),
        "http://localhost",
      );
      expect(latestUrl.searchParams.get("include_batches")).toBe("true");
    });
  });
});
