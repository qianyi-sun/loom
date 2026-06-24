import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { Route, Routes } from "react-router-dom";
import { beforeEach, describe, expect, it, vi } from "vitest";

import RunLibrary from "../../pages/RunLibrary";
import RunLibraryBatchDetail from "../../pages/RunLibraryBatchDetail";
import { renderWithProviders } from "../../test-utils/renderWithProviders";

const sharedBatch = {
  id: "batch-alpha",
  team_id: "team-alpha",
  owner_team: { id: "team-alpha", name: "Alpha Research" },
  name: "shared alpha run",
  description: null,
  task_filter: { subset_kind: "explicit", task_ids: ["humaneval/0"] },
  trial_config: {
    agent_name: "litellm",
    agent_model: { provider: "openai", name: "gpt-4o-mini" },
  },
  backend: "docker",
  combinations: [],
  provider_connection_id: null,
  provider_model_id: "gpt-4o-mini",
  state: "finished",
  result_status: "succeeded",
  visibility: "org",
  share_status: "shared",
  source_provenance: [],
  expected_trial_count: 1,
  created_by_token_prefix: "abc12345",
  created_at: "2026-06-22T20:00:00Z",
  finished_at: "2026-06-22T20:03:00Z",
  trial_summary: { succeeded: 1 },
  aggregate_reward: 1,
  artifact_summary: {
    reports: 1,
    trajectories: 0,
    reusable_outputs: 0,
    logs_diagnostics: 0,
    raw_diagnostics: 1,
  },
};

const detailBatch = {
  ...sharedBatch,
  provider_connection_id: "source-provider",
  source_provenance: [
    {
      kind: "cloned_batch_config",
      source_batch_id: "batch-source",
      source_artifact_key: "alpha/report.json",
    },
  ],
  artifact_inventory: {
    reports: [
      {
        trial_id: "trial-alpha",
        key: "team-alpha/trial-alpha/main/report.json",
        size: 17,
        role: "reports",
        share_status: "shared",
        blocked_reason: null,
        download_url:
          "http://svc/api/v1/run-library/trials/trial-alpha/artifacts/download?key=team-alpha%2Ftrial-alpha%2Fmain%2Freport.json",
      },
    ],
    trajectories: [],
    reusable_outputs: [],
    logs_diagnostics: [],
    raw_diagnostics: [
      {
        trial_id: "trial-alpha",
        key: "team-alpha/trial-alpha/main/debug.log",
        size: 21,
        role: "raw_diagnostics",
        share_status: "blocked",
        blocked_reason: "secret-like content detected",
        download_url:
          "http://svc/api/v1/run-library/trials/trial-alpha/artifacts/download?key=team-alpha%2Ftrial-alpha%2Fmain%2Fdebug.log",
      },
    ],
  },
};

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

function mockRunLibrary({
  platformAdmin = false,
}: { platformAdmin?: boolean } = {}): ReturnType<typeof vi.spyOn> {
  return vi
    .spyOn(globalThis, "fetch")
    .mockImplementation((input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if (url.includes("/api/v1/auth/me")) {
        return Promise.resolve(
          jsonResponse({
            user: {
              id: "user-beta",
              email: "beta@example.com",
              display_name: platformAdmin ? "Platform Admin" : "Beta User",
              is_platform_admin: platformAdmin,
            },
            teams: [
              { id: "team-beta", name: "Beta Apps", role: "owner" },
            ],
            current_team: { id: "team-beta", name: "Beta Apps", role: "owner" },
            role: platformAdmin ? "platform_admin" : "owner",
            scopes: platformAdmin ? ["admin:platform"] : ["read:own", "submit"],
            is_platform_admin: platformAdmin,
            csrf_token: "csrf-test",
          }),
        );
      }
      if (url.endsWith("/api/v1/admin/teams")) {
        return Promise.resolve(
          jsonResponse({
            items: [
              { id: "team-alpha", name: "Alpha Research" },
              { id: "team-beta", name: "Beta Apps" },
            ],
          }),
        );
      }
      if (
        url.endsWith(
          "/api/v1/run-library/batches?scope=all&team_id=team-alpha",
        )
      ) {
        return Promise.resolve(
          jsonResponse({
            items: [sharedBatch],
            next_cursor: null,
          }),
        );
      }
      if (url.endsWith("/api/v1/run-library/batches?scope=all")) {
        return Promise.resolve(
          jsonResponse({
            items: [
              sharedBatch,
              {
                ...sharedBatch,
                id: "batch-beta",
                team_id: "team-beta",
                owner_team: { id: "team-beta", name: "Beta Apps" },
                name: "beta team run",
                visibility: "team",
                share_status: "pending_scan",
              },
            ],
            next_cursor: null,
          }),
        );
      }
      if (url.endsWith("/api/v1/run-library/batches")) {
        return Promise.resolve(
          jsonResponse({
            items: [{ ...sharedBatch, id: "batch-beta", name: "beta team run" }],
            next_cursor: null,
          }),
        );
      }
      if (url.endsWith("/api/v1/run-library/batches/batch-alpha")) {
        return Promise.resolve(jsonResponse(detailBatch));
      }
      if (url.endsWith("/api/v1/provider-connections")) {
        return Promise.resolve(
          jsonResponse({
            items: [
              {
                id: "beta-provider",
                name: "Beta provider",
                type: "openai-compatible",
                status: "valid",
              },
            ],
          }),
        );
      }
      if (
        url.endsWith("/api/v1/run-library/batches/batch-alpha/clone-config") &&
        init?.method === "POST"
      ) {
        return Promise.resolve(
          jsonResponse(
            {
              batch_id: "batch-clone",
              cloned_from_batch_id: "batch-alpha",
              provider_connection_id: null,
              source_provenance: [
                { kind: "cloned_batch_config", source_batch_id: "batch-alpha" },
              ],
              state: "submitted",
              created_at: "2026-06-22T20:04:00Z",
            },
            201,
          ),
        );
      }
      if (
        url.endsWith("/api/v1/run-library/trials/trial-alpha/artifacts/reuse") &&
        init?.method === "POST"
      ) {
        return Promise.resolve(
          jsonResponse(
            {
              batch_id: "batch-reuse",
              source_artifact: {
                trial_id: "trial-alpha",
                key: "team-alpha/trial-alpha/main/report.json",
                role: "reports",
              },
              source_provenance: [
                {
                  kind: "reused_artifact",
                  source_trial_id: "trial-alpha",
                  source_artifact_key: "team-alpha/trial-alpha/main/report.json",
                },
              ],
              state: "submitted",
              created_at: "2026-06-22T20:05:00Z",
            },
            201,
          ),
        );
      }
      if (
        url.endsWith(
          "/api/v1/run-library/trials/trial-alpha/artifacts/download?key=team-alpha%2Ftrial-alpha%2Fmain%2Freport.json",
        )
      ) {
        return Promise.resolve(new Response("report", { status: 200 }));
      }
      return Promise.reject(new Error(`unexpected fetch ${url}`));
    });
}

describe("RunLibrary", () => {
  beforeEach(() => {
    vi.restoreAllMocks();
  });

  it("lists all-team shared runs with owner team and artifact status", async () => {
    mockRunLibrary();
    renderWithProviders(<RunLibrary />, { route: "/library?scope=all" });

    expect(await screen.findByText("shared alpha run")).toBeInTheDocument();
    expect(screen.getByText("Reuse guide")).toBeInTheDocument();
    expect(screen.getByText(/Provider credentials are not copied/i)).toBeInTheDocument();
    expect(screen.getByText("Alpha Research")).toBeInTheDocument();
    expect(screen.getAllByText("org / shared").length).toBeGreaterThan(0);
    expect(screen.getAllByText("Reports 1").length).toBeGreaterThan(0);
    expect(screen.getAllByText("Raw/internal 1").length).toBeGreaterThan(0);
    expect(
      screen.getByRole("button", { name: "All teams" }),
    ).toHaveAttribute("aria-pressed", "true");
  });

  it("defaults to my-team scope", async () => {
    const fetchMock = mockRunLibrary();
    renderWithProviders(<RunLibrary />, { route: "/library" });

    expect(await screen.findByText("beta team run")).toBeInTheDocument();
    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledWith(
        "/api/v1/run-library/batches",
        expect.objectContaining({ credentials: "include" }),
      );
    });
    expect(
      screen.getByRole("button", { name: "My team" }),
    ).toHaveAttribute("aria-pressed", "true");
  });

  it("lets platform admins filter the run library by any internal team name", async () => {
    const fetchMock = mockRunLibrary({ platformAdmin: true });
    renderWithProviders(<RunLibrary />, {
      route: "/library?scope=all&team_id=team-alpha",
    });

    expect(await screen.findByText("shared alpha run")).toBeInTheDocument();
    expect(screen.getByLabelText("Team")).toHaveValue("team-alpha");
    expect(
      await screen.findByRole("option", { name: "Alpha Research" }),
    ).toBeInTheDocument();

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledWith(
        "/api/v1/admin/teams",
        expect.objectContaining({ credentials: "include" }),
      );
      const runLibraryRequest = fetchMock.mock.calls.find(([input]) =>
        String(input).includes("/api/v1/run-library/batches"),
      );
      expect(runLibraryRequest).toBeTruthy();
      const url = new URL(String(runLibraryRequest![0]), "http://localhost");
      expect(url.searchParams.get("scope")).toBe("all");
      expect(url.searchParams.get("team_id")).toBe("team-alpha");
    });
  });
});

describe("RunLibraryBatchDetail", () => {
  beforeEach(() => {
    vi.restoreAllMocks();
  });

  it("groups artifacts and exposes clone, download, and reuse actions", async () => {
    const fetchMock = mockRunLibrary();
    const createObjectURL = vi.spyOn(URL, "createObjectURL").mockReturnValue("blob:report");
    vi.spyOn(URL, "revokeObjectURL").mockImplementation(() => undefined);

    renderWithProviders(
      <Routes>
        <Route path="/library/batches/:batchId" element={<RunLibraryBatchDetail />} />
      </Routes>,
      { route: "/library/batches/batch-alpha" },
    );

    expect(await screen.findByText("shared alpha run")).toBeInTheDocument();
    expect(screen.getByText("Library CLI downloads")).toBeInTheDocument();
    expect(
      screen.getByText(
        "loom eval trial download trial-alpha --kind artifact --artifact-key team-alpha/trial-alpha/main/report.json --output artifact.bin",
      ),
    ).toBeInTheDocument();
    expect(screen.getByText("Owner team")).toBeInTheDocument();
    expect(screen.getByText("Alpha Research")).toBeInTheDocument();
    expect(screen.getByText("Visibility")).toBeInTheDocument();
    expect(screen.getByText("org / shared")).toBeInTheDocument();
    expect(screen.getByText("Provenance")).toBeInTheDocument();
    expect(screen.getByText("Reports")).toBeInTheDocument();
    expect(screen.getByText("Raw/internal diagnostics")).toBeInTheDocument();
    expect(screen.getByText("secret-like content detected")).toBeInTheDocument();

    const user = userEvent.setup();
    await user.selectOptions(
      screen.getByLabelText("Provider connection"),
      "beta-provider",
    );
    await user.click(screen.getByRole("button", { name: "Clone config" }));
    await user.click(
      screen.getByRole("button", {
        name: /Download team-alpha\/trial-alpha\/main\/report\.json/i,
      }),
    );
    await user.click(
      screen.getByRole("button", {
        name: /Reuse team-alpha\/trial-alpha\/main\/report\.json/i,
      }),
    );

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledWith(
        "/api/v1/run-library/batches/batch-alpha/clone-config",
        expect.objectContaining({
          method: "POST",
          body: expect.stringContaining("beta-provider"),
        }),
      );
      expect(fetchMock).toHaveBeenCalledWith(
        "/api/v1/run-library/trials/trial-alpha/artifacts/reuse",
        expect.objectContaining({ method: "POST" }),
      );
      expect(createObjectURL).toHaveBeenCalled();
    });
  });
});
