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
  submitted_by_user: {
    id: "user-ada",
    username: "Ada",
    team_id: "team-dev",
    team_name: "Dev",
  },
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
  diagnosis: {
    schema_version: "1",
    entity: { type: "batch", id: "batch-alpha" },
    summary: (
      "The batch failed because most failed child trials hit provider "
      + "gateway errors before scoring."
    ),
    primary_cause: {
      reason_code: "trial.gateway_error",
      category: "gateway",
      attribution: "provider",
      confidence: "medium",
      affected_trials: 2,
      affected_ratio: 0.5,
    },
    impact: "The aggregate score is not reliable by itself.",
    evidence: ["2/4 affected trial(s) matched trial.gateway_error"],
    next_actions: [
      {
        label: "Run provider preflight",
        kind: "cli_command",
        command: "loom providers models --preflight gpt-5-mini",
      },
    ],
    reason_clusters: [
      {
        reason_code: "trial.gateway_error",
        category: "gateway",
        attribution: "provider",
        count: 2,
        affected_ratio: 0.5,
        representative_trial_id: "trial-alpha",
      },
    ],
  },
  debug_evidence: {
    schema_version: "1",
    entity: { type: "batch", id: "batch-alpha", team_id: "team-alpha" },
    lifecycle: { state: "finished", terminal_status: "partial_failed" },
    failure: {
      reason_code: "batch.partial_failed",
      reason: "partial_failed",
      category: "aggregate",
      attribution: "mixed",
      message: "Some child trials failed.",
    },
    provider: { llm_calls_count: 2, models: ["openai/gpt-5-mini"] },
    next_actions: ["Open failed child trials and inspect their debug evidence."],
  },
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
        id: "artifact-metric",
        trial_id: "trial-alpha",
        key: "team-alpha/trial-alpha/main/report.json",
        size: 17,
        role: "reports",
        artifact_type: "metric_table",
        artifact_type_label: "Metric table",
        artifact_schema_version: "1.0",
        owner_team: { id: "team-alpha", name: "Alpha Research" },
        source: {
          kind: "trial",
          batch_id: "batch-alpha",
          trial_id: "trial-alpha",
        },
        share_status: "shared",
        safety_state: "safe",
        redaction_state: "redacted",
        content_hash: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        storage: {
          backend: "object_store",
          bucket: "artifacts",
          key: "team-alpha/trial-alpha/main/report.json",
          media_type: "application/json",
          size_bytes: 17,
        },
        provenance: {
          relation: "produced_from",
          source_trial_ids: ["trial-alpha"],
        },
        metadata: { metric_name: "aggregate_reward" },
        parents: [],
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
        id: "artifact-debug",
        trial_id: "trial-alpha",
        key: "team-alpha/trial-alpha/main/debug.log",
        size: 21,
        role: "raw_diagnostics",
        artifact_type: "debug_bundle",
        artifact_type_label: "Debug bundle",
        artifact_schema_version: "1.0",
        owner_team: { id: "team-alpha", name: "Alpha Research" },
        source: {
          kind: "trial",
          batch_id: "batch-alpha",
          trial_id: "trial-alpha",
        },
        share_status: "shared",
        safety_state: "unsafe",
        redaction_state: "blocked",
        content_hash: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        storage: {
          backend: "object_store",
          bucket: "artifacts",
          key: "team-alpha/trial-alpha/main/debug.log",
          media_type: "text/plain",
          size_bytes: 21,
        },
        provenance: {
          relation: "produced_from",
          source_trial_ids: ["trial-alpha"],
        },
        metadata: { debug_kind: "raw_log" },
        parents: [],
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
              username: platformAdmin ? "Qianyi" : "Beta",
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
      const parsed = new URL(url, "http://localhost");
      if (
        parsed.pathname === "/api/v1/run-library/batches" &&
        parsed.searchParams.has("q")
      ) {
        return Promise.resolve(
          jsonResponse({
            items: [sharedBatch],
            next_cursor: null,
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
      if (parsed.pathname === "/api/v1/run-library/batches/batch-alpha") {
        return Promise.resolve(
          jsonResponse(
            parsed.searchParams.get("include_debug") === "true"
              ? detailBatch
              : {
                  ...detailBatch,
                  debug_evidence: undefined,
                  diagnosis: undefined,
                },
          ),
        );
      }
      if (parsed.pathname === "/api/v1/run-library/artifacts/export") {
        return Promise.resolve(
          new Response('{"id":"artifact-metric"}\n', {
            status: 200,
            headers: { "Content-Type": "application/x-ndjson" },
          }),
        );
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

  it("lists all-team shared runs with submitting user owner and artifact status", async () => {
    mockRunLibrary();
    renderWithProviders(<RunLibrary />, { route: "/library?scope=all" });

    expect(await screen.findByText("shared alpha run")).toBeInTheDocument();
    expect(screen.getByText("Reuse guide")).toBeInTheDocument();
    expect(screen.getByText(/Provider credentials are not copied/i)).toBeInTheDocument();
    expect(screen.getAllByText("Ada / Dev").length).toBeGreaterThan(0);
    expect(screen.getAllByText("org / shared").length).toBeGreaterThan(0);
    expect(screen.getAllByText("Reports 1").length).toBeGreaterThan(0);
    expect(screen.getAllByText("Raw/internal 1").length).toBeGreaterThan(0);
    expect(screen.getByRole("option", { name: "Metric table" })).toBeInTheDocument();
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

  it("hydrates structured library filters from the URL and sends them to the API", async () => {
    const fetchMock = mockRunLibrary({ platformAdmin: true });
    renderWithProviders(<RunLibrary />, {
      route:
        "/library?scope=all&q=alpha&benchmark_id=humaneval&agent_name=litellm&model_provider=openai&model_name=gpt-4o-mini&provider_connection_id=source-provider&provider_model_id=gpt-4o-mini",
    });

    expect(await screen.findByText("shared alpha run")).toBeInTheDocument();
    expect(screen.getByLabelText("Search")).toHaveValue("alpha");
    expect(screen.getByLabelText("Benchmark")).toHaveValue("humaneval");
    expect(screen.getByLabelText("Agent")).toHaveValue("litellm");
    expect(screen.getByLabelText("Model provider")).toHaveValue("openai");
    expect(screen.getByLabelText("Model name")).toHaveValue("gpt-4o-mini");
    expect(screen.getByLabelText("Provider connection")).toHaveValue("source-provider");
    expect(screen.getByLabelText("Provider model")).toHaveValue("gpt-4o-mini");

    await waitFor(() => {
      const runLibraryRequest = fetchMock.mock.calls.find(([input]) =>
        String(input).includes("/api/v1/run-library/batches"),
      );
      expect(runLibraryRequest).toBeTruthy();
      const url = new URL(String(runLibraryRequest![0]), "http://localhost");
      expect(url.searchParams.get("scope")).toBe("all");
      expect(url.searchParams.get("q")).toBe("alpha");
      expect(url.searchParams.get("benchmark_id")).toBe("humaneval");
      expect(url.searchParams.get("agent_name")).toBe("litellm");
      expect(url.searchParams.get("model_provider")).toBe("openai");
      expect(url.searchParams.get("model_name")).toBe("gpt-4o-mini");
      expect(url.searchParams.get("provider_connection_id")).toBe("source-provider");
      expect(url.searchParams.get("provider_model_id")).toBe("gpt-4o-mini");
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
    const user = userEvent.setup();

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
    expect(screen.getByText("Owner")).toBeInTheDocument();
    expect(screen.getByText("Ada / Dev")).toBeInTheDocument();
    expect(screen.getByText("Visibility")).toBeInTheDocument();
    expect(screen.getByText("org / shared")).toBeInTheDocument();
    expect(screen.getByText("Load diagnostics")).toBeInTheDocument();
    expect(screen.queryByText("Diagnosis")).not.toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "Load diagnostics" }));

    expect(await screen.findByText("Diagnosis")).toBeInTheDocument();
    expect(
      screen.getByText(
        "The batch failed because most failed child trials hit provider gateway errors before scoring.",
      ),
    ).toBeInTheDocument();
    expect(screen.getByText("Run provider preflight")).toBeInTheDocument();
    expect(screen.getByText("Debug evidence")).toBeInTheDocument();
    expect(screen.getByText("batch.partial_failed")).toBeInTheDocument();
    expect(
      screen.getByText(
        "Open failed child trials and inspect their debug evidence.",
      ),
    ).toBeInTheDocument();
    expect(screen.getByText("Provenance")).toBeInTheDocument();
    expect(screen.getByText("Reports")).toBeInTheDocument();
    expect(screen.getByText("Metric table")).toBeInTheDocument();
    expect(screen.getByText("safe / redacted")).toBeInTheDocument();
    expect(screen.getAllByText("Source trial trial-alpha").length).toBeGreaterThan(0);
    expect(screen.getByText(/sha256:aaaaaaaaaaaa/)).toBeInTheDocument();
    expect(screen.getByText("Raw/internal diagnostics")).toBeInTheDocument();
    expect(screen.getByText("Debug bundle")).toBeInTheDocument();
    expect(screen.getByText("unsafe / blocked")).toBeInTheDocument();
    expect(screen.getByText("secret-like content detected")).toBeInTheDocument();
    expect(
      screen.queryByRole("button", {
        name: /Download team-alpha\/trial-alpha\/main\/debug\.log/i,
      }),
    ).not.toBeInTheDocument();
    expect(
      screen.queryByRole("button", {
        name: /Reuse team-alpha\/trial-alpha\/main\/debug\.log/i,
      }),
    ).not.toBeInTheDocument();

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
    await user.click(
      screen.getByRole("button", { name: "Export artifact metadata" }),
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
      expect(fetchMock).toHaveBeenCalledWith(
        expect.stringContaining("/api/v1/run-library/artifacts/export"),
        expect.anything(),
      );
      expect(createObjectURL).toHaveBeenCalled();
    });
    const exportCall = fetchMock.mock.calls.find(([input]) =>
      String(input).includes("/api/v1/run-library/artifacts/export"),
    );
    expect(exportCall).toBeDefined();
    const exportUrl = new URL(String(exportCall?.[0]), "http://localhost");
    expect(exportUrl.searchParams.get("scope")).toBe("all");
    expect(exportUrl.searchParams.get("source_batch_id")).toBe("batch-alpha");
    expect(exportUrl.searchParams.get("format")).toBe("jsonl");
  });
});
