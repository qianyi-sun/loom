/**
 * TrialDetail trajectory-section button-state regression. Bug:
 * before the backend started returning {events: [], next_cursor: null}
 * for not-yet-written trajectories (returned 404 instead), the
 * "Load more" button stayed active on a trial with no events. Now
 * that the backend returns an empty page, `next_cursor: null` sets
 * `done = true` and the button MUST disappear.
 *
 * Also pins the new error-state UX: when the trajectory page errors,
 * we surface a "Retry" button rather than the regular "Load more"
 * (which mis-implies more pages exist).
 */
import { screen, waitFor } from "@testing-library/react";
import { Route, Routes } from "react-router-dom";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type { components } from "../../api/schema";
import TrialDetail from "../../pages/TrialDetail";
import { renderWithProviders } from "../../test-utils/renderWithProviders";

const TRIAL_ID = "11111111-1111-1111-1111-111111111111";

const TRIAL_BODY: components["schemas"]["TrialDetail"] = {
  id: TRIAL_ID,
  task_id: "hello-world",
  team_id: "team-1",
  state: "queued",
  agent_name: "oracle",
  model: {
    provider: "openai-compatible",
    name: "gpt-5-mini",
  },
  aggregate_reward: null,
  cost_usd: 0,
  submitted_at: "2026-06-11T00:00:00Z",
  started_at: null,
  finished_at: null,
  attempt_count: 0,
  failure_reason: null,
  atif_ready: false,
  atif_url: "",
  trajectory_ready: false,
  trajectory_url: "",
  artifacts: [],
};

function fetchSpy(
  trajectory:
    | { ok: true; body: unknown }
    | { ok: false; status: number; body: unknown },
  trialBody: typeof TRIAL_BODY = TRIAL_BODY,
): ReturnType<typeof vi.spyOn> {
  return vi
    .spyOn(globalThis, "fetch")
    .mockImplementation((input: RequestInfo | URL) => {
      const url = typeof input === "string" ? input : String(input);
      if (url.endsWith(`/trials/${TRIAL_ID}`)) {
        return Promise.resolve(
          new Response(JSON.stringify(trialBody), {
            status: 200,
            headers: { "Content-Type": "application/json" },
          }),
        );
      }
      if (url.includes(`/trials/${TRIAL_ID}/trajectory`)) {
        if (trajectory.ok) {
          return Promise.resolve(
            new Response(JSON.stringify(trajectory.body), {
              status: 200,
              headers: { "Content-Type": "application/json" },
            }),
          );
        }
        return Promise.resolve(
          new Response(JSON.stringify(trajectory.body), {
            status: trajectory.status,
            headers: { "Content-Type": "application/json" },
          }),
        );
      }
      return Promise.reject(new Error(`unexpected fetch ${url}`));
    });
}

describe("TrialDetail trajectory section", () => {
  beforeEach(() => {
    window.localStorage.clear();
    window.localStorage.setItem("loom_token", "test-token");
    vi.restoreAllMocks();
  });

  it("hides Load more when the trajectory is empty", async () => {
    fetchSpy({ ok: true, body: { events: [], next_cursor: null } });
    renderWithProviders(
      <Routes>
        <Route path="/trials/:trialId" element={<TrialDetail />} />
      </Routes>,
      { route: `/trials/${TRIAL_ID}` },
    );
    await waitFor(() =>
      expect(screen.getByText(/No events yet/i)).toBeInTheDocument(),
    );
    expect(
      screen.getByText("openai-compatible/gpt-5-mini"),
    ).toBeInTheDocument();
    expect(
      screen.queryByRole("button", { name: /Load more/i }),
    ).not.toBeInTheDocument();
    expect(
      screen.queryByRole("button", { name: /Retry/i }),
    ).not.toBeInTheDocument();
  });

  it("renders artifact download links from trial detail metadata", async () => {
    fetchSpy(
      { ok: true, body: { events: [], next_cursor: null } },
      {
        ...TRIAL_BODY,
        state: "succeeded",
        finished_at: "2026-06-11T00:05:00Z",
        artifacts: [
          {
            step_name: "main",
            key: "main/result.txt",
            size: 701,
            download_url: "http://localhost:9000/artifacts/result.txt",
          },
        ],
      },
    );
    renderWithProviders(
      <Routes>
        <Route path="/trials/:trialId" element={<TrialDetail />} />
      </Routes>,
      { route: `/trials/${TRIAL_ID}` },
    );

    const artifactLink = await screen.findByRole("link", {
      name: /Download artifact main\/result\.txt/i,
    });
    expect(artifactLink).toHaveAttribute(
      "href",
      "http://localhost:9000/artifacts/result.txt",
    );
    expect(screen.getByText("701 B")).toBeInTheDocument();
  });

  it("shows Retry instead of Load more on trajectory error", async () => {
    fetchSpy({ ok: false, status: 500, body: { detail: "boom" } });
    renderWithProviders(
      <Routes>
        <Route path="/trials/:trialId" element={<TrialDetail />} />
      </Routes>,
      { route: `/trials/${TRIAL_ID}` },
    );
    await waitFor(() =>
      expect(
        screen.getByRole("button", { name: /Retry/i }),
      ).toBeInTheDocument(),
    );
    expect(
      screen.queryByRole("button", { name: /Load more/i }),
    ).not.toBeInTheDocument();
  });
});
