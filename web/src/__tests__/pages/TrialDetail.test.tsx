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
import userEvent from "@testing-library/user-event";
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
      if (url.includes(`/trials/${TRIAL_ID}/artifacts/download`)) {
        return Promise.resolve(new Response("artifact", { status: 200 }));
      }
      return Promise.reject(new Error(`unexpected fetch ${url}`));
    });
}

function stubBlobUrls(url = "blob:download") {
  return {
    createObjectURL: vi.spyOn(URL, "createObjectURL").mockReturnValue(url),
    revokeObjectURL: vi
      .spyOn(URL, "revokeObjectURL")
      .mockImplementation(() => undefined),
  };
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

  it("downloads artifacts through the authenticated service endpoint", async () => {
    const fetchMock = fetchSpy(
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
            download_url: (
              `http://svc/api/v1/trials/${TRIAL_ID}/artifacts/download`
              + "?key=main%2Fresult.txt"
            ),
          },
        ],
      },
    );
    const { createObjectURL, revokeObjectURL } = stubBlobUrls(
      "blob:artifact",
    );
    renderWithProviders(
      <Routes>
        <Route path="/trials/:trialId" element={<TrialDetail />} />
      </Routes>,
      { route: `/trials/${TRIAL_ID}` },
    );

    const user = userEvent.setup();
    await user.click(await screen.findByRole("button", {
      name: /Download artifact main\/result\.txt/i,
    }));

    await waitFor(() =>
      expect(fetchMock).toHaveBeenCalledWith(
        `/api/v1/trials/${TRIAL_ID}/artifacts/download?key=main%2Fresult.txt`,
        expect.objectContaining({
          headers: expect.objectContaining({
            Authorization: "Bearer test-token",
          }),
        }),
      ),
    );
    expect(createObjectURL).toHaveBeenCalled();
    expect(revokeObjectURL).toHaveBeenCalledWith("blob:artifact");
    expect(screen.getByText("701 B")).toBeInTheDocument();
  });

  it("downloads trajectory through the authenticated service endpoint", async () => {
    const fetchMock = vi
      .fn()
      .mockImplementation((input: RequestInfo | URL, init?: RequestInit) => {
        const url = typeof input === "string" ? input : String(input);
        if (url.endsWith(`/trials/${TRIAL_ID}`)) {
          return Promise.resolve(
            new Response(
              JSON.stringify({
                ...TRIAL_BODY,
                state: "succeeded",
                started_at: "2026-06-11T00:01:00Z",
                finished_at: "2026-06-11T00:05:00Z",
                trajectory_ready: true,
                trajectory_url: "http://localhost:9000/trajectories/events.jsonl",
              }),
              {
                status: 200,
                headers: { "Content-Type": "application/json" },
              },
            ),
          );
        }
        if (url.endsWith(`/trials/${TRIAL_ID}/trajectory/download`)) {
          expect(init?.headers).toMatchObject({
            Authorization: "Bearer test-token",
          });
          return Promise.resolve(
            new Response("{}", {
              status: 200,
              headers: { "Content-Type": "application/jsonl" },
            }),
          );
        }
        if (url.includes(`/trials/${TRIAL_ID}/trajectory`)) {
          return Promise.resolve(
            new Response(JSON.stringify({ events: [], next_cursor: null }), {
              status: 200,
              headers: { "Content-Type": "application/json" },
            }),
          );
        }
        return Promise.reject(new Error(`unexpected fetch ${url}`));
      });
    vi.stubGlobal("fetch", fetchMock);
    const { createObjectURL, revokeObjectURL } = stubBlobUrls(
      "blob:trajectory",
    );

    renderWithProviders(
      <Routes>
        <Route path="/trials/:trialId" element={<TrialDetail />} />
      </Routes>,
      { route: `/trials/${TRIAL_ID}` },
    );

    const user = userEvent.setup();
    await user.click(await screen.findByRole("button", {
      name: /Download trajectory/i,
    }));

    await waitFor(() =>
      expect(fetchMock).toHaveBeenCalledWith(
        `/api/v1/trials/${TRIAL_ID}/trajectory/download`,
        expect.objectContaining({
          headers: expect.objectContaining({
            Authorization: "Bearer test-token",
          }),
        }),
      ),
    );
    expect(createObjectURL).toHaveBeenCalled();
    expect(revokeObjectURL).toHaveBeenCalledWith("blob:trajectory");
  });

  it("shows platform outcome and humanized failure reason for failed trials", async () => {
    fetchSpy(
      { ok: true, body: { events: [], next_cursor: null } },
      {
        ...TRIAL_BODY,
        state: "failed",
        finished_at: "2026-06-11T00:05:00Z",
        failure_reason: "trajectory_flush_failed",
      },
    );

    renderWithProviders(
      <Routes>
        <Route path="/trials/:trialId" element={<TrialDetail />} />
      </Routes>,
      { route: `/trials/${TRIAL_ID}` },
    );

    expect(await screen.findByText("Platform outcome")).toBeInTheDocument();
    expect(screen.getByText(/Trial failed/i)).toBeInTheDocument();
    expect(screen.getByText("Trajectory flush failed")).toBeInTheDocument();
    expect(screen.getByText("trajectory_flush_failed")).toBeInTheDocument();
    expect(
      screen.getByText(/could not persist the trial trajectory log/i),
    ).toBeInTheDocument();
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
