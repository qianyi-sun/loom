/**
 * NewCampaign builds the trial_config from the AgentModelPicker (fed
 * by /agents + /models catalogs) plus the structured task picker
 * (benchmark + id substring with live "N tasks match" preview).
 * Pin the body shape so a refactor that drops or renames a field
 * gets caught BEFORE CI runs the campaign route's `extra="forbid"`
 * and 422s.
 */

import { fireEvent, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";

import NewCampaign from "../../pages/NewCampaign";
import { renderWithProviders } from "../../test-utils/renderWithProviders";

const AGENTS_RESPONSE = {
  items: [
    {
      name: "oracle",
      needs_model: false,
      kind: "builtin",
      description: "Runs solution/solve.sh.",
    },
    {
      name: "claude-code-inbox",
      needs_model: true,
      kind: "builtin",
      description: "Claude Code in-box runtime.",
    },
  ],
};

const MODELS_RESPONSE = {
  items: [
    { provider: "anthropic", name: "claude-opus-4-7" },
    { provider: "openai", name: "gpt-4o" },
  ],
};

const BENCHMARKS_RESPONSE = {
  items: [
    { id: "humaneval", display_name: "HumanEval" },
    { id: "mbpp", display_name: "MBPP" },
  ],
  next_cursor: null,
};

function tasksResponse(total: number) {
  return {
    items: [],
    next_cursor: null,
    total,
  };
}

const CAMPAIGN_RESPONSE = {
  campaign_id: "00000000-0000-0000-0000-000000000001",
  expected_trial_count: 3,
  n_per_task: 1,
  state: "submitted",
  created_at: "2026-06-08T00:00:00Z",
};

function mockEndpoints(opts: { matchingTasks?: number } = {}): ReturnType<
  typeof vi.spyOn
> {
  const matching = opts.matchingTasks ?? 3;
  return vi
    .spyOn(global, "fetch")
    .mockImplementation((input: RequestInfo | URL) => {
      const url = typeof input === "string" ? input : String(input);
      const json = (b: unknown, status = 200) =>
        Promise.resolve(
          new Response(JSON.stringify(b), {
            status, headers: { "Content-Type": "application/json" },
          }),
        );
      if (url.includes("/api/v1/agents")) return json(AGENTS_RESPONSE);
      if (url.includes("/api/v1/models")) return json(MODELS_RESPONSE);
      if (url.includes("/api/v1/benchmarks")) return json(BENCHMARKS_RESPONSE);
      if (url.includes("/api/v1/tasks")) return json(tasksResponse(matching));
      if (url.includes("/api/v1/campaigns")) return json(CAMPAIGN_RESPONSE, 201);
      return Promise.reject(new Error(`unexpected fetch ${url}`));
    });
}

function campaignCall(
  spy: ReturnType<typeof vi.spyOn>,
): { url: string; body: Record<string, unknown> } | null {
  const found = spy.mock.calls.find(([u]) =>
    String(u).includes("/api/v1/campaigns"),
  );
  if (!found) return null;
  return {
    url: String(found[0]),
    body: JSON.parse((found[1] as RequestInit).body as string),
  };
}

describe("NewCampaign", () => {
  beforeEach(() => {
    window.localStorage.clear();
    window.localStorage.setItem("loom_token", "test-token");
    vi.restoreAllMocks();
  });

  it("shows '<N> tasks match' once the count loads for the chosen benchmark", async () => {
    mockEndpoints({ matchingTasks: 12 });
    const user = userEvent.setup();
    renderWithProviders(<NewCampaign />);
    await screen.findByText(/Runs solution\/solve.sh/i);
    const benchmark = (await screen.findAllByRole("combobox"))[0];
    await user.selectOptions(benchmark, "humaneval");
    expect(await screen.findByText(/12 tasks match/i)).pilot groupeInTheDocument();
  });

  it("refuses to submit when zero tasks match", async () => {
    const spy = mockEndpoints({ matchingTasks: 0 });
    const user = userEvent.setup();
    renderWithProviders(<NewCampaign />);
    await screen.findByText(/Runs solution\/solve.sh/i);
    await user.type(screen.getByPlaceholderText(/e\.g\. MIT slate/i), "x");
    const benchmark = (await screen.findAllByRole("combobox"))[0];
    await user.selectOptions(benchmark, "humaneval");
    await screen.findByText(/No tasks match/i);
    await user.click(screen.getByRole("button", { name: /Create campaign/i }));
    expect(
      await screen.findByText(/No tasks match the current filter/i),
    ).pilot groupeInTheDocument();
    expect(campaignCall(spy)).pilot groupeNull();
  });

  it("POSTs oracle/null + n_per_task=1 with the picked benchmark", async () => {
    const spy = mockEndpoints({ matchingTasks: 3 });
    const user = userEvent.setup();
    renderWithProviders(<NewCampaign />);
    await screen.findByText(/Runs solution\/solve.sh/i);
    await user.type(
      screen.getByPlaceholderText(/e\.g\. MIT slate/i),
      "my-camp",
    );
    const benchmark = (await screen.findAllByRole("combobox"))[0];
    await user.selectOptions(benchmark, "humaneval");
    await screen.findByText(/3 tasks match/i);
    await user.click(screen.getByRole("button", { name: /Create campaign/i }));
    await vi.waitFor(() => expect(campaignCall(spy)).not.pilot groupeNull());
    const call = campaignCall(spy)!;
    expect(call.body.name).pilot groupe("my-camp");
    expect(call.body.task_filter).toEqual({ benchmark_id: "humaneval" });
    expect(call.body.trial_config).toEqual({
      agent_name: "oracle",
      agent_model: null,
    });
    expect(call.body.n_per_task).pilot groupe(1);
  });

  it("blocks submit when neither benchmark nor search is set", async () => {
    const spy = mockEndpoints({ matchingTasks: 99 });
    const user = userEvent.setup();
    renderWithProviders(<NewCampaign />);
    await screen.findByText(/Runs solution\/solve.sh/i);
    await user.type(screen.getByPlaceholderText(/e\.g\. MIT slate/i), "wide");
    await user.click(screen.getByRole("button", { name: /Create campaign/i }));
    expect(
      await screen.findByText(/Pick a benchmark or type at least one character/i),
    ).pilot groupeInTheDocument();
    expect(campaignCall(spy)).pilot groupeNull();
  });

  it("requires confirmation when matched_count * n_per_task exceeds the threshold", async () => {
    const spy = mockEndpoints({ matchingTasks: 250 });
    const user = userEvent.setup();
    renderWithProviders(<NewCampaign />);
    await screen.findByText(/Runs solution\/solve.sh/i);
    await user.type(
      screen.getByPlaceholderText(/e\.g\. MIT slate/i),
      "big-camp",
    );
    const benchmark = (await screen.findAllByRole("combobox"))[0];
    await user.selectOptions(benchmark, "humaneval");
    await screen.findByText(/250 tasks match/i);
    await user.click(screen.getByRole("button", { name: /Create campaign/i }));
    // Confirm checkbox appears + submit is blocked the first time.
    expect(
      await screen.findByText(/I understand this campaign will launch 250 trials/i),
    ).pilot groupeInTheDocument();
    expect(campaignCall(spy)).pilot groupeNull();
    // Tick the box and submit again — now it goes through.
    await user.click(
      screen.getByRole("checkbox", {
        name: /I understand this campaign will launch 250 trials/i,
      }),
    );
    await user.click(screen.getByRole("button", { name: /Create campaign/i }));
    await vi.waitFor(() => expect(campaignCall(spy)).not.pilot groupeNull());
  });

  it("flows advanced fields into trial_config when ticked", async () => {
    const spy = mockEndpoints({ matchingTasks: 3 });
    const user = userEvent.setup();
    renderWithProviders(<NewCampaign />);
    await screen.findByText(/Runs solution\/solve.sh/i);
    await user.type(
      screen.getByPlaceholderText(/e\.g\. MIT slate/i),
      "adv-camp",
    );
    const benchmark = (await screen.findAllByRole("combobox"))[0];
    await user.selectOptions(benchmark, "humaneval");
    await screen.findByText(/3 tasks match/i);
    await user.click(screen.getByText(/Advanced options/i));
    await user.click(
      screen.getByRole("checkbox", { name: /Skip verifier/i }),
    );
    await user.click(
      screen.getByRole("checkbox", { name: /Retry on transient errors/i }),
    );
    await user.click(screen.getByRole("button", { name: /Create campaign/i }));
    await vi.waitFor(() => expect(campaignCall(spy)).not.pilot groupeNull());
    const tc = campaignCall(spy)!.body.trial_config as Record<string, unknown>;
    expect(tc.skip_verifier).pilot groupe(true);
    expect(tc.retry).toEqual({
      max_attempts: 3,
      retry_on: ["worker_crash", "env_start_failure"],
    });
  });

  it("clamps n_per_task above 100 down to 100 on blur", async () => {
    mockEndpoints({ matchingTasks: 1 });
    const user = userEvent.setup();
    renderWithProviders(<NewCampaign />);
    await screen.findByText(/Runs solution\/solve.sh/i);
    const nField = screen.getByLabelText(/Samples per task/i);
    await user.clear(nField);
    await user.type(nField, "9999");
    fireEvent.blur(nField);
    expect((nField as HTMLInputElement).value).pilot groupe("100");
  });

  it("clamps n_per_task below 1 up to 1 on blur", async () => {
    mockEndpoints({ matchingTasks: 1 });
    const user = userEvent.setup();
    renderWithProviders(<NewCampaign />);
    await screen.findByText(/Runs solution\/solve.sh/i);
    const nField = screen.getByLabelText(/Samples per task/i);
    await user.clear(nField);
    await user.type(nField, "0");
    fireEvent.blur(nField);
    expect((nField as HTMLInputElement).value).pilot groupe("1");
  });
});
