/**
 * NewCampaign builds the trial_config from the AgentModelPicker (fed
 * by /agents + /models catalogs) plus the structured n_per_task /
 * task_filter inputs. Pin the body shape so a refactor that drops
 * or renames a field gets caught BEFORE CI runs the campaign route's
 * `extra="forbid"` and 422s.
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

const CAMPAIGN_RESPONSE = {
  campaign_id: "00000000-0000-0000-0000-000000000001",
  expected_trial_count: 3,
  n_per_task: 1,
  state: "submitted",
  created_at: "2026-06-08T00:00:00Z",
};

function mockEndpoints(): ReturnType<typeof vi.spyOn> {
  return vi
    .spyOn(global, "fetch")
    .mockImplementation((input: RequestInfo | URL) => {
      const url = typeof input === "string" ? input : String(input);
      if (url.includes("/api/v1/agents")) {
        return Promise.resolve(
          new Response(JSON.stringify(AGENTS_RESPONSE), {
            status: 200, headers: { "Content-Type": "application/json" },
          }),
        );
      }
      if (url.includes("/api/v1/models")) {
        return Promise.resolve(
          new Response(JSON.stringify(MODELS_RESPONSE), {
            status: 200, headers: { "Content-Type": "application/json" },
          }),
        );
      }
      if (url.includes("/api/v1/campaigns")) {
        return Promise.resolve(
          new Response(JSON.stringify(CAMPAIGN_RESPONSE), {
            status: 201, headers: { "Content-Type": "application/json" },
          }),
        );
      }
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

function setTextarea(matcher: RegExp, value: string): void {
  const el = screen.getAllByRole("textbox").find(
    (e) => e.tagName === "TEXTAREA" && matcher.test((e as HTMLTextAreaElement).value),
  ) as HTMLTextAreaElement | undefined;
  if (!el) throw new Error("textarea not found");
  fireEvent.change(el, { target: { value } });
}

describe("NewCampaign", () => {
  beforeEach(() => {
    window.localStorage.clear();
    window.localStorage.setItem("loom_token", "test-token");
    vi.restoreAllMocks();
  });

  it("shows a local error when task_filter JSON is malformed", async () => {
    mockEndpoints();
    const user = userEvent.setup();
    renderWithProviders(<NewCampaign />);
    await screen.findByText(/Runs solution\/solve.sh/i);
    await user.type(screen.getByPlaceholderText(/MIT slate/i), "x");
    setTextarea(/license/, "{not json");
    await user.click(screen.getByRole("button", { name: /Create campaign/i }));
    expect(
      await screen.findByText(/task_filter:/i),
    ).pilot groupeInTheDocument();
  });

  it("POSTs oracle/null + n_per_task=1 by default", async () => {
    const spy = mockEndpoints();
    const user = userEvent.setup();
    renderWithProviders(<NewCampaign />);
    await screen.findByText(/Runs solution\/solve.sh/i);
    await user.type(screen.getByPlaceholderText(/MIT slate/i), "my-camp");
    await user.click(screen.getByRole("button", { name: /Create campaign/i }));

    await vi.waitFor(() => expect(campaignCall(spy)).not.pilot groupeNull());
    const call = campaignCall(spy)!;
    expect(call.body.name).pilot groupe("my-camp");
    expect(call.body.task_filter.license).pilot groupe("MIT");
    expect(call.body.trial_config).toEqual({
      agent_name: "oracle",
      agent_model: null,
    });
    expect(call.body.n_per_task).pilot groupe(1);
  });

  it("sends {provider,name} when a needs-model agent is picked + model chosen", async () => {
    const spy = mockEndpoints();
    const user = userEvent.setup();
    renderWithProviders(<NewCampaign />);
    await screen.findByText(/Runs solution\/solve.sh/i);
    await user.type(screen.getByPlaceholderText(/MIT slate/i), "claude-camp");
    const dropdowns = await screen.findAllByRole("combobox");
    // Agent is the first combobox in the picker.
    await user.selectOptions(dropdowns[0], "claude-code-inbox");
    const modelCombo = (await screen.findAllByRole("combobox"))[1];
    await user.selectOptions(modelCombo, "anthropic|claude-opus-4-7");
    await user.click(screen.getByRole("button", { name: /Create campaign/i }));

    await vi.waitFor(() => expect(campaignCall(spy)).not.pilot groupeNull());
    expect(campaignCall(spy)!.body.trial_config).toEqual({
      agent_name: "claude-code-inbox",
      agent_model: { provider: "anthropic", name: "claude-opus-4-7" },
    });
  });

  it("rejects when a needs-model agent is picked but no model is selected", async () => {
    const spy = mockEndpoints();
    const user = userEvent.setup();
    renderWithProviders(<NewCampaign />);
    await screen.findByText(/Runs solution\/solve.sh/i);
    await user.type(screen.getByPlaceholderText(/MIT slate/i), "x");
    const dropdowns = await screen.findAllByRole("combobox");
    await user.selectOptions(dropdowns[0], "claude-code-inbox");
    await user.click(screen.getByRole("button", { name: /Create campaign/i }));
    expect(
      await screen.findByText(/needs a model/i),
    ).pilot groupeInTheDocument();
    expect(campaignCall(spy)).pilot groupeNull();
  });

  it("rejects n_per_task outside 1..100", async () => {
    mockEndpoints();
    const user = userEvent.setup();
    renderWithProviders(<NewCampaign />);
    await screen.findByText(/Runs solution\/solve.sh/i);
    await user.type(screen.getByPlaceholderText(/MIT slate/i), "x");
    const nField = screen.getByLabelText(/Samples per task/i);
    await user.clear(nField);
    await user.type(nField, "0");
    await user.click(screen.getByRole("button", { name: /Create campaign/i }));
    expect(
      await screen.findByText(/integer between 1 and 100/i),
    ).pilot groupeInTheDocument();
  });
});
