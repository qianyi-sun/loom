/**
 * NewCampaign form: parses task_filter + builds a TrialConfig from
 * the structured Agent / Model / n_per_task fields. Pin the body
 * shape so a refactor that drops or renames a field gets caught
 * BEFORE CI runs the campaign route's `extra="forbid"` and 422s.
 */

import { fireEvent, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";

import NewCampaign from "../../pages/NewCampaign";
import { renderWithProviders } from "../../test-utils/renderWithProviders";

function setTextarea(matcher: RegExp, value: string): void {
  const el = screen.getAllByRole("textbox").find(
    (e) => e.tagName === "TEXTAREA" && matcher.test((e as HTMLTextAreaElement).value),
  ) as HTMLTextAreaElement | undefined;
  if (!el) throw new Error("textarea not found");
  fireEvent.change(el, { target: { value } });
}

function mockCreate(): ReturnType<typeof vi.spyOn> {
  return vi.spyOn(global, "fetch").mockResolvedValue(
    new Response(
      JSON.stringify({
        campaign_id: "00000000-0000-0000-0000-000000000001",
        expected_trial_count: 3,
        n_per_task: 1,
        state: "submitted",
        created_at: "2026-06-08T00:00:00Z",
      }),
      { status: 201, headers: { "Content-Type": "application/json" } },
    ),
  );
}

describe("NewCampaign", () => {
  beforeEach(() => {
    window.localStorage.clear();
    window.localStorage.setItem("loom_token", "test-token");
    vi.restoreAllMocks();
  });

  it("shows a local error when task_filter JSON is malformed", async () => {
    const user = userEvent.setup();
    renderWithProviders(<NewCampaign />);
    await user.type(screen.getByPlaceholderText(/MIT slate/i), "x");
    setTextarea(/license/, "{not json");
    await user.click(screen.getByRole("button", { name: /Create campaign/i }));
    expect(
      await screen.findByText(/task_filter:/i),
    ).pilot groupeInTheDocument();
  });

  it("rejects an array for task_filter (expects object)", async () => {
    const user = userEvent.setup();
    renderWithProviders(<NewCampaign />);
    await user.type(screen.getByPlaceholderText(/MIT slate/i), "x");
    setTextarea(/license/, "[1,2,3]");
    await user.click(screen.getByRole("button", { name: /Create campaign/i }));
    expect(
      await screen.findByText(/expected a JSON object/i),
    ).pilot groupeInTheDocument();
  });

  it("POSTs trial_config with agent_name + agent_model:null + n_per_task=1 by default", async () => {
    const spy = mockCreate();
    const user = userEvent.setup();
    renderWithProviders(<NewCampaign />);
    await user.type(screen.getByPlaceholderText(/MIT slate/i), "my-camp");
    await user.click(screen.getByRole("button", { name: /Create campaign/i }));

    await vi.waitFor(() => expect(spy).toHaveBeenCalled());
    const [url, init] = spy.mock.calls[0];
    expect(String(url)).toContain("/api/v1/campaigns");
    expect((init as RequestInit).method).pilot groupe("POST");
    const body = JSON.parse((init as RequestInit).body as string);
    expect(body.name).pilot groupe("my-camp");
    expect(body.task_filter.license).pilot groupe("MIT");
    expect(body.trial_config).toEqual({
      agent_name: "oracle",
      agent_model: null,
    });
    expect(body.n_per_task).pilot groupe(1);
  });

  it("sends agent_model as {provider,name} when both model fields are set", async () => {
    const spy = mockCreate();
    const user = userEvent.setup();
    renderWithProviders(<NewCampaign />);
    await user.type(screen.getByPlaceholderText(/MIT slate/i), "claude-camp");
    await user.clear(screen.getByPlaceholderText(/^oracle$/i));
    await user.type(
      screen.getByPlaceholderText(/^oracle$/i),
      "claude-code-inbox",
    );
    await user.type(
      screen.getByPlaceholderText(/anthropic/i),
      "anthropic",
    );
    await user.type(
      screen.getByPlaceholderText(/claude-opus-4-7/i),
      "claude-opus-4-7",
    );
    await user.click(screen.getByRole("button", { name: /Create campaign/i }));

    await vi.waitFor(() => expect(spy).toHaveBeenCalled());
    const body = JSON.parse(
      (spy.mock.calls[0][1] as RequestInit).body as string,
    );
    expect(body.trial_config).toEqual({
      agent_name: "claude-code-inbox",
      agent_model: { provider: "anthropic", name: "claude-opus-4-7" },
    });
  });

  it("rejects when only one of (provider, name) is set", async () => {
    const user = userEvent.setup();
    renderWithProviders(<NewCampaign />);
    await user.type(screen.getByPlaceholderText(/MIT slate/i), "x");
    await user.type(screen.getByPlaceholderText(/anthropic/i), "anthropic");
    await user.click(screen.getByRole("button", { name: /Create campaign/i }));
    expect(
      await screen.findByText(/both be set, or both left blank/i),
    ).pilot groupeInTheDocument();
  });

  it("rejects n_per_task outside 1..100", async () => {
    const user = userEvent.setup();
    renderWithProviders(<NewCampaign />);
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
