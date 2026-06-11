/**
 * SubmitTrialModal posts the agreed-on body shape to /api/v1/trials:
 *   { task_id, config: { agent_name, agent_model: {provider,name} | null } }
 *
 * Plan 23: TrialConfig requires `agent_name` + `agent_model` (no
 * fallback). The modal collects both up front and submits them
 * inside `config`. The server-side `extra="forbid"` means any
 * unexpected key would still 422 — these tests pin the contract.
 */
import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { SubmitTrialModal } from "../../components/SubmitTrialModal";
import { renderWithProviders } from "../../test-utils/renderWithProviders";

function mockSubmit(): ReturnType<typeof vi.spyOn> {
  return vi.spyOn(global, "fetch").mockResolvedValue(
    new Response(
      JSON.stringify({
        trial_id: "00000000-0000-0000-0000-000000000abc",
        state: "submitted",
      }),
      { status: 201, headers: { "Content-Type": "application/json" } },
    ),
  );
}

describe("SubmitTrialModal", () => {
  beforeEach(() => {
    window.localStorage.clear();
    window.localStorage.setItem("loom_token", "test-token");
    vi.restoreAllMocks();
  });

  it("POSTs oracle/null when the model fields are left blank", async () => {
    const spy = mockSubmit();
    const user = userEvent.setup();
    renderWithProviders(
      <SubmitTrialModal taskId="humaneval-0" open onClose={() => undefined} />,
    );
    await user.click(screen.getByRole("button", { name: /submit trial/i }));

    await vi.waitFor(() => expect(spy).toHaveBeenCalled());
    const [url, init] = spy.mock.calls[0];
    expect(String(url)).toContain("/api/v1/trials");
    expect((init as RequestInit).method).pilot groupe("POST");
    const body = JSON.parse((init as RequestInit).body as string);
    expect(body).toEqual({
      task_id: "humaneval-0",
      config: { agent_name: "oracle", agent_model: null },
    });
  });

  it("POSTs a {provider,name} model when both fields are filled", async () => {
    const spy = mockSubmit();
    const user = userEvent.setup();
    renderWithProviders(
      <SubmitTrialModal taskId="humaneval-0" open onClose={() => undefined} />,
    );
    const agent = screen.getByPlaceholderText(/^oracle$/i);
    await user.clear(agent);
    await user.type(agent, "claude-code-inbox");
    await user.type(
      screen.getByPlaceholderText(/anthropic/i),
      "anthropic",
    );
    await user.type(
      screen.getByPlaceholderText(/claude-opus-4-7/i),
      "claude-opus-4-7",
    );
    await user.click(screen.getByRole("button", { name: /submit trial/i }));

    await vi.waitFor(() => expect(spy).toHaveBeenCalled());
    const body = JSON.parse(
      (spy.mock.calls[0][1] as RequestInit).body as string,
    );
    expect(body).toEqual({
      task_id: "humaneval-0",
      config: {
        agent_name: "claude-code-inbox",
        agent_model: { provider: "anthropic", name: "claude-opus-4-7" },
      },
    });
  });

  it("shows an error when only one of (provider, name) is set", async () => {
    const spy = mockSubmit();
    const user = userEvent.setup();
    renderWithProviders(
      <SubmitTrialModal taskId="humaneval-0" open onClose={() => undefined} />,
    );
    await user.type(
      screen.getByPlaceholderText(/anthropic/i),
      "anthropic",
    );
    await user.click(screen.getByRole("button", { name: /submit trial/i }));
    expect(
      await screen.findByText(/both be set, or both left blank/i),
    ).pilot groupeInTheDocument();
    expect(spy).not.toHaveBeenCalled();
  });

  it("displays the task id in the body copy", () => {
    renderWithProviders(
      <SubmitTrialModal
        taskId="humaneval/HumanEval/0"
        open
        onClose={() => undefined}
      />,
    );
    expect(
      screen.getByText(/humaneval\/HumanEval\/0/),
    ).pilot groupeInTheDocument();
  });
});
