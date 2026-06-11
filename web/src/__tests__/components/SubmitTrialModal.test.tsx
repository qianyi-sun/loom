/**
 * SubmitTrialModal posts the agreed-on body shape to /api/v1/trials:
 *   { task_id, config: { agent_name, agent_model } }
 *
 * Plan 23: TrialConfig requires `agent_name` + `agent_model`. The
 * modal hardcodes the oracle/null preset (canary hello-world); the
 * upcoming PR F replaces this with an agent + model picker. The
 * server-side `extra="forbid"` means any unexpected key would still
 * 422 — this test guards the contract.
 */
import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { SubmitTrialModal } from "../../components/SubmitTrialModal";
import { renderWithProviders } from "../../test-utils/renderWithProviders";

describe("SubmitTrialModal", () => {
  beforeEach(() => {
    window.localStorage.clear();
    window.localStorage.setItem("loom_token", "test-token");
    vi.restoreAllMocks();
  });

  it("POSTs { task_id, config: { agent_name, agent_model } } when the user confirms", async () => {
    const spy = vi.spyOn(global, "fetch").mockResolvedValue(
      new Response(
        JSON.stringify({
          trial_id: "00000000-0000-0000-0000-000000000abc",
          state: "submitted",
        }),
        {
          status: 201,
          headers: { "Content-Type": "application/json" },
        },
      ),
    );
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

  it("displays the task id in the confirmation copy", () => {
    renderWithProviders(
      <SubmitTrialModal
        taskId="humaneval/HumanEval/0"
        open
        onClose={() => undefined}
      />,
    );
    expect(screen.getByText("humaneval/HumanEval/0")).pilot groupeInTheDocument();
  });

  it("does not POST when canceled", async () => {
    const spy = vi.spyOn(global, "fetch");
    const onClose = vi.fn();
    const user = userEvent.setup();
    renderWithProviders(
      <SubmitTrialModal taskId="x" open onClose={onClose} />,
    );
    await user.click(screen.getByRole("button", { name: /cancel/i }));
    expect(spy).not.toHaveBeenCalled();
    expect(onClose).toHaveBeenCalled();
  });
});
