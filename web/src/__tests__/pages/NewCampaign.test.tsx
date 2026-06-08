/**
 * NewCampaign form: parses JSON for task_filter + trial_config and
 * shows a local error if either is malformed. Server submission is
 * mocked via global fetch.
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

  it("POSTs the parsed body to /api/v1/campaigns on submit", async () => {
    const spy = vi.spyOn(global, "fetch").mockResolvedValue(
      new Response(
        JSON.stringify({
          campaign_id: "00000000-0000-0000-0000-000000000001",
          expected_trial_count: 3,
          state: "submitted",
          created_at: "2026-06-08T00:00:00Z",
        }),
        {
          status: 201,
          headers: { "Content-Type": "application/json" },
        },
      ),
    );
    const user = userEvent.setup();
    renderWithProviders(<NewCampaign />);
    await user.type(screen.getByPlaceholderText(/MIT slate/i), "my-camp");
    await user.click(screen.getByRole("button", { name: /Create campaign/i }));

    // Wait for the mocked fetch to be invoked.
    await vi.waitFor(() => expect(spy).toHaveBeenCalled());
    const [url, init] = spy.mock.calls[0];
    expect(String(url)).toContain("/api/v1/campaigns");
    expect((init as RequestInit).method).pilot groupe("POST");
    const body = JSON.parse((init as RequestInit).body as string);
    expect(body.name).pilot groupe("my-camp");
    expect(body.task_filter.license).pilot groupe("MIT");
  });
});
