/**
 * TrialCompare URL routing + picker behaviour. We don't try to
 * exercise the trial-column data path here — the existing
 * TrialDetail tests would cover that, and mocking two separate
 * endpoint shapes inside a single fetch spy is fragile. Focus on:
 *
 *   - no `?a=…` → empty-state prompt
 *   - `?a=` set but no `?b=` → second column is the picker
 *   - typing into the picker enables the Compare button
 *
 * `TrialColumn` queries are stubbed to never resolve (pending forever)
 * so the column renders its LoadingState rather than the error path.
 */
import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";

import TrialCompare from "../../pages/TrialCompare";
import { renderWithProviders } from "../../test-utils/renderWithProviders";

function neverResolvingFetch(): void {
  vi.spyOn(globalThis, "fetch").mockImplementation(
    () => new Promise(() => undefined),
  );
}

describe("TrialCompare", () => {
  beforeEach(() => {
    window.localStorage.clear();
    window.localStorage.setItem("loom_token", "test-token");
    vi.restoreAllMocks();
  });

  it("renders an empty-state prompt when neither trial is selected", () => {
    renderWithProviders(<TrialCompare />, { route: "/trials/compare" });
    expect(screen.getByText("Compare trials")).toBeInTheDocument();
    expect(screen.getByText("Compare checklist")).toBeInTheDocument();
    expect(
      screen.getByText(/Open a trial from the Trials list/i),
    ).toBeInTheDocument();
  });

  it("renders the picker when only `a` is supplied", () => {
    neverResolvingFetch();
    renderWithProviders(<TrialCompare />, {
      route: "/trials/compare?a=aaa",
    });
    expect(
      screen.getByPlaceholderText(/0{8}-0{4}-0{4}-0{4}-0{12}/),
    ).toBeInTheDocument();
  });

  it("Compare button in the picker enables once a value is typed", async () => {
    neverResolvingFetch();
    const user = userEvent.setup();
    renderWithProviders(<TrialCompare />, {
      route: "/trials/compare?a=aaa",
    });
    const button = screen.getByRole("button", { name: /^compare$/i });
    expect(button).toBeDisabled();
    const input = screen.getByPlaceholderText(/0{8}/);
    await user.type(input, "bbb");
    expect(button).not.toBeDisabled();
  });
});
