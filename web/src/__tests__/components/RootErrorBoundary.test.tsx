import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import { RootErrorBoundary } from "../../components/RootErrorBoundary";
import {
  setBrowserFailureReporter,
  type BrowserFailureReport,
} from "../../lib/errorReporting";

function BrokenRoot(): JSX.Element {
  throw new Error("raw-root-loom_api_abcdefghijklmnopqrstuvwxyz012345");
}

describe("RootErrorBoundary", () => {
  afterEach(() => {
    window.history.replaceState(null, "", "/");
    setBrowserFailureReporter(null);
    vi.restoreAllMocks();
  });

  it("replaces a root render failure with a redacted, recoverable panel", async () => {
    window.history.replaceState(
      null,
      "",
      "/prod/library/loom_api_abcdefghijklmnopqrstuvwxyz012345?signature=raw#raw",
    );
    const reports: BrowserFailureReport[] = [];
    setBrowserFailureReporter((report) => reports.push(report));
    vi.spyOn(console, "error").mockImplementation(() => undefined);
    const onReload = vi.fn();
    const user = userEvent.setup();

    render(
      <RootErrorBoundary onReload={onReload}>
        <BrokenRoot />
      </RootErrorBoundary>,
    );

    const alert = screen.getByRole("alert");
    expect(alert).toHaveTextContent("Loom could not display this page");
    expect(alert.textContent).toMatch(/WEB-[0-9A-F]{8}/u);
    expect(alert).not.toHaveTextContent("raw-root");
    expect(screen.getByRole("link", { name: "Go to Loom home" })).toHaveAttribute(
      "href",
      "/prod/",
    );
    await user.click(screen.getByRole("button", { name: "Reload Loom" }));
    expect(onReload).toHaveBeenCalledTimes(1);
    expect(reports).toEqual([
      expect.objectContaining({
        kind: "root-render",
        referenceId: expect.stringMatching(/^WEB-[0-9A-F]{8}$/u),
        pathname: expect.not.stringContaining("loom_api_"),
      }),
    ]);
    expect(JSON.stringify(reports)).not.toContain("signature");
    expect(JSON.stringify(reports)).not.toContain("raw-root");
  });
});
