import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import { RootErrorBoundary } from "../../components/RootErrorBoundary";
import {
  installBrowserConsoleErrorRedaction,
  markBrowserFailureForConsoleRedaction,
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
    // Capture the output *under* the production console bridge. This asserts
    // what React forwards after redaction instead of mocking the bridge away.
    const consoleSink = vi
      .spyOn(console, "error")
      .mockImplementation(() => undefined);
    const restoreConsoleRedaction = installBrowserConsoleErrorRedaction();
    const onReload = vi.fn();
    const user = userEvent.setup();

    try {
      render(
        <RootErrorBoundary onReload={onReload}>
          <BrokenRoot />
        </RootErrorBoundary>,
      );

      const alert = screen.getByRole("alert");
      expect(alert).toHaveTextContent("Loom could not display this page");
      const referenceId = alert.textContent?.match(/WEB-[0-9A-F]{8}/u)?.[0];
      expect(referenceId).toBeDefined();
      expect(alert).not.toHaveTextContent("raw-root");
      expect(
        screen.getByRole("link", { name: "Go to Loom home" }),
      ).toHaveAttribute("href", "/prod/");
      await user.click(screen.getByRole("button", { name: "Reload Loom" }));
      expect(onReload).toHaveBeenCalledTimes(1);
      expect(reports).toEqual([
        expect.objectContaining({
          kind: "root-render",
          referenceId,
          pathname: expect.not.stringContaining("loom_api_"),
        }),
      ]);
      expect(JSON.stringify(reports)).not.toContain("signature");
      expect(JSON.stringify(reports)).not.toContain("raw-root");
      const serializedConsole = JSON.stringify(consoleSink.mock.calls);
      expect(serializedConsole).not.toContain("raw-root");
      expect(serializedConsole).not.toContain("loom_api_");
      expect(serializedConsole).not.toContain("signature");
      expect(serializedConsole).not.toContain("#raw");
    } finally {
      restoreConsoleRedaction();
    }
  });

  it("redacts only an error value marked by the boundary", () => {
    const consoleSink = vi
      .spyOn(console, "error")
      .mockImplementation(() => undefined);
    const restoreConsoleRedaction = installBrowserConsoleErrorRedaction();
    const captured = new Error(
      "raw-root-loom_api_abcdefghijklmnopqrstuvwxyz012345",
    );

    try {
      markBrowserFailureForConsoleRedaction(captured);
      console.error(captured);
      console.error("unrelated safe diagnostic");

      expect(consoleSink.mock.calls).toEqual([
        ["Loom browser error details redacted."],
        ["unrelated safe diagnostic"],
      ]);
    } finally {
      restoreConsoleRedaction();
    }
  });
});
