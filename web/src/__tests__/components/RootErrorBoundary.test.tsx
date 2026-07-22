import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import { RootErrorBoundary } from "../../components/RootErrorBoundary";
import {
  BROWSER_ERROR_REDACTION,
  clearBrowserFailureConsoleRedaction,
  installBrowserErrorEventRedaction,
  installBrowserConsoleErrorRedaction,
  markBrowserFailureForConsoleRedaction,
  setBrowserFailureReporter,
  setBrowserFailureUnhandledSignaler,
  type BrowserFailureReport,
} from "../../lib/errorReporting";
import { allowExpectedUnhandledRejection } from "../../test-utils/qualityGuards";

const BROKEN_ROOT_ERROR = new Error(
  "raw-root-loom_api_abcdefghijklmnopqrstuvwxyz012345",
);

function BrokenRoot(): JSX.Element {
  throw BROKEN_ROOT_ERROR;
}

describe("RootErrorBoundary", () => {
  afterEach(() => {
    window.history.replaceState(null, "", "/");
    setBrowserFailureReporter(null);
    setBrowserFailureUnhandledSignaler(null);
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
      const callsAfterBoundary = consoleSink.mock.calls.length;
      console.error(BROKEN_ROOT_ERROR);
      expect(consoleSink).toHaveBeenCalledTimes(callsAfterBoundary);
      await new Promise((resolve) => window.setTimeout(resolve, 75));
      console.error(BROKEN_ROOT_ERROR);
      expect(consoleSink.mock.calls.at(-1)).toEqual([
        BROWSER_ERROR_REDACTION,
      ]);
      const serializedConsole = JSON.stringify(consoleSink.mock.calls);
      expect(serializedConsole).not.toContain("raw-root");
      expect(serializedConsole).not.toContain("loom_api_");
      expect(serializedConsole).not.toContain("signature");
      expect(serializedConsole).not.toContain("#raw");
    } finally {
      restoreConsoleRedaction();
    }
  });

  it("clears boundary state when the user retries", async () => {
    let shouldFail = true;
    const retryError = new Error("retryable root failure");
    const onRetry = vi.fn(() => {
      shouldFail = false;
    });
    vi.spyOn(console, "error").mockImplementation(() => undefined);
    setBrowserFailureReporter(() => undefined);

    function RetryableRoot(): JSX.Element {
      if (shouldFail) throw retryError;
      return <p>Root recovered</p>;
    }

    render(
      <RootErrorBoundary onRetry={onRetry}>
        <RetryableRoot />
      </RootErrorBoundary>,
    );

    const user = userEvent.setup();
    await user.click(screen.getByRole("button", { name: "Retry" }));
    expect(onRetry).toHaveBeenCalledTimes(1);
    expect(screen.getByText("Root recovered")).toBeInTheDocument();
  });

  it("reports a fresh reference when retry fails with the same error", async () => {
    const reports: BrowserFailureReport[] = [];
    const repeatedError = new Error("same retry failure");
    setBrowserFailureReporter((report) => reports.push(report));
    vi.spyOn(console, "error").mockImplementation(() => undefined);

    function RepeatedFailure(): JSX.Element {
      throw repeatedError;
    }

    render(
      <RootErrorBoundary>
        <RepeatedFailure />
      </RootErrorBoundary>,
    );
    const firstReference = screen
      .getByRole("alert")
      .textContent?.match(/WEB-[0-9A-F]{8}/u)?.[0];
    expect(firstReference).toBeDefined();

    await new Promise((resolve) => window.setTimeout(resolve, 75));
    await userEvent
      .setup()
      .click(screen.getByRole("button", { name: "Retry" }));

    const secondReference = screen
      .getByRole("alert")
      .textContent?.match(/WEB-[0-9A-F]{8}/u)?.[0];
    expect(secondReference).toBeDefined();
    expect(secondReference).not.toBe(firstReference);
    expect(reports.map(({ referenceId }) => referenceId)).toEqual([
      firstReference,
      secondReference,
    ]);
  });

  it("reports distinct root sibling failures with distinct references", () => {
    const reports: BrowserFailureReport[] = [];
    setBrowserFailureReporter((report) => reports.push(report));
    vi.spyOn(console, "error").mockImplementation(() => undefined);

    function BrokenSibling({ error }: { error: Error }): JSX.Element {
      throw error;
    }

    render(
      <RootErrorBoundary>
        <BrokenSibling error={new Error("first root sibling")} />
        <BrokenSibling error={new Error("second root sibling")} />
      </RootErrorBoundary>,
    );

    expect(screen.getByRole("alert")).toBeInTheDocument();
    expect(reports).toHaveLength(2);
    expect(reports.map(({ kind }) => kind)).toEqual([
      "root-render",
      "root-render",
    ]);
    expect(new Set(reports.map(({ referenceId }) => referenceId)).size).toBe(
      2,
    );
  });

  it("keeps tainted objects redacted without retaining primitive markers", () => {
    const consoleSink = vi
      .spyOn(console, "error")
      .mockImplementation(() => undefined);
    const restoreConsoleRedaction = installBrowserConsoleErrorRedaction();
    const captured = new Error(
      "raw-root-loom_api_abcdefghijklmnopqrstuvwxyz012345",
    ) as Error & { responseBody: string };
    captured.responseBody = "raw-response-body-secret";
    const primitive = "raw-primitive-loom-token";

    try {
      markBrowserFailureForConsoleRedaction(captured);
      console.error(captured);
      clearBrowserFailureConsoleRedaction(captured);
      console.error(captured);
      markBrowserFailureForConsoleRedaction(primitive);
      console.error(primitive);
      clearBrowserFailureConsoleRedaction(primitive);
      console.error(primitive);
      console.error("unrelated safe diagnostic");

      expect(consoleSink.mock.calls[0]).toEqual([BROWSER_ERROR_REDACTION]);
      expect(consoleSink.mock.calls[1]).toEqual([BROWSER_ERROR_REDACTION]);
      expect(consoleSink.mock.calls[2]).toEqual([BROWSER_ERROR_REDACTION]);
      expect(consoleSink.mock.calls[3]).toEqual([primitive]);
      expect(consoleSink.mock.calls[4]).toEqual([
        "unrelated safe diagnostic",
      ]);
    } finally {
      restoreConsoleRedaction();
    }
  });

  it("sanitizes and reports an unclaimed browser error with bounded location", async () => {
    window.history.replaceState(null, "", "/dev/settings?token=raw#secret");
    const consoleSink = vi
      .spyOn(console, "error")
      .mockImplementation(() => undefined);
    const reports: BrowserFailureReport[] = [];
    const signals: Array<{ kind: string; error: Error }> = [];
    setBrowserFailureReporter((report) => reports.push(report));
    setBrowserFailureUnhandledSignaler((kind, error) => {
      signals.push({ kind, error });
    });
    const restoreConsoleRedaction = installBrowserConsoleErrorRedaction();
    const restoreErrorRedaction = installBrowserErrorEventRedaction();
    const downstream = vi.fn<(event: ErrorEvent) => void>();
    window.addEventListener("error", downstream);
    const rawError = new Error(
      "raw-event-loom_api_abcdefghijklmnopqrstuvwxyz012345",
    ) as Error & { responseBody: string };
    rawError.responseBody = "raw-response-body-secret";
    const event = new ErrorEvent("error", {
      cancelable: true,
      colno: 42,
      error: rawError,
      filename: `${window.location.origin}/src/raw-token.js?signature=raw#secret`,
      lineno: 17,
      message: rawError.message,
    });

    try {
      window.dispatchEvent(event);
      await new Promise((resolve) => window.setTimeout(resolve, 75));

      expect(event.defaultPrevented).toBe(true);
      expect(downstream).toHaveBeenCalledTimes(1);
      expect(event.error).not.toBe(rawError);
      expect(event.error).toBeInstanceOf(Error);
      expect((event.error as Error).message).toBe(BROWSER_ERROR_REDACTION);
      expect((event.error as Error).stack).toBe(
        `Error: ${BROWSER_ERROR_REDACTION}`,
      );
      expect(event.message).toBe(BROWSER_ERROR_REDACTION);
      expect(event.filename).toBe("");
      expect(event.lineno).toBe(0);
      expect(event.colno).toBe(0);
      expect(rawError.message).toBe(BROWSER_ERROR_REDACTION);
      expect(rawError.stack).toBe(`Error: ${BROWSER_ERROR_REDACTION}`);
      expect(consoleSink).not.toHaveBeenCalled();
      console.error(rawError);
      expect(consoleSink.mock.calls.at(-1)).toEqual([
        BROWSER_ERROR_REDACTION,
      ]);
      expect(reports).toEqual([
        expect.objectContaining({
          column: 42,
          kind: "uncaught-runtime",
          line: 17,
          pathname: "/dev/settings",
          referenceId: expect.stringMatching(/^WEB-[0-9A-F]{8}$/u),
          sourcePath: "/src/raw-token.js",
        }),
      ]);
      expect(JSON.stringify(reports)).not.toContain("signature");
      expect(JSON.stringify(reports)).not.toContain("response-body");
      expect(signals).toHaveLength(1);
      expect(signals[0]?.kind).toBe("uncaught-runtime");
      expect(signals[0]?.error.message).toMatch(
        /^Loom browser error details redacted\. Reference WEB-[0-9A-F]{8}\.$/u,
      );
      expect(signals[0]?.error.message).toContain(
        reports[0]?.referenceId ?? "missing-reference",
      );
    } finally {
      window.removeEventListener("error", downstream);
      restoreErrorRedaction();
      restoreConsoleRedaction();
    }
  });

  it("keeps distinct errors from the same source location independently observable", async () => {
    window.history.replaceState(null, "", "/dev/monitor");
    vi.spyOn(console, "error").mockImplementation(() => undefined);
    const reports: BrowserFailureReport[] = [];
    const signals: Array<{ kind: string; error: Error }> = [];
    setBrowserFailureReporter((report) => reports.push(report));
    setBrowserFailureUnhandledSignaler((kind, error) => {
      signals.push({ kind, error });
    });
    const restoreConsoleRedaction = installBrowserConsoleErrorRedaction();
    const restoreErrorRedaction = installBrowserErrorEventRedaction();
    const filename = `${window.location.origin}/src/repeated-source.ts`;

    try {
      for (const label of ["first raw failure", "second raw failure"]) {
        const rawError = new Error(label);
        window.dispatchEvent(
          new ErrorEvent("error", {
            cancelable: true,
            colno: 9,
            error: rawError,
            filename,
            lineno: 27,
            message: rawError.message,
          }),
        );
      }
      await new Promise((resolve) => window.setTimeout(resolve, 75));

      expect(reports).toHaveLength(2);
      expect(signals).toHaveLength(2);
      expect(new Set(reports.map((report) => report.referenceId)).size).toBe(2);
      expect(
        reports.map(({ kind, sourcePath, line, column }) => ({
          kind,
          sourcePath,
          line,
          column,
        })),
      ).toEqual([
        {
          kind: "uncaught-runtime",
          sourcePath: "/src/repeated-source.ts",
          line: 27,
          column: 9,
        },
        {
          kind: "uncaught-runtime",
          sourcePath: "/src/repeated-source.ts",
          line: 27,
          column: 9,
        },
      ]);
      expect(signals.map(({ error }) => error.message)).toEqual(
        reports.map(
          ({ referenceId }) =>
            `${BROWSER_ERROR_REDACTION} Reference ${referenceId}.`,
        ),
      );
    } finally {
      restoreErrorRedaction();
      restoreConsoleRedaction();
    }
  });

  it("sanitizes an unhandled rejection and retains a safe reference", async () => {
    window.history.replaceState(null, "", "/prod/monitor?token=raw#secret");
    const consoleSink = vi
      .spyOn(console, "error")
      .mockImplementation(() => undefined);
    const reports: BrowserFailureReport[] = [];
    const signals: Array<{ kind: string; error: Error }> = [];
    setBrowserFailureReporter((report) => reports.push(report));
    setBrowserFailureUnhandledSignaler((kind, error) => {
      signals.push({ kind, error });
    });
    const restoreConsoleRedaction = installBrowserConsoleErrorRedaction();
    const restoreErrorRedaction = installBrowserErrorEventRedaction();
    const downstream = vi.fn<(event: PromiseRejectionEvent) => void>();
    window.addEventListener("unhandledrejection", downstream);
    const rawReason = new Error(
      "raw-rejection-loom_api_abcdefghijklmnopqrstuvwxyz012345",
    ) as Error & { responseBody: string };
    rawReason.responseBody = "raw-rejection-response-body";
    const event = new Event("unhandledrejection", {
      cancelable: true,
    }) as PromiseRejectionEvent;
    Object.defineProperties(event, {
      promise: { configurable: true, value: Promise.resolve() },
      reason: { configurable: true, value: rawReason },
    });

    try {
      // This test deliberately dispatches a synthetic unhandled rejection to
      // verify redaction; acknowledge it so the shared quality guard does not
      // treat it as an unexpected failure.
      allowExpectedUnhandledRejection();
      window.dispatchEvent(event);
      await new Promise((resolve) => window.setTimeout(resolve, 75));

      expect(event.defaultPrevented).toBe(true);
      expect(downstream).toHaveBeenCalledTimes(1);
      expect(event.reason).not.toBe(rawReason);
      expect((event.reason as Error).message).toBe(
        BROWSER_ERROR_REDACTION,
      );
      expect(consoleSink).not.toHaveBeenCalled();
      console.error(rawReason);
      expect(consoleSink.mock.calls.at(-1)).toEqual([
        BROWSER_ERROR_REDACTION,
      ]);
      expect(reports).toEqual([
        expect.objectContaining({
          kind: "unhandled-rejection",
          pathname: "/prod/monitor",
          referenceId: expect.stringMatching(/^WEB-[0-9A-F]{8}$/u),
        }),
      ]);
      expect(JSON.stringify(reports)).not.toContain("response-body");
      expect(JSON.stringify(reports)).not.toContain("loom_api_");
      expect(signals).toHaveLength(1);
      expect(signals[0]?.kind).toBe("unhandled-rejection");
      expect(signals[0]?.error.message).toContain(
        reports[0]?.referenceId ?? "missing-reference",
      );
    } finally {
      window.removeEventListener("unhandledrejection", downstream);
      restoreErrorRedaction();
      restoreConsoleRedaction();
    }
  });
});
