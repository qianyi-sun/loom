import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import { BrowserErrorBoundary } from "../../components/BrowserErrorBoundary";
import {
  BROWSER_ERROR_REDACTION,
  installBrowserConsoleErrorRedaction,
  setBrowserFailureReporter,
  type BrowserFailureReport,
} from "../../lib/errorReporting";

describe("BrowserErrorBoundary", () => {
  afterEach(() => {
    setBrowserFailureReporter(null);
    vi.restoreAllMocks();
  });

  it("reports a bounded route failure and retries without retaining the throwable", async () => {
    let shouldFail = true;
    const rawError = Object.assign(
      new Error("raw route loom_api_abcdefghijklmnopqrstuvwxyz012345"),
      { responseBody: "raw route response body" },
    );
    const reports: BrowserFailureReport[] = [];
    setBrowserFailureReporter((report) => reports.push(report));
    const consoleSink = vi
      .spyOn(console, "error")
      .mockImplementation(() => undefined);
    const restoreConsoleRedaction = installBrowserConsoleErrorRedaction();

    function RoutePage(): JSX.Element {
      if (shouldFail) throw rawError;
      return <h1>Route recovered</h1>;
    }

    try {
      render(
        <BrowserErrorBoundary
          resetKey="route-a"
          pathname="/dev/library/loom_api_abcdefghijklmnopqrstuvwxyz012345"
          renderFallback={({ referenceId, retry }) => (
            <div role="alert">
              <span>{referenceId}</span>
              <button
                type="button"
                onClick={() => {
                  shouldFail = false;
                  retry();
                }}
              >
                Retry route
              </button>
            </div>
          )}
        >
          <RoutePage />
        </BrowserErrorBoundary>,
      );

      const alert = screen.getByRole("alert");
      const referenceId = alert.textContent?.match(/WEB-[0-9A-F]{8}/u)?.[0];
      expect(referenceId).toBeDefined();
      expect(alert).not.toHaveTextContent("raw route");
      expect(reports).toEqual([
        expect.objectContaining({
          kind: "route-render",
          pathname: expect.not.stringContaining("loom_api_"),
          referenceId,
        }),
      ]);
      console.error(rawError);
      expect(consoleSink.mock.calls.at(-1)).toEqual([
        BROWSER_ERROR_REDACTION,
      ]);
      expect(JSON.stringify(consoleSink.mock.calls)).not.toContain(
        "response body",
      );

      const user = userEvent.setup();
      await user.click(
        screen.getByRole("button", { name: "Retry route" }),
      );
      expect(
        screen.getByRole("heading", { name: "Route recovered" }),
      ).toBeInTheDocument();
    } finally {
      restoreConsoleRedaction();
    }
  });

  it.each([
    [
      "frozen error",
      Object.freeze(new Error("raw frozen loom_api_abcdefghijklmnopqrstuvwxyz012345")),
    ],
    ["primitive", "raw primitive loom_api_abcdefghijklmnopqrstuvwxyz012345"],
  ])("keeps fallback and report references equal for a %s", (_label, throwable) => {
    const reports: BrowserFailureReport[] = [];
    setBrowserFailureReporter((report) => reports.push(report));
    vi.spyOn(console, "error").mockImplementation(() => undefined);
    const restoreConsoleRedaction = installBrowserConsoleErrorRedaction();

    function BrokenRoute(): JSX.Element {
      throw throwable;
    }

    try {
      render(
        <BrowserErrorBoundary
          resetKey="non-extensible"
          pathname="/dev/non-extensible"
          renderFallback={({ referenceId }) => (
            <div role="alert">Failed safely {referenceId}</div>
          )}
        >
          <BrokenRoute />
        </BrowserErrorBoundary>,
      );

      const referenceId = screen
        .getByRole("alert")
        .textContent?.match(/WEB-[0-9A-F]{8}/u)?.[0];
      expect(referenceId).toBeDefined();
      expect(reports).toEqual([
        expect.objectContaining({
          kind: "route-render",
          referenceId,
        }),
      ]);
      expect(JSON.stringify(reports)).not.toContain("loom_api_");
    } finally {
      restoreConsoleRedaction();
    }
  });

  it.each([
    ["frozen error", Object.freeze(new Error("raw frozen sibling secret"))],
    ["primitive", "raw primitive sibling secret"],
  ])("keeps sibling fallback/report references aligned for a shared %s", (
    _label,
    throwable,
  ) => {
    const reports: BrowserFailureReport[] = [];
    setBrowserFailureReporter((report) => reports.push(report));
    vi.spyOn(console, "error").mockImplementation(() => undefined);
    const restoreConsoleRedaction = installBrowserConsoleErrorRedaction();

    function BrokenRoute(): JSX.Element {
      throw throwable;
    }

    try {
      render(
        <>
          <BrowserErrorBoundary
            resetKey="shared-one"
            pathname="/dev/shared-one"
            renderFallback={({ referenceId }) => (
              <div data-testid="shared-one">{referenceId}</div>
            )}
          >
            <BrokenRoute />
          </BrowserErrorBoundary>
          <BrowserErrorBoundary
            resetKey="shared-two"
            pathname="/dev/shared-two"
            renderFallback={({ referenceId }) => (
              <div data-testid="shared-two">{referenceId}</div>
            )}
          >
            <BrokenRoute />
          </BrowserErrorBoundary>
        </>,
      );

      const fallbackReferences = [
        screen.getByTestId("shared-one").textContent,
        screen.getByTestId("shared-two").textContent,
      ];
      expect(fallbackReferences).toEqual([
        expect.stringMatching(/^WEB-[0-9A-F]{8}$/u),
        expect.stringMatching(/^WEB-[0-9A-F]{8}$/u),
      ]);
      expect(reports.map(({ referenceId }) => referenceId)).toEqual(
        fallbackReferences,
      );
      expect(JSON.stringify(reports)).not.toContain("sibling secret");
    } finally {
      restoreConsoleRedaction();
    }
  });

  it("clears a failed route when the router reset key changes", () => {
    let shouldFail = true;
    const error = new Error("failed route");
    vi.spyOn(console, "error").mockImplementation(() => undefined);
    setBrowserFailureReporter(() => undefined);

    function RoutePage(): JSX.Element {
      if (shouldFail) throw error;
      return <p>Healthy sibling route</p>;
    }

    const fallback = ({ referenceId }: { referenceId: string }) => (
      <div role="alert">Failed safely {referenceId}</div>
    );
    const { rerender } = render(
      <BrowserErrorBoundary
        resetKey="route-a"
        pathname="/dev/broken"
        renderFallback={fallback}
      >
        <RoutePage />
      </BrowserErrorBoundary>,
    );
    expect(screen.getByRole("alert")).toBeInTheDocument();

    shouldFail = false;
    rerender(
      <BrowserErrorBoundary
        resetKey="route-b"
        pathname="/dev/healthy"
        renderFallback={fallback}
      >
        <RoutePage />
      </BrowserErrorBoundary>,
    );

    expect(screen.getByText("Healthy sibling route")).toBeInTheDocument();
    expect(screen.queryByRole("alert")).not.toBeInTheDocument();
  });

  it("reports distinct sibling failures with their own references", () => {
    const reports: BrowserFailureReport[] = [];
    setBrowserFailureReporter((report) => reports.push(report));
    vi.spyOn(console, "error").mockImplementation(() => undefined);
    const firstError = new Error("first sibling failure");
    const secondError = new Error("second sibling failure");

    function BrokenSibling({ error }: { error: Error }): JSX.Element {
      throw error;
    }

    render(
      <BrowserErrorBoundary
        resetKey="two-siblings"
        pathname="/dev/two-siblings"
        renderFallback={({ referenceId }) => (
          <div role="alert">Failed safely {referenceId}</div>
        )}
      >
        <BrokenSibling error={firstError} />
        <BrokenSibling error={secondError} />
      </BrowserErrorBoundary>,
    );

    expect(screen.getByRole("alert")).toBeInTheDocument();
    expect(reports).toHaveLength(2);
    expect(reports.map(({ kind }) => kind)).toEqual([
      "route-render",
      "route-render",
    ]);
    expect(new Set(reports.map(({ referenceId }) => referenceId)).size).toBe(
      2,
    );
  });
});
