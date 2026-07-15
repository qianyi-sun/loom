import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import React from "react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { FrontendBootstrap } from "../../bootstrap/FrontendBootstrap";
import {
  setBrowserFailureReporter,
  type BrowserFailureReport,
} from "../../lib/errorReporting";
import { setFrontendConfigForTests } from "../../lib/frontendConfig";

function validDevConfig(): Response {
  return new Response(
    JSON.stringify({
      environment: "development",
      environmentLabel: "Development / staging",
      routePath: "/dev",
      apiBase: "/dev",
    }),
    { status: 200, headers: { "Content-Type": "application/json" } },
  );
}

describe("FrontendBootstrap", () => {
  afterEach(() => {
    window.history.replaceState(null, "", "/");
    setFrontendConfigForTests(null);
    setBrowserFailureReporter(null);
    vi.restoreAllMocks();
  });

  it("renders a startup shell and issues one request under StrictMode", async () => {
    window.history.replaceState(null, "", "/dev/monitor");
    const fetchMock = vi
      .spyOn(globalThis, "fetch")
      .mockResolvedValue(validDevConfig());

    render(
      <React.StrictMode>
        <FrontendBootstrap>
          {(config) => <p>Ready on {config.routePath}</p>}
        </FrontendBootstrap>
      </React.StrictMode>,
    );

    expect(screen.getByRole("status")).toHaveTextContent("Starting Loom");
    expect(await screen.findByText("Ready on /dev")).toBeInTheDocument();
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it("reports only a redacted pathname and renders bounded recovery copy", async () => {
    window.history.replaceState(
      null,
      "",
      "/dev/auth/reset/loom_reset_abcdefghijklmnopqrstuvwxyz012345?token=secret#raw",
    );
    vi.spyOn(globalThis, "fetch").mockRejectedValue(
      new Error("raw-error-loom_api_abcdefghijklmnopqrstuvwxyz012345"),
    );
    const reports: BrowserFailureReport[] = [];
    setBrowserFailureReporter((report) => reports.push(report));

    render(
      <FrontendBootstrap>{() => <p>Ready</p>}</FrontendBootstrap>,
    );

    const alert = await screen.findByRole("alert");
    expect(alert).toHaveTextContent("Loom could not start");
    expect(alert).toHaveTextContent("could not reach its runtime configuration");
    expect(alert.textContent).toMatch(/WEB-[0-9A-F]{8}/u);
    expect(alert).not.toHaveTextContent("raw-error");
    expect(alert).not.toHaveTextContent("loom_api_");
    expect(screen.getByRole("link", { name: "Go to Loom home" })).toHaveAttribute(
      "href",
      "/dev/",
    );
    expect(reports).toEqual([
      expect.objectContaining({
        kind: "frontend-config-network",
        referenceId: expect.stringMatching(/^WEB-[0-9A-F]{8}$/u),
        pathname: expect.not.stringContaining("loom_reset_"),
      }),
    ]);
    expect(JSON.stringify(reports)).not.toContain("?token=");
    expect(JSON.stringify(reports)).not.toContain("#raw");
    expect(JSON.stringify(reports)).not.toContain("raw-error");
  });

  it("starts exactly one fresh request when the user retries", async () => {
    window.history.replaceState(null, "", "/dev/settings");
    const fetchMock = vi
      .spyOn(globalThis, "fetch")
      .mockRejectedValueOnce(new Error("offline"))
      .mockResolvedValueOnce(validDevConfig());
    setBrowserFailureReporter(() => undefined);
    const user = userEvent.setup();

    render(
      <React.StrictMode>
        <FrontendBootstrap>{() => <p>Recovered</p>}</FrontendBootstrap>
      </React.StrictMode>,
    );

    await user.click(await screen.findByRole("button", { name: "Try again" }));
    expect(await screen.findByText("Recovered")).toBeInTheDocument();
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2));
  });

  it.each([
    [
      "HTTP",
      () => new Response("secret upstream body", { status: 503 }),
      "temporarily unavailable",
    ],
    [
      "invalid",
      () => new Response("secret invalid payload", { status: 200 }),
      "invalid runtime configuration",
    ],
  ])(
    "renders distinct bounded copy for a %s config failure",
    async (_label, responseFactory, expectedCopy) => {
      window.history.replaceState(null, "", "/dev/monitor");
      vi.spyOn(globalThis, "fetch").mockResolvedValue(responseFactory());
      setBrowserFailureReporter(() => undefined);

      render(
        <FrontendBootstrap>{() => <p>Ready</p>}</FrontendBootstrap>,
      );

      const alert = await screen.findByRole("alert");
      expect(alert).toHaveTextContent(expectedCopy);
      expect(alert).not.toHaveTextContent("secret");
    },
  );
});
