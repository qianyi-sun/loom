import type { Page, Route } from "@playwright/test";

import { expect, test, waitForReady } from "./fixtures/guardedTest";

// #2183: an already-open page must notice a newly served frontend build via
// the 10-minute visible-page check and focus/visibility changes, without
// reloading or discarding input, and refresh only on request.
const SERVED_B = "b2183".padEnd(40, "b");
const TEN_MINUTES = 10 * 60_000;

/**
 * Headless Chromium keeps every tab "visible" and emits no focus or
 * visibility events on tab switches (`bringToFront` and window minimising
 * included), so the transition is driven here: the page's reported
 * visibility changes and the browser dispatches the same events a real tab
 * switch does. Everything else is the real built bundle.
 */
async function switchTab(page: Page, state: "hidden" | "visible"): Promise<void> {
  await page.evaluate((next) => {
    if (next === "hidden") {
      Object.defineProperty(document, "visibilityState", {
        configurable: true,
        get: () => "hidden",
      });
      window.dispatchEvent(new Event("blur"));
      document.dispatchEvent(new Event("visibilitychange"));
    } else {
      // Restore the native (visible) value so later lifecycle events, such
      // as the unload on Refresh, report the browser's own state.
      delete (document as { visibilityState?: unknown }).visibilityState;
      document.dispatchEvent(new Event("visibilitychange"));
      window.dispatchEvent(new Event("focus"));
    }
  }, state);
}

test("an open page detects a new served build and refreshes only on request", async ({
  apiHarness,
  browserHarness,
  page,
  failureSink,
}, testInfo) => {
  void failureSink;
  test.skip(
    (page.viewportSize()?.width ?? 1440) < 1024,
    "the version entry sits behind the compact Menu below lg; covered by sidebar-version.test.ts",
  );
  const loaded = browserHarness.loadedBuildRevision;
  let servedRevision = loaded;
  let configRequests = 0;

  await page.clock.install();
  await apiHarness.install({ role: "user" });
  // Registered after the closed fixture router, so it answers first.
  await page.route(`**${browserHarness.routePrefix}/loom-frontend-config.json`, (route: Route) => {
    configRequests += 1;
    return route.fulfill({
      status: 200,
      contentType: "application/json",
      headers: { "Cache-Control": "no-store" },
      body: JSON.stringify({
        environment: browserHarness.runtimeEnvironment,
        environmentLabel: "Local browser quality gate fixture",
        routePath: browserHarness.routePrefix,
        apiBase: browserHarness.routePrefix,
        apiRouteBase: browserHarness.apiBaseURL,
        buildRevision: servedRevision,
      }),
    });
  });

  await page.goto(`${browserHarness.baseURL}/providers/new`);
  await waitForReady(page, { locator: "main" });
  const revisionLine = page.getByTestId("sidebar-build-revision");
  const dot = page.getByTitle("A newer build is available");
  await expect(revisionLine).toHaveText(`Build ${loaded.slice(0, 12)}`);
  await expect(dot).toHaveCount(0);
  expect(configRequests).toBe(1);

  // includeHidden: the details modal later marks the page behind it inert.
  const draft = page.getByRole("textbox", { includeHidden: true }).first();
  await draft.fill("unsaved #2183 input");

  // Build B is rolled out while page A stays open.
  servedRevision = SERVED_B;
  await page.clock.runFor(TEN_MINUTES - 1_000);
  expect(configRequests).toBe(1);
  await page.clock.runFor(1_000);
  await expect(dot).toBeVisible();
  expect(configRequests).toBe(2);
  await expect(revisionLine).toHaveText(`Build ${loaded.slice(0, 12)}`);
  await expect(draft).toHaveValue("unsaved #2183 input");

  // Switching away hides the page: no periodic checks while hidden.
  await switchTab(page, "hidden");
  await page.clock.runFor(3 * TEN_MINUTES);
  expect(configRequests).toBe(2);

  // Returning fires focus + visibilitychange: exactly one coalesced check,
  // no catch-up burst for the missed intervals.
  await switchTab(page, "visible");
  await expect.poll(() => configRequests).toBe(3);
  await page.clock.runFor(5_000);
  expect(configRequests).toBe(3);

  // Switching away and back again inside 60 seconds is throttled.
  await switchTab(page, "hidden");
  await switchTab(page, "visible");
  await page.clock.runFor(1_000);
  expect(configRequests).toBe(3);

  // Opening details checks immediately, even inside the throttle window.
  await page.getByRole("button", { name: "Deployed version details" }).click();
  await expect.poll(() => configRequests).toBe(4);
  const dialog = page.getByRole("dialog", { name: "Deployed version" });
  await expect(dialog.getByText("A newer frontend build is available.")).toBeVisible();
  await expect(draft).toHaveValue("unsaved #2183 input");
  await testInfo.attach("update-notice", {
    body: await page.screenshot(),
    contentType: "image/png",
  });

  // This harness can only serve its one stamped bundle, so the "replacement"
  // is served as matching once the user explicitly refreshes.
  servedRevision = loaded;
  await dialog.getByRole("button", { name: "Refresh" }).click();
  await waitForReady(page, { locator: "main" });
  await expect(revisionLine).toHaveText(`Build ${loaded.slice(0, 12)}`);
  await expect(dot).toHaveCount(0);
  expect(configRequests).toBe(5);
});
