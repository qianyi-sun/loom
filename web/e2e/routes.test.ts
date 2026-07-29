import AxeBuilder from "@axe-core/playwright";

import type { BrowserRole } from "./fixtures/api";
import { expect, test, waitForReady } from "./fixtures/guardedTest";

const UNAUTHORIZED_RESOURCE_ERROR =
  "Failed to load resource: the server responded with a status of 401 (Unauthorized)";
const NOT_FOUND_RESOURCE_ERROR =
  "Failed to load resource: the server responded with a status of 404 (Not Found)";

const routes: Record<BrowserRole, string[]> = {
  "logged-out": [
    "/",
    "/auth/login",
    "/settings",
    "/auth/setup?token=expired",
    "/auth/reset?token=expired",
    "/invites/accept?code=expired",
  ],
  user: [
    "/",
    "/batches/new",
    "/monitor",
    "/library",
    "/providers",
    "/task-sets",
    "/settings",
  ],
  admin: ["/admin/access", "/rate-cards"],
};

for (const role of Object.keys(routes) as BrowserRole[]) {
  for (const path of routes[role]) {
    test(`${role} route ${path} mounts, survives reload, and passes axe`, async ({
      apiHarness,
      browserHarness,
      page,
      failureSink,
    }) => {
      if (role === "logged-out") {
        failureSink.expectDiagnostic({
          kind: "console",
          level: "error",
          message: UNAUTHORIZED_RESOURCE_ERROR,
          count: 2,
        });
      }
      if (path.startsWith("/auth/") || path.startsWith("/invites/")) {
        failureSink.expectDiagnostic({
          kind: "console",
          level: "error",
          message: NOT_FOUND_RESOURCE_ERROR,
          count: 2,
        });
      }
      await apiHarness.install({
        role,
        expectations: [
          {
            name: "runtime config is loaded once per navigation",
            method: "GET",
            path: "/loom-frontend-config.json",
            status: 200,
            count: 2,
          },
          {
            name: "authentication settles once per navigation",
            method: "GET",
            path: "/api/v1/auth/me",
            status: role === "logged-out" ? 401 : 200,
            count: 2,
          },
        ],
      });
      const response = await page.goto(`${browserHarness.baseURL}${path}`);
      expect(response?.ok()).toBe(true);
      await expect(page.locator("#root")).toHaveAttribute("data-loom-mounted", "true");
      await expect(page.locator("#root")).toHaveAttribute("data-loom-auth-settled", "true");
      await expect(page.locator("#root")).not.toBeEmpty();
      await waitForReady(page, {
        locator: "main, [data-testid='public-onboarding-shell']",
      });

      await page.addStyleTag({
        content: "*, *::before, *::after { animation: none !important; transition: none !important; }",
      });

      const results = await new AxeBuilder({ page }).analyze();
      const serious = results.violations.filter(
        (violation) => violation.impact === "serious" || violation.impact === "critical",
      );
      expect(serious, JSON.stringify(serious, null, 2)).toEqual([]);

      await page.reload();
      await expect(page.locator("#root")).toHaveAttribute("data-loom-mounted", "true");
      await expect(page.locator("#root")).toHaveAttribute("data-loom-auth-settled", "true");
      await expect(page.locator("#root")).not.toBeEmpty();
    });
  }
}

test("exact prefix canonicalizes without losing the route", async ({
  apiHarness,
  browserHarness,
  page,
  failureSink,
}) => {
  failureSink.expectDiagnostic({
    kind: "console",
    level: "error",
    message: UNAUTHORIZED_RESOURCE_ERROR,
    count: 1,
  });
  await apiHarness.install({
    role: "logged-out",
    expectations: [
      {
        name: "canonical navigation authentication",
        method: "GET",
        path: "/api/v1/auth/me",
        status: 401,
        count: 1,
      },
    ],
  });
  const response = await page.goto(browserHarness.baseURL);
  expect(response?.ok()).toBe(true);
  await expect(page).toHaveURL(`${browserHarness.baseURL}/auth/login`);
  await expect(page.locator("#root")).toHaveAttribute("data-loom-mounted", "true");
});
