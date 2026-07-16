import AxeBuilder from "@axe-core/playwright";

import { installApiFixture, type BrowserRole } from "./fixtures/api";
import { expect, test } from "./fixtures/guardedTest";

const routes: Record<BrowserRole, string[]> = {
  "logged-out": [
    "/",
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
    test(`${role} route ${path} mounts, survives reload, and passes axe`, async ({ page, failureSink }) => {
      if (role === "logged-out") failureSink.allowedUnauthorizedConsoleErrors = 2;
      if (path.startsWith("/auth/") || path.startsWith("/invites/")) {
        failureSink.allowedNotFoundConsoleErrors = 2;
      }
      await installApiFixture(page, role);
      const response = await page.goto(`/dev${path}`);
      expect(response?.ok()).toBe(true);
      await expect(page.locator("#root")).toHaveAttribute("data-loom-mounted", "true");
      await expect(page.locator("#root")).not.toBeEmpty();
      await expect(page.locator("main, [data-testid='public-onboarding-shell']").first()).toBeVisible();

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
      await expect(page.locator("#root")).not.toBeEmpty();
    });
  }
}

test("exact prefix canonicalizes without losing the route", async ({ page, failureSink }) => {
  failureSink.allowedUnauthorizedConsoleErrors = 1;
  await installApiFixture(page, "logged-out");
  const response = await page.goto("/dev");
  expect(response?.ok()).toBe(true);
  await expect(page).toHaveURL(/\/dev\/settings$/u);
  await expect(page.locator("#root")).toHaveAttribute("data-loom-mounted", "true");
});
