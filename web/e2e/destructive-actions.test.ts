import AxeBuilder from "@axe-core/playwright";
import type { Page } from "@playwright/test";

import { expect, test } from "./fixtures/guardedTest";

async function expectNoSeriousAxeViolations(
  page: Page,
): Promise<void> {
  const results = await new AxeBuilder({ page })
    // The modal backdrop intentionally changes composited colors; contrast is
    // owned by the route-level #777 checks, while this check covers dialog
    // semantics and interaction-specific serious/critical rules.
    .disableRules(["color-contrast"])
    .analyze();
  const serious = results.violations.filter(
    (violation) =>
      violation.impact === "serious" || violation.impact === "critical",
  );
  expect(serious, JSON.stringify(serious, null, 2)).toEqual([]);
}

test("typed provider deletion preserves failure context and focuses the destination", async ({
  apiHarness,
  browserHarness,
  failureSink,
  page,
}) => {
  failureSink.expectDiagnostic({
    kind: "console",
    level: "error",
    message:
      "Failed to load resource: the server responded with a status of 409 (Conflict)",
    count: 1,
  });
  await apiHarness.install({
    role: "user",
    overrides: [
      {
        name: "provider detail",
        method: "GET",
        path: "/api/v1/provider-connections/provider-1",
        response: {
          kind: "json",
          status: 200,
          body: {
            id: "provider-1",
            name: "browser-provider",
            type: "openai-compatible",
            base_url: "https://provider.example.test/v1",
            status: "valid",
            allowed_models: ["browser-model"],
            pricing_source: null,
            rate_card_provider: null,
          },
        },
      },
      {
        name: "first provider delete conflicts",
        method: "DELETE",
        path: "/api/v1/provider-connections/provider-1",
        response: {
          kind: "json",
          status: 409,
          body: {
            detail:
              "synthetic conflict sk-proj-abcdefghijklmnopqrstuvwxyz0123456789",
          },
        },
      },
      {
        name: "provider delete retry succeeds",
        method: "DELETE",
        path: "/api/v1/provider-connections/provider-1",
        response: { kind: "json", status: 200, body: {} },
      },
    ],
    expectations: [
      {
        name: "provider deletion attempted exactly twice",
        method: "DELETE",
        path: "/api/v1/provider-connections/provider-1",
        status: 409,
        count: 1,
      },
      {
        name: "provider deletion succeeds exactly once",
        method: "DELETE",
        path: "/api/v1/provider-connections/provider-1",
        status: 200,
        count: 1,
      },
    ],
  });

  await page.goto(
    `${browserHarness.baseURL}/providers/provider-1?tab=settings`,
  );
  await page.getByRole("button", { name: "Delete" }).click();
  const dialog = page.getByRole("dialog", { name: "Delete connection" });
  await expect(dialog).toBeVisible();
  const confirmation = page.getByLabel(
    "Type connection name to confirm: browser-provider",
  );
  await confirmation.fill("Browser-Provider");
  await expect(
    page.getByRole("button", { name: "Delete connection" }),
  ).toBeDisabled();
  await confirmation.fill("browser-provider");
  await page.getByRole("button", { name: "Delete connection" }).click();

  await expect(dialog.getByRole("alert")).toContainText("Error 409");
  await expect(dialog.getByRole("alert")).toContainText("[REDACTED]");
  await expect(confirmation).toHaveValue("browser-provider");
  await expectNoSeriousAxeViolations(page);

  await page.getByRole("button", { name: "Delete connection" }).click();
  await expect(page).toHaveURL(`${browserHarness.baseURL}/providers`);
  await expect(
    page.getByRole("heading", { name: "Provider connections" }),
  ).toBeFocused();
});

test("simple token revoke stays modal while pending and submits once", async ({
  apiHarness,
  browserHarness,
  page,
}) => {
  await apiHarness.install({
    role: "user",
    overrides: [
      {
        name: "token list before and after revoke",
        method: "GET",
        path: "/api/v1/tokens",
        count: 2,
        response: {
          kind: "json",
          status: 200,
          body: {
            items: [
              {
                name: "Browser CLI",
                token_hash_prefix: "e2e12345",
                type: "team",
                scopes: ["read:own"],
                team_id: "team-eai",
                issued_at: "2026-07-20T00:00:00Z",
                expires_at: "2027-07-20T00:00:00Z",
                revoked_at: null,
                last_used_at: null,
              },
            ],
          },
        },
      },
      {
        name: "delayed token revoke",
        method: "DELETE",
        path: "/api/v1/tokens/e2e12345",
        response: {
          kind: "text",
          status: 204,
          body: "",
          contentType: "text/plain",
          delayMs: 400,
        },
      },
    ],
    expectations: [
      {
        name: "token revoke is submitted once",
        method: "DELETE",
        path: "/api/v1/tokens/e2e12345",
        status: 204,
        count: 1,
      },
    ],
  });

  await page.goto(`${browserHarness.baseURL}/settings`);
  const tokenRow = page.getByRole("row").filter({ hasText: "Browser CLI" });
  const trigger = tokenRow.getByRole("button", { name: "Revoke" });
  await trigger.click();
  const dialog = page.getByRole("dialog", { name: "Revoke API token" });
  await expect(dialog).toContainText("Browser CLI (e2e12345)");
  await expectNoSeriousAxeViolations(page);

  const confirm = dialog.getByRole("button", { name: "Revoke token" });
  await confirm.click();
  await expect(dialog.getByRole("status")).toHaveText("Revoking…");
  await expect(dialog.getByRole("button", { name: "Cancel" })).toBeDisabled();
  await page.keyboard.press("Escape");
  await expect(dialog).toBeVisible();

  await expect(dialog).toBeHidden();
  await expect(trigger).toBeFocused();
});
