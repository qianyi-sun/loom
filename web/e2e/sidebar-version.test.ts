import { expect, test, waitForReady } from "./fixtures/guardedTest";

// #2009 (reopened): the loaded frontend revision must be readable in the
// sidebar with details closed, even beside a long environment label (the
// fixture's "Local browser quality gate fixture" is deliberately long).
test("sidebar shows the loaded build revision without opening details", async ({
  apiHarness,
  browserHarness,
  page,
  failureSink,
}, testInfo) => {
  void failureSink;
  await apiHarness.install({ role: "user" });
  await page.goto(`${browserHarness.baseURL}/settings`);
  await waitForReady(page, { locator: "main" });

  // Below lg the whole nav (version entry included) collapses behind the
  // compact Menu button; open it first. The version details stay closed.
  const nav = page.getByRole("navigation", { name: "Primary" });
  if ((page.viewportSize()?.width ?? 1440) < 1024) {
    await nav.getByRole("button", { name: "Menu", exact: true }).click();
  }

  const entry = page.getByRole("button", { name: "Deployed version details" });
  const revision = page.getByTestId("sidebar-build-revision");
  await expect(revision).toBeVisible();
  // Unstamped browser-test builds honestly show "local"; stamped builds
  // show the 12-character commit.
  await expect(revision).toHaveText(/^Build (local|[0-9a-f]{12})$/);
  await expect(page.getByRole("dialog", { name: "Deployed version" })).toHaveCount(0);

  const clipped = await revision.evaluate((el) => {
    const line = el.getBoundingClientRect();
    const box = el.parentElement!.getBoundingClientRect();
    return el.scrollWidth > el.clientWidth || line.right > box.right + 0.5;
  });
  expect(clipped).toBe(false);
  await expect
    .poll(() => page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth))
    .toBe(true);

  await testInfo.attach("sidebar-version-entry", {
    path: await nav
      .screenshot({ path: testInfo.outputPath("sidebar-version-entry.png") })
      .then(() => testInfo.outputPath("sidebar-version-entry.png")),
    contentType: "image/png",
  });

  await entry.click();
  await expect(page.getByRole("dialog", { name: "Deployed version" })).toBeVisible();
});
