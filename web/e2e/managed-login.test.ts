import { readFile } from "node:fs/promises";
import { expect, test } from "@playwright/test";

// HTTPS is browser-visible but fully intercepted: no DNS/TLS/service contact.
// Serve the actual built entrypoint so fragment scrubbing is exercised before
// the first frontend-config/auth request, not only in a component harness.
test("built managed login scrubs the proof before startup and submits it only on click", async ({ page }) => {
  const origin = "https://managed-login.example.test";
  const token = "loom_env_login_" + "a".repeat(43);
  const failures: string[] = [];
  const submitted: string[] = [];
  let startupReads = 0;
  const owner = {
    user: { id: "owner", username: "owner", email: null, display_name: "Alice", is_platform_admin: false },
    teams: [{ id: "team", name: "Development alice", role: "owner" }],
    current_team: { id: "team", name: "Development alice", role: "owner" },
    role: "owner", scopes: ["read:own", "submit"], is_platform_admin: false, csrf_token: "child-csrf",
  };
  page.on("pageerror", (error) => failures.push(error.message));
  page.on("console", (message) => {
    if (message.type() === "error" || message.type() === "warning" || message.text().includes(token)) {
      failures.push(message.text());
    }
  });
  await page.route(origin + "/**", async (route) => {
    const request = route.request();
    const path = new URL(request.url()).pathname;
    expect(request.url()).not.toContain(token);
    if (path === "/auth/managed") {
      await route.fulfill({ contentType: "text/html", body: await readFile("dist/index.html") });
      return;
    }
    const asset = path.match(/^\/(?:dev\/|prod\/)?assets\/([A-Za-z0-9_-]+\.(js|css))$/);
    if (asset) {
      await route.fulfill({ contentType: asset[2] === "js" ? "text/javascript" : "text/css",
                            body: await readFile("dist/assets/" + asset[1]) });
      return;
    }
    expect(await page.evaluate(() => window.location.hash + window.location.search)).toBe("");
    if (path === "/loom-frontend-config.json") {
      startupReads += 1;
      await route.fulfill({ json: { environment: "development", environmentLabel: "Personal dev", routePath: "",
                                  apiBase: "", apiRouteBase: origin + "/api" } });
    } else if (path === "/api/v1/auth/me") {
      await route.fulfill({ json: owner });
    } else if (path === "/api/v1/auth/login/complete") {
      submitted.push(request.postDataJSON().token);
      await route.fulfill({ json: owner });
    } else {
      failures.push("unexpected request " + path);
      await route.fulfill({ status: 404, body: "not found" });
    }
  });
  await page.goto(origin + "/auth/managed#token=" + token);
  const button = page.getByRole("button", { name: "Sign into this environment" });
  await expect(button).toBeVisible();
  expect(startupReads).toBeGreaterThan(0);
  expect(submitted).toEqual([]);
  expect(page.url()).toBe(origin + "/auth/managed");
  await button.click();
  await expect(page.getByText("Signed in to this environment.")).toBeVisible();
  expect(submitted).toEqual([token]);
  expect(failures).toEqual([]);
});
