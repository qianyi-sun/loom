#!/usr/bin/env node
// Healthy-path admin acceptance through the same login form used by users.
import fs from "node:fs/promises";
import process from "node:process";
import { pathToFileURL } from "node:url";

export function parseOptions(argv) {
  const options = {};
  for (let i = 0; i < argv.length; i += 2) {
    const key = argv[i];
    if (!["--url", "--username", "--password", "--team-id"].includes(key) || !argv[i + 1] || key in options) {
      throw new Error("Expected --url URL --username USER --password env:NAME or file:PATH");
    }
    options[key] = argv[i + 1];
  }
  const url = new URL(options["--url"]);
  if (url.username || url.password || url.search || url.hash ||
      (url.protocol !== "https:" && !(url.protocol === "http:" && ["localhost", "127.0.0.1", "[::1]"].includes(url.hostname)))) {
    throw new Error("Use HTTPS, or local HTTP for a disposable acceptance environment");
  }
  if (!options["--username"] || !/^(env|file):.+/.test(options["--password"] ?? "")) {
    throw new Error("Username and an environment/file password reference are required");
  }
  return { url: url.href.replace(/\/$/, ""), username: options["--username"], passwordSource: options["--password"], teamId: options["--team-id"] };
}

export async function readPassword(source, env = process.env) {
  let password;
  if (source.startsWith("env:")) password = env[source.slice(4)];
  else if (source.startsWith("file:")) {
    const stat = await fs.stat(source.slice(5));
    if (!stat.isFile() || stat.size > 16384) throw new Error("Invalid password source");
    password = (await fs.readFile(source.slice(5), "utf8")).replace(/\r?\n$/, "");
  }
  if (!password || password.length > 16384) throw new Error("Password source is empty or too large");
  return password;
}

export async function runSmoke(options, { chromium, env = process.env } = {}) {
  const password = await readPassword(options.passwordSource, env);
  chromium ??= (await import("@playwright/test")).chromium;
  // Do not pass the password or other credential-bearing environment to Chromium.
  const browserEnv = Object.fromEntries(["PATH", "HOME", "TMPDIR", "DISPLAY", "PLAYWRIGHT_BROWSERS_PATH"]
    .filter(key => env[key] !== undefined).map(key => [key, env[key]]));
  const browser = await chromium.launch({ headless: true, env: browserEnv });
  const context = await browser.newContext();
  let loggedIn = false;
  let cleanLogout = false;
  try {
    const page = await context.newPage();
    page.setDefaultTimeout(30000);
    const errors = [];
    const responses = new Map();
    page.on("pageerror", () => errors.push("page error"));
    page.on("console", message => { if (message.type() === "error" && (loggedIn || !message.text().includes("401"))) errors.push("console error"); });
    page.on("response", response => {
      const url = new URL(response.url());
      if (url.pathname.includes("/api/v1/")) responses.set(url.pathname + url.search, response.status());
    });
    await page.goto(`${options.url}/auth/login`);
    if (new URL(page.url()).origin !== new URL(options.url).origin) {
      throw new Error("Login redirected outside the requested origin");
    }
    await page.getByLabel("Username", { exact: true }).fill(options.username);
    await page.getByLabel("Password", { exact: true }).fill(password);
    const loginResponse = page.waitForResponse(response => response.url() === `${options.url}/api/v1/auth/login`);
    // Even a lost response may follow a server-created session.
    loggedIn = true;
    await page.getByRole("button", { name: "Sign in", exact: true }).click();
    const login = await loginResponse;
    if (login.status() !== 200) throw new Error("Normal login failed");
    const me = await login.json();
    if (!me.is_platform_admin || me.user.username.toLowerCase() !== options.username.toLowerCase()) {
      throw new Error("Expected the dedicated platform administrator");
    }
    if (options.teamId) {
      await page.goto(`${options.url}/settings`);
      const switchResponse = page.waitForResponse(response => response.url() === `${options.url}/api/v1/auth/team`);
      await page.getByLabel("Current team", { exact: true }).selectOption(options.teamId);
      const switched = await switchResponse;
      if (switched.status() !== 200 || (await switched.json()).current_team?.id !== options.teamId) {
        throw new Error("Team switch failed");
      }
      await page.reload();
      await page.waitForFunction(id => document.querySelector('#current-team')?.value === id, options.teamId);
      const refreshed = await context.request.get(`${options.url}/api/v1/auth/me`);
      if (refreshed.status() !== 200 || (await refreshed.json()).current_team?.id !== options.teamId) {
        throw new Error("Team switch did not persist");
      }
    }
    const completedRead = async suffix => {
      const matches = url => {
        const actual = new URL(url);
        const expected = new URL(suffix, options.url);
        return actual.pathname.endsWith(expected.pathname) &&
          [...expected.searchParams].every(([key, value]) => actual.searchParams.get(key) === value);
      };
      const existing = [...responses].find(([path]) => matches(new URL(path, options.url).href));
      const status = existing?.[1] ?? (await page.waitForResponse(response => matches(response.url()))).status();
      if (status !== 200) throw new Error("An admin page read failed");
    };
    await page.goto(`${options.url}/admin/access`);
    const tabs = [
      ["Requests", ["/api/v1/admin/registration-requests?status=pending", "/api/v1/admin/team-registrations?status=pending"]],
      ["Accounts", ["/api/v1/admin/password-reset-requests?status=pending"]],
      ["Teams", ["/api/v1/admin/teams"]],
      ["Invites", ["/api/v1/invites?status=pending"]],
      ["API tokens", ["/api/v1/tokens"]],
      ["Audit", ["/api/v1/admin/audit-events?limit=50"]],
    ];
    for (const [tabName, reads] of tabs) {
      const tab = page.getByRole("tab", { name: tabName, exact: true });
      await tab.click();
      await page.waitForFunction(name => {
        const tab = [...document.querySelectorAll('[role="tab"]')].find(el => el.textContent.trim() === name);
        const panel = tab && document.getElementById(tab.getAttribute("aria-controls"));
        return tab?.getAttribute("aria-selected") === "true" && panel?.getAttribute("role") === "tabpanel";
      }, tabName);
      for (const read of reads) await completedRead(read);
    }
    await page.goto(`${options.url}/rate-cards`);
    await page.getByRole("heading", { name: /Rate cards/i }).first().waitFor();
    await completedRead("/api/v1/rate-cards");
    if (errors.length) throw new Error("Browser reported an error");
    return { status: "passed", checks: ["normal-login", ...(options.teamId ? ["team-switch-readback"] : []), "admin-tabs", "audit", "rate-cards", "logout"] };
  } finally {
    try {
      if (loggedIn) {
        const meResponse = await context.request.get(`${options.url}/api/v1/auth/me`);
        if (meResponse.status() === 200) {
          const me = await meResponse.json();
          const logout = await context.request.post(`${options.url}/api/v1/auth/logout`, {
            headers: { "X-Loom-CSRF": me.csrf_token },
          });
          cleanLogout = logout.status() === 204 &&
            (await context.request.get(`${options.url}/api/v1/auth/me`)).status() === 401;
        } else cleanLogout = meResponse.status() === 401;
      }
    } finally {
      await context.close();
      await browser.close();
    }
    if (loggedIn && !cleanLogout) throw new Error("Browser session cleanup failed");
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try {
    const result = await runSmoke(parseOptions(process.argv.slice(2)));
    process.stdout.write(`${JSON.stringify(result)}\n`);
  } catch {
    // Never serialize browser/network errors or supplied credentials.
    process.stderr.write("Admin browser smoke failed; check login, page readiness, and session cleanup.\n");
    process.exitCode = 1;
  }
}
