#!/usr/bin/env node

import { Buffer } from "node:buffer";
import { randomUUID } from "node:crypto";
import { lookup } from "node:dns/promises";
import { constants as fsConstants } from "node:fs";
import * as fs from "node:fs/promises";
import { isIP } from "node:net";
import path from "node:path";
import process from "node:process";
import { pathToFileURL } from "node:url";

const ADMIN_ACTOR = "staging-admin-browser-smoke";
const AUDIT_ACTION = "auth.staging_admin_browser_session.create";
const BOOTSTRAP_PATH = "/api/v1/auth/staging-admin-browser-session";
const CANONICAL_STAGING_ROUTE = "https://yylx.world/dev";
const EXPECTED_TTL_SEC = 900;
const NETWORK_QUIET_WINDOW_MS = 500;
const MAX_SECRET_BYTES = 16 * 1024;
const DEFAULT_TIMEOUT_MS = 30_000;
const DEFAULT_VIEWPORT = Object.freeze({ width: 1440, height: 900 });
const ADMIN_TABS = Object.freeze([
  "Requests",
  "Accounts",
  "Teams",
  "Invites",
  "API tokens",
  "Audit",
]);

const SHA_RE = /^[0-9a-f]{40}$/;
const UUID_RE =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const USERNAME_RE = /^[A-Za-z0-9][A-Za-z0-9_.-]{1,63}$/;
const ADMIN_TOKEN_RE = /^loom_admin_[A-Za-z0-9._~+/=-]{32,}$/;
const SAFE_VERSION_RE = /^[0-9A-Za-z][0-9A-Za-z._+~-]{0,63}$/;
const SECRET_TEXT_RE =
  /\b(?:Bearer\s+)?loom_(?:admin|api|invite|team|w|session|csrf|login|setup|reset)_[A-Za-z0-9._~+/=-]+/gi;
const SECRET_ENV_VALUE_RE =
  /\b(?:Bearer\s+)?loom_(?:admin|api|invite|team|w|session|csrf|login|setup|reset)_[A-Za-z0-9._~+/=-]+/i;
const SAFE_BROWSER_ENV_KEYS = new Set([
  "DISPLAY",
  "FONTCONFIG_FILE",
  "FONTCONFIG_PATH",
  "HOME",
  "LANG",
  "LANGUAGE",
  "PATH",
  "PLAYWRIGHT_BROWSERS_PATH",
  "SSL_CERT_DIR",
  "SSL_CERT_FILE",
  "TEMP",
  "TMP",
  "TMPDIR",
  "TZ",
  "XAUTHORITY",
  "XDG_RUNTIME_DIR",
]);
const SENSITIVE_KEYS = new Set([
  "authorization",
  "cookie",
  "set_cookie",
  "csrf",
  "csrf_token",
  "password",
  "secret",
  "token",
]);

export class SafeSmokeError extends Error {
  constructor(code, message) {
    super(message);
    this.name = "SafeSmokeError";
    this.code = code;
  }
}

function requiredValue(argv, index) {
  const value = argv[index + 1];
  if (!value || value.startsWith("--")) {
    throw new SafeSmokeError(
      "invalid_arguments",
      "a required argument value is missing",
    );
  }
  return value;
}

export function canonicalStagingRoute(value) {
  let parsed;
  try {
    parsed = new URL(value);
  } catch {
    throw new SafeSmokeError("invalid_route", "route URL is malformed");
  }
  if (
    parsed.origin !== "https://yylx.world" ||
    parsed.username ||
    parsed.password ||
    parsed.port ||
    parsed.search ||
    parsed.hash ||
    !["/dev", "/dev/"].includes(parsed.pathname)
  ) {
    throw new SafeSmokeError(
      "invalid_route",
      "route must be the canonical https://yylx.world/dev staging route",
    );
  }
  return CANONICAL_STAGING_ROUTE;
}

export function parseArgs(argv) {
  const options = {
    route: "",
    expectedDeployedSha: "",
    adminTokenSource: "",
    username: "",
    reportPath: "",
    timeoutMs: DEFAULT_TIMEOUT_MS,
    insecureForKind: false,
    viewport: { ...DEFAULT_VIEWPORT },
    help: false,
  };
  const valueArguments = new Set([
    "--route",
    "--expected-deployed-sha",
    "--admin-token-source",
    "--username",
    "--report",
    "--timeout-ms",
  ]);
  const seen = new Set();

  for (let index = 0; index < argv.length; index += 1) {
    const argument = argv[index];
    if (argument === "--help") {
      options.help = true;
      continue;
    }
    if (argument === "--insecure-for-kind") {
      if (seen.has(argument)) {
        throw new SafeSmokeError(
          "invalid_arguments",
          "arguments may not be repeated",
        );
      }
      seen.add(argument);
      options.insecureForKind = true;
      continue;
    }
    if (!valueArguments.has(argument)) {
      throw new SafeSmokeError(
        "invalid_arguments",
        "unknown staging admin browser smoke argument",
      );
    }
    if (seen.has(argument)) {
      throw new SafeSmokeError(
        "invalid_arguments",
        "arguments may not be repeated",
      );
    }
    seen.add(argument);
    const value = requiredValue(argv, index);
    index += 1;
    if (argument === "--route") options.route = value;
    if (argument === "--expected-deployed-sha") {
      options.expectedDeployedSha = value;
    }
    if (argument === "--admin-token-source") {
      options.adminTokenSource = value;
    }
    if (argument === "--username") options.username = value;
    if (argument === "--report") options.reportPath = value;
    if (argument === "--timeout-ms") options.timeoutMs = Number(value);
  }

  if (options.help) return options;
  if (
    !options.route ||
    !options.expectedDeployedSha ||
    !options.adminTokenSource ||
    !options.username ||
    !options.reportPath
  ) {
    throw new SafeSmokeError(
      "invalid_arguments",
      "route, deployed SHA, token source, username, and report are required",
    );
  }
  options.route = canonicalStagingRoute(options.route);
  if (!SHA_RE.test(options.expectedDeployedSha)) {
    throw new SafeSmokeError(
      "invalid_deployed_identity",
      "deployed SHA must be 40 lowercase hexadecimal characters",
    );
  }
  const username = options.username.trim().toLowerCase();
  if (!USERNAME_RE.test(username)) {
    throw new SafeSmokeError(
      "invalid_username",
      "username must match the Loom username contract",
    );
  }
  options.username = username;
  if (
    !Number.isInteger(options.timeoutMs) ||
    options.timeoutMs < 1_000 ||
    options.timeoutMs > 120_000
  ) {
    throw new SafeSmokeError(
      "invalid_timeout",
      "timeout must be an integer from 1000 to 120000 milliseconds",
    );
  }
  if (
    !path.isAbsolute(options.reportPath) ||
    options.reportPath.includes("\0") ||
    path.extname(options.reportPath) !== ".json"
  ) {
    throw new SafeSmokeError(
      "invalid_report_path",
      "report must use an absolute .json path",
    );
  }
  if (
    options.adminTokenSource !== "-" &&
    !options.adminTokenSource.startsWith("file:")
  ) {
    throw new SafeSmokeError(
      "invalid_token_source",
      "admin token source must be file:/absolute/path or -",
    );
  }
  if (
    options.insecureForKind &&
    !(
      process.env.CI === "true" &&
      process.env.GITHUB_ACTIONS === "true"
    )
  ) {
    throw new SafeSmokeError(
      "invalid_tls_mode",
      "--insecure-for-kind requires GitHub Actions",
    );
  }
  return options;
}

function validateAdminToken(value) {
  const token = String(value).trim();
  if (
    Buffer.byteLength(token, "utf8") > MAX_SECRET_BYTES ||
    !ADMIN_TOKEN_RE.test(token) ||
    /\s/.test(token)
  ) {
    throw new SafeSmokeError(
      "invalid_admin_token",
      "admin token source does not contain a singleton admin token",
    );
  }
  return token;
}

async function readBoundedStdin(stdin) {
  if (stdin?.isTTY === true) {
    throw new SafeSmokeError(
      "interactive_stdin",
      "admin token stdin must be redirected from a non-interactive source",
    );
  }
  const chunks = [];
  let size = 0;
  for await (const chunk of stdin) {
    const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(String(chunk));
    size += buffer.length;
    if (size > MAX_SECRET_BYTES) {
      throw new SafeSmokeError(
        "invalid_admin_token",
        "admin token stdin source exceeds the safe size limit",
      );
    }
    chunks.push(buffer);
  }
  return Buffer.concat(chunks).toString("utf8");
}

export async function loadAdminToken(
  source,
  { fsModule = fs, stdin = process.stdin } = {},
) {
  if (source === "-") {
    return validateAdminToken(await readBoundedStdin(stdin));
  }
  if (!source.startsWith("file:")) {
    throw new SafeSmokeError(
      "invalid_token_source",
      "admin token source must be file:/absolute/path or -",
    );
  }

  const filePath = source.slice("file:".length);
  if (!path.isAbsolute(filePath) || filePath.includes("\0")) {
    throw new SafeSmokeError(
      "invalid_token_source",
      "admin token file source must use an absolute path",
    );
  }
  let handle;
  try {
    const before = await fsModule.lstat(filePath);
    if (before.isSymbolicLink() || !before.isFile()) {
      throw new SafeSmokeError(
        "unsafe_token_file",
        "admin token source must be a regular non-symlink file",
      );
    }
    handle = await fsModule.open(
      filePath,
      fsConstants.O_RDONLY | (fsConstants.O_NOFOLLOW ?? 0),
    );
    const opened = await handle.stat();
    if (
      !opened.isFile() ||
      opened.dev !== before.dev ||
      opened.ino !== before.ino ||
      (typeof process.getuid === "function" &&
      opened.uid !== process.getuid()) ||
      opened.size < 1 ||
      opened.size > MAX_SECRET_BYTES ||
      (opened.mode & 0o7777) !== 0o600
    ) {
      throw new SafeSmokeError(
        "unsafe_token_file",
        "admin token file must be owner-matched with exact mode 0600",
      );
    }
    const buffer = Buffer.alloc(MAX_SECRET_BYTES + 1);
    const { bytesRead } = await handle.read(
      buffer,
      0,
      MAX_SECRET_BYTES + 1,
      0,
    );
    if (bytesRead > MAX_SECRET_BYTES) {
      throw new SafeSmokeError(
        "unsafe_token_file",
        "admin token source exceeds the safe size limit",
      );
    }
    return validateAdminToken(buffer.subarray(0, bytesRead).toString("utf8"));
  } catch (error) {
    if (error instanceof SafeSmokeError) throw error;
    throw new SafeSmokeError(
      "unsafe_token_file",
      "admin token file could not be read safely",
    );
  } finally {
    await handle?.close().catch(() => {});
  }
}

function loopbackAddress(address) {
  if (isIP(address) === 4) {
    return address.startsWith("127.");
  }
  if (isIP(address) !== 6) return false;
  const normalized = address.toLowerCase();
  return normalized === "::1" || normalized.startsWith("::ffff:127.");
}

export async function assertInsecureKindBoundary(
  options,
  { env = process.env, dnsLookup = lookup } = {},
) {
  if (!options.insecureForKind) return;
  if (!(env.CI === "true" && env.GITHUB_ACTIONS === "true")) {
    throw new SafeSmokeError(
      "invalid_tls_mode",
      "--insecure-for-kind requires GitHub Actions",
    );
  }
  let records;
  try {
    records = await dnsLookup(new URL(options.route).hostname, {
      all: true,
      verbatim: true,
    });
  } catch {
    throw new SafeSmokeError(
      "invalid_tls_target",
      "canonical staging route could not be resolved safely",
    );
  }
  if (
    !Array.isArray(records) ||
    records.length === 0 ||
    records.some((record) => !loopbackAddress(record?.address ?? ""))
  ) {
    throw new SafeSmokeError(
      "invalid_tls_target",
      "--insecure-for-kind requires a loopback-resolved staging route",
    );
  }
}

export function scrubBrowserEnvironment(env, knownSecrets = []) {
  const output = {};
  for (const [key, rawValue] of Object.entries(env ?? {})) {
    if (
      typeof rawValue !== "string" ||
      !(SAFE_BROWSER_ENV_KEYS.has(key) || key.startsWith("LC_"))
    ) {
      continue;
    }
    if (
      knownSecrets.some((secret) => secret && rawValue.includes(secret)) ||
      SECRET_ENV_VALUE_RE.test(rawValue)
    ) {
      continue;
    }
    output[key] = rawValue;
  }
  return output;
}

function sensitiveKey(key) {
  const normalized = String(key).trim().toLowerCase().replaceAll("-", "_");
  return (
    SENSITIVE_KEYS.has(normalized) ||
    normalized.endsWith("_token") ||
    normalized.endsWith("_secret") ||
    normalized.endsWith("_password")
  );
}

export function sanitizeReportValue(value, knownSecrets = []) {
  if (typeof value === "string") {
    let safe = value;
    for (const secret of knownSecrets.filter(Boolean)) {
      safe = safe.split(secret).join("[REDACTED]");
    }
    return safe.replace(SECRET_TEXT_RE, "[REDACTED]");
  }
  if (Array.isArray(value)) {
    return value.map((item) => sanitizeReportValue(item, knownSecrets));
  }
  if (!value || typeof value !== "object") return value;
  const output = {};
  for (const [key, inner] of Object.entries(value)) {
    output[key] = sensitiveKey(key)
      ? "[REDACTED]"
      : sanitizeReportValue(inner, knownSecrets);
  }
  return output;
}

function safeBrowserVersion(value) {
  return SAFE_VERSION_RE.test(String(value)) ? String(value) : "unknown";
}

function initialChecks() {
  return {
    bootstrap_status_204: false,
    bootstrap_empty_body: false,
    bootstrap_no_store: false,
    deployed_build_sha_present: false,
    deployed_build_sha_matches_expected: false,
    secure_http_only_lax_cookie: false,
    authenticated_target_user: false,
    platform_admin_authority: false,
    audit_event_correlated: false,
    admin_access_document_2xx: false,
    authenticated_react_mount: false,
    admin_tabs_accessibility: false,
    admin_requests_apis_200: false,
    admin_requests_ui_visible: false,
    admin_accounts_apis_200: false,
    admin_accounts_ui_visible: false,
    admin_teams_api_200: false,
    admin_teams_ui_visible: false,
    admin_invites_apis_200: false,
    admin_invites_ui_visible: false,
    admin_tokens_api_200: false,
    admin_tokens_ui_visible: false,
    admin_audit_api_200: false,
    all_admin_tabs_operable: false,
    audit_tab_event_visible: false,
    rate_cards_api_200: false,
    rate_cards_ui_visible: false,
    browser_console_clean: false,
    browser_page_errors_clean: false,
    browser_request_failures_clean: false,
    browser_server_errors_clean: false,
  };
}

export function validateBootstrapCookie(cookies, nowSeconds) {
  const matches = cookies.filter((cookie) => cookie.name === "loom_session");
  if (matches.length !== 1) return false;
  const cookie = matches[0];
  const remaining = cookie.expires - nowSeconds;
  return (
    typeof cookie.value === "string" &&
    cookie.value.startsWith("loom_session_staging_admin_") &&
    cookie.httpOnly === true &&
    cookie.secure === true &&
    cookie.sameSite === "Lax" &&
    cookie.path === "/" &&
    remaining > 0 &&
    remaining <= EXPECTED_TTL_SEC + 10
  );
}

export function findCorrelatedAuditEvent(
  payload,
  { requestId, targetUserId, username, buildSha },
) {
  if (!payload || !Array.isArray(payload.items)) return null;
  return (
    payload.items.find(
      (event) =>
        event?.action === AUDIT_ACTION &&
        event.actor === ADMIN_ACTOR &&
        event.request_id === requestId &&
        event.target_type === "user" &&
        event.target_id === targetUserId &&
        event.metadata?.target_username === username &&
        event.metadata?.auth_source === "singleton_admin_bearer" &&
        event.metadata?.build_sha === buildSha &&
        ["active", "pending_setup"].includes(event.metadata?.target_status) &&
        event.metadata?.ttl_seconds === EXPECTED_TTL_SEC &&
        UUID_RE.test(event.id ?? ""),
    ) ?? null
  );
}

function responsePathMatches(response, targetUrl) {
  return response.request().method() === "GET" && response.url() === targetUrl;
}

function createResponseLedger(page) {
  const responses = new Map();
  page.on("response", (response) => {
    if (response.request().method() !== "GET") return;
    const existing = responses.get(response.url()) ?? [];
    existing.push(response);
    responses.set(response.url(), existing);
  });
  return {
    waitForGet(targetUrl, timeoutMs) {
      const existing = responses.get(targetUrl);
      if (existing?.length) return Promise.resolve(existing.at(-1));
      return page.waitForResponse(
        (response) => responsePathMatches(response, targetUrl),
        { timeout: timeoutMs },
      );
    },
  };
}

export function createAuthenticatedPageMonitor(
  page,
  route,
  { nowFn = () => Date.now() } = {},
) {
  const expectedOrigin = new URL(route).origin;
  const consoleErrors = [];
  const crossOriginNonScriptFailures = [];
  let pageErrorCount = 0;
  let requestFailureCount = 0;
  let serverErrorCount = 0;
  let activeRequestCount = 0;
  let lastActivityAt = nowFn();

  function recordActivity() {
    lastActivityAt = nowFn();
  }

  function completeRequest() {
    activeRequestCount = Math.max(0, activeRequestCount - 1);
    recordActivity();
  }

  function parsedNetworkUrl(value) {
    try {
      const parsed = new URL(value);
      return ["http:", "https:"].includes(parsed.protocol) ? parsed : null;
    } catch {
      return null;
    }
  }

  page.on("console", (message) => {
    recordActivity();
    if (message.type() !== "error") return;
    const location = message.location();
    consoleErrors.push({
      text: message.text(),
      url: location.url ?? "",
      line: location.lineNumber ?? location.line ?? -1,
      column: location.columnNumber ?? location.column ?? -1,
    });
  });
  page.on("pageerror", () => {
    recordActivity();
    pageErrorCount += 1;
  });
  page.on("request", () => {
    activeRequestCount += 1;
    recordActivity();
  });
  page.on("requestfinished", completeRequest);
  page.on("requestfailed", (request) => {
    completeRequest();
    const parsed = parsedNetworkUrl(request.url());
    if (!parsed) return;
    const sameOrigin = parsed.origin === expectedOrigin;
    if (sameOrigin || request.resourceType() === "script") {
      requestFailureCount += 1;
      return;
    }
    crossOriginNonScriptFailures.push(request.url());
  });
  page.on("response", (response) => {
    recordActivity();
    if (response.status() < 500) return;
    const parsed = parsedNetworkUrl(response.url());
    if (!parsed) return;
    const sameOrigin = parsed.origin === expectedOrigin;
    if (sameOrigin || response.request().resourceType() === "script") {
      serverErrorCount += 1;
      return;
    }
    crossOriginNonScriptFailures.push(response.url());
  });

  return {
    async waitForQuiet(timeoutMs) {
      const deadline = nowFn() + timeoutMs;
      for (;;) {
        const now = nowFn();
        const quietFor = now - lastActivityAt;
        if (
          activeRequestCount === 0 &&
          quietFor >= NETWORK_QUIET_WINDOW_MS
        ) {
          return;
        }
        const remaining = deadline - now;
        if (remaining <= 0) {
          throw new SafeSmokeError(
            "browser_not_quiet",
            "authenticated browser runtime did not reach network quiet",
          );
        }
        const untilQuiet = Math.max(
          1,
          NETWORK_QUIET_WINDOW_MS - quietFor,
        );
        await page.waitForTimeout(Math.min(100, untilQuiet, remaining));
      }
    },
    applyChecks(checks) {
      const blockingConsoleErrors = consoleErrors.filter((error) => {
        const browserGenerated =
          error.text.startsWith("Failed to load resource:") &&
          error.line === 0 &&
          error.column === 0 &&
          Boolean(error.url);
        return !(
          browserGenerated &&
          crossOriginNonScriptFailures.includes(error.url)
        );
      });
      checks.browser_console_clean = blockingConsoleErrors.length === 0;
      checks.browser_page_errors_clean = pageErrorCount === 0;
      checks.browser_request_failures_clean = requestFailureCount === 0;
      checks.browser_server_errors_clean = serverErrorCount === 0;
      if (
        !checks.browser_console_clean ||
        !checks.browser_page_errors_clean ||
        !checks.browser_request_failures_clean ||
        !checks.browser_server_errors_clean
      ) {
        throw new SafeSmokeError(
          "browser_runtime_error",
          "authenticated browser runtime reported a blocking error",
        );
      }
    },
  };
}

function responseStatusIs200(response) {
  return response?.status() === 200;
}

async function selectAdminTab(page, name, timeoutMs) {
  const tab = page.getByRole("tab", { name, exact: true });
  await tab.waitFor({ state: "visible", timeout: timeoutMs });
  await tab.click();
  if ((await tab.getAttribute("aria-selected")) !== "true") {
    throw new SafeSmokeError(
      "admin_tab_failed",
      "an Admin Access tab did not become selected",
    );
  }
}

async function waitForAdminTabState(page, name, timeoutMs) {
  try {
    await page.waitForFunction(
      ({ expectedName }) => {
        const tabList = document.querySelector(
          '[role="tablist"][aria-label="Team access sections"]',
        );
        const tab = Array.from(
          tabList?.querySelectorAll('[role="tab"]') ?? [],
        ).find((candidate) => candidate.textContent?.trim() === expectedName);
        if (!(tab instanceof HTMLElement)) return false;
        const panelId = tab.getAttribute("aria-controls");
        const panel =
          panelId === null ? null : document.getElementById(panelId);
        return (
          document.activeElement === tab &&
          tab.getAttribute("aria-selected") === "true" &&
          tab.tabIndex === 0 &&
          panel?.getAttribute("role") === "tabpanel" &&
          panel.getAttribute("aria-labelledby") === tab.id &&
          panel.hidden === false
        );
      },
      { expectedName: name },
      { timeout: timeoutMs },
    );
  } catch {
    throw new SafeSmokeError(
      "admin_tabs_accessibility_failed",
      "Admin Access tabs did not preserve focus, selection, and panel semantics",
    );
  }
}

export async function verifyAdminTabsAccessibility(page, timeoutMs) {
  const tabList = page.getByRole("tablist", {
    name: "Team access sections",
    exact: true,
  });
  await tabList.waitFor({ state: "visible", timeout: timeoutMs });
  const snapshot = await tabList.evaluate((element) => {
    const tabs = Array.from(element.querySelectorAll('[role="tab"]'));
    const relationshipsValid = tabs.every((tab) => {
      if (!(tab instanceof HTMLElement) || tab.id.length === 0) return false;
      const panelId = tab.getAttribute("aria-controls");
      const panel = panelId === null ? null : document.getElementById(panelId);
      return (
        panel?.getAttribute("role") === "tabpanel" &&
        panel.getAttribute("aria-labelledby") === tab.id
      );
    });
    return {
      names: tabs.map((tab) => tab.textContent?.trim() ?? ""),
      orientation: element.getAttribute("aria-orientation"),
      relationshipsValid,
      selectedCount: tabs.filter(
        (tab) => tab.getAttribute("aria-selected") === "true",
      ).length,
      rovingTabStopCount: tabs.filter(
        (tab) => tab instanceof HTMLElement && tab.tabIndex === 0,
      ).length,
    };
  });
  if (
    snapshot.orientation !== "horizontal" ||
    snapshot.relationshipsValid !== true ||
    snapshot.selectedCount !== 1 ||
    snapshot.rovingTabStopCount !== 1 ||
    snapshot.names.length !== ADMIN_TABS.length ||
    snapshot.names.some((name, index) => name !== ADMIN_TABS[index])
  ) {
    throw new SafeSmokeError(
      "admin_tabs_accessibility_failed",
      "Admin Access tabs did not expose the expected ARIA tab-panel structure",
    );
  }

  const requests = page.getByRole("tab", { name: "Requests", exact: true });
  await requests.focus();
  await waitForAdminTabState(page, "Requests", timeoutMs);
  for (const [key, expectedName] of [
    ["End", "Audit"],
    ["Home", "Requests"],
    ["ArrowRight", "Accounts"],
    ["ArrowLeft", "Requests"],
  ]) {
    await page.keyboard.press(key);
    await waitForAdminTabState(page, expectedName, timeoutMs);
  }
  return true;
}

async function waitForCardHeading(page, name, timeoutMs) {
  await page
    .getByRole("heading", { name, exact: true, level: 3 })
    .waitFor({ state: "visible", timeout: timeoutMs });
}

export async function waitForSuccessfulQueryCard(page, queryName, timeoutMs) {
  await page
    .locator(
      `[data-loom-query="${queryName}"]` +
        '[data-loom-query-status="success"]',
    )
    .waitFor({ state: "visible", timeout: timeoutMs });
}

async function waitForAuthenticatedMount(page, timeoutMs) {
  await page.waitForSelector(
    '#root[data-loom-mounted="true"]' +
      '[data-loom-auth-settled="true"]' +
      '[data-loom-auth-state="authenticated"]',
    { state: "attached", timeout: timeoutMs },
  );
}

async function checkAdminAccess(page, options, checks, auditIdentity, ledger) {
  const pageUrl = `${options.route}/admin/access`;
  const urls = {
    audit: `${options.route}/api/v1/admin/audit-events?limit=50`,
    teams: `${options.route}/api/v1/admin/teams`,
    registrations:
      `${options.route}/api/v1/admin/registration-requests?status=pending`,
    teamRegistrations:
      `${options.route}/api/v1/admin/team-registrations?status=pending`,
    invites: `${options.route}/api/v1/invites?status=pending`,
    tokens: `${options.route}/api/v1/tokens`,
    passwordResets:
      `${options.route}/api/v1/admin/password-reset-requests?status=pending`,
  };
  const navigation = await page.goto(pageUrl, {
    waitUntil: "domcontentloaded",
    timeout: options.timeoutMs,
  });
  const [
    teamsResponse,
    registrationsResponse,
    teamRegistrationsResponse,
    invitesResponse,
    tokensResponse,
  ] = await Promise.all([
    ledger.waitForGet(urls.teams, options.timeoutMs),
    ledger.waitForGet(urls.registrations, options.timeoutMs),
    ledger.waitForGet(urls.teamRegistrations, options.timeoutMs),
    ledger.waitForGet(urls.invites, options.timeoutMs),
    ledger.waitForGet(urls.tokens, options.timeoutMs),
  ]);
  checks.admin_access_document_2xx =
    navigation !== null && navigation.status() >= 200 && navigation.status() < 300;
  await waitForAuthenticatedMount(page, options.timeoutMs);
  checks.authenticated_react_mount = page.url() === pageUrl;
  await page
    .getByRole("heading", { name: "Team access", exact: true, level: 1 })
    .waitFor({ state: "visible", timeout: options.timeoutMs });
  checks.admin_tabs_accessibility = await verifyAdminTabsAccessibility(
    page,
    options.timeoutMs,
  );

  await selectAdminTab(page, "Requests", options.timeoutMs);
  await waitForCardHeading(page, "Account requests", options.timeoutMs);
  await waitForCardHeading(page, "Legacy team registrations", options.timeoutMs);
  await waitForSuccessfulQueryCard(
    page,
    "registration-requests",
    options.timeoutMs,
  );
  await waitForSuccessfulQueryCard(
    page,
    "team-registrations",
    options.timeoutMs,
  );
  checks.admin_requests_apis_200 =
    responseStatusIs200(registrationsResponse) &&
    responseStatusIs200(teamRegistrationsResponse) &&
    responseStatusIs200(teamsResponse);
  checks.admin_requests_ui_visible = true;

  await selectAdminTab(page, "Accounts", options.timeoutMs);
  const passwordResetsResponse = await ledger.waitForGet(
    urls.passwordResets,
    options.timeoutMs,
  );
  await waitForCardHeading(page, "Account requests", options.timeoutMs);
  await waitForCardHeading(page, "Password resets", options.timeoutMs);
  await waitForSuccessfulQueryCard(
    page,
    "registration-requests",
    options.timeoutMs,
  );
  await waitForSuccessfulQueryCard(
    page,
    "password-reset-requests",
    options.timeoutMs,
  );
  checks.admin_accounts_apis_200 =
    responseStatusIs200(registrationsResponse) &&
    responseStatusIs200(passwordResetsResponse);
  checks.admin_accounts_ui_visible = true;

  await selectAdminTab(page, "Teams", options.timeoutMs);
  await waitForCardHeading(page, "Internal teams", options.timeoutMs);
  await waitForSuccessfulQueryCard(page, "admin-teams", options.timeoutMs);
  checks.admin_teams_api_200 = responseStatusIs200(teamsResponse);
  checks.admin_teams_ui_visible = true;

  await selectAdminTab(page, "Invites", options.timeoutMs);
  await waitForCardHeading(page, "Create invite", options.timeoutMs);
  await waitForCardHeading(page, "Pending invites", options.timeoutMs);
  await waitForSuccessfulQueryCard(page, "invites", options.timeoutMs);
  checks.admin_invites_apis_200 =
    responseStatusIs200(invitesResponse) && responseStatusIs200(teamsResponse);
  checks.admin_invites_ui_visible = true;

  await selectAdminTab(page, "API tokens", options.timeoutMs);
  await waitForCardHeading(page, "API tokens", options.timeoutMs);
  await waitForSuccessfulQueryCard(page, "api-tokens", options.timeoutMs);
  checks.admin_tokens_api_200 = responseStatusIs200(tokensResponse);
  checks.admin_tokens_ui_visible = true;

  await selectAdminTab(page, "Audit", options.timeoutMs);
  const auditResponse = await ledger.waitForGet(urls.audit, options.timeoutMs);
  await waitForCardHeading(page, "Audit log", options.timeoutMs);
  await waitForSuccessfulQueryCard(page, "audit-events", options.timeoutMs);
  checks.admin_audit_api_200 = responseStatusIs200(auditResponse);
  let auditPayload = null;
  try {
    auditPayload = await auditResponse.json();
  } catch {
    auditPayload = null;
  }
  const pageAuditEvent = findCorrelatedAuditEvent(auditPayload, auditIdentity);
  const exactAuditEvent =
    pageAuditEvent !== null && pageAuditEvent.id === auditIdentity.auditEventId;
  if (exactAuditEvent) {
    const exactRow = page
      .getByRole("row")
      .filter({
        has: page.getByRole("cell", { name: ADMIN_ACTOR, exact: true }),
      })
      .filter({
        has: page.getByRole("cell", { name: AUDIT_ACTION, exact: true }),
      })
      .filter({
        has: page.getByRole("cell", {
          name: `user:${auditIdentity.targetUserId}`,
          exact: true,
        }),
      })
      .filter({
        has: page.getByRole("cell", {
          name: auditIdentity.requestId,
          exact: true,
        }),
      });
    await exactRow.first().waitFor({
      state: "visible",
      timeout: options.timeoutMs,
    });
  }
  checks.audit_tab_event_visible = exactAuditEvent;
  checks.all_admin_tabs_operable =
    ADMIN_TABS.length === 6 &&
    checks.admin_tabs_accessibility &&
    checks.admin_requests_apis_200 &&
    checks.admin_requests_ui_visible &&
    checks.admin_accounts_apis_200 &&
    checks.admin_accounts_ui_visible &&
    checks.admin_teams_api_200 &&
    checks.admin_teams_ui_visible &&
    checks.admin_invites_apis_200 &&
    checks.admin_invites_ui_visible &&
    checks.admin_tokens_api_200 &&
    checks.admin_tokens_ui_visible &&
    checks.admin_audit_api_200 &&
    checks.audit_tab_event_visible;
}

async function checkRateCards(page, options, checks, ledger) {
  const pageUrl = `${options.route}/rate-cards`;
  const apiUrl = `${options.route}/api/v1/rate-cards`;
  const navigation = await page.goto(pageUrl, {
    waitUntil: "domcontentloaded",
    timeout: options.timeoutMs,
  });
  const apiResponse = await ledger.waitForGet(apiUrl, options.timeoutMs);
  await waitForAuthenticatedMount(page, options.timeoutMs);
  checks.rate_cards_api_200 = apiResponse.status() === 200;
  await page
    .getByRole("heading", { name: "Rate cards", exact: true, level: 1 })
    .waitFor({ state: "visible", timeout: options.timeoutMs });
  await page
    .getByRole("heading", { name: "Published", exact: true })
    .waitFor({ state: "visible", timeout: options.timeoutMs });
  await page
    .getByRole("heading", { name: "Publish a new rate card", exact: true })
    .waitFor({ state: "visible", timeout: options.timeoutMs });
  await waitForSuccessfulQueryCard(page, "rate-cards", options.timeoutMs);
  checks.rate_cards_ui_visible =
    navigation !== null && navigation.status() >= 200 && navigation.status() < 300;
}

async function jsonResponse(response) {
  try {
    return await response.json();
  } catch {
    return null;
  } finally {
    await response.dispose().catch(() => {});
  }
}

function reportStatus(checks, cleanup, failureCode) {
  return Object.values(checks).every(Boolean) &&
    failureCode === null &&
    cleanup.logout_status === 204 &&
    cleanup.auth_me_after_logout_status === 401
    ? "pass"
    : "fail";
}

export async function executeSmoke(
  options,
  {
    env = process.env,
    fsModule = fs,
    stdin = process.stdin,
    dnsLookup = lookup,
    playwrightModule,
    randomUUIDFn = randomUUID,
    nowFn = () => Date.now(),
  } = {},
) {
  await assertInsecureKindBoundary(options, { env, dnsLookup });
  const token = await loadAdminToken(options.adminTokenSource, {
    fsModule,
    stdin,
  });
  const playwright = playwrightModule ?? (await import("@playwright/test"));
  const requestId = `staging-admin-browser-${randomUUIDFn()}`;
  const checks = initialChecks();
  const cleanup = {
    logout_status: null,
    auth_me_after_logout_status: null,
  };
  let browser;
  let context;
  let page;
  let failureCode = null;
  let browserVersion = "unknown";
  let targetUserId = null;
  let auditEventId = null;
  let observedDeployedSha = null;
  let bootstrapCreated = false;

  try {
    browser = await playwright.chromium.launch({
      headless: true,
      env: scrubBrowserEnvironment(env, [token]),
    });
    browserVersion = safeBrowserVersion(browser.version());
    context = await browser.newContext({
      viewport: { ...options.viewport },
      ignoreHTTPSErrors: options.insecureForKind,
      recordVideo: undefined,
    });

    const bootstrap = await context.request.post(
      `${options.route}${BOOTSTRAP_PATH}`,
      {
        headers: {
          Authorization: `Bearer ${token}`,
          "X-Loom-Admin-Actor": ADMIN_ACTOR,
          "X-Request-ID": requestId,
        },
        data: { username: options.username },
        failOnStatusCode: false,
        timeout: options.timeoutMs,
      },
    );
    try {
      checks.bootstrap_status_204 = bootstrap.status() === 204;
      checks.bootstrap_empty_body = (await bootstrap.body()).length === 0;
      const headers = bootstrap.headers();
      checks.bootstrap_no_store =
        headers["cache-control"] === "no-store" &&
        headers.pragma === "no-cache";
      observedDeployedSha = SHA_RE.test(headers["x-loom-build-sha"] ?? "")
        ? headers["x-loom-build-sha"]
        : null;
      checks.deployed_build_sha_present = observedDeployedSha !== null;
      checks.deployed_build_sha_matches_expected =
        observedDeployedSha === options.expectedDeployedSha;
    } finally {
      await bootstrap.dispose().catch(() => {});
    }
    bootstrapCreated = checks.bootstrap_status_204;
    if (!bootstrapCreated) {
      throw new SafeSmokeError(
        "bootstrap_rejected",
        "staging admin browser bootstrap was rejected",
      );
    }
    if (!checks.deployed_build_sha_matches_expected) {
      throw new SafeSmokeError(
        "deployed_identity_mismatch",
        "staging runtime does not match the expected deployed SHA",
      );
    }

    const cookies = await context.cookies(options.route);
    checks.secure_http_only_lax_cookie = validateBootstrapCookie(
      cookies,
      nowFn() / 1000,
    );

    const meResponse = await context.request.get(
      `${options.route}/api/v1/auth/me`,
      { failOnStatusCode: false, timeout: options.timeoutMs },
    );
    const meStatus = meResponse.status();
    const me = meStatus === 200 ? await jsonResponse(meResponse) : null;
    if (meStatus !== 200) await meResponse.dispose().catch(() => {});
    const userId = me?.user?.id;
    targetUserId = UUID_RE.test(userId ?? "") ? userId : null;
    checks.authenticated_target_user =
      meStatus === 200 &&
      typeof me?.user?.username === "string" &&
      me.user.username.toLowerCase() === options.username &&
      targetUserId !== null;
    checks.platform_admin_authority =
      me?.is_platform_admin === true &&
      me?.user?.is_platform_admin === true &&
      me?.role === "platform_admin";

    const auditResponse = await context.request.get(
      `${options.route}/api/v1/admin/audit-events?limit=50`,
      { failOnStatusCode: false, timeout: options.timeoutMs },
    );
    const auditStatus = auditResponse.status();
    const auditPayload =
      auditStatus === 200 ? await jsonResponse(auditResponse) : null;
    if (auditStatus !== 200) await auditResponse.dispose().catch(() => {});
    const auditEvent = findCorrelatedAuditEvent(auditPayload, {
      requestId,
      targetUserId,
      username: options.username,
      buildSha: observedDeployedSha,
    });
    if (auditEvent) auditEventId = auditEvent.id;
    checks.audit_event_correlated = auditEvent !== null;

    page = await context.newPage();
    const ledger = createResponseLedger(page);
    const pageMonitor = createAuthenticatedPageMonitor(page, options.route);
    await checkAdminAccess(
      page,
      options,
      checks,
      {
        requestId,
        targetUserId,
        username: options.username,
        buildSha: observedDeployedSha,
        auditEventId,
      },
      ledger,
    );
    await checkRateCards(page, options, checks, ledger);
    await pageMonitor.waitForQuiet(options.timeoutMs);
    await page.close();
    page = null;
    pageMonitor.applyChecks(checks);
  } catch (error) {
    failureCode =
      error instanceof SafeSmokeError ? error.code : "browser_check_failed";
  } finally {
    await page?.close().catch(() => {});
    if (context) {
      try {
        const meResponse = await context.request.get(
          `${options.route}/api/v1/auth/me`,
          { failOnStatusCode: false, timeout: options.timeoutMs },
        );
        const meStatus = meResponse.status();
        const me = meStatus === 200 ? await jsonResponse(meResponse) : null;
        if (meStatus !== 200) await meResponse.dispose().catch(() => {});
        if (bootstrapCreated && typeof me?.csrf_token === "string") {
          const logout = await context.request.post(
            `${options.route}/api/v1/auth/logout`,
            {
              headers: { "X-Loom-CSRF": me.csrf_token },
              failOnStatusCode: false,
              timeout: options.timeoutMs,
            },
          );
          cleanup.logout_status = logout.status();
          await logout.dispose().catch(() => {});
        }
        const afterLogout = await context.request.get(
          `${options.route}/api/v1/auth/me`,
          { failOnStatusCode: false, timeout: options.timeoutMs },
        );
        cleanup.auth_me_after_logout_status = afterLogout.status();
        await afterLogout.dispose().catch(() => {});
      } catch {
        if (failureCode === null) failureCode = "cleanup_failed";
      }
      await context.clearCookies().catch(() => {});
      await context.close().catch(() => {});
    }
    await browser?.close().catch(() => {});
  }

  if (
    cleanup.logout_status !== 204 ||
    cleanup.auth_me_after_logout_status !== 401
  ) {
    failureCode ??= "cleanup_failed";
  }
  const report = {
    schema_version: 2,
    status: reportStatus(checks, cleanup, failureCode),
    deployment_identity: {
      expected_deployed_sha: options.expectedDeployedSha,
      observed_deployed_sha: observedDeployedSha,
      matched:
        observedDeployedSha !== null &&
        observedDeployedSha === options.expectedDeployedSha,
    },
    route: options.route,
    request_id: requestId,
    target: {
      username: options.username,
      user_id: targetUserId,
    },
    audit_event_id: auditEventId,
    browser: {
      name: "chromium",
      version: browserVersion,
      viewport: { ...options.viewport },
    },
    checks,
    cleanup,
    failure_code: failureCode,
  };
  return sanitizeReportValue(report, [token]);
}

async function writeReport(reportPath, report) {
  await fs.writeFile(reportPath, `${JSON.stringify(report, null, 2)}\n`, {
    encoding: "utf8",
    mode: 0o600,
    flag: "wx",
  });
}

const USAGE =
  "Usage: node web/scripts/staging-admin-browser-smoke.mjs " +
  "--route https://yylx.world/dev --expected-deployed-sha <40-hex-sha> " +
  "--admin-token-source <file:/absolute/path|-> " +
  "--username <platform-admin> --report <sanitized.json> " +
  "[--timeout-ms <milliseconds>] [--insecure-for-kind]";

async function main() {
  let options;
  try {
    options = parseArgs(process.argv.slice(2));
    if (options.help) {
      process.stdout.write(`${USAGE}\n`);
      return;
    }
    const report = await executeSmoke(options);
    await writeReport(options.reportPath, report);
    process.stdout.write(
      `${JSON.stringify({ status: report.status, report: options.reportPath })}\n`,
    );
    process.exitCode = report.status === "pass" ? 0 : 1;
  } catch (error) {
    const code = error instanceof SafeSmokeError ? error.code : "execution_failed";
    if (options?.reportPath) {
      const failure = sanitizeReportValue({
        schema_version: 2,
        status: "fail",
        deployment_identity: {
          expected_deployed_sha: options.expectedDeployedSha,
          observed_deployed_sha: null,
          matched: false,
        },
        route: options.route,
        target: { username: options.username },
        failure_code: code,
      });
      await writeReport(options.reportPath, failure).catch(() => {});
    }
    process.stderr.write(`${code}: staging admin browser smoke failed safely\n`);
    process.exitCode = 2;
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  await main();
}
