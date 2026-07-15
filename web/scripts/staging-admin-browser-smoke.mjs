#!/usr/bin/env node

import { Buffer } from "node:buffer";
import { randomUUID } from "node:crypto";
import { constants as fsConstants } from "node:fs";
import * as fs from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import { pathToFileURL } from "node:url";

const ADMIN_ACTOR = "staging-admin-browser-smoke";
const AUDIT_ACTION = "auth.staging_admin_browser_session.create";
const BOOTSTRAP_PATH = "/api/v1/auth/staging-admin-browser-session";
const EXPECTED_TTL_SEC = 900;
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
const ENV_NAME_RE = /^[A-Za-z_][A-Za-z0-9_]*$/;
const ADMIN_TOKEN_RE = /^loom_admin_[A-Za-z0-9._~+/=-]{32,}$/;
const SAFE_VERSION_RE = /^[0-9A-Za-z][0-9A-Za-z._+~-]{0,63}$/;
const SECRET_TEXT_RE =
  /\b(?:Bearer\s+)?loom_(?:admin|api|invite|team|w|session|csrf|login|setup|reset)_[A-Za-z0-9._~+/=-]+/gi;
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
    parsed.protocol !== "https:" ||
    !parsed.hostname ||
    parsed.username ||
    parsed.password ||
    parsed.port ||
    parsed.search ||
    parsed.hash ||
    parsed.pathname.replace(/\/+$/, "") !== "/dev"
  ) {
    throw new SafeSmokeError(
      "invalid_route",
      "route must be a credential-free HTTPS origin ending in /dev",
    );
  }
  parsed.pathname = "/dev";
  return parsed.href.replace(/\/$/, "");
}

export function parseArgs(argv) {
  const options = {
    route: "",
    candidateSha: "",
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
    "--candidate-sha",
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
    if (argument === "--candidate-sha") options.candidateSha = value;
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
    !options.candidateSha ||
    !options.adminTokenSource ||
    !options.username ||
    !options.reportPath
  ) {
    throw new SafeSmokeError(
      "invalid_arguments",
      "route, candidate SHA, token source, username, and report are required",
    );
  }
  options.route = canonicalStagingRoute(options.route);
  if (!SHA_RE.test(options.candidateSha)) {
    throw new SafeSmokeError(
      "invalid_candidate",
      "candidate SHA must be 40 lowercase hexadecimal characters",
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
    !options.adminTokenSource.startsWith("env:") &&
    !options.adminTokenSource.startsWith("file:")
  ) {
    throw new SafeSmokeError(
      "invalid_token_source",
      "admin token source must be env:VAR, file:/absolute/path, or -",
    );
  }
  if (options.insecureForKind && process.env.CI !== "true") {
    throw new SafeSmokeError(
      "invalid_tls_mode",
      "--insecure-for-kind requires CI=true",
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
  { env = process.env, fsModule = fs, stdin = process.stdin } = {},
) {
  if (source === "-") {
    return validateAdminToken(await readBoundedStdin(stdin));
  }
  if (source.startsWith("env:")) {
    const name = source.slice("env:".length);
    if (!ENV_NAME_RE.test(name)) {
      throw new SafeSmokeError(
        "invalid_token_source",
        "admin token environment source is malformed",
      );
    }
    const value = env[name];
    if (typeof value !== "string" || value.length === 0) {
      throw new SafeSmokeError(
        "missing_admin_token",
        "admin token environment source is unavailable",
      );
    }
    return validateAdminToken(value);
  }
  if (!source.startsWith("file:")) {
    throw new SafeSmokeError(
      "invalid_token_source",
      "admin token source must be env:VAR, file:/absolute/path, or -",
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
      opened.size < 1 ||
      opened.size > MAX_SECRET_BYTES ||
      (opened.mode & 0o027) !== 0
    ) {
      throw new SafeSmokeError(
        "unsafe_token_file",
        "admin token file ownership or mode contract is unsafe",
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
    secure_http_only_lax_cookie: false,
    authenticated_target_user: false,
    platform_admin_authority: false,
    audit_event_correlated: false,
    admin_access_document_2xx: false,
    authenticated_react_mount: false,
    all_admin_tabs_operable: false,
    audit_tab_event_visible: false,
    rate_cards_api_200: false,
    rate_cards_ui_visible: false,
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
  { requestId, targetUserId, username },
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
        ["active", "pending_setup"].includes(event.metadata?.target_status) &&
        event.metadata?.ttl_seconds === EXPECTED_TTL_SEC &&
        UUID_RE.test(event.id ?? ""),
    ) ?? null
  );
}

function responsePathMatches(response, targetUrl) {
  return response.request().method() === "GET" && response.url() === targetUrl;
}

async function waitForAuthenticatedMount(page, timeoutMs) {
  await page.waitForSelector(
    '#root[data-loom-mounted="true"]' +
      '[data-loom-auth-settled="true"]' +
      '[data-loom-auth-state="authenticated"]',
    { state: "attached", timeout: timeoutMs },
  );
}

async function checkAdminAccess(page, options, checks) {
  const pageUrl = `${options.route}/admin/access`;
  const auditApiUrl = `${options.route}/api/v1/admin/audit-events?limit=50`;
  const auditResponsePromise = page.waitForResponse(
    (response) => responsePathMatches(response, auditApiUrl),
    { timeout: options.timeoutMs },
  );
  const navigation = await page.goto(pageUrl, {
    waitUntil: "domcontentloaded",
    timeout: options.timeoutMs,
  });
  const auditResponse = await auditResponsePromise;
  checks.admin_access_document_2xx =
    navigation !== null && navigation.status() >= 200 && navigation.status() < 300;
  await waitForAuthenticatedMount(page, options.timeoutMs);
  checks.authenticated_react_mount = page.url() === pageUrl;
  await page
    .getByRole("heading", { name: "Team access", exact: true, level: 1 })
    .waitFor({ state: "visible", timeout: options.timeoutMs });

  for (const name of ADMIN_TABS) {
    const tab = page.getByRole("tab", { name, exact: true });
    await tab.waitFor({ state: "visible", timeout: options.timeoutMs });
    await tab.click();
    if ((await tab.getAttribute("aria-selected")) !== "true") {
      throw new SafeSmokeError(
        "admin_tab_failed",
        "an Admin Access tab did not become selected",
      );
    }
  }
  checks.all_admin_tabs_operable = true;
  await page
    .getByRole("heading", { name: "Audit log", exact: true })
    .waitFor({ state: "visible", timeout: options.timeoutMs });
  await page
    .getByText(AUDIT_ACTION, { exact: true })
    .waitFor({ state: "visible", timeout: options.timeoutMs });
  checks.audit_tab_event_visible = auditResponse.status() === 200;
}

async function checkRateCards(page, options, checks) {
  const pageUrl = `${options.route}/rate-cards`;
  const apiUrl = `${options.route}/api/v1/rate-cards`;
  const apiResponsePromise = page.waitForResponse(
    (response) => responsePathMatches(response, apiUrl),
    { timeout: options.timeoutMs },
  );
  const navigation = await page.goto(pageUrl, {
    waitUntil: "domcontentloaded",
    timeout: options.timeoutMs,
  });
  const apiResponse = await apiResponsePromise;
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

function reportStatus(checks, cleanup) {
  return Object.values(checks).every(Boolean) &&
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
    playwrightModule,
    randomUUIDFn = randomUUID,
    nowFn = () => Date.now(),
  } = {},
) {
  const token = await loadAdminToken(options.adminTokenSource, {
    env,
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
  let bootstrapCreated = false;

  try {
    browser = await playwright.chromium.launch({ headless: true });
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
    });
    if (auditEvent) auditEventId = auditEvent.id;
    checks.audit_event_correlated = auditEvent !== null;

    page = await context.newPage();
    await checkAdminAccess(page, options, checks);
    await checkRateCards(page, options, checks);
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
    schema_version: 1,
    status: reportStatus(checks, cleanup),
    candidate_sha: options.candidateSha,
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
  "--route https://host/dev --candidate-sha <40-hex-sha> " +
  "--admin-token-source <env:VAR|file:/absolute/path|-> " +
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
        schema_version: 1,
        status: "fail",
        candidate_sha: options.candidateSha,
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
