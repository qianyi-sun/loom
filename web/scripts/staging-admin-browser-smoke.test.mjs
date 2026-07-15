import { Buffer } from "node:buffer";
import * as fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import process from "node:process";

import { afterEach, describe, expect, it, vi } from "vitest";

import {
  SafeSmokeError,
  assertInsecureKindBoundary,
  canonicalStagingRoute,
  createAuthenticatedPageMonitor,
  executeSmoke,
  findCorrelatedAuditEvent,
  loadAdminToken,
  parseArgs,
  sanitizeReportValue,
  scrubBrowserEnvironment,
  validateBootstrapCookie,
  waitForSuccessfulQueryCard,
} from "./staging-admin-browser-smoke.mjs";

const RAW_ADMIN_TOKEN = `loom_admin_${"A".repeat(43)}`;
const DEPLOYED_SHA = "a".repeat(40);
const originalCi = process.env.CI;
const originalGithubActions = process.env.GITHUB_ACTIONS;

function validArgs(tokenSource = "file:/run/secrets/admin-token") {
  return [
    "--route",
    "https://yylx.world/dev",
    "--expected-deployed-sha",
    DEPLOYED_SHA,
    "--admin-token-source",
    tokenSource,
    "--username",
    "Qianyi",
    "--report",
    "/tmp/staging-admin-browser-smoke.json",
  ];
}

afterEach(() => {
  if (originalCi === undefined) delete process.env.CI;
  else process.env.CI = originalCi;
  if (originalGithubActions === undefined) delete process.env.GITHUB_ACTIONS;
  else process.env.GITHUB_ACTIONS = originalGithubActions;
});

describe("staging admin browser smoke arguments", () => {
  it("accepts the exact staging route and secret-source contract", () => {
    process.env.CI = "true";
    process.env.GITHUB_ACTIONS = "true";
    const options = parseArgs([...validArgs(), "--insecure-for-kind"]);

    expect(options).toMatchObject({
      route: "https://yylx.world/dev",
      expectedDeployedSha: DEPLOYED_SHA,
      adminTokenSource: "file:/run/secrets/admin-token",
      username: "qianyi",
      reportPath: "/tmp/staging-admin-browser-smoke.json",
      insecureForKind: true,
    });
  });

  it.each(["file:/run/secrets/admin-token", "-"])(
    "accepts only supported source shape %s",
    (source) => {
      expect(parseArgs(validArgs(source)).adminTokenSource).toBe(source);
    },
  );

  it("rejects literal credentials and artifact-recording arguments", () => {
    expect(() => parseArgs(validArgs(RAW_ADMIN_TOKEN))).toThrowError(
      SafeSmokeError,
    );
    expect(() => parseArgs([...validArgs(), "--trace", "/tmp/trace.zip"]))
      .toThrowError("unknown staging admin browser smoke argument");
    expect(() =>
      parseArgs([...validArgs(), "--screenshot", "/tmp/screenshot.png"]),
    ).toThrowError("unknown staging admin browser smoke argument");
    expect(() =>
      parseArgs([...validArgs(), "--storage-state", "/tmp/state.json"]),
    ).toThrowError("unknown staging admin browser smoke argument");
    expect(() => parseArgs(validArgs("env:ADMIN_TOKEN"))).toThrowError(
      "admin token source must be file:/absolute/path or -",
    );
  });

  it("rejects production, credentials, query strings, and ports", () => {
    for (const route of [
      "https://yylx.world/prod",
      "https://user@yylx.world/dev",
      "https://yylx.world/dev?debug=1",
      "https://yylx.world:444/dev",
      "https://attacker.example/dev",
    ]) {
      expect(() => canonicalStagingRoute(route)).toThrowError(SafeSmokeError);
    }
  });

  it("requires an absolute JSON report path", () => {
    const relative = validArgs();
    relative[relative.indexOf("--report") + 1] = "report.json";
    expect(() => parseArgs(relative)).toThrowError("report must use an absolute .json path");
  });
});

describe("admin token source isolation", () => {
  it("loads a bounded stdin source", async () => {
    async function* stdin() {
      yield Buffer.from(`${RAW_ADMIN_TOKEN}\n`);
    }

    await expect(loadAdminToken("-", { stdin: stdin() })).resolves.toBe(
      RAW_ADMIN_TOKEN,
    );
  });

  it("rejects interactive stdin", async () => {
    const stdin = { isTTY: true };
    await expect(loadAdminToken("-", { stdin })).rejects.toMatchObject({
      code: "interactive_stdin",
    });
  });

  it("loads a private regular file and rejects a public one", async () => {
    const directory = await fs.mkdtemp(
      path.join(os.tmpdir(), "loom-admin-browser-smoke-"),
    );
    const secretFile = path.join(directory, "admin-token");
    const secretLink = path.join(directory, "admin-token-link");
    try {
      await fs.writeFile(secretFile, `${RAW_ADMIN_TOKEN}\n`, { mode: 0o600 });
      await expect(loadAdminToken(`file:${secretFile}`)).resolves.toBe(
        RAW_ADMIN_TOKEN,
      );

      await fs.chmod(secretFile, 0o640);
      await expect(loadAdminToken(`file:${secretFile}`)).rejects.toMatchObject({
        code: "unsafe_token_file",
      });

      await fs.chmod(secretFile, 0o400);
      await expect(loadAdminToken(`file:${secretFile}`)).rejects.toMatchObject({
        code: "unsafe_token_file",
      });

      await fs.chmod(secretFile, 0o700);
      await expect(loadAdminToken(`file:${secretFile}`)).rejects.toMatchObject({
        code: "unsafe_token_file",
      });

      await fs.chmod(secretFile, 0o644);
      await expect(loadAdminToken(`file:${secretFile}`)).rejects.toMatchObject({
        code: "unsafe_token_file",
      });

      await fs.chmod(secretFile, 0o600);
      await fs.symlink(secretFile, secretLink);
      await expect(loadAdminToken(`file:${secretLink}`)).rejects.toMatchObject({
        code: "unsafe_token_file",
      });
    } finally {
      await fs.rm(directory, { recursive: true, force: true });
    }
  });
});

describe("kind TLS boundary", () => {
  it("requires GitHub Actions and loopback-only DNS before bypassing TLS", async () => {
    const options = {
      route: "https://yylx.world/dev",
      insecureForKind: true,
    };
    await expect(
      assertInsecureKindBoundary(options, {
        env: { CI: "true" },
        dnsLookup: async () => [{ address: "127.0.0.1", family: 4 }],
      }),
    ).rejects.toMatchObject({ code: "invalid_tls_mode" });
    await expect(
      assertInsecureKindBoundary(options, {
        env: { CI: "true", GITHUB_ACTIONS: "true" },
        dnsLookup: async () => [{ address: "203.0.113.10", family: 4 }],
      }),
    ).rejects.toMatchObject({ code: "invalid_tls_target" });
    await expect(
      assertInsecureKindBoundary(options, {
        env: { CI: "true", GITHUB_ACTIONS: "true" },
        dnsLookup: async () => [
          { address: "127.0.0.1", family: 4 },
          { address: "::1", family: 6 },
        ],
      }),
    ).resolves.toBeUndefined();
  });

  it("does not resolve DNS when strict TLS remains enabled", async () => {
    let called = false;
    await assertInsecureKindBoundary(
      { route: "https://yylx.world/dev", insecureForKind: false },
      {
        env: {},
        dnsLookup: async () => {
          called = true;
          return [];
        },
      },
    );
    expect(called).toBe(false);
  });

  it("rejects unsafe DNS before reading the singleton token", async () => {
    process.env.CI = "true";
    process.env.GITHUB_ACTIONS = "true";
    const options = parseArgs([...validArgs(), "--insecure-for-kind"]);
    let secretRead = false;

    await expect(
      executeSmoke(options, {
        env: { CI: "true", GITHUB_ACTIONS: "true" },
        dnsLookup: async () => [{ address: "203.0.113.10", family: 4 }],
        fsModule: {
          lstat: async () => {
            secretRead = true;
            throw new Error("must not read");
          },
        },
      }),
    ).rejects.toMatchObject({ code: "invalid_tls_target" });
    expect(secretRead).toBe(false);
  });
});

describe("sanitized evidence contract", () => {
  it("requires a visible React query success marker before crediting UI", async () => {
    const waitFor = vi.fn().mockResolvedValue(undefined);
    const locator = vi.fn().mockReturnValue({ waitFor });

    await waitForSuccessfulQueryCard(
      { locator },
      "audit-events",
      12_345,
    );

    expect(locator).toHaveBeenCalledWith(
      '[data-loom-query="audit-events"]' +
        '[data-loom-query-status="success"]',
    );
    expect(waitFor).toHaveBeenCalledWith({
      state: "visible",
      timeout: 12_345,
    });
  });

  it("requires a full trailing quiet window before sealing browser checks", async () => {
    const handlers = new Map();
    let now = 0;
    const page = {
      on: vi.fn().mockImplementation((event, handler) => {
        handlers.set(event, handler);
      }),
      waitForTimeout: vi.fn().mockImplementation(async (milliseconds) => {
        now += milliseconds;
      }),
    };
    const monitor = createAuthenticatedPageMonitor(
      page,
      "https://yylx.world/dev",
      { nowFn: () => now },
    );

    handlers.get("request")();
    const waiting = monitor.waitForQuiet(1_000);
    await Promise.resolve();
    handlers.get("requestfinished")();
    const requestFinishedAt = now;
    await waiting;

    expect(now - requestFinishedAt).toBeGreaterThanOrEqual(500);
    expect(page.waitForTimeout).toHaveBeenCalled();
  });

  it("fails closed on authenticated page errors and same-origin 5xx", () => {
    const handlers = new Map();
    const page = {
      on: vi.fn().mockImplementation((event, handler) => {
        handlers.set(event, handler);
      }),
    };
    const monitor = createAuthenticatedPageMonitor(
      page,
      "https://yylx.world/dev",
    );
    const checks = {};

    handlers.get("pageerror")(new Error("render failed"));
    handlers.get("response")({
      status: () => 503,
      url: () => "https://yylx.world/dev/api/v1/admin/audit-events",
      request: () => ({ resourceType: () => "fetch" }),
    });

    expect(() => monitor.applyChecks(checks)).toThrowError(
      "authenticated browser runtime reported a blocking error",
    );
    expect(checks.browser_page_errors_clean).toBe(false);
    expect(checks.browser_server_errors_clean).toBe(false);
  });

  it("redacts known values, Loom credentials, and sensitive keys", () => {
    const report = sanitizeReportValue(
      {
        deployed_sha: DEPLOYED_SHA,
        detail: `Bearer ${RAW_ADMIN_TOKEN}`,
        authorization: `Bearer ${RAW_ADMIN_TOKEN}`,
        nested: { csrf_token: "loom_csrf_secret-value" },
      },
      [RAW_ADMIN_TOKEN],
    );
    const rendered = JSON.stringify(report);

    expect(report.deployed_sha).toBe(DEPLOYED_SHA);
    expect(rendered).not.toContain(RAW_ADMIN_TOKEN);
    expect(rendered).not.toContain("loom_csrf_secret-value");
    expect(report.authorization).toBe("[REDACTED]");
  });

  it("always logs out and confirms 401 after a browser-check failure", async () => {
    const requestUuid = "11111111-1111-4111-8111-111111111111";
    const requestId = `staging-admin-browser-${requestUuid}`;
    const targetUserId = "22222222-2222-4222-8222-222222222222";
    const nowMs = 1_800_000_000_000;
    const calls = [];
    let launchOptions = null;
    const closed = { browser: false, context: false, cookies: false };

    function response(status, payload = null, headers = {}) {
      return {
        status: () => status,
        body: async () => Buffer.alloc(0),
        headers: () => headers,
        json: async () => payload,
        dispose: async () => {},
      };
    }

    const me = {
      user: {
        id: targetUserId,
        username: "qianyi",
        is_platform_admin: true,
      },
      is_platform_admin: true,
      role: "platform_admin",
      csrf_token: "loom_csrf_cleanup-only",
    };
    const context = {
      request: {
        post: async (url) => {
          calls.push(["POST", url]);
          if (url.endsWith("/auth/logout")) return response(204);
          return response(204, null, {
            "cache-control": "no-store",
            pragma: "no-cache",
            "x-loom-build-sha": DEPLOYED_SHA,
          });
        },
        get: async (url) => {
          calls.push(["GET", url]);
          if (url.includes("/admin/audit-events")) {
            return response(200, {
              items: [
                {
                  id: "33333333-3333-4333-8333-333333333333",
                  actor: "staging-admin-browser-smoke",
                  action: "auth.staging_admin_browser_session.create",
                  request_id: requestId,
                  target_type: "user",
                  target_id: targetUserId,
                  metadata: {
                    target_username: "qianyi",
                    target_status: "pending_setup",
                    auth_source: "singleton_admin_bearer",
                    ttl_seconds: 900,
                    build_sha: DEPLOYED_SHA,
                  },
                },
              ],
            });
          }
          const meCalls = calls.filter(
            ([method, candidate]) =>
              method === "GET" && candidate.endsWith("/api/v1/auth/me"),
          ).length;
          return meCalls >= 3 ? response(401) : response(200, me);
        },
      },
      cookies: async () => [
        {
          name: "loom_session",
          value: "loom_session_staging_admin_example",
          httpOnly: true,
          secure: true,
          sameSite: "Lax",
          path: "/",
          expires: nowMs / 1000 + 900,
        },
      ],
      newPage: async () => {
        throw new Error("synthetic browser failure");
      },
      clearCookies: async () => {
        closed.cookies = true;
      },
      close: async () => {
        closed.context = true;
      },
    };
    const playwrightModule = {
      chromium: {
        launch: async (options) => {
          launchOptions = options;
          return {
            version: () => "123.0.0",
            newContext: async () => context,
            close: async () => {
              closed.browser = true;
            },
          };
        },
      },
    };

    async function* stdin() {
      yield Buffer.from(`${RAW_ADMIN_TOKEN}\n`);
    }

    const report = await executeSmoke(parseArgs(validArgs("-")), {
      env: {
        PATH: "/usr/bin",
        GITHUB_TOKEN: "must-not-reach-browser",
        LEAK: RAW_ADMIN_TOKEN,
      },
      stdin: stdin(),
      playwrightModule,
      randomUUIDFn: () => requestUuid,
      nowFn: () => nowMs,
    });

    expect(report.status).toBe("fail");
    expect(report.failure_code).toBe("browser_check_failed");
    expect(report.cleanup).toEqual({
      logout_status: 204,
      auth_me_after_logout_status: 401,
    });
    expect(calls).toContainEqual([
      "POST",
      "https://yylx.world/dev/api/v1/auth/logout",
    ]);
    expect(closed).toEqual({ browser: true, context: true, cookies: true });
    expect(launchOptions).toEqual({
      headless: true,
      env: { PATH: "/usr/bin" },
    });
    expect(JSON.stringify(report)).not.toContain(RAW_ADMIN_TOKEN);
  });

  it("requires one short Secure HttpOnly SameSite=Lax cookie", () => {
    const now = 1_800_000_000;
    expect(
      validateBootstrapCookie(
        [
          {
            name: "loom_session",
            value: "loom_session_staging_admin_example",
            httpOnly: true,
            secure: true,
            sameSite: "Lax",
            path: "/",
            expires: now + 900,
          },
        ],
        now,
      ),
    ).toBe(true);
    expect(
      validateBootstrapCookie(
        [
          {
            name: "loom_session",
            value: "loom_session_staging_admin_example",
            httpOnly: true,
            secure: false,
            sameSite: "Lax",
            path: "/",
            expires: now + 900,
          },
        ],
        now,
      ),
    ).toBe(false);
    expect(
      validateBootstrapCookie(
        [
          {
            name: "loom_session",
            value: "loom_session_normal_user",
            httpOnly: true,
            secure: true,
            sameSite: "Lax",
            path: "/",
            expires: now + 900,
          },
        ],
        now,
      ),
    ).toBe(false);
  });

  it("correlates only the exact safe bootstrap audit event", () => {
    const event = {
      id: "11111111-1111-4111-8111-111111111111",
      actor: "staging-admin-browser-smoke",
      action: "auth.staging_admin_browser_session.create",
      request_id: "staging-admin-browser-request",
      target_type: "user",
      target_id: "22222222-2222-4222-8222-222222222222",
      metadata: {
        target_username: "qianyi",
        target_status: "pending_setup",
        auth_source: "singleton_admin_bearer",
        ttl_seconds: 900,
        build_sha: DEPLOYED_SHA,
      },
    };
    const found = findCorrelatedAuditEvent(
      { items: [event] },
      {
        requestId: "staging-admin-browser-request",
        targetUserId: "22222222-2222-4222-8222-222222222222",
        username: "qianyi",
        buildSha: DEPLOYED_SHA,
      },
    );

    expect(found).toEqual(event);
    expect(
      findCorrelatedAuditEvent(
        { items: [{ ...event, request_id: "different" }] },
        {
          requestId: "staging-admin-browser-request",
          targetUserId: "22222222-2222-4222-8222-222222222222",
          username: "qianyi",
          buildSha: DEPLOYED_SHA,
        },
      ),
    ).toBeNull();
    expect(
      findCorrelatedAuditEvent(
        { items: [event] },
        {
          requestId: "staging-admin-browser-request",
          targetUserId: "22222222-2222-4222-8222-222222222222",
          username: "qianyi",
          buildSha: "b".repeat(40),
        },
      ),
    ).toBeNull();
  });

  it("scrubs secret-bearing keys and values from Chromium environment", () => {
    expect(
      scrubBrowserEnvironment(
        {
          PATH: "/usr/bin",
          HOME: "/home/runner",
          ADMIN_TOKEN: RAW_ADMIN_TOKEN,
          GITHUB_TOKEN: "github-secret",
          INDIRECT: `prefix-${RAW_ADMIN_TOKEN}`,
          LOOM_VALUE: "loom_csrf_secret-value",
        },
        [RAW_ADMIN_TOKEN],
      ),
    ).toEqual({ PATH: "/usr/bin", HOME: "/home/runner" });
  });
});
