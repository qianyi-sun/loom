import { Buffer } from "node:buffer";
import * as fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import process from "node:process";

import { afterEach, describe, expect, it } from "vitest";

import {
  SafeSmokeError,
  canonicalStagingRoute,
  executeSmoke,
  findCorrelatedAuditEvent,
  loadAdminToken,
  parseArgs,
  sanitizeReportValue,
  validateBootstrapCookie,
} from "./staging-admin-browser-smoke.mjs";

const RAW_ADMIN_TOKEN = `loom_admin_${"A".repeat(43)}`;
const CANDIDATE_SHA = "a".repeat(40);
const originalCi = process.env.CI;

function validArgs(tokenSource = "env:ADMIN_TOKEN") {
  return [
    "--route",
    "https://yylx.world/dev",
    "--candidate-sha",
    CANDIDATE_SHA,
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
});

describe("staging admin browser smoke arguments", () => {
  it("accepts the exact staging route and secret-source contract", () => {
    process.env.CI = "true";
    const options = parseArgs([...validArgs(), "--insecure-for-kind"]);

    expect(options).toMatchObject({
      route: "https://yylx.world/dev",
      candidateSha: CANDIDATE_SHA,
      adminTokenSource: "env:ADMIN_TOKEN",
      username: "qianyi",
      reportPath: "/tmp/staging-admin-browser-smoke.json",
      insecureForKind: true,
    });
  });

  it.each(["env:ADMIN_TOKEN", "file:/run/secrets/admin-token", "-"])(
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
  });

  it("rejects production, credentials, query strings, and ports", () => {
    for (const route of [
      "https://yylx.world/prod",
      "https://user@yylx.world/dev",
      "https://yylx.world/dev?debug=1",
      "https://yylx.world:444/dev",
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
  it("loads an environment source without changing the value", async () => {
    await expect(
      loadAdminToken("env:ADMIN_TOKEN", {
        env: { ADMIN_TOKEN: RAW_ADMIN_TOKEN },
      }),
    ).resolves.toBe(RAW_ADMIN_TOKEN);
  });

  it("loads a bounded stdin source", async () => {
    async function* stdin() {
      yield Buffer.from(`${RAW_ADMIN_TOKEN}\n`);
    }

    await expect(loadAdminToken("-", { stdin: stdin() })).resolves.toBe(
      RAW_ADMIN_TOKEN,
    );
  });

  it("loads a private regular file and rejects a public one", async () => {
    const directory = await fs.mkdtemp(
      path.join(os.tmpdir(), "loom-admin-browser-smoke-"),
    );
    const secretFile = path.join(directory, "admin-token");
    const secretLink = path.join(directory, "admin-token-link");
    try {
      await fs.writeFile(secretFile, `${RAW_ADMIN_TOKEN}\n`, { mode: 0o640 });
      await expect(loadAdminToken(`file:${secretFile}`)).resolves.toBe(
        RAW_ADMIN_TOKEN,
      );

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

describe("sanitized evidence contract", () => {
  it("redacts known values, Loom credentials, and sensitive keys", () => {
    const report = sanitizeReportValue(
      {
        candidate_sha: CANDIDATE_SHA,
        detail: `Bearer ${RAW_ADMIN_TOKEN}`,
        authorization: `Bearer ${RAW_ADMIN_TOKEN}`,
        nested: { csrf_token: "loom_csrf_secret-value" },
      },
      [RAW_ADMIN_TOKEN],
    );
    const rendered = JSON.stringify(report);

    expect(report.candidate_sha).toBe(CANDIDATE_SHA);
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
        launch: async () => ({
          version: () => "123.0.0",
          newContext: async () => context,
          close: async () => {
            closed.browser = true;
          },
        }),
      },
    };

    const report = await executeSmoke(parseArgs(validArgs()), {
      env: { ADMIN_TOKEN: RAW_ADMIN_TOKEN },
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
      },
    };
    const found = findCorrelatedAuditEvent(
      { items: [event] },
      {
        requestId: "staging-admin-browser-request",
        targetUserId: "22222222-2222-4222-8222-222222222222",
        username: "qianyi",
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
        },
      ),
    ).toBeNull();
  });
});
