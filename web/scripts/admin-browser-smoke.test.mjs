import { describe, it, expect, vi } from "vitest";
import { parseOptions, readPassword, runSmoke } from "./admin-browser-smoke.mjs";

describe("normal-login admin smoke", () => {
  it("accepts secret references and local disposable targets", () => {
    expect(parseOptions(["--url", "http://127.0.0.1:8000", "--username", "tester", "--password", "env:PASS"]).passwordSource).toBe("env:PASS");
  });
  it.each(["http://example.com", "https://user:pass@example.com", "https://example.com?token=secret"])('rejects unsafe target %s', url => {
    expect(() => parseOptions(["--url", url, "--username", "tester", "--password", "env:PASS"])).toThrow();
  });
  it("rejects literal passwords and missing secrets", async () => {
    expect(() => parseOptions(["--url", "https://example.com", "--username", "tester", "--password", "literal-secret"])).toThrow();
    await expect(readPassword("env:PASS", {})).rejects.toThrow();
    expect(await readPassword("env:PASS", { PASS: "test-password" })).toBe("test-password");
  });
});

function fakeBrowser({ pageFailure = false, logoutStatus = 204 } = {}) {
  let signedIn = true;
  const identity = { is_platform_admin: true, user: { username: "tester" }, csrf_token: "test-csrf" };
  const response = { status: () => 200, json: async () => identity };
  const page = {
    setDefaultTimeout() {}, on() {}, url: () => "https://example.com/auth/login",
    goto: async url => { if (pageFailure && url.endsWith("/admin/access")) throw new Error("page failed"); },
    getByLabel: () => ({ fill: async () => {} }),
    getByRole: () => ({ click: async () => {}, first: () => ({ waitFor: async () => {} }) }),
    waitForResponse: async () => response, waitForFunction: async () => {},
  };
  const context = {
    newPage: async () => page, close: vi.fn(),
    request: {
      get: async () => ({ status: () => signedIn ? 200 : 401, json: async () => identity }),
      post: vi.fn(async () => { signedIn = false; return { status: () => logoutStatus }; }),
    },
  };
  const browser = { newContext: async () => context, close: vi.fn() };
  const chromium = { launch: vi.fn(async () => browser) };
  return { chromium, context, browser };
}

it("revokes the normal session and closes the browser after a page failure", async () => {
  const fixture = fakeBrowser({ pageFailure: true });
  await expect(runSmoke({ url: "https://example.com", username: "tester", passwordSource: "env:PASS" }, {
    chromium: fixture.chromium, env: { PASS: "test-password", PATH: "/bin" },
  })).rejects.toThrow("page failed");
  expect(fixture.context.request.post).toHaveBeenCalledWith("https://example.com/api/v1/auth/logout", {
    headers: { "X-Loom-CSRF": "test-csrf" },
  });
  expect(fixture.chromium.launch.mock.calls[0][0].env).not.toHaveProperty("PASS");
  expect(fixture.context.close).toHaveBeenCalled();
  expect(fixture.browser.close).toHaveBeenCalled();
});

it("cannot report success if session cleanup fails", async () => {
  const fixture = fakeBrowser({ logoutStatus: 500 });
  await expect(runSmoke({ url: "https://example.com", username: "tester", passwordSource: "env:PASS" }, {
    chromium: fixture.chromium, env: { PASS: "test-password" },
  })).rejects.toThrow("cleanup failed");
  expect(fixture.browser.close).toHaveBeenCalled();
});
