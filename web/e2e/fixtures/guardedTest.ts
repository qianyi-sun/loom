import { test as base, expect, type Page, type Response } from "@playwright/test";

export type FailureSink = {
  failures: string[];
  allowedUnauthorizedConsoleErrors: number;
  allowedNotFoundConsoleErrors: number;
};

const UNAUTHORIZED_RESOURCE_ERROR =
  "Failed to load resource: the server responded with a status of 401 (Unauthorized)";
const NOT_FOUND_RESOURCE_ERROR =
  "Failed to load resource: the server responded with a status of 404 (Not Found)";

function responseIsBadAsset(response: Response): boolean {
  const url = new URL(response.url());
  if (url.origin !== "http://127.0.0.1:4173") return false;
  const type = response.request().resourceType();
  if (!new Set(["script", "stylesheet"]).has(type)) return false;
  if (!response.ok()) return true;
  const contentType = response.headers()["content-type"] ?? "";
  return type === "script"
    ? !/javascript/u.test(contentType)
    : !/text\/css/u.test(contentType);
}

function installGuards(page: Page, sink: FailureSink): void {
  page.on("pageerror", (error) => sink.failures.push(`pageerror: ${error.message}`));
  page.on("console", (message) => {
    if (message.type() === "error" || message.type() === "warning") {
      if (
        message.type() === "error" &&
        message.text() === UNAUTHORIZED_RESOURCE_ERROR &&
        sink.allowedUnauthorizedConsoleErrors > 0
      ) {
        sink.allowedUnauthorizedConsoleErrors -= 1;
        return;
      }
      if (
        message.type() === "error" &&
        message.text() === NOT_FOUND_RESOURCE_ERROR &&
        sink.allowedNotFoundConsoleErrors > 0
      ) {
        sink.allowedNotFoundConsoleErrors -= 1;
        return;
      }
      sink.failures.push(`console ${message.type()}: ${message.text()}`);
    }
  });
  page.on("requestfailed", (request) => {
    const url = new URL(request.url());
    if (url.origin === "http://127.0.0.1:4173") {
      sink.failures.push(`same-origin request failed: ${url.pathname}`);
    }
  });
  page.on("response", (response) => {
    if (responseIsBadAsset(response)) {
      sink.failures.push(`bad same-origin asset response: ${new URL(response.url()).pathname}`);
    }
  });
}

export const test = base.extend<{ failureSink: FailureSink }>({
  failureSink: async ({ page }, use) => {
    const sink = {
      failures: [],
      allowedUnauthorizedConsoleErrors: 0,
      allowedNotFoundConsoleErrors: 0,
    };
    installGuards(page, sink);
    await use(sink);
    expect(sink.failures, sink.failures.join("\n")).toEqual([]);
  },
});

export { expect };
