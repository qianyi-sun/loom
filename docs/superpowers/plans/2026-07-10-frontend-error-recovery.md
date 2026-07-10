# Frontend Error Recovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Loom mount a visible React startup shell immediately and recover from runtime-configuration, session-hydration, root-render, route-render, and future lazy-chunk failures without an empty root or secret disclosure.

**Architecture:** A small `FrontendBootstrap` state machine mounts before runtime configuration loads, then installs the existing Query, Router, Auth, and App providers behind a root error boundary. Auth hydration becomes an explicit `loading | authenticated | signed-out | unavailable` state machine, while a location-keyed route boundary keeps the authenticated shell and sibling routes alive when one page fails. Every failure renders one shared accessible recovery panel and reports only redacted data through a replaceable browser reporter; the default reporter is console-only in local development and a no-op in production until an operator-selected telemetry adapter is installed.

**Tech Stack:** React 18.3, TypeScript 5.5 strict mode, React Router 6.26, TanStack Query 5.51, Vite 8, Vitest 4.1.8, Testing Library, and the deterministic Playwright Chromium harness delivered by #773.

## Global Constraints

- Start implementation only after #773's typecheck, warning/network guard, coverage, production-build, and Playwright foundation has merged into `origin/dev`; rebase the #783 branch onto that merged commit before the first RED run.
- Scope is #783. `web/src/App.tsx` remains synchronously imported and unchanged. #212 alone owns converting page imports to `React.lazy`, route chunking, bundle budgets, and chunk composition.
- #783 supplies the `Suspense` and error-recovery contract that #212 will consume. A synthetic `React.lazy` rejection in Vitest proves that contract without adding a production lazy route.
- #772 owns canonical `/dev` and `/prod` redirects, prefix-stable build assets, and executable asset MIME checks. #783 consumes the resulting `routePath`; it does not add a root asset route or another redirect.
- The browser must show static `Starting Loom` content before JavaScript executes, a React startup status while runtime config loads, and a React session status while `/auth/me` is pending. None of those phases may render an empty `#root`.
- A `401` from `/auth/me` means `signed-out`, clears session/query state, and allows the existing Settings redirect. A network error or non-401 response means `unavailable`, preserves cached query data, does not pretend the user is signed out, and shows Retry, Reload, and Home actions.
- Runtime config failure is fail-closed on `/dev` and `/prod`. The existing unprefixed local-development fallback remains supported because Vite may return its HTML shell for a missing `/loom-frontend-config.json`.
- Recovery UI never renders an exception message, stack, component stack, response body, request body, cookie, CSRF value, provider key, bearer token, setup/reset/invite value, signed URL, query string, or hash.
- Reporter adapters receive only a redacted `BrowserErrorReport`; raw `Error` and `ApiError` objects never cross the reporter interface.
- Browser error IDs use `WEB-<eight uppercase hexadecimal characters>` when `crypto.randomUUID()` is available and contain no user, route-query, credential, provider, or request data.
- Home resolves to `/`, `/dev/`, or `/prod/` from the validated runtime route. Retry stays on the current route, and Reload reloads the current document, so all actions remain basename-safe.
- The route boundary resets when React Router's `location.key` changes. A failed page may not poison a healthy sibling route.
- Native buttons, links, focus management, `role="status"`, `role="alert"`, and live-region semantics are required now. #777 may later restyle these surfaces through shared accessible primitives but may not weaken their behavior.
- Vitest and Playwright remain deterministic and offline. Expected injected HTTP failures are counted explicitly; uncaught `pageerror`, unexpected console output, unexpected network, and unused failure allowances always fail.
- The production reporter performs no network request in #783. Adding Sentry, OpenTelemetry, or another transport requires a separately reviewed adapter and deployment configuration.
- Raw credentials, cookies, CSRF values, storage state, setup/reset/invite links, provider keys, bearer tokens, signed URLs, and query strings never enter screenshots, traces, test artifacts, logs, docs examples, issue comments, or staging evidence.

---

## Baseline and Ownership Map

At `cbf04761564cb019113771110ae71933cc7f9d5e`:

- `web/src/main.tsx` waits for `loadFrontendConfig()` before calling `createRoot`, so `#root` is empty during config I/O and receives an inline string on failure.
- `web/src/components/Layout.tsx` returns an empty full-height element during `/auth/me` and treats a non-401 session failure as signed out after loading completes.
- `web/src/App.tsx` has one synchronous route tree and no root or route boundary.
- `web/src/lib/frontendConfig.ts` throws untyped errors for prefixed config failures and silently defaults every unprefixed failure.
- `web/src/components/ErrorState.tsx` already redacts page-level API details, but it has no error identity, recovery actions, or reporter contract.

The implementation uses these ownership boundaries:

- `web/src/lib/frontendConfig.ts`: typed runtime-config failures and route/home helpers.
- `web/src/lib/errorReporting.ts`: error identity generation, safe normalization, redaction, and replaceable reporter registration.
- `web/src/components/StartupStatus.tsx`: visible non-error startup and auth-hydration state.
- `web/src/components/RecoveryPanel.tsx`: fixed human copy, support identity, focus, Retry, Reload, and Home controls.
- `web/src/components/BrowserErrorBoundary.tsx`: class boundary for render failures and explicit retry.
- `web/src/components/RouteRecoveryBoundary.tsx`: `location.key` reset plus the future-lazy `Suspense` boundary.
- `web/src/FrontendBootstrap.tsx`: config state machine and configured provider tree behind the root boundary.
- `web/src/auth/AuthContext.tsx` and `web/src/auth/authContextValue.ts`: explicit session state machine.
- `web/src/components/Layout.tsx`: visible auth states and route-boundary placement around each `<Outlet>`.
- `web/index.html` and `web/src/main.tsx`: static pre-JavaScript fallback and immediate React mount.
- Post-#773 `web/e2e/fixtures/api.ts`, `web/e2e/fixtures/guardedTest.ts`, and `web/e2e/recovery.spec.ts`: deterministic browser fault injection and recovery proof.
- `docs/architecture/frontend-error-recovery.md` and `docs/runbooks/operator-runbook.md`: developer contract and operator triage path.

`web/src/App.tsx` is intentionally absent from the modification list. The route tree already nests every page below `Layout`, so one boundary around `Outlet` contains page and future lazy-module failures without moving or lazily importing any route.

## Public Interfaces

```ts
export type FrontendConfigLoadErrorKind = "network" | "http" | "invalid";

export class FrontendConfigLoadError extends Error {
  readonly kind: FrontendConfigLoadErrorKind;
  readonly status: number | null;
}

export function frontendRoutePathForLocation(
  location: Location | URL,
): "" | "/dev" | "/prod";

export function frontendHomeHref(routePath: string): "/" | "/dev/" | "/prod/";

export type BrowserErrorScope = "startup" | "session" | "root" | "route";

export interface BrowserErrorReport {
  id: string;
  scope: BrowserErrorScope;
  errorName: string;
  message: string;
  stack: string | null;
  componentStack: string | null;
  pathname: string;
}

export interface BrowserErrorReporter {
  report(event: BrowserErrorReport): void;
}

export function setBrowserErrorReporter(
  reporter: BrowserErrorReporter,
): () => void;

export type AuthSessionStatus =
  | "loading"
  | "authenticated"
  | "signed-out"
  | "unavailable";
```

The reporter setter returns a restoration closure so tests and future adapters cannot leak module-global reporter state across cases. `BrowserErrorReport.pathname` excludes `location.search` and `location.hash` by construction.

## Task 1: Type Runtime-Configuration Failures and Basename-Safe Home

**Files:**

- Modify: `web/src/__tests__/frontend-config.test.ts`
- Modify: `web/src/lib/frontendConfig.ts`

**Interfaces:**

- Produces: `FrontendConfigLoadError`, `frontendRoutePathForLocation()`, `frontendHomeHref()`, and typed fail-closed behavior for prefixed runtime config.
- Preserves: unprefixed local Vite fallback and the existing `FrontendConfig`, `resolveFrontendConfig()`, `getFrontendConfig()`, `getApiBase()`, and `setFrontendConfigForTests()` contracts.

- [ ] **Step 1: Write failing config/error/home tests**

Extend `web/src/__tests__/frontend-config.test.ts` with the following imports and cases. Reset the browser URL to `/` in the existing `afterEach` hook.

```ts
import {
  FrontendConfigLoadError,
  frontendHomeHref,
  frontendRoutePathForLocation,
  getFrontendConfig,
  loadFrontendConfig,
  resolveFrontendConfig,
  setFrontendConfigForTests,
} from "../lib/frontendConfig";

afterEach(() => {
  setFrontendConfigForTests(null);
  window.history.replaceState(null, "", "/");
  vi.restoreAllMocks();
});

it("surfaces a prefixed runtime-config 500 without reading its body", async () => {
  window.history.replaceState(null, "", "/dev/settings");
  vi.spyOn(globalThis, "fetch").mockResolvedValue(
    new Response("provider_key=sk-proj-abcdefghijklmnopqrstuvwxyz0123456789", {
      status: 500,
      headers: { "Content-Type": "text/plain" },
    }),
  );

  const rejection = loadFrontendConfig();

  await expect(rejection).rejects.toMatchObject({
    name: "FrontendConfigLoadError",
    kind: "http",
    status: 500,
    message: "Frontend configuration returned HTTP 500",
  });
  expect(await (await globalThis.fetch.mock.results[0].value).bodyUsed).toBe(false);
});

it.each([
  ["invalid JSON", "{not-json"],
  [
    "route mismatch",
    JSON.stringify({
      environment: "production",
      environmentLabel: "Production",
      routePath: "/prod",
      apiBase: "/prod",
    }),
  ],
])("classifies %s as an invalid prefixed config", async (_label, body) => {
  window.history.replaceState(null, "", "/dev/settings");
  vi.spyOn(globalThis, "fetch").mockResolvedValue(
    new Response(body, {
      status: 200,
      headers: { "Content-Type": "application/json" },
    }),
  );

  await expect(loadFrontendConfig()).rejects.toMatchObject({
    name: "FrontendConfigLoadError",
    kind: "invalid",
    status: null,
    message: "Frontend configuration response was invalid",
  });
});

it("keeps the unprefixed Vite fallback for a missing local config", async () => {
  window.history.replaceState(null, "", "/settings");
  vi.spyOn(globalThis, "fetch").mockResolvedValue(
    new Response("<!doctype html><title>Loom</title>", {
      status: 200,
      headers: { "Content-Type": "text/html" },
    }),
  );

  await expect(loadFrontendConfig()).resolves.toMatchObject({
    environment: "local",
    routePath: "",
  });
});

it.each([
  [new URL("https://yylx.world/"), "", "/"],
  [new URL("https://yylx.world/dev/monitor?view=trials"), "/dev", "/dev/"],
  [new URL("https://yylx.world/prod/batches/batch-1"), "/prod", "/prod/"],
] as const)("derives a safe home for %s", (location, routePath, home) => {
  expect(frontendRoutePathForLocation(location)).toBe(routePath);
  expect(frontendHomeHref(routePath)).toBe(home);
});

it("keeps the typed error constructible without a response body", () => {
  expect(new FrontendConfigLoadError("network")).toMatchObject({
    kind: "network",
    status: null,
    message: "Frontend configuration request failed",
  });
});
```

Use a typed local `fetchMock` variable rather than accessing `.mock` on `globalThis.fetch` if #773's `FetchMock` helper makes that necessary after rebase; the assertion remains `bodyUsed === false` and proves the 500 body is never copied into UI or telemetry.

- [ ] **Step 2: Run the focused tests and verify RED**

```bash
cd web
npm test -- src/__tests__/frontend-config.test.ts
```

Expected: FAIL at TypeScript collection because `FrontendConfigLoadError`, `frontendRoutePathForLocation`, and `frontendHomeHref` are not exported.

- [ ] **Step 3: Add the typed error and public route helpers**

In `web/src/lib/frontendConfig.ts`, replace the private `detectRoutePath` with these exported definitions immediately after `RawFrontendConfig`:

```ts
export type FrontendConfigLoadErrorKind = "network" | "http" | "invalid";

export class FrontendConfigLoadError extends Error {
  readonly kind: FrontendConfigLoadErrorKind;
  readonly status: number | null;

  constructor(
    kind: FrontendConfigLoadErrorKind,
    options: { status?: number; cause?: unknown } = {},
  ) {
    const message =
      kind === "http"
        ? `Frontend configuration returned HTTP ${options.status ?? 0}`
        : kind === "invalid"
          ? "Frontend configuration response was invalid"
          : "Frontend configuration request failed";
    super(message, { cause: options.cause });
    this.name = "FrontendConfigLoadError";
    this.kind = kind;
    this.status = options.status ?? null;
  }
}

export function frontendRoutePathForLocation(
  location: Location | URL,
): "" | "/dev" | "/prod" {
  const pathname = location.pathname.replace(/\/+$/u, "") || "/";
  if (pathname === "/prod" || pathname.startsWith("/prod/")) return "/prod";
  if (pathname === "/dev" || pathname.startsWith("/dev/")) return "/dev";
  return "";
}

export function frontendHomeHref(
  routePath: string,
): "/" | "/dev/" | "/prod/" {
  if (routePath === "/dev") return "/dev/";
  if (routePath === "/prod") return "/prod/";
  return "/";
}
```

Replace every internal `detectRoutePath(...)` call with `frontendRoutePathForLocation(...)`.

- [ ] **Step 4: Replace `loadFrontendConfig()` with typed fail-closed loading**

Keep the existing `defaultConfig()` and replace `configUrlForLocation()` plus `loadFrontendConfig()` with:

```ts
function configUrlForLocation(location: Location | URL): string {
  const routePath = frontendRoutePathForLocation(location);
  return `${routePath}/loom-frontend-config.json`;
}

function useLocalConfigFallback(routePath: string): FrontendConfig {
  if (routePath !== "") {
    throw new Error("local config fallback is only valid at the root route");
  }
  currentConfig = defaultConfig();
  return currentConfig;
}

export async function loadFrontendConfig(): Promise<FrontendConfig> {
  const routePath = frontendRoutePathForLocation(window.location);
  let response: Response;
  try {
    response = await fetch(configUrlForLocation(window.location), {
      cache: "no-store",
      credentials: "same-origin",
      headers: { Accept: "application/json" },
    });
  } catch (cause) {
    if (routePath === "") return useLocalConfigFallback(routePath);
    throw new FrontendConfigLoadError("network", { cause });
  }

  if (!response.ok) {
    if (routePath === "") return useLocalConfigFallback(routePath);
    throw new FrontendConfigLoadError("http", { status: response.status });
  }

  let raw: RawFrontendConfig;
  try {
    raw = (await response.json()) as RawFrontendConfig;
  } catch (cause) {
    if (routePath === "") return useLocalConfigFallback(routePath);
    throw new FrontendConfigLoadError("invalid", { cause });
  }

  try {
    currentConfig = resolveFrontendConfig(raw, window.location);
    return currentConfig;
  } catch (cause) {
    if (routePath === "") return useLocalConfigFallback(routePath);
    throw new FrontendConfigLoadError("invalid", { cause });
  }
}
```

The HTTP branch never calls `response.text()` or `response.json()`. The wrapper stores the original cause for local debugging, but later tasks normalize only its safe public message and never pass `cause` to UI or a reporter.

- [ ] **Step 5: Verify GREEN and commit**

```bash
cd web
npm test -- src/__tests__/frontend-config.test.ts
npm run typecheck
npm run lint
```

Expected: the config test file passes, typecheck exits 0, lint exits 0, prefixed 500/invalid responses reject with the typed class, and the unprefixed local fallback still resolves.

```bash
git add web/src/lib/frontendConfig.ts web/src/__tests__/frontend-config.test.ts
git commit -m "fix(web): type runtime config recovery (#783)"
```

## Task 2: Add Redacted, Replaceable Error Reporting

**Files:**

- Create: `web/src/lib/errorReporting.ts`
- Create: `web/src/__tests__/lib/errorReporting.test.ts`

**Interfaces:**

- Consumes: `redactText()` from `web/src/lib/redaction.ts`.
- Produces: `BrowserErrorScope`, `BrowserErrorReport`, `BrowserErrorReporter`, `createBrowserErrorId()`, `reportBrowserError()`, and `setBrowserErrorReporter()`.
- Guarantees: the adapter receives strings only after redaction; reporter exceptions are contained; URL query/hash data is never collected.

- [ ] **Step 1: Write the failing reporter contract tests**

Create `web/src/__tests__/lib/errorReporting.test.ts`:

```ts
import { afterEach, describe, expect, it, vi } from "vitest";

import {
  reportBrowserError,
  setBrowserErrorReporter,
  type BrowserErrorReport,
} from "../../lib/errorReporting";

describe("browser error reporting", () => {
  let restoreReporter: (() => void) | null = null;

  afterEach(() => {
    restoreReporter?.();
    restoreReporter = null;
  });

  it("passes only redacted fields and a pathname to a replacement reporter", () => {
    const captured: BrowserErrorReport[] = [];
    restoreReporter = setBrowserErrorReporter({
      report: (event) => captured.push(event),
    });
    const error = new Error(
      "Bearer loom_api_abcdefghijklmnopqrstuvwxyz012345 at " +
        "http://minio.internal/a?X-Amz-Signature=secret",
    );
    error.name = "Provider sk-proj-abcdefghijklmnopqrstuvwxyz0123456789";

    const report = reportBrowserError(error, {
      id: "WEB-12AB34CD",
      scope: "route",
      componentStack:
        "at SecretPanel (http://loom-control-plane:8080/?token=loom_reset_abcdefghijklmnopqrstuvwxyz012345)",
      pathname: "/dev/auth/reset",
    });

    expect(report).toEqual(captured[0]);
    expect(report).toMatchObject({
      id: "WEB-12AB34CD",
      scope: "route",
      pathname: "/dev/auth/reset",
    });
    expect(JSON.stringify(report)).not.toContain("loom_api_");
    expect(JSON.stringify(report)).not.toContain("sk-proj-");
    expect(JSON.stringify(report)).not.toContain("minio.internal");
    expect(JSON.stringify(report)).not.toContain("X-Amz-Signature=secret");
    expect(JSON.stringify(report)).not.toContain("loom_reset_");
    expect(JSON.stringify(report)).not.toContain("?token=");
    expect(JSON.stringify(report)).toContain("[REDACTED]");
  });

  it("normalizes an API-shaped failure without exposing structured detail", () => {
    const report = reportBrowserError(
      { status: 503, detail: { token: "loom_api_abcdefghijklmnopqrstuvwxyz012345" } },
      {
        id: "WEB-89ABCDEF",
        scope: "session",
        pathname: "/prod/",
      },
    );

    expect(report).toMatchObject({
      errorName: "HTTP 503",
      message: "Structured error detail",
      stack: null,
    });
  });

  it("contains a broken reporter instead of replacing the recovery UI", () => {
    restoreReporter = setBrowserErrorReporter({
      report: () => {
        throw new Error("telemetry unavailable");
      },
    });

    expect(() =>
      reportBrowserError(new Error("render failed"), {
        id: "WEB-13572468",
        scope: "root",
        pathname: "/dev/",
      }),
    ).not.toThrow();
  });

  it("creates a support identity without embedding input data", () => {
    const randomUUID = vi
      .spyOn(globalThis.crypto, "randomUUID")
      .mockReturnValue("abcdef12-3456-4789-abcd-ef1234567890");

    const report = reportBrowserError(new Error("customer@example.com"), {
      scope: "startup",
      pathname: "/dev/",
    });

    expect(report.id).toBe("WEB-ABCDEF12");
    expect(report.id).not.toContain("customer");
    randomUUID.mockRestore();
  });
});
```

- [ ] **Step 2: Run the reporter test and verify RED**

```bash
cd web
npm test -- src/__tests__/lib/errorReporting.test.ts
```

Expected: FAIL because `web/src/lib/errorReporting.ts` does not exist.

- [ ] **Step 3: Implement the complete reporter module**

Create `web/src/lib/errorReporting.ts`:

```ts
import { redactText } from "./redaction";

export type BrowserErrorScope = "startup" | "session" | "root" | "route";

export interface BrowserErrorReport {
  id: string;
  scope: BrowserErrorScope;
  errorName: string;
  message: string;
  stack: string | null;
  componentStack: string | null;
  pathname: string;
}

export interface BrowserErrorReporter {
  report(event: BrowserErrorReport): void;
}

interface ReportContext {
  id?: string;
  scope: BrowserErrorScope;
  componentStack?: string | null;
  pathname?: string;
}

const noopReporter: BrowserErrorReporter = { report: () => undefined };
const consoleReporter: BrowserErrorReporter = {
  report: (event) => console.error("Loom browser error", event),
};

let fallbackSequence = 0;
let activeReporter: BrowserErrorReporter =
  import.meta.env.DEV && import.meta.env.MODE !== "test"
    ? consoleReporter
    : noopReporter;

function safePathname(pathname: string): string {
  return redactText(pathname.split(/[?#]/u, 1)[0] || "/");
}

function normalizedError(error: unknown): {
  errorName: string;
  message: string;
  stack: string | null;
} {
  if (error instanceof Error) {
    return {
      errorName: redactText(error.name || "Error"),
      message: redactText(error.message || "Browser error"),
      stack: error.stack ? redactText(error.stack) : null,
    };
  }
  if (typeof error === "object" && error !== null && "status" in error) {
    const status = (error as { status: unknown }).status;
    const detail = "detail" in error ? (error as { detail: unknown }).detail : null;
    return {
      errorName: `HTTP ${typeof status === "number" ? status : "error"}`,
      message:
        typeof detail === "string"
          ? redactText(detail)
          : detail === null || detail === undefined
            ? "Request failed"
            : "Structured error detail",
      stack: null,
    };
  }
  return {
    errorName: "UnknownError",
    message: "Unknown browser error",
    stack: null,
  };
}

export function createBrowserErrorId(): string {
  if (typeof globalThis.crypto?.randomUUID === "function") {
    return `WEB-${globalThis.crypto.randomUUID().slice(0, 8).toUpperCase()}`;
  }
  fallbackSequence = (fallbackSequence + 1) % 1_000_000;
  const time = Date.now().toString(16).slice(-8).padStart(8, "0");
  const sequence = fallbackSequence.toString(16).padStart(6, "0");
  return `WEB-${time.slice(0, 2)}${sequence}`.toUpperCase();
}

export function setBrowserErrorReporter(
  reporter: BrowserErrorReporter,
): () => void {
  const previous = activeReporter;
  activeReporter = reporter;
  return () => {
    activeReporter = previous;
  };
}

export function reportBrowserError(
  error: unknown,
  context: ReportContext,
): BrowserErrorReport {
  const normalized = normalizedError(error);
  const report: BrowserErrorReport = {
    id: context.id ?? createBrowserErrorId(),
    scope: context.scope,
    errorName: normalized.errorName,
    message: normalized.message,
    stack: normalized.stack,
    componentStack: context.componentStack
      ? redactText(context.componentStack)
      : null,
    pathname: safePathname(context.pathname ?? window.location.pathname),
  };
  try {
    activeReporter.report(report);
  } catch {
    // Recovery UI must remain available when the optional reporter fails.
  }
  return report;
}
```

Do not add `cause`, `location.href`, `location.search`, `location.hash`, arbitrary object serialization, or the raw error object to `BrowserErrorReport`.

- [ ] **Step 4: Verify GREEN and commit**

```bash
cd web
npm test -- src/__tests__/lib/errorReporting.test.ts \
  src/__tests__/lib/redaction.test.ts
npm run typecheck
npm run lint
```

Expected: both test files pass with no console/network guard output; typecheck and lint exit 0.

```bash
git add web/src/lib/errorReporting.ts \
  web/src/__tests__/lib/errorReporting.test.ts
git commit -m "feat(web): add redacted recovery diagnostics (#783)"
```

## Task 3: Build the Accessible Recovery Surface and Render Boundaries

**Files:**

- Create: `web/src/components/StartupStatus.tsx`
- Create: `web/src/components/RecoveryPanel.tsx`
- Create: `web/src/components/BrowserErrorBoundary.tsx`
- Create: `web/src/components/RouteRecoveryBoundary.tsx`
- Create: `web/src/__tests__/components/RecoveryPanel.test.tsx`
- Create: `web/src/__tests__/components/BrowserErrorBoundary.test.tsx`

**Interfaces:**

- `RecoveryPanel` accepts only fixed display copy, an error ID, `homeHref`, optional `onRetry`, and optional `onReload`; it never accepts an error object.
- `BrowserErrorBoundary` reports one redacted event per caught failure, supports same-route Retry, and resets when `resetKey` changes.
- `RouteRecoveryBoundary` uses `location.key`, wraps its child or `<Outlet>` in `Suspense`, and owns the approved route/lazy failure copy.

- [ ] **Step 1: Write RED component tests**

Create `RecoveryPanel.test.tsx` with parameterized `/`, `/dev/`, and `/prod/` href assertions, native-button click assertions for Retry and Reload, `role="alert"`, and an assertion that the recovery heading receives focus. Create `BrowserErrorBoundary.test.tsx` with these four cases:

```tsx
it("catches a root render error without rendering its secret", async () => {
  const events: BrowserErrorReport[] = [];
  const restore = setBrowserErrorReporter({ report: (event) => events.push(event) });
  const reactError = vi.spyOn(console, "error").mockImplementation(() => undefined);
  function Broken(): JSX.Element {
    throw new Error("Bearer loom_api_abcdefghijklmnopqrstuvwxyz012345");
  }
  render(
    <BrowserErrorBoundary
      scope="root"
      title="Loom encountered an application error"
      message="Reload the application or return home."
      homeHref="/dev/"
    >
      <Broken />
    </BrowserErrorBoundary>,
  );
  expect(await screen.findByRole("heading", {
    name: "Loom encountered an application error",
  })).toBeInTheDocument();
  expect(document.body.textContent).not.toContain("loom_api_");
  expect(events).toHaveLength(1);
  expect(JSON.stringify(events[0])).not.toContain("loom_api_");
  expect(reactError).toHaveBeenCalled();
  restore();
});

it("retries the same boundary after the failure is repaired", async () => {
  let shouldThrow = true;
  function Child(): JSX.Element {
    if (shouldThrow) throw new Error("render failed");
    return <h1>Recovered child</h1>;
  }
  vi.spyOn(console, "error").mockImplementation(() => undefined);
  const user = userEvent.setup();
  render(
    <BrowserErrorBoundary
      scope="root"
      title="Loom encountered an application error"
      message="Reload the application or return home."
      homeHref="/"
    >
      <Child />
    </BrowserErrorBoundary>,
  );
  await screen.findByRole("alert");
  shouldThrow = false;
  await user.click(screen.getByRole("button", { name: "Retry" }));
  expect(await screen.findByRole("heading", { name: "Recovered child" })).toBeInTheDocument();
});

it("resets a failed route when the location key changes", async () => {
  vi.spyOn(console, "error").mockImplementation(() => undefined);
  const user = userEvent.setup();
  function Broken(): JSX.Element {
    throw new Error("route render failed");
  }
  render(
    <MemoryRouter initialEntries={["/broken"]}>
      <Link to="/healthy">Open healthy sibling</Link>
      <Routes>
        <Route element={<RouteRecoveryBoundary />}>
          <Route path="broken" element={<Broken />} />
          <Route path="healthy" element={<h1>Healthy sibling</h1>} />
        </Route>
      </Routes>
    </MemoryRouter>,
  );
  await screen.findByRole("heading", { name: "This page could not be displayed" });
  await user.click(screen.getByRole("link", { name: "Open healthy sibling" }));
  expect(await screen.findByRole("heading", { name: "Healthy sibling" })).toBeInTheDocument();
});

it("turns a rejected lazy module into the route fallback without adding a lazy route", async () => {
  vi.spyOn(console, "error").mockImplementation(() => undefined);
  const LazyFailure = lazy(async () => {
    throw new Error("Failed to fetch dynamically imported module");
  });
  render(
    <MemoryRouter>
      <RouteRecoveryBoundary><LazyFailure /></RouteRecoveryBoundary>
    </MemoryRouter>,
  );
  expect(await screen.findByRole("heading", {
    name: "This page could not be displayed",
  })).toBeInTheDocument();
  expect(screen.getByRole("button", { name: "Reload" })).toBeInTheDocument();
});
```

Import `lazy` from React, Router test helpers from `react-router-dom`, Testing Library, `userEvent`, the two boundary modules, and reporter types/functions. Every `console.error` spy is local, asserted where content matters, and restored by #773's suite cleanup.

- [ ] **Step 2: Run RED**

```bash
cd web
npm test -- src/__tests__/components/RecoveryPanel.test.tsx \
  src/__tests__/components/BrowserErrorBoundary.test.tsx
```

Expected: FAIL because all four production component modules are absent.

- [ ] **Step 3: Implement the status and recovery components**

Create `StartupStatus.tsx` as a full-screen `role="status"`, `aria-live="polite"` surface with a focusable heading and props `{ title: string; message: string }`. Create `RecoveryPanel.tsx` with this exact public contract and action behavior:

```tsx
export interface RecoveryPanelProps {
  title: string;
  message: string;
  errorId: string;
  homeHref: "/" | "/dev/" | "/prod/";
  onRetry?: () => void;
  onReload?: () => void;
}

export default function RecoveryPanel(props: RecoveryPanelProps): JSX.Element {
  const heading = useRef<HTMLHeadingElement>(null);
  useEffect(() => heading.current?.focus(), []);
  return (
    <section role="alert" aria-live="assertive"
      className="mx-auto max-w-xl rounded-xl border border-red-200 bg-white p-6 shadow-sm">
      <h1 ref={heading} tabIndex={-1} className="text-xl font-semibold text-slate-950">
        {props.title}
      </h1>
      <p className="mt-2 text-sm text-slate-600">{props.message}</p>
      <p className="mt-3 text-xs text-slate-500">
        Error ID: <code>{props.errorId}</code>
      </p>
      <div className="mt-5 flex flex-wrap gap-3">
        {props.onRetry ? <Button variant="primary" onClick={props.onRetry}>Retry</Button> : null}
        <Button onClick={props.onReload ?? (() => window.location.reload())}>Reload</Button>
        <a href={props.homeHref}
          className="inline-flex items-center rounded-lg border border-slate-200 px-3.5 py-2 text-sm font-medium text-slate-700">
          Home
        </a>
      </div>
    </section>
  );
}
```

Import `useEffect`, `useRef`, and `Button`. Do not add an `error`, `detail`, or `stack` prop.

- [ ] **Step 4: Implement root and route boundaries**

Create `BrowserErrorBoundary.tsx`:

```tsx
interface BrowserErrorBoundaryProps {
  scope: "root" | "route";
  title: string;
  message: string;
  homeHref: "/" | "/dev/" | "/prod/";
  resetKey?: string;
  onReload?: () => void;
  children: ReactNode;
}

interface BoundaryState { error: unknown | null; errorId: string | null }
const healthy: BoundaryState = { error: null, errorId: null };

export default class BrowserErrorBoundary extends Component<
  BrowserErrorBoundaryProps,
  BoundaryState
> {
  state: BoundaryState = healthy;

  static getDerivedStateFromError(error: unknown): BoundaryState {
    return { error, errorId: createBrowserErrorId() };
  }

  componentDidCatch(error: unknown, info: ErrorInfo): void {
    reportBrowserError(error, {
      id: this.state.errorId ?? createBrowserErrorId(),
      scope: this.props.scope,
      componentStack: info.componentStack,
    });
  }

  componentDidUpdate(previous: BrowserErrorBoundaryProps): void {
    if (this.state.error !== null && previous.resetKey !== this.props.resetKey) {
      this.setState(healthy);
    }
  }

  private readonly retry = (): void => this.setState(healthy);

  render(): ReactNode {
    if (this.state.error !== null && this.state.errorId !== null) {
      return <RecoveryPanel {...this.props} errorId={this.state.errorId} onRetry={this.retry} />;
    }
    return this.props.children;
  }
}
```

Destructure the fields passed to `RecoveryPanel` instead of spreading `children`, `scope`, or `resetKey` onto it. Import `Component`, `ErrorInfo`, `ReactNode`, the reporter functions, and `RecoveryPanel`.

Create `RouteRecoveryBoundary.tsx`:

```tsx
export default function RouteRecoveryBoundary({ children }: { children?: ReactNode }): JSX.Element {
  const location = useLocation();
  const homeHref = frontendHomeHref(getFrontendConfig().routePath);
  return (
    <BrowserErrorBoundary
      scope="route"
      resetKey={location.key}
      title="This page could not be displayed"
      message="Retry this page, reload after a deployment, or open a safe home page."
      homeHref={homeHref}
    >
      <Suspense fallback={
        <StartupStatus title="Loading page" message="Loading the selected Loom page…" />
      }>
        {children ?? <Outlet />}
      </Suspense>
    </BrowserErrorBoundary>
  );
}
```

- [ ] **Step 5: Verify GREEN and commit**

```bash
cd web
npm test -- src/__tests__/components/RecoveryPanel.test.tsx \
  src/__tests__/components/BrowserErrorBoundary.test.tsx
npm run typecheck
npm run lint
```

Expected: both files pass; the route changes to the healthy sibling; the lazy rejection renders a fallback; no secret appears in DOM or reporter data.

```bash
git add web/src/components/StartupStatus.tsx \
  web/src/components/RecoveryPanel.tsx \
  web/src/components/BrowserErrorBoundary.tsx \
  web/src/components/RouteRecoveryBoundary.tsx \
  web/src/__tests__/components/RecoveryPanel.test.tsx \
  web/src/__tests__/components/BrowserErrorBoundary.test.tsx
git commit -m "feat(web): contain root and route render failures (#783)"
```

## Task 4: Mount React Immediately and Make Config Retryable

**Files:**

- Modify: `web/index.html`
- Create: `web/src/FrontendBootstrap.tsx`
- Create: `web/src/__tests__/FrontendBootstrap.test.tsx`
- Modify: `web/src/main.tsx`

**Interfaces:**

- `FrontendBootstrap` accepts optional `loadConfig`, `queryClient`, and `onReload` seams for deterministic tests.
- The component remains under the root `StrictMode`; a per-attempt promise ref deduplicates Strict Effects without hiding bootstrap code from development checks.
- Config Retry increments an attempt counter and calls the same loader without navigating.

- [ ] **Step 1: Write RED bootstrap tests**

Create `FrontendBootstrap.test.tsx` with a deferred config promise that proves `Starting Loom` is present before resolution, then parameterize `/dev/settings` and `/prod/settings` over loaders that reject once with `FrontendConfigLoadError("http", { status: 500 })` or `FrontendConfigLoadError("invalid")`, resolve a valid config on Retry, and mock `/auth/me` as 401. Render the component inside `StrictMode`. Assert the initial Strict Effects cycle still calls the loader exactly once, the config-specific heading shows a `WEB-` identity with no exception text, Home is `/dev/` or `/prod/`, Retry makes exactly one additional loader call, and the signed-out Settings heading eventually renders.

- [ ] **Step 2: Run RED**

```bash
cd web
npm test -- src/__tests__/FrontendBootstrap.test.tsx
```

Expected: FAIL because `FrontendBootstrap` does not exist.

- [ ] **Step 3: Put a visible fallback in the HTML shell**

Replace `<div id="root"></div>` in `web/index.html` with:

```html
<div id="root">
  <main role="status" aria-live="polite"
    style="min-height:100vh;display:grid;place-items:center;padding:24px;font-family:system-ui,sans-serif;background:#f8fafc;color:#1e293b">
    <div>
      <h1 style="margin:0;font-size:20px">Starting Loom</h1>
      <p style="margin:8px 0 0">Loading frontend configuration…</p>
    </div>
  </main>
</div>
```

- [ ] **Step 4: Implement the bootstrap state machine**

Create `FrontendBootstrap.tsx` with state `{ status: "loading" } | { status: "ready"; config } | { status: "error"; errorId; error: FrontendConfigLoadError }`, a cancellation-safe effect keyed by `attempt`, and one promise per attempt:

```tsx
const requestRef = useRef<Promise<FrontendConfig> | null>(null);

useEffect(() => {
  let cancelled = false;
  setState({ status: "loading" });
  const request = requestRef.current ?? loadConfig();
  requestRef.current = request;
  void request.then(
    (config) => {
      if (!cancelled) setState({ status: "ready", config });
    },
    (error: unknown) => {
      if (cancelled) return;
      const configError =
        error instanceof FrontendConfigLoadError
          ? error
          : new FrontendConfigLoadError("network", { cause: error });
      const report = reportBrowserError(configError, { scope: "startup" });
      setState({ status: "error", error: configError, errorId: report.errorId });
    },
  );
  return () => {
    cancelled = true;
  };
}, [attempt, loadConfig]);

function retry(): void {
  requestRef.current = null;
  setAttempt((value) => value + 1);
}
```

The ready tree is:

```tsx
<BrowserErrorBoundary
  scope="root"
  title="Loom encountered an application error"
  message="Retry the application, reload after a deployment, or return home."
  homeHref={frontendHomeHref(config.routePath)}
  onReload={onReload}
>
  <QueryClientProvider client={client}>
    <BrowserRouter basename={config.routePath || undefined}>
      <AuthProvider><App /></AuthProvider>
    </BrowserRouter>
  </QueryClientProvider>
</BrowserErrorBoundary>
```

The loading branch renders `StartupStatus` with `Starting Loom` / `Loading frontend configuration…`. The catch branch calls `reportBrowserError(error, { scope: "startup" })` once and renders `RecoveryPanel` with title `Loom could not load its configuration`; use `The configuration service returned HTTP N.` only for typed HTTP failures and `The configuration response did not match this deployment.` for typed invalid failures. The Retry callback increments `attempt`; never render `error.message`.

Keep the existing QueryClient options in an exported `createAppQueryClient()` and create one owned client with `useState(createAppQueryClient)`.

- [ ] **Step 5: Reduce `main.tsx` to immediate mount**

```tsx
import ReactDOM from "react-dom/client";
import { StrictMode } from "react";
import FrontendBootstrap from "./FrontendBootstrap";
import "./index.css";

const rootElement = document.getElementById("root");
if (!rootElement) throw new Error("Loom root element is missing");
ReactDOM.createRoot(rootElement).render(
  <StrictMode>
    <FrontendBootstrap />
  </StrictMode>,
);
```

Remove the old promise chain and `innerHTML` fallback. The StrictMode regression test is required because duplicate initial config requests would be a bootstrap defect, not a reason to remove StrictMode.

- [ ] **Step 6: Verify GREEN and commit**

```bash
cd web
npm test -- src/__tests__/FrontendBootstrap.test.tsx \
  src/__tests__/frontend-config.test.ts
npm run typecheck
npm run lint
npm run build
```

Expected: tests pass, config Retry succeeds for both basenames, and the production build exits 0.

```bash
git add web/index.html web/src/main.tsx web/src/FrontendBootstrap.tsx \
  web/src/__tests__/FrontendBootstrap.test.tsx
git commit -m "fix(web): mount a recoverable startup shell (#783)"
```

## Task 5: Separate Signed-Out and Unavailable Session States

**Files:**

- Modify: `web/src/auth/authContextValue.ts`
- Modify: `web/src/auth/AuthContext.tsx`
- Modify: `web/src/pages/Settings.tsx`
- Modify: `web/src/components/Layout.tsx`
- Modify: `web/src/__tests__/AuthContext.test.tsx`
- Modify: `web/src/__tests__/components/Layout.test.tsx`

**Interfaces:**

- Adds `sessionStatus: AuthSessionStatus` and `sessionErrorId: string | null` to `AuthCtx`.
- Removes `authError`; transport details no longer flow into Settings.
- Retains derived `me`, `isAuthenticated`, `isLoading`, `isAdmin`, and all auth action signatures.

- [ ] **Step 1: Write RED auth and visible-layout tests**

Update the Auth test display to print `sessionStatus` and `sessionErrorId` and expose a `retry-session` button calling `refreshMe()`. Assert:

1. 200 produces `authenticated`.
2. 401 produces `signed-out`, no error ID, clears cached data, and emits no reporter event.
3. 503 with a secret-shaped detail produces `unavailable`, a `WEB-` ID, no secret in DOM/reporter, and preserves cached data.
4. After the 503, Retry with a 200 response produces `authenticated`.

In `Layout.test.tsx`, add a deferred `/auth/me` test that sees `Checking your session`, and a 503-then-200 test that sees `Loom could not verify your session`, does not see `Sign in`, clicks Retry, then sees `Team overview`.

- [ ] **Step 2: Run RED**

```bash
cd web
npm test -- src/__tests__/AuthContext.test.tsx \
  src/__tests__/components/Layout.test.tsx
```

Expected: FAIL because `sessionStatus`, `sessionErrorId`, and the visible session states are absent.

- [ ] **Step 3: Implement the session union**

Add to `authContextValue.ts`:

```ts
export type AuthSessionStatus =
  | "loading"
  | "authenticated"
  | "signed-out"
  | "unavailable";
```

Replace AuthProvider's three startup fields with:

```ts
type SessionState =
  | { status: "loading" }
  | { status: "signed-out" }
  | { status: "unavailable"; errorId: string }
  | { status: "authenticated"; me: AuthMe };

const [session, setSession] = useState<SessionState>({ status: "loading" });
const me = session.status === "authenticated" ? session.me : null;
```

Use one `installSession(next)` helper to set CSRF and `{ status: "authenticated", me: next }`. `refreshMe()` must set loading, install on success, clear CSRF and query cache then set signed-out on 401, and on every other failure clear CSRF, preserve query cache, call `reportBrowserError(error, { scope: "session" })`, then set unavailable with its ID. The global unauthorized handler and logout both clear CSRF/query cache and set signed-out. Login, invite, and team-switch success all call `installSession`.

Build context fields as:

```ts
sessionStatus: session.status,
sessionErrorId: session.status === "unavailable" ? session.errorId : null,
me,
isAuthenticated: session.status === "authenticated",
isLoading: session.status === "loading",
isAdmin: me?.is_platform_admin ?? false,
```

- [ ] **Step 4: Render session and route recovery in Layout**

Before redirect logic, render `StartupStatus` for `loading` and `RecoveryPanel` for `unavailable`, with title `Loom could not verify your session`, fixed service-unavailable copy, `onRetry={() => void refreshMe()}`, and `frontendHomeHref(getFrontendConfig().routePath)`. Replace both authenticated and public `<Outlet />` uses with `<RouteRecoveryBoundary />` so the nav/public shell survives a page failure.

Remove `authError` from Settings. Render failed sign-in through `<ErrorState error={signIn.error} />`, which preserves the existing redaction contract.

- [ ] **Step 5: Verify GREEN and commit**

```bash
cd web
npm test -- src/__tests__/AuthContext.test.tsx \
  src/__tests__/components/Layout.test.tsx \
  src/__tests__/pages/Settings.test.tsx
npm run typecheck
npm run lint
```

Expected: all named files pass; 401 and 503 follow distinct states; cached data survives 503; Retry restores the authenticated app.

```bash
git add web/src/auth/authContextValue.ts web/src/auth/AuthContext.tsx \
  web/src/pages/Settings.tsx web/src/components/Layout.tsx \
  web/src/__tests__/AuthContext.test.tsx \
  web/src/__tests__/components/Layout.test.tsx
git commit -m "fix(web): distinguish unavailable browser sessions (#783)"
```

## Task 6: Add Deterministic Post-#773 Playwright Recovery Coverage

**Files:**

- Modify after #773 merge: `web/e2e/fixtures/api.ts`
- Modify after #773 merge: `web/e2e/fixtures/guardedTest.ts`
- Create: `web/e2e/recovery.spec.ts`

**Interfaces:**

- Adds `RecoveryScenario = "healthy" | "config-delayed" | "config-500-once" | "config-invalid-once" | "auth-503-once" | "root-render-once" | "route-render-once"` to the deterministic API fixture.
- Adds a Playwright option `recoveryScenario`, defaulting to `healthy`.
- Only `config-500-once` receives one exact expected-response allowance for `GET */loom-frontend-config.json` status 500. `pageerror` and console errors can never be allowlisted.

- [ ] **Step 1: Write the recovery spec against the missing scenario option**

Create `web/e2e/recovery.spec.ts` with one `test.describe` per non-healthy scenario. Use the existing user role and assert:

```ts
test("a config 500 retries into the app", async ({ page }) => {
  await page.goto("/");
  await expect(page.getByRole("heading", {
    name: "Loom could not load its configuration",
  })).toBeVisible();
  await expect(page.getByText(/^Error ID: WEB-/)).toBeVisible();
  await page.getByRole("button", { name: "Retry" }).click();
  await expect(page.getByRole("heading", { name: "Team overview" })).toBeVisible();
});

test("an invalid config retries into the app", async ({ page }) => {
  await page.goto("/");
  await expect(page.getByText(
    "The configuration response did not match this deployment.",
  )).toBeVisible();
  await page.getByRole("button", { name: "Retry" }).click();
  await expect(page.getByRole("heading", { name: "Team overview" })).toBeVisible();
});

test("a session 503 is not presented as signed out", async ({ page }) => {
  await page.goto("/");
  await expect(page.getByRole("heading", {
    name: "Loom could not verify your session",
  })).toBeVisible();
  await expect(page.getByRole("heading", { name: /Sign in/ })).toHaveCount(0);
  await page.getByRole("button", { name: "Retry" }).click();
  await expect(page.getByRole("heading", { name: "Team overview" })).toBeVisible();
});

test("a root render failure remounts cleanly", async ({ page }) => {
  await page.goto("/");
  await expect(page.getByRole("heading", {
    name: "Loom encountered an application error",
  })).toBeVisible();
  await page.getByRole("button", { name: "Retry" }).click();
  await expect(page.getByRole("heading", { name: "Team overview" })).toBeVisible();
});

test("a route failure leaves a healthy sibling usable", async ({ page }) => {
  await page.goto("/");
  await expect(page.getByRole("heading", {
    name: "This page could not be displayed",
  })).toBeVisible();
  await page.getByRole("link", { name: "Monitor" }).click();
  await expect(page.getByRole("heading", { name: "Monitor" })).toBeVisible();
});
```

The delayed-config case asserts `Starting Loom` before releasing its response. Parameterize each describe with `test.use({ role: "user", recoveryScenario: "..." })`.

- [ ] **Step 2: Run RED**

```bash
cd web
npm run test:e2e -- e2e/recovery.spec.ts
```

Expected: FAIL at Playwright typecheck/fixture setup because `recoveryScenario` is unknown.

- [ ] **Step 3: Implement deterministic one-shot fixture responses**

Extend the existing closed API router, reusing its valid config/auth/overview bodies:

- `config-delayed`: hold the config response for 300 ms, then return the healthy config.
- `config-500-once`: first config request returns 500 with the fixed body `configuration unavailable`; subsequent config requests return healthy config.
- `config-invalid-once`: first config request returns JSON with `routePath: "/prod"` while the test is at root; subsequent requests return healthy config.
- `auth-503-once`: first `/api/v1/auth/me` returns 503 with `{ "detail": "session service unavailable" }`; subsequent requests return the healthy user.
- `root-render-once`: first auth response is HTTP 200 with `{ "csrf_token": null }`, which violates the typed AuthMe shape and makes Layout throw inside the root boundary; subsequent requests return the healthy user.
- `route-render-once`: first overview response copies the healthy object with `next_actions: null`, which throws in Home inside the route boundary; subsequent requests return the healthy overview.

Keep per-page counters inside fixture setup so tests cannot share state. Extend the guarded response ledger to consume exactly one config 500 for that scenario; fail if it is absent, repeated, or has another status. Do not relax the existing `console`, `pageerror`, `requestfailed`, MIME, static-resource, or non-empty-root checks.

- [ ] **Step 4: Verify GREEN and the full browser matrix**

```bash
cd web
npm run test:e2e -- e2e/recovery.spec.ts
npm run test:e2e
```

Expected: every recovery case passes in desktop and mobile Chromium; all injected failures are consumed exactly once; recovery reaches a healthy heading; there are zero uncaught page errors, unexpected console entries, resource failures, MIME failures, or empty-root failures.

- [ ] **Step 5: Commit**

```bash
git add web/e2e/fixtures/api.ts web/e2e/fixtures/guardedTest.ts \
  web/e2e/recovery.spec.ts
git commit -m "test(web): cover browser error recovery (#783)"
```

## Task 7: Document the Contract and Operator Triage Path

**Files:**

- Create: `docs/architecture/frontend-error-recovery.md`
- Modify: `docs/architecture/README.md`
- Modify: `docs/runbooks/operator-runbook.md`
- Modify after #773 merge: `docs/contributing/contributor-quickstart.md`

**Interfaces:**

- Documents the exact state table, boundary ownership, reporter schema, redaction boundary, #212 handoff, local commands, and operator evidence that is safe to collect.

- [ ] **Step 1: Create the architecture contract**

Write `docs/architecture/frontend-error-recovery.md` with these explicit facts:

- startup sequence is static HTML → React config status → configured root boundary → session status → route boundary;
- config errors distinguish network, HTTP status, and invalid response but never show bodies;
- session table maps 200 to authenticated, 401 to signed-out/cache clear, and every other failure to unavailable/cache preserve;
- root catches providers/router/layout while route catches the current Outlet and future lazy child;
- `location.key` resets route failure state;
- #212 owns all production lazy imports and consumes #783's `Suspense`/Reload contract;
- `BrowserErrorReport` contains exactly the seven fields in this plan and reporters never receive raw exceptions;
- production ships no telemetry transport; local development uses the redacted console reporter;
- Home, Retry, and Reload semantics for root, `/dev`, and `/prod`;
- exact focused commands for Vitest and `npm run test:e2e -- e2e/recovery.spec.ts`.

Link the new document from the Human-readable SPA section in `docs/architecture/README.md`.

- [ ] **Step 2: Add operator troubleshooting**

Add a row to the operator troubleshooting matrix for `SPA displays WEB-… recovery screen`. The first check must be: record candidate SHA/image, environment, pathname without query, timestamp, visible error ID, and which action recovered; fetch only status, MIME, and cache headers for `loom-frontend-config.json`; then run the canonical frontend route smoke. State explicitly that operators must not request or record cookies, CSRF values, browser storage, query strings, response bodies containing user/provider data, setup/reset/invite links, or signed URLs.

Add a `Frontend recovery` subsection explaining that a 401 is normal signed-out behavior, a session-unavailable screen is a service/config incident, and an ID can be correlated with a configured reporter only when such an adapter exists.

- [ ] **Step 3: Document the local check**

In `docs/contributing/contributor-quickstart.md`, add:

```bash
cd web
npm test -- src/__tests__/FrontendBootstrap.test.tsx \
  src/__tests__/AuthContext.test.tsx \
  src/__tests__/components/BrowserErrorBoundary.test.tsx
npm run test:e2e -- e2e/recovery.spec.ts
```

State that the E2E fault bodies are fixed, one-shot, local-only fixtures and that uncaught page errors are never allowed.

- [ ] **Step 4: Verify docs and commit**

```bash
test -f docs/architecture/frontend-error-recovery.md
rg -n 'BrowserErrorReport|location.key|#212|signed-out|unavailable|WEB-' \
  docs/architecture/frontend-error-recovery.md \
  docs/runbooks/operator-runbook.md
git diff --check
```

Expected: the file exists, every required contract term is found, and `git diff --check` prints nothing.

```bash
git add docs/architecture/frontend-error-recovery.md \
  docs/architecture/README.md docs/runbooks/operator-runbook.md \
  docs/contributing/contributor-quickstart.md
git commit -m "docs(web): document frontend recovery operations (#783)"
```

## Task 8: Run the Complete Gate and Fixed-Candidate Acceptance

**Files:** No additional repository files unless verification exposes a focused defect.

- [ ] **Step 1: Run the complete repository-side gate**

```bash
cd web
npm ci
npm run typecheck
npm run lint
npm test
npm run test:coverage
npm run build
npx playwright install chromium
npm run test:e2e
cd ..
git diff --check
git status --short
```

Expected: every command exits 0; coverage retains #773's 80% statement/line/function and 75% branch floors; no warning/network/unhandled guard fires; all browser projects report zero uncaught page errors; only intentional implementation and documentation files are modified.

- [ ] **Step 2: Open the implementation PR under repository rules**

Target `dev`, use `Refs #783`, add `ci:images`, and enable squash auto-merge immediately. Keep #783 open until current-head required checks and fixed-candidate staging acceptance pass. Do not claim #212 chunking or lazy-route acceptance in this PR.

- [ ] **Step 3: Run fixed-candidate staging acceptance**

On the exact merged `dev` SHA/web image at `https://yylx.world/dev/`, record only candidate identity, image digest, browser version, viewport, pathname without query, status/MIME/cache results, and pass/fail. Verify static startup content exists in the served shell, the configured app mounts, a signed-out session reaches Settings, an authenticated normal user reaches Home/Monitor, Retry/Reload/Home are keyboard reachable, and the sanitized browser guard records zero uncaught page errors. Do not inject a shared-staging 500 or malformed auth payload; those destructive cases are owned by the deterministic local Playwright scenarios.

For `/prod`, retain the unit-proven `/prod/` action contract and repeat the same live check when #486 supplies the production route. A missing production deployment is not a reason to add a fake `/prod` surface.

- [ ] **Step 4: Reconcile acceptance before closure**

Confirm current-head `repository-checks` includes successful selected `web-checks`, link sanitized staging evidence to #783, #493, and #715, and close #783 only when the original root/route/config/session recovery acceptance is green. If staging exposes a residual blank root, secret disclosure, broken action, or uncaught error, keep the issue open and record exact candidate/path/reproduction without sensitive data.

---

## Implementation Order and Review Gates

1. Task 1: typed config failure and basename contract.
2. Task 2: redacted reporter seam.
3. Task 3: shared UI, root boundary, route boundary, lazy-rejection contract.
4. Task 4: immediate static/React bootstrap and config Retry.
5. Task 5: auth state separation and Layout integration.
6. Task 6: post-#773 deterministic browser injection.
7. Task 7: architecture/operator/contributor docs.
8. Task 8: full checks, PR, and fixed-candidate evidence.

Each code commit is independently testable. Keep one #783 implementation PR unless a merge conflict with the still-landing #773 browser foundation requires the browser spec to follow in a second `Advances #783` PR; both PRs still target `dev`, carry `ci:images`, and leave #783 open until acceptance is complete.
