import { redactText } from "./redaction";

export type BrowserFailureKind =
  | "frontend-config-network"
  | "frontend-config-http"
  | "frontend-config-invalid"
  | "root-render"
  | "route-render"
  | "uncaught-runtime"
  | "unhandled-rejection";

export interface BrowserFailureReport {
  referenceId: string;
  kind: BrowserFailureKind;
  pathname: string;
  sourcePath?: string;
  line?: number;
  column?: number;
}

export type BrowserFailureReporter = (report: BrowserFailureReport) => void;

function defaultReporter(report: BrowserFailureReport): void {
  const env = (import.meta as unknown as { env?: { DEV?: boolean } }).env;
  if (env?.DEV === true) {
    // The report is already bounded and redacted. Never attach the raw Error,
    // component stack, query string, hash, or a response body here.
    console.error("Loom browser failure", report);
  }
}

let reporter: BrowserFailureReporter = defaultReporter;
const redactionStateKey = Symbol.for("loom.browser-redaction-state");
const failureContextKey = Symbol.for("loom.browser-failure-context");
const consoleInstallationKey = Symbol.for(
  "loom.browser-console-redaction-installation",
);
const errorEventInstallationKey = Symbol.for(
  "loom.browser-error-event-redaction-installation",
);

interface BrowserRedactionState {
  capturedObjects: WeakSet<object>;
  capturedPrimitives: Set<unknown>;
  pendingObjectContexts: WeakMap<object, BrowserFailureContext>;
  pendingPrimitiveContexts: Map<unknown, BrowserFailureContext>;
}

interface SharedInstallation {
  cleanup: () => void;
  users: number;
}

const runtimeScope = globalThis as typeof globalThis &
  Record<PropertyKey, unknown>;
const redactionState =
  (runtimeScope[redactionStateKey] as BrowserRedactionState | undefined) ??
  {
    capturedObjects: new WeakSet<object>(),
    capturedPrimitives: new Set<unknown>(),
    pendingObjectContexts: new WeakMap<object, BrowserFailureContext>(),
    pendingPrimitiveContexts: new Map<unknown, BrowserFailureContext>(),
  };
redactionState.pendingObjectContexts ??=
  new WeakMap<object, BrowserFailureContext>();
redactionState.pendingPrimitiveContexts ??=
  new Map<unknown, BrowserFailureContext>();
runtimeScope[redactionStateKey] = redactionState;
const {
  capturedObjects,
  capturedPrimitives,
  pendingObjectContexts,
  pendingPrimitiveContexts,
} = redactionState;

interface BrowserFailureContext {
  allowSafeUncaughtSignal: boolean;
  claimedByBoundary: boolean;
  referenceId: string;
  settled: boolean;
}

interface BrowserFailureDiagnostics {
  sourcePath?: string;
  line?: number;
  column?: number;
}

type BrowserFailureCarrier = object & {
  [failureContextKey]?: BrowserFailureContext;
};

export type BrowserFailureUnhandledSignaler = (
  kind: "uncaught-runtime" | "unhandled-rejection",
  error: Error,
) => void;

export const BROWSER_ERROR_REDACTION =
  "Loom browser error details redacted.";
const BOUNDARY_CLAIM_WINDOW_MS = 50;

function defaultUnhandledSignaler(
  kind: "uncaught-runtime" | "unhandled-rejection",
  error: Error,
): void {
  window.setTimeout(() => {
    if (kind === "uncaught-runtime") throw error;
    void Promise.reject(error);
  }, 0);
}

let unhandledSignaler: BrowserFailureUnhandledSignaler =
  defaultUnhandledSignaler;

function retainSharedInstallation(
  key: symbol,
  createCleanup: () => () => void,
): () => void {
  let installation = runtimeScope[key] as SharedInstallation | undefined;
  if (!installation) {
    installation = { cleanup: createCleanup(), users: 0 };
    runtimeScope[key] = installation;
  }
  installation.users += 1;
  let released = false;

  return () => {
    if (released) return;
    released = true;
    installation.users -= 1;
    if (installation.users === 0 && runtimeScope[key] === installation) {
      installation.cleanup();
      delete runtimeScope[key];
    }
  };
}

function redactedPathname(locationLike: Pick<Location, "pathname">): string {
  return redactText(locationLike.pathname).slice(0, 512) || "/";
}

export function createBrowserFailureId(): string {
  const bytes = new Uint8Array(4);
  globalThis.crypto.getRandomValues(bytes);
  const suffix = Array.from(bytes, (value) =>
    value.toString(16).padStart(2, "0"),
  )
    .join("")
    .toUpperCase();
  return `WEB-${suffix}`;
}

function isObjectLike(value: unknown): value is object {
  return (
    (typeof value === "object" && value !== null) ||
    typeof value === "function"
  );
}

function defineRedactedProperty(
  target: object,
  key: PropertyKey,
  value: unknown,
): boolean {
  try {
    Object.defineProperty(target, key, {
      configurable: true,
      enumerable: false,
      value,
      writable: true,
    });
    return true;
  } catch {
    return false;
  }
}

function createRedactedError(
  context: BrowserFailureContext,
  includeReference = false,
): Error {
  const message = includeReference
    ? `${BROWSER_ERROR_REDACTION} Reference ${context.referenceId}.`
    : BROWSER_ERROR_REDACTION;
  const error = new Error(message);
  defineRedactedProperty(error, "name", "Error");
  defineRedactedProperty(error, "stack", `Error: ${message}`);
  defineRedactedProperty(error, failureContextKey, context);
  return error;
}

function failureContext(value: unknown): BrowserFailureContext | null {
  if (!isObjectLike(value)) return null;
  try {
    return (value as BrowserFailureCarrier)[failureContextKey] ?? null;
  } catch {
    return null;
  }
}

function createFailureContext(): BrowserFailureContext {
  return {
    allowSafeUncaughtSignal: false,
    claimedByBoundary: false,
    referenceId: createBrowserFailureId(),
    settled: false,
  };
}

function pendingFailureContext(value: unknown): BrowserFailureContext {
  if (isObjectLike(value)) {
    const existing = pendingObjectContexts.get(value);
    if (existing) return existing;
    const context = createFailureContext();
    pendingObjectContexts.set(value, context);
    return context;
  }

  // Primitive throwables cannot carry the context symbol. Keep only the most
  // recent context long enough for a synchronous React boundary claim, but do
  // not merge separate browser events that happen to throw the same value.
  const context = createFailureContext();
  pendingPrimitiveContexts.set(value, context);
  return context;
}

function clearPendingFailureContext(
  value: unknown,
  context: BrowserFailureContext,
): void {
  if (isObjectLike(value)) {
    if (pendingObjectContexts.get(value) === context) {
      pendingObjectContexts.delete(value);
    }
  } else if (pendingPrimitiveContexts.get(value) === context) {
    pendingPrimitiveContexts.delete(value);
  }
}

function existingPendingFailureContext(
  value: unknown,
): BrowserFailureContext | null {
  if (isObjectLike(value)) {
    return pendingObjectContexts.get(value) ?? null;
  }
  return pendingPrimitiveContexts.get(value) ?? null;
}

function boundedPosition(value: number): number | undefined {
  if (!Number.isSafeInteger(value) || value < 0) return undefined;
  return Math.min(value, 1_000_000);
}

function browserErrorDiagnostics(event: ErrorEvent): BrowserFailureDiagnostics {
  const diagnostics: BrowserFailureDiagnostics = {};
  if (event.filename) {
    try {
      const sourceUrl = new URL(event.filename, window.location.href);
      if (sourceUrl.origin === window.location.origin) {
        diagnostics.sourcePath = redactedPathname({
          pathname: sourceUrl.pathname,
        });
      }
    } catch {
      // An invalid or cross-origin source is omitted instead of copied into the
      // bounded diagnostic report.
    }
  }
  diagnostics.line = boundedPosition(event.lineno);
  diagnostics.column = boundedPosition(event.colno);
  return diagnostics;
}

/**
 * Replace the fields browsers and React normally serialize from a throwable.
 * The console bridge below remains the fail-closed fallback for frozen errors,
 * custom enumerable properties, and primitive values.
 */
export function redactBrowserThrowable(value: unknown): void {
  if (!isObjectLike(value)) return;
  defineRedactedProperty(value, "name", "Error");
  defineRedactedProperty(value, "message", BROWSER_ERROR_REDACTION);
  defineRedactedProperty(value, "stack", `Error: ${BROWSER_ERROR_REDACTION}`);
  defineRedactedProperty(value, "cause", undefined);
}

export function markBrowserFailureForConsoleRedaction(value: unknown): void {
  if (isObjectLike(value)) {
    capturedObjects.add(value as object);
    return;
  }
  capturedPrimitives.add(value);
}

export function clearBrowserFailureConsoleRedaction(value: unknown): void {
  // Objects that have carried a browser failure remain tainted. WeakSet does
  // not retain them, and keeping membership prevents later console calls from
  // exposing custom fields that could not be safely enumerated or rewritten.
  if (isObjectLike(value)) {
    return;
  }
  capturedPrimitives.delete(value);
}

export function prepareBrowserFailureForBoundary(error: unknown): string {
  redactBrowserThrowable(error);
  markBrowserFailureForConsoleRedaction(error);
  const context =
    failureContext(error) ??
    existingPendingFailureContext(error) ??
    createFailureContext();
  context.claimedByBoundary = true;
  if (isObjectLike(error)) {
    defineRedactedProperty(error, failureContextKey, context);
  }
  return context.referenceId;
}

function isCapturedFailure(value: unknown): boolean {
  if (isObjectLike(value)) {
    return capturedObjects.has(value as object);
  }
  return capturedPrimitives.has(value);
}

/**
 * React 18 production logs a captured boundary value directly to console.error
 * before componentDidCatch. Primitive markers are released after the capture
 * window. Object markers remain in a WeakSet because the same Error can retain
 * secret-bearing custom fields and WeakSet membership does not retain it.
 */
export function installBrowserConsoleErrorRedaction(): () => void {
  return retainSharedInstallation(consoleInstallationKey, () => {
    const original = console.error;
    const wrapped: typeof console.error = (...args: unknown[]) => {
      if (args.some((value) => isCapturedFailure(value))) {
        original.call(console, BROWSER_ERROR_REDACTION);
        return;
      }
      original.apply(console, args);
    };
    console.error = wrapped;

    return () => {
      if (console.error === wrapped) console.error = original;
    };
  });
}

/**
 * React 18 development first reports render failures through a browser
 * ErrorEvent, before an error boundary's getDerivedStateFromError runs. Capture
 * that event before React's per-render listener, replace its payload, and
 * prevent the browser's raw default diagnostic. Vite's earlier raw-event
 * forwarding is disabled separately in vite.config.ts. Propagation
 * intentionally continues so React can still deliver the safe Error to the
 * boundary.
 */
export function installBrowserErrorEventRedaction(): () => void {
  return retainSharedInstallation(errorEventInstallationKey, () => {
    const pendingReports = new Set<number>();

    const scheduleUnhandledReport = (
      kind: "uncaught-runtime" | "unhandled-rejection",
      context: BrowserFailureContext,
      rawValue: unknown,
      pathname: string,
      diagnostics: BrowserFailureDiagnostics = {},
    ): void => {
      const timer = window.setTimeout(() => {
        pendingReports.delete(timer);
        clearBrowserFailureConsoleRedaction(rawValue);
        clearPendingFailureContext(rawValue, context);
        if (context.settled) return;
        context.settled = true;
        if (!context.claimedByBoundary) {
          reportBrowserFailure(
            kind,
            context.referenceId,
            { pathname },
            diagnostics,
          );
          context.allowSafeUncaughtSignal = true;
          unhandledSignaler(kind, createRedactedError(context, true));
        }
      }, BOUNDARY_CLAIM_WINDOW_MS);
      pendingReports.add(timer);
    };

    const redactEvent = (event: ErrorEvent): void => {
      if (event.error === null || event.error === undefined) return;
      if (failureContext(event.error)?.allowSafeUncaughtSignal) return;

      const rawError = event.error as unknown;
      const diagnostics = browserErrorDiagnostics(event);
      const pathname = window.location.pathname;
      const context = pendingFailureContext(rawError);
      redactBrowserThrowable(rawError);
      markBrowserFailureForConsoleRedaction(rawError);
      if (isObjectLike(rawError)) {
        defineRedactedProperty(rawError, failureContextKey, context);
      }
      const redactedError = createRedactedError(context);
      defineRedactedProperty(event, "error", redactedError);
      defineRedactedProperty(event, "message", BROWSER_ERROR_REDACTION);
      defineRedactedProperty(event, "filename", "");
      defineRedactedProperty(event, "lineno", 0);
      defineRedactedProperty(event, "colno", 0);
      event.preventDefault();
      console.error(BROWSER_ERROR_REDACTION);
      scheduleUnhandledReport(
        "uncaught-runtime",
        context,
        rawError,
        pathname,
        diagnostics,
      );
    };

    const redactRejection = (event: PromiseRejectionEvent): void => {
      if (failureContext(event.reason)?.allowSafeUncaughtSignal) return;
      const pathname = window.location.pathname;
      const rawReason = event.reason as unknown;
      const context = pendingFailureContext(rawReason);
      redactBrowserThrowable(rawReason);
      markBrowserFailureForConsoleRedaction(rawReason);
      defineRedactedProperty(event, "reason", createRedactedError(context));
      event.preventDefault();
      console.error(BROWSER_ERROR_REDACTION);
      scheduleUnhandledReport(
        "unhandled-rejection",
        context,
        rawReason,
        pathname,
      );
    };

    window.addEventListener("error", redactEvent, { capture: true });
    window.addEventListener("unhandledrejection", redactRejection, {
      capture: true,
    });
    return () => {
      window.removeEventListener("error", redactEvent, { capture: true });
      window.removeEventListener("unhandledrejection", redactRejection, {
        capture: true,
      });
      for (const timer of pendingReports) window.clearTimeout(timer);
      pendingReports.clear();
    };
  });
}

export function reportBrowserFailure(
  kind: BrowserFailureKind,
  referenceId: string,
  locationLike: Pick<Location, "pathname"> = window.location,
  diagnostics: BrowserFailureDiagnostics = {},
): void {
  const report: BrowserFailureReport = Object.freeze({
    referenceId,
    kind,
    pathname: redactedPathname(locationLike),
    ...(diagnostics.sourcePath
      ? { sourcePath: diagnostics.sourcePath }
      : {}),
    ...(diagnostics.line === undefined ? {} : { line: diagnostics.line }),
    ...(diagnostics.column === undefined
      ? {}
      : { column: diagnostics.column }),
  });
  try {
    reporter(report);
  } catch {
    // Telemetry must never turn a recoverable browser failure into another
    // render failure. The UI reference remains available to support.
  }
}

export function setBrowserFailureReporter(
  next: BrowserFailureReporter | null,
): void {
  reporter = next ?? defaultReporter;
}

export function setBrowserFailureUnhandledSignaler(
  next: BrowserFailureUnhandledSignaler | null,
): void {
  unhandledSignaler = next ?? defaultUnhandledSignaler;
}
