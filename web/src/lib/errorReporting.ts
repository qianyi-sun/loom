import { redactText } from "./redaction";

export type BrowserFailureKind =
  | "frontend-config-network"
  | "frontend-config-http"
  | "frontend-config-invalid"
  | "root-render";

export interface BrowserFailureReport {
  referenceId: string;
  kind: BrowserFailureKind;
  pathname: string;
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
const capturedObjects = new WeakMap<object, number>();
const capturedPrimitives = new Map<unknown, number>();

const REACT_ERROR_REDACTION = "Loom browser error details redacted.";

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

function incrementCapturedFailure(value: unknown): void {
  if ((typeof value === "object" && value !== null) || typeof value === "function") {
    const objectValue = value as object;
    capturedObjects.set(objectValue, (capturedObjects.get(objectValue) ?? 0) + 1);
    return;
  }
  capturedPrimitives.set(value, (capturedPrimitives.get(value) ?? 0) + 1);
}

function consumeCapturedFailure(value: unknown): boolean {
  if ((typeof value === "object" && value !== null) || typeof value === "function") {
    const objectValue = value as object;
    const count = capturedObjects.get(objectValue);
    if (!count) return false;
    if (count === 1) capturedObjects.delete(objectValue);
    else capturedObjects.set(objectValue, count - 1);
    return true;
  }

  const count = capturedPrimitives.get(value);
  if (!count) return false;
  if (count === 1) capturedPrimitives.delete(value);
  else capturedPrimitives.set(value, count - 1);
  return true;
}

/**
 * React 18 logs a captured boundary value directly to console.error before it
 * calls componentDidCatch. Mark that exact value during render so the console
 * bridge can replace React's one diagnostic without muting unrelated logs.
 */
export function markBrowserFailureForConsoleRedaction(value: unknown): void {
  incrementCapturedFailure(value);
}

export function installBrowserConsoleErrorRedaction(): () => void {
  const original = console.error;
  const wrapped: typeof console.error = (...args: unknown[]) => {
    if (args.some((value) => consumeCapturedFailure(value))) {
      original.call(console, REACT_ERROR_REDACTION);
      return;
    }
    original.apply(console, args);
  };
  console.error = wrapped;

  return () => {
    if (console.error === wrapped) console.error = original;
  };
}

export function reportBrowserFailure(
  kind: BrowserFailureKind,
  referenceId: string,
  locationLike: Pick<Location, "pathname"> = window.location,
): void {
  const report: BrowserFailureReport = Object.freeze({
    referenceId,
    kind,
    pathname: redactedPathname(locationLike),
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
