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
