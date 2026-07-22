import { afterEach, beforeEach, vi } from "vitest";

type ConsoleMethod = "error" | "warn";

let consoleFailures: string[] = [];
let unhandledRejections: string[] = [];
let consoleSpies: Array<ReturnType<typeof vi.spyOn>> = [];
let allowedRejectionMatchers: Array<(reason: unknown) => boolean> = [];

/**
 * Acknowledge a single deliberately-dispatched unhandled rejection (e.g. a test
 * that verifies the app's rejection-redaction path). The next matching rejection
 * is consumed instead of failing the guard. Opt-in per test; the default guard
 * behavior for genuinely unexpected rejections is unchanged.
 */
export function allowExpectedUnhandledRejection(
  matches: (reason: unknown) => boolean = () => true,
): void {
  allowedRejectionMatchers.push(matches);
}

function safeMessage(value: unknown): string {
  if (value instanceof Error) return `${value.name}: ${value.message}`;
  if (typeof value === "string") return value;
  return Object.prototype.toString.call(value);
}

function safeRequestPath(input: RequestInfo | URL): string {
  const raw = input instanceof Request ? input.url : String(input);
  try {
    const base = typeof window === "undefined" ? "http://unit.test" : window.location.origin;
    const parsed = new URL(raw, base);
    return `${parsed.origin}${parsed.pathname}`;
  } catch {
    return "<invalid-url>";
  }
}

function captureConsole(method: ConsoleMethod): void {
  consoleSpies.push(
    vi.spyOn(console, method).mockImplementation((...args: unknown[]) => {
      consoleFailures.push(`${method}: ${args.map(safeMessage).join(" ")}`);
    }),
  );
}

function onUnhandledRejection(event: PromiseRejectionEvent): void {
  event.preventDefault();
  const index = allowedRejectionMatchers.findIndex((matches) =>
    matches(event.reason),
  );
  if (index >= 0) {
    allowedRejectionMatchers.splice(index, 1);
    return;
  }
  unhandledRejections.push(safeMessage(event.reason));
}

beforeEach(() => {
  consoleFailures = [];
  unhandledRejections = [];
  consoleSpies = [];
  allowedRejectionMatchers = [];
  captureConsole("error");
  captureConsole("warn");
  if (typeof window !== "undefined") {
    window.addEventListener("unhandledrejection", onUnhandledRejection);
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL) => {
        throw new Error(`unexpected unit-test network request: ${safeRequestPath(input)}`);
      }),
    );
  }
});

afterEach(() => {
  if (typeof window !== "undefined") {
    window.removeEventListener("unhandledrejection", onUnhandledRejection);
  }
  for (const spy of consoleSpies) spy.mockRestore();
  vi.unstubAllGlobals();

  const failures = [
    ...consoleFailures.map((message) => `unexpected console output: ${message}`),
    ...unhandledRejections.map((message) => `unhandled rejection: ${message}`),
  ];
  if (failures.length > 0) {
    throw new Error(failures.join("\n"));
  }
});
