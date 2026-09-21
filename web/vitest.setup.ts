import { beforeEach } from "vitest";

import "@testing-library/jest-dom/vitest";
import "./src/test-utils/qualityGuards";
import { setBrowserFailureReporter } from "./src/lib/errorReporting";

// Node 22+ may expose an inert window.localStorage unless
// --localstorage-file is set; happy-dom then leaves it undefined.
if (
  typeof window !== "undefined" &&
  (window.localStorage === undefined ||
    typeof window.localStorage?.clear !== "function")
) {
  const store = new Map<string, string>();
  Object.defineProperty(window, "localStorage", {
    configurable: true,
    value: {
      get length() {
        return store.size;
      },
      clear: () => store.clear(),
      getItem: (key: string) => store.get(key) ?? null,
      key: (index: number) => Array.from(store.keys())[index] ?? null,
      removeItem: (key: string) => {
        store.delete(key);
      },
      setItem: (key: string, value: string) => {
        store.set(key, String(value));
      },
    } satisfies Storage,
  });
}

// The production entrypoint (main.tsx) installs a real browser-failure reporter.
// Mirror that default in tests so expected reports go to a sink instead of the
// DEV console.error fallback (import.meta.env.DEV is true under vitest), which
// the shared quality guard would otherwise treat as unhandled console output.
// Tests that assert reporting install their own reporter in the test body.
beforeEach(() => {
  setBrowserFailureReporter(() => undefined);
});
