import { beforeEach } from "vitest";

import "@testing-library/jest-dom/vitest";
import "./src/test-utils/qualityGuards";
import { setBrowserFailureReporter } from "./src/lib/errorReporting";

// The production entrypoint (main.tsx) installs a real browser-failure reporter.
// Mirror that default in tests so expected reports go to a sink instead of the
// DEV console.error fallback (import.meta.env.DEV is true under vitest), which
// the shared quality guard would otherwise treat as unhandled console output.
// Tests that assert reporting install their own reporter in the test body.
beforeEach(() => {
  setBrowserFailureReporter(() => undefined);
});
