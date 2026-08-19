/**
 * Compile-time browser-harness marker. Vite `define` replaces
 * `__LOOM_BROWSER_TEST_BUILD__` with `true` only for the Playwright bundle
 * (`VITE_BROWSER_TEST_BUILD=true`). It must never be derived from URL, env
 * runtime config, or API state.
 *
 * Vite 8.0.16's dev transform can leave the identifier unbound. `typeof` does
 * not throw in that case, so the SPA still boots instead of sticking on the
 * static "Starting Loom…" shell.
 */
export const IS_BROWSER_TEST_BUILD: boolean =
  typeof __LOOM_BROWSER_TEST_BUILD__ === "boolean"
    ? __LOOM_BROWSER_TEST_BUILD__
    : false;

export type BrowserTestRecoveryFault =
  | "root-render-once"
  | "route-render-once";

const recoveryFaultKey = Symbol.for("loom.browser-test-recovery-fault");
const runtimeScope = globalThis as typeof globalThis &
  Record<PropertyKey, unknown>;

/**
 * Browser recovery faults are available only in the dedicated Playwright
 * bundle. The harness arms this symbol before navigation; no URL, runtime
 * config, API response, or production endpoint can enable it.
 *
 * Keep the fault armed until the boundary's explicit recovery path clears it.
 * React may replay a failed render before committing the fallback, so consuming
 * the value during the first render would make the injected failure flaky.
 */
export function shouldTriggerBrowserTestRecoveryFault(
  expected: BrowserTestRecoveryFault,
): boolean {
  return (
    IS_BROWSER_TEST_BUILD &&
    runtimeScope[recoveryFaultKey] === expected
  );
}

export function clearBrowserTestRecoveryFault(
  expected: BrowserTestRecoveryFault,
): void {
  if (
    IS_BROWSER_TEST_BUILD &&
    runtimeScope[recoveryFaultKey] === expected
  ) {
    delete runtimeScope[recoveryFaultKey];
  }
}
