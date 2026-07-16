/**
 * Compile-time browser-harness marker. Normal production builds replace this
 * with `false`; only the local Playwright build command replaces it with
 * `true`. It must never be derived from runtime state.
 */
export const IS_BROWSER_TEST_BUILD: boolean = __LOOM_BROWSER_TEST_BUILD__;
