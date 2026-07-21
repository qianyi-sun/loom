/// <reference types="vitest" />
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// `/api` is proxied to loom_service. The host varies by where Vite
// runs:
//   - On the developer's host (`cd web && npm run dev`):
//       http://localhost:8090 — the published port from the
//       loom-service compose service.
//   - Inside the dev-compose `web` container:
//       http://loom-service:8090 — the in-network DNS name.
// Default matches the host case; docker-compose sets
// VITE_API_PROXY_TARGET to override.
const apiProxyTarget =
  process.env.VITE_API_PROXY_TARGET ?? "http://localhost:8090";

export default defineConfig({
  plugins: [react()],
  // Recovery tests may consume this compile-time constant, but the normal
  // production build always substitutes false. Only build-browser-test.mjs
  // opts the local Playwright bundle into the test-only branch.
  define: {
    __LOOM_BROWSER_TEST_BUILD__: JSON.stringify(
      process.env.VITE_BROWSER_TEST_BUILD === "true",
    ),
  },
  // Keep the shipped relative-asset contract by default. The browser quality
  // harness supplies an explicit prefix so deep-route reloads exercise the
  // same production build without depending on a live deployment.
  base: process.env.VITE_E2E_ROUTE_BASE ?? "./",
  server: {
    port: 5173,
    proxy: { "/api": apiProxyTarget },
  },
  test: {
    environment: "happy-dom",
    setupFiles: ["./vitest.setup.ts"],
    globals: true,
    exclude: ["e2e/**", "node_modules/**", "dist/**"],
    coverage: {
      provider: "v8",
      reporter: ["text", "json-summary", "lcov"],
      include: ["src/**/*.{ts,tsx}"],
      exclude: [
        "src/api/schema.d.ts",
        "src/__tests__/**",
        "src/test-utils/**",
      ],
      thresholds: {
        statements: 80,
        lines: 80,
        functions: 80,
        branches: 75,
      },
    },
  },
});
