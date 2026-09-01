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

export default defineConfig(({ command }) => ({
  plugins: [react()],
  // Recovery tests may consume this compile-time constant, but the normal
  // production build always substitutes false. Only build-browser-test.mjs
  // opts the local Playwright bundle into the test-only branch.
  define: {
    __LOOM_BROWSER_TEST_BUILD__: JSON.stringify(
      process.env.VITE_BROWSER_TEST_BUILD === "true",
    ),
  },
  // Production builds keep relative assets. The Vite 8.0.16 dev server with
  // `base: "./"` does not match `/api` proxy rules, so `/api/v1/auth/me`
  // 404s and the SPA shows "session service is temporarily unavailable".
  base: process.env.VITE_E2E_ROUTE_BASE ?? (command === "build" ? "./" : "/"),
  server: {
    port: 5173,
    proxy: {
      "/api": {
        target: apiProxyTarget,
        changeOrigin: true,
      },
    },
    // Vite registers its raw ErrorEvent forwarder before the application
    // entrypoint. Disable that channel so root-boundary failures cannot be
    // serialized to an agent/dev terminal before our capture listener redacts
    // them. Bounded console.error reports remain forwarded for diagnostics.
    forwardConsole: {
      unhandledErrors: false,
      logLevels: ["error", "warn"],
    },
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
        "src/components/artifacts/BehaviorRollout*.tsx",
        "src/components/artifacts/useBoundedJson.ts",
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
}));
