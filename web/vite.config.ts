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
  base: "./",
  server: {
    port: 5173,
    proxy: { "/api": apiProxyTarget },
  },
  test: {
    environment: "happy-dom",
    setupFiles: ["./vitest.setup.ts"],
    globals: true,
  },
});
