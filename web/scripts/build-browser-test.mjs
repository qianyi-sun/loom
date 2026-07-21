#!/usr/bin/env node

import { spawn } from "node:child_process";

import { readBrowserHarnessConfig } from "./browser-harness-config.mjs";

const config = readBrowserHarnessConfig();
const vite = spawn(process.execPath, ["node_modules/vite/bin/vite.js", "build"], {
  env: {
    ...process.env,
    VITE_E2E_ROUTE_BASE: `${config.routePrefix}/`,
    VITE_BROWSER_TEST_BUILD: "true",
  },
  stdio: "inherit",
});

vite.on("error", (error) => {
  process.stderr.write(`failed to launch browser-test build: ${error.message}\n`);
  process.exitCode = 1;
});

vite.on("exit", (code, signal) => {
  if (signal) {
    process.stderr.write(`browser-test build terminated by ${signal}\n`);
    process.exitCode = 1;
    return;
  }
  process.exitCode = code ?? 1;
});
