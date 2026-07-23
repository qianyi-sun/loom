#!/usr/bin/env node

import { readdir, readFile } from "node:fs/promises";
import { extname, resolve } from "node:path";
import { pathToFileURL } from "node:url";

const FORBIDDEN_BROWSER_TEST_MARKERS = [
  "loom.browser-test-recovery-fault",
  "browser-test-only root render fault",
  "browser-test-only route render fault",
  "root-render-once",
  "route-render-once",
];

async function emittedFiles(directory) {
  const entries = await readdir(directory, {
    recursive: true,
    withFileTypes: true,
  });
  return entries
    .filter((entry) => entry.isFile())
    .map((entry) => resolve(entry.parentPath, entry.name))
    .filter((path) => [".html", ".js", ".mjs"].includes(extname(path)));
}

export async function verifyProductionBuild(directory = "dist") {
  const buildDirectory = resolve(directory);
  const files = await emittedFiles(buildDirectory);
  if (files.length === 0) {
    throw new Error("production build verification found no emitted assets");
  }

  for (const file of files) {
    const content = await readFile(file, "utf8");
    for (const marker of FORBIDDEN_BROWSER_TEST_MARKERS) {
      if (content.includes(marker)) {
        throw new Error(
          `production build contains browser-test recovery marker in ${file}`,
        );
      }
    }
  }
}

if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(resolve(process.argv[1])).href
) {
  verifyProductionBuild(process.argv[2]).catch((error) => {
    process.stderr.write(`${error.message}\n`);
    process.exitCode = 1;
  });
}
