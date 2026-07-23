/// <reference types="node" />

import { spawnSync } from "node:child_process";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, describe, expect, it } from "vitest";

let buildDirectory: string | undefined;

afterEach(async () => {
  if (buildDirectory) {
    await rm(buildDirectory, { force: true, recursive: true });
    buildDirectory = undefined;
  }
});

async function emittedBuild(source: string): Promise<string> {
  buildDirectory = await mkdtemp(join(tmpdir(), "loom-production-build-"));
  const assets = join(buildDirectory, "assets");
  await mkdir(assets);
  await writeFile(join(buildDirectory, "index.html"), "<main>Loom</main>");
  await writeFile(join(assets, "index.js"), source);
  return buildDirectory;
}

function runVerifier(directory: string) {
  return spawnSync(
    process.execPath,
    ["scripts/verify-production-build.mjs", directory],
    { encoding: "utf8" },
  );
}

describe("production build verification", () => {
  it("accepts a build without browser-test recovery markers", async () => {
    const result = runVerifier(
      await emittedBuild("console.log('loom');"),
    );

    expect(result.status, result.stderr).toBe(0);
  });

  it("rejects a build containing a browser-test recovery marker", async () => {
    const result = runVerifier(
      await emittedBuild("'loom.browser-test-recovery-fault'"),
    );

    expect(result.status).toBe(1);
    expect(result.stderr).toContain(
      "production build contains browser-test recovery marker",
    );
  });
});
