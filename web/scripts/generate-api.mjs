import { execFileSync } from "node:child_process";
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const web = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const root = resolve(web, "..");
const temporary = mkdtempSync(join(tmpdir(), "loom-openapi-"));
const python = process.env.PYTHON ?? (existsSync(join(root, ".venv/bin/python")) ? join(root, ".venv/bin/python") : "python3");
try {
  const spec = join(temporary, "openapi.json");
  const generated = join(temporary, "schema.d.ts");
  execFileSync(python, [join(root, "scripts/export_openapi.py"), spec], {
    cwd: root, env: { ...process.env, PYTHONPATH: join(root, "src") }, stdio: "pipe",
  });
  execFileSync(process.execPath, [join(web, "node_modules/openapi-typescript/bin/cli.js"), spec, "-o", generated], { cwd: web, stdio: "pipe" });
  const output = readFileSync(generated, "utf8");
  const target = join(web, "src/api/schema.d.ts");
  if (process.argv.includes("--check")) {
    if (readFileSync(target, "utf8") !== output) {
      throw new Error("API types differ from the backend contract. Run npm run gen-api.");
    }
    console.log("API types match the offline backend contract.");
  } else {
    writeFileSync(target, output);
    console.log("Generated API types from the offline backend contract.");
  }
} finally {
  rmSync(temporary, { recursive: true, force: true });
}
