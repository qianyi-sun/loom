#!/usr/bin/env node

import { createReadStream, existsSync, statSync } from "node:fs";
import { createServer } from "node:http";
import { extname, join, normalize, resolve } from "node:path";

const host = "127.0.0.1";
const port = Number(process.env.PORT ?? "4173");
const prefix = "/dev";
const dist = resolve("dist");

const mimeTypes = new Map([
  [".css", "text/css; charset=utf-8"],
  [".html", "text/html; charset=utf-8"],
  [".js", "text/javascript; charset=utf-8"],
  [".json", "application/json; charset=utf-8"],
  [".map", "application/json; charset=utf-8"],
  [".svg", "image/svg+xml"],
]);

function sendFile(response, filePath) {
  response.writeHead(200, {
    "Content-Type": mimeTypes.get(extname(filePath)) ?? "application/octet-stream",
    "Cache-Control": "no-store",
  });
  createReadStream(filePath).pipe(response);
}

function localFile(pathname) {
  const relative = normalize(pathname.slice(`${prefix}/`.length));
  const candidate = resolve(join(dist, relative));
  if (!candidate.startsWith(`${dist}/`) || !existsSync(candidate)) return null;
  return statSync(candidate).isFile() ? candidate : null;
}

createServer((request, response) => {
  const url = new URL(request.url ?? "/", `http://${host}:${port}`);
  if (url.pathname === prefix) {
    response.writeHead(308, { Location: `${prefix}/` });
    response.end();
    return;
  }
  if (!url.pathname.startsWith(`${prefix}/`)) {
    response.writeHead(404, { "Content-Type": "text/plain; charset=utf-8" });
    response.end("not found");
    return;
  }
  if (url.pathname === `${prefix}/loom-frontend-config.json`) {
    response.writeHead(200, {
      "Content-Type": "application/json; charset=utf-8",
      "Cache-Control": "no-store",
    });
    response.end(JSON.stringify({
      environment: "local",
      environmentLabel: "Local browser quality gate",
      routePath: prefix,
      apiBase: prefix,
      apiRouteBase: `http://${host}:${port}${prefix}/api`,
    }));
    return;
  }
  const file = localFile(url.pathname);
  sendFile(response, file ?? join(dist, "index.html"));
}).listen(port, host, () => {
  process.stdout.write(`prefix preview listening on http://${host}:${port}${prefix}/\n`);
});
