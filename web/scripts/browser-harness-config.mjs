const DEFAULT_ORIGIN = "http://127.0.0.1:4173";
const DEFAULT_ROUTE_PREFIX = "/dev";
const LOCAL_HOSTS = new Set(["127.0.0.1", "localhost"]);

function validatedOrigin(value) {
  const url = new URL(value);
  if (
    url.protocol !== "http:" ||
    !LOCAL_HOSTS.has(url.hostname) ||
    url.username ||
    url.password ||
    url.pathname !== "/" ||
    url.search ||
    url.hash
  ) {
    throw new Error(
      "LOOM_E2E_ORIGIN must be a credential-free local HTTP origin without a path, query, or fragment",
    );
  }
  return url.origin;
}

function validatedRoutePrefix(value) {
  if (value !== "/dev" && value !== "/prod") {
    throw new Error("LOOM_E2E_ROUTE_PREFIX must be exactly /dev or /prod");
  }
  return value;
}

export function readBrowserHarnessConfig(environment = process.env) {
  const origin = validatedOrigin(environment.LOOM_E2E_ORIGIN ?? DEFAULT_ORIGIN);
  const routePrefix = validatedRoutePrefix(
    environment.LOOM_E2E_ROUTE_PREFIX ?? DEFAULT_ROUTE_PREFIX,
  );
  const originUrl = new URL(origin);
  return Object.freeze({
    origin,
    hostname: originUrl.hostname,
    port: Number(originUrl.port || "80"),
    routePrefix,
    baseURL: `${origin}${routePrefix}`,
    apiBaseURL: `${origin}${routePrefix}/api`,
    configURL: `${origin}${routePrefix}/loom-frontend-config.json`,
    runtimeEnvironment: routePrefix === "/prod" ? "production" : "development",
  });
}

export function browserWebServerConfig(config) {
  return Object.freeze({
    command: "node scripts/build-browser-test.mjs && node scripts/prefix-preview-server.mjs",
    url: config.configURL,
    timeout: 120_000,
    reuseExistingServer: false,
  });
}
