export type FrontendEnvironment =
  | "local"
  | "development"
  | "staging"
  | "production";

export interface FrontendConfig {
  environment: FrontendEnvironment;
  environmentLabel: string;
  routePath: string;
  apiBase: string;
  apiRouteBase: string;
}

interface RawFrontendConfig {
  environment?: unknown;
  environmentLabel?: unknown;
  routePath?: unknown;
  apiBase?: unknown;
  apiRouteBase?: unknown;
}

function viteApiBase(): string {
  const env = (
    import.meta as unknown as { env: { VITE_API_BASE?: string } }
  ).env;
  return env?.VITE_API_BASE ?? "";
}

function expectString(
  value: unknown,
  field: keyof RawFrontendConfig,
): string {
  if (typeof value !== "string") {
    throw new Error(`frontend config ${field} must be a string`);
  }
  return value.trim();
}

function normalizePath(raw: string, field: string): string {
  if (raw === "" || raw === "/") return "";
  if (!raw.startsWith("/")) {
    throw new Error(`frontend config ${field} must start with /`);
  }
  return raw.replace(/\/+$/u, "");
}

function detectRoutePath(location: Location | URL): string {
  const pathname = location.pathname.replace(/\/+$/u, "") || "/";
  if (pathname === "/prod" || pathname.startsWith("/prod/")) return "/prod";
  if (pathname === "/dev" || pathname.startsWith("/dev/")) return "/dev";
  return "";
}

function apiBasePath(apiBase: string, location: URL): string {
  if (apiBase === "") return "";
  if (apiBase.startsWith("http://") || apiBase.startsWith("https://")) {
    return normalizePath(new URL(apiBase).pathname, "apiBase");
  }
  return normalizePath(new URL(apiBase, location.origin).pathname, "apiBase");
}

function apiRouteBase(apiBase: string, location: URL): string {
  const base = apiBase || "/";
  const url = new URL(`${base.replace(/\/+$/u, "")}/api`, location.origin);
  return url.href.replace(/\/+$/u, "");
}

function resolveApiRouteBase(
  raw: RawFrontendConfig,
  apiBase: string,
  location: URL,
): string {
  const fallback = apiRouteBase(apiBase, location);
  if (raw.apiRouteBase === undefined || raw.apiRouteBase === null) {
    return fallback;
  }
  const value = expectString(raw.apiRouteBase, "apiRouteBase");
  if (!value) return fallback;
  const parsed = new URL(value, location.origin);
  const expectedPath = `${apiBase || ""}/api`;
  if (parsed.pathname.replace(/\/+$/u, "") !== expectedPath) {
    throw new Error(
      `frontend config apiRouteBase ${value} must match apiBase ${apiBase || "/"}`,
    );
  }
  return value.replace(/\/+$/u, "");
}

function routeMatches(routePath: string, location: URL): boolean {
  if (routePath === "") return true;
  return (
    location.pathname === routePath ||
    location.pathname.startsWith(`${routePath}/`)
  );
}

function isFrontendEnvironment(value: string): value is FrontendEnvironment {
  return ["local", "development", "staging", "production"].includes(value);
}

export function resolveFrontendConfig(
  raw: RawFrontendConfig,
  locationLike: Location | URL = window.location,
  opts: { allowRouteMismatch?: boolean } = {},
): FrontendConfig {
  const location = new URL(locationLike.href);
  const environment = expectString(raw.environment, "environment");
  if (!isFrontendEnvironment(environment)) {
    throw new Error(`frontend config environment ${environment} is not supported`);
  }
  const environmentLabel = expectString(
    raw.environmentLabel,
    "environmentLabel",
  );
  if (!environmentLabel) {
    throw new Error("frontend config environmentLabel must not be empty");
  }
  const routePath = normalizePath(
    expectString(raw.routePath ?? "", "routePath"),
    "routePath",
  );
  const apiBase = normalizePath(
    expectString(raw.apiBase ?? routePath, "apiBase"),
    "apiBase",
  );

  if (!opts.allowRouteMismatch && !routeMatches(routePath, location)) {
    throw new Error(
      `frontend config routePath ${routePath || "/"} does not match current route ${location.pathname}`,
    );
  }

  if (apiBasePath(apiBase, location) !== routePath) {
    throw new Error(
      `frontend config apiBase ${apiBase || "/"} must match routePath ${routePath || "/"}`,
    );
  }

  const detectedRoutePath = detectRoutePath(location);
  if (
    !opts.allowRouteMismatch &&
    detectedRoutePath &&
    detectedRoutePath !== routePath
  ) {
    throw new Error(
      `frontend config routePath ${routePath || "/"} does not match current route ${detectedRoutePath}`,
    );
  }

  if (environment === "production" && routePath !== "/prod") {
    throw new Error("production frontend config must use routePath /prod");
  }
  if (environment !== "production" && routePath === "/prod") {
    throw new Error("non-production frontend config must not use routePath /prod");
  }
  if (environment === "production" && /beta/i.test(environmentLabel)) {
    throw new Error("production frontend label must not contain beta wording");
  }

  return {
    environment,
    environmentLabel,
    routePath,
    apiBase,
    apiRouteBase: resolveApiRouteBase(raw, apiBase, location),
  };
}

function defaultConfig(): FrontendConfig {
  return resolveFrontendConfig(
    {
      environment: "local",
      environmentLabel: "Local development",
      routePath: "",
      apiBase: viteApiBase(),
    },
    window.location,
    { allowRouteMismatch: true },
  );
}

let currentConfig: FrontendConfig = defaultConfig();

function configUrlForLocation(location: Location): string {
  const routePath = detectRoutePath(location);
  return `${routePath}/loom-frontend-config.json`;
}

export async function loadFrontendConfig(): Promise<FrontendConfig> {
  const url = configUrlForLocation(window.location);
  const routePath = detectRoutePath(window.location);
  try {
    const resp = await fetch(url, {
      cache: "no-store",
      credentials: "same-origin",
      headers: { Accept: "application/json" },
    });
    if (!resp.ok) {
      if (routePath) {
        throw new Error(`frontend runtime config returned HTTP ${resp.status}`);
      }
      currentConfig = defaultConfig();
      return currentConfig;
    }
    const raw = (await resp.json()) as RawFrontendConfig;
    currentConfig = resolveFrontendConfig(raw, window.location);
    return currentConfig;
  } catch (err) {
    if (routePath) throw err;
    currentConfig = defaultConfig();
    return currentConfig;
  }
}

export function getFrontendConfig(): FrontendConfig {
  return currentConfig;
}

export function getApiBase(): string {
  return currentConfig.apiBase;
}

export function setFrontendConfigForTests(config: FrontendConfig | null): void {
  currentConfig = config ?? defaultConfig();
}
