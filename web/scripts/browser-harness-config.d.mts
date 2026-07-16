export type BrowserHarnessConfig = Readonly<{
  origin: string;
  hostname: string;
  port: number;
  routePrefix: "/dev" | "/prod";
  baseURL: string;
  apiBaseURL: string;
  configURL: string;
  runtimeEnvironment: "local" | "production";
}>;

export function readBrowserHarnessConfig(
  environment?: Readonly<Record<string, string | undefined>>,
): BrowserHarnessConfig;
