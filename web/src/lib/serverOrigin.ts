import { getFrontendConfig } from "./frontendConfig";

export function currentServerOrigin(): string {
  if (typeof window === "undefined") return "<server-url>";
  const origin = window.location.origin;
  if (!origin || origin === "null") return "<server-url>";
  return `${origin}${getFrontendConfig().routePath}`;
}
