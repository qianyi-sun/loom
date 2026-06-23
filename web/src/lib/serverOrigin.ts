export function currentServerOrigin(): string {
  if (typeof window === "undefined") return "<server-url>";
  const origin = window.location.origin;
  return origin && origin !== "null" ? origin : "<server-url>";
}
