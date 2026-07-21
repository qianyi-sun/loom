import type { MockInstance } from "vitest";

export type FetchMock = MockInstance<typeof fetch>;
export type FetchCall = Parameters<typeof fetch>;

export function requestUrl(input: FetchCall[0]): string {
  return input instanceof Request ? input.url : String(input);
}

export function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

export function jsonRequestBody<T>(call: FetchCall): T {
  const body = call[1]?.body;
  if (typeof body !== "string") {
    throw new Error("expected JSON string request body");
  }
  return JSON.parse(body) as T;
}
