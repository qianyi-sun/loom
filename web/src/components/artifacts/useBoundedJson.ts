import { useEffect, useState } from "react";

type JsonState =
  | { status: "idle" | "loading"; value: null; error: null }
  | { status: "ready"; value: unknown; error: null }
  | { status: "error"; value: null; error: Error };

const decoder = new TextDecoder("utf-8", { fatal: true });

async function readBoundedJson(
  url: string,
  limit: number,
  signal: AbortSignal,
): Promise<unknown> {
  const response = await fetch(url, {
    credentials: "include",
    headers: { Accept: "application/json" },
    signal,
  });
  if (!response.ok) throw new Error("Artifact JSON is unavailable");
  const declared = Number(response.headers.get("Content-Length"));
  if (Number.isFinite(declared) && declared > limit) {
    throw new Error("Artifact JSON exceeds the viewer limit");
  }
  if (!response.body) throw new Error("Artifact JSON response has no body");
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      total += value.byteLength;
      if (total > limit) {
        await reader.cancel();
        throw new Error("Artifact JSON exceeds the viewer limit");
      }
      chunks.push(value);
    }
  } finally {
    reader.releaseLock();
  }
  const body = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    body.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return JSON.parse(decoder.decode(body)) as unknown;
}

export function useBoundedJson(url: string | null, limit: number): JsonState {
  const [state, setState] = useState<JsonState>({
    status: "idle",
    value: null,
    error: null,
  });
  useEffect(() => {
    if (url === null) {
      setState({ status: "idle", value: null, error: null });
      return undefined;
    }
    const controller = new AbortController();
    let active = true;
    setState({ status: "loading", value: null, error: null });
    void readBoundedJson(url, limit, controller.signal).then(
      (value) => {
        if (active) setState({ status: "ready", value, error: null });
      },
      (error: unknown) => {
        if (active && !controller.signal.aborted) {
          setState({
            status: "error",
            value: null,
            error: error instanceof Error ? error : new Error("Artifact JSON is invalid"),
          });
        }
      },
    );
    return () => {
      active = false;
      controller.abort();
    };
  }, [limit, url]);
  return state;
}
