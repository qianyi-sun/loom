/**
 * #5 Slice 6 — SPA EventSource consumer for the SSE
 * `/api/v1/trials/{id}/stream` endpoint.
 *
 * Opens an `EventSource` and exposes `{events, status}`. The hook
 * dedupes by event seq so the browser's native auto-reconnect (which
 * sends `Last-Event-ID` as a header the server currently ignores)
 * can't produce a duplicate event in the consumer's state.
 *
 * Status values mirror the SSE wire contract emitted by
 * `loom_service.routes.trajectory.stream_events`:
 *   - `connecting`: hook just mounted, EventSource not yet opened
 *   - `open`:       initial connection succeeded; receiving events
 *   - `complete`:   server emitted `event: complete` (trial reached
 *                   terminal state); we closed the connection
 *   - `reconnect`:  server emitted `event: reconnect` (connection
 *                   budget exhausted); next mount will resume
 *   - `error`:      EventSource errored; the browser will auto-
 *                   reconnect, but caller should consider polling
 *                   fallback for persistent failures
 *
 * Caller pairs with the legacy `/events?after_seq=N` poll path as a
 * fallback for environments where `EventSource` is unavailable or
 * blocked (some corp proxies strip `text/event-stream`).
 */
import { useEffect, useRef, useState } from "react";

import type { components } from "../api/schema";
import { getApiBase } from "../lib/frontendConfig";

type TrajEvent = components["schemas"]["TrajectoryEvent"];

export type TrialEventStreamStatus =
  | "connecting"
  | "open"
  | "complete"
  | "reconnect"
  | "error";

export interface UseTrialEventStreamOptions {
  /**
   * Skip opening the connection (e.g. trial has no events expected,
   * or caller wants to defer streaming). Defaults to enabled.
   */
  enabled?: boolean;
  /**
   * Override the base URL the EventSource opens against. Defaults to
   * the runtime frontend API base, matching `apiBase()` in `api/client.ts`.
   */
  baseUrl?: string;
  /**
   * Inject an `EventSource` constructor — only used by tests to
   * pass a fake. Production code path falls through to the
   * `globalThis.EventSource` default.
   */
  eventSourceCtor?: typeof EventSource;
}

export interface UseTrialEventStreamResult {
  events: TrajEvent[];
  status: TrialEventStreamStatus;
}

function _defaultBaseUrl(): string {
  return getApiBase();
}

export function useTrialEventStream(
  trialId: string,
  opts: UseTrialEventStreamOptions = {},
): UseTrialEventStreamResult {
  const { enabled = true, baseUrl, eventSourceCtor } = opts;
  const [events, setEvents] = useState<TrajEvent[]>([]);
  const [status, setStatus] = useState<TrialEventStreamStatus>("connecting");
  // Track the highest seq we've observed so the dedupe survives a
  // browser-native auto-reconnect that replays already-seen events
  // (the server ignores Last-Event-ID for now; clients can't rely
  // on perfect resume semantics until the server consumes it).
  const lastSeqRef = useRef<number>(-1);

  useEffect(() => {
    if (!enabled || !trialId) {
      return;
    }
    const ctor = eventSourceCtor ?? (globalThis as { EventSource?: typeof EventSource }).EventSource;
    if (typeof ctor !== "function") {
      // Environment without EventSource — caller must fall back to
      // polling. Surface as an error so the caller's status check
      // catches it.
      setStatus("error");
      return;
    }
    const base = baseUrl ?? _defaultBaseUrl();
    const url = `${base}/api/v1/trials/${trialId}/stream?after_seq=${lastSeqRef.current}`;
    const es: EventSource = new ctor(url, { withCredentials: true });

    es.onopen = (): void => setStatus("open");

    es.onmessage = (e: MessageEvent): void => {
      let ev: TrajEvent;
      try {
        ev = JSON.parse(e.data) as TrajEvent;
      } catch {
        // Malformed payload — skip but don't tear down the connection.
        return;
      }
      const seq = (ev as { seq?: unknown }).seq;
      if (typeof seq !== "number") {
        // Server-side contract violation; skip.
        return;
      }
      if (seq <= lastSeqRef.current) {
        // Dedupe — auto-reconnect can replay events we've seen.
        return;
      }
      lastSeqRef.current = seq;
      setEvents((prev) => [...prev, ev]);
    };

    es.addEventListener("complete", () => {
      setStatus("complete");
      es.close();
    });

    es.addEventListener("reconnect", () => {
      setStatus("reconnect");
      es.close();
    });

    es.onerror = (): void => {
      // EventSource auto-reconnects internally; we surface `error`
      // so the caller can decide whether to swap to polling on
      // persistent failure (e.g. proxy stripping text/event-stream).
      setStatus("error");
    };

    return (): void => {
      es.close();
    };
  }, [trialId, enabled, baseUrl, eventSourceCtor]);

  return { events, status };
}
