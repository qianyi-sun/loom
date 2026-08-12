import { useCallback, useEffect, useRef, useState } from "react";

import {
  listPipelineEvents,
  type ApiError,
  type PipelineEventPage,
} from "../api/client";

type PipelineEvent = PipelineEventPage["events"][number];

export type PipelinePollerClock = {
  now: () => number;
  setTimeout: (callback: () => void, delayMs: number) => ReturnType<typeof setTimeout>;
  clearTimeout: (timer: ReturnType<typeof setTimeout>) => void;
};

export type PipelineEventFetcher = (
  runId: string,
  params: { after_seq: number; limit: 500 },
  signal: AbortSignal,
) => Promise<PipelineEventPage>;

export type PipelineEventPollerOptions = {
  fetchPage?: PipelineEventFetcher;
  clock?: PipelinePollerClock;
};

export type PipelineEventPollerState = {
  events: PipelineEvent[];
  nextAfterSeq: number;
  olderEventsOmitted: number;
  terminal: boolean;
  isPolling: boolean;
  error: unknown | null;
  degradedMessage: string | null;
  retry: () => void;
};

const FAILURE_DELAYS = [1_000, 2_000, 4_000, 8_000, 15_000] as const;
const EVENT_LIMIT = 5_000;

const browserClock: PipelinePollerClock = {
  now: () => Date.now(),
  setTimeout: (callback, delayMs) => setTimeout(callback, delayMs),
  clearTimeout: (timer) => clearTimeout(timer),
};

const defaultFetcher: PipelineEventFetcher = (id, params, signal) =>
  listPipelineEvents(id, params, signal);

function retryable(error: unknown): boolean {
  if (error instanceof TypeError) return true;
  if (typeof error !== "object" || error === null || !("status" in error)) return true;
  const status = (error as ApiError).status;
  return status === 429 || status >= 500;
}

function retryHint(error: unknown): number | null {
  if (typeof error !== "object" || error === null || !("retry_after_ms" in error)) return null;
  const value = (error as { retry_after_ms?: unknown }).retry_after_ms;
  return typeof value === "number" && Number.isInteger(value) && value >= 1 && value <= 15_000
    ? value
    : null;
}

function contractError(message: string): Error {
  return new Error(`pipeline event contract error: ${message}`);
}

function validatePage(page: PipelineEventPage, requestedCursor: number): PipelineEvent[] {
  const unique = new Map<number, PipelineEvent>();
  for (const event of page.events) {
    if (event.seq <= requestedCursor) continue;
    if (!unique.has(event.seq)) unique.set(event.seq, event);
  }
  const events = [...unique.values()].sort((left, right) => left.seq - right.seq);
  for (let index = 0; index < events.length; index += 1) {
    if (events[index].seq !== requestedCursor + index + 1) {
      throw contractError("event sequence regression or gap");
    }
  }
  const expectedCursor = events.length > 0 ? events[events.length - 1].seq : requestedCursor;
  if (page.next_after_seq !== expectedCursor) {
    throw contractError("next_after_seq does not match the accepted high-water mark");
  }
  return events;
}

export function usePipelineEventPoller(
  runId: string | undefined,
  options: PipelineEventPollerOptions = {},
): PipelineEventPollerState {
  const fetchPage = options.fetchPage ?? defaultFetcher;
  const clock = options.clock ?? browserClock;
  const [events, setEvents] = useState<PipelineEvent[]>([]);
  const [nextAfterSeq, setNextAfterSeq] = useState(0);
  const [olderEventsOmitted, setOlderEventsOmitted] = useState(0);
  const [terminal, setTerminal] = useState(false);
  const [isPolling, setIsPolling] = useState(false);
  const [error, setError] = useState<unknown | null>(null);
  const [degradedMessage, setDegradedMessage] = useState<string | null>(null);
  const generationRef = useRef(0);
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const controllerRef = useRef<AbortController | null>(null);
  const inFlightRef = useRef(false);
  const cursorRef = useRef(0);
  const failureCountRef = useRef(0);
  const failureStartedRef = useRef<number | null>(null);
  const lastSuccessRef = useRef<number | null>(null);
  const pollRef = useRef<() => void>(() => undefined);

  const clearTimer = useCallback(() => {
    if (timerRef.current !== null) {
      clock.clearTimeout(timerRef.current);
      timerRef.current = null;
    }
  }, [clock]);

  useEffect(() => {
    generationRef.current += 1;
    const generation = generationRef.current;
    clearTimer();
    controllerRef.current?.abort();
    controllerRef.current = null;
    inFlightRef.current = false;
    cursorRef.current = 0;
    failureCountRef.current = 0;
    failureStartedRef.current = null;
    lastSuccessRef.current = null;
    setEvents([]);
    setNextAfterSeq(0);
    setOlderEventsOmitted(0);
    setTerminal(false);
    setIsPolling(false);
    setError(null);
    setDegradedMessage(null);

    if (!runId) return undefined;

    const schedule = (delayMs: number): void => {
      clearTimer();
      timerRef.current = clock.setTimeout(() => {
        timerRef.current = null;
        pollRef.current();
      }, delayMs);
    };

    const poll = (): void => {
      if (generation !== generationRef.current || inFlightRef.current) return;
      inFlightRef.current = true;
      setIsPolling(true);
      const requestedCursor = cursorRef.current;
      const controller = new AbortController();
      controllerRef.current = controller;
      void fetchPage(runId, { after_seq: requestedCursor, limit: 500 }, controller.signal)
        .then((page) => {
          if (generation !== generationRef.current || controller.signal.aborted) return;
          const accepted = validatePage(page, requestedCursor);
          cursorRef.current = page.next_after_seq;
          setNextAfterSeq(page.next_after_seq);
          if (accepted.length > 0) {
            setEvents((current) => {
              const merged = [...current, ...accepted];
              const overflow = Math.max(0, merged.length - EVENT_LIMIT);
              if (overflow > 0) setOlderEventsOmitted((count) => count + overflow);
              return overflow > 0 ? merged.slice(overflow) : merged;
            });
          }
          failureCountRef.current = 0;
          failureStartedRef.current = null;
          lastSuccessRef.current = clock.now();
          setError(null);
          setDegradedMessage(null);
          setTerminal(page.terminal);
          if (!page.terminal) schedule(1_000);
        })
        .catch((caught: unknown) => {
          if (generation !== generationRef.current || controller.signal.aborted) return;
          setError(caught);
          if (!retryable(caught) || (caught instanceof Error && caught.message.startsWith("pipeline event contract error"))) {
            return;
          }
          const now = clock.now();
          if (failureStartedRef.current === null) failureStartedRef.current = now;
          failureCountRef.current += 1;
          if (now - failureStartedRef.current >= 60_000) {
            const last = lastSuccessRef.current;
            setDegradedMessage(
              `Live updates degraded; last successful event at ${last === null ? "never" : new Date(last).toLocaleTimeString()}`,
            );
          }
          const scheduled = FAILURE_DELAYS[Math.min(failureCountRef.current - 1, FAILURE_DELAYS.length - 1)];
          schedule(Math.max(scheduled, retryHint(caught) ?? 0));
        })
        .finally(() => {
          if (generation !== generationRef.current) return;
          inFlightRef.current = false;
          controllerRef.current = null;
          setIsPolling(false);
        });
    };

    pollRef.current = poll;
    poll();
    return () => {
      generationRef.current += 1;
      clearTimer();
      controllerRef.current?.abort();
      controllerRef.current = null;
      inFlightRef.current = false;
    };
  }, [clearTimer, clock, fetchPage, runId]);

  const retry = useCallback(() => {
    if (!runId || inFlightRef.current) return;
    clearTimer();
    pollRef.current();
  }, [clearTimer, runId]);

  return {
    events,
    nextAfterSeq,
    olderEventsOmitted,
    terminal,
    isPolling,
    error,
    degradedMessage,
    retry,
  };
}
