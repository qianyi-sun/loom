import { act, renderHook } from "@testing-library/react";
import { vi } from "vitest";

import {
  type PipelineEventFetcher,
  usePipelineEventPoller,
} from "../../hooks/usePipelineEventPoller";

const event = (seq: number) => ({ seq, stage_run_id: null, execution_attempt_id: null, event_type: "advanced", payload: {}, created_at: "2026-08-12T00:00:00Z" });

beforeEach(() => vi.useFakeTimers());
afterEach(() => vi.useRealTimers());

test("polls cursor GET after exactly one second and stops terminal", async () => {
  const fetchPage = vi.fn().mockResolvedValueOnce({ events: [], next_after_seq: 0, terminal: false, retry_after_ms: 1000 }).mockResolvedValueOnce({ events: [event(1)], next_after_seq: 1, terminal: true, retry_after_ms: null });
  const { result } = renderHook(() => usePipelineEventPoller("run", { fetchPage }));
  await act(async () => Promise.resolve());
  expect(fetchPage).toHaveBeenCalledWith("run", { after_seq: 0, limit: 500 }, expect.any(AbortSignal));
  await act(async () => { await vi.advanceTimersByTimeAsync(1_000); });
  expect(result.current.events.map((item) => item.seq)).toEqual([1]);
  expect(result.current.terminal).toBe(true);
  await act(async () => { await vi.advanceTimersByTimeAsync(30_000); });
  expect(fetchPage).toHaveBeenCalledTimes(2);
});

test("stops on a cursor gap with visible contract error", async () => {
  const fetchPage = vi.fn().mockResolvedValue({ events: [event(2)], next_after_seq: 2, terminal: false, retry_after_ms: 1000 });
  const { result } = renderHook(() => usePipelineEventPoller("run", { fetchPage }));
  await act(async () => Promise.resolve());
  expect(String(result.current.error)).toContain("pipeline event contract error");
  await act(async () => { await vi.advanceTimersByTimeAsync(30_000); });
  expect(fetchPage).toHaveBeenCalledTimes(1);
});

test("aborts and clears state on run change", async () => {
  let firstSignal: AbortSignal | undefined;
  const fetchPage = vi.fn<PipelineEventFetcher>((id, _params, signal) => { if (id === "one") { firstSignal = signal; return new Promise(() => undefined); } return Promise.resolve({ events: [], next_after_seq: 0, terminal: true, retry_after_ms: null }); });
  const { rerender } = renderHook(({ id }) => usePipelineEventPoller(id, { fetchPage }), { initialProps: { id: "one" } });
  rerender({ id: "two" });
  expect(firstSignal?.aborted).toBe(true);
  await act(async () => Promise.resolve());
  expect(fetchPage).toHaveBeenCalledTimes(2);
});

test("ignores stale duplicates, sorts accepted events, and retains only the newest 5000", async () => {
  const events = Array.from({ length: 5_001 }, (_, index) => event(index + 1)).reverse();
  events.push(event(1));
  const fetchPage = vi.fn().mockResolvedValue({ events, next_after_seq: 5_001, terminal: true, retry_after_ms: null });
  const { result } = renderHook(() => usePipelineEventPoller("run", { fetchPage }));
  await act(async () => Promise.resolve());
  expect(result.current.events).toHaveLength(5_000);
  expect(result.current.events[0].seq).toBe(2);
  expect(result.current.events.at(-1)?.seq).toBe(5_001);
  expect(result.current.olderEventsOmitted).toBe(1);
  expect(result.current.nextAfterSeq).toBe(5_001);
  expect(result.current.error).toBeNull();
});

test("rejects a high-water mark that does not match accepted events", async () => {
  const fetchPage = vi.fn().mockResolvedValue({ events: [event(1)], next_after_seq: 2, terminal: false, retry_after_ms: null });
  const { result } = renderHook(() => usePipelineEventPoller("run", { fetchPage }));
  await act(async () => Promise.resolve());
  expect(String(result.current.error)).toContain("next_after_seq");
  expect(result.current.isPolling).toBe(false);
  expect(vi.getTimerCount()).toBe(0);
});

test("uses a valid server retry hint and permits an explicit retry after a non-retryable error", async () => {
  const fetchPage = vi.fn()
    .mockRejectedValueOnce({ status: 429, retry_after_ms: 3_000 })
    .mockRejectedValueOnce({ status: 400 })
    .mockResolvedValueOnce({ events: [], next_after_seq: 0, terminal: true, retry_after_ms: null });
  const { result } = renderHook(() => usePipelineEventPoller("run", { fetchPage }));
  await act(async () => Promise.resolve());
  expect(fetchPage).toHaveBeenCalledTimes(1);
  await act(async () => { await vi.advanceTimersByTimeAsync(2_999); });
  expect(fetchPage).toHaveBeenCalledTimes(1);
  await act(async () => { await vi.advanceTimersByTimeAsync(1); });
  expect(fetchPage).toHaveBeenCalledTimes(2);
  expect(result.current.error).toEqual({ status: 400 });
  await act(async () => result.current.retry());
  expect(fetchPage).toHaveBeenCalledTimes(3);
  expect(result.current.error).toBeNull();
  expect(result.current.terminal).toBe(true);
});

test("caps exponential failures and reports degradation after a minute", async () => {
  vi.setSystemTime(new Date("2026-08-12T00:00:00Z"));
  const fetchPage = vi.fn().mockRejectedValue(new TypeError("offline"));
  const { result } = renderHook(() => usePipelineEventPoller("run", { fetchPage }));
  await act(async () => Promise.resolve());
  for (const delay of [1_000, 2_000, 4_000, 8_000, 15_000, 15_000, 15_000]) {
    await act(async () => { await vi.advanceTimersByTimeAsync(delay); });
  }
  expect(fetchPage.mock.calls.length).toBeGreaterThanOrEqual(8);
  expect(result.current.degradedMessage).toContain("last successful event at never");
  expect(result.current.error).toBeInstanceOf(TypeError);
});

test("does not poll without a run id or duplicate an in-flight request", async () => {
  let resolveRequest: ((page: { events: never[]; next_after_seq: number; terminal: boolean; retry_after_ms: null }) => void) | undefined;
  const fetchPage = vi.fn(() => new Promise<{ events: never[]; next_after_seq: number; terminal: boolean; retry_after_ms: null }>((resolve) => { resolveRequest = resolve; }));
  const { result, rerender } = renderHook(({ id }: { id: string | undefined }) => usePipelineEventPoller(id, { fetchPage }), { initialProps: { id: undefined as string | undefined } });
  act(() => result.current.retry());
  expect(fetchPage).not.toHaveBeenCalled();

  rerender({ id: "run" });
  expect(fetchPage).toHaveBeenCalledTimes(1);
  act(() => result.current.retry());
  expect(fetchPage).toHaveBeenCalledTimes(1);
  await act(async () => resolveRequest?.({ events: [], next_after_seq: 0, terminal: true, retry_after_ms: null }));
  expect(result.current.isPolling).toBe(false);
});
