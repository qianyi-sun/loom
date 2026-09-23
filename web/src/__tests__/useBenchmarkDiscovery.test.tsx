import { act, renderHook } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import type { PropsWithChildren } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { useBenchmarkDiscovery } from "../hooks/useBenchmarkDiscovery";

describe("aggregate benchmark discovery", () => {
  afterEach(() => { vi.useRealTimers(); vi.restoreAllMocks(); });
  it.each([1, 100])("sends one request after selecting %i benchmarks", async (count) => {
    vi.useFakeTimers();
    const fetch = vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response(JSON.stringify({ items: [] }), { status: 200 }));
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const wrapper = ({ children }: PropsWithChildren) => <QueryClientProvider client={client}>{children}</QueryClientProvider>;
    const { rerender } = renderHook(({ ids }) => useBenchmarkDiscovery(ids), { initialProps: { ids: [] as string[] }, wrapper });
    const ids = Array.from({ length: count }, (_, n) => `benchmark-${n}`);
    for (let n = 1; n <= count; n++) rerender({ ids: ids.slice(0, n) });
    expect(fetch).not.toHaveBeenCalled();
    await act(async () => { await vi.advanceTimersByTimeAsync(300); });
    expect(fetch).toHaveBeenCalledTimes(1);
    const [url, request] = fetch.mock.calls[0];
    expect(String(url)).toContain("/api/v1/benchmarks/discover");
    expect(JSON.parse(String(request?.body))).toEqual({ benchmark_ids: ids });
  });
});
