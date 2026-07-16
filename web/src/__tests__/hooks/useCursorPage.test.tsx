import { act, renderHook } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { useCursorPage } from "../../hooks/useCursorPage";

describe("useCursorPage", () => {
  it("moves forward and backward through session-local cursor history", () => {
    const { result } = renderHook(() => useCursorPage("scope=my"));

    expect(result.current.state).toEqual({ current: null, stack: [] });
    act(() => result.current.next("cursor-2"));
    expect(result.current.state).toEqual({
      current: "cursor-2",
      stack: [null],
    });
    act(() => result.current.next("cursor-3"));
    expect(result.current.state).toEqual({
      current: "cursor-3",
      stack: [null, "cursor-2"],
    });
    act(() => result.current.prev());
    expect(result.current.state).toEqual({
      current: "cursor-2",
      stack: [null],
    });
  });

  it("coalesces concurrent transitions to the same next cursor", () => {
    const { result } = renderHook(() => useCursorPage("scope=my"));

    act(() => {
      result.current.next("cursor-2");
      result.current.next("cursor-2");
    });

    expect(result.current.state).toEqual({
      current: "cursor-2",
      stack: [null],
    });
  });

  it("exposes page one synchronously when resetKey changes", () => {
    const { result, rerender } = renderHook(
      ({ resetKey }) => useCursorPage(resetKey),
      { initialProps: { resetKey: "scope=my|state=" } },
    );

    act(() => result.current.next("stale-cursor"));
    expect(result.current.cursor).toBe("stale-cursor");

    rerender({ resetKey: "scope=all|state=finished" });

    expect(result.current.cursor).toBeNull();
    expect(result.current.state).toEqual({ current: null, stack: [] });
    act(() => result.current.prev());
    expect(result.current.state).toEqual({ current: null, stack: [] });
  });

  it("resets explicitly without changing the key", () => {
    const { result } = renderHook(() => useCursorPage("audit"));

    act(() => result.current.next("audit-page-2"));
    act(() => result.current.reset());

    expect(result.current.cursor).toBeNull();
    expect(result.current.state.stack).toEqual([]);
  });
});
