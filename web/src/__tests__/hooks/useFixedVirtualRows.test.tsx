import { act, renderHook } from "@testing-library/react";

import { useFixedVirtualRows } from "../../hooks/useFixedVirtualRows";

test("virtual rows use the exact fixed geometry and mount bound", () => {
  const { result } = renderHook(() => useFixedVirtualRows(250, 44, 660, 12));
  expect(result.current.totalHeight).toBe(11_000);
  expect(result.current.rows).toHaveLength(27);
  act(() => result.current.onScroll(4_400));
  expect(result.current.rows[0].index).toBe(88);
  expect(result.current.rows.length).toBeLessThanOrEqual(39);
});

test("scrollToIndex brings a focused row into view", () => {
  const { result } = renderHook(() => useFixedVirtualRows(250));
  act(() => expect(result.current.scrollToIndex(249)).toBe(10_340));
  expect(result.current.rows.at(-1)?.index).toBe(249);
});

test("clamps negative scroll and moves upward only when needed", () => {
  const { result } = renderHook(() => useFixedVirtualRows(10, 20, 60, 0));
  act(() => result.current.onScroll(-100));
  expect(result.current.rows[0].index).toBe(0);
  act(() => result.current.onScroll(100));
  act(() => expect(result.current.scrollToIndex(-1)).toBe(0));
  expect(result.current.rows[0].index).toBe(0);
  act(() => expect(result.current.scrollToIndex(1)).toBe(0));
  act(() => expect(result.current.scrollToIndex(999)).toBe(140));
});
