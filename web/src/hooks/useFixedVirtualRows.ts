import { useCallback, useMemo, useState } from "react";

export type FixedVirtualRow = {
  index: number;
  start: number;
  size: number;
};

export type FixedVirtualRows = {
  rows: FixedVirtualRow[];
  totalHeight: number;
  onScroll: (scrollTop: number) => void;
  scrollToIndex: (index: number) => number;
};

export function useFixedVirtualRows(
  count: number,
  rowHeight = 44,
  viewportHeight = 660,
  overscan = 12,
): FixedVirtualRows {
  const [scrollTop, setScrollTop] = useState(0);
  const visibleCount = Math.ceil(viewportHeight / rowHeight);
  const rows = useMemo(() => {
    const firstVisible = Math.floor(scrollTop / rowHeight);
    const start = Math.max(0, firstVisible - overscan);
    const end = Math.min(count, firstVisible + visibleCount + overscan);
    return Array.from({ length: Math.max(0, end - start) }, (_, offset) => {
      const index = start + offset;
      return { index, start: index * rowHeight, size: rowHeight };
    });
  }, [count, overscan, rowHeight, scrollTop, visibleCount]);

  const onScroll = useCallback((next: number) => {
    setScrollTop(Math.max(0, next));
  }, []);

  const scrollToIndex = useCallback(
    (index: number): number => {
      const target = Math.max(0, Math.min(count - 1, index));
      const rowStart = target * rowHeight;
      const rowEnd = rowStart + rowHeight;
      let next = scrollTop;
      if (rowStart < scrollTop) next = rowStart;
      else if (rowEnd > scrollTop + viewportHeight) next = rowEnd - viewportHeight;
      setScrollTop(next);
      return next;
    },
    [count, rowHeight, scrollTop, viewportHeight],
  );

  return { rows, totalHeight: count * rowHeight, onScroll, scrollToIndex };
}
