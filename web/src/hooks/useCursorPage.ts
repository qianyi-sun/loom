import { useCallback, useEffect, useState } from "react";

import {
  initialPage,
  nextPage,
  prevPage,
  type PageState,
} from "../components/paginationState";

type KeyedPageState = {
  resetKey: string;
  page: PageState;
};

export interface CursorPageController {
  state: PageState;
  cursor: string | null;
  next: (cursor: string) => void;
  prev: () => void;
  reset: () => void;
}

/**
 * Own session-local keyset history while synchronously exposing page one for
 * a changed filter key.  The synchronous derivation is important: callers
 * must never construct a new-filter request with the previous filter's cursor.
 */
export function useCursorPage(resetKey: string): CursorPageController {
  const [stored, setStored] = useState<KeyedPageState>(() => ({
    resetKey,
    page: initialPage,
  }));

  const state = stored.resetKey === resetKey ? stored.page : initialPage;

  useEffect(() => {
    setStored((current) =>
      current.resetKey === resetKey
        ? current
        : { resetKey, page: initialPage },
    );
  }, [resetKey]);

  const update = useCallback(
    (transition: (page: PageState) => PageState) => {
      setStored((current) => {
        const currentPage =
          current.resetKey === resetKey ? current.page : initialPage;
        return {
          resetKey,
          page: transition(currentPage),
        };
      });
    },
    [resetKey],
  );

  const next = useCallback(
    (cursor: string) =>
      update((page) =>
        page.current === cursor ? page : nextPage(page, cursor),
      ),
    [update],
  );
  const prev = useCallback(
    () => update((page) => prevPage(page)),
    [update],
  );
  const reset = useCallback(
    () => setStored({ resetKey, page: initialPage }),
    [resetKey],
  );

  return {
    state,
    cursor: state.current,
    next,
    prev,
    reset,
  };
}
