/**
 * Generic cursor-pagination control. The list endpoints return
 * `next_cursor: string | null`; we keep a stack of previous cursors
 * so the user can go back without reloading.
 */
import { Button } from "./Button";
import { PAGINATION_NEXT_HELP, PAGINATION_PREV_HELP } from "../lib/helpText";

export type PageState = {
  current: string | null;
  stack: (string | null)[];
};

export const initialPage: PageState = { current: null, stack: [] };

export function nextPage(state: PageState, cursor: string): PageState {
  return { current: cursor, stack: [...state.stack, state.current] };
}

export function prevPage(state: PageState): PageState {
  if (state.stack.length === 0) return state;
  const stack = [...state.stack];
  const current = stack.pop() ?? null;
  return { current, stack };
}

export default function Pagination({
  state,
  hasNext,
  onNext,
  onPrev,
}: {
  state: PageState;
  hasNext: boolean;
  onNext: () => void;
  onPrev: () => void;
}): JSX.Element {
  return (
    <div className="mt-4 flex items-center justify-end gap-2">
      <Button
        size="sm"
        onClick={onPrev}
        disabled={state.stack.length === 0}
        aria-label="previous page"
        title={PAGINATION_PREV_HELP}
      >
        ← Prev
      </Button>
      <Button
        size="sm"
        onClick={onNext}
        disabled={!hasNext}
        aria-label="next page"
        title={PAGINATION_NEXT_HELP}
      >
        Next →
      </Button>
    </div>
  );
}
