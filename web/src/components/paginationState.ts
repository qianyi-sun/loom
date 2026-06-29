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
