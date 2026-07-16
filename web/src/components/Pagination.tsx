/** Cursor pagination with a session-local previous-cursor stack. */
import { useEffect, useRef, type MouseEvent } from "react";

import { Button } from "./Button";
import { cn } from "../lib/cn";
import { PAGINATION_NEXT_HELP, PAGINATION_PREV_HELP } from "../lib/helpText";
import type { PageState } from "./paginationState";

export interface PaginationProps {
  state: PageState;
  hasNext: boolean;
  isLoading?: boolean;
  isError?: boolean;
  onNext: () => void;
  onPrev: () => void;
  onRetry?: () => void;
  className?: string;
}

export default function Pagination({
  state,
  hasNext,
  isLoading = false,
  isError = false,
  onNext,
  onPrev,
  onRetry,
  className = "mt-4",
}: PaginationProps): JSX.Element {
  const pageNumber = state.stack.length + 1;
  const activationLock = useRef(false);
  const prevBlocked = isLoading || state.stack.length === 0;
  const retryBlocked = isLoading;
  const nextBlocked = isLoading || isError || !hasNext;

  useEffect(() => {
    activationLock.current = false;
  }, [hasNext, isError, isLoading, state.stack.length]);

  const guardActivation =
    (blocked: boolean, action: () => void) =>
    (event: MouseEvent<HTMLButtonElement>): void => {
      if (blocked || activationLock.current) {
        event.preventDefault();
        return;
      }
      activationLock.current = true;
      action();
    };

  const status = isLoading
    ? `Loading page ${pageNumber}…`
    : isError
      ? `Page ${pageNumber} could not be loaded.`
      : hasNext
        ? `Page ${pageNumber}, more results available.`
        : `Page ${pageNumber}, end of results.`;

  return (
    <div
      className={cn("flex items-center justify-between gap-3", className)}
      aria-busy={isLoading}
    >
      <p className="text-xs text-slate-500" role="status" aria-live="polite">
        {status}
      </p>
      <div className="flex items-center gap-2">
        <Button
          size="sm"
          onClick={guardActivation(prevBlocked, onPrev)}
          aria-disabled={prevBlocked}
          aria-label="previous page"
          title={PAGINATION_PREV_HELP}
          className={cn(prevBlocked && "cursor-not-allowed opacity-50")}
        >
          ← Prev
        </Button>
        {isError && onRetry ? (
          <Button
            size="sm"
            onClick={guardActivation(retryBlocked, onRetry)}
            aria-disabled={retryBlocked}
            aria-label="retry page"
            className={cn(retryBlocked && "cursor-not-allowed opacity-50")}
          >
            Retry
          </Button>
        ) : null}
        <Button
          size="sm"
          onClick={guardActivation(nextBlocked, onNext)}
          aria-disabled={nextBlocked}
          aria-label="next page"
          title={PAGINATION_NEXT_HELP}
          className={cn(nextBlocked && "cursor-not-allowed opacity-50")}
        >
          Next →
        </Button>
      </div>
    </div>
  );
}
