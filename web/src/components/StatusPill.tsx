/**
 * StatusPill — small colored badge for a state value. The variant
 * names mirror the underlying domain semantics; component callers map
 * raw status strings to a variant. Keeping the variant set narrow on
 * purpose so the colour vocabulary stays consistent across pages.
 *
 * Accessibility: no `role` attribute — pills decorate the cell they
 * live in (e.g. a "State" column in a table). The screen reader
 * already reads the visible text "succeeded" / "failed" / etc., so
 * adding `role="status"` (a live region) would cause every pill on
 * every refresh of a 50-row table to be re-announced. If a caller
 * needs a SR-only label override they can pass `aria-label`.
 */
import { forwardRef, type HTMLAttributes } from "react";

import { cn } from "../lib/cn";
import { helpForState } from "../lib/helpText";

export type StatusVariant =
  | "success"
  | "running"
  | "queued"
  | "warning"
  | "failed"
  | "cancelled"
  | "info"
  | "neutral";

const VARIANT_CLASSES: Record<StatusVariant, string> = {
  success: "bg-emerald-50 text-emerald-700 border-emerald-100",
  running: "bg-indigo-50 text-indigo-700 border-indigo-100",
  queued: "bg-amber-50 text-amber-700 border-amber-100",
  warning: "bg-amber-50 text-amber-800 border-amber-200",
  failed: "bg-red-50 text-red-700 border-red-100",
  cancelled: "bg-slate-100 text-slate-600 border-slate-200",
  info: "bg-sky-50 text-sky-700 border-sky-100",
  neutral: "bg-slate-100 text-slate-600 border-slate-200",
};

export interface StatusPillProps extends HTMLAttributes<HTMLSpanElement> {
  variant: StatusVariant;
}

export const StatusPill = forwardRef<HTMLSpanElement, StatusPillProps>(
  function StatusPill({ variant, className, children, title, ...rest }, ref) {
    return (
      <span
        ref={ref}
        title={title ?? helpForState(children)}
        className={cn(
          "inline-flex items-center rounded-md border px-2 py-0.5 text-xs font-medium",
          VARIANT_CLASSES[variant],
          className,
        )}
        {...rest}
      >
        {children}
      </span>
    );
  },
);
