/**
 * Modal — light-weight overlay primitive. Renders into a portal so it
 * escapes any overflow:hidden parents. Closes on backdrop click,
 * Escape, or a programmatic onClose call.
 *
 * Focus management:
 *   - On open, capture `document.activeElement` (the element that
 *     was focused before the modal mounted).
 *   - Focus the first focusable element inside the modal so keyboard
 *     users land where they expect.
 *   - On close, restore focus to the previously-focused element
 *     (WCAG 2.4.3) so screen-reader users return to context.
 *
 * Accessibility: the backdrop sits as a *sibling* of the dialog so
 * the dialog's accessible name + tree isn't masked by `aria-hidden`.
 * The backdrop is decorative; the click target lives on a transparent
 * overlay button to catch close-on-outside-click without touching the
 * dialog's a11y tree.
 */
import { useEffect, useRef, type ReactNode } from "react";
import { createPortal } from "react-dom";

import { cn } from "../lib/cn";
import { MODAL_CLOSE_HELP } from "../lib/helpText";

export interface ModalProps {
  open: boolean;
  onClose: () => void;
  title: string;
  description?: string;
  children: ReactNode;
  footer?: ReactNode;
  /**
   * `sm` = 400px, `md` = 560px (default), `lg` = 720px.
   */
  size?: "sm" | "md" | "lg";
}

const SIZE_CLASSES: Record<NonNullable<ModalProps["size"]>, string> = {
  sm: "max-w-md",
  md: "max-w-xl",
  lg: "max-w-3xl",
};

export function Modal({
  open,
  onClose,
  title,
  description,
  children,
  footer,
  size = "md",
}: ModalProps): JSX.Element | null {
  const dialogRef = useRef<HTMLDivElement>(null);
  const previouslyFocusedRef = useRef<HTMLElement | null>(null);

  // Escape closes.
  useEffect(() => {
    if (!open) return;
    const onKey = (e: KeyboardEvent): void => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [open, onClose]);

  // Capture the previously focused element on open; focus first
  // focusable inside the modal; restore on close.
  useEffect(() => {
    if (!open) return;
    previouslyFocusedRef.current =
      document.activeElement instanceof HTMLElement
        ? document.activeElement
        : null;
    if (dialogRef.current) {
      const focusable = dialogRef.current.querySelector<HTMLElement>(
        'input, textarea, select, button:not([aria-label="close"])',
      );
      focusable?.focus();
    }
    return () => {
      previouslyFocusedRef.current?.focus?.();
    };
  }, [open]);

  if (!open) return null;

  return createPortal(
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4 animate-fade-in">
      {/* Backdrop — decorative, click-to-close. Sibling of the
          dialog so it doesn't mask the dialog's accessibility tree. */}
      <button
        type="button"
        onClick={onClose}
        aria-label="close modal"
        title={MODAL_CLOSE_HELP}
        className="absolute inset-0 modal-backdrop"
        tabIndex={-1}
      />
      <div
        ref={dialogRef}
        role="dialog"
        aria-modal="true"
        aria-labelledby="modal-title"
        aria-describedby={description ? "modal-description" : undefined}
        className={cn(
          "glass-card relative w-full overflow-hidden animate-slide-up",
          SIZE_CLASSES[size],
        )}
      >
        <div className="flex items-start justify-between gap-4 border-b border-slate-200 px-5 py-4">
          <div className="min-w-0">
            <h2
              id="modal-title"
              className="text-base font-semibold text-slate-900"
            >
              {title}
            </h2>
            {description ? (
              <p
                id="modal-description"
                className="mt-1 text-xs text-slate-500"
              >
                {description}
              </p>
            ) : null}
          </div>
          <button
            type="button"
            onClick={onClose}
            aria-label="close"
            title={MODAL_CLOSE_HELP}
            className="rounded-md p-1.5 text-slate-500 hover:bg-slate-100 hover:text-slate-700"
          >
            <svg
              width="16"
              height="16"
              viewBox="0 0 16 16"
              fill="none"
              aria-hidden="true"
            >
              <path
                d="M4 4l8 8M12 4l-8 8"
                stroke="currentColor"
                strokeWidth="1.5"
                strokeLinecap="round"
              />
            </svg>
          </button>
        </div>
        <div className="px-5 py-4">{children}</div>
        {footer ? (
          <div className="flex items-center justify-end gap-2 border-t border-slate-200 bg-slate-50/50 px-5 py-3">
            {footer}
          </div>
        ) : null}
      </div>
    </div>,
    document.body,
  );
}
