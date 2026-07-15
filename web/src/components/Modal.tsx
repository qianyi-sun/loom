/**
 * Modal — the shared accessible dialog boundary.
 *
 * The primitive owns labelling, focus containment/restoration, Escape and
 * backdrop dismissal, background inertness, and body scroll locking. Callers
 * only provide dialog content and actions.
 */
import {
  useEffect,
  useId,
  useLayoutEffect,
  useRef,
  useState,
  type ReactNode,
} from "react";
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
  /** Whether Escape, the backdrop, and the close button may dismiss the dialog. */
  dismissible?: boolean;
  /** `sm` = 400px, `md` = 560px (default), `lg` = 720px. */
  size?: "sm" | "md" | "lg";
}

const SIZE_CLASSES: Record<NonNullable<ModalProps["size"]>, string> = {
  sm: "max-w-md",
  md: "max-w-xl",
  lg: "max-w-3xl",
};

const FOCUSABLE_SELECTOR = [
  "a[href]",
  "area[href]",
  "button:not(:disabled)",
  "input:not(:disabled):not([type='hidden'])",
  "select:not(:disabled)",
  "textarea:not(:disabled)",
  "[contenteditable='true']",
  "[tabindex]:not([tabindex='-1'])",
].join(",");

interface BackgroundState {
  ariaHidden: string | null;
  element: HTMLElement;
  inert: string | null;
}

let modalLayer: HTMLDivElement | null = null;
let environmentLocks = 0;
let backgroundState: BackgroundState[] = [];
let backgroundObserver: MutationObserver | null = null;
let previousBodyOverflow = "";
let environmentRestoreTarget: HTMLElement | null = null;
const dialogStack: HTMLElement[] = [];

function ensureModalLayer(): HTMLDivElement {
  if (modalLayer?.isConnected) return modalLayer;
  modalLayer = document.createElement("div");
  modalLayer.dataset.loomModalLayer = "true";
  document.body.appendChild(modalLayer);
  return modalLayer;
}

function hideNewBackgroundElements(layer: HTMLElement): void {
  for (const element of Array.from(document.body.children)) {
    if (!(element instanceof HTMLElement) || element === layer) continue;
    if (backgroundState.some((state) => state.element === element)) continue;
    backgroundState.push({
      ariaHidden: element.getAttribute("aria-hidden"),
      element,
      inert: element.getAttribute("inert"),
    });
    element.setAttribute("inert", "");
    element.setAttribute("aria-hidden", "true");
  }
}

function acquireModalEnvironment(layer: HTMLElement): () => boolean {
  const isFirstLock = environmentLocks === 0;
  if (isFirstLock) {
    environmentRestoreTarget =
      document.activeElement instanceof HTMLElement
        ? document.activeElement
        : null;
    previousBodyOverflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";
  }
  hideNewBackgroundElements(layer);
  environmentLocks += 1;
  if (isFirstLock) {
    backgroundObserver = new MutationObserver(() => {
      if (environmentLocks > 0 && layer.isConnected) {
        hideNewBackgroundElements(layer);
      }
    });
    backgroundObserver.observe(document.body, { childList: true });
  }
  let released = false;

  return () => {
    if (released) return false;
    released = true;
    environmentLocks = Math.max(0, environmentLocks - 1);
    if (environmentLocks !== 0) return false;

    backgroundObserver?.disconnect();
    backgroundObserver = null;
    document.body.style.overflow = previousBodyOverflow;
    for (const state of backgroundState) {
      if (state.inert === null) state.element.removeAttribute("inert");
      else state.element.setAttribute("inert", state.inert);
      if (state.ariaHidden === null) state.element.removeAttribute("aria-hidden");
      else state.element.setAttribute("aria-hidden", state.ariaHidden);
    }
    backgroundState = [];
    const restoreTarget = environmentRestoreTarget;
    environmentRestoreTarget = null;
    if (restoreTarget?.isConnected) restoreTarget.focus();
    return true;
  };
}

function focusableElements(dialog: HTMLElement): HTMLElement[] {
  return Array.from(dialog.querySelectorAll<HTMLElement>(FOCUSABLE_SELECTOR)).filter(
    (element) =>
      !element.hidden &&
      element.tabIndex >= 0 &&
      !element.closest("[hidden], [inert], [aria-hidden='true']"),
  );
}

function initialFocusTarget(dialog: HTMLElement): HTMLElement {
  const focusable = focusableElements(dialog);
  return (
    focusable.find((element) => element.dataset.modalClose !== "true") ??
    focusable[0] ??
    dialog
  );
}

function isTopDialog(dialog: HTMLElement): boolean {
  return dialogStack[dialogStack.length - 1] === dialog;
}

function syncDialogStackAccessibility(): void {
  const topDialog = dialogStack[dialogStack.length - 1];
  for (const dialog of dialogStack) {
    const overlay = dialog.closest<HTMLElement>('[data-loom-modal-overlay="true"]');
    if (!overlay) continue;
    if (dialog === topDialog) {
      overlay.removeAttribute("inert");
      overlay.removeAttribute("aria-hidden");
    } else {
      overlay.setAttribute("inert", "");
      overlay.setAttribute("aria-hidden", "true");
    }
  }
}

type ModalContentProps = Omit<ModalProps, "open"> & {
  descriptionId: string;
  layer: HTMLDivElement;
  titleId: string;
};

export function Modal({ open, ...contentProps }: ModalProps): JSX.Element | null {
  const [layer, setLayer] = useState<HTMLDivElement | null>(null);
  const generatedId = useId();
  const titleId = `${generatedId}-title`;
  const descriptionId = `${generatedId}-description`;

  useEffect(() => {
    if (!open || typeof document === "undefined") return;
    setLayer(ensureModalLayer());
  }, [open]);

  if (!open || !layer?.isConnected) return null;

  return createPortal(
    <ModalContent
      {...contentProps}
      descriptionId={descriptionId}
      layer={layer}
      titleId={titleId}
    />,
    layer,
  );
}

function ModalContent({
  onClose,
  title,
  description,
  children,
  footer,
  dismissible = true,
  size = "md",
  descriptionId,
  layer,
  titleId,
}: ModalContentProps): JSX.Element {
  const dialogRef = useRef<HTMLDivElement>(null);
  const onCloseRef = useRef(onClose);
  const dismissibleRef = useRef(dismissible);
  onCloseRef.current = onClose;
  dismissibleRef.current = dismissible;

  useLayoutEffect(() => {
    const dialog = dialogRef.current;
    if (!dialog) return;
    const layerRestoreTarget =
      document.activeElement instanceof HTMLElement
        ? document.activeElement
        : null;
    const releaseEnvironment = acquireModalEnvironment(layer);
    dialogStack.push(dialog);
    syncDialogStackAccessibility();

    const keepFocusInside = (): void => {
      if (isTopDialog(dialog)) initialFocusTarget(dialog).focus();
    };

    const handleKeyDown = (event: KeyboardEvent): void => {
      if (!isTopDialog(dialog)) return;
      if (event.key === "Escape") {
        event.preventDefault();
        event.stopPropagation();
        if (dismissibleRef.current) onCloseRef.current();
        return;
      }
      if (event.key !== "Tab") return;

      const focusable = focusableElements(dialog);
      if (focusable.length === 0) {
        event.preventDefault();
        dialog.focus();
        return;
      }
      const first = focusable[0];
      const last = focusable[focusable.length - 1];
      const active = document.activeElement;
      if (!active || !dialog.contains(active)) {
        event.preventDefault();
        (event.shiftKey ? last : first).focus();
      } else if (event.shiftKey && active === first) {
        event.preventDefault();
        last.focus();
      } else if (!event.shiftKey && active === last) {
        event.preventDefault();
        first.focus();
      }
    };

    const handleFocusIn = (event: FocusEvent): void => {
      if (
        isTopDialog(dialog) &&
        event.target instanceof Node &&
        !dialog.contains(event.target)
      ) {
        event.stopPropagation();
        keepFocusInside();
      }
    };

    window.addEventListener("keydown", handleKeyDown, true);
    document.addEventListener("focusin", handleFocusIn, true);
    initialFocusTarget(dialog).focus();

    return () => {
      window.removeEventListener("keydown", handleKeyDown, true);
      document.removeEventListener("focusin", handleFocusIn, true);
      const wasTopDialog = isTopDialog(dialog);
      const stackIndex = dialogStack.lastIndexOf(dialog);
      if (stackIndex >= 0) dialogStack.splice(stackIndex, 1);
      syncDialogStackAccessibility();
      const releasedFinalEnvironmentLock = releaseEnvironment();
      const topDialog = dialogStack[dialogStack.length - 1];
      if (!releasedFinalEnvironmentLock && topDialog?.isConnected) {
        if (
          wasTopDialog &&
          layerRestoreTarget?.isConnected &&
          topDialog.contains(layerRestoreTarget)
        ) {
          layerRestoreTarget.focus();
          if (!topDialog.contains(document.activeElement)) {
            initialFocusTarget(topDialog).focus();
          }
        } else if (!topDialog.contains(document.activeElement)) {
          initialFocusTarget(topDialog).focus();
        }
      }
    };
  }, [layer]);

  return (
    <div
      data-loom-modal-overlay="true"
      className="fixed inset-0 z-50 flex items-center justify-center p-4 animate-fade-in"
    >
      {dismissible ? (
        <button
          type="button"
          onClick={onClose}
          aria-label="close modal"
          title={MODAL_CLOSE_HELP}
          className="absolute inset-0 modal-backdrop"
          tabIndex={-1}
        />
      ) : (
        <div aria-hidden="true" className="absolute inset-0 modal-backdrop" />
      )}
      <div
        ref={dialogRef}
        role="dialog"
        aria-modal="true"
        aria-labelledby={titleId}
        aria-describedby={description ? descriptionId : undefined}
        tabIndex={-1}
        className={cn(
          "glass-card relative w-full overflow-hidden animate-slide-up",
          SIZE_CLASSES[size],
        )}
      >
        <div className="flex items-start justify-between gap-4 border-b border-slate-200 px-5 py-4">
          <div className="min-w-0">
            <h2 id={titleId} className="text-base font-semibold text-slate-900">
              {title}
            </h2>
            {description ? (
              <p id={descriptionId} className="mt-1 text-xs text-slate-500">
                {description}
              </p>
            ) : null}
          </div>
          {dismissible ? (
            <button
              type="button"
              onClick={onClose}
              aria-label="close"
              title={MODAL_CLOSE_HELP}
              data-modal-close="true"
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
          ) : null}
        </div>
        <div className="px-5 py-4">{children}</div>
        {footer ? (
          <div className="flex items-center justify-end gap-2 border-t border-slate-200 bg-slate-50/50 px-5 py-3">
            {footer}
          </div>
        ) : null}
      </div>
    </div>
  );
}
