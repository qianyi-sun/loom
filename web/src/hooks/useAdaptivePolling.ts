/**
 * React hook over `calculateRefreshPolicy` that wires up the
 * document-visibility + window-focus listeners and exposes the
 * `refetchInterval` decision back to the caller. Designed to be
 * dropped into a react-query `useQuery({ refetchInterval })`.
 *
 * Lifecycle:
 *   - On mount: read the current `document.visibilityState` and
 *     `document.hasFocus()` so we don't briefly poll-on-hidden when
 *     a user revisits a tab that was loaded but never focused.
 *   - On `visibilitychange` / `focus` / `blur`: re-read and update.
 *   - On unmount: detach listeners.
 *
 * Caller controls the cadence (base / min / max) and the
 * behaviour-on-hidden / on-blur. Pure visibility tracking is in
 * the hook; the math is in `calculateRefreshPolicy` so it's
 * testable without a DOM.
 */
import { useCallback, useEffect, useMemo, useState } from "react";

import {
  calculateRefreshPolicy,
  type RefreshFreshnessHint,
  type RefreshPolicyDecision,
  type RefreshPolicyOptions,
} from "../lib/refreshPolicy";

export interface UseAdaptivePollingOptions
  extends Omit<
    RefreshPolicyOptions,
    "visible" | "focused" | "manualPaused"
  > {
  /**
   * Top-level kill switch — when false, polling is paused regardless
   * of visibility/focus. Useful for screens where data is intrinsically
   * static after first load.
   */
  enabled?: boolean;
  /**
   * Initial value for the user-facing pause toggle. Defaults to
   * "not paused".
   */
  initialPaused?: boolean;
}

export interface UseAdaptivePollingResult extends RefreshPolicyDecision {
  isManuallyPaused: boolean;
  visibilityState: DocumentVisibilityState;
  isFocused: boolean;
  pause: () => void;
  resume: () => void;
  setPaused: (paused: boolean) => void;
}

function readVisibility(): DocumentVisibilityState {
  return typeof document === "undefined" ? "visible" : document.visibilityState;
}

function readFocus(): boolean {
  return typeof document === "undefined" || typeof document.hasFocus !== "function"
    ? true
    : document.hasFocus();
}

export function useAdaptivePolling(
  options: UseAdaptivePollingOptions,
): UseAdaptivePollingResult {
  const { enabled = true, initialPaused = false, ...policy } = options;
  const [isManuallyPaused, setIsManuallyPaused] = useState(initialPaused);
  const [visibilityState, setVisibilityState] = useState<DocumentVisibilityState>(
    readVisibility,
  );
  const [isFocused, setIsFocused] = useState<boolean>(readFocus);

  useEffect(() => {
    if (typeof document === "undefined" || typeof window === "undefined") {
      return;
    }
    const onVisibility = (): void => setVisibilityState(document.visibilityState);
    const onFocus = (): void => setIsFocused(true);
    const onBlur = (): void => setIsFocused(false);
    document.addEventListener("visibilitychange", onVisibility);
    window.addEventListener("focus", onFocus);
    window.addEventListener("blur", onBlur);
    return () => {
      document.removeEventListener("visibilitychange", onVisibility);
      window.removeEventListener("focus", onFocus);
      window.removeEventListener("blur", onBlur);
    };
  }, []);

  // Memoise so the returned object identity is stable across renders
  // when nothing observable has changed — callers can drop the
  // decision into a react-query options object without churning
  // dependency arrays. We list each scalar field instead of `policy`
  // itself because `policy` is a fresh object on every call; the
  // eslint rule warns because it doesn't know about that pattern.
  const decision = useMemo(
    () =>
      calculateRefreshPolicy({
        ...policy,
        manualPaused: !enabled || isManuallyPaused,
        visible: visibilityState === "visible",
        focused: isFocused,
      }),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [
      policy.baseIntervalMs,
      policy.minIntervalMs,
      policy.maxIntervalMs,
      policy.hiddenBehavior,
      policy.blurBehavior,
      policy.freshness,
      enabled,
      isManuallyPaused,
      visibilityState,
      isFocused,
    ],
  );

  const pause = useCallback(() => setIsManuallyPaused(true), []);
  const resume = useCallback(() => setIsManuallyPaused(false), []);

  return {
    ...decision,
    isManuallyPaused,
    visibilityState,
    isFocused,
    pause,
    resume,
    setPaused: setIsManuallyPaused,
  };
}

export type { RefreshFreshnessHint };
