/**
 * Adaptive refresh policy — produces a `refetchInterval` value for
 * react-query (or any poller) that respects the user's attention.
 *
 * Three knobs in (caller-controlled):
 *   - `baseIntervalMs` — the ideal cadence when the page is the active tab.
 *   - `minIntervalMs` / `maxIntervalMs` — clamps the result.
 *   - `hiddenBehavior` / `blurBehavior` — how aggressively to throttle
 *     when the document is hidden or the window has lost focus.
 *
 * One knob out: the `refetchInterval` to hand to react-query (or
 * `false` to pause polling entirely).
 *
 * The freshness hint lets callers slow further when they know their
 * data is intrinsically slow-moving — e.g. an Overview dashboard
 * whose backend caches stats with a TTL. The default is "active",
 * meaning the caller has no opinion.
 */

export type HiddenBehavior = "pause" | "slow";
export type BlurBehavior = "active" | "slow";
export type RefreshFreshnessHint = "fresh" | "active" | "stale";

export interface RefreshPolicyOptions {
  baseIntervalMs: number;
  minIntervalMs?: number;
  maxIntervalMs?: number;
  hiddenBehavior?: HiddenBehavior;
  blurBehavior?: BlurBehavior;
  freshness?: RefreshFreshnessHint;
  manualPaused?: boolean;
  visible?: boolean;
  focused?: boolean;
}

export interface RefreshPolicyDecision {
  refetchInterval: number | false;
  reason:
    | "paused"
    | "hidden"
    | "blurred"
    | "fresh"
    | "stale"
    | "active";
}

const DEFAULT_MIN = 1_000;
const DEFAULT_MAX = 5 * 60_000;

/**
 * Pure function — given the current visibility/focus/hint state,
 * return the next `refetchInterval`. No side effects, no I/O — easy
 * to unit-test.
 */
export function calculateRefreshPolicy(
  options: RefreshPolicyOptions,
): RefreshPolicyDecision {
  const {
    baseIntervalMs,
    minIntervalMs = DEFAULT_MIN,
    maxIntervalMs = DEFAULT_MAX,
    hiddenBehavior = "pause",
    blurBehavior = "slow",
    freshness = "active",
    manualPaused = false,
    visible = true,
    focused = true,
  } = options;

  if (manualPaused) {
    return { refetchInterval: false, reason: "paused" };
  }

  if (!visible && hiddenBehavior === "pause") {
    return { refetchInterval: false, reason: "hidden" };
  }

  let interval = baseIntervalMs;

  if (!visible && hiddenBehavior === "slow") {
    interval = Math.max(interval, baseIntervalMs * 4);
  } else if (!focused && blurBehavior === "slow") {
    interval = Math.max(interval, baseIntervalMs * 2);
  }

  if (freshness === "fresh") {
    interval = Math.max(interval, baseIntervalMs * 2);
  } else if (freshness === "stale") {
    interval = Math.min(interval, Math.max(minIntervalMs, baseIntervalMs / 2));
  }

  interval = Math.max(minIntervalMs, Math.min(maxIntervalMs, interval));

  let reason: RefreshPolicyDecision["reason"] = "active";
  if (!visible) reason = "hidden";
  else if (!focused) reason = "blurred";
  else if (freshness === "fresh") reason = "fresh";
  else if (freshness === "stale") reason = "stale";

  return { refetchInterval: interval, reason };
}
