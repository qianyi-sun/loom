/**
 * Status-string → visual-variant mappings. Kept here (not in
 * StatusPill.tsx) so the component file stays component-only and
 * Vite's react-refresh can hot-reload it cleanly.
 *
 * Adding a new server-side state? Add a case here. The default
 * `neutral` is a deliberate fallback so an unknown state shows as
 * a grey pill rather than crashing the page.
 */
import type { StatusVariant } from "../components/StatusPill";

/** Map a `trial.state` from the Loom API to a `StatusPill` variant. */
export function trialStateVariant(state: string): StatusVariant {
  switch (state) {
    case "succeeded":
      return "success";
    case "running":
    case "claimed":
      return "running";
    case "queued":
    case "submitted":
      return "queued";
    case "failed":
    case "failed_terminal":
      return "failed";
    case "cancelled":
      return "cancelled";
    default:
      return "neutral";
  }
}

/** Map a `campaign.state` from the Loom API to a `StatusPill` variant. */
export function campaignStateVariant(state: string): StatusVariant {
  switch (state) {
    case "finished":
      return "success";
    case "running":
      return "running";
    case "submitted":
      return "queued";
    case "cancelled":
      return "cancelled";
    default:
      return "neutral";
  }
}
