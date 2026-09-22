/**
 * #2009: staleness check for the persistent version entry.
 *
 * Two independent signals, both polled only on window focus and throttled
 * via react-query's `staleTime` (built-in request dedup/throttling — no
 * hand-rolled timer):
 *   - the frontend's own *currently served* build (a fresh fetch of
 *     `loom-frontend-config.json`), compared against the *loaded* build
 *     this tab's JS was actually built from (buildInfo.ts, frozen, never
 *     refetched);
 *   - the backend's own build, from whichever instance answers the
 *     request — evidence for that instance only, not proof every replica
 *     has rolled.
 *
 * Neither check ever triggers a reload or discards anything; they only
 * inform the explicit refresh affordance in the UI. A failed fetch reports
 * "unknown", never a false "update available".
 */
import { useQuery, type UseQueryResult } from "@tanstack/react-query";

import { apiFetch } from "../api/client";
import { fetchServedBuildInfo } from "./frontendConfig";

const STALE_TIME_MS = 60_000;

export interface BackendVersion {
  buildRevision: string | null;
  buildTime: string | null;
}

async function fetchBackendVersion(): Promise<BackendVersion | null> {
  try {
    return await apiFetch<BackendVersion>("/api/v1/version");
  } catch {
    return null;
  }
}

export function useServedFrontendBuild(): UseQueryResult<
  { revision: string | null; sourceRef: string | null; buildTime: string | null } | null
> {
  return useQuery({
    queryKey: ["build-version", "frontend-served"],
    queryFn: fetchServedBuildInfo,
    refetchOnWindowFocus: true,
    staleTime: STALE_TIME_MS,
    retry: false,
  });
}

export function useBackendVersion(): UseQueryResult<BackendVersion | null> {
  return useQuery({
    queryKey: ["build-version", "backend"],
    queryFn: fetchBackendVersion,
    refetchOnWindowFocus: true,
    staleTime: STALE_TIME_MS,
    retry: false,
  });
}

export interface FrontendUpdateStatus {
  /** True only when both the loaded and served revisions are known and
   * differ — a failed/unknown served fetch never claims an update. */
  hasNewerBuild: boolean;
  servedRevision: string | null;
}

/**
 * Pure by design (takes the loaded revision as a parameter rather than
 * reading buildInfo.ts's module-level constant): easy to exercise both
 * branches in a unit test, where the "loaded" identity is otherwise
 * always empty (no VITE_BUILD_REVISION for a plain `vitest run`).
 */
export function frontendUpdateStatus(
  loadedRevision: string | null,
  served: { revision: string | null } | null | undefined,
): FrontendUpdateStatus {
  const servedRevision = served?.revision ?? null;
  const hasNewerBuild =
    loadedRevision !== null &&
    servedRevision !== null &&
    servedRevision !== loadedRevision;
  return { hasNewerBuild, servedRevision };
}
