/**
 * #2009: staleness check for the persistent version entry.
 *
 * Two independent signals, both throttled via react-query's `staleTime`
 * (built-in request dedup/throttling — no hand-rolled timer):
 *   - the frontend's own *currently served* build, compared against the
 *     *loaded* build this tab's JS was actually built from (buildInfo.ts,
 *     frozen, never refetched). The served value seeds from the config
 *     `loadFrontendConfig()` already fetched at startup — no extra request
 *     on mount, only ever a fresh fetch when the window regains focus.
 *     Startup's own fetch is the app's single source of truth for routing
 *     (`apiBase`/`apiRouteBase`); an independent fetch here on every mount
 *     would double it and break the "runtime config loads once per
 *     navigation" contract the app relies on.
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
import { fetchServedBuildInfo, getFrontendConfig } from "./frontendConfig";

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
    // Seed from startup's own already-fetched config instead of issuing a
    // second request for the same resource on mount.
    initialData: () => {
      const config = getFrontendConfig();
      return {
        revision: config.servedBuildRevision ?? null,
        sourceRef: config.servedSourceRef ?? null,
        buildTime: config.servedBuildTime ?? null,
      };
    },
    refetchOnMount: false,
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
