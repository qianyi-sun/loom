import { apiDownload, apiFetch, qs } from "./core";

export type RunVisibility = "team" | "org" | "private";

export type ShareStatus = "pending_scan" | "shared" | "blocked";

export type ArtifactGroup =
  | "reports"
  | "trajectories"
  | "reusable_outputs"
  | "logs_diagnostics"
  | "raw_diagnostics";

export type RunLibraryOwnerTeam = import("./schema").components["schemas"]["RunLibraryOwnerTeam"];

export type RunLibraryArtifact = import("./schema").components["schemas"]["RunLibraryArtifact"];

export type ArtifactSummary = Record<ArtifactGroup, number>;

export type ArtifactInventory = Record<ArtifactGroup, RunLibraryArtifact[]>;

export type RunLibraryBatch = import("./schema").components["schemas"]["RunLibraryBatch"];

export type RunLibraryBatchDetail = import("./schema").components["schemas"]["RunLibraryBatchDetail"];

export type RunLibraryBatchList = import("./schema").components["schemas"]["RunLibraryBatchList"];

export type RetryPolicyView = import("./schema").components["schemas"]["RetryPolicyView"];

export type RetryDefaultSnapshotMismatch =
  import("./schema").components["schemas"]["RetryDefaultSnapshotMismatch"];

export type CloneRunLibraryBatchResult =
  import("./schema").components["schemas"]["CloneRunLibraryBatchResult"];

export type ReuseRunLibraryArtifactResult =
  import("./schema").components["schemas"]["ReuseRunLibraryArtifactResult"];

export const libraryApi = {
  listRunLibraryBatches: (q: Record<string, string | undefined> = {}) =>
    apiFetch<RunLibraryBatchList>(`/api/v1/run-library/batches${qs(q)}`),
  listRunLibraryArtifacts: (q: Record<string, string | undefined> = {}) =>
    apiFetch<{ items: RunLibraryArtifact[]; next_cursor: string | null }>(
      `/api/v1/run-library/artifacts${qs(q)}`,
    ),
  getRunLibraryBatch: (id: string, includeDebug = false) =>
    apiFetch<RunLibraryBatchDetail>(
      `/api/v1/run-library/batches/${id}${qs({
        include_debug: includeDebug ? "true" : undefined,
      })}`,
    ),
  cloneRunLibraryBatchConfig: (
    id: string,
    body: {
      name: string;
      description?: string | null;
      provider_connection_id?: string | null;
      provider_model_id?: string | null;
    },
  ) =>
    apiFetch<CloneRunLibraryBatchResult>(`/api/v1/run-library/batches/${id}/clone-config`, {
      method: "POST",
      body: JSON.stringify(body),
    }),
  reuseRunLibraryArtifact: (
    trialId: string,
    body: {
      key: string;
      name: string;
      description?: string | null;
      provider_connection_id?: string | null;
      provider_model_id?: string | null;
    },
  ) =>
    apiFetch<ReuseRunLibraryArtifactResult>(`/api/v1/run-library/trials/${trialId}/artifacts/reuse`, {
      method: "POST",
      body: JSON.stringify(body),
    }),
  downloadRunLibraryArtifact: (trialId: string, key: string, filename: string) =>
    apiDownload(`/api/v1/run-library/trials/${trialId}/artifacts/download${qs({ key })}`, filename),
  exportRunLibraryArtifacts: (
    q: Record<string, string | undefined> = {},
    filename = "run-library-artifacts.jsonl",
  ) => apiDownload(`/api/v1/run-library/artifacts/export${qs(q)}`, filename),
};
