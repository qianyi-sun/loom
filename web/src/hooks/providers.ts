/**
 * Provider-connection mutation hooks (#167).
 *
 * Each hook wraps a single api method + handles React Query
 * invalidation. Read-side uses inline `useQuery` in components
 * (matches existing Settings.tsx pattern).
 *
 * Invalidation strategy (targeted, not bulk):
 * - create / update / test     → invalidate ["providers"] + ["providers", id]
 * - delete                     → invalidate ["providers"] + remove ["providers", id]
 * - addModel / refresh / preflight / hide / unhide → invalidate ["providers", id, "models"]
 */
import { useMutation, useQueryClient } from "@tanstack/react-query";

import { api } from "../api/client";

export function useCreateConnection() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (payload: Parameters<typeof api.createProviderConnection>[0]) =>
      api.createProviderConnection(payload),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["providers"] });
    },
  });
}

export function useEditConnection(id: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (patch: Parameters<typeof api.updateProviderConnection>[1]) =>
      api.updateProviderConnection(id, patch),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["providers"] });
      qc.invalidateQueries({ queryKey: ["providers", id] });
    },
  });
}

export function useRotateConnectionKey(id: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (newKey: string) =>
      api.updateProviderConnection(id, { api_key: newKey }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["providers"] });
      qc.invalidateQueries({ queryKey: ["providers", id] });
    },
  });
}

export function useDeleteConnection() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (id: string) => api.deleteProviderConnection(id),
    onSuccess: (_void, id) => {
      qc.invalidateQueries({ queryKey: ["providers"] });
      qc.removeQueries({ queryKey: ["providers", id] });
    },
  });
}

export function useTestConnection(id: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: () => api.testProviderConnection(id),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["providers"] });
      qc.invalidateQueries({ queryKey: ["providers", id] });
    },
  });
}

export function useRefreshModels(id: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: () => api.refreshProviderConnectionModels(id),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["providers", id, "models"] });
    },
  });
}

export function usePreflightModel(id: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (modelId: string) =>
      api.preflightProviderConnectionModel(id, modelId),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["providers", id, "models"] });
      qc.invalidateQueries({ queryKey: ["models"] });
    },
  });
}

export function useAddManualModel(id: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (model: Parameters<typeof api.addProviderConnectionModel>[1]) =>
      api.addProviderConnectionModel(id, model),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["providers", id, "models"] });
    },
  });
}

export function useHideModel(id: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (modelId: string) =>
      api.hideProviderConnectionModel(id, modelId),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["providers", id, "models"] });
    },
  });
}

export function useUnhideModel(id: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (modelId: string) =>
      api.unhideProviderConnectionModel(id, modelId),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["providers", id, "models"] });
    },
  });
}
