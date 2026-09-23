import { apiFetch, qs } from "./core";
import { type Usage } from "./runs";

export const usageApi = {
  listRateCards: () => apiFetch<{ items: unknown[] }>("/api/v1/rate-cards"),
  createRateCard: (body: Record<string, unknown>) =>
    apiFetch<unknown>("/api/v1/rate-cards", {
      method: "POST",
      body: JSON.stringify(body),
    }),
  getUsage: (q: {
    team_id?: string;
    start: string;
    end: string;
    group_by?: string;
    include_batches?: boolean;
  }) => apiFetch<Usage>(`/api/v1/usage${qs(q)}`),
};
