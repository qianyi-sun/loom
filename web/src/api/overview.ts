import { apiFetch } from "./core";

export type OverviewStatus = "ready" | "needs_setup" | "blocked";

export type OverviewActionKind = "user" | "operator";

export type OverviewAction = import("./schema").components["schemas"]["OverviewAction"];

export type OverviewSummary = import("./schema").components["schemas"]["OverviewSummary"];

export const overviewApi = {
  getOverview: () => apiFetch<OverviewSummary>("/api/v1/overview", { cache: "no-store" }),
};
