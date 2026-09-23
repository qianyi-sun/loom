import type { components } from "./schema";

/** Normalize optional wire collections and legacy diagnostic payloads for the UI. */
export function taskSetDetailView(data: components["schemas"]["TaskSetDetailResponse"]) {
  return {
    ...data,
    warnings: data.warnings ?? [],
    error_summary: (data.error_summary ?? []).map((value, index) => {
      const record = value !== null && typeof value === "object" ? (value as Record<string, unknown>) : {};
      return {
        instance_index: typeof record.instance_index === "number" ? record.instance_index : index,
        code: typeof record.code === "string" ? record.code : "unknown",
        message: typeof record.message === "string" ? record.message : JSON.stringify(value),
      };
    }),
  };
}
export type TaskSetDetailView = ReturnType<typeof taskSetDetailView>;
