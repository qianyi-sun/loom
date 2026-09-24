import { useMemo, useState } from "react";

import { EMPTY_BENCHMARK_HELP } from "../../lib/helpText";
import { isTaskSetId, type BenchmarkItem } from "./taskSources";

function benchmarkSelectable(r: BenchmarkItem): boolean {
  if (typeof r.selectable === "boolean") return r.selectable;
  return (r.task_count ?? 0) > 0;
}

function benchmarkReadinessLabel(r: BenchmarkItem): string {
  if (r.readiness_label) return r.readiness_label;
  return benchmarkSelectable(r) ? "Ready" : "Needs publish";
}

function benchmarkReadinessMessage(r: BenchmarkItem): string | undefined {
  if (r.readiness_message) return r.readiness_message;
  return benchmarkSelectable(r) ? undefined : EMPTY_BENCHMARK_HELP;
}

function benchmarkCountText(r: BenchmarkItem): string | null {
  const valid = r.valid_task_config_count ?? r.task_count;
  const raw = r.raw_task_count;
  if (valid === undefined) return null;
  if (raw !== undefined && raw > valid) {
    return `${valid}/${raw} runnable`;
  }
  return `${valid} task${valid === 1 ? "" : "s"}`;
}

function benchmarkReadinessBadgeClass(r: BenchmarkItem): string {
  if (benchmarkSelectable(r)) {
    return "rounded border border-emerald-200 bg-emerald-50 px-1.5 py-0.5 text-[11px] font-medium text-emerald-700";
  }
  if (r.blocker_reason === "manifest_missing") {
    return "rounded border border-amber-200 bg-amber-50 px-1.5 py-0.5 text-[11px] font-medium text-amber-700";
  }
  return "rounded border border-rose-200 bg-rose-50 px-1.5 py-0.5 text-[11px] font-medium text-rose-700";
}

/**
 * Series-grouped task source multi-select.
 *
 * Groups rows by `series` (NULL → "Other" at the bottom). Each group
 * has a "Select all" affordance — the SPA's group-select path the
 * series catalog redesign was built for. The picker is purely
 * controlled; selection state lives in the parent.
 */
export function BenchmarkPicker({
  items,
  loading,
  selected,
  onChange,
  loadingLabel = "Loading…",
  emptyLabel = "No sources available.",
  sourceKind = "benchmark",
  flat = false,
}: {
  items: BenchmarkItem[];
  loading: boolean;
  selected: Set<string>;
  onChange: (next: Set<string>) => void;
  loadingLabel?: string;
  emptyLabel?: string;
  sourceKind?: "benchmark" | "TaskSet";
  /** Skip series group headers (use for a single-kind TaskSet list). */
  flat?: boolean;
}): JSX.Element {
  const [search, setSearch] = useState("");
  const groups = useMemo(() => {
    const bySeries = new Map<string, BenchmarkItem[]>();
    for (const b of items) {
      if (search.trim() && !`${b.display_name ?? ""} ${b.id} ${b.detail_label ?? ""}`.toLowerCase().includes(search.trim().toLowerCase())) continue;
      const key = b.series ?? "";
      const bucket = bySeries.get(key) ?? [];
      bucket.push(b);
      bySeries.set(key, bucket);
    }
    return Array.from(bySeries.entries())
      .map(([series, rows]) => ({
        series,
        rows: rows.sort((a, b) =>
          (a.display_name ?? a.id).localeCompare(b.display_name ?? b.id),
        ),
      }))
      .sort((a, b) => {
        // "Other" (empty series) sinks to the bottom.
        if (a.series === "" && b.series !== "") return 1;
        if (b.series === "" && a.series !== "") return -1;
        return a.series.localeCompare(b.series);
      });
  }, [items, search]);

  const toggleOne = (id: string): void => {
    const next = new Set(selected);
    if (next.has(id)) next.delete(id);
    else next.add(id);
    onChange(next);
  };
  const toggleGroup = (rows: BenchmarkItem[]): void => {
    const selectable = rows.filter(benchmarkSelectable);
    if (selectable.length === 0) return;
    const next = new Set(selected);
    const allOn = selectable.every((r) => next.has(r.id));
    for (const r of selectable) {
      if (allOn) next.delete(r.id);
      else next.add(r.id);
    }
    onChange(next);
  };

  if (loading && items.length === 0) {
    return <p className="mt-1 text-xs text-slate-500">{loadingLabel}</p>;
  }
  if (!loading && items.length === 0) {
    return <p className="mt-1 text-xs text-slate-500">{emptyLabel}</p>;
  }

  return (
    <div className="mt-1 max-h-72 min-w-0 overflow-x-hidden overflow-y-auto rounded-lg border border-slate-200 bg-white">
      <div className="sticky top-0 z-10 bg-white p-2"><input type="search" aria-label={`Search ${sourceKind} sources`} placeholder="Search by name or ID" value={search} onChange={(event) => setSearch(event.target.value)} className="w-full rounded-md border border-slate-200 px-3 py-2 text-sm" /></div>
      {groups.length === 0 ? <p className="p-3 text-sm text-slate-500">No sources match this search. Clear it to see all sources; your selections are retained.</p> : null}
      {groups.map(({ series, rows }) => {
        const seriesLabel = series === "" ? "Other" : series;
        const selectableRows = rows.filter(benchmarkSelectable);
        const allOn =
          selectableRows.length > 0 &&
          selectableRows.every((r) => selected.has(r.id));
        const someOn =
          !allOn && selectableRows.some((r) => selected.has(r.id));
        const populated = selectableRows.length;
        const rowPad = flat
          ? "flex min-w-0 items-center gap-2 px-3 py-1.5 text-sm"
          : "flex min-w-0 items-center gap-2 pl-9 pr-3 py-1.5 text-sm";
        return (
          <div key={seriesLabel || "flat"} className="min-w-0 border-b border-slate-100 last:border-b-0">
            {!flat ? (
              <label
                className={
                  populated > 0
                    ? "flex min-w-0 items-center gap-2 bg-indigo-50/60 px-3 py-2 text-sm font-semibold text-indigo-900"
                    : "flex min-w-0 items-center gap-2 bg-slate-50 px-3 py-2 text-sm font-semibold text-slate-600"
                }
              >
                <input
                  type="checkbox"
                  checked={allOn}
                  ref={(el) => {
                    if (el) el.indeterminate = someOn;
                  }}
                  onChange={() => toggleGroup(rows)}
                  disabled={populated === 0}
                  aria-label={`Select all in series ${seriesLabel}`}
                  title={
                    populated > 0
                      ? `Select or clear all ready sources in the ${seriesLabel} group.`
                      : `The ${seriesLabel} group has no ready sources to select.`
                  }
                  className="h-4 w-4 shrink-0 border-slate-300 disabled:cursor-not-allowed disabled:opacity-50"
                />
                <span
                  className={
                    populated > 0
                      ? "inline-block h-2 w-2 shrink-0 rounded-full bg-indigo-400"
                      : "inline-block h-2 w-2 shrink-0 rounded-full bg-slate-300"
                  }
                  aria-hidden="true"
                />
                <span className="min-w-0 truncate uppercase tracking-wider text-xs">
                  {seriesLabel}
                </span>
                <span
                  className={
                    populated > 0
                      ? "ml-auto shrink-0 font-normal normal-case text-xs text-indigo-800"
                      : "ml-auto shrink-0 font-normal normal-case text-xs text-slate-600"
                  }
                >
                  {populated}/{rows.length} ready
                </span>
              </label>
            ) : null}
            {rows.map((r) => {
              const label = r.display_name ?? r.id;
              const countText = benchmarkCountText(r);
              const selectable = benchmarkSelectable(r);
              const readinessLabel = benchmarkReadinessLabel(r);
              const readinessMessage = benchmarkReadinessMessage(r);
              const rowKind = isTaskSetId(r.id) ? "TaskSet" : sourceKind;
              return (
                <label
                  key={r.id}
                  className={
                    !selectable
                      ? `${rowPad} cursor-not-allowed text-slate-600`
                      : `${rowPad} text-slate-700 hover:bg-slate-50`
                  }
                  title={readinessMessage}
                >
                  <input
                    type="checkbox"
                    checked={selected.has(r.id)}
                    onChange={() => toggleOne(r.id)}
                    disabled={!selectable}
                    aria-label={`Select ${rowKind} ${r.id}`}
                    className="h-4 w-4 shrink-0 border-slate-300 disabled:cursor-not-allowed"
                  />
                  <span className="min-w-0 flex-1">
                    <span className="block truncate">{label}</span>
                    {r.detail_label ? <span className="block break-words text-xs text-slate-500">{r.detail_label}</span> : null}
                  </span>
                  {countText ? (
                    <span
                      className={
                        !selectable
                          ? "shrink-0 text-xs italic text-slate-600"
                          : "shrink-0 text-xs text-slate-600"
                      }
                    >
                      {countText}
                    </span>
                  ) : null}
                  <span className={`shrink-0 ${benchmarkReadinessBadgeClass(r)}`}>
                    {readinessLabel}
                  </span>
                </label>
              );
            })}
          </div>
        );
      })}
    </div>
  );
}
