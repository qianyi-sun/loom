export interface TaskFilterSummary {
  primary: string;
  details: string[];
  diagnostics: string[];
}

export interface TaskFilterSummaryContext {
  matchedTaskCount?: number;
}

const KNOWN_TASK_FILTER_KEYS = new Set([
  "benchmark_id",
  "benchmark_ids",
  "license",
  "n",
  "seed",
  "subset_kind",
  "tag_filters",
  "task_ids",
]);

const BENCHMARK_LABELS: Record<string, string> = {
  "aime-22": "AIME 2022",
  humaneval: "HumanEval",
  mbpp: "MBPP",
};

export function humanizeTaskFilter(
  filter: Record<string, unknown>,
  context: TaskFilterSummaryContext = {},
): TaskFilterSummary {
  const subset = String(filter.subset_kind ?? "all");
  const benchmarkIds = benchmarkIdsFromFilter(filter);
  const details = detailsFromFilter(filter, benchmarkIds);
  const diagnostics = diagnosticsFromUnknownKeys(filter, KNOWN_TASK_FILTER_KEYS);

  if (subset === "explicit") {
    const ids = Array.isArray(filter.task_ids)
      ? filter.task_ids.map(String)
      : [];
    return {
      primary: `${ids.length} explicit task ID${ids.length === 1 ? "" : "s"}`,
      details: ids.slice(0, 5),
      diagnostics: [
        ...diagnostics,
        ...(ids.length > 5 ? [`${ids.length - 5} more IDs hidden`] : []),
      ],
    };
  }

  const benchmarkLabel = benchmarkSummary(benchmarkIds);

  if (subset === "first_n" || subset === "last_n") {
    const direction = subset === "first_n" ? "first" : "last";
    return {
      primary: `${benchmarkLabel} / ${direction} ${displayNumber(filter.n)} tasks`,
      details,
      diagnostics,
    };
  }

  if (subset === "random_n") {
    return {
      primary: `${benchmarkLabel} / random ${displayNumber(filter.n)} tasks / seed ${displayNumber(filter.seed)}`,
      details,
      diagnostics,
    };
  }

  const countText =
    typeof context.matchedTaskCount === "number"
      ? `${context.matchedTaskCount} task${context.matchedTaskCount === 1 ? "" : "s"}`
      : "all matching tasks";
  return {
    primary: `${benchmarkLabel} / all runnable tasks / ${countText}`,
    details,
    diagnostics,
  };
}

function benchmarkIdsFromFilter(filter: Record<string, unknown>): string[] {
  if (Array.isArray(filter.benchmark_ids)) {
    return filter.benchmark_ids.map(String);
  }
  if (typeof filter.benchmark_id === "string" && filter.benchmark_id) {
    return [filter.benchmark_id];
  }
  return [];
}

function benchmarkSummary(ids: string[]): string {
  if (ids.length === 0) return "Selected tasks";
  if (ids.length === 1) return displayBenchmark(ids[0]);
  return `${ids.length} benchmarks`;
}

function detailsFromFilter(
  filter: Record<string, unknown>,
  benchmarkIds: string[],
): string[] {
  const details = benchmarkIds.map((id) => `Benchmark: ${id}`);
  if (typeof filter.license === "string" && filter.license) {
    details.push(`License: ${filter.license}`);
  }
  const tagFilters = humanizeTagFilters(filter.tag_filters);
  if (tagFilters.length > 0) details.push(...tagFilters);
  return details;
}

function humanizeTagFilters(value: unknown): string[] {
  if (!value || typeof value !== "object" || Array.isArray(value)) return [];
  return Object.entries(value as Record<string, unknown>).flatMap(
    ([key, raw]) => {
      const vals = Array.isArray(raw) ? raw.map(String) : [String(raw)];
      return vals.length > 0 ? [`Tag ${key}: ${vals.join(", ")}`] : [];
    },
  );
}

function diagnosticsFromUnknownKeys(
  value: Record<string, unknown>,
  known: Set<string>,
): string[] {
  return Object.keys(value)
    .filter((key) => !known.has(key))
    .sort()
    .map((key) => `Unrecognized field: ${key}`);
}

function displayBenchmark(id: string): string {
  return BENCHMARK_LABELS[id] ?? id;
}

function displayNumber(value: unknown): string {
  const n = Number(value);
  return Number.isFinite(n) ? String(n) : "?";
}
