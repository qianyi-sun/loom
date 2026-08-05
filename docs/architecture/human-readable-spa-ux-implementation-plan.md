# Human-Readable Frontend UX Implementation Plan

Status: planning
Last updated: 2026-06-19

**Goal:** Make Loom's SPA default views human-readable while preserving raw/internal data in explicit diagnostics panels.

**Architecture:** Add shared explanation and diagnostics primitives, then replace page-local raw JSON displays with pure humanizer functions plus compact summary components. Keep API request/response shapes unchanged; presentation logic lives in `web/src/components` and `web/src/lib`.

**Tech Stack:** React 18, TypeScript, TanStack Query, Vite, Vitest, Testing Library, Tailwind CSS.

---

## Scope Check

The spec covers the whole SPA, so implementation should be split into separate
GitHub issues and PRs. Each PR below is independently testable and should target
`dev`. Do not combine the full plan into one PR.

## File Map

- Create `web/src/components/InfoHint.tsx`: compact explanatory copy for labels, groups, and metric definitions.
- Create `web/src/components/DiagnosticPanel.tsx`: shared collapsed diagnostics wrapper around named raw/internal data blocks.
- Create `web/src/components/CopyableId.tsx`: shortened IDs with copy affordance.
- Create `web/src/lib/humanizeTaskFilter.ts`: pure task-filter summaries.
- Create `web/src/lib/humanizeTrialConfig.ts`: pure trial-config summaries.
- Create `web/src/lib/humanizeFailureReason.ts`: known failure-code labels and descriptions.
- Modify `web/src/lib/helpText.ts`: reuse existing state descriptions through page-facing helpers.
- Modify `web/src/pages/BatchDetail.tsx`: replace `Filter + config` with `Run plan` and diagnostics.
- Modify `web/src/pages/NewBatch.tsx`: improve task-selection, combination, advanced-option, and launch-summary copy.
- Modify `web/src/pages/Monitor.tsx`: clarify table headings, filters, state help, and reward semantics.
- Modify `web/src/pages/TrialDetail.tsx`: clarify outcome, failures, artifacts, and download labels.
- Modify `web/src/components/EventTimeline.tsx`: make raw event JSON a row-level diagnostic disclosure.
- Modify `web/src/components/AgentModelPicker.tsx`: rename raw model mode and explain model-source choices.
- Modify `web/src/pages/ProviderDetail.tsx`: humanize provider readiness, allowed models, and test failures.
- Modify `web/src/pages/ProvidersList.tsx`: explain provider status and ready-state summary.
- Modify `web/src/pages/RateCardsAdmin.tsx`: add readable summary before raw JSON.
- Modify `web/src/pages/Settings.tsx`: explain token types and revoked state.
- Modify `web/src/pages/AdminAccess.tsx`: explain approve/reject effects.
- Modify `web/src/pages/UsageDashboard.tsx`: clarify metric labels and definitions.
- Test `web/src/__tests__/lib/humanizeTaskFilter.test.ts`.
- Test `web/src/__tests__/lib/humanizeTrialConfig.test.ts`.
- Test `web/src/__tests__/lib/humanizeFailureReason.test.ts`.
- Extend page tests under `web/src/__tests__/pages/`.
- Update `docs/user-guide.md` after the UX changes land.

## Proposed GitHub Issues And PRs

### Issue 1 / PR 1: Shared UX Language Foundation

Issue title:

`[UX] Add shared help, diagnostics, and config-summary components`

Labels:

`workstream:product-design`, `workstream:mvp`, `type:feature`, `priority:P1`, `area:web`

Branch:

`codex/ux-human-readable-foundation`

Acceptance:

- Shared diagnostics wrapper exists and is closed by default.
- Task-filter and trial-config humanizers have unit coverage.
- Empty `trial_config` is represented as `Defaults only`.
- No page behavior changes are required in this PR beyond importing tests and components.

#### Task 1: Add Humanizer Unit Tests

**Files:**
- Create: `web/src/__tests__/lib/humanizeTaskFilter.test.ts`
- Create: `web/src/__tests__/lib/humanizeTrialConfig.test.ts`
- Create: `web/src/__tests__/lib/humanizeFailureReason.test.ts`

- [ ] **Step 1: Write failing task-filter tests**

```ts
import { describe, expect, it } from "vitest";

import { humanizeTaskFilter } from "../../lib/humanizeTaskFilter";

describe("humanizeTaskFilter", () => {
  it("summarizes all runnable tasks for benchmark ids", () => {
    const out = humanizeTaskFilter(
      { subset_kind: "all", benchmark_ids: ["humaneval"] },
      { matchedTaskCount: 164 },
    );
    expect(out.primary).toBe("HumanEval / all runnable tasks / 164 tasks");
    expect(out.details).toContain("Benchmark: humaneval");
  });

  it("summarizes seeded random subsets", () => {
    const out = humanizeTaskFilter(
      { subset_kind: "random_n", benchmark_ids: ["mbpp"], n: 25, seed: 7 },
      { matchedTaskCount: 25 },
    );
    expect(out.primary).toBe("MBPP / random 25 tasks / seed 7");
  });

  it("summarizes explicit ids without exposing raw JSON", () => {
    const out = humanizeTaskFilter(
      { subset_kind: "explicit", task_ids: ["humaneval/HumanEval/0", "humaneval/HumanEval/1"] },
      {},
    );
    expect(out.primary).toBe("2 explicit task IDs");
  });
});
```

- [ ] **Step 2: Write failing trial-config tests**

```ts
import { describe, expect, it } from "vitest";

import { humanizeTrialConfig } from "../../lib/humanizeTrialConfig";

describe("humanizeTrialConfig", () => {
  it("shows defaults for empty config", () => {
    const out = humanizeTrialConfig({});
    expect(out.primary).toBe("Defaults only");
    expect(out.items).toEqual([]);
  });

  it("summarizes retry and timeout settings", () => {
    const out = humanizeTrialConfig({
      override_agent_timeout_sec: 300,
      retry: { max_attempts: 2, retry_on: ["agent_timeout"] },
    });
    expect(out.items).toContain("Agent timeout: 300s");
    expect(out.items).toContain("Retry: up to 2 attempts on agent timeout");
  });

  it("summarizes verifier and priority overrides", () => {
    const out = humanizeTrialConfig({
      skip_verifier: true,
      submit_priority: 300,
    });
    expect(out.items).toContain("Verifier: skipped");
    expect(out.items).toContain("Submit priority: 300");
  });
});
```

- [ ] **Step 3: Write failing failure-reason tests**

```ts
import { describe, expect, it } from "vitest";

import { humanizeFailureReason } from "../../lib/humanizeFailureReason";

describe("humanizeFailureReason", () => {
  it("maps known failure codes to human labels", () => {
    expect(humanizeFailureReason("artifact_upload_failed").label).toBe(
      "Artifact upload failed",
    );
  });

  it("preserves unknown codes for diagnostics", () => {
    const out = humanizeFailureReason("custom_runner_failure");
    expect(out.label).toBe("Custom runner failure");
    expect(out.code).toBe("custom_runner_failure");
  });
});
```

- [ ] **Step 4: Run tests and verify they fail**

Run:

```bash
cd web && npm test -- humanizeTaskFilter humanizeTrialConfig humanizeFailureReason
```

Expected:

The command fails because the three modules do not exist yet.

#### Task 2: Implement Foundation Utilities And Components

**Files:**
- Create: `web/src/lib/humanizeTaskFilter.ts`
- Create: `web/src/lib/humanizeTrialConfig.ts`
- Create: `web/src/lib/humanizeFailureReason.ts`
- Create: `web/src/components/InfoHint.tsx`
- Create: `web/src/components/DiagnosticPanel.tsx`
- Create: `web/src/components/CopyableId.tsx`

- [ ] **Step 1: Implement `humanizeTaskFilter`**

Create a pure function with this exported shape:

```ts
export interface TaskFilterSummary {
  primary: string;
  details: string[];
  diagnostics: string[];
}

export interface TaskFilterSummaryContext {
  matchedTaskCount?: number;
}

export function humanizeTaskFilter(
  filter: Record<string, unknown>,
  context: TaskFilterSummaryContext = {},
): TaskFilterSummary {
  const subset = String(filter.subset_kind ?? "all");
  const benchmarkIds = Array.isArray(filter.benchmark_ids)
    ? filter.benchmark_ids.map(String)
    : filter.benchmark_id
      ? [String(filter.benchmark_id)]
      : [];
  const benchmarkLabel =
    benchmarkIds.length === 1 ? displayBenchmark(benchmarkIds[0]) :
      benchmarkIds.length > 1 ? `${benchmarkIds.length} benchmarks` : "Selected tasks";
  const count = context.matchedTaskCount;

  if (subset === "explicit") {
    const ids = Array.isArray(filter.task_ids) ? filter.task_ids : [];
    return {
      primary: `${ids.length} explicit task ID${ids.length === 1 ? "" : "s"}`,
      details: ids.slice(0, 5).map(String),
      diagnostics: ids.length > 5 ? [`${ids.length - 5} more IDs hidden`] : [],
    };
  }

  if (subset === "first_n" || subset === "last_n") {
    const n = Number(filter.n);
    const direction = subset === "first_n" ? "first" : "last";
    return {
      primary: `${benchmarkLabel} / ${direction} ${n} tasks`,
      details: benchmarkIds.map((id) => `Benchmark: ${id}`),
      diagnostics: [],
    };
  }

  if (subset === "random_n") {
    return {
      primary: `${benchmarkLabel} / random ${Number(filter.n)} tasks / seed ${Number(filter.seed)}`,
      details: benchmarkIds.map((id) => `Benchmark: ${id}`),
      diagnostics: [],
    };
  }

  const countText = typeof count === "number"
    ? `${count} task${count === 1 ? "" : "s"}`
    : "all matching tasks";
  return {
    primary: `${benchmarkLabel} / all runnable tasks / ${countText}`,
    details: benchmarkIds.map((id) => `Benchmark: ${id}`),
    diagnostics: [],
  };
}

function displayBenchmark(id: string): string {
  const known: Record<string, string> = {
    humaneval: "HumanEval",
    mbpp: "MBPP",
    "aime-22": "AIME 2022",
  };
  return known[id] ?? id;
}
```

- [ ] **Step 2: Implement `humanizeTrialConfig`**

Create a pure function that returns `Defaults only` for empty configs and item
strings for known non-default fields:

```ts
export interface TrialConfigSummary {
  primary: string;
  items: string[];
  diagnostics: string[];
}

export function humanizeTrialConfig(
  config: Record<string, unknown> | null | undefined,
): TrialConfigSummary {
  const c = config ?? {};
  const items: string[] = [];
  const diagnostics: string[] = [];

  addSeconds(items, "Agent timeout", c.override_agent_timeout_sec);
  addSeconds(items, "Verifier timeout", c.override_verifier_timeout_sec);
  addSeconds(items, "Environment build timeout", c.override_env_build_timeout_sec);
  addMultiplier(items, "Agent timeout multiplier", c.agent_timeout_multiplier);
  addMultiplier(items, "Verifier timeout multiplier", c.verifier_timeout_multiplier);
  addMultiplier(items, "Environment build timeout multiplier", c.env_build_timeout_multiplier);

  if (c.force_build === true) items.push("Environment image: force rebuild");
  if (c.delete_env === false) items.push("Environment container: keep after finish");
  if (c.skip_verifier === true) items.push("Verifier: skipped");
  if (typeof c.verifier_env_mode === "string") {
    items.push(`Verifier environment: ${c.verifier_env_mode}`);
  }
  if (typeof c.submit_priority === "number" && c.submit_priority !== 100) {
    items.push(`Submit priority: ${c.submit_priority}`);
  }

  const retry = c.retry;
  if (retry && typeof retry === "object") {
    const r = retry as { max_attempts?: unknown; retry_on?: unknown };
    const attempts = Number(r.max_attempts);
    const reasons = Array.isArray(r.retry_on) ? r.retry_on.map(String) : [];
    if (Number.isFinite(attempts) && attempts > 1 && reasons.length > 0) {
      items.push(`Retry: up to ${attempts} attempts on ${reasons.map(prettyCode).join(", ")}`);
    }
  }

  for (const key of Object.keys(c)) {
    if (!KNOWN_TRIAL_CONFIG_KEYS.has(key)) diagnostics.push(`Unrecognized field: ${key}`);
  }

  return {
    primary: items.length === 0 ? "Defaults only" : `${items.length} override${items.length === 1 ? "" : "s"}`,
    items,
    diagnostics,
  };
}

const KNOWN_TRIAL_CONFIG_KEYS = new Set([
  "force_build",
  "delete_env",
  "skip_verifier",
  "verifier_env_mode",
  "override_agent_timeout_sec",
  "override_verifier_timeout_sec",
  "override_env_build_timeout_sec",
  "agent_timeout_multiplier",
  "verifier_timeout_multiplier",
  "env_build_timeout_multiplier",
  "retry",
  "submit_priority",
]);

function addSeconds(items: string[], label: string, value: unknown): void {
  if (typeof value === "number") items.push(`${label}: ${value}s`);
}

function addMultiplier(items: string[], label: string, value: unknown): void {
  if (typeof value === "number" && value !== 1) items.push(`${label}: ${value}x`);
}

function prettyCode(value: string): string {
  return value.replaceAll("_", " ");
}
```

- [ ] **Step 3: Implement components**

Use these minimal public APIs:

```tsx
export function InfoHint({ children }: { children: React.ReactNode }): JSX.Element {
  return <p className="mt-1 text-xs leading-relaxed text-slate-500">{children}</p>;
}
```

```tsx
import JsonViewer from "./JsonViewer";

export interface DiagnosticBlock {
  title: string;
  data: unknown;
}

export function DiagnosticPanel({
  blocks,
  description = "Raw request and internal fields for debugging, support, and API reproducibility.",
}: {
  blocks: DiagnosticBlock[];
  description?: string;
}): JSX.Element | null {
  if (blocks.length === 0) return null;
  return (
    <details className="rounded-xl border border-slate-200 bg-white">
      <summary className="cursor-pointer px-5 py-3 text-sm font-semibold text-slate-900">
        Diagnostics
      </summary>
      <div className="space-y-4 border-t border-slate-100 px-5 py-4">
        <p className="text-xs text-slate-500">{description}</p>
        {blocks.map((block) => (
          <div key={block.title}>
            <p className="mb-2 text-xs font-medium uppercase tracking-wider text-slate-500">
              {block.title}
            </p>
            <JsonViewer data={block.data} />
          </div>
        ))}
      </div>
    </details>
  );
}
```

```tsx
export function CopyableId({ value, chars = 8 }: { value: string; chars?: number }): JSX.Element {
  const short = value.length > chars * 2 + 1
    ? `${value.slice(0, chars)}...${value.slice(-chars)}`
    : value;
  return (
    <button
      type="button"
      title={value}
      onClick={() => void navigator.clipboard?.writeText(value)}
      className="rounded bg-slate-100 px-1.5 py-0.5 font-mono text-xs text-slate-700 hover:bg-slate-200"
    >
      {short}
    </button>
  );
}
```

- [ ] **Step 4: Run foundation tests**

Run:

```bash
cd web && npm test -- humanizeTaskFilter humanizeTrialConfig humanizeFailureReason
```

Expected:

All new humanizer tests pass.

- [ ] **Step 5: Commit PR 1**

```bash
git add web/src/components web/src/lib web/src/__tests__/lib
git commit -m "feat(web): add human-readable diagnostics foundation"
```

### Issue 2 / PR 2: Batch Workflow Humanization

Issue title:

`[UX] Replace Batch Detail raw payload with Run Plan summary`

Branch:

`codex/ux-batch-run-plan`

Acceptance:

- Batch Detail default view shows `Run plan`.
- The screenshot case shows `HumanEval / all runnable tasks / 164 tasks` and `Defaults only`.
- `task_filter` and `trial_config` appear only after opening `Diagnostics`.
- New Batch explains task selection, combinations, samples, and advanced settings.
- Existing New Batch submit payload tests still pass.

#### Task 1: Write Batch Detail Tests

**Files:**
- Modify: `web/src/__tests__/pages/BatchDetail.test.tsx`

- [ ] **Step 1: Add a test for readable run plan**

Add a mocked batch detail response with:

```ts
task_filter: { subset_kind: "all", benchmark_ids: ["humaneval"] },
trial_config: {},
expected_trial_count: 164,
combinations: [
  {
    label: "combo1",
    agent_name: "litellm",
    agent_model: { provider: "openai", name: "qwen2.5-coder-7b-instruct" },
    n_per_task: 1,
  },
],
```

Assert:

```ts
expect(await screen.findByText(/Run plan/i)).toBeInTheDocument();
expect(screen.getByText(/HumanEval / all runnable tasks / 164 tasks/i)).toBeInTheDocument();
expect(screen.getByText(/Defaults only/i)).toBeInTheDocument();
expect(screen.queryByText("task_filter")).not.toBeInTheDocument();
expect(screen.queryByText("trial_config")).not.toBeInTheDocument();
```

- [ ] **Step 2: Add a diagnostics disclosure test**

Click `Diagnostics`, then assert:

```ts
expect(screen.getByText("task_filter")).toBeInTheDocument();
expect(screen.getByText("trial_config")).toBeInTheDocument();
```

#### Task 2: Implement Batch Detail Run Plan

**Files:**
- Modify: `web/src/pages/BatchDetail.tsx`

- [ ] **Step 1: Build summaries**

Use `humanizeTaskFilter(c.task_filter, { matchedTaskCount: c.expected_trial_count })`
and `humanizeTrialConfig(c.trial_config)`.

- [ ] **Step 2: Replace raw card**

Replace the `Filter + config` card with:

- `Card.Header title="Run plan"`.
- A row for `Task selection`.
- A row for `Agent/model combinations`.
- A row for `Shared trial settings`.
- A `DiagnosticPanel` containing `task_filter`, `trial_config`, `combinations`, and `fanout_errors` when present.

- [ ] **Step 3: Run Batch Detail tests**

Run:

```bash
cd web && npm test -- BatchDetail
```

Expected:

Batch Detail tests pass.

#### Task 3: Clarify New Batch Copy

**Files:**
- Modify: `web/src/pages/NewBatch.tsx`
- Modify: `web/src/__tests__/pages/NewBatch.test.tsx`

- [ ] **Step 1: Rename sections**

Change visible copy:

- `Which tasks` to `Task selection`.
- `Combinations` to `Agent/model combinations`.
- `Advanced options` to `Advanced trial settings`.

- [ ] **Step 2: Add explanatory copy**

Add short `InfoHint` copy under task selection, combination samples, and each
advanced settings group.

- [ ] **Step 3: Keep payload tests stable**

Run:

```bash
cd web && npm test -- NewBatch
```

Expected:

All existing New Batch payload assertions still pass.

- [ ] **Step 4: Commit PR 2**

```bash
git add web/src/pages/BatchDetail.tsx web/src/pages/NewBatch.tsx web/src/__tests__/pages/BatchDetail.test.tsx web/src/__tests__/pages/NewBatch.test.tsx
git commit -m "feat(web): humanize batch run plan and launch controls"
```

### Issue 3 / PR 3: Monitor And Trial Detail Humanization

Issue title:

`[UX] Humanize monitor and trial detail status, failures, and timeline data`

Branch:

`codex/ux-monitor-trial-humanization`

Acceptance:

- Monitor uses `Planned trials`.
- Trial Detail shows outcome text and human failure labels.
- Raw trajectory event JSON appears only under row-level `Raw event data`.

#### Tasks

- [ ] Add Trial Detail tests for outcome text and failure code labels in `web/src/__tests__/pages/TrialDetail.test.tsx`.
- [ ] Modify `web/src/pages/Monitor.tsx` table labels and filter placeholders.
- [ ] Modify `web/src/pages/TrialDetail.tsx` download labels and failure rendering.
- [ ] Modify `web/src/components/EventTimeline.tsx` so each row has a `Raw event data` disclosure around `JsonViewer`.
- [ ] Run:

```bash
cd web && npm test -- TrialDetail EventTimeline
cd web && npm run build
```

- [ ] Commit:

```bash
git add web/src/pages/Monitor.tsx web/src/pages/TrialDetail.tsx web/src/components/EventTimeline.tsx web/src/__tests__/pages/TrialDetail.test.tsx web/src/__tests__/components/EventTimeline.test.tsx
git commit -m "feat(web): clarify monitor and trial diagnostics"
```

### Issue 4 / PR 4: Provider And Operator Page Humanization

Issue title:

`[UX] Clarify provider and operator pages without hiding diagnostics`

Branch:

`codex/ux-provider-operator-humanization`

Acceptance:

- Provider pages explain `valid`, `invalid`, and `untested`.
- Model picker raw mode is renamed to `Include hidden/discovered models`.
- Rate-card raw JSON is behind diagnostics.
- Settings, Admin Access, and Usage metrics have concise definitions.

#### Tasks

- [ ] Update `web/src/components/AgentModelPicker.tsx` visible label from `Show raw` to `Include hidden/discovered models`.
- [ ] Extend `web/src/__tests__/pages/ProviderDetail.test.tsx` with provider readiness copy assertions.
- [ ] Modify `web/src/pages/ProviderDetail.tsx` and `web/src/pages/ProvidersList.tsx` to show status explanations.
- [ ] Modify `web/src/pages/RateCardsAdmin.tsx` so raw rate-card JSON is inside `DiagnosticPanel`.
- [ ] Modify `web/src/pages/Settings.tsx`, `web/src/pages/AdminAccess.tsx`, and `web/src/pages/UsageDashboard.tsx` to add concise definitions.
- [ ] Run:

```bash
cd web && npm test -- ProviderDetail ProvidersList AgentModelPicker
cd web && npm run build
```

- [ ] Commit:

```bash
git add web/src/components/AgentModelPicker.tsx web/src/pages/ProviderDetail.tsx web/src/pages/ProvidersList.tsx web/src/pages/RateCardsAdmin.tsx web/src/pages/Settings.tsx web/src/pages/AdminAccess.tsx web/src/pages/UsageDashboard.tsx web/src/__tests__
git commit -m "feat(web): clarify provider and operator diagnostics"
```

### Issue 5 / PR 5: Documentation And Final UX Regression Pass

Issue title:

`[Docs] Document human-readable SPA diagnostics model`

Branch:

`codex/docs-human-readable-spa-ux`

Acceptance:

- `docs/user-guide.md` explains default views and diagnostics disclosures.
- `docs/architecture/loom-spa-v3.md` records the two-layer UI rule.
- Full web test, lint, and build pass.

#### Tasks

- [ ] Update `docs/user-guide.md` with a short section named `Default views and diagnostics`.
- [ ] Update `docs/architecture/loom-spa-v3.md` with the default-layer/diagnostics-layer rule.
- [ ] Run:

```bash
cd web && npm test
cd web && npm run lint
cd web && npm run build
git diff --check
```

- [ ] Commit:

```bash
git add docs/user-guide.md docs/architecture/loom-spa-v3.md
git commit -m "docs: describe human-readable SPA diagnostics model"
```

## Final Verification Before Merge

Run on the final PR in the stack:

```bash
cd web && npm test
cd web && npm run lint
cd web && npm run build
git diff --check
```

Expected:

- Vitest passes.
- ESLint passes.
- Vite build succeeds.
- `git diff --check` reports no whitespace errors.

## Open Issue Creation Commands

Use these only after human approval to create the issue stack:

```bash
gh issue create --repo qianyi-sun/loom \
  --title "[UX] Make SPA default views human-readable while preserving diagnostics" \
  --label "workstream:product-design,workstream:mvp,type:feature,priority:P1,area:web,area:docs" \
  --body-file docs/architecture/human-readable-spa-ux.md
```

Create child issues from the five issue sections above and link them back to
the umbrella issue in each issue body.
