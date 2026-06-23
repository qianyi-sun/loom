# Human-Readable Frontend UX And Diagnostics Mode

Status: design
Last updated: 2026-06-19

## Context

The Loom SPA currently exposes many backend data structures directly. This is
useful while building the platform, but it makes normal evaluation workflows
hard to understand. Examples include `task_filter`, `trial_config`, raw
trajectory events, raw rate-card JSON, and terse state or failure codes.

The product now needs a two-layer UI:

- Default layer: human-readable summaries, clear actions, and explicit
  explanations of what each option changes.
- Diagnostics layer: raw payloads, internal field names, IDs, and event data
  for debugging, support, and reproducing API requests.

## Goals

- Make the normal UI understandable without knowing internal API field names.
- Keep all current diagnostic information available, but move it behind clearly
  labeled `Diagnostics` or `Advanced` disclosures.
- Give every major control, button, option, status, and empty/error state a
  concise explanation of what it means and what the user can do next.
- Replace default raw JSON displays with human-readable summaries.
- Share translation logic through small frontend components and `lib/`
  humanizers so page-specific code stays focused.
- Preserve current workflows and API payloads. This is a presentation-layer
  change unless a later issue explicitly calls for API support.

## Non-Goals

- Do not remove raw JSON entirely. Operators and developers still need it for
  debugging.
- Do not redesign visual branding, navigation IA, or page layout from scratch.
- Do not change batch, trial, provider, or task API contracts in this slice.
- Do not introduce a localization system. UI copy remains English for now.
- Do not make hidden diagnostics admin-only in this slice; access control can
  be designed separately.

## UX Model

### Default Layer

The default layer must answer four questions on each page:

1. What object am I looking at?
2. What is its current state?
3. What can I do next?
4. If something is unavailable or failed, why?

Default copy should use product nouns:

- `Task selection`, not `task_filter`.
- `Shared trial settings`, not `trial_config`.
- `Run plan`, not `payload`.
- `Planned trials`, not `expected_trial_count`.
- `Result artifacts`, not only object-store keys.

### Diagnostics Layer

Diagnostics must be explicit and folded away by default. Every diagnostics
panel should include a short description such as:

> Raw request and internal fields for debugging, support, and API
> reproducibility.

Diagnostics can show:

- Raw `task_filter`.
- Raw `trial_config`.
- Raw trajectory event payloads.
- Raw rate-card JSON.
- Full IDs and internal codes.
- Fan-out submission errors.

## Shared Frontend Building Blocks

### `InfoHint`

Small inline explanatory text for field labels and action groups.

Required behavior:

- Render as muted text under a field or next to a compact label.
- Support plain strings and React children.
- Keep copy short; long docs should link to a docs page.

### `DiagnosticPanel`

Shared wrapper for all raw/internal data.

Required behavior:

- Uses a `details` disclosure.
- Default closed.
- Title defaults to `Diagnostics`.
- Description explains why the raw/internal data is shown.
- Contains one or more named diagnostic blocks.
- Uses `JsonViewer` only inside this panel unless a page is explicitly an
  admin/config JSON editor.

### `CopyableId`

Component for IDs and keys.

Required behavior:

- Shows a shortened ID by default when the value is long.
- Provides a copy button or click-to-copy affordance.
- Uses `title` or adjacent text to expose the full value.
- Keeps full ID available in diagnostics.

### Humanizer Functions

Create small pure functions under `web/src/lib/`:

- `humanizeTaskFilter(filter, context)` returns task-selection summary lines.
- `humanizeTrialConfig(config)` returns shared trial-setting summary lines.
- `humanizeFailureReason(code)` maps known failure codes to plain-English
  labels and descriptions.
- `humanizeState(kind, state)` maps batch/trial/token/provider states to
  descriptions. This should build on `helpText.ts`, not fork it.

Each humanizer must return structured data, not JSX, so tests can cover the
translation logic without rendering full pages.

## Page Requirements

### New Batch

The page should read like a launch form, not an API builder.

Required changes:

- Rename `Which tasks` to `Task selection`.
- Explain that task selection decides which tasks will be expanded into trials.
- Show benchmark readiness badges from `/api/v1/benchmarks`: `Ready` rows are
  selectable, blocked rows are disabled with API-provided guidance and
  raw-versus-runnable counts. License metadata is informational and does not
  create a blocker.
- Add one-line explanations for each subset option:
  - `All tasks`: run every runnable task in the selected benchmarks.
  - `First N`: deterministic smoke slice from the start of the sorted task list.
  - `Last N`: deterministic slice from the end of the sorted task list.
  - `Random N`: reproducible random sample controlled by seed.
  - `Explicit task ids`: run only the pasted task IDs.
- Rename `Combinations` to `Agent/model combinations`.
- Explain that every combination runs across the same selected task slate.
- Explain `Samples per task` as repeats per selected task for that combination.
- Rename `Advanced options` to `Advanced trial settings`.
- Explain each advanced group:
  - `Environment`: container build and cleanup behavior.
  - `Timeouts`: how long each trial phase may run.
  - `Retry`: which transient failures should create another attempt.
  - `Scheduling`: relative queue priority.
- Keep the submit summary formula visible:
  `tasks x samples x combinations = planned trials`.
- When the user uses defaults, do not mention internal `trial_config`.

### Batch Detail

Batch Detail should show what was launched and how it is progressing.

Required changes:

- Replace the default `Filter + config` raw JSON card with a `Run plan` card.
- Show task selection in plain language:
  `HumanEval / all runnable tasks / 164 tasks`.
- Show combination summaries:
  `combo1 / litellm / openai/qwen2.5-coder-7b-instruct / n=1`.
- Show shared settings summary:
  - `Defaults only` when `trial_config` is empty.
  - Plain-English bullets when non-default values exist.
- Show backend and provider override information in human-readable form.
- Move raw `task_filter`, `trial_config`, `combinations`, and fan-out errors
  into `Diagnostics`.
- If fan-out errors exist, show a normal alert with human-readable count and
  first actionable message before the raw diagnostic block.

### Monitor

Monitor should help users decide what needs attention.

Required changes:

- `Expected` column becomes `Planned trials`.
- Search placeholders say exactly what they search.
- State filters show or expose state descriptions.
- Empty states tell the next action: clear filter, create batch, or wait for
  workers.
- Batch and trial state pills use `StateExplainer` descriptions.
- Reward copy clarifies that reward is evaluator score, while platform
  success/failure is represented by terminal state.

### Trial Detail

Trial Detail should separate platform outcome, evaluator outcome, and raw logs.

Required changes:

- Add a short outcome sentence under the header:
  - `succeeded`: platform completed the trial and saved outputs.
  - `failed`: platform could not complete the trial; inspect failure and
    timeline.
  - active states: worker or queue status in plain language.
- Render failure reason as human label plus code:
  `Artifact upload failed` and `Code: artifact_upload_failed`.
- Rename download buttons:
  - `Download ATIF report`.
  - `Download trajectory log`.
- Group artifacts by role when metadata supports it; otherwise keep a flat list
  with readable labels and sizes.
- Timeline rows default to summaries.
- Raw event JSON appears only under row-level `Raw event data` disclosure.

### Providers

Provider pages should explain whether a connection is ready for real runs.

Required changes:

- Provider statuses explain:
  - `valid`: last test succeeded.
  - `invalid`: last test failed; runs may fail until fixed.
  - `untested`: saved but never tested.
- `Allowed models` displays:
  - `All discovered models` when unrestricted.
  - `N allowed models` when allow-listed.
  - `No allowed models` when blocked.
- Rename `Show raw` in model picker to `Include hidden/discovered models`.
- Explain manual model mode as an ad-hoc model ID for the selected provider
  connection.
- Keep provider test error details visible in readable text, with raw provider
  response in diagnostics only if present.

### Settings, Admin, Rate Cards, Usage

These pages are operator-heavy but still need clear labels.

Required changes:

- Settings explains token type, scopes, and revoked state.
- Admin access explains approve/reject consequences before action.
- Rate cards default to a readable provider/model pricing summary. Raw pricing
  payloads move to diagnostics.
- Usage metric labels use product names:
  - `Failed trials now`.
  - `Total cost`.
  - `Completed batches`.
  - `Running trials`.
- Each metric includes a short definition through `InfoHint` or `title`.

## Copy Rules

- Prefer user-facing nouns over field names.
- Keep explanations one sentence unless the control is risky.
- Use title attributes for compact controls, but do not rely on hover-only
  content for critical guidance.
- Use code-style formatting only for real IDs, command snippets, and internal
  codes.
- Avoid repeating raw API keys, secret references, or provider credentials in
  default copy.

## Testing Strategy

Add focused frontend tests for translation behavior:

- `humanizeTaskFilter`:
  - all benchmark tasks.
  - first/last/random N.
  - explicit IDs.
  - tag filters.
  - unknown keys are represented as diagnostics, not silently dropped.
- `humanizeTrialConfig`:
  - empty config returns `Defaults only`.
  - timeout overrides.
  - retry policy.
  - skip verifier.
  - scheduling priority.
- Page tests:
  - Batch Detail default view does not show `task_filter` or `trial_config`.
  - Batch Detail diagnostics reveal raw JSON when expanded.
  - New Batch advanced settings render explanatory group copy.
  - Monitor uses `Planned trials`.
  - Trial Detail shows human failure label plus raw code.
- Keep existing payload tests to ensure UI copy changes do not change API
  request shape.

Verification commands:

```bash
cd web && npm test
cd web && npm run build
cd web && npm run lint
```

## Accessibility And Layout

- Disclosures must use native `details` / `summary` or equivalent accessible
  button state.
- Status explanations must be available to keyboard users and screen readers.
- Long IDs and model names must wrap without overlapping adjacent controls.
- Buttons must have clear visible labels; destructive buttons need action
  verbs and confirmation where appropriate.
- Diagnostics panels must not dominate first-viewport content.

## Rollout Strategy

This should be delivered in small PRs. Each PR should improve a visible user
workflow and keep diagnostics intact.

1. Foundation: shared components and humanizer utilities.
2. Batch workflow: New Batch and Batch Detail.
3. Monitoring workflow: Monitor, Trial Detail, and Event Timeline.
4. Provider and operator workflow: Providers, Settings, Admin, Rate Cards,
   and Usage.
5. Polish and documentation: user-guide screenshots/copy and regression
   cleanup if needed.

## Proposed GitHub Issue Plan

### Umbrella Issue

Title:

`[UX] Make SPA default views human-readable while preserving diagnostics`

Labels:

- `workstream:product-design`
- `workstream:mvp`
- `type:feature`
- `priority:P1`
- `area:web`
- `area:docs`

Acceptance:

- Default user-facing views avoid raw JSON for batch/trial/provider workflows.
- Diagnostics remain available behind explicit disclosures.
- Major controls and options have concise explanations.
- New humanizers are unit-tested.
- Existing submit payload behavior remains unchanged.

### Child Issue 1

Title:

`[UX] Add shared help, diagnostics, and config-summary components`

PR scope:

- Add `InfoHint`, `DiagnosticPanel`, `CopyableId`.
- Add task-filter and trial-config humanizers.
- Add unit tests for the humanizers.

Acceptance:

- Empty `trial_config` displays as `Defaults only`.
- Known task filters produce readable summaries.
- Raw JSON rendering is centralized through `DiagnosticPanel`.

### Child Issue 2

Title:

`[UX] Replace Batch Detail raw payload with Run Plan summary`

PR scope:

- Replace `Filter + config` with `Run plan`.
- Move raw batch payload fields into diagnostics.
- Humanize fan-out errors.
- Add Batch Detail tests.

Acceptance:

- The screenshot scenario reads as `HumanEval / all runnable tasks / 164 tasks`
  and `Shared trial settings: Defaults only`.
- `task_filter` and `trial_config` are not visible until diagnostics opens.

### Child Issue 3

Title:

`[UX] Clarify New Batch controls and launch summary`

PR scope:

- Rename and explain task selection, combinations, samples, and advanced trial
  settings.
- Improve submit summary formula.
- Keep API payload tests unchanged.

Acceptance:

- A new user can infer how planned trial count is calculated from the page.
- Every advanced option group explains its runtime effect.

### Child Issue 4

Title:

`[UX] Humanize monitor and trial detail status, failures, and timeline data`

PR scope:

- Rename Monitor columns and placeholders.
- Add state/failure explanations.
- Move raw trajectory event JSON behind `Raw event data`.
- Add Trial Detail tests for failure labels and diagnostics.

Acceptance:

- Platform outcome, evaluator reward, and raw logs are visually distinct.
- Raw event payloads remain available but are not the default row content.

### Child Issue 5

Title:

`[UX] Clarify provider and operator pages without hiding diagnostics`

PR scope:

- Provider status and allowed-model summaries.
- Model picker raw-mode rename.
- Rate-card readable summary before raw payload.
- Settings/Admin/Usage metric definitions.

Acceptance:

- Operator pages explain readiness and risk in plain language.
- Raw pricing/provider details stay in diagnostics.

## Open Decisions

- Whether diagnostics should be admin-only later. This spec keeps diagnostics
  visible to authorized page users because current workflows rely on them.
- Whether to add a global `Show diagnostics` user preference later. This spec
  uses local disclosures only.
- Whether backend endpoints should send precomputed summaries later. This spec
  keeps summaries client-side because the needed data already exists in the SPA.
