# Web presentation and diagnostics

The Loom web application presents product concepts in plain language by
default and keeps internal request fields, raw payloads, identifiers, and event
data in explicit diagnostics disclosures.

## Shared presentation components

- `InfoHint` supplies short explanations beside controls and labels.
- `DiagnosticPanel` renders a closed `details` disclosure for raw/internal
  data. `JsonViewer` is used inside diagnostics unless the page is an explicit
  JSON editor.
- `CopyableId` shortens long identifiers visually while preserving the full
  value and a copy action.
- `DestructiveActionDialog` provides the confirmation, pending, retry, and
  focus behavior for destructive or credential-invalidating mutations.
- `humanizeTaskFilter`, `humanizeTrialConfig`, `humanizeFailureReason`, and
  `humanizeState` return structured summaries shared across pages.

## Page behavior

### New Batch

The form uses `Task selection`, `Agent/model combinations`, `Samples per task`,
and `Advanced trial settings` rather than API field names. Readiness data from
`/api/v1/benchmarks` determines which catalog rows are selectable. The summary
shows the expansion formula:

```text
tasks × samples × combinations = planned trials
```

Internal `task_filter` and `trial_config` names are not shown when defaults are
used. Submission locks synchronously so repeated clicks cannot create duplicate
requests.

### Batch and Run Library details

Default views show the task selection, combinations, backend/provider choice,
and shared trial settings as a run plan. Raw filters, trial configuration,
combination payloads, and fan-out details remain available through diagnostics.
Fan-out failures also produce a readable alert before the raw payload.

### Monitor and Trial Detail

Monitor distinguishes planned trials, platform state, and evaluator reward.
Trial Detail renders a plain-language platform outcome, a humanized failure
reason plus its stable code, readable artifact/download labels, and summarized
timeline rows. Raw event objects are available per row under `Raw event data`.
Nebius service execution has a separate capacity and lifecycle panel: fresh
executable slots are not conflated with configured scale headroom, and Pod,
materialization, retry, source-retention, and complete-bundle states remain
visible without exposing provider target ids or raw internal errors to ordinary
users.

### Providers and operator pages

Provider states are presented as readiness (`Ready`, `Needs attention`, or
`Untested`) with allowed-model summaries. Settings, Admin, Rate Cards, and Usage
use product labels and short definitions; raw provider and pricing payloads are
diagnostic data.

## Destructive action contract

- Provider deletion, provider credential rotation, and TaskSet deletion require
  the exact case-sensitive target.
- Batch cancellation, token mutation, invite mutation, and registration/reset
  rejection require an explicit confirmation naming the consequence.
- While a mutation is pending, confirm, cancel, Escape, backdrop, and close
  dismissal are disabled.
- Failed requests keep the selected target and entered fields mounted and show
  one redacted retryable error.
- Dialogs close or navigate only after server-confirmed success. Busy state is
  scoped to the exact target.
- Server authorization, CSRF, audit, and conflict checks remain authoritative.

## Accessibility

Diagnostics use native disclosure semantics. Shared tabs expose tab/panel
relationships and keyboard navigation. The shared modal owns labelled title and
description relationships, initial focus, focus containment, Escape/backdrop
behavior, background inertness, body scroll locking, and final focus
restoration. Long identifiers and model names wrap without obscuring controls.

## Verification

Presentation behavior is covered by focused component, humanizer, and page
tests under `web/src/__tests__/`. The full frontend gate is:

```bash
cd web
npm test
npm run build
npm run lint
```

The cross-layer accessibility contract also runs through:

```bash
uv run pytest -q tests/ops/test_frontend_accessibility_contract.py
```
