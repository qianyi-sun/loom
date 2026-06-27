# Run Library

The Run Library is Loom's org-wide view for completed work. It lets teams reuse
safe completed run metadata and artifacts without changing the normal execution
boundary.

## Boundaries

- Team remains the boundary for execution, cost attribution, provider
  credentials, members, and API tokens.
- Existing batch, trial, trajectory, ATIF, artifact, cancellation, rerun, and
  provider routes remain current-team scoped unless the caller is a platform
  admin.
- The Run Library is the only cross-team read/reuse surface for completed
  results. Cross-team sharing must not be implemented by weakening
  `require_team_or_admin()` on execution routes.
- Quota and rate-limit enforcement is not part of the public-beta Run Library.
  Spend and abuse response remain operational controls until an explicit
  product policy exists.

## Visibility Model

`batches` and `trials` carry three sharing fields:

- `visibility`: `team`, `org`, or `private`.
- `share_status`: `pending_scan`, `shared`, or `blocked`.
- `source_provenance`: JSON metadata that records clone/reuse source ids and
  artifact keys.

New batches and trials default to `visibility = "org"` and
`share_status = "shared"` so completed metadata enters the org-wide Library by
default. Teams can opt out by setting `team` or `private`; unsafe artifacts
still need their own artifact-level `share_status = "shared"` before another
team can download or reuse the object body.

A batch is visible in the org-wide Run Library only when:

- `visibility = "org"`.
- `share_status = "shared"`.
- `state` is terminal enough to inspect (`finished` or `cancelled`).

A trial artifact can be downloaded or reused across teams only when the parent
batch is Run-Library-readable and the artifact's own `share_status` is
`shared`. A hand-submitted trial with no parent batch uses its own visibility
and share status. `pending_scan` and `blocked` artifacts remain owner-team
diagnostics.

## API Surface

- `GET /api/v1/run-library/batches`: list library rows. `scope=my` shows the
  caller's team. `scope=all` shows the caller's team plus org-shared completed
  runs from other teams; platform admins can inspect all rows.
- `GET /api/v1/run-library/batches/{batch_id}`: detail view with owner-team
  label, task/config summary data, deterministic diagnosis, redacted batch
  debug evidence, provenance, trial rollup, and grouped artifact inventory.
- `PATCH /api/v1/run-library/batches/{batch_id}/visibility`: owner/admin update
  for `visibility` and `share_status`.
- `POST /api/v1/run-library/batches/{batch_id}/clone-config`: create a new
  batch record in the caller's team using the shared task filter, trial config,
  backend, and combinations. Source provider connections are never copied; if
  the source used a provider connection, the caller must choose one owned by
  the destination team. Run Library detail returns the source provider ids only
  as metadata so clients can decide whether to require that selector.
- `GET /api/v1/run-library/trials/{trial_id}/artifacts/download`: stream a
  safe shared artifact through the authenticated Loom service. It never exposes
  raw object-store URLs.
- `POST /api/v1/run-library/trials/{trial_id}/artifacts/reuse`: create a new
  batch record in the caller's team that records the shared artifact as source
  provenance. Unsafe artifacts are denied before reuse.

## SPA Behavior

The top-level Run Library page provides:

- My team / All teams scope toggle.
- URL-backed team, state, artifact-type, free-text search, benchmark, agent,
  model provider/name, provider connection, and provider model filters. These
  are server-side structured filters; the UI does not parse generated display
  names to find reusable runs.
- Owner-team labels.
- Human-readable task, agent/model, status, score, cost, trial, artifact, and
  share-state columns.

For platform-admin sessions, the team filter is populated from the fixed
internal-team registry so admins can filter by any team name. Non-admin users
only see teams returned by their session membership.

The Run Library detail page groups artifacts into reports, trajectories,
reusable outputs, logs/diagnostics, and raw/internal diagnostics. Shared
artifacts expose Download, Copy URL, and Reuse actions. Blocked artifacts show
only a safe blocked reason and do not expose cross-team actions. The same
Diagnosis and Debug evidence cards used by Batch Detail appear on Run Library
detail when the API includes `diagnosis` and `debug_evidence`; diagnosis shows
the human-readable summary, primary cause, impact, reason clusters, and next
actions first, while the exact redacted debug JSON remains collapsed.

Existing Batch Detail and Trial Detail pages also show owner team, visibility,
share status, and provenance when those fields are present, so cloned/reused
work remains explainable outside the Library.

## Safety Tests

Run Library changes should cover these cases:

- My team and All teams views include the right rows and `username / team`
  ownership labels, with legacy team fallback for old rows.
- Private, pending-scan, and non-terminal runs do not leak into another team's
  All teams view.
- Cross-team direct artifact routes remain denied.
- Cross-team Run Library artifact download works only for safe shared
  artifacts.
- Blocked artifacts return a safe denial and cannot be reused.
- Clone/reuse creates destination-team records with source provenance and does
  not copy source provider secrets.
- Cross-team mutation of the original source run remains denied.
