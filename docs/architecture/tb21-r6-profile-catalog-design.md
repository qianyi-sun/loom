# TB2.1 rev 6 profile catalog design

**Issue:** #749

**Status:** approved design; implementation has not started

**Date:** 2026-07-10

## Decision

Loom will expose `terminal-bench-2` as a stable public catalog alias, but
execute only the immutable Terminal-Bench 2.1 Harbor Hub revision 6 profile:

```text
public selector: terminal-bench-2
                     |
                     v
active physical profile: terminal-bench-2@tb2.1-r6
task ids: terminal-bench-2@tb2.1-r6/<task-name>

historical-only profile: terminal-bench-2@tb2.0-91e10457
historical task ids: terminal-bench-2/<task-name>       (unchanged)
```

The alias may be resolved for new submissions only to the TB2.1 profile.
The historical profile remains readable for old trials, exports, task bundles,
and scores, but cannot be selected for a new batch, canary, release gate, or
export-acceptance run. A failed TB2.1 import or preflight disables new TB2
submissions; it never silently falls back to TB2.0.

This is the selected **versioned profile plus public alias** approach. It is
the only option that preserves credible historical score provenance while
keeping the normal catalog UX to one current TB2 entry.

## Problem and root cause

The existing adapter is a TB2.0 adapter, not a version-neutral catalog:

- `packages/loom-benchmark-terminal-bench-2/.../upstream.py` pins
  `laude-institute/terminal-bench@91e10457` and `terminal-bench-core v0.1.1`.
- `adapter.py` assumes the legacy `task.yaml`/Compose layout, advertises 84
  tasks, and explicitly filters two legacy-only incompatible tasks.
- `Task.id` is the primary key and `Trial.task_id` is a foreign key to it.
  Reusing a legacy task ID for a TB2.1 bundle would make old trials appear to
  have run a different task/verifier/checksum.
- A single `Benchmark.id` currently represents both user-facing selection and
  physical benchmark identity, so it cannot express a moving public alias
  without an explicit catalog layer.

The two formerly filtered tasks (`broken-networking` and `extract-safely`) are
TB2.0-only historical inputs. They are not in the TB2.1 rev-6 89-task
manifest. They will not be repaired, copied, or carried into TB2.1.

## Canonical data lock

The implementation must make this tuple executable and fail closed if any
member changes:

| Field | Required value |
| --- | --- |
| Harbor dataset | `terminal-bench/terminal-bench-2-1` |
| Harbor revision | `6` |
| Harbor metadata version | `sha256:7d7bdc1cbedad549fc1140404bd4dc45e5fd0ea7c4186773687d177ad3a0699a` |
| Physical profile | `terminal-bench-2@tb2.1-r6` |
| Source-reference repository | `harbor-framework/terminal-bench-2-1` |
| Source-reference snapshot | `dde3cd95b80ff25af5abd99a80b6513a018ad3b4` |
| Source-reference manifest | `tasks/dataset.toml` |
| Source-reference manifest SHA-256 | `d90b4389992d07ed6f4ab8de963a70241eaa4b60072eeaec4c3b261b6c4a6dd8` |
| Task packages | exactly 89 immutable Harbor metadata package digests |

The [Harbor Hub revision 6](https://hub.harborframework.com/datasets/terminal-bench/terminal-bench-2-1/6)
is the sole authority for execution content. The pinned [source snapshot](https://github.com/harbor-framework/terminal-bench-2-1/tree/dde3cd95b80ff25af5abd99a80b6513a018ad3b4)
and its [dataset manifest](https://github.com/harbor-framework/terminal-bench-2-1/blob/dde3cd95b80ff25af5abd99a80b6513a018ad3b4/tasks/dataset.toml)
remain source-reference provenance, not a second execution authority.

The adapter package contains a reviewed, machine-readable lock derived from
Hub metadata. It records all 89 task names and immutable package digests, the
Hub metadata version, the source-reference snapshot and manifest hash, and a
reviewed source-manifest comparison. Fetching performs both checks below before
conversion:

1. Resolve `terminal-bench/terminal-bench-2-1@6` through the Harbor package
   registry. The returned metadata version and 89 package references must match
   the checked-in lock exactly.
2. Fetch the source-reference repository at the fixed commit, hash
   `tasks/dataset.toml`, and require 89 unique names matching the Hub name set.
   The lock records the only approved divergence:
   `terminal-bench/sanitize-git-repo` has source digest
   `sha256:73c94a21ebe370bae843adbeeaaa9e991374867b18483aaf56c7cd470dcddea7`
   and Hub digest
   `sha256:6e86297715fae62cd499fbdd27013e11a38d05d7e05b7f661cb50b4ecead128f`.
   Any additional, missing, or changed divergence fails closed and is surfaced
   in the audit; it is never silently normalized.

The Harbor download client is publishing tooling, not the agent runtime. Its
version/hash is recorded in catalog-import evidence, while Terminus2's Harbor
runtime version, wheel/SBOM/image identity, templates, and Gateway bridge
remain separate runtime provenance owned by #744. Neither provenance may be
inferred from the other.

## Data model and migration

### Physical profiles

`benchmarks.id` becomes an immutable physical profile ID for versioned
benchmarks. Add the smallest catalog metadata needed to distinguish a profile
from a public alias:

- `Benchmark.execution_state`: `runnable` or `historical` (default existing
  non-versioned benchmarks to `runnable`).
- `Benchmark.profile_provenance`: catalog-level JSON containing the Hub dataset
  ref/revision, Hub metadata version, source-reference snapshot and manifest
  SHA, reviewed divergence record, 89-entry lock fingerprint, and
  adapter/importer identity.
- `Task.source_provenance`: task-level JSON containing the canonical task name,
  Hub package digest and metadata version, source-reference comparison when
  divergent, verifier identity, and task-image provenance. The existing
  `Task.checksum` remains the Loom bundle checksum; it is repeated in audit
  output rather than treated as an upstream digest.
- `benchmark_aliases`: public `alias` primary key, target `benchmark_id` FK,
  and an immutable activation/audit timestamp. Only a target with
  `execution_state = runnable` may be an active alias.

The migration is deliberately ordered so no `Trial.task_id` changes:

1. Insert `terminal-bench-2@tb2.0-91e10457` with the current legacy benchmark
   metadata and `execution_state = historical`.
2. Re-parent every existing `tasks.benchmark_id = 'terminal-bench-2'` row to
   that profile. Task IDs, checksums, configs, stored artifacts, trials,
   rewards, exports, and trial provenance are untouched.
3. Remove the old unversioned Benchmark row only after no task refers to it.
   Historical presentation labels it `Terminal-Bench 2.0 (archived,
   91e10457)`; its data is not rewritten.
4. Create `terminal-bench-2@tb2.1-r6` as `runnable`, register the newly
   converted 89 tasks under `terminal-bench-2@tb2.1-r6/<task-name>`, and store
   their checked source/package/bundle/verifier/image provenance.
5. In one DB transaction, create or replace alias `terminal-bench-2` to point
   to `terminal-bench-2@tb2.1-r6`. Until that commit, no public TB2 selection
   changes. No inactive or half-published profile is aliased.

Existing non-TB benchmarks do not need aliases or a rename. New benchmark
versions use the same profile/alias pattern only when they need a moving public
selector and immutable score history.

### Submission, history, and APIs

All new task-filter, CLI `--benchmark`/`--dataset`, SPA catalog, canary, and
release-gate paths resolve a public alias before task selection. The stored
batch filter preserves the user-requested selector in audit metadata and
persists the resolved physical profile and physical task IDs used to create
trials. Later alias changes therefore cannot change a queued, rerun, exported,
or historical batch.

Direct selection of `terminal-bench-2@tb2.0-91e10457` returns a typed
`benchmark_retired` validation error for new execution. Read APIs, old trial
detail, delivery exports, and task-bundle downloads continue to resolve it.
The catalog lists only the public current entry by default; a historical view
may show the archived profile and its source revision, never as release-ready
evidence.

Catalog and trial responses expose both values where relevant:

- public selector/display name (`terminal-bench-2`, `Terminal-Bench 2.1`);
- resolved physical profile;
- task ID, source snapshot, Harbor package digest, Loom bundle checksum;
- verifier identity/result and task-image provenance;
- separate Terminus2/runtime provenance when that agent executes the trial.

## Adapter and verifier boundary

Replace the active TB2 conversion path with a Harbor-native adapter. It reads
each locked task directory without translating it through the legacy
`task.yaml`/Compose schema:

```text
tasks/<task>/
  task.toml
  instruction.md
  environment/
  tests/test.sh
  solution/solve.sh
```

The converter preserves the exact native schema-1.1 TOML as
`upstream-task.toml` beside a Loom-schema-1 `task.toml`. The Loom config is
produced by an explicit native-1.1 normalizer, which rewrites only the physical
task ID and maps representable execution fields. Native resource quota,
internet, and architecture fields that Loom's current `TaskConfig` cannot
represent remain byte-preserved in `upstream-task.toml` and are carried into
task provenance and preflight/audit rather than being silently dropped. The
converter also preserves instruction bytes, environment assets and image/build
definition, timeout limits, test files, and solution isolation. It never exposes
`solution/solve.sh` to a normal agent. It does not retain the legacy
`_UNSUPPORTED_INSTANCES` filter: the lock requires exactly 89 converted
bundles, not an adapter-selected subset.

Verifier execution preserves the native test contract and captures test output,
CTRF when produced, verifier artifacts, and the numeric reward. Reward `0` is
a successful platform evaluation with a benchmark failure outcome. Missing,
empty, malformed, timed-out, or otherwise unparseable reward is a
platform/verifier failure with an explicit classification; it is never coerced
to `0`, silently skipped, or reported as a model score.

Task image architecture is derived from the converted task metadata/image
inspection and recorded per task. Preflight rejects unavailable worker-pool
placement with the task ID, required/observed architecture, and remediation;
it does not rewrite the task image or fall back to a different profile.

## Publication, activation, and rollback

Publication is a two-phase operation:

1. **Prepare:** fetch, lock-check, convert, validate all 89 bundles, publish
   immutable objects, register the physical profile, mirror objects, and audit
   every bundle checksum and provenance field. This phase has no public alias.
2. **Activate:** after the package/digest audit, architecture preflight, and
   required Oracle evidence pass, atomically switch `terminal-bench-2` to the
   new runnable profile. Release configuration and canaries use the resolved
   physical task ID, not a floating alias.

If prepare or activate validation fails, new TB2 submission is disabled or
kept disabled and the failure is recorded. Rollback may remove the active
alias or disable TB2 submission, but may not repoint it to TB2.0. The TB2.1
profile and audit artifacts remain for investigation; historical data is never
deleted.

## Rejected approaches

1. **Overwrite `terminal-bench-2/<task>` in place.** Rejected because it
   changes the meaning of historical `Trial.task_id` foreign keys and makes
   old scores/verifier evidence non-credible.
2. **Delete TB2.0.** Rejected because it destroys provenance required to read
   existing trials, artifacts, and exports.
3. **Keep TB2.0 as an execution fallback or a reduced TB2.1 subset.** Rejected
   by the owner decision and #749's 89-task credibility requirement. A broken
   new profile must be visible as a block, not hidden by legacy results.

## Implementation surfaces

The implementation plan will cover these coordinated surfaces:

- `src/loom/db/schema.py` and a new Alembic migration for profile state,
  aliases, legacy re-parenting, and transactional activation.
- Task-filter/batch creation, catalog listing, CLI dataset/eval selection,
  release smoke defaults, and task compatibility prefixes so only the current
  profile can execute while history stays readable.
- `packages/loom-benchmarks` source fetching/lock validation and
  `packages/loom-benchmark-terminal-bench-2` adapter, report, package metadata,
  fixtures, and verifier contract.
- Local publication/registration/audit and object-store provenance so an audit
  emits 89 source digest, Loom checksum, verifier, image, and profile records.
- Runtime, export, debug, score, and evidence projections that show benchmark
  provenance separately from Harbor/Terminus2 runtime provenance.
- Documentation: benchmark architecture/onboarding, Terminus2 provenance
  boundary, operator and first-production runbooks, user catalog semantics,
  score-alignment material, the adapter README, and the v1 release program.

This issue does not implement #744's Terminus2 runtime bridge, #745's v2
export format, or #746's final staging validator. It supplies their canonical
TB2.1 input and the evidence they must consume.

## Verification and release gates

### Automated regression coverage

- Lock parser tests prove the exact 89 Hub names/digests, Hub metadata version,
  fixed Harbor revision, source-reference SHA, manifest SHA, and reviewed
  single divergence. Count drift, missing/extra tasks, duplicate task names,
  Hub-digest drift, or any unrecorded source-reference difference fails before
  conversion.
- Fetch tests simulate Hub metadata/version drift, truncated downloads,
  unrecorded source-snapshot disagreement, and offline cache reuse without
  consulting a floating `latest` alias.
- Native-layout fixture tests cover instruction, TOML metadata, environment,
  timeouts/resources, architecture, tests, solution isolation, and all 89
  conversion outputs. They assert no skip list exists.
- Verifier tests cover reward `1`, reward `0`, missing/empty/malformed reward,
  timeout, non-zero test execution, and CTRF/artifact retention.
- Migration tests seed a legacy task and trial, run upgrade, and prove the
  legacy task ID/trial/artifact/reward remains byte-for-byte referentially
  intact while a TB2.1 task has a distinct profile-scoped ID.
- API/CLI/batch tests prove `terminal-bench-2` resolves to TB2.1, a stored
  batch records its physical profile, direct legacy submission is rejected,
  and history/export remains readable.
- Publication/audit tests prove the alias cannot point to a partial profile and
  that all 89 registered/mirrored bundles have source/package/bundle/verifier/
  image provenance.

### Candidate and live evidence

- A candidate audit reports 89/89 Hub-locked packages, the exact Hub metadata
  version, the reviewed source-reference divergence, converted bundles,
  registrations, object-store mirrors, and checksum matches.
- Architecture audit covers all 89 tasks. Every unavailable architecture or
  environment exception has a task-level classification and linked issue; none
  is silently excluded from the catalog count.
- Oracle runs execute every runnable task on an upstream-compatible worker
  architecture. Each terminal result has a numeric reward or a classified
  platform/task exception with evidence.
- Representative known-pass, known-fail, build-heavy, long-timeout, and
  verifier-sensitive canaries retain task/verifier/image provenance and numeric
  rewards. A zero reward is accepted only with complete verifier evidence.
- #744 executes at least one pinned TB2.1 task through its independently
  pinned Harbor Terminus2 runtime; #745 consumes the resulting native
  artifacts; #746 validates final dataset, task digest, verifier result,
  runtime provenance, export SHA, and secret scan in staging.
- Release gate and runbooks use a TB2.1 physical task ID and assert that no
  active/default/canary/release path resolves `91e10457`.

## Completion definition

#749 is complete only after the implementation, review, merge, CI, and the
candidate/live gates above prove that all new TB2 execution uses the canonical
TB2.1 rev-6 profile, all 89 packages are auditable, old results remain
historically readable, and no legacy fallback exists. A merged adapter or a
single passing canary alone is insufficient.
