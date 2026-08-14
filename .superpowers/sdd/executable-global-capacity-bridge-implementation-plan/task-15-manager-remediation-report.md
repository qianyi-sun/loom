# Task 15A — Manager and protected-data remediation report

## Scope

Implemented the five requested remediation items while preserving the inert executable bridge posture: no live activation/apply/start path, no positive checked-in/rendered ceiling, and fixed namespace derivation (`loom-dev`, `loom-dev-<owner>`, no shared personal namespace).

Commit: `68d2bc373` (`Fix Task 15A capacity remediation gaps`).

## Item 1 — Protected admission source/publication separation

Root cause: protected executable admission compared the executable candidate publication SHA-256 to the legacy `candidate_digest` field, which was also used as the source identity for personal/static candidates. That collapsed two different trust facts and failed when source identity and publication artifact digest legitimately differed.

RED:

`uv run --no-sync pytest -q tests/integration/test_capacity_agent_executable_admission.py::test_executable_admission_separates_candidate_source_and_publication -vv`

Expected failure observed: `AgentRegistrationV1` rejected the new candidate provenance fields as extra, proving the protected registration contract could not represent separated source identity and publication digest.

Implementation:

- Added tagged `candidate_identity_algorithm`, `candidate_identity`, and `candidate_publication_sha256` fields to protected agent registration contracts, with backward defaults from `candidate_digest`.
- Persisted/read/reconfigured those fields in the capacity agent store.
- Updated executable admission to compare algorithm, source identity, and publication digest independently.
- Added `guard_0015` to backfill and constrain protected registration provenance and to patch protected functions/allow-lists, including inert submission and legacy compatibility functions that consume registration-bound payloads.
- Preserved Git SHA-1 identities as tagged 40-hex source identities and SHA-256 publication digests as 64-hex publication facts.

GREEN/regression:

- `uv run --no-sync pytest -q tests/integration/test_capacity_agent_executable_admission.py tests/integration/test_capacity_agent_store.py tests/integration/test_capacity_agent_migrations.py tests/integration/test_capacity_guard_migrations.py tests/integration/test_capacity_submission_store.py tests/integration/test_capacity_legacy_fence_store.py -vv` → 136 passed.
- `uv run --no-sync pytest -q tests/integration/test_capacity_grant_store.py tests/unit/test_capacity_agent_contracts.py tests/unit/test_capacity_agent_admission_contracts.py tests/unit/test_capacity_manager_executable_contracts.py ... -vv` (combined final run below) covered executable contract tagged-candidate validation.

## Item 2 — Retained drain telemetry exact active-context matching

Root cause: drain-only retained heartbeat/inventory matching accepted changed original active context fields, so stale or altered writer epoch, execution state, ceiling, or rate could advance retained evidence while the current authority was drain-only.

RED:

`uv run --no-sync pytest -q tests/integration/test_capacity_manager_execution_store.py::test_retained_drain_telemetry_rejects_changed_original_active_context -vv`

Expected failure observed: all changed-field cases failed with “DID NOT RAISE ExecutionConflictError”.

Implementation:

- Tightened retained drain heartbeat/inventory matching to require the frozen active writer epoch, `execution_state="active"`, requested ceiling, requested rate, and the existing exact epoch/manifest/configuration/release bindings.
- Kept retained close-command handling separately scoped so drain-only close remains possible without weakening telemetry evidence.
- Updated test fixtures to submit the original active context for retained drain evidence.

GREEN/regression:

- `uv run --no-sync pytest -q tests/integration/test_capacity_management_store.py tests/integration/test_capacity_manager_execution_store.py tests/integration/test_capacity_manager_execution_epoch.py tests/integration/test_capacity_manager_migrate.py tests/integration/test_capacity_management_migrations.py tests/integration/test_executable_global_capacity_bridge.py tests/integration/test_capacity_manager_api.py -vv` → 206 passed, 1 warning.

## Item 3 — Personal authority-coordinate derivation

Root cause: the ORM/migration check only constrained personal capacity namespace/database syntax, not exact derivation from the personal instance name. Direct SQL could persist mismatched coordinates such as name `alice` with another personal namespace/database.

RED:

`uv run --no-sync pytest -q tests/integration/test_alembic_migrations.py::test_dev_instance_capacity_coordinates_are_derived_from_personal_name -vv`

Expected failure observed: direct SQL accepted mismatched personal capacity coordinates.

Implementation:

- Added ORM check deriving `capacity_namespace = 'loom-dev-' || name` and `capacity_database = 'loom_dev_' || replace(name, '-', '_')`.
- Added forward app migration `0097_personal_capacity_coordinate_derivation.py` to backfill derived values and replace the old syntax-only check.
- Updated migration-head policy and readiness/preflight expected revision counts.

GREEN/regression:

- `uv run --no-sync pytest -q tests/integration/test_alembic_migrations.py::test_dev_instance_capacity_coordinates_are_derived_from_personal_name tests/loom_cli/rollout/test_migration_readiness.py tests/loom_cli/rollout/test_preflight_registered_checks.py tests/integration/test_data_lifecycle_prepare.py tests/loom_cli/test_capacity_control_plane.py tests/loom_cli/test_capacity_control_plane_packaging.py tests/integration/test_capacity_grant_store.py tests/unit/test_capacity_agent_contracts.py tests/unit/test_capacity_agent_admission_contracts.py tests/unit/test_capacity_manager_executable_contracts.py tests/ops/test_global_fleet_pool_executor_once.py tests/ops/test_global_dev_fleet_autoscaler_external_once.py tests/integration/test_worker_pool_autoscaler_reconcile.py -vv` → 246 passed.

## Item 4 — Static candidate-provenance seeding

Root cause: executable lookup requires durable tagged candidate provenance, but static configuration activation did not seed exact candidate provenance. Tests and harnesses could bypass this with direct SQL, leaving the public store path unable to prove static executable candidate ownership.

RED:

`uv run --no-sync pytest -q tests/integration/test_capacity_management_store.py::test_activation_seeds_static_candidate_provenance_for_executable_lookup -vv`

Expected failure observed: `StaticCandidateProvenanceV1` was missing and activation had no trusted input path for static candidate provenance.

Implementation:

- Added `StaticCandidateProvenanceV1` and `ConfigurationActivationV1.static_candidate_provenance`.
- Required static subjects without existing candidate rows to provide exact operator-owned provenance; duplicate, missing, stale, or mismatched provenance fails closed.
- Persisted exact algorithm/identity/publication and preserved manager-derived lifecycle candidates separately.
- Updated public API/test activation payloads and executable harness activation paths to provide static provenance without direct SQL seeding.

GREEN/regression:

- Included in the 206-test manager/API suite above.
- Included in the 136-test guard/agent suite above for protected payload compatibility.

## Item 5 — Database-enforced retirement inventory freshness

Root cause: the database constraint allowed `retirement_safe=true` when `last_heartbeat_at <= last_inventory_at`; the store logic required a post-inventory heartbeat/journal confirmation, but direct SQL could preserve or set unsafe retirement state.

RED:

`uv run --no-sync pytest -q tests/integration/test_capacity_manager_migrate.py::test_executor_retirement_safety_requires_post_inventory_heartbeat -vv`

Expected failure observed: direct SQL accepted `retirement_safe=true` with heartbeat at the same time as inventory.

Implementation:

- Added capacity migration `capacity_0007_retirement_heartbeat_freshness.py`.
- Invalidated existing safe rows during upgrade before installing the stronger constraint.
- Strengthened ORM constraint to require `last_heartbeat_at > last_inventory_at`.
- Changed inventory ingestion to store evidence without marking safe; the confirming heartbeat re-evaluates stored final inventory and then marks retirement safe.
- Updated rendered inert migration Job hash for `capacity_0007`.

GREEN/regression:

- Included in the 206-test manager/API suite above.
- Focused migration-head check: `uv run --no-sync pytest -q tests/integration/test_capacity_management_migrations.py::test_capacity_models_match_migration_head tests/loom_cli/test_capacity_control_plane_packaging.py::test_wheel_contains_complete_capacity_migration_package tests/loom_cli/rollout/test_migration_readiness.py::test_repository_migration_plan_is_single_head_and_policy_bound -vv` → 3 passed.

## Static verification

- `python3 -m json.tool config/staging-migration-policy.json >/dev/null` → passed.
- `uv run --no-sync ruff format --check <32 touched Python files>` → 32 files already formatted.
- `uv run --no-sync ruff check src tests migrations capacity_guard_migrations capacity_migrations` → all checks passed.
- `uv run --no-sync mypy` → success, no issues in 805 source files.
- `git diff --check` → passed.
- `rg "capacity_0006|migrate-capacity-0006|0096|guard_0014" tests src config deploy migrations capacity_migrations capacity_guard_migrations` → only historical revision files/down-revisions, the intentionally named guard downgrade test, and the expected packaging membership entry.

## Additional verification

- Attempted `uv run --no-sync pytest -q`; the broad full suite surfaced many capacity-adjacent failures before interruption. Those failures were triaged into explicit suites and fixed where branch-related:
  - guard migration head/test fixture drift,
  - API activation missing static provenance,
  - inert submission/legacy guard PL/pgSQL payload validators missing new provenance fields,
  - downgrade tests missing new guard env roles / new rollback head.
- Attempted `uv run --no-sync pytest -q --lf -vv`; after the cache changed it expanded beyond the intended last-failed scope and was interrupted after 62 passed / 6 skipped.

## Concerns

- Full repository pytest was not completed; it expanded far outside Task 15A and was intentionally interrupted. All task-required and triaged capacity-adjacent suites listed above passed.
- Ruff format was intentionally scoped to touched Python files because full-repo format check reports pre-existing unrelated formatting drift; full Ruff lint was run and passed.

## Fix round 1 — RED evidence

Review findings addressed in this round:

1. personal capacity installer used the source candidate digest as the protected publication digest;
2. retained drain telemetry compared the frozen active fence to the mutable current writer epoch after writer failover;
3. guard_0015 backfilled legacy registration columns without preserving upgrade/replay-safe canonical agent audits;
4. capacity_0007 retirement JSON predicates allowed SQL UNKNOWN for missing inventory fields;
5. static candidate provenance activation ignored extra unmatched entries.

RED:

- `uv run --no-sync pytest -q tests/unit/test_personal_dev_reconciler.py::test_capacity_installer_configuration_uses_candidate_publication_digest -vv` → failed as expected: `candidate_publication_sha256` was `aaaaaaaa...` instead of `ffffffff...`.
- `uv run --no-sync pytest -q tests/integration/test_capacity_manager_execution_store.py::test_drain_telemetry_accepts_retained_active_registration_after_writer_failover -vv` → failed as expected with `ExecutionConflictError: execution fence changed`.
- `uv run --no-sync pytest -q tests/integration/test_capacity_guard_migrations.py::test_guard_0015_backfills_legacy_agent_registration_audit_for_replay -vv` → failed as expected during `guard_0014 -> guard_0015` upgrade: legacy registration backfill tripped `agent registration generation did not advance monotonically`.
- `uv run --no-sync pytest -q tests/integration/test_capacity_manager_migrate.py::test_executor_retirement_safety_rejects_missing_inventory_binding_fields -vv` → failed as expected: 10 missing-field cases accepted `retirement_safe=true` with `DID NOT RAISE IntegrityError`.
- `uv run --no-sync pytest -q tests/integration/test_capacity_management_store.py::test_activation_rejects_unmatched_static_candidate_provenance -vv` → failed as expected: activation ignored extra static provenance and did not raise `ConfigurationConflictError`.

Implementation:

- Personal capacity installer now carries `claim.candidate.publication_sha256` into `ReporterConfigurationV1.candidate_publication_sha256` and fails closed if the ready candidate has no publication digest.
- Retained drain telemetry now compares the frozen active execution context to `CapacityExecutionEpoch.prepared_writer_epoch`, not the mutable `current_writer_epoch`, so writer transfer during drain does not strand retained executor telemetry.
- `guard_0015` now performs migration-scoped trigger disables for the legacy registration column backfill and canonical audit-event backfill, enriches legacy `agent_registered.v1` / `agent_reconfigured.v1` payloads with candidate identity/publication fields, and recomputes their canonical payload digests.
- Capacity retirement safety checks now make the full `retirement_safe` branch explicitly boolean with `IS TRUE`, rejecting missing JSON binding fields that previously evaluated to SQL `UNKNOWN`.
- Static candidate provenance activation now rejects supplied provenance for subjects not present in the activated static subject set, while leaving manager-derived lifecycle candidates independent.
- Added explicit 0097 model/database parity coverage and restored the unrelated prior `src/loom/db/schema.py` formatting-only churn; the remaining schema diff versus the pre-remediation base is only the derived-coordinate constraint.

GREEN/regression:

- `uv run --no-sync pytest -q tests/unit/test_personal_dev_reconciler.py::test_capacity_installer_configuration_uses_candidate_publication_digest -vv` → 1 passed.
- `uv run --no-sync pytest -q tests/integration/test_capacity_management_store.py::test_activation_rejects_unmatched_static_candidate_provenance -vv` → 1 passed.
- `uv run --no-sync pytest -q tests/integration/test_capacity_manager_execution_store.py::test_drain_telemetry_accepts_retained_active_registration_after_writer_failover -vv` → 1 passed.
- `uv run --no-sync pytest -q tests/integration/test_capacity_manager_migrate.py::test_executor_retirement_safety_rejects_missing_inventory_binding_fields -vv` → 10 passed.
- `uv run --no-sync pytest -q tests/integration/test_capacity_guard_migrations.py::test_guard_0015_backfills_legacy_agent_registration_audit_for_replay -vv` → 1 passed.
- `uv run --no-sync pytest -q tests/integration/test_capacity_manager_migrate.py::test_retirement_lifecycle_schema_matches_model_and_migration -vv` → 1 passed.
- `uv run --no-sync pytest -q tests/integration/test_alembic_migrations.py::test_dev_instance_capacity_coordinate_constraint_matches_model_and_migration -vv` → 1 passed.
- `uv run --no-sync pytest -q tests/unit/test_personal_dev_reconciler.py::test_capacity_installer_configuration_uses_candidate_publication_digest tests/integration/test_capacity_manager_execution_store.py::test_drain_telemetry_accepts_retained_active_registration_after_writer_failover tests/integration/test_capacity_guard_migrations.py::test_guard_0015_backfills_legacy_agent_registration_audit_for_replay tests/integration/test_capacity_manager_migrate.py::test_executor_retirement_safety_rejects_missing_inventory_binding_fields tests/integration/test_capacity_management_store.py::test_activation_rejects_unmatched_static_candidate_provenance tests/integration/test_alembic_migrations.py::test_dev_instance_capacity_coordinate_constraint_matches_model_and_migration tests/integration/test_capacity_manager_migrate.py::test_retirement_lifecycle_schema_matches_model_and_migration -vv` → 16 passed.
- `uv run --no-sync pytest -q tests/integration/test_capacity_agent_executable_admission.py tests/integration/test_capacity_agent_store.py tests/integration/test_capacity_agent_migrations.py tests/integration/test_capacity_guard_migrations.py tests/integration/test_capacity_submission_store.py tests/integration/test_capacity_legacy_fence_store.py -vv` → 137 passed.
- `uv run --no-sync pytest -q tests/integration/test_capacity_management_store.py tests/integration/test_capacity_manager_execution_store.py tests/integration/test_capacity_manager_execution_epoch.py tests/integration/test_capacity_manager_migrate.py tests/integration/test_capacity_management_migrations.py tests/integration/test_executable_global_capacity_bridge.py tests/integration/test_capacity_manager_api.py -vv` → 218 passed, 1 warning.
- `uv run --no-sync pytest -q tests/integration/test_alembic_migrations.py::test_dev_instance_capacity_coordinates_are_derived_from_personal_name tests/loom_cli/rollout/test_migration_readiness.py tests/loom_cli/rollout/test_preflight_registered_checks.py tests/integration/test_data_lifecycle_prepare.py tests/loom_cli/test_capacity_control_plane.py tests/loom_cli/test_capacity_control_plane_packaging.py tests/integration/test_capacity_grant_store.py tests/unit/test_capacity_agent_contracts.py tests/unit/test_capacity_agent_admission_contracts.py tests/unit/test_capacity_manager_executable_contracts.py tests/ops/test_global_fleet_pool_executor_once.py tests/ops/test_global_dev_fleet_autoscaler_external_once.py tests/integration/test_worker_pool_autoscaler_reconcile.py -vv` → 246 passed.
- `uv run --no-sync pytest -q tests/integration/test_capacity_management_migrations.py::test_capacity_models_match_migration_head tests/loom_cli/test_capacity_control_plane_packaging.py::test_wheel_contains_complete_capacity_migration_package tests/loom_cli/rollout/test_migration_readiness.py::test_repository_migration_plan_is_single_head_and_policy_bound -vv` → 3 passed.

Static verification:

- `python3 -m json.tool config/staging-migration-policy.json >/dev/null` → passed.
- `git diff --check` → passed.
- `rg "capacity_0006|migrate-capacity-0006|0096|guard_0014" tests src config deploy migrations capacity_migrations capacity_guard_migrations` → only historical revision/down-revision files, expected packaging membership, and guard downgrade/backfill tests.
- `uv run --no-sync ruff format --check capacity_guard_migrations/versions/guard_0015_candidate_provenance.py capacity_migrations/versions/capacity_0007_retirement_heartbeat_freshness.py src/loom/personal_dev_capacity_runtime.py src/loom_capacity_manager/execution_store.py src/loom_capacity_manager/models.py src/loom_capacity_manager/store.py tests/integration/test_alembic_migrations.py tests/integration/test_capacity_guard_migrations.py tests/integration/test_capacity_management_store.py tests/integration/test_capacity_manager_execution_store.py tests/integration/test_capacity_manager_migrate.py tests/unit/test_personal_dev_reconciler.py` → 12 files already formatted.
- `uv run --no-sync ruff check src tests migrations capacity_guard_migrations capacity_migrations` → all checks passed.
- `uv run --no-sync mypy` → success, no issues in 805 source files.

Final pre-commit continuation check:

- Removed the leftover working-tree-only `src/loom/db/schema.py` line-wrapping churn from this fix-round commit; `git diff -- src/loom/db/schema.py` → no output.
- `git diff --check` → passed.
- `uv run --no-sync pytest -q tests/unit/test_personal_dev_reconciler.py::test_capacity_installer_configuration_uses_candidate_publication_digest tests/integration/test_capacity_manager_execution_store.py::test_drain_telemetry_accepts_retained_active_registration_after_writer_failover tests/integration/test_capacity_guard_migrations.py::test_guard_0015_backfills_legacy_agent_registration_audit_for_replay tests/integration/test_capacity_manager_migrate.py::test_executor_retirement_safety_rejects_missing_inventory_binding_fields tests/integration/test_capacity_management_store.py::test_activation_rejects_unmatched_static_candidate_provenance tests/integration/test_alembic_migrations.py::test_dev_instance_capacity_coordinate_constraint_matches_model_and_migration tests/integration/test_capacity_manager_migrate.py::test_retirement_lifecycle_schema_matches_model_and_migration -vv` → 16 passed in 34.57s.
