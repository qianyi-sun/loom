# Nebius CI on dev

`dev` supports Nebius and remains the pre-main branch for the combined repository.
The earlier `codex/nebius-main` CI supplied path routing, optional Python coverage
and a smaller runtime scope. Daily dev CI now follows the same Nebius runtime
boundary while retaining the four protected admission contexts.

## Retained validation

All four protected contexts remain required: `repository-checks`, `images-gate`,
`cluster-smoke-gate`, and `staging-smoke-gate`. The shared planner selects their
work; selected failures, cancellations and unexpected skips still block merging.
Draft filtering, current-head/base requirements, and native squash auto-merge
retain the [repository contract](../../CONTRIBUTING.md).

The manifest retains ownership of both historical and Nebius tests. Daily dev CI
selects Nebius and common contracts; historical compatibility is manual. PR image CI uses the same seven-image Nebius set as candidate publication,
builds only affected AMD64 images and scans the resulting artifacts. Existing signed publication
consumers retain their declared architecture manifests until migrated. Disposable
Kubernetes checks include the Nebius platform and execution contracts. Candidate
publication uses the protected `nebius-integration` Environment from `dev`;
PR checks do not receive its credentials. CI does not deploy the live platform.

## Nebius path routing

Static checks remain the ordinary baseline. Frontend-only changes do not start
Python test, Go, runtime-payload, dependency-lock, Terraform, Compose or Kubernetes
runners. The Compose system fixture has no Web service; browser/auth changes are
covered by the frontend job. Backend/frontend mixed changes and explicit smoke
labels still select the corresponding checks. Independent
test edits start their owning test jobs. Shared and unknown inputs retain the full
baseline; Terraform runs for its own inputs and full-regression requests.
Manifest-ignored retired inputs do not allocate backend test runners; static
validation still runs. Additional
heavy validation follows the changed files:

| Changed file | Additional validation |
| --- | --- |
| Frontend source, Web Dockerfile, Nginx SPA config or browser runtime config | Web type/lint/unit/coverage/browser checks and affected images |
| Platform renderer source | Integration, affected images, Kubernetes |
| Independent platform renderer unit tests | Kubernetes |
| Deployment/render operator scripts and `deploy/nebius/` configuration | Integration, Kubernetes |
| Restore verifier operator script | Integration |
| Nebius Terraform and its checker | Integration, the IaC checks |
| Candidate workflow/publisher, registry authentication, shared scan validator | Full validation |
| Unknown runtime or operator files | Full validation |

Mixed changes take the union of their requirements. CI labels can add checks and
cannot subtract path-selected checks. This avoids treating known deployment
files as unknown code while retaining full validation for publication authority.
The actual tests are selected from `config/component-ownership.toml`.

## Coverage collection

Python root/package/integration tests normally run without coverage instrumentation.
Request `ci:coverage-summary` or dispatch CI with `coverage_summary=true` for
full selection within the active scope and combined accounting. Nebius coverage
is reported without applying the historical all-platform **70%** floor to a
different test population. The floor still applies in compatibility/main scope.
Coverage collection/reporting errors still block the repository gate. Frontend
coverage thresholds remain separate. No p50/p90 speedup is claimed before live
measurement; the removed instrumentation and artifact work is deterministic.

## Test-only changes

An edit consisting only of independent Python test modules runs those files in
their owning lanes. Any external reference to an edited test module name keeps
full validation because test modules can provide shared fixtures. Shared or deleted
test inputs select their possible consumer jobs as well as retaining all files
inside those jobs, including when mixed with source changes. Runtime,
migration, configuration, fixture, deleted-file and unknown changes stay full
unless a suite explicitly declares an audited unaffected component.
CI selector labels and manual runs request full regression within the active scope; ordinary labels such
as `bug` do not change test selection. Sharding happens before filtering,
so ownership and paired-fixture ordering remain stable. Empty selections do not
invoke pytest, and selector errors fail the job.

Go/Python guard and publication interoperability tests have one owner,
`go-checks`, and run only in manual compatibility/main scope. Ordinary Go checks
retain execution runtime, gateway sandbox and sandbox runtime; they do not install
Python or build the historical task-image-builder supervisor test binary.

## Integration shard balance

The fast integration lane uses four complete, non-overlapping file shards.
`config/component-ownership.toml` owns the stable hash salt and the paired
username/password fixture ordering. New files do not reshuffle existing files.
Docker integration remains a separate lane, and every required gate still waits
for all selected shards. Required platform tests are retained.

The successful PR #1973 head `69359261` ran its two integration shards in
66 and 90 minutes. Timestamped progress for 518 modules accounts for about
155 minutes of test work. Repartitioning that same work projects approximately
39.0, 38.2, 38.8 and 39.1 minutes across four runners, before setup, queueing and
new tests. This is a planning estimate, not a measured speedup. The 75-minute
job budget retains room for optional coverage and runner variation.

All four source workflows use GitHub-hosted runners directly. The OLDLAB route
action, lease broker, controller, custom CheckRun publisher and dedicated KVM
runner assets have been removed. This does not stop existing host services or
cancel in-flight jobs. See [runner placement](../architecture/ci-runner-acceleration.md).

Harbor runtime builds follow its manifest-owned inputs rather than every image
change. Manual compatibility retains recursive Go vet/race checks and the compiled
supervisor binary required by Python–Go interoperability tests.

## Publication ownership

`nebius-candidate.yml` is the only automatic **dev** image publisher. `images.yml`
uses the Nebius manifest set for dev PRs and untrusted manual validation. Harbor
uses its own selection, so a Web-only PR builds one image and a complete platform
validation builds seven, without a second Harbor build. PR scanner preparation
fetches only AMD64.

Historical personal-dev consumers still require dual-architecture manifests.
Their compatibility publication remains available through a manual `trusted-image-release-controller.yml` dispatch; that workflow
performs the bot-authenticated trusted `images.yml` dispatch, and
the controller has no schedule. Existing main-branch production publication is
unchanged. This CI change neither retires personal-dev product functionality nor
stops existing OLDLAB services. Migrating those consumers remains separate work.

## Daily regression and expensive components

CI runs once daily at 08:23 UTC on the default branch (`dev`). It uses the same
jobs with full Nebius/common root/package/Go/Web/integration/Docker selection and coverage;
it does not publish or deploy and adds no PR admission context. The scheduled
result is named `repository-checks-scheduled`.

The historical schema-reference matrix is manual compatibility only. Nebius
bootstrap, migration lineage, Alembic upgrades, application migration authority,
runtime grants and schema inventory remain daily. Within manual compatibility,
the existing component selector can omit the historical matrix for audited
unrelated changes; full compatibility and coverage requests still run it.
Docker test-only changes use the same selection and failure propagation as
ordinary integration.

The pre-change integration sample (run `35485701767`, shard 3) spent 41m55s in
pytest and about 22s preparing the runner and dependencies. This identifies test
work as the dominant cost; it is not a measurement of this change's speedup.


## Daily tests and manual compatibility

The existing CI workflow accepts `legacy_compatibility=true`; no new workflow or
required check is introduced. Daily PRs, normal dispatch and scheduled runs use
`CI_TEST_SCOPE=nebius` plus the pytest expression `not legacy_pool`. Main production
validation and an explicit compatibility dispatch use `all`. Scope filtering is
after stable sharding, so shard pins and fixture ordering do not move. Unknown
and mixed/common test modules stay in daily validation.

`config/component-ownership.toml` owns the explicit `compatibility_test_paths`:

| Manual compatibility only | Reason | Daily coverage retained |
| --- | --- | --- |
| Historical CLI `rollout`, protected database ownership/login handoff and the six-profile schema-reference matrix | Nebius uses `deploy_nebius_platform` and `nebius_platform_bootstrap.bootstrap_database`, without the old sealed-owner takeover protocol | Nebius bootstrap/deployment/render/restore, application migration authority, runtime grants, schema inventory and common migrations |
| Dedicated OLDLAB/GB10/Slurm modules and existing `legacy_pool` cases in mixed modules | Nebius disables the Slurm controller and uses its execution actuator | Nebius quota/capacity, scheduling, registration and common execution contracts |
| CNPG operator handoff/fencing tests | Independent Nebius deployment does not use the historical CNPG takeover chain | Common PostgreSQL migrations, privileges, backup and restore |
| Capacity-manager/agent/guard/executor, global fleet, personal-dev stores/provisioners/builders and old native host recovery | These services are not deployed by the Nebius renderer; `dev_instances_enabled` defaults to false, personal-dev pools are empty and its native builder is disabled | Current execution capacity, Nebius quota/autoscaling, service execution, common auth/storage and shared migrations |
| Standalone task-image authority/guard/signer/publication, historical KVM builder and Python–Go supervisor handoff | Nebius uses `NativeTaskImageController`, not the standalone authority and personal-dev build service chain | Build plans, registration, materialization, source journals, ready bindings, execution pins and mixed shared-store tests |

The old staging cluster render/isolation checks use the same manual scope;
daily cluster smoke retains the execution actuator and Nebius platform/runtime
contracts. Ownership is retained, and excluded modules remain importable as
fixtures. Scope selection does not alter source lint, runtime configuration,
image publication or branch protection.

### Runtime scope audit after #2007

The integration branch's small test population was primarily due to its
`ci_ignored_paths` excluding entire inactive runtime families. Merely excluding
GB10/Slurm filenames and `legacy_pool` cases on dev left thousands of capacity,
personal-dev and standalone image-authority tests in normal admission.

These families now use `compatibility_test_paths`, so manual validation restores
them without dropping their ownership. This is a runtime boundary, not a fixed
test-count budget. New tests outside the audited compatibility families remain
daily by default. Do not copy every historical exclusion: for example,
`test_worker_pool_autoscaler_api.py` now tests Nebius finance APIs and stays daily.
`test_native_build_source_isolation.py` and `test_task_image_build_plan.py` also
exercise current execution and remain daily. Shared fixture imports do not mean
the disabled service's entire test suite must execute.

Current render/config evidence is in `src/loom/nebius_platform_render.py`,
`src/loom_service/app.py` and `src/loom_control_plane/app.py`: the old dev-instance
provisioner and standalone task-image execution service are not configured.
`src/loom_execution_actuator/task_image_controller.py` directly owns Nebius image
builds using current materialization and execution-capacity stores.

Integration-branch runs 35394447854 and 35155582621 completed in about 14.5 minutes.
The final #2007 run 35492370318 took 38m41s, passing 7,341 ordinary integration
cases (7,351 including skips), versus 1,821 passes across two shards in 35394447854.
The initial #2011 runtime-family correction collected 2,391 cases: 1,863 in
existing integration-branch modules and 528 in 45 added modules. The next audit
also isolated standalone authority cleanup/credential tests, reducing ordinary
integration to **2,277 cases in 295 files**, from 7,351 in 519 files before #2011.
Current migration/source/materialization/lease boundary tests remain. These are
collection counts, not a claim that every selected test has passed on each head.

### Full PR lane audit

The table audits every remaining lane, including work outside pytest selection.
Timings below are job wall times from #2011 head `187872c9d`, not estimates for
later amendments. CI run 35495791483 succeeded in **10m26s**, versus **38m41s**
for #2007. Its companion cluster run failed with a Kubernetes TLS EOF, so this
head did **not** pass complete four-context admission. Do not conflate the CI
workflow timing with full PR acceptance or live Nebius verification.

| Check | Observed job time | Current scope and decision |
| --- | --- | --- |
| Lint/static/codegen | 47s | Keep shared Python type/import, syntax, settings and dependency contracts. Only the inactive global-capacity no-mutation audit moves to manual compatibility. |
| Independent locked install | 16s | Ordinary source changes reuse `uv sync --locked` and `uv pip check` in their test jobs. Keep this separate probe for dependency/CI authority, unknown inputs and full manual/scheduled/coverage requests. |
| Root tests | 6m11s / 8m12s | 600 files at measured head; now 566 after tracing standalone image-authority clients, old staging attachments and standalone host tools. Current build plans, source isolation, GPU capability contracts and mixed worker/CI security modules stay. |
| Package tests | 38s | Keep all 58 manifest-owned benchmark/launcher/checksum package files. These serve current imports, runtime and catalogs. |
| Ordinary integration | 6m24s–9m42s across four shards | Keep current API/auth, gateway, SQL/migrations, scheduling, source/image materialization and execution leases. Standalone authority issuance/retirement inventory is manual; mixed shared-store fencing stays daily. |
| Docker integration | 1m57s | Keep 21 files covering actual driver/network/gateway/artifact and Nebius TLS/restore behavior. Old KVM/personal-dev/authority chains are already manual. |
| Go | 43s | Keep current execution/gateway/sandbox packages and future packages by default. Historical builder supervisor/Python handoff is manual. |
| Runtime payload | 24s | Ten current/common fixture files remain. Two GB10 catalog cases now obey the same manifest compatibility scope, including when invoked by the standalone runner. |
| Installed wheel | Included in root shard 1 | Verify installed Nebius renderer/actuator imports and packaged settings schema. Old cluster/rollout imports, templates and dashboard/Envoy assets are checked only in compatibility. Keep this packaging boundary because editable source tests cannot detect missing wheel files. |
| Frontend | 2m35s | Keep all 83 owned files plus production build, browser, accessibility, network/console, type/lint and coverage checks. Pure frontend edits no longer start a backend Compose stack that does not serve the UI. |
| Images | About 3m17s for the source workflow | Keep affected AMD64 builds and artifact vulnerability scans for the seven Nebius images. Historical publishers stay manual; existing integrity boundaries are unchanged. |
| Kubernetes contract | 1m59s, failed | Keep three modules covering current manifests and actuator/runtime behavior. `test_runtime_executes_task_native_sidecar_and_verifier_without_docker_socket` hit TLS EOF while reading its broker Pod; do not skip or relabel it as legacy. This CI fixture result is not a live platform observation. |
| Backend system smoke | 6m33s | Keep eight files for cross-service API/database/gateway/worker behavior, cancellation/crash recovery and benchmark ingestion. It is a disposable common backend regression, not full Nebius deployment acceptance. |

The validator itself was also profiled: the expanded compatibility list repeatedly
classified the entire tracked tree for each pattern. It now computes runnable
paths once per invocation, retaining full ownership, syntax and marker validation.
Three local real-CLI runs improved from 3.009/3.012/3.131s to
2.195/2.177/2.180s. There is no persistent cache or stale-file trust shortcut.

Tests of old attachment tools named `nebius_staging_*` are compatibility-only:
they connect Nebius to the historical CNPG/staging deployment. The independent
platform bootstrap, renderer, current capacity API and task-image actuator remain
in daily scope. CI reliability reporting and release policy tests with shared
semantics stay; a legacy reference alone does not justify dropping a mixed test.

Run the full compatibility tests, including both integration tiers:

```sh
gh workflow run ci.yml --ref dev -f legacy_compatibility=true
# Historical Kubernetes/system contracts use the existing smoke workflows:
gh workflow run cluster-smoke.yml --ref dev -f legacy_compatibility=true
gh workflow run staging-smoke.yml --ref dev -f legacy_compatibility=true
# Also request the historical all-platform coverage floor:
gh workflow run ci.yml --ref dev -f legacy_compatibility=true -f coverage_summary=true
```

This validates disposable fixtures and does not publish or deploy. Historical
image publication retains its separate manual controller entry point. Known
PostgreSQL/k3s fixture failures recorded in #2008 remain unresolved compatibility
issues. The owner closed that investigation as not planned; changing daily CI
scope does not claim a fix or add retries for those defects.
