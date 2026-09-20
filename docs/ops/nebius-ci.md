# Nebius CI on dev

`dev` supports Nebius and remains the pre-main branch for the combined repository.
The earlier `codex/nebius-main` CI supplied useful path routing and optional Python
coverage collection. Its platform retirement and single-check admission policy
belonged to that isolated branch.

## Retained validation

All four protected contexts remain required: `repository-checks`, `images-gate`,
`cluster-smoke-gate`, and `staging-smoke-gate`. The shared planner selects their
work; selected failures, cancellations and unexpected skips still block merging.
Draft filtering, current-head/base requirements, and native squash auto-merge
retain the [repository contract](../../CONTRIBUTING.md).

The manifest retains both existing-platform and Nebius tests. PR image CI uses the same seven-image Nebius set as candidate publication,
builds only affected AMD64 images and scans the resulting artifacts. Existing signed publication
consumers retain their declared architecture manifests until migrated. Disposable
Kubernetes checks include the Nebius platform and execution contracts. Candidate
publication uses the protected `nebius-integration` Environment from `dev`;
PR checks do not receive its credentials. CI does not deploy the live platform.

## Nebius path routing

Static checks remain the ordinary baseline. Frontend-only changes do not start
Python test, Go, runtime-payload, dependency-lock or Terraform runners. Independent
test edits start their owning test jobs. Shared and unknown inputs retain the full
baseline; Terraform runs for its own inputs and full-regression requests. Additional
heavy validation follows the changed files:

| Changed file | Additional validation |
| --- | --- |
| Frontend source or web Dockerfile | Web, affected images and system smoke; auth/ingress contracts also select Kubernetes |
| Platform renderer source | Integration, affected images, Kubernetes |
| Platform renderer unit tests | Kubernetes |
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
full test selection, combined accounting and the **70%** fast-tier floor.
Once requested, coverage failures still block the repository gate. Frontend
coverage thresholds remain separate. No p50/p90 speedup is claimed before live
measurement; the removed instrumentation and artifact work is deterministic.

## Test-only changes

An edit consisting only of independent Python test modules runs those files in
their owning lanes. Any external reference to an edited test module name keeps
full validation because test modules can provide shared fixtures. Runtime,
migration, configuration, fixture, deleted-file and unknown changes stay full
unless a suite explicitly declares an audited unaffected component.
CI selector labels and manual runs request full regression; ordinary labels such
as `bug` do not change test selection. Sharding happens before filtering,
so ownership and paired-fixture ordering remain stable. Empty selections do not
invoke pytest, and selector errors fail the job.

Go/Python guard and publication interoperability tests have one owner,
`go-checks`, rather than running again in ordinary integration shards.

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
change. The Go recursive vet/race-test commands cover the supervisor once; its
compiled binary remains available for the Python–Go interoperability tests.

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
jobs with full root/package/Go/Web/integration/Docker selection and coverage;
it does not publish or deploy and adds no PR admission context. The scheduled
result is named `repository-checks-scheduled`.

The schema-reference suite provisions all historical revisions, PostgreSQL majors
and owner profiles. Its full matrix still runs for DB/library/CLI/provisioning,
dependency, fixture, unknown or mixed changes, and for nightly/manual/coverage
runs. Only audited unrelated service, gateway, Web and documentation-only changes
can omit this suite. The manifest owns this small component map; other integration
suites continue to run. No shared mutable database or cached schema substitutes
for independent provisioning. Docker test-only changes now use the same selection
and failure propagation as ordinary integration.

The pre-change integration sample (run `35485701767`, shard 3) spent 41m55s in
pytest and about 22s preparing the runner and dependencies. This identifies test
work as the dominant cost; it is not a measurement of this change's speedup.
