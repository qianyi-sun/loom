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

The manifest retains both existing-platform and Nebius tests. Native image CI
continues to build AMD64 and ARM64 and scan the resulting artifacts. Disposable
Kubernetes checks include the Nebius platform and execution contracts. Candidate
publication uses the protected `nebius-integration` Environment from `dev`;
PR checks do not receive its credentials. CI does not deploy the live platform.

## Nebius path routing

Every non-documentation change retains root/package tests, static checks and
the ordinary baseline. Additional heavy validation follows the changed files:

| Changed file | Additional validation |
| --- | --- |
| Platform renderer source | Integration, affected images, Kubernetes |
| Platform renderer unit tests | Kubernetes |
| Deployment/render operator scripts and `deploy/nebius/` configuration | Integration, Kubernetes |
| Restore verifier operator script | Integration |
| Nebius Terraform and its checker | Integration, plus the baseline IaC checks |
| Candidate workflow/publisher, registry authentication, shared scan validator | Full validation |
| Unknown runtime or operator files | Full validation |

Mixed changes take the union of their requirements. Labels can add checks and
cannot subtract path-selected checks. This avoids treating known deployment
files as unknown code while retaining full validation for publication authority.
The actual tests are selected from `config/component-ownership.toml`.

## Coverage collection

Root and package shards always collect Python coverage. Their combined report
still enforces **70%** through `fast-checks`. Shards save coverage data; report
generation occurs after combination instead of generating unused reports in
every shard. Frontend coverage thresholds remain enforced separately.

Integration tests normally run without Python coverage instrumentation. To
collect combined root/package/integration coverage, add `ci:coverage-summary`
or dispatch CI with `coverage_summary=true`. Either request also selects the
integration lane. `ci:integration` requests the functional tests alone.
The optional report covers the same source scope and test selection; it does
not change the required fast-tier floor. Once requested, its existing report
validation remains enforced by the repository gate.

No wall-clock speedup is assumed: runner queueing and test runtimes vary. The
removed work is integration coverage instrumentation/uploads on ordinary runs
and redundant per-shard report generation. Functional test selection is unchanged.

## Integration shard balance

The fast integration lane uses four complete, non-overlapping file shards.
`config/component-ownership.toml` owns the stable hash salt and the paired
username/password fixture ordering. New files do not reshuffle existing files.
Docker integration remains a separate lane, and every required gate still waits
for all selected shards. Root coverage, ARM builds and platform tests are retained.

The successful PR #1973 head `69359261` ran its two integration shards in
66 and 90 minutes. Timestamped progress for 518 modules accounts for about
155 minutes of test work. Repartitioning that same work projects approximately
39.0, 38.2, 38.8 and 39.1 minutes across four runners, before setup, queueing and
new tests. This is a planning estimate, not a measured speedup. The 75-minute
job budget retains room for optional coverage and runner variation.

The four integration shards use GitHub-hosted runners directly. The pinned
trusted routing action recognizes only the former two-shard keys; promoting a
new routing contract requires its normal protected release. Other eligible jobs
retain OLDLAB routing, with root tests first in the available slot order. This
change neither promotes unmerged routing code nor requires a controller rollout.
