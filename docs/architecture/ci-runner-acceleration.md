# CI Runner Routing and Isolation

Loom CI uses hosted GitHub runners by default and can route selected x86_64
work classes to the isolated OLDLAB runner pool. Routing is an optimization;
the workflow's validation contract and required checks do not change with the
selected runner.

## Routing contract

The routing action under `.github/actions/ci-runner-route/` consumes one
versioned, signed route document and produces a frozen `runs-on` array for each
requested job key. A missing or late document selects the hosted fallback after
a bounded wait. An invalid, ambiguous, incomplete, or inconsistent document
remains a hard failure rather than being accepted as routing authority. Jobs
also verify their actual runner identity and architecture before executing.

The self-hosted label set is fixed and work-class-specific. Workflows do not
accept a caller-provided runner label, repository ref, image, or host command.
Jobs that require macOS, arm64, deployment authority, publishing credentials,
or another protected environment remain on their explicit hosted or protected
runner class.

The root-owned controller publishes the complete route CheckRun directly with
a dedicated GitHub App. The App is installed only on `qianyi-sun/loom`, has
Actions-read, Contents-read, Pull-requests-read, metadata-read, and Checks-write
repository permissions, subscribes to no events, and has no webhook. The
controller mints one repository-scoped installation token with exactly those
permissions from a systemd credential and uses it for both trusted reads and
CheckRun publication. It never loads a user token, so operator API activity
cannot consume the controller's rate-limit bucket. The controller also verifies
the returned CheckRun's exact App ID. Route delivery therefore does not wait in
a GitHub-hosted workflow queue.

Before publishing, the controller commits the canonical request, oldlab
eligibility, capacity assignments, canonical response, exact trusted workflow
generation, and delivery state in the same SQLite transaction. The stored
response is an immutable outbox item: restarts and direct-publisher retries
reuse it exactly and never recompute an age-sensitive route. Superseded
generations cannot partially update the matrix.

## Isolated pool

`scripts/ops/ci_runner_pool.py` manages the pool described by
`deploy/ci-runners/oldlab5.toml`. Each job gets a short-lived GitHub JIT runner
inside a fresh QEMU/KVM guest. The host does not run repository job code
directly.

The disposable guest disk is a sparse 128 GiB boundary so image builds and
their local vulnerability scans cannot exhaust the former 64 GiB boundary.
Large-image jobs must still use a job-owned Buildx builder and remove only that
builder, exact local tags, and job-scoped temporary directories; broad Docker
pruning is forbidden on the shared host.

The guest image and boot assets are pinned by digest. The manager validates the
candidate image, reserves a work-class slot, creates a one-job JIT
registration, starts the VM, observes completion, removes the runner
registration, and destroys the guest. Stale idle guests and registrations are
reconciled; busy runners are drained before removal.

Runner credentials come from a current-owner `0600` file and are excluded from
arguments, logs, route documents, and repository artifacts. Pool state and
cache roots are service-owned. The guest has Docker and sudo because CI jobs
need them, so VM destruction is the security boundary between jobs.

## Capacity and fallback

Work classes have explicit slot ceilings and resource shapes. The router may
prefer self-hosted capacity only when the published generation reports a
healthy compatible slot. Hosted fallback remains valid when capacity is full,
the pool is draining, or health cannot be proven. Only a route request first
observed within 90 seconds of its GitHub artifact creation may consume oldlab
capacity. The controller discovers requests from the bounded inventories of the
four active routed workflows and then performs an exact artifact-name lookup
for each run. It does not walk repository-wide artifact history or rely on a
global artifact cursor, so unrelated artifact bursts and one failed delivery
cannot block newer workflows.

Route reconciliation has its own thirty-second systemd timer and state-only
service. It is not a second `ExecStart` behind runner-pool reconciliation and
does not share the pool's QEMU, Docker, cache, or 300-second service deadline.
Pool builds, guest cleanup, GitHub JIT registration, and drain work therefore
cannot consume the route request's 90-second freshness window.

If the root-owned controller or GitHub App remains unavailable, the pinned
route action freezes the run onto the workflow's exact GitHub-hosted label after
180 seconds. A later controller pass accepts an exact late CheckRun only when it
matches the persisted response. If the route resolver has already completed
without that CheckRun, the outbox item is abandoned and its unconsumed leases
are released without blocking fresh requests. The next fresh workflow can
prefer oldlab again automatically.

`LOOM_CI_ROUTE_MODE=oldlab-preferred-v1` is therefore a persistent preference,
not a transient host-health switch. Do not delete it merely because oldlab is
temporarily unreachable. Deleting the variable remains an explicit maintenance
or emergency policy stop; restoring that policy is still an operator decision.

Queue and runner metrics use bounded work-class labels. Dynamic PR, branch,
actor, and repository-provided strings are not metric labels.

## Trusted workflow generations

The installed route runtime and the workflow eligibility baseline are separate
trust identities. `LOOM_CI_RUNNER_ROUTE_RUNTIME_SHA` pins the installed
controller code. It moves only through an explicitly authorized controller
rollout. The broker persists a
monotonic trusted workflow generation independently: exact `dev` commit and
tree SHAs, all four controlled workflow blob SHAs, predecessor identity,
protected-merge evidence, canonical digest, and acceptance time.

On each reconcile, the controller compares the current generation with `dev`
and advances at most one first-parent commit. The successor must be an exact
same-repository squash merge into `dev` with one associated merged PR. That PR's
head must have one authoritative current terminal-successful direct `repository-checks`,
`images-gate`, `cluster-smoke-gate`, and `staging-smoke-gate` CheckRun from the
GitHub Actions app (`app.id=15368`), each linked to its source Actions job. A
same-head retry may leave older direct source CheckRuns in GitHub's
`filter=latest` inventory, so the controller accepts only the unique newest
run by `started_at` after verifying that every same-name candidate comes from
app 15368 and a direct source-job URL. An older failure cannot override a newer
success, while a newer failure, tied newest run, wrong-app duplicate,
same-name status, missing or malformed check, direct push, rollback,
non-linear successor, or incomplete merge identity preserves the previous
generation. Multiple protected merges converge one generation per timer pass,
including after an API interruption or service restart.

Every route decision records the exact trusted generation and structured
eligibility reason used to freeze it. Publisher retries replay that stored
response; a later generation never rewrites an older decision. Whole-workflow
blob equality remains mandatory. The workflow-changing PR is hosted until its
protected merge is accepted, while the first fresh request inheriting the
accepted blob may use oldlab without editing `candidate.env`, reinstalling the
controller, rebuilding runner images, or toggling route mode.

The broker `status` command exposes the installed runtime SHA, exact publisher
App ID, trusted generation SHA and digest, observed `dev` SHA, generation lag,
per-workflow blob drift, last promotion result or blocker, and bounded numeric
lag, blocked, and workflow-drift metrics. A blocked promotion is therefore
diagnosable without changing trust state. Hosted fallback reasons are persisted
on individual route decisions as `trusted_workflow_match`,
`workflow_blob_drift`, `future_request`, `stale_request`, or
`legacy_schema2_frozen`.

## Operations

The pool tool exposes build, preflight, reconcile, status, and drain
operations. Run its `--help` against the installed candidate before use. A
safe rollout validates the pinned runner release and checksums, QEMU/KVM,
Docker, disk and memory headroom, guest boot, JIT registration, one disposable
job, teardown, and hosted fallback.

The systemd service requires two independent exact commit identities in
`candidate.env`. `LOOM_CI_RUNNER_POOL_CANDIDATE_SHA` is the commit bound into
the current golden QEMU image and advances only with a matching image build and
preflight. `LOOM_CI_RUNNER_ROUTE_RUNTIME_SHA` is the installed controller
runtime. The dedicated publisher App and installation IDs are also exact,
non-secret configuration. All values are mandatory. The service has no
implicit shared-candidate fallback, so a missing or malformed identity fails
closed instead of silently coupling the two release lifecycles. Write the file
as:

```text
LOOM_CI_RUNNER_POOL_CANDIDATE_SHA=<full golden-image commit SHA>
LOOM_CI_RUNNER_ROUTE_RUNTIME_SHA=<full installed route-controller commit SHA>
LOOM_CI_RUNNER_ROUTE_PUBLISHER_APP_ID=<dedicated GitHub App numeric ID>
LOOM_CI_RUNNER_ROUTE_PUBLISHER_INSTALLATION_ID=<loom-only installation numeric ID>
```

Install the route-controller modules and service unit from that exact runtime
commit. Build the root-owned
`/usr/local/lib/loom-ci-runner-controller/.venv` from the exact `uv.lock` in the
same commit and require the locked PyJWT and cryptography versions; the route
unit invokes that interpreter directly and must not inherit the host's system
Python packages. Ordinary protected workflow merges advance the database-backed
workflow generation automatically and do not change this file. The legacy
`LOOM_CI_RUNNER_ROUTE_CANDIDATE_SHA` and `LOOM_CI_RUNNER_CANDIDATE_SHA` may
coexist only while an old unit is retained for rollback; the current unit reads
neither variable.

Install and enable `loom-ci-runner-route-controller.service` and
`loom-ci-runner-route-controller.timer` independently of the existing pool
service and timer. The pool unit must contain only the pool reconcile command;
the route unit must contain only the route controller. During rollout, fail the
preflight if either unit contains both commands or if the route timer is not
active with a thirty-second interval. Each process may make at most 35 GitHub
core requests, so even a continuously saturated timer is bounded to 4,200
requests per hour, below the installation's 5,000-request budget. The
controller records the core `limit`, `remaining`, and `reset` response headers
in the existing SQLite metadata after every request. A new oneshot process
therefore inherits the previous process's budget and stops before the final 250
requests until the recorded reset time; it never turns a local restart into a
rate-limit bypass. A no-work pass performs five GitHub read requests (the
trusted branch plus four bounded workflow inventories), keeping steady-state
use near 600 requests per hour. Repeated run, job, workflow-blob, and CheckRun
reads within one reconcile use the first validated snapshot instead of spending
the installation budget twice. The route freshness window is 90 seconds and
the workflow-side bounded wait remains 180 seconds, leaving one full timer
interval for ordinary API, artifact-publication, and service latency. A
transient malformed or count-inconsistent active-run inventory is re-read at
most three times within that pass; the scan bound still fails immediately, and
persistent inconsistency fails closed without routing work.

Store the App's unencrypted PEM private key at
`/etc/loom-ci-runner-pool/route-publisher-app-private-key.pem`, owned by root
with mode `0600`. systemd exposes it only as the
`route-publisher-app-private-key` credential. Do not place the key, a minted
installation token, or a user token in `candidate.env`, command-line output,
the SQLite database, GitHub artifacts, or repository secrets. The route unit
must not load the pool's `github-token` credential. Rotate by installing a
second App key, validating both an App-owned trusted read and direct CheckRun,
then revoking the old key. The checked-in hosted publisher remains only as a
pre-activation rollback path for the older installed controller; the current
unit neither dispatches it nor waits for hosted runner capacity.

Activation is one bounded transition:

1. Create and install the dedicated App with only the permissions, repository,
   and disabled webhook/event contract above. Record the App and installation
   IDs without recording the private key in evidence.
2. Stop both CI timers, wait for the pool and route oneshots to exit, and prove
   there are no pending route decisions or active oldlab assignments. Back up
   `leases.sqlite3` with its WAL/SHM files, `candidate.env`, both installed
   units/timers, both controller wrappers/modules, and the old HMAC credential.
3. Install the exact merged runtime, its lock-derived isolated Python
   environment, the split units, and the App private-key credential; add the
   runtime, App, and installation IDs to `candidate.env`; prove the route unit
   has no user-token credential, prove the service interpreter reports the
   locked PyJWT and cryptography versions, then run `systemd-analyze verify`
   and `daemon-reload` before starting anything.
4. Start only the route service once. Require schema-3 readback, the exact
   runtime/App/generation identities, zero generation lag/blob drift, and one
   direct App-owned CheckRun that arrives before the pinned action deadline.
5. Enable the thirty-second route timer and the existing pool timer. Exercise
   one fresh normal, image, cluster-smoke, and staging-smoke route; verify the
   actual disposable runner identity and terminal lease release for every
   oldlab job.

Rollback stops both new timers, restores the pre-transition database and unit
snapshot together, restores the prior candidate environment and HMAC
credential, reloads systemd, and starts the old combined unit. Do not run an old
schema-2 controller against a schema-3 database or mix the old combined unit
with the new route timer.

The broker upgrades schema-1 or schema-2 lease databases in place to schema 3
on first controller start. Existing assignments, lease epochs, frozen route
decisions, and pending publisher outbox entries are preserved. The controller
then atomically bootstraps the initial trusted generation from the exact
installed runtime commit/tree and four workflow blobs; legacy decisions are
bound to that initial generation without changing their stored response or
dispatch state. Keep the database and its WAL files on the service-owned
persistent state volume. A pre-upgrade artifact cursor file may remain for
rollback of the old binary, but schema 2 and later never read or advance it.
Published and abandoned decisions are retained for seven days after their
terminal transition and are deleted only after every associated assignment is
released.

Draining stops new self-hosted selection, waits for busy jobs, removes idle JIT
registrations and guests, and leaves workflows using hosted runners. Do not
delete a busy GitHub runner or VM to accelerate drain unless the job itself is
being explicitly cancelled.

The current runner version floor and Node runtime compatibility are recorded
in `config/ci-upgrade-policy.json`. Workflow routing is implemented in
`.github/workflows/ci.yml`, `images.yml`, `cluster-smoke.yml`, and
`staging-smoke.yml`.

## Release image evidence

Image routing remains separate from release authority. The untrusted build and
trusted publication scans both use pinned Trivy v0.74.0 with `scan-type: image`,
`vuln-type: os,library`, `timeout: 20m0s`, `severity: CRITICAL`, `exit-code:
'1'`, `ignore-unfixed: 'false'`, `scanners: vuln`, and `cache: 'false'`. Before
each scan, a repository helper writes the fixed config and reviewed ignore file
outside the checkout. Maintained images and dependencies are upgraded before
an exception is considered. The temporary exceptions cover only the three
unfixed CRITICAL Perl findings (CVE-2026-13221, CVE-2026-42496, and
CVE-2026-8376) on the exact Debian Perl packages required by Debian base
runtimes, the agent toolchain, and the staging-compatible PostgreSQL 17.4
rehearsal image, CVE-2026-43185 on the agent compiler's
`linux-libc-dev`, and CVE-2025-7458, CVE-2026-6653, and CVE-2023-45853 on the
staging-compatible PostgreSQL 17.4 rehearsal dependencies. Each structured
exception records its exact Debian PURL scope and review statement and expires
at 2026-09-12 UTC; policy generation fails closed at that boundary. A second
repository-owned helper installs only
the architecture-specific v0.74.0 release archive against its
repository-pinned SHA-256; this avoids relying on actions forbidden by
repository policy. The
signed release predicate binds every reviewed field, scanner name/version,
release URL and architecture archive digest, controlled-file hashes, explicit
exception IDs, package scopes, statements, expiries, and resulting report
digest. A failed scan prints a bounded, log-safe critical-finding summary
while preserving Trivy's exit code.

The untrusted PR builder keeps each architecture archive job-local, scans it
with the controlled Trivy policy, and does not upload it. The hosted publisher
rebuilds every architecture archive from the protected release commit and
captures the single digest emitted by each architecture push. Official evidence
accepts only `trusted-rebuild` and binds the release head, tree, ref, and current
run; it never downloads, loads, attests, or publishes PR-built bytes. After
immutable-digest attestation verification, each architecture uploads one
uniquely named canonical record. The manifest job downloads and accepts exactly
the current image's AMD64 and ARM64 records, verifies their recorded registry
subjects, and joins only their immutable
digests.

Manifest creation writes only the temporary `manifest-${HEAD_SHA}` tag and
captures the creation digest directly. Registry validation and final
attestation verification use that digest, never a mutable-tag rediscovery. The
official SHA and branch tags are promoted only after that verification.
