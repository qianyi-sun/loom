# CI runner acceleration

Issue [#1058](https://github.com/qianyi-sun/loom/issues/1058) owns the
oldlab-5 CI migration. The objective is to reduce Loom's overall CI critical
path and aggregate runner time without weakening the existing protected-check,
candidate, or publishing boundaries.

## Routing contract

Every eligible workflow has a small GitHub-hosted route-plan job. With
`LOOM_CI_ROUTE_MODE` absent or equal to `disabled`, the pinned route action
returns the class's GitHub-hosted label for every selected job. Setting the
variable to exactly `oldlab-preferred-v1` activates trusted per-job assignment;
any other value fails closed.

The retired static variables `LOOM_CI_NORMAL_RUNS_ON`,
`LOOM_CI_IMAGE_RUNS_ON`, `LOOM_CI_SMOKE_RUNS_ON`, and
`LOOM_CI_ACCELERATOR_RUNS_ON` are no longer workflow placement inputs. Keep
them absent so stale operator state cannot be mistaken for active routing.
The separate manual `cluster-deploy-spikes` proof remains explicitly
GitHub-hosted and is not part of the required-check route contract.

The accepted class-specific oldlab-5 values append exactly one reserved-pool
label to the shared boundary:

```json
["self-hosted","linux","x64","loom-ci","oldlab-5","ephemeral-kvm","loom-ci-normal"]
["self-hosted","linux","x64","loom-ci","oldlab-5","ephemeral-kvm","loom-ci-image"]
["self-hosted","linux","x64","loom-ci","oldlab-5","ephemeral-kvm","loom-ci-smoke"]
```

The first migration slice covers:

- lint/static, root test shards, package tests, runtime payload, Go, and web;
- fast and Docker integration;
- untrusted AMD64 image builds (ARM64 builds use native GitHub-hosted ARM);
- the mutation-free live-k3s render contract, manifest-owned system smoke, and
  deploy spikes.

Planning, aggregation, protected gate publication, and trusted image publishing
remain on GitHub-hosted runners. Image validation is architecture-aware: AMD64
uses the accepted oldlab image class when its route is active, while ARM64 uses
`ubuntu-24.04-arm` and never waits behind or consumes an x86_64 KVM slot. Trusted
pushes build both architectures on matching GitHub-hosted CPUs, publish
head-and-architecture tags, then join and verify exactly `linux/amd64` and
`linux/arm64` in a separate manifest job. The Linux ARM locked-environment check
also remains GitHub-hosted. Small setup-dominated jobs can remain hosted when
moving them would consume accelerator capacity without reducing the workflow
critical path.

Changing the route mode changes only placement. It must never change the job
steps, selected test paths, permissions, timeout, protected context, image
candidate identity, or result aggregation.

## Capacity-aware preferred routing

Issue [#1207](https://github.com/qianyi-sun/loom/issues/1207) supersedes static
class-variable activation as the intended steady-state placement policy. The
oldlab pool must remain warm and is preferred for every eligible AMD64 job, but
a newly created job uses GitHub-hosted capacity when every slot in its class is
already leased. Releasing an oldlab lease makes that slot the preferred target
for the next allocation.

GitHub Actions does not implement ordered fallback between a self-hosted label
set and a GitHub-hosted label. It also does not move an already queued job when
a repository variable changes. Consequently, toggling
`LOOM_CI_*_RUNS_ON` based on queue age is not capacity-aware overflow and must
not be used to claim #1207 acceptance.

`scripts/ops/ci_runner_lease_broker.py` is the atomic placement authority. It
uses a root-owned SQLite database (default
`/var/lib/loom-ci-runner-pool/leases.sqlite3`) and the checked-in pool profile
to enforce exactly five normal, four image, and two smoke leases across
concurrent workflow runs. Each request binds repository, workflow run, attempt,
job key, head SHA, and work class. An exact replay returns the frozen placement;
changed identity fields fail closed. The broker assigns the lowest available
oldlab class slot and returns the class-specific GitHub-hosted label only when
all oldlab slots are already leased.

The trusted controller submits one schema-1 route request for the complete set
of eligible jobs in a workflow attempt. The request also binds the immutable
workflow name and numeric workflow ID; the broker accepts only the checked-in
`CI`, `images`, `cluster-smoke`, and `staging-smoke` contracts and derives the
class, exact eligible job-key allowlist, and diagnostic lease deadline instead
of trusting caller-supplied values. A request may select a needed subset of the
allowlist but cannot invent capacity-consuming jobs. Every selected job is allocated under one
`BEGIN IMMEDIATE` transaction. A replay mismatch or invalid job rolls the
entire batch back, so a controller crash or malformed request cannot publish a
partially leased route. The returned document includes a canonical request
SHA-256 plus the frozen assignment, lease epoch, slot, and `runs-on` array for
every job key.

The root-owned `ci_runner_route_controller.py` is the outbound-only transport
between GitHub and that broker. On each pool timer tick it scans a bounded set
of `loom-ci-route-request-v1-<workflow-id>-<run>-<attempt>` artifacts, reads
exactly one bounded `loom-ci-route-request.json`, and verifies the artifact, live
workflow run, repository, workflow ID, attempt, event, and head SHA before
allocating. The GitHub credential is sent only to `api.github.com`; artifact
redirects are followed in a separate request with no authorization header.
The REST run `name` is intentionally not an identity field because workflows
with `run-name` expose that dynamic display title there; the allowlisted
workflow ID remains the stable workflow-name binding.

Oldlab eligibility additionally requires the source workflow's Git blob at the
run head to equal the blob in the installed merged controller candidate. A PR
that changes its workflow therefore receives a frozen all-hosted route even if
oldlab has space. The controller signs the canonical CheckRun request with a
host-only HMAC key and dispatches the default-branch
`ci-runner-route-publisher` workflow. That trusted workflow checks out the
publisher script from the exact installed candidate and uses its GitHub Actions
workflow token to create the CheckRun. The host PAT never writes CheckRuns
directly. The publisher accepts only the `loom-ci-route-v1` schema, exact
repository/workflow/run/head/request-digest identity, and an identical replay;
an invalid signature, foreign CheckRun, or duplicate identity fails closed.
The controller polls for that exact GitHub-Actions-app-owned result before
advancing its artifact cursor. It then observes the exact attempt's job list
and releases each matching lease only after the job, or the entire run when a
job was never created, is terminal.

Source workflows must consume `.github/actions/ci-runner-route` by the full
merge SHA that introduced the reviewed action, never as a local action and
never by a mutable branch. The pinned action creates the exact artifact and
accepts only the matching successful CheckRun, request digest, job-key order,
class labels, lease epochs, and class-bounded unique slots. A missing
controller response fails the route-plan job; it is not permission to use
hosted capacity while oldlab availability is unknown.

Example route request:

```json
{
  "schema_version": 1,
  "repository": "qianyi-sun/loom",
  "workflow_name": "CI",
  "workflow_id": 302898379,
  "workflow_run_id": 123456789,
  "run_attempt": 1,
  "head_sha": "0123456789abcdef0123456789abcdef01234567",
  "job_keys": ["lint-static", "tests-root-1-of-4"]
}
```

A lease deadline is diagnostic, not permission to reuse a possibly queued or
running slot. Cancellation, completion, supersession, or expiry releases a
lease only after the trusted controller observes the exact job as terminal and
supplies the matching lease epoch. This prevents a slow job from overlapping a
new assignment merely because a wall-clock timeout elapsed.

Example against a non-production state database:

```bash
uv run --no-sync python scripts/ops/ci_runner_lease_broker.py \
  --state-db /tmp/loom-ci-runner-leases.sqlite3 \
  --profile deploy/ci-runners/oldlab5.toml \
  allocate-route --request-file /tmp/route-request.json

uv run --no-sync python scripts/ops/ci_runner_lease_broker.py \
  --state-db /tmp/loom-ci-runner-leases.sqlite3 \
  --profile deploy/ci-runners/oldlab5.toml \
  status
```

The controller and lease core alone do not authorize static route activation.
The source workflow still has to upload the request, validate the immutable
CheckRun response, and consume only its job-key assignment. Until that workflow
integration merges and passes the controlled overflow/reuse A/B, keep the class
variables absent and treat the pool and route as separate live controls.

## Activation prerequisites

Do not set `LOOM_CI_ROUTE_MODE=oldlab-preferred-v1` until all of these are true:

1. oldlab-5 launches runners only inside disposable KVM guests. A workflow
   never executes directly on the host and never receives the host Docker
   socket.
2. Every registered runner uses `--ephemeral`, accepts exactly one job, and is
   discarded with its writable guest disk after the job.
3. The guest has no route or credentials to Loom staging, production, shared
   storage, host management, or other private control planes. Required public
   dependency egress is explicitly bounded.
4. Runner registration tokens are acquired just in time, injected without
   logging, and absent after registration. A job receives no persistent GitHub,
   SSH, registry, cloud, kubeconfig, or host credentials.
5. The pool enforces the accepted labels, x86_64 architecture, `umask 022`,
   Docker/Buildx/Kind capacity, and a host-level aggregate budget no larger
   than 22 physical CPUs and 80 GiB for the initial acceptance window.
6. Health readback proves enough idle one-job runners for the selected matrix,
   and teardown proves that runner registrations, guests, disks, containers,
   volumes, writable caches, and QEMU/binfmt handlers do not persist.

PR image builds remain read-only and cache-write-free. The `publish` image job
continues to run on GitHub-hosted infrastructure with its existing trusted
branch and package-write checks.

## Pool shape and measured resource sensitivity

The initial pool has 11 one-job KVM guests. Each guest has 6 GiB RAM and sees
8 virtual CPUs so a job can burst when the host has idle capacity. These 88
visible guest vCPUs are deliberately overcommitted; they are not 88 physical
CPUs. Every QEMU container is attached to
`loom-ci-runner-pool.slice`, which caps the entire pool at 22 physical CPUs and
80 GiB while equal CPU shares prevent one guest from reserving the host.

The checked-in reservation assigns five slots to normal tests, four to image
builds, and two to cluster/staging smoke. This resource-neutral rebalance keeps
the eleven-guest and host-budget limits unchanged while addressing the normal
one-job-runner turnover breach observed during the 2026-08-05 live A/B. The
controller timer reconciles every ten seconds so a completed disposable guest
does not add a full thirty-second polling delay before replacement. A runner is
registered with exactly
one of `loom-ci-normal`, `loom-ci-image`, or `loom-ci-smoke`; status is healthy
only when the runner's GitHub labels match the class reserved for its local
slot. Therefore an eleven-way image matrix can use at most four oldlab slots
and cannot consume the five normal-test or two smoke reservations. Zero-slot
classes are accepted only by unit-test profiles; the checked-in production
profile and its regression test require `5/4/2`.
This is pool profile schema 2; a schema-1 profile is rejected rather than being
silently interpreted as class-isolated.

The first isolated Track 3 A/B briefly selected two obsolete kind deployment
rehearsals in addition to the manifest-owned system smoke. Those two jobs no
longer represented live staging after its migration to the five-node k3s
cluster, and their simultaneous occupancy of both smoke slots caused the
cluster lane to breach its queue boundary. That result is workflow-contract
drift, not evidence that the pool needs a twelfth guest. The required context
names stay stable, but their selected work is now one mutation-free k3s render
contract plus one manifest-owned system smoke. The accepted `5/4/2` profile
therefore remains the Track 3 candidate for the replacement A/B.

This shape avoids hard-partitioning oldlab-5 into eleven 2-core machines. A
cold, isolated `agent-sandbox` multi-architecture build on 2026-07-28 measured
784 seconds at 2 vCPU/6 GiB and 791 seconds at 8 vCPU/6 GiB. The 0.9% difference
is noise for this ARM64-emulation-dominated job, and both are well below the
recorded 1,915-second GitHub-hosted full-job baseline. Other checks such as Go,
web, and root shards can use more parallel CPU, so the production profile
retains burstable vCPUs while enforcing the aggregate host boundary.

The reproducible cold comparison is:

```bash
sudo /usr/local/libexec/loom-ci-runner-pool \
  --profile /etc/loom-ci-runner-pool/profile.toml \
  --candidate-sha "$CANDIDATE_SHA" \
  benchmark-agent-sandbox --vcpus 2 --execute

sudo /usr/local/libexec/loom-ci-runner-pool \
  --profile /etc/loom-ci-runner-pool/profile.toml \
  --candidate-sha "$CANDIDATE_SHA" \
  benchmark-agent-sandbox --vcpus 8 --execute
```

Each run creates a fresh overlay from the same sealed golden image and has an
independent Docker daemon and BuildKit cache. It never registers a GitHub
runner.

## Work-class queue metrics

`scripts/ops/ci_runner_capacity_metrics.py` collects exact Actions job attempts
and reports queue (`created_at` to `started_at`), execution (`started_at` to
`completed_at`), failure count, and total runner-active seconds by work class,
runner class, and image CPU architecture. Legacy `(multi-arch)` jobs are reported
as `emulated_multi_arch`; native jobs must be named `(linux/amd64)` or
`(linux/arm64)`. `--require-native-architectures` fails unless both native
architectures have terminal, failure-free evidence and no emulated job remains.
Planner, aggregator, publisher, and skipped jobs are excluded by an explicit
workflow/job-name contract. Missing timestamps, unsupported workflows, negative
durations, and non-terminal jobs fail closed.

The final pre-isolation attempt for PR #1156 used the shared classless pool:

| Work class | Runs | Jobs | Queue p50 / p95 / max | Execution p50 / p95 / max |
| --- | --- | ---: | --- | --- |
| normal | CI `30950724994` | 9 | 450 / 644 / 645 s | 38 / 330 / 399 s |
| image | images `30950714254` | 11 | 451 / 615 / 616 s | 263 / 391 / 886 s |
| smoke | cluster `30950714243`, staging `30950719444` | 3 | 208 / 208 / 259 s | 308 / 308 / 341 s |

All 23 selected jobs succeeded. This single-head snapshot is the Track 3
before datum, not the controlled A/B acceptance by itself. At readback time the
repository had 11 online, idle `oldlab5-kvm-*` runners, none with a class label,
and only the legacy route variable existed.

With `GITHUB_TOKEN` or `GH_TOKEN` supplied through an existing secret-safe
environment, reproduce the report with:

```bash
uv run --no-sync python scripts/ops/ci_runner_capacity_metrics.py \
  --repository qianyi-sun/loom \
  --run CI:30950724994 \
  --run images:30950714254 \
  --run cluster-smoke:30950714243 \
  --run staging-smoke:30950719444
```

For Track 4 evidence, provide the same exact image workflow attempt and add
`--require-native-architectures`. Preserve the `by_architecture` and
`by_architecture_and_runner` sections so aggregate image time cannot hide an
ARM64 queue or execution regression.

Track 6 keeps retry semantics separate from capacity measurement. Operators
must use the dispatch-only `classified-ci-retry` workflow and attach a
same-repository evidence URL before rerunning a required source workflow.
`scripts/ops/ci_reliability_metrics.py` then joins exact attempts with those
classifications and reports queue time and terminal causes by runner class. A
queue breach can therefore be classified as `capacity_queue` without turning a
deterministic job failure into a flake. The acceptance mode requires at least 30
source runs and one observed governed retry.

The Track 4 hosted before-datum is images run `31050441797`, attempt 1, on head
`97f22611af455eb27b462353c4a1257334dbc7f4`. Its eleven emulated multi-arch jobs
had queue p50/p95/max `22/56/57s`, execution p50/p95/max `528/660/1858s`, and
5,529 runner-active seconds. The architecture acceptance correctly fails that
legacy sample because it contains no independently measurable native AMD64 or
ARM64 job. Post-change acceptance must use an exact-head run with both native
groups, zero failures, manifest verification, and separately reported queue,
execution, and runner-active data.

There is deliberately no automatic QEMU fallback. If the native ARM runner is
unavailable, the ARM job stays queued or fails and `images-gate` remains
non-successful. On trusted pushes, per-architecture staging tags cannot move the
branch or short-SHA manifest tags: `publish-manifest` runs only after the entire
native publish matrix succeeds, then rejects any platform set other than exactly
`linux/amd64` plus `linux/arm64`. Rollback is a workflow revert followed by an
exact-head rerun; do not weaken the gate, publish a partial manifest, or route a
package-write job to the untrusted oldlab pool.

For the post-activation controlled attempt, add `--require-bounded-wait`. It
exits `3` unless every class has at least one terminal job, zero failures, and
queue p95 no greater than its configured boundary. Preserve the full JSON from
both attempts; aggregate wall time alone is not valid acceptance evidence.

## Build and preflight

Build the candidate-bound QEMU controller image from the exact reviewed
checkout:

```bash
CANDIDATE_SHA=$(git rev-parse HEAD)
docker build \
  --build-arg "LOOM_CANDIDATE_SHA=$CANDIDATE_SHA" \
  --tag loom-ci-runner-qemu:ubuntu-24.04-v1 \
  --file deploy/ci-runners/qemu.Dockerfile \
  .
```

Install the checked-in profile, controller, slice, service, and timer using
root-owned files from the exact merged candidate. The route-controller wrapper
loads only the separately installed candidate modules:

```bash
sudo install -D -m 0755 scripts/ops/ci_runner_pool.py \
  /usr/local/libexec/loom-ci-runner-pool
sudo install -D -m 0755 scripts/ops/ci_runner_route_controller.py \
  /usr/local/libexec/loom-ci-runner-route-controller
sudo install -D -m 0644 src/loom_control_plane/ci_runner_lease_broker.py \
  /usr/local/lib/loom-ci-runner-controller/loom_control_plane/ci_runner_lease_broker.py
sudo install -D -m 0644 src/loom_control_plane/ci_runner_route_controller.py \
  /usr/local/lib/loom-ci-runner-controller/loom_control_plane/ci_runner_route_controller.py
sudo install -D -m 0644 deploy/ci-runners/oldlab5.toml \
  /etc/loom-ci-runner-pool/profile.toml
sudo install -D -m 0644 deploy/ci-runners/loom-ci-runner-pool.service \
  /etc/systemd/system/loom-ci-runner-pool.service
sudo install -D -m 0644 deploy/ci-runners/loom-ci-runner-pool.timer \
  /etc/systemd/system/loom-ci-runner-pool.timer
sudo install -D -m 0644 deploy/ci-runners/loom-ci-runner-pool.slice \
  /etc/systemd/system/loom-ci-runner-pool.slice
```

Do not enable the timer yet. The GitHub administration token must be a
root-owned mode-0600 file at
`/etc/loom-ci-runner-pool/github-token`; it needs repository Actions runner
administration, artifact and contents read, and workflow-dispatch access, but
it is not a CheckRun writer and needs no package, deployment, or
repository-content write permission. Provision one strong opaque HMAC value in
both the root-owned mode-0600
`/etc/loom-ci-runner-pool/route-publisher-hmac` file and the repository Actions
secret `LOOM_CI_ROUTE_PUBLISH_HMAC_KEY`. Transfer it through stdin or another
secret-safe channel; never print it or place it in shell history. The systemd
unit exposes both host files only as service credentials.
`/etc/loom-ci-runner-pool/candidate.env` contains only:

```text
LOOM_CI_RUNNER_CANDIDATE_SHA=<full reviewed commit SHA>
```

Before enabling the timer or creating any route-request artifact, initialize
the artifact high-water cursor once. Reconcile fails closed if this root-owned
cursor is absent, so losing it cannot silently skip a waiting request:

```bash
sudo env PYTHONPATH=/usr/local/lib/loom-ci-runner-controller \
  /usr/local/libexec/loom-ci-runner-route-controller \
  --profile /etc/loom-ci-runner-pool/profile.toml \
  --candidate-sha "$CANDIDATE_SHA" \
  --token-file /etc/loom-ci-runner-pool/github-token \
  --publisher-secret-file /etc/loom-ci-runner-pool/route-publisher-hmac \
  --initialize-cursor
```

Run the secret-free host and image checks before supplying the temporary
Docker Hub credential. Create a root-owned mode-0600
`/etc/loom-ci-runner-pool/dockerhub-credentials.json`
containing `{"username":"...","token":"..."}`. Use a scoped Docker Hub access
token, not an account password. It is used only while sealing the base guest to
populate a candidate-bound allowlisted registry containing the public
Docker/Kind/test images that otherwise cause an anonymous 11-runner pull burst:

```bash
sudo /usr/local/libexec/loom-ci-runner-pool \
  --profile /etc/loom-ci-runner-pool/profile.toml \
  --candidate-sha "$CANDIDATE_SHA" \
  preflight --execute

sudo /usr/local/libexec/loom-ci-runner-pool \
  --profile /etc/loom-ci-runner-pool/profile.toml \
  --candidate-sha "$CANDIDATE_SHA" \
  --dockerhub-credentials-file \
    /etc/loom-ci-runner-pool/dockerhub-credentials.json \
  prepare-base --execute

sudo rm /etc/loom-ci-runner-pool/dockerhub-credentials.json
```

`preflight` requires oldlab-5, `/dev/kvm`, Docker with the systemd cgroup
driver, and a QEMU image whose OCI candidate label matches the requested SHA.
`prepare-base` verifies the pinned Ubuntu cloud image and Actions runner
checksums, installs Docker/Buildx/Compose, a `python` → Python 3 compatibility
command, and the C compiler required by Go race tests in a KVM guest. It copies
the checksum-pinned x86_64 `uv` archive into the sealed guest and serves a
single-version manifest plus that archive on loopback only. Accelerated
workflows select this manifest by the bounded runner-name prefix, avoiding a
synchronized external manifest request while preserving the same version and
archive checksum used on GitHub-hosted runners. It also
authenticates only for the base-image mirror population, logs out, removes
root's Docker configuration, switches the guest registry to read-only, and
verifies the credential is absent before sealing the candidate-bound qcow2
manifest. The registry has no upstream proxy and therefore cannot expose
private Docker Hub repositories or use the account for arbitrary PR-controlled
pulls. Docker and oldlab-only BuildKit builders read the local mirror; the
guest Docker daemon resolves the mirror's amd64 manifest digest and pulls that
single manifest explicitly, so `kind load` never tries to import missing
architectures, while BuildKit retains the mirror's amd64 and arm64 manifests.
The credential ISO and build directory are deleted after sealing. Every
one-job runner starts the Actions process itself with `umask 022`, matching the
permission assumptions of the GitHub-hosted test environment.

## Reconcile, health, and drain

Mutation commands are dry-run unless `--execute` is present. Reconcile creates
one JIT registration and one disposable guest per empty slot:

```bash
sudo /usr/local/libexec/loom-ci-runner-pool \
  --profile /etc/loom-ci-runner-pool/profile.toml \
  --candidate-sha "$CANDIDATE_SHA" \
  --token-file /etc/loom-ci-runner-pool/github-token \
  reconcile --execute

sudo /usr/local/libexec/loom-ci-runner-pool \
  --profile /etc/loom-ci-runner-pool/profile.toml \
  --token-file /etc/loom-ci-runner-pool/github-token \
  status
```

The JIT configuration is stored only long enough for the runner to become
online, then unlinked. A completed guest is deleted along with its writable
overlay and stale GitHub runner record before the slot is replenished.
`status` reports target, ready, and busy slots separately for normal, image,
and smoke work. An online runner missing its reserved class label is not ready;
reconcile replaces it only when it is idle and fails closed if GitHub still
reports it busy.

Rollback routing first. Drain refuses to proceed while
`LOOM_CI_ROUTE_MODE` or any retired static route variable exists and never
removes a runner GitHub reports as busy:

```bash
gh variable delete LOOM_CI_ROUTE_MODE --repo qianyi-sun/loom

sudo /usr/local/libexec/loom-ci-runner-pool \
  --profile /etc/loom-ci-runner-pool/profile.toml \
  --token-file /etc/loom-ci-runner-pool/github-token \
  drain --execute
```

## Activation and rollback

Activation is an explicit live repository operation after the pool
prerequisites pass. Merging the repository contract does not authorize any of
these commands. The controlled migration order is:

1. remove every retired static route, drain all classless runners, install the exact
   merged candidate, and reconcile until `status` reports `5/4/2` ready slots
   with matching labels;
2. initialize the controller cursor, enable the persistent timer, verify one
   clean controller tick, then set the single route-mode variable below;
3. dispatch a non-release acceptance PR that selects normal, image, cluster,
   and staging lanes;
4. collect queue delay, execution time, failures, and runner-active time by
   work class, then compare the same head/input identity with the hosted
   control attempt.

```bash
gh variable set LOOM_CI_ROUTE_MODE \
  --repo qianyi-sun/loom --body 'oldlab-preferred-v1'
```

Immediately dispatch or synchronize a non-release acceptance PR that selects
repository, image, cluster, and staging lanes. Confirm the runner labels,
exact-head protected contexts, job results, queue delay, wall clock, and guest
cleanup before admitting another PR.

The queue boundaries are five minutes for normal tests, fifteen minutes for
image builds, and five minutes for smoke. Every selected job must remain within
its class boundary; a p95 below the threshold cannot hide one maximum breach.
These are evidence thresholds, not permission for an unattended route
mutation. If one class crosses its boundary, delete `LOOM_CI_ROUTE_MODE` before
cancelling and rerunning the affected workflow on the same head. Existing jobs
keep their immutable assignment; only the replacement workflow receives the
disabled-mode hosted map. Required checks remain pending/failing until the
replacement terminal result is published; there is no success fallback.

Full rollback is required when any class is below its target ready capacity,
labels or candidate identity drift, guest cleanup fails, the controlled A/B
regresses its class-specific queue or failure boundary, or the host exceeds its
CPU/memory budget. Delete the mode and every retired static route before drain,
then rerun affected workflows on the same head:

```bash
gh variable delete LOOM_CI_ROUTE_MODE --repo qianyi-sun/loom
gh variable delete LOOM_CI_NORMAL_RUNS_ON --repo qianyi-sun/loom
gh variable delete LOOM_CI_IMAGE_RUNS_ON --repo qianyi-sun/loom
gh variable delete LOOM_CI_SMOKE_RUNS_ON --repo qianyi-sun/loom
gh variable delete LOOM_CI_ACCELERATOR_RUNS_ON --repo qianyi-sun/loom
```

Because disabled mode returns the class's GitHub-hosted label, the replacement
run returns to hosted placement without a code change. The pool refuses to
drain while the mode or any retired routing variable still exists. Protected
checks remain fail-closed throughout.
