# CI runner acceleration

Issue [#1058](https://github.com/qianyi-sun/loom/issues/1058) owns the
oldlab-5 CI migration. The objective is to reduce Loom's overall CI critical
path and aggregate runner time without weakening the existing protected-check,
candidate, or publishing boundaries.

## Routing contract

Compute-heavy jobs read the repository configuration variable
`LOOM_CI_ACCELERATOR_RUNS_ON` as a JSON array accepted by GitHub Actions
`runs-on`. When the variable is absent or empty, every migrated job defaults to
`["ubuntu-latest"]`.

The accepted oldlab-5 value is:

```json
["self-hosted","linux","x64","loom-ci","oldlab-5","ephemeral-kvm"]
```

The first migration slice covers:

- lint/static, root test shards, package tests, runtime payload, Go, and web;
- fast and Docker integration;
- untrusted multi-architecture image builds;
- cluster smoke, staging smoke, system smoke, and deploy spikes.

Planning, aggregation, protected gate publication, and trusted image publishing
remain on GitHub-hosted runners. The Linux ARM locked-environment check also
remains GitHub-hosted. Small setup-dominated jobs can remain hosted when moving
them would consume accelerator capacity without reducing the workflow critical
path.

Changing the variable changes only placement. It must never change the job
steps, selected test paths, permissions, timeout, protected context, image
candidate identity, or result aggregation.

## Activation prerequisites

Do not set `LOOM_CI_ACCELERATOR_RUNS_ON` until all of these are true:

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
root-owned files. Do not enable the timer yet. The GitHub administration token
must be a root-owned mode-0600 file at
`/etc/loom-ci-runner-pool/github-token`; it must have repository Actions runner
administration access and no workflow/package/deployment credentials.
`/etc/loom-ci-runner-pool/candidate.env` contains only:

```text
LOOM_CI_RUNNER_CANDIDATE_SHA=<full reviewed commit SHA>
```

Run the secret-free checks before supplying either credential. Create a
root-owned mode-0600 `/etc/loom-ci-runner-pool/dockerhub-credentials.json`
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
command, and the C compiler required by Go race tests in a KVM guest. It also
authenticates only for the base-image mirror population, logs out, removes
root's Docker configuration, switches the guest registry to read-only, and
verifies the credential is absent before sealing the candidate-bound qcow2
manifest. The registry has no upstream proxy and therefore cannot expose
private Docker Hub repositories or use the account for arbitrary PR-controlled
pulls. Docker and oldlab-only BuildKit builders read the local mirror; the
guest Docker daemon receives explicit amd64-only imports so `kind load` never
tries to import missing architectures, while BuildKit retains the mirror's
amd64 and arm64 manifests. The credential ISO and build directory are deleted
after sealing. Every one-job runner starts the Actions process itself with
`umask 022`, matching the permission assumptions of the GitHub-hosted test
environment.

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

Rollback routing first. Drain refuses to proceed while
`LOOM_CI_ACCELERATOR_RUNS_ON` exists and never removes a runner GitHub reports
as busy:

```bash
gh variable delete LOOM_CI_ACCELERATOR_RUNS_ON --repo qianyi-sun/loom

sudo /usr/local/libexec/loom-ci-runner-pool \
  --profile /etc/loom-ci-runner-pool/profile.toml \
  --token-file /etc/loom-ci-runner-pool/github-token \
  drain --execute
```

## Activation and rollback

Activation is an explicit repository operation after the pool prerequisites
pass:

```bash
gh variable set LOOM_CI_ACCELERATOR_RUNS_ON \
  --repo qianyi-sun/loom \
  --body '["self-hosted","linux","x64","loom-ci","oldlab-5","ephemeral-kvm"]'
```

Immediately dispatch or synchronize a non-release acceptance PR that selects
repository, image, cluster, and staging lanes. Confirm the runner labels,
exact-head protected contexts, job results, queue delay, wall clock, and guest
cleanup before admitting another PR.

Rollback is a repository-variable removal followed by rerunning the affected
workflow on the same head:

```bash
gh variable delete LOOM_CI_ACCELERATOR_RUNS_ON --repo qianyi-sun/loom
```

Because the workflow default is `["ubuntu-latest"]`, the replacement run
returns to GitHub-hosted placement without a code change. Removing the variable
does not move an already queued job; cancel and rerun the affected workflow
after rollback. Protected checks remain fail-closed throughout.
