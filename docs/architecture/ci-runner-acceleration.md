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
   Docker/Buildx/Kind capacity, and an aggregate budget no larger than 22 vCPU
   and 80 GiB for the initial acceptance window.
6. Health readback proves enough idle one-job runners for the selected matrix,
   and teardown proves that runner registrations, guests, disks, containers,
   volumes, writable caches, and QEMU/binfmt handlers do not persist.

PR image builds remain read-only and cache-write-free. The `publish` image job
continues to run on GitHub-hosted infrastructure with its existing trusted
branch and package-write checks.

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
