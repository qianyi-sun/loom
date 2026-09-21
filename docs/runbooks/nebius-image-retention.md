# Nebius image retention

The `nebius-image-retention` workflow runs daily at 07:23 UTC on a GitHub-hosted
runner. It is independent of PR admission and publication. The initial scheduled
mode is **preview**: it reads the registry, Kubernetes and database and saves a
decision report without acquiring a maintenance guard, retiring rows or deleting
images. No new CI gate, controller or runner is required.

## Policy

- Platform images with only Loom `candidate-<commit>` tags are retained for at
  least 30 days after creation or the last update. Keep at least three newest
  candidate images per component in addition to referenced images.
- Protect current Kubernetes workloads, retained ReplicaSets/rollback templates,
  the live runtime profile, catalog image references, registered agent runtimes,
  and frozen historical Batch runtime profiles. Registered Harbor versions are
  not automatically deregistered by registry maintenance.
- Native task images use the existing materialization retirement lease. Current
  catalog revisions and nonterminal Trial references prevent retirement. The
  first apply pass records an unreferenced observation; at least 30 days must
  elapse before a later pass may claim it. Old image creation time alone never
  establishes this grace period.
- Retiring a task cache preserves task sources, Trial history, trajectories,
  rewards and artifacts. New demand during retirement waits for/requeues image
  preparation. Shared digests remain while another materialization or catalog
  reference owns them. This is cache retention, not a promise to preserve every
  historical container indefinitely.
- Unrecognized tags, repositories, untracked task images and non-manifest
  artifacts are reported and retained for separate ownership review. No registry,
  bucket, repository prefix, node cache or task input is bulk-deleted.

Image sizes include shared layers and must not be summed as reclaimed/billed
storage. A successful pass confirms deleted artifact IDs no longer appear in the
provider inventory; settled storage billing is a separate provider observation.

## Preview and activation

```sh
gh workflow run nebius-image-retention.yml --ref dev -f apply=false
```

Download `nebius-image-retention-<run>-<attempt>` and review the per-image reason
and task retirement candidates. Then deploy the version containing the scoped
materialization claim parameter before the first apply pass:

```sh
gh workflow run nebius-image-retention.yml --ref dev -f apply=true
```

Only after the preview and a normal bounded apply pass have been verified, set
the `nebius-integration` environment variable `NEBIUS_IMAGE_RETENTION_APPLY=true`
to enable daily deletion. Set it to `false` to return schedules to preview.
Manual dispatch defaults to preview regardless of the scheduled setting.

The workflow reuses the existing deployment SSH credentials and native registry
service-account credential. The registry identity must allow artifact list and
delete in the selected registry; no broader cloud permissions are needed. A
permission/readback failure stops maintenance and is never interpreted as an
empty inventory or permission to acknowledge a task's deletion.

Local operators can use their existing Nebius CLI authentication:

```sh
uv run --extra cluster python scripts/ops/nebius_image_retention.py \
  --kubeconfig "$KUBECONFIG" --expected-cluster-id "$LOOM_CLUSTER_ID" \
  --registry-prefix "$LOOM_REGISTRY_PREFIX" --output image-retention.json
```

Add `--apply` only for execution. Defaults are `--days 30 --keep 3 --max-delete 20`.
The existing `LOOM_DEPLOY_SSH_*` transport supports remote kubeconfig paths.

## Execution and recovery

Apply acquires the existing database idle rollout guard, then refreshes the
inventory and protection set. Active execution, native builds, cleanup or another
rollout cause an immediate skip. Maintenance has its own GitHub concurrency group
so it does not cancel a pending rollout. It performs at most 20 artifact deletions
per pass, validates current tags before each deletion and records completed work.
If the limit is reached partway through a claimed task image set, the report is
`bounded`; its retiring lease remains for the next pass rather than acknowledging
unfinished deletion or treating the normal limit as a provider failure.

Task images are claimed before deletion and acknowledged only after the provider
operations succeed. Failed/partial deletions retain the existing retiring lease;
after lease expiry, a later pass can resume using a fresh inventory. No cache is
marked ready while deletion is in flight. Ordinary failures release only this
maintenance run's idle guard. As with rollout, a killed runner may leave its guard
paused: inspect the report and registry operations before using the existing
owner-specific rollout-guard recovery. Never clear another run's guard.

This implements the image-retention portion of #1912; it does not close the
broader backup, restore, object-storage or historical migration acceptance.
