# Staging → dev-tip cutover runbook

Status: ready-to-execute, pending the two live confirmations flagged in
[Preconditions](#preconditions). Flips the live staging environment from the
sealed candidate `e833dd3` (serving `/dev`) to the tip of `dev` (serving
`/staging`), in one coordinated maintenance window.

This is a high-blast-radius operation — it moves the public `yylx.world` route,
migrates the live staging database, and renames the worker pool across 14 GB10
hosts. Run it as a supervised window with the abort path in hand, not
unattended.

## Why a single window

Three changes are mutually coupled and must land together:

- **Route.** dev tip serves `/staging`; the live env serves `/dev`. The web
  image is prefix-stable but the *deployed* `e833dd3` image only accepts
  `/dev` — so the route only moves when dev-tip's image deploys.
- **DB schema.** dev tip's migration head is `0074`; live staging is stamped
  `0069` on a pre-#857 lineage (see the divergence in #949). The reconcile
  migration `0073` (#951) plus a one-time stamp bring them together.
- **Pool identity.** dev tip renames the worker pool `gb10-arm64` → `gb10`
  (#883, via `0073`). The deployed `e833dd3` config still uses `gb10-arm64`, so
  renaming the DB rows before dev-tip deploys would break live scheduling.

## Preconditions

- All cutover code fixes merged to `dev`: #937 (route-transition), #941 (marker),
  #947 (baseline binding), #951 (`0073` reconcile), #953 (capacity threshold).
- Runner healthy on sealed `e833dd3`: `loom-staging-rollout check` → `ok: true`,
  `status` → `idle`.
- Sealed re-install pins on hand (the abort anchor):
  `--sealed-source-sha e833dd3d472ba147b35577186518a85a216ced9e`
  `--sealed-source-tree cd2869342b310335086a060b6a18465fd6b306eb`
  `--sealed-approved-base-sha eed7ff5eb438cb1d9a715a8afa49da94e9fee5eb`
- Smoke team id: `9b1de3bf-9655-489a-813f-e8a7adf81290`.

**Two things to CONFIRM before the window** (I could not fully verify these from
outside the window):

1. **Does the rollout's migration step stamp `0069`→`0072` before `upgrade
   head`?** The live DB already holds the `0069`–`0072` *content* under old
   numbers, so a plain `alembic upgrade head` from `0069` would try to re-run
   `0070` (capacity) and conflict on the existing table. The migration must
   `stamp 0072` first (accept the content it has), then run `0073`. If the
   driver does not do this, step 4 includes the manual stamp.
2. **How the dev candidate is materialized on the 14 GB10 shared checkouts**
   (`gb10.candidate-source`): via the `loom-staging-rollout-gb10-trust` broker
   command, the driver's GB10 prep step, or the shared-repo export authority
   (#890/#891). Confirm which, and that the operator has the access.

## Steps

### 1. Freeze + snapshot

- Announce a staging Do-Not-Submit window; confirm no critical in-flight batches.
- Snapshot the staging Postgres as the DB rollback anchor:
  ```bash
  kubectl -n loom-staging exec loom-postgres-0 -c postgres -- \
    pg_dump -U loom -Fc loom > /data/loom-staging/backups/pre-cutover-$(date -u +%Y%m%dT%H%M%SZ).dump
  ```
- Back up the runner state (config + credentials):
  ```bash
  sudo cp -a /etc/loom /home/hongjian/loom-runner-backup-precutover-etc
  ```

### 2. Un-seal the runner to merged-dev

```bash
DEV_SHA=$(git -C /home/hongjian/loom rev-parse origin/dev)
sudo git -C /opt/loom-staging-runner/source fetch -q origin dev
sudo git -C /opt/loom-staging-runner/source checkout -q "$DEV_SHA"
sudo /usr/bin/python3 /opt/loom-staging-runner/source/scripts/ops/staging_rollout_host.py install \
  --source-mode merged-dev \
  --smoke-on-behalf-team-id 9b1de3bf-9655-489a-813f-e8a7adf81290
```
Verify: install `ok: true`; the dev candidate is materialized + published.

**Abort:** re-point source to `e833dd3` and run the sealed re-install (see
[Abort](#abort--rollback)). Do not leave a half-applied merged-dev install.

### 3. Provision the operational preconditions

Run `loom-staging-rollout preflight` and clear each remaining blocker:

- **`object-store-readiness-failed`** — refresh the readonly MinIO credential and
  confirm the MinIO pod is healthy:
  ```bash
  sudo systemctl start loom-rollout-credential-refresh.service
  kubectl -n loom-staging get pod loom-minio-0
  ```
- **`gb10.candidate-source.drift` (×14)** — materialize the dev candidate on the
  GB10 shared checkouts (mechanism per precondition #2).
- **`external-supervisor.predecessor.drift`** — provision/restore the #907
  supervisor authority for the dev candidate.
- **`backup.lease.ineligible` / `backup.rotation-capacity.blocked`** — ensure a
  fresh backup lease is admissible (the `start` in step 4 creates the checkpoint;
  clear any stale incomplete-backup request first with
  `loom-staging-rollout cleanup-incomplete-backup <request-id>`).

The `migration.manifest` / `systemd.render` / `production-defaults` /
`artifacts.publish` blockers are **build-once artifacts** the driver produces in
step 4 — they are expected "unavailable" in a requestless preflight and are not
separate work.

Re-run `preflight` until only the build-once artifacts remain unavailable.

### 4. Cut over (point of no return)

If precondition #1 is unconfirmed, stamp the live DB first (accepts the
lifecycle content it already has; `0073` then adds the inserted-migration
content):
```bash
# from a checkout at dev tip, LOOM_DB_URL pointed at the live staging DB
alembic -c migrations/alembic.ini stamp 0072
```

Then run the rollout, which performs — atomically — backup → build images → kind
load → GB10 prep → **migrate (`upgrade head` → `0074`: includes the `0073`
benchmark-profile, `prod_pressure_state`, and `gb10-arm64`→`gb10` reconciliation,
then persists Slurm containment identity and exact resource requests)** →
deploy dev-tip (web/service/control-plane at `/staging`) → `environment-state
apply` (reinstalls timers with `--pool-name gb10`) → final gate:
```bash
loom-staging-rollout start
```

### 5. Verify

```bash
curl -sk -o /dev/null -w "/staging -> %{http_code}\n" https://yylx.world/staging/api/v1/health   # expect 200
curl -sk -o /dev/null -w "/dev -> %{http_code}\n" https://yylx.world/dev/                          # expect 308/404
kubectl -n loom-staging exec loom-postgres-0 -c postgres -- \
  psql -U loom -d loom -tAc "select version_num from alembic_version;"                             # expect 0074
kubectl -n loom-staging exec loom-postgres-0 -c postgres -- \
  psql -U loom -d loom -tAc "select distinct pool_name from worker_pool_autoscaler_policies;"      # expect gb10
```
Confirm: a frontend asset loads under `/staging/assets/*`; the GB10 autoscaler
timers query `gb10`; workers schedule.

### 6. Close out

- Drop `frontend_route_path_from = "/dev"` from
  `deploy/environments/staging.cluster.toml` in a follow-up PR once `/staging`
  is confirmed live (the marker is only for the in-flight transition).
- Close #883 (live rename validated), #949 (lineage reconciled); advance #879.
- Remove the sealed-cumulative pin from the runner narrative; merged-dev is now
  the steady state.

## Abort / rollback

- **Before step 4 (`start`):** re-point source to `e833dd3` and sealed re-install:
  ```bash
  sudo git -C /opt/loom-staging-runner/source checkout -q e833dd3d472ba147b35577186518a85a216ced9e
  sudo /usr/bin/python3 /opt/loom-staging-runner/source/scripts/ops/staging_rollout_host.py install \
    --source-mode sealed-cumulative \
    --sealed-source-sha e833dd3d472ba147b35577186518a85a216ced9e \
    --sealed-source-tree cd2869342b310335086a060b6a18465fd6b306eb \
    --sealed-approved-base-sha eed7ff5eb438cb1d9a715a8afa49da94e9fee5eb \
    --smoke-on-behalf-team-id 9b1de3bf-9655-489a-813f-e8a7adf81290
  ```
  If a bare DB `stamp 0072` was applied without deploying, re-stamp `0069`.
- **After step 4:** restore the pre-cutover Postgres dump (step 1) and redeploy
  `e833dd3` via the sealed rollout. The route reverts with the redeploy.

## Notes

- Never `kubectl`-mutate live staging resources by hand — it leaves
  field-ownership drift the rollout's `manifests.field-ownership` check rejects
  (learned the hard way; converge only through the protected manager).
- On bb8-1, run rollout tooling under `umask 0022` — group-writable files trip
  the `credential_authority` protected-file check.
