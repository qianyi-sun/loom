# Shared development fleet

This directory contains the operator-owned substrate for `loom dev`. It is
separate from `loom service`, which remains the local Docker Compose workflow.

## Architecture

- `shared-fixture.yaml` runs one development Postgres server and one MinIO
  server. Every environment gets a derived database role/database and three
  buckets. Database `PUBLIC` connectivity is revoked, and the fixture admin
  sidecar creates a dedicated MinIO user/policy that can name only those three
  buckets. Shared root credentials never enter an instance namespace.
- The management `loom-service` owns the durable `dev_instances` registry and
  guarded lifecycle API. It renders only control-plane, gateway, service,
  migration, Service, and Ingress objects into `loom-dev-<name>`. Requests
  claim an operation and return `202`; an independently-sessioned runner
  executes/resumes the fenced lifecycle.
- One submit-host timer reads the complete registry/demand cohort, allocates
  the global budget transactionally, and calls the existing Slurm actuator for
  every environment. Individual environments cannot raise their own grant.

## Activation

Activation is deliberately separate from application rollout and requires an
operator change window:

1. Create `loom-dev-fixture` in `loom-dev-shared` from an owner-only secret
   source with keys `postgres-user`, `postgres-password`, `minio-access-key`,
   and `minio-secret-key`. Do not place values in argv or commit them.
2. Apply `shared-fixture.yaml`, verify both readiness probes, wildcard DNS/TLS
   for `*.dev.yylx.world`, and the external MinIO endpoint.
3. Give only the management service account namespace/Secret/Deployment/
   Service/Ingress/Job lifecycle RBAC for namespaces labeled as Loom dev
   instances, plus `pods/exec` to the `admin` container of
   `loom-dev-minio-0` in `loom-dev-shared`. Configure the
   `LOOM_SVC_DEV_INSTANCE_*` fields documented by `config/loom-schema.toml`;
   enabling with a partial contract fails startup. The Slurm `env_file`
   template must render `/var/lib/loom-dev-workers/{environment}.env`.
4. Install the service/timer under a dedicated `loom-dev-autoscaler` account.
   Its two database URL files and `/var/lib/loom-dev-workers` must be mode
   `0600`/`0700`, owned by that account, and must not be symlinks. The grant
   report directory is likewise owner-only. Its kubeconfig and Slurm command
   access are read-only except for the exact namespaces/jobs it owns.
5. Run the oneshot manually, inspect the owner-only grant report and Slurm
   dry-run evidence, then enable the timer:

   ```text
   systemctl start loom-global-dev-fleet-autoscaler.service
   systemctl enable --now loom-global-dev-fleet-autoscaler.timer
   ```

If the timer or a complete snapshot fails, no new grant is issued. Existing
leases expire and local reconcilers converge toward zero; pending jobs are
cancelled, active workers are fenced to drain, and their slots remain committed
until terminal observation. Never delete or edit the SQLite ledger by hand.

The repository does not apply this manifest, install these units, create DNS,
or enable non-zero capacity automatically. Those mutations remain behind the
operations gate in #906.
