# Multi-node staging on k3s

Shared staging runs in namespace `loom-staging` on the OLDLAB k3s cluster.
Its version-controlled render input is
`deploy/environments/staging.multinode.cluster.toml`: CloudNativePG with three
Postgres replicas, distributed MinIO with four Longhorn-backed replicas,
required hostname anti-affinity, and a disruption budget with
`minAvailable=3`.

The installed [protected rollout authority](../architecture/staging-rollout.md)
is the normal operator interface. The host-local
`scripts/ops/deploy_staging_k3s.sh` is an implementation and authorized repair
helper; access to the host or script does not bypass the shared-staging
invariant in the [operator runbook](operator-runbook.md#shared-staging).

## Host prerequisites

The k3s control-plane host requires:

- `/etc/rancher/k3s/k3s.yaml` with authority for `loom-staging`;
- the checked-in registry rewrite and reachable on-host image registry;
- Docker for building the six Loom application images;
- namespace Secrets `loom-secrets` and
  `loom-postgres-cnpg-credentials` with environment-scoped values;
- Longhorn, CloudNativePG, ingress-nginx, cert-manager, and the required
  NetworkPolicy enforcement;
- the public entry/TLS units installed by
  `scripts/ops/bootstrap_staging_k3s_entry_tls.sh`.

The entry/TLS bootstrap is idempotent. It applies the dedicated
`ingress-nginx/loom-staging-public-entry` NodePort Service and starts supervised
host proxies on `18080/18443`. Each proxy connection enters the fixed
`32080/32443` NodePort path, letting kube-proxy select any ready ingress
endpoint without a host reference to kube-proxy's generated chains. A cutover
unit verifies the Kubernetes routes and both end-to-end proxy paths, installs
destination-scoped public DNAT, and removes obsolete predecessor route rules. The
bootstrap waits up to 60 seconds for this reconciliation before continuing. It
also configures
cert-manager for the host's working ACME egress path, applies the issuer and
`loom-staging-tls` Certificate, and installs the private fleet forward units.
Re-run it after a cert-manager reinstall because the `hostNetwork` setting is
an imperative patch.

Never put secret values in the cluster profile. The optional QA relay reads its
API key from the Kubernetes Secret key declared by
`gateway_local_providers`; when that key is absent only its `local/<name>/*`
routes are unavailable.

## Authorized host deployment helper

Run only from a clean checkout containing the admitted 40-character candidate
SHA and with the protected rollout or explicit repair authority:

```bash
export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
scripts/ops/deploy_staging_k3s.sh CANDIDATE_SHA
```

`--skip-build` is valid only when all six candidate-tagged images are already
present in the configured registry. The helper:

1. syncs the locked cluster runtime;
2. verifies the namespace and required Secrets;
3. builds and pushes the six application images as
   `staging-<candidate-prefix>` unless skipped;
4. renders `staging.multinode.cluster.toml` with that image tag;
5. runs and waits for the Alembic migration Job before rolling application
   workloads;
6. applies the rendered resources and reports cluster status;
7. rotates the deployment-managed `loom-smoke` user token and the service's
   batch-runner Control Plane token when the service is reachable; and
8. initializes the data-lifecycle mutation epoch when the database is eligible.

The final credential and lifecycle steps warn instead of failing the workload
rollout. Treat any warning as an incomplete validation state and repair it
before running release smoke.

## Verify

Check the candidate identity, workload readiness, stateful quorum, certificate,
and public route:

```bash
uv run --no-sync loom cluster audit \
  --config deploy/environments/staging.multinode.cluster.toml
uv run --no-sync loom cluster status \
  --config deploy/environments/staging.multinode.cluster.toml
kubectl -n loom-staging get pods,pvc,certificate
curl -fsS https://yylx.world/staging/api/v1/health
```

Expect the service health response to report `ok`, CloudNativePG to have three
healthy instances, MinIO to have four healthy replicas, and the SPA to load
assets under `/staging/`. Then run the complete
[staging validation](staging-launch.md).

For the QA relay, submit through a model configured as
`source="local-server"` and `local_server="<name>"`. The Gateway serializes it
as `local/<name>/<model>` and retains usage attribution without exposing the
upstream key to the worker or sandbox.

## Rollback and recovery

Redeploy a previously admitted candidate image identity through the protected
rollout path. Database migrations are forward-only, so an incompatible schema
requires the recorded database/object-store restore procedure rather than only
rolling image tags back.

Do not disable the public-entry proxies or delete their Service unless a
separately verified replacement route is ready. Restore that route before
removing the proxy path; the retired local cluster is not a rollback target.
Preserve candidate, manifest, migration, and stateful-backend evidence before
repairing a failed deployment.
