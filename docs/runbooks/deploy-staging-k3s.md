# Deploy staging to the multi-node OLDLAB k3s cluster

The sanctioned, repeatable way to deploy the `loom-staging` stack to the
five-node OLDLAB **k3s** cluster. This is the interim deploy engine until the
#1097 reconciler takes over; it replaces ad-hoc `kubectl apply` with one
reproducible path so the environment does not depend on operator memory.

- Desired state: [`deploy/environments/staging.multinode.cluster.toml`](../../deploy/environments/staging.multinode.cluster.toml)
- Deploy script: [`scripts/ops/deploy_staging_k3s.sh`](../../scripts/ops/deploy_staging_k3s.sh)

## Topology

`loom-staging` on k3s runs the multi-node distributed backends: **4-pod
distributed MinIO** (Longhorn PVCs, required `kubernetes.io/hostname`
anti-affinity, PDB `minAvailable=3`) across four distinct OLDLAB nodes, **CNPG
3-replica Postgres**, and the app Deployments (control-plane, service, web,
llm-gateway, family-orchestrator, egress-xds) plus the gateway-router DaemonSet.

## Prerequisites

1. **k3s kubeconfig** — `export KUBECONFIG=/etc/rancher/k3s/k3s.yaml` (cluster-admin, on the control-plane host bb8-1). The default `~/.kube/config` only has the kind contexts.
2. **On-host registry** — the `loom-registry` container serves `:5000`. k3s rewrites `192.168.50.13:5000` → `http://192.168.50.103:5000` via `/etc/rancher/k3s/registries.yaml`; images are pushed over `localhost:5000` (docker's insecure `127.0.0.0/8`).
3. **Docker with host networking** — image builds use `--network=host` because the docker bridge network is MITM'd with a self-signed cert (pip/npm fail TLS verify); the host egress has valid certs.
4. **Secrets bootstrapped** (one-time, not created by the render): namespace `loom-staging` must hold `loom-secrets` (`cp-db-url`, `svc-db-url`, `gw-db-url`, `minio-access-key`, `minio-secret-key`, provider API keys, …) and the CNPG `loom-postgres-cnpg-credentials`. DB URLs use the namespace-relative host `loom-postgres:5432` so they stay coherent. The deploy script fails closed if these are absent.
5. **TLS secret** — `loom-staging/loom-staging-tls` (the real `yylx.world` Let's Encrypt cert) must exist for the ingress; a static copy is kept fresh by `/usr/local/sbin/loom-staging-tls-sync.sh` (see #1114 for native issuance).

## Deploy

```bash
export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
# from a clean checkout at the target sha:
scripts/ops/deploy_staging_k3s.sh <git-sha>
```

The script:
1. Builds + pushes the 6 app images at `staging-<shortsha>` (skip with `--skip-build` if already pushed).
2. Renders the desired state from the committed config with the tag substituted.
3. Renders + applies the DB migration Job (`loom-control-plane:<tag> alembic upgrade head`, `LOOM_DB_URL` from `loom-secrets/cp-db-url`) and **waits for it to complete before** rolling the app, so pods never boot against an old schema.
4. Applies the workloads (rolling update to the new tag) and prints `loom cluster status`.
5. Provisions the **headless smoke-user credential** (see below) and stores it in `loom-secrets/smoke-api-token`.
6. Provisions the **batch-runner CP token** (`loom admin ensure-batch-runner-token`), stores it in `loom-secrets/batch-runner-cp-token`, and restarts loom-service — without it, batch fan-out to the control-plane `POST /trials` 401s and batches never dispatch (single-trial API submits are unaffected).
7. Bootstraps the **data-lifecycle mutation-epoch** on a fresh DB (idempotent; no-op on an already-bootstrapped or already-dirty DB — see #1137) so retention/GC can run.

## Headless smoke-user credential

The release-gate / operator trajectory smoke submits `oracle × gb10-smoke` via
`POST /api/v1/trials`, which requires a **user-owned** API token — a bare admin
or legacy team token is rejected (`require_submitting_user`). Step 5 provisions
one in a deployment-managed way (no human login, no personal token):

```bash
# runs INSIDE loom-service — the host can't reach the CNPG DB directly
# (NetworkPolicy), but the service pod has the loom CLI + LOOM_SVC_DB_URL.
loom admin ensure-smoke-user --format json   # LOOM_DB_URL=$LOOM_SVC_DB_URL
```

This idempotently ensures a dedicated non-human `loom-smoke` User + Team + owner
membership and mints a fresh user-owned `submit` token (rotating any prior one),
which the script writes into `loom-secrets/smoke-api-token`. Point the smoke at
it with `smoke_api_token_source` (or `LOOM_SMOKE_API_TOKEN` from that secret
key). The identity is stable across deploys; only the token rotates.

## External entrypoint (one-time)

Public `https://yylx.world/staging/` reaches k3s through an iptables DNAT that
redirects the host `:443` to the k3s ingress-nginx (`:8443`), persisted across
reboots:

```bash
# installed once; idempotent re-insert on @reboot via root crontab
/usr/local/sbin/loom-staging-k3s-cutover.sh
# = iptables -t nat -I PREROUTING 1 -p tcp --dport 443 -j DNAT --to-destination 192.168.50.103:8443
```

The legacy single-node **kind** cluster remains bound to the host-local `:443`
underneath this rule as the rollback anchor.

## Verify

```bash
# the real external path (bb8-1's own /etc/hosts sends yylx.world -> 127.0.0.1,
# which bypasses the PREROUTING DNAT and hits the kind anchor — so resolve to
# the k3s front explicitly):
curl -sSk --resolve yylx.world:8443:192.168.50.103 https://yylx.world:8443/staging/
curl -sSk --resolve yylx.world:8443:192.168.50.103 https://yylx.world:8443/staging/api/v1/health
kubectl -n loom-staging get pods
```

Expect the SPA (`/staging/assets/index-*.js`), `/api/v1/health` = `{"status":"ok"}`, MinIO 4/4, CNPG 3/3.

## Rollback

- **Fast (entrypoint):** remove the DNAT so `:443` falls back to the kind anchor:
  `sudo iptables -t nat -D PREROUTING -p tcp --dport 443 -j DNAT --to-destination 192.168.50.103:8443` (and remove the `@reboot` cron entry).
- **Redeploy a prior sha:** re-run the script with the previous `<git-sha>` (workloads roll back; DB migrations are forward-only — a schema rollback is a separate restore).
