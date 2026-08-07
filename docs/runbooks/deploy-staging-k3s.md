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
4. **Secrets bootstrapped** (one-time, not created by the render): namespace `loom-staging` must hold `loom-secrets` (`cp-db-url`, `svc-db-url`, `gw-db-url`, `minio-access-key`, `minio-secret-key`, provider API keys, …) and the CNPG `loom-postgres-cnpg-credentials`. DB URLs use the namespace-relative host `loom-postgres:5432` so they stay coherent. The deploy script fails closed if these are absent. **Optional — QA relay for real LLM evals:** to let agents run genuine evals through the operator relay (see [Local-provider relay](#local-provider-relay-real-llm-evals)), also put the relay API key in `loom-secrets` under the key named by `gateway_local_providers` (`qa-relay-api-key` for the `yibu` entry): `kubectl -n loom-staging patch secret loom-secrets --type merge -p "{\"data\":{\"qa-relay-api-key\":\"$(printf %s "$KEY" | base64 -w0)\"}}"`. The env is rendered with `optional: true`, so a missing key doesn't crash the gateway — only `local/yibu/*` routes 400 until it's set.
5. **Public entry + TLS** (one-time, idempotent — `scripts/ops/bootstrap_staging_k3s_entry_tls.sh`): installs the entry cutover (host `:443/:80` → k3s ingress `:8443/:8080`, persisted via `loom-staging-k3s-cutover.service`) and native Let's Encrypt issuance. cert-manager self-issues + auto-renews `loom-staging/loom-staging-tls` (#1114) — no kind dependency. It runs the controller on `hostNetwork` (the CNI pod net MITMs outbound HTTPS; only the host has clean ACME egress) and applies `deploy/staging-k3s/tls-acme.yaml`. It also installs the **GB10 fleet forwards** (`loom-k3s-fleet-fwd@.service` instances) that route the ports the nodes tunnel to — bb8-1 `:18081`/`:18082`→`:30080` (worker-router), `:19000`→`:30900` (minio-router), `:19100`→`:30443` (gateway-router) — so the fleet reaches k3s instead of kind (INTERIM until the node-agent dials the routers directly, #906). **NOTE:** the cert-manager `hostNetwork` patch is applied imperatively; re-run this script after any cert-manager reinstall/upgrade. Rollback to the kind entry: delete the two `PREROUTING` DNAT rules + `systemctl disable loom-staging-k3s-cutover.service`.

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

## Local-provider relay (real LLM evals)

Staging carries an operator QA relay so real LLM agents (`litellm`,
`mini-swe-agent`) can run genuine evals — not just the no-LLM `oracle` smoke.
`gateway_local_providers` in the staging config declares OpenAI-compatible
upstreams the gateway exposes as `local/<name>/<model>` routes:

```toml
# deploy/environments/staging.multinode.cluster.toml
gateway_local_providers = ["yibu|https://yibuapi.com/v1|qa-relay-api-key"]
```

Each `<name>|<base_url>|<secret_key>` entry renders `LOOM_GW_LOCAL_<NAME>_BASE_URL`
(literal) + `LOOM_GW_LOCAL_<NAME>_API_KEY` (from `loom-secrets/<secret_key>`,
`optional: true`) onto the gateway Deployment. Empty on production, so prod stays
relay-free. The gateway forwards `local/yibu/<model>` to the upstream as the
OpenAI dialect (`loom_llm_gateway/routes/chat.py`, `local` provider path); no
per-team BYO provider connection is involved.

Submit a real eval (routes to `local/yibu/gpt-4o-mini`):

```bash
# from inside loom-service (has the smoke token in loom-secrets/smoke-api-token):
curl -sS -X POST http://localhost:8090/api/v1/trials \
  -H "Authorization: Bearer $SMOKE_TOKEN" -H 'Content-Type: application/json' \
  -d '{"task_id":"loom-smoke/gb10-oracle-hello-world",
       "config":{"agent_name":"litellm",
         "agent_model":{"provider":"openai","name":"gpt-4o-mini",
                        "source":"local-server","local_server":"yibu"}}}'
```

`agent_model.source="local-server"` + `local_server="yibu"` is what serializes to
the gateway `model="local/yibu/gpt-4o-mini"` (`ModelSpec.to_gateway_model_string`).
A worker claims it, the agent calls the gateway, and an `llm_calls` row lands
(`model=yibu/gpt-4o-mini`, real token counts) plus a trajectory in MinIO.

## External entrypoint (one-time)

Public `https://yylx.world/staging/` reaches k3s through an iptables DNAT that
redirects only the host entry address on `:443` to the k3s ingress-nginx
(`:8443`), persisted across reboots. The destination match is intentional: a
port-only PREROUTING rule also hijacks Docker/container HTTPS egress.

```bash
# installed once; idempotent re-insert on @reboot via root crontab
/usr/local/sbin/loom-staging-k3s-cutover.sh
# = iptables -t nat -I PREROUTING 1 -d 192.168.50.103/32 -p tcp --dport 443 -j DNAT --to-destination 192.168.50.103:8443
```

The former single-node kind deployment is retired and is not a live rollback
anchor. Do not remove the k3s entry rule unless a separately verified endpoint
has first been established.

## Verify

```bash
# Verify the public path and, when a host-local mapping bypasses PREROUTING,
# resolve the k3s front explicitly:
curl -sSk https://yylx.world/staging/
curl -sSk https://yylx.world/staging/api/v1/health
curl -sSk --resolve yylx.world:8443:192.168.50.103 https://yylx.world:8443/staging/
curl -sSk --resolve yylx.world:8443:192.168.50.103 https://yylx.world:8443/staging/api/v1/health
kubectl -n loom-staging get pods
```

Expect the SPA (`/staging/assets/index-*.js`), `/api/v1/health` = `{"status":"ok"}`, MinIO 4/4, CNPG 3/3.

## Rollback

- **Redeploy a prior sha:** re-run the script with the previous `<git-sha>` (workloads roll back; DB migrations are forward-only — a schema rollback is a separate restore).
- **Entrypoint rollback:** switch only to a separately verified replacement
  endpoint. Removing the DNAT without such an endpoint is an outage, because
  the retired kind runtime is no longer underneath it.
