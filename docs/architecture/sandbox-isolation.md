# Sandbox isolation — current trust boundary

**Status: as-shipped state, 2026-06-18. Epic [#78](https://github.com/carinrc/loom/issues/78) closed.**

What this doc is: an honest description of the sandbox isolation Loom
ships **today**, so operators can make informed decisions about which
network policy + isolation mode to use for which workload.

The full chain described in `cluster-deploy.md` §Sandbox→gateway
flow — per-trial Docker bridges, `loom-llm-gateway-sandbox`
singleton, `loom-egress-proxy` + `loom-egress-xds`, step-JWT
rotation — is **shipped** as of #78's close. Default-off
(`LOOM_WORKER_SANDBOX_ISOLATION=0`) preserves the pre-#78 single-
bridge behavior so operators can roll it out per environment.

## Trust model

A sandbox is **untrusted** for the purposes of provider-key access:

- The user-supplied agent runtime + verifier may be from
  upstream-benchmark code or third-party agent adapters.
- Provider API keys live exclusively on the **server side** of the
  gateway. They are never bind-mounted, env-injected, or otherwise
  exposed to sandbox containers.
- Sandboxes call models via `loom-llm-gateway` using a
  **step-scoped JWT** (`loom_step_*` prefix, HS256-signed,
  caller-supplied TTL — typically 60 minutes; claims include
  `trial_id`, `team_id`, `step_id`, and optionally
  `provider_connection_id` per #72) — not the underlying provider
  key. `cluster-deploy.md` describes a target Ed25519-signed
  shape; the as-shipped implementation uses HS256 with a shared
  signing key.

The sandbox is **trusted** for the workload it's running: it can
execute arbitrary code chosen by the task's agent. The isolation
goal is to bound the blast radius of that code, not prevent the
task itself from running.

## Enforcement mechanisms — as shipped

### Container-level: iptables-based NetworkPolicy

`src/loom/driver/network_policy.py` translates a Pydantic
`NetworkPolicy` (one of three kinds) into iptables OUTPUT-chain
rules applied inside the container's netns by `DockerDriver`:

| Policy | Outbound rule | When to use |
|---|---|---|
| `Public` | **No iptables rules emitted** | Default. Backwards-compatible with vanilla images that don't have iptables installed. **No cloud-metadata / link-local blocking** (gap below). |
| `NoNetwork` | `iptables -P OUTPUT DROP` after flush | Pure-compute tasks that never need network. |
| `Allowlist(domains, cidrs)` | Default DROP + ACCEPT for resolved domain IPs + CIDRs + `lo` + ESTABLISHED | Recommended for gateway-only workflows. |

Allowlist resolves domains at apply-time via `getent ahosts`
(IPv4 only, see networking.py:80 comment for the IPv6 caveat), pins
the IPs into `/etc/hosts`, then opens iptables to those IPs. DNS
lookups inside the container after apply go through the default
DROP — that's intentional, so a compromised agent can't dial new
hosts via DNS.

### Container-level: always-blocked CIDRs (#78 slice B, shipped)

When iptables rules are emitted at all (i.e. for `NoNetwork` and
`Allowlist`), `_ALWAYS_BLOCKED_CIDRS` DROP rules are inserted at the
top of the OUTPUT chain so they match BEFORE any operator-supplied
ACCEPT:

| CIDR | What it blocks |
|---|---|
| `169.254.169.254/32` | AWS / GCP / Azure instance metadata service |
| `169.254.0.0/16` | IPv4 link-local (IMDSv2 sibling IPs + arbitrary link-local services) |

iptables matches top-down and stops at the first match. The DROPs
come before any ACCEPT, so an operator who accidentally widens an
Allowlist to cover `0.0.0.0/0` (or any range containing the metadata
IP) still gets metadata traffic dropped.

### Container-level: NetworkPolicy `Public` remains a no-op

**Known gap (`Public` policy):** vanilla images run with no iptables
rules at all, so cloud-metadata IPs and the rest of the host's
reachable network are wide open. An agent running under a
`Public` policy on AWS / GCP / Azure can fetch the instance-metadata
service and exfiltrate node-level credentials. `Public` is
deliberately left as a no-op so vanilla container images without
iptables installed still work.

**Mitigation today:** never use `Public` for an untrusted workload
on a cloud-hosted cluster. Use `Allowlist({domains: ["gateway.loom"]})`
or `NoNetwork` and let the always-blocked CIDR DROPs + the explicit
allowlist do the work.

### Cluster-level: k8s NetworkPolicy on Loom components (#78 slice C, shipped)

`loom cluster render` emits seven `NetworkPolicy` resources, one per
required component, restricting ingress/egress to only what each
component needs:

| Component | Ingress allowed from | Egress allowed to |
|---|---|---|
| `loom-postgres` | `loom-control-plane`, `loom-llm-gateway`, `loom-service` | kube-dns |
| `loom-minio` | `loom-service`, `loom-worker`, `loom-llm-gateway` | kube-dns |
| `loom-control-plane` | `loom-worker`, `loom-service` | `loom-postgres`, kube-dns |
| `loom-llm-gateway` | `loom-worker`, `loom-service` | `loom-postgres`, `loom-minio`, kube-dns, public 80/443 (egress-proxy slice E will narrow this) |
| `loom-worker` | (none — workers only initiate) | `loom-control-plane`, `loom-llm-gateway`, `loom-minio`, kube-dns |
| `loom-service` | any (public REST surface) | `loom-control-plane`, `loom-llm-gateway`, `loom-postgres`, `loom-minio`, kube-dns |
| `loom-web` | any (public SPA) | kube-dns |

`loom cluster audit` enforces presence: any required component without
a selecting NetworkPolicy fails with `missing-network-policy`. The
auditor checks coverage, not correctness — operators who edit policies
should re-render against `deploy/k8s/network-policies.yaml` as the
golden.

**CNI requirement:** NetworkPolicy is CNI-enforced. Kind's default
CNI accepts the resources but does NOT enforce them; production
clusters need calico, cilium, or equivalent. The kind smoke
validates the schema + apply path; in-cluster traffic-shaping is
production-CNI work.

### Gateway-level: SSRF defense

`loom_service` enforces SSRF rejection at provider-connection
create/test time (`src/loom_service/provider_connections_service.py`):
the base URL is DNS-resolved and the resulting IPs are classified
against RFC1918 / ULA / loopback / link-local CIDRs. Connections
that resolve to private space are rejected before the row lands
in the database.

What this catches: an operator (or a compromised admin token)
cannot create a connection that targets the cluster's internal
network.

What this does NOT catch yet: a connection whose hostname
resolves to a public IP at create-time but resolves to private
space later (DNS rebinding mid-trial). The `resolved_egress_ips`
re-resolver described in `cluster-deploy.md` §Re-resolver is
partly designed, not shipped — provider_connection_id binding in
step JWTs (#72, shipped) is the current per-call check;
gateway-side IP allowlist enforcement is a #78 follow-up slice.

## Expected failure modes

| Scenario | Observable outcome |
|---|---|
| Agent under `NoNetwork` tries to dial anything | Connection refused / DROP'd at the iptables OUTPUT chain. Container-level error surfaces in the trial trajectory `events.jsonl`. |
| Agent under `Allowlist` tries to dial a non-allowlisted domain | DNS resolution succeeds (the iptables rule pins IPs at apply-time but doesn't block DNS UDP itself), but TCP connect drops. Look like a network timeout to the agent. |
| Agent under `Allowlist` tries to dial the gateway with an expired step JWT | Gateway returns `401`; agent sees an HTTP error. JWT expiry events surface in CP logs. |
| Agent under `Public` reaches `169.254.169.254` | **NOT blocked.** AWS / GCP / Azure metadata service responds with node-level credentials. Mitigation: don't use `Public` on cloud-hosted clusters. |
| Agent under `Allowlist` or `NoNetwork` reaches `169.254.169.254` | DROP'd by the always-blocked-CIDR rule (#78 slice B). TCP connect drops; agent sees a network timeout. |
| Gateway tries to dial a provider URL that resolves to a private IP | Gateway-side SSRF check rejects pre-connect. Returns a 403 with a clear `ssrf_blocked` reason. |
| Provider key leaks via trajectory event | Should not happen — keys are never given to the worker process, let alone the sandbox. If it does, treat as a critical bug, file a security issue. |

## Verifying the boundary

These are checks an operator can run today:

1. **Allowlist deny path.** Submit a trial with
   `NetworkPolicy=Allowlist(domains=["loom-llm-gateway.loom.svc"])`,
   then verify the agent can reach the gateway but not arbitrary IPs:
   ```bash
   # Inside a running sandbox (`docker exec`):
   curl -sf http://loom-llm-gateway.loom.svc:9100/healthz  # OK
   curl -sf http://1.1.1.1/                                 # times out / DROP'd
   ```

2. **NoNetwork deny path.** Same with `NoNetwork`; both calls should
   fail.

3. **JWT scoping.** Mint a step JWT for trial A, try to use it
   for a gateway call referencing trial B's connection — gateway
   returns 401 with the `wrong_trial` claim mismatch.

4. **No provider keys in trajectory.** Use the bundled audit script:
   ```bash
   ./scripts/check_no_provider_keys_in_artifacts.py <trial-id>
   ```
   Exit 0 = clean; exit 1 = pattern hit (with a redacted preview).
   Patterns cover: `sk-ant-*` (anthropic), `sk-*` (openai),
   `AIza*` (google), `xai-*`, `gsk_*` (groq), `nvapi-*`, `tgp_v1_*`,
   `r8_*` (replicate). Both modes: local-path or remote-trial-via-
   MinIO.

5. **Sandbox can't bypass the singleton.** With
   `LOOM_WORKER_SANDBOX_ISOLATION=1`, exec into a sandbox container
   and verify host egress is denied:
   ```bash
   docker exec -it <sandbox> wget -T 3 -O- http://1.1.1.1/  # times out
   docker exec -it <sandbox> curl -k https://loom-sandbox-gateway.local:8443/healthz  # OK
   ```
   The first fails because the `--internal` bridge has no host
   route; the second works because the singleton is attached.

## Sandbox-isolation mode (#78 — shipped)

Operators turn the full chain on via `LOOM_WORKER_SANDBOX_ISOLATION=1`
on the worker. Default off → pre-#78 single-bridge behavior, no
breaking change for existing deployments.

When on, every claimed trial:

1. **Per-trial bridge.** Worker allocates a free `/24` from the
   `10.{42+worker_index}.{1..254}.0/24` pool and runs
   `docker network create --internal …` — no host route. The container
   attaches via `StartOptions.network`. (Phase A, PR [#189](https://github.com/carinrc/loom/pull/189) + [#195](https://github.com/carinrc/loom/pull/195).)
2. **Per-node singleton.** Worker spawns ONE
   `loom-llm-gateway-sandbox` container per worker process (Go binary,
   ~14 MB distroless) and attaches it to every per-trial bridge. The
   singleton TLS-terminates on `loom-sandbox-gateway.local:8443`,
   validates step-JWTs (HS256, shared signing key), and reverse-
   proxies to `gateway-router`. (Phase B, PR [#220](https://github.com/carinrc/loom/pull/220) + [#221](https://github.com/carinrc/loom/pull/221) + [#224](https://github.com/carinrc/loom/pull/224).)
3. **Egress chain.** Gateway-router's outbound provider calls go
   through `loom-egress-proxy` (Envoy) which fetches per-connection
   CDS+RDS from `loom-egress-xds` (gRPC). Routes match on
   `(x-loom-connection-id, :authority)` pair so a tenant header alone
   isn't enough — destination hostname must also match. Cluster
   endpoints are `provider_connections.resolved_egress_ips`, so a
   compromised gateway can ONLY reach the IPs the connection was
   verified to resolve to. (Phase C, PRs [#192](https://github.com/carinrc/loom/pull/192) + [#200](https://github.com/carinrc/loom/pull/200) + [#209](https://github.com/carinrc/loom/pull/209) + [#215](https://github.com/carinrc/loom/pull/215) + [#217](https://github.com/carinrc/loom/pull/217).)
4. **Step-JWT rotation.** Worker mounts
   `/var/lib/loom/sandbox-secrets/trials/<trial>/run/loom/` into the
   sandbox at `/run/loom/`. A rotator writes the initial token before
   `driver.start()`, then atomically replaces `step-jwt` every
   `TTL/2` (default 300s) via `write(tmp) + os.replace(tmp, dst)` —
   POSIX-atomic. Concurrent readers see old-or-new contents, never
   partial. (Phase D, PR [#225](https://github.com/carinrc/loom/pull/225) + [#230](https://github.com/carinrc/loom/pull/230).)

The chain fails closed at every layer:

- Driver doesn't support `StartOptions.network` → `RuntimeError`
  before any state change (`Capabilities.supports_custom_network`).
- Singleton fails to start → isolation disabled for the worker
  process (logged), but worker keeps claiming trials so legacy
  paths still work.
- JWT mint fails mid-rotation → the previous token stays valid
  until its real expiry. The rotator logs + retries on the next tick.
- Envoy can't reach `loom-egress-xds` → no routes published →
  sandbox CONNECTs return 404 from Envoy.

### Operator switches

| Env | Default | Effect |
|---|---|---|
| `LOOM_WORKER_SANDBOX_ISOLATION` | `false` | Master switch. On = bridge + singleton + rotator + bind-mount. |
| `LOOM_WORKER_SANDBOX_SINGLETON_IMAGE` | `loom-llm-gateway-sandbox:dev` | Image tag for the per-node Go singleton. |
| `LOOM_WORKER_SANDBOX_SINGLETON_SECRETS_DIR` | `/var/lib/loom/sandbox-secrets` | Host-side root the rotator writes step-JWT files into. Mounted into the sandbox at `/run/loom/`. |
| `LOOM_WORKER_SANDBOX_WORKER_INDEX` | `0` | 0..15. Partitions the `/24` pool so multiple workers per host don't collide. |
| `LOOM_WORKER_SANDBOX_STEP_JWT_TTL_SEC` | `600` | TTL the worker requests for minted step-JWTs. Rotation cadence = TTL/2. |
| `LOOM_GW_EGRESS_PROXY_URL` | `""` (direct) | Gateway-side: when set, facade routes (`/openai/*`, `/anthropic/*`, `/google/*`) route through Envoy with per-connection CONNECT header. |

### Acceptance evidence

- `tests/integration/test_sandbox_isolation_acceptance.py` — pins
  the full chain order with a recording fake docker runner.
- `tests/integration/test_sandbox_network_docker.py` — real-docker
  proof that a `--internal` bridge denies `wget http://1.1.1.1/`.
- `tests/integration/test_egress_xds_envoy.py` — Postgres-fed xDS
  server publishes the expected Cluster + Route shapes.
- `deploy/envoy/spike/` — manual reproducible CONNECT proxy spike
  ([#196 findings](https://github.com/carinrc/loom/issues/196#issuecomment-4743811525)) that surfaced the
  `(header, :authority)` pair-match requirement (CDN-fronted
  providers share IPs).
- `deploy/docker-compose.dev.yml` — egress chain runs end-to-end in
  dev compose by default.
- `scripts/check_no_provider_keys_in_artifacts.py` — auditable
  proof a trial's artifacts contain no provider-key prefixes.

### Known limits

- **Per-step cost attribution** is coarse under isolation: the
  rotator mints with a fixed `step_id="sandbox-rotated"` rather than
  the live step. Cost attribution stays correct at the trial level;
  per-step granularity is a follow-up.
- **`routes/chat.py` (litellm path)** does NOT yet route through
  the egress proxy — litellm creates its own httpx client per call
  and per-CONNECT header injection requires either monkey-patching
  litellm or bypassing it. Sandboxes that should be IP-allowlisted
  MUST use the facade routes (`/openai/v1/*`, `/anthropic/v1/*`,
  `/google/v1beta/*`). Tracked at [#216](https://github.com/carinrc/loom/issues/216).

## See also

- [cluster-deploy.md §Sandbox→gateway flow](cluster-deploy.md#sandboxgateway-flow) — the full target architecture.
- [llm-gateway.md](llm-gateway.md) — gateway behavior + step JWT details.
- [driver-protocol.md](driver-protocol.md) — `NetworkPolicy` API.
- [#78](https://github.com/carinrc/loom/issues/78) — epic tracking remaining slices.
