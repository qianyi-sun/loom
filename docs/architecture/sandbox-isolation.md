# Sandbox isolation — current trust boundary

**Status: design + as-shipped state, 2026-06-16.**

What this doc is: an honest description of the sandbox isolation Loom
ships **today**, including known gaps, so operators can make informed
decisions about which network policy to use for which workload.

What this doc is NOT: the full target architecture described in
`cluster-deploy.md` §Sandbox→gateway flow — that includes
`loom-egress-proxy` + `loom-egress-xds` + per-trial Docker bridges +
`loom-llm-gateway-sandbox` singleton, which are partly shipped (basic
gateway + SSRF defense) and partly aspirational (per-trial bridges +
sandbox singleton). Tracked by epic [#78](https://github.com/carinrc/loom/issues/78).

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

### Cluster-level: k8s NetworkPolicy on Loom components

Not yet shipped. Once shipped, the policy set will:

- `loom-control-plane` ingress: only from `loom-worker` + `loom-service`.
- `loom-llm-gateway` ingress: only from `loom-worker` (production via
  `loom-gateway-router` DaemonSet pod, hostPort 30443) + `loom-service`.
- `loom-postgres` + `loom-minio`: only from listed services.
- Egress on every component: explicit allowlist of cluster DNS +
  the listed sibling services.

Until shipped, cluster-internal traffic is enforced only by Service
selector + the public/internal Ingress boundary (#77, `loom cluster audit`).

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

4. **No provider keys in trajectory.** `loom eval trial show <id>`
   then download the trajectory:
   ```bash
   grep -i 'sk-\|anthropic\|api[-_]key' events.jsonl
   ```
   Should return nothing for any trial. If it does, file a bug.

## Roadmap

The remaining slices of #78, in priority order:

1. **Cluster NetworkPolicies.** Ship k8s NetworkPolicy templates
   for the in-cluster components. Extends `loom cluster audit`
   to verify they're present.
2. **Per-trial Docker bridges.** Move from shared bridge to a
   `--internal` per-trial bridge with the worker spawning a
   `loom-llm-gateway-sandbox` singleton (architecture per
   cluster-deploy.md §Sandbox→gateway flow).
3. **`loom-gateway-router` DaemonSet.** Implements the
   sandbox-Docker-network → in-cluster-Service hop with hostPort
   30443.
4. **Egress proxy.** `loom-egress-proxy` Envoy + `loom-egress-xds`
   provider-CDS feeds for "even a compromised gateway can only
   reach approved provider IPs."

Each slice updates this doc as it lands.

Shipped slices:

- **Slice A** (PR #116): this doc, honest as-shipped trust boundary.
- **Slice B** (this PR): always-blocked CIDRs for metadata + link-local IPs.

## See also

- [cluster-deploy.md §Sandbox→gateway flow](cluster-deploy.md#sandboxgateway-flow) — the full target architecture.
- [llm-gateway.md](llm-gateway.md) — gateway behavior + step JWT details.
- [driver-protocol.md](driver-protocol.md) — `NetworkPolicy` API.
- [#78](https://github.com/carinrc/loom/issues/78) — epic tracking remaining slices.
