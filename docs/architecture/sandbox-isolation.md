# Sandbox Isolation

Loom treats task and agent code as untrusted with respect to platform and
provider credentials. Provider keys remain in the service and LLM Gateway;
sandboxes receive short-lived, trial-scoped step JWTs instead of upstream
credentials.

## Workload trust mode

The active isolation controls bound network and credential exposure. They do
not make arbitrary uploaded code safe to execute. The supported workload trust
mode is `internal_trusted`: user TaskSet transforms are disabled, and a
manifest containing a transform is rejected before its blobs are fetched.

## Container network policies

Drivers expose three task-level policies:

| Policy | Behavior |
| --- | --- |
| `Public` | Emits no iptables rules. The container uses its normal network. |
| `NoNetwork` | Allows loopback and established traffic, blocks link-local ranges, then sets the OUTPUT policy to DROP. |
| `Allowlist` | Resolves and pins approved domains, permits approved IPv4 CIDRs, blocks link-local ranges, then drops other outbound traffic. |

`NoNetwork` and `Allowlist` require iptables in the task image. Domain
allowlists resolve at policy-application time and enforce IPv4 only. Both
explicitly drop `169.254.169.254/32` and `169.254.0.0/16` before user-supplied
allow rules.

`Public` is deliberately a no-op for compatibility with minimal images. It
does not block cloud metadata or host-reachable services and must not be used
for untrusted workloads on cloud nodes.

## Optional per-trial bridge isolation

`LOOM_WORKER_SANDBOX_ISOLATION` is `false` by default. When enabled and the
sandbox singleton starts successfully, the worker:

1. creates a per-trial Docker `--internal` bridge;
2. attaches the trial container and the node-local
   `loom-llm-gateway-sandbox` singleton;
3. writes the initial step JWT under the configured secrets directory and
   rotates it every half-TTL using atomic replacement; and
4. injects the sandbox-facing gateway URL into supported subprocess agents.

The default step-JWT TTL is 600 seconds. Multiple workers on one host must use
different `LOOM_WORKER_SANDBOX_WORKER_INDEX` values so their subnet pools do
not overlap.

If the singleton cannot start, the worker logs the failure and continues
without the per-trial bridge path. Enabling the setting therefore does not by
itself guarantee isolation; operators must monitor startup and verify the
effective network path.

## Provider egress controls

Provider-connection creation and update resolve the upstream host and reject
unsafe private, loopback, link-local, or otherwise disallowed targets. The
approved results are stored as `resolved_egress_ips`.

When `LOOM_GW_EGRESS_PROXY_URL` is set and the egress xDS and Envoy components
are running, Gateway clients route through the proxy with an internal
connection identifier. Envoy routes match both that identifier and the
upstream authority, while xDS publishes only the stored IPs and port for that
connection. With the default empty proxy URL and zero proxy/xDS replicas,
Gateway calls use their direct path.

Cluster configuration can add explicit non-standard provider destinations
through `provider_egress_allowlist`. Entries must be narrow IP-or-CIDR and TCP
port pairs; hostnames are rejected because Kubernetes NetworkPolicy cannot
enforce DNS names.

## Kubernetes boundary

`loom cluster render` emits NetworkPolicy objects for Loom components and
`loom cluster audit` rejects a required component that has no selecting
policy. The audit proves coverage, not packet-level correctness. NetworkPolicy
is enforced by the cluster CNI and must be verified on every shared target.

Only the web application and `/api/v1` service are public through Ingress. The
Control Plane, LLM Gateway, Postgres, object storage, xDS, and egress proxy stay
on internal service or node routes described in [service mode](service-mode.md).

## Verification

- Run `loom cluster render` followed by `loom cluster audit` for the target
  profile.
- Use a real-Docker trial to prove that the internal bridge cannot reach a
  public IP while its sandbox gateway remains reachable.
- Exercise `NoNetwork` and `Allowlist` in an image that contains iptables.
- Verify that an expired or wrong-trial step JWT receives `401` from the
  Gateway.
- Run `scripts/check_no_provider_keys_in_artifacts.py <trial-id>` against
  completed trial output.

Repository coverage includes
`tests/integration/test_sandbox_isolation_acceptance.py`,
`tests/integration/test_sandbox_network_docker.py`, and
`tests/integration/test_egress_xds_envoy.py`.

See also [the Driver protocol](driver-protocol.md),
[LLM Gateway](llm-gateway.md), and [cluster deployment](cluster-deploy.md).
