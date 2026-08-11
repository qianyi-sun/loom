# Envoy egress-proxy spike — findings (closes #196)

> Archived infrastructure experiment. Current egress behavior is documented in
> `docs/architecture/sandbox-isolation.md`.

Pre-PR-C1b investigation: validates the planned Envoy filter shape
before the gRPC xDS server is written. Run 2026-06-18 against Envoy
v1.30 on Linux.

**TL;DR — pattern A wins** (CDS-only with per-connection cluster +
header_match routing) **PAIRED with `:authority` CONNECT-target
match** for hostname-level allowlisting. The cluster IPs alone are
not sufficient when providers are CDN-fronted; details below.

## Reproduce

```bash
# Spike A — minimal HTTP CONNECT forward proxy works
docker run -d --name envoy-spike-a --network=host \
  -v $(pwd)/bootstrap-a-minimal.yaml:/etc/envoy/envoy.yaml \
  envoyproxy/envoy:v1.30-latest -c /etc/envoy/envoy.yaml --log-level warn
curl -x http://localhost:30443 https://example.com/   # → 200
docker rm -f envoy-spike-a

# Spike B — per-connection cluster + header-driven routing
docker run -d --name envoy-spike-b --network=host \
  -v $(pwd)/bootstrap-b-per-connection.yaml:/etc/envoy/envoy.yaml \
  envoyproxy/envoy:v1.30-latest -c /etc/envoy/envoy.yaml --log-level warn

# Note: curl's `-H` adds headers to the GET INSIDE the tunnel,
# NOT the CONNECT itself. Use `--proxy-header` to attach the
# x-loom-connection-id to the CONNECT request Envoy sees.
curl -x http://localhost:30443 --proxy-header 'x-loom-connection-id: A' https://example.com/
curl -x http://localhost:30443 https://example.com/   # no header → 404

docker rm -f envoy-spike-b
```

## Findings

### A. Minimal forward proxy works as expected

HCM with `upgrade_configs: CONNECT` + a route with `connect_matcher`
+ `dynamic_forward_proxy` cluster yields a working HTTPS forward
proxy. CONNECT terminates at Envoy; bytes tunnel through. Verified
against `example.com`, `api.openai.com`, `httpbin.org`.

### B. Per-connection cluster routing works for the simple case

Two static clusters (`egress-A` with `172.66.147.243`,
`egress-B` with `162.159.140.245`) plus header_match on
`x-loom-connection-id`:

| Test | Header | CONNECT target | Result |
|------|--------|----------------|--------|
| 1    | `A`    | `example.com`     | **200** — TLS handshake against cluster A's endpoint succeeded |
| 2    | `A`    | `api.openai.com`  | **421 Misdirected** — bytes routed to cluster A's IP, upstream rejected the hostname |
| 3    | `B`    | `api.openai.com`  | **421 Misdirected** — see "CDN finding" below |
| 4    | `B`    | `example.com`     | **200** — see "CDN finding" below |
| 5    | (none) | `example.com`     | **404** — no virtual_host matched; Envoy denied at the route layer |

### C. CDN-fronted services collapse IP-allowlisting

Tests 3 and 4 showed unexpected results. Investigation:
`example.com` resolves to `104.20.23.154` (Cloudflare) and
`api.openai.com` resolves to `162.159.140.245` (also Cloudflare). The
Cloudflare edge serves multiple domains from the same IP via SNI.

**Implication:** IP-allowlist enforcement at the egress-proxy layer
is essentially "you can only reach Cloudflare" rather than "you can
only reach api.openai.com" — and the Cloudflare edge serves
thousands of domains. The IP allowlist is too coarse for CDN-fronted
providers, which includes all of OpenAI, Anthropic (Cloudflare), and
most managed LLM APIs.

**Mitigation:** layer a `:authority` target match in the route so the
route ONLY matches when both the header and the target hostname/port
are expected. Loom stores the provider `base_url` and derived
`upstream_host` on `provider_connections`; the xDS server publishes a
per-connection route whose match clause is:

```yaml
- match:
    connect_matcher: {}
    headers:
      - name: x-loom-connection-id
        string_match: { exact: "<connection_id>" }
      - name: ":authority"
        string_match: { exact: "<upstream_host>:<port>" }
  route:
    cluster: egress-<connection_id>
```

HTTPS providers use the CONNECT route shape above. HTTP providers use
the same header + authority pair on an ordinary forward-proxy route
instead of `connect_matcher`, and the route strips
`x-loom-connection-id` before forwarding upstream. This forces BOTH
header AND target authority to be the expected pair.
Then the cluster's IP set is defense-in-depth (Envoy still can't
physically reach an IP not in the cluster, even if the route match
were bypassed).

### D. Note on `--proxy-header` vs `-H`

curl's `-H` adds headers to the GET request **inside** the CONNECT
tunnel; the CONNECT itself sees only `:authority`, `Host`, and
`Proxy-Connection`. To get header-driven routing, the upstream
gateway-router client MUST add the tenant header to the
`Authorization`/`x-api-key`/`x-loom-connection-id` field via
`--proxy-header` semantics, NOT request headers. In httpx this is
`event_hooks={"request": …}` setting headers on the proxy connect
request, OR (cleaner) using an outbound `Proxy` configured with
explicit proxy headers.

This is a real coupling point: the gateway-router HTTP client (which
Loom owns, post-#192) MUST send the connection ID on the CONNECT,
not the inner request. PR-C2 must include a test that verifies
this — the failure mode is silent (no routing, just default route
or 404) so a missed change here breaks isolation invisibly.

## Decision: pattern A + `:authority` match (CDS only, no RDS/LDS)

Of the three options from the plan:

- **A. CDS-only with per-connection cluster.** ✅ **Picked.**
  - One Cluster per connection_id; endpoints = `resolved_egress_ips`.
  - VirtualHost route matches on header AND `:authority`; route action = cluster.
  - **xDS server delivers ONLY CDS** (Clusters). Route config is part of the bootstrap (static), regenerated and pushed only when the connection set changes via... wait — routes ALSO change per connection. So RDS too.

Revising:

- **A'. CDS + RDS, no LDS.** Listener (with the HCM bootstrap) stays
  static. RouteConfiguration is published via RDS, one VirtualHost
  per connection_id (or one shared VirtualHost with one Route per
  connection_id — operationally equivalent). Clusters published via
  CDS, one per connection_id, endpoints from `resolved_egress_ips`.

- **B. CDS + RDS + RBAC filter.** ❌ Skipped. RBAC at HCM level
  works on `destination_ip` of the LOCAL listener, not upstream IP.
  Network RBAC would need to inspect post-routing upstream cluster
  endpoint, which isn't a first-class match. RBAC adds complexity
  without buying anything the cluster-endpoint set doesn't already
  enforce.

- **C. ext_authz callback.** ❌ Skipped. Adds per-request latency
  (one gRPC round-trip per provider call), and ext_authz can't
  influence cluster selection — it only allows/denies an
  already-routed request. We'd still need CDS+RDS for the routing,
  with ext_authz as belt-and-braces. Not worth it.

## Implications for PR-C1b

The xDS server implements **two discovery services**:

1. **CDS (ClusterDiscoveryService)** — one Cluster per connection_id,
   `type: STATIC`, `load_assignment` from `resolved_egress_ips`. Hot-
   reloaded when the snapshot version changes (#192's watcher).
2. **RDS (RouteDiscoveryService)** — one RouteConfiguration containing
   N VirtualHosts (or one VirtualHost with N routes); each route
   matches on `x-loom-connection-id: <id>` AND `:authority:
   <upstream_host>:<port>`, action `cluster: egress-<id>`. The port is
   derived from `provider_connections.base_url`: explicit ports are
   preserved, HTTPS defaults to 443, and HTTP defaults to 80.

**Bootstrap (static, baked into `deploy/envoy/egress-proxy.yaml`):**
- Listener on `:30443` with HCM
- HCM `route_config`: RDS pointing at `loom-egress-xds:18000`
- Cluster `loom-egress-xds` (the control plane itself) for ADS bootstrap

**LDS unnecessary** — the listener config doesn't change across
connection mutations.

**Postgres NOTIFY → snapshot push** flow (already shipped in #192's
watcher): on each snapshot version change, the xDS server pushes a
new `DiscoveryResponse` for both CDS and RDS in one transaction.

## Out of scope for the spike (handled in PR-C1b)

- xDS gRPC server impl (the actual `ClusterDiscoveryService` +
  `RouteDiscoveryService` servicers).
- `Snapshot → Envoy protobuf` translation (the `loom_egress_xds.
  config_builder` Snapshot is shape-stable; PR-C1b adds a `to_envoy_
  protos` function).
- Dockerfile, k8s manifest, docker-compose integration.
- Testcontainer integration test running Envoy + xds server + mock
  Postgres against a known-good connection row.
- Gateway-router client change to send `x-loom-connection-id` via
  CONNECT proxy header (PR-C2).
