"""Public/internal boundary auditor for rendered manifests (#77).

Catches accidental public exposure of internal services. Walks
multi-doc YAML and flags:

- Services with `type: LoadBalancer` or `NodePort` (only ClusterIP is
  allowed for internal workloads; the public surface goes through the
  shared Ingress).
- Ingress backends that aren't on the public allowlist (`loom-service`
  + `loom-web` only), or route the allowlisted backend on the wrong
  public path.
- Ingresses without TLS, or with a defaultBackend catch-all.
- `hostPort` declarations on any pod template (a hostPort binds to a
  node interface — equivalent to a NodePort but harder to spot in
  review).

The auditor is read-only and pure — it takes rendered YAML as input
and emits a list of violations. `loom cluster audit` wraps it with
a config loader + render call, and the cluster smoke (#107) runs it
before apply so a boundary regression fails CI loudly instead of
landing silently.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import yaml  # type: ignore[import-untyped]

# Service names allowed as Ingress backends. loom-service is the
# public REST API under /api/v1; loom-web is the SPA shell at /.
_PUBLIC_ALLOWLIST: frozenset[str] = frozenset({"loom-service", "loom-web"})

_EXPECTED_INGRESS_ROUTES: dict[str, str] = {
    "loom-service": "/api/v1",
    "loom-web": "/",
}
_EXPECTED_PREFIXED_INGRESS_ROUTES: dict[str, frozenset[str]] = {
    "loom-service": frozenset({
        "/(?-i:prod)/(api/v1((/[^/%]+)*/?))$",
        "/(?-i:staging)/(api/v1((/[^/%]+)*/?))$",
        "/(?-i:dev)/(api/v1((/[^/%]+)*/?))$",
    }),
    "loom-web": frozenset({
        "/(?-i:prod)/(([^/%]+/)*[^/%]+/?)?$",
        "/(?-i:staging)/(([^/%]+/)*[^/%]+/?)?$",
        "/(?-i:dev)/(([^/%]+/)*[^/%]+/?)?$",
        "/(?-i:prod)$",
        "/(?-i:staging)$",
        "/(?-i:dev)$",
    }),
}

# Pods that MUST have a NetworkPolicy selecting them (#78 slice C).
# These are the in-cluster Loom components; an internal-only workload
# without any NetworkPolicy is reachable from every other pod in the
# namespace by default (k8s allow-all default), which is exactly the
# boundary the public/internal split is supposed to enforce.
_REQUIRES_NETWORK_POLICY: frozenset[str] = frozenset({
    "loom-control-plane",
    "loom-llm-gateway",
    "loom-service",
    "loom-web",
    "loom-worker",
    "loom-postgres",
    "loom-minio",
    "loom-gateway-router",
    "loom-worker-router",
    "loom-minio-router",
})

# Workloads that legitimately need a hostPort. The cluster-deploy
# design explicitly calls out these uses:
# - `loom-gateway-router` binds hostPort 30443 (socat TCP forwarder)
# - `loom-worker-router` binds hostPort 30080 (socat TCP forwarder giving
#   EXTERNAL workers a private node-IP endpoint into the control-plane;
#   the CP's bearer-token auth gates it, and it is not publicly routable)
# - `loom-minio-router` binds hostPort 30900 (socat TCP forwarder giving
#   the same EXTERNAL workers a private node-IP endpoint into the loom-minio
#   object store; MinIO's S3v4 signature auth gates it, not publicly routable)
# - `loom-llm-gateway-sandbox` binds hostPort 8443 (TLS-terminating
#   HTTP CONNECT proxy, #547 item #3, closes #78 Phase B)
# Sandbox Docker containers spawned by the worker + external workers
# can't reach in-cluster Service DNS — they need stable per-node TCP
# endpoints. The auditor exempts named workloads here; any other
# hostPort is still flagged.
_HOSTPORT_ALLOWLIST: frozenset[str] = frozenset({
    "loom-gateway-router",
    "loom-worker-router",
    "loom-minio-router",
    "loom-llm-gateway-sandbox",
})


@dataclass(frozen=True)
class BoundaryViolation:
    """One boundary violation. `kind` is the violation category;
    `object_*` identifies the offending manifest object."""

    kind: str
    object_kind: str  # "Service" | "Ingress" | "Deployment" | ...
    object_name: str
    detail: str


def audit_boundary(
    yaml_text: str, *,
    gateway_public_host: str | None = None,
    require_network_policies: bool = True,
) -> list[BoundaryViolation]:
    """Walk rendered manifests and flag boundary violations.

    `gateway_public_host` is accepted for backwards-compatible callers
    but no longer changes the allowlist: the staging boundary keeps
    the LLM Gateway internal-only.

    `require_network_policies` (default True, matching the CLI's
    behavior) flags Loom components that aren't selected by any
    NetworkPolicy. Set to False when auditing a manifest fragment
    that intentionally omits NetworkPolicies (e.g., the
    Service/Ingress-only fixtures used by unit tests for the other
    checks).
    """
    violations: list[BoundaryViolation] = []
    allowlist = set(_PUBLIC_ALLOWLIST)

    # Track which Loom components have a NetworkPolicy selecting them
    # (#78 slice C). We collect the set of selected app-labels in
    # pass 1, then in pass 2 flag any required component without
    # coverage.
    np_covered_apps: set[str] = set()
    observed_workloads: set[str] = set()

    for doc in yaml.safe_load_all(yaml_text):
        if not doc:
            continue
        kind = doc.get("kind")
        name = doc.get("metadata", {}).get("name", "<unnamed>")

        if kind == "Service":
            violations.extend(_audit_service(doc, name))
        elif kind == "Ingress":
            violations.extend(_audit_ingress(doc, name, allowlist))
        elif kind in ("Deployment", "StatefulSet", "DaemonSet"):
            violations.extend(_audit_pod_template(doc, kind, name))
            observed_workloads.add(name)
        elif kind == "NetworkPolicy":
            np_covered_apps |= _network_policy_selected_apps(doc)

    if require_network_policies:
        # Pass 2: every required component MUST be selected by at
        # least one NetworkPolicy. A pod without any NetworkPolicy
        # selecting it gets k8s's default allow-all behavior, which
        # is exactly the boundary we want to enforce. Components that
        # were legitimately omitted from the render (e.g.
        # `loom-worker` on profiles with `k8s_worker.enabled=false`
        # per #383) don't need a policy because there's no pod to
        # protect — restrict the check to workloads actually present.
        missing = (
            _REQUIRES_NETWORK_POLICY & observed_workloads
        ) - np_covered_apps
        for app in sorted(missing):
            violations.append(BoundaryViolation(
                kind="missing-network-policy",
                object_kind="NetworkPolicy",
                object_name=app,
                detail=(
                    f"component {app!r} has no NetworkPolicy selecting "
                    f"it. Without a policy, k8s defaults to allow-all "
                    f"ingress and egress — internal traffic from "
                    f"arbitrary pods in the namespace is unrestricted."
                ),
            ))

    return violations


def _network_policy_selected_apps(doc: dict[str, Any]) -> set[str]:
    """Extract the `app` labels from a NetworkPolicy's `podSelector`.
    Returns an empty set for selectors that don't use a simple
    `matchLabels.app: <name>` shape — non-app selectors don't
    contribute to the required-component coverage check."""
    spec = doc.get("spec") or {}
    selector = spec.get("podSelector") or {}
    match_labels = selector.get("matchLabels") or {}
    app = match_labels.get("app")
    if isinstance(app, str):
        return {app}
    return set()


def _audit_service(
    doc: dict[str, Any], name: str,
) -> list[BoundaryViolation]:
    svc_type = doc.get("spec", {}).get("type", "ClusterIP")
    if svc_type in ("LoadBalancer", "NodePort"):
        return [BoundaryViolation(
            kind="service-type",
            object_kind="Service",
            object_name=name,
            detail=(
                f"Service type {svc_type!r} exposes pods to external "
                f"traffic; internal services must be ClusterIP. The "
                f"public surface goes through the shared Ingress."
            ),
        )]
    return []


def _audit_ingress(
    doc: dict[str, Any], name: str, allowlist: set[str],
) -> list[BoundaryViolation]:
    out: list[BoundaryViolation] = []
    spec = doc.get("spec", {}) or {}
    if not _ingress_has_valid_tls(spec):
        out.append(BoundaryViolation(
            kind="ingress-tls",
            object_kind="Ingress",
            object_name=name,
            detail=(
                "Ingress must declare TLS with secretName. Hostless "
                "TLS is allowed for IP-address entrypoints; public "
                "Loom traffic must use HTTPS."
            ),
        ))
    # `rules` may be missing entirely (defaultBackend-only Ingress is
    # legal) or null. Treat both as "no rules" — the defaultBackend
    # check below still runs.
    for rule in spec.get("rules") or []:
        http = rule.get("http") or {}
        for path in http.get("paths", []):
            backend = path.get("backend", {}).get("service", {})
            svc_name = backend.get("name")
            if svc_name and svc_name not in allowlist:
                out.append(BoundaryViolation(
                    kind="ingress-backend",
                    object_kind="Ingress",
                    object_name=name,
                    detail=(
                        f"Ingress backend {svc_name!r} is not on the "
                        f"public allowlist {sorted(allowlist)}. "
                        f"Internal services must not be reachable from "
                        f"the public Ingress."
                    ),
                ))
                continue
            if svc_name:
                expected_path = _EXPECTED_INGRESS_ROUTES[svc_name]
                actual_path = path.get("path", "")
                if not _is_expected_ingress_route(svc_name, actual_path):
                    out.append(BoundaryViolation(
                        kind="ingress-path",
                        object_kind="Ingress",
                        object_name=name,
                        detail=(
                            f"Ingress backend {svc_name!r} must be routed "
                            f"only at {expected_path!r}; found path "
                            f"{actual_path!r}. Public ingress may expose "
                            f"only SPA '/' and API '/api/v1', or their "
                            f"canonical /prod, /staging, and /dev prefixed routes."
                        ),
                    ))
    # `defaultBackend` catches every request that doesn't match a
    # rule — pointing it at an internal service is the same boundary
    # violation as listing one under `rules`.
    default = spec.get("defaultBackend") or {}
    default_svc = default.get("service", {}).get("name")
    if default_svc:
        out.append(BoundaryViolation(
            kind="ingress-default-backend",
            object_kind="Ingress",
            object_name=name,
            detail=(
                f"Ingress defaultBackend {default_svc!r} catches every "
                f"unmatched request. Public Loom ingress must use explicit "
                f"paths only: /api/v1 -> loom-service and / -> loom-web."
            ),
        ))
    return out


def _is_expected_ingress_route(svc_name: str, actual_path: str) -> bool:
    return (
        actual_path == _EXPECTED_INGRESS_ROUTES[svc_name]
        or actual_path in _EXPECTED_PREFIXED_INGRESS_ROUTES[svc_name]
    )


def _ingress_has_valid_tls(spec: dict[str, Any]) -> bool:
    for entry in spec.get("tls") or []:
        if entry.get("secretName"):
            return True
    return False


def _audit_pod_template(
    doc: dict[str, Any], kind: str, name: str,
) -> list[BoundaryViolation]:
    out: list[BoundaryViolation] = []
    # Whitelisted workloads (cluster-deploy.md called out hostPort
    # use cases) skip the hostPort check entirely.
    if name in _HOSTPORT_ALLOWLIST:
        return out
    pod_spec = (
        doc.get("spec", {}).get("template", {}).get("spec", {})
    )
    # initContainers are scheduled BEFORE containers and run in the
    # same pod network namespace — a hostPort declared there binds
    # the node interface just like in a regular container, but is
    # easier to miss in a code review. Audit both lists.
    for container_list_key in ("containers", "initContainers"):
        for container in pod_spec.get(container_list_key) or []:
            c_name = container.get("name", "<unnamed>")
            for port in container.get("ports") or []:
                if "hostPort" in port:
                    out.append(BoundaryViolation(
                        kind="host-port",
                        object_kind=kind,
                        object_name=name,
                        detail=(
                            f"container {c_name!r} (in "
                            f"{container_list_key}) declares "
                            f"hostPort={port['hostPort']}; a hostPort "
                            f"binds the container to a node interface "
                            f"(equivalent to NodePort but easier to miss "
                            f"in review)."
                        ),
                    ))
    return out


def format_violations(violations: list[BoundaryViolation]) -> str:
    """Render violations as a human-readable table for the CLI."""
    if not violations:
        return "Boundary: no violations found.\n"
    lines = [
        f"Boundary: {len(violations)} violation(s) found:\n",
    ]
    for v in violations:
        lines.append(
            f"  [{v.kind}] {v.object_kind}/{v.object_name}\n"
            f"    {v.detail}\n",
        )
    return "".join(lines)
