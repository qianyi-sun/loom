"""Public/internal boundary auditor for rendered manifests (#77).

Catches accidental public exposure of internal services. Walks
multi-doc YAML and flags:

- Services with `type: LoadBalancer` or `NodePort` (only ClusterIP is
  allowed for internal workloads; the public surface goes through the
  shared Ingress).
- Ingress backends that aren't on the public allowlist (`loom-service`
  + `loom-web` by default; `loom-llm-gateway` only when the operator
  explicitly opts in via `gateway_public_host`).
- `hostPort` declarations on any pod template (a hostPort binds to a
  node interface — equivalent to a NodePort but harder to spot in
  review).

The auditor is read-only and pure — it takes rendered YAML as input
and emits a list of violations. `loom cluster audit` wraps it with
a config loader + render call, and the kind smoke (#107) runs it
before apply so a boundary regression fails CI loudly instead of
landing silently.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import yaml  # type: ignore[import-untyped]

# Service names allowed as Ingress backends in the default install.
# loom-service: public REST API; loom-web: SPA shell.
_PUBLIC_ALLOWLIST: frozenset[str] = frozenset({"loom-service", "loom-web"})

# Service names allowed only when the operator opts in (currently:
# loom-llm-gateway, gated on `gateway_public_host` being set).
_PUBLIC_ALLOWLIST_OPTIONAL: frozenset[str] = frozenset({"loom-llm-gateway"})


@dataclass(frozen=True)
class BoundaryViolation:
    """One boundary violation. `kind` is the violation category;
    `object_*` identifies the offending manifest object."""

    kind: str  # "service-type" | "ingress-backend" | "host-port"
    object_kind: str  # "Service" | "Ingress" | "Deployment" | ...
    object_name: str
    detail: str


def audit_boundary(
    yaml_text: str, *, gateway_public_host: str | None = None,
) -> list[BoundaryViolation]:
    """Walk rendered manifests and flag boundary violations.

    `gateway_public_host` is the operator's opt-in flag for exposing
    the LLM gateway on its own public host. When set (non-empty),
    `loom-llm-gateway` is added to the Ingress backend allowlist;
    when None/empty, an Ingress rule pointing at it is flagged.
    """
    violations: list[BoundaryViolation] = []
    allowlist = set(_PUBLIC_ALLOWLIST)
    if gateway_public_host:
        allowlist |= _PUBLIC_ALLOWLIST_OPTIONAL

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

    return violations


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
    # `defaultBackend` catches every request that doesn't match a
    # rule — pointing it at an internal service is the same boundary
    # violation as listing one under `rules`.
    default = spec.get("defaultBackend") or {}
    default_svc = default.get("service", {}).get("name")
    if default_svc and default_svc not in allowlist:
        out.append(BoundaryViolation(
            kind="ingress-backend",
            object_kind="Ingress",
            object_name=name,
            detail=(
                f"Ingress defaultBackend {default_svc!r} is not on "
                f"the public allowlist {sorted(allowlist)}. "
                f"defaultBackend catches every unmatched request — "
                f"internal services must not appear here."
            ),
        ))
    return out


def _audit_pod_template(
    doc: dict[str, Any], kind: str, name: str,
) -> list[BoundaryViolation]:
    out: list[BoundaryViolation] = []
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
