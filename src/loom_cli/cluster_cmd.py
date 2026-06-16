"""`loom cluster {status, ...}` — k8s-mode operator surface.

Cluster mode targets an independently operated Kubernetes cluster
(cluster-deploy.md). The dev/demo path stays on `loom service`;
nothing here changes that.

Phase 1A (#76): read-only `status` command. Reads pod / service /
ingress state from the target context and reports component
readiness. Foundation for the rest of the cluster surface (render,
preflight, up, down — coming in subsequent PRs).

Lazy-imports the python `kubernetes` package so users who don't
need cluster mode aren't forced to install it. The CLI surfaces a
clear `pip install loom[cluster]` hint when the import fails.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, field
from typing import Any, cast

# These are the Deployments cluster-deploy.md §Component map lists as
# the in-cluster surface. The status command renders one row per
# component plus stateful sets (postgres, minio) when running in the
# default `--storage in-cluster` mode.
#
# Each tuple is (logical name, k8s label selector value). Adjust if a
# deployment manifest renames itself; the selectors are intentionally
# decoupled from the python list so a misspelled selector fails
# loudly (component reads as not-found) rather than silently
# skipping.
_COMPONENT_DEPLOYMENTS: tuple[tuple[str, str], ...] = (
    ("loom-service", "loom-service"),
    ("loom-control-plane", "loom-control-plane"),
    ("loom-llm-gateway", "loom-llm-gateway"),
    ("loom-web", "loom-web"),
)

_COMPONENT_DAEMONSETS: tuple[tuple[str, str], ...] = (
    ("loom-worker", "loom-worker"),
)

_COMPONENT_STATEFULSETS: tuple[tuple[str, str], ...] = (
    ("postgres", "loom-postgres"),
    ("minio", "loom-minio"),
)

# Logical-name → human description for the status output. Falls
# through to the component name itself if unknown.
_COMPONENT_DESCRIPTIONS: dict[str, str] = {
    "loom-service": "Public REST API",
    "loom-control-plane": "Internal scheduler + worker control",
    "loom-llm-gateway": "Provider-connection facade",
    "loom-web": "SPA (paused by default)",
    "loom-worker": "Trial runner DaemonSet (one per node)",
    "postgres": "Postgres (state)",
    "minio": "Object store (trajectories + ATIF)",
}


@dataclass
class ComponentStatus:
    """One row in the status table."""

    name: str
    kind: str  # "Deployment" | "DaemonSet" | "StatefulSet"
    ready: int
    desired: int
    available: bool
    last_restart_reason: str | None = None
    note: str | None = None

    @property
    def healthy(self) -> bool:
        return self.available and self.ready == self.desired and self.desired > 0


@dataclass
class IngressEndpoint:
    """One ingress host + path → service mapping."""

    host: str
    paths: list[str] = field(default_factory=list)
    tls: bool = False


@dataclass
class ClusterStatus:
    namespace: str
    context: str | None
    components: list[ComponentStatus]
    ingresses: list[IngressEndpoint]
    warnings: list[str]

    @property
    def all_ready(self) -> bool:
        return bool(self.components) and all(c.healthy for c in self.components)


def _format_table(status: ClusterStatus) -> str:
    """Stable human-readable table. Greppable + pipeable. Component
    state goes through stdout; warnings go to the same stream but
    after a blank line so they don't clobber the table."""
    lines: list[str] = []
    lines.append(
        f"namespace: {status.namespace}"
        + (f" (context: {status.context})" if status.context else ""),
    )
    lines.append("")
    lines.append(
        f"{'COMPONENT':<22} {'KIND':<14} {'READY':<8} {'STATUS':<10} "
        f"DESCRIPTION",
    )
    for c in status.components:
        ready_str = f"{c.ready}/{c.desired}"
        state = "ready" if c.healthy else "not-ready"
        desc = _COMPONENT_DESCRIPTIONS.get(c.name, "")
        if c.note:
            desc = f"{desc} — {c.note}" if desc else c.note
        lines.append(
            f"{c.name:<22} {c.kind:<14} {ready_str:<8} {state:<10} {desc}",
        )
    if status.ingresses:
        lines.append("")
        lines.append("INGRESS")
        for ing in status.ingresses:
            scheme = "https" if ing.tls else "http"
            for p in ing.paths or ["/"]:
                lines.append(f"  {scheme}://{ing.host}{p}")
    if status.warnings:
        lines.append("")
        lines.append("WARNINGS")
        for w in status.warnings:
            lines.append(f"  - {w}")
    return "\n".join(lines) + "\n"


def _format_json(status: ClusterStatus) -> str:
    """Machine-readable variant for CI / scripting. Includes the same
    information as the table; structure stable across versions."""
    obj: dict[str, Any] = {
        "namespace": status.namespace,
        "context": status.context,
        "all_ready": status.all_ready,
        "components": [
            {
                "name": c.name,
                "kind": c.kind,
                "ready": c.ready,
                "desired": c.desired,
                "available": c.available,
                "healthy": c.healthy,
                "last_restart_reason": c.last_restart_reason,
                "note": c.note,
            }
            for c in status.components
        ],
        "ingresses": [
            {"host": i.host, "paths": i.paths, "tls": i.tls}
            for i in status.ingresses
        ],
        "warnings": status.warnings,
    }
    return json.dumps(obj, indent=2) + "\n"


def _load_clients(
    context: str | None,
) -> tuple[object, object, object]:
    """Lazy import + load kube config. Returns (apps_v1, networking_v1,
    core_v1) API clients. Raises a RuntimeError with a friendly install
    hint if the kubernetes package isn't available; falls through to
    the kubernetes lib's own ConfigException on context errors."""
    try:
        from kubernetes import client, config  # type: ignore[import-not-found]
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "the 'kubernetes' package is required for `loom cluster` "
            "commands. install it with `pip install loom[cluster]` "
            "(or `uv add 'loom[cluster]'`).",
        ) from exc

    # Try in-cluster first (when running inside a Job/Pod), then fall
    # back to kubeconfig. The order matters: an operator running
    # `loom cluster status` from a laptop won't have in-cluster creds.
    try:
        config.load_kube_config(context=context)
    except Exception:
        # Re-raised so the caller maps to exit code 2 with a helpful
        # message. `config.ConfigException` is the documented type but
        # we catch broadly because the kubernetes lib raises a variety
        # of exception types here depending on what's missing.
        config.load_incluster_config()
    return client.AppsV1Api(), client.NetworkingV1Api(), client.CoreV1Api()


def _collect_workload(
    api: Any, namespace: str,
    deployments: tuple[tuple[str, str], ...],
    daemonsets: tuple[tuple[str, str], ...],
    statefulsets: tuple[tuple[str, str], ...],
) -> list[ComponentStatus]:
    """Read each workload's ready/desired counts. A workload that
    isn't deployed at all surfaces as ready=0/desired=0 with a
    `not-found` note — operators expect to see every expected
    component, not just the ones that happen to exist."""
    out: list[ComponentStatus] = []
    apps = api  # AppsV1Api
    for name, _ in deployments:
        try:
            d = apps.read_namespaced_deployment(name=name, namespace=namespace)
            spec = d.spec
            stat = d.status
            out.append(ComponentStatus(
                name=name,
                kind="Deployment",
                ready=int(stat.ready_replicas or 0),
                desired=int(spec.replicas or 0),
                available=(stat.available_replicas or 0) > 0,
            ))
        except Exception as exc:  # ApiException 404 most commonly
            out.append(ComponentStatus(
                name=name, kind="Deployment",
                ready=0, desired=0, available=False,
                note=_exception_to_note(exc),
            ))
    for name, _ in daemonsets:
        try:
            d = apps.read_namespaced_daemon_set(name=name, namespace=namespace)
            stat = d.status
            desired = int(stat.desired_number_scheduled or 0)
            ready = int(stat.number_ready or 0)
            out.append(ComponentStatus(
                name=name, kind="DaemonSet",
                ready=ready, desired=desired,
                available=ready > 0,
            ))
        except Exception as exc:
            out.append(ComponentStatus(
                name=name, kind="DaemonSet",
                ready=0, desired=0, available=False,
                note=_exception_to_note(exc),
            ))
    for name, _ in statefulsets:
        try:
            s = apps.read_namespaced_stateful_set(name=name, namespace=namespace)
            spec = s.spec
            stat = s.status
            ready = int(stat.ready_replicas or 0)
            desired = int(spec.replicas or 0)
            out.append(ComponentStatus(
                name=name, kind="StatefulSet",
                ready=ready, desired=desired,
                available=ready > 0,
            ))
        except Exception as exc:
            out.append(ComponentStatus(
                name=name, kind="StatefulSet",
                ready=0, desired=0, available=False,
                note=_exception_to_note(exc),
            ))
    return out


def _exception_to_note(exc: Exception) -> str:
    """Map a k8s API exception to a short status note. 404 →
    `not-found` so operators see which components haven't been
    deployed; other errors get the exception's type name + a
    truncated message."""
    cls = type(exc).__name__
    if cls == "ApiException":
        status = getattr(exc, "status", None)
        if status == 404:
            return "not-found"
        return f"k8s {status}: {str(exc)[:80]}"
    return f"{cls}: {str(exc)[:80]}"


def _collect_ingresses(api: Any, namespace: str) -> list[IngressEndpoint]:
    """Snapshot the ingress hosts + paths. NetworkingV1Api.
    list_namespaced_ingress lists each ingress; rules.host carries
    the host. Empty list when no ingress controller is installed
    (silently absent in the output)."""
    try:
        result = api.list_namespaced_ingress(namespace=namespace)
    except Exception:
        return []
    out: list[IngressEndpoint] = []
    for ing in result.items:
        spec = ing.spec
        if spec is None:
            continue
        # TLS hostnames are in spec.tls; flag them so the URL hint
        # uses https://.
        tls_hosts: set[str] = set()
        for t in (spec.tls or []):
            for h in (t.hosts or []):
                tls_hosts.add(h)
        for rule in (spec.rules or []):
            host = rule.host or ""
            paths: list[str] = []
            http = getattr(rule, "http", None)
            if http is not None:
                for p in (http.paths or []):
                    if p.path:
                        paths.append(p.path)
            out.append(IngressEndpoint(
                host=host, paths=paths, tls=(host in tls_hosts),
            ))
    return out


def collect_status(
    apps_v1: Any, networking_v1: Any, core_v1: Any,
    namespace: str, *, context: str | None,
) -> ClusterStatus:
    """Pure-collection function — the api clients are passed in so
    tests can inject fakes. Keeps the network-touching glue
    (`_load_clients`) out of the assertion surface."""
    components = _collect_workload(
        apps_v1, namespace,
        _COMPONENT_DEPLOYMENTS,
        _COMPONENT_DAEMONSETS,
        _COMPONENT_STATEFULSETS,
    )
    ingresses = _collect_ingresses(networking_v1, namespace)
    warnings: list[str] = []
    # Surface common missing-secret cases without poking each
    # component's pod-level events (that's preflight's job; this is
    # status). list_namespaced_secret with a name filter is cheap.
    if not _secret_present(core_v1, namespace, "loom-secrets"):
        warnings.append(
            "Secret 'loom-secrets' is missing — components that read "
            "from it (postgres URL, minio creds, JWT keys) will fail "
            "to start. See cluster-deploy.md §Bootstrap or run "
            "`loom cluster preflight` (coming in a follow-up).",
        )
    return ClusterStatus(
        namespace=namespace,
        context=context,
        components=components,
        ingresses=ingresses,
        warnings=warnings,
    )


def _secret_present(api: Any, namespace: str, name: str) -> bool:
    """Existence check via list+filter (cheaper than read+catch when
    the secret is gone) — only ever called once per command, so the
    cost is negligible either way."""
    try:
        api.read_namespaced_secret(name=name, namespace=namespace)
        return True
    except Exception:
        return False


def _status(args: argparse.Namespace) -> int:
    try:
        apps_v1, net_v1, core_v1 = _load_clients(args.context)
    except RuntimeError as exc:
        # Install-hint path.
        sys.stderr.write(f"error: {exc}\n")
        return 2
    except Exception as exc:
        # Kubeconfig / context errors. Map to exit 2 (cluster
        # unreachable) per the documented exit-code contract.
        sys.stderr.write(
            f"error: cannot connect to cluster: "
            f"{type(exc).__name__}: {exc}\n",
        )
        return 2

    try:
        status = collect_status(
            apps_v1, net_v1, core_v1, args.namespace,
            context=args.context,
        )
    except Exception as exc:
        sys.stderr.write(
            f"error: failed to read cluster state: "
            f"{type(exc).__name__}: {exc}\n",
        )
        return 2

    if args.format == "json":
        sys.stdout.write(_format_json(status))
    else:
        sys.stdout.write(_format_table(status))

    return 0 if status.all_ready else 1


def dispatch(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        prog="loom cluster",
        description=(
            "Manage a Loom deployment on Kubernetes (cluster-deploy.md). "
            "`loom service` remains the dev/demo path; cluster mode is "
            "a sibling for production deployments."
        ),
    )
    sub = parser.add_subparsers(dest="cluster_cmd", required=True)

    p_status = sub.add_parser(
        "status",
        help=(
            "Show component readiness + ingress endpoints for the "
            "configured Loom namespace."
        ),
    )
    p_status.add_argument(
        "--context", default=None,
        help="kubeconfig context (default: current context).",
    )
    p_status.add_argument(
        "--namespace", default="loom",
        help="Kubernetes namespace (default: loom).",
    )
    p_status.add_argument(
        "--format", choices=["table", "json"], default="table",
        help="Output format. JSON for CI/scripting.",
    )
    p_status.set_defaults(handler=_status)

    args = parser.parse_args(argv)
    return cast(int, args.handler(args))
