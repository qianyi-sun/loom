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
import ipaddress
import json
import sys
from dataclasses import dataclass, field
from importlib import resources
from pathlib import Path
from typing import Any, cast

from loom_cli.cluster_backup_guard import (
    DEFAULT_BACKUP_MAX_AGE_HOURS,
    infer_environment,
    is_protected_environment,
    validate_backup_manifest,
    write_backup_manifest,
)
from loom_cli.cluster_config import ClusterConfig, load_cluster_config
from loom_config.doctor import (
    DoctorReport,
)
from loom_config.doctor import (
    reconcile as _doctor_reconcile,
)
from loom_config.doctor import (
    reconcile_rendered as _doctor_reconcile_rendered,
)
from loom_config.loader import load_schema as _load_schema

# Repo root: cluster_cmd.py → loom_cli → src → loom (parents[2])
_REPO_ROOT = Path(__file__).resolve().parents[2]

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
    ("loom-worker", "loom-worker"),
)

# Reserved for future DaemonSet components (e.g., per-node log
# shippers). The trial-runner workload was DaemonSet in the original
# design but ships as a Deployment today — see issue #108 for the
# rationale and the deferred DaemonSet redesign.
_COMPONENT_DAEMONSETS: tuple[tuple[str, str], ...] = ()

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
    "loom-worker": "Trial runner Deployment (scales horizontally)",
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
        """A component is healthy when it has reached the desired
        replica count. A `desired=0` component (e.g., the default
        `loom-web` paused state, or `loom-worker` when an operator
        scales it to zero) is healthy by definition — the operator
        configured it that way.

        Before #128 caught it, this required `desired > 0` which
        meant `loom cluster up --wait` could never succeed against
        the default config (where `replicas.web = 0`). Available-
        replica check is dropped for desired=0 since available
        only makes sense when there's something to be available.
        """
        if self.desired == 0:
            return self.ready == 0
        return self.available and self.ready == self.desired


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
        f"{'COMPONENT':<22} {'KIND':<14} {'READY':<8} {'STATUS':<10} DESCRIPTION",
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
        "ingresses": [{"host": i.host, "paths": i.paths, "tls": i.tls} for i in status.ingresses],
        "warnings": status.warnings,
    }
    return json.dumps(obj, indent=2) + "\n"


def _load_clients(
    context: str | None,
) -> tuple[object, object, object, object]:
    """Lazy import + load kube config. Returns (apps_v1, networking_v1,
    core_v1, storage_v1) API clients. Raises a RuntimeError with a
    friendly install hint if the kubernetes package isn't available;
    falls through to the kubernetes lib's own ConfigException on
    context errors.

    `storage_v1` is used by `loom cluster preflight` to check for a
    default StorageClass; `status` ignores it. Kept in the same
    helper so every cluster subcommand uses one auth path."""
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
    return (
        client.AppsV1Api(),
        client.NetworkingV1Api(),
        client.CoreV1Api(),
        client.StorageV1Api(),
    )


def _effective_kube_context(context: str | None) -> str | None:
    if context:
        return context
    try:
        from kubernetes import config
    except ModuleNotFoundError:
        return None
    try:
        _contexts, active_context = config.list_kube_config_contexts()
    except Exception:
        return None
    if not isinstance(active_context, dict):
        return None
    name = active_context.get("name")
    return str(name) if name else None


def _collect_workload(
    api: Any,
    namespace: str,
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
    # Each tuple is (display_name, k8s_resource_name). For deployments
    # + daemonsets the two are identical; for statefulsets they
    # diverge ("postgres" → "loom-postgres"). The API call MUST use
    # the k8s name; the status row carries the display name.
    for display_name, k8s_name in deployments:
        try:
            d = apps.read_namespaced_deployment(name=k8s_name, namespace=namespace)
            spec = d.spec
            stat = d.status
            out.append(
                ComponentStatus(
                    name=display_name,
                    kind="Deployment",
                    ready=int(stat.ready_replicas or 0),
                    desired=int(spec.replicas or 0),
                    available=(stat.available_replicas or 0) > 0,
                )
            )
        except Exception as exc:  # ApiException 404 most commonly
            out.append(
                ComponentStatus(
                    name=display_name,
                    kind="Deployment",
                    ready=0,
                    desired=0,
                    available=False,
                    note=_exception_to_note(exc),
                )
            )
    for display_name, k8s_name in daemonsets:
        try:
            d = apps.read_namespaced_daemon_set(name=k8s_name, namespace=namespace)
            stat = d.status
            desired = int(stat.desired_number_scheduled or 0)
            ready = int(stat.number_ready or 0)
            out.append(
                ComponentStatus(
                    name=display_name,
                    kind="DaemonSet",
                    ready=ready,
                    desired=desired,
                    available=ready > 0,
                )
            )
        except Exception as exc:
            out.append(
                ComponentStatus(
                    name=display_name,
                    kind="DaemonSet",
                    ready=0,
                    desired=0,
                    available=False,
                    note=_exception_to_note(exc),
                )
            )
    for display_name, k8s_name in statefulsets:
        try:
            s = apps.read_namespaced_stateful_set(name=k8s_name, namespace=namespace)
            spec = s.spec
            stat = s.status
            ready = int(stat.ready_replicas or 0)
            desired = int(spec.replicas or 0)
            out.append(
                ComponentStatus(
                    name=display_name,
                    kind="StatefulSet",
                    ready=ready,
                    desired=desired,
                    available=ready > 0,
                )
            )
        except Exception as exc:
            out.append(
                ComponentStatus(
                    name=display_name,
                    kind="StatefulSet",
                    ready=0,
                    desired=0,
                    available=False,
                    note=_exception_to_note(exc),
                )
            )
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
        for t in spec.tls or []:
            for h in t.hosts or []:
                tls_hosts.add(h)
        for rule in spec.rules or []:
            host = rule.host or ""
            paths: list[str] = []
            http = getattr(rule, "http", None)
            if http is not None:
                for p in http.paths or []:
                    if p.path:
                        paths.append(p.path)
            out.append(
                IngressEndpoint(
                    host=host,
                    paths=paths,
                    tls=(host in tls_hosts),
                )
            )
    return out


def collect_status(
    apps_v1: Any,
    networking_v1: Any,
    core_v1: Any,
    namespace: str,
    *,
    context: str | None,
) -> ClusterStatus:
    """Pure-collection function — the api clients are passed in so
    tests can inject fakes. Keeps the network-touching glue
    (`_load_clients`) out of the assertion surface."""
    components = _collect_workload(
        apps_v1,
        namespace,
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


# ──────────────────────────────────────────────────────────────────────
# render (#76 Phase 1B)
# ──────────────────────────────────────────────────────────────────────


# Order matters: Postgres + MinIO come before the workloads that
# depend on them so `kubectl apply -f - < render` brings the cluster
# up in a topologically sound order. Ingress is last so it doesn't
# accept traffic until the backing Services exist.
_TEMPLATE_ORDER: tuple[str, ...] = (
    "persistent-storage.yaml.j2",
    "postgres.yaml.j2",
    "minio.yaml.j2",
    "control-plane.yaml.j2",
    "loom-service.yaml.j2",
    "llm-gateway.yaml.j2",
    "worker.yaml.j2",
    "web.yaml.j2",
    "ingress.yaml.j2",
    # gateway-router DaemonSet — per-node TCP proxy giving sandbox
    # Docker containers a stable hostPort:30443 endpoint to dial the
    # in-cluster gateway through. Carries its own NetworkPolicy.
    "gateway-router.yaml.j2",
    # Egress xDS control plane + Envoy proxy (#190 Phase C). The
    # xds-server reads provider_connections from Postgres + serves
    # CDS+RDS; Envoy fetches the dynamic config + acts as a forward
    # proxy for outbound provider traffic. Default replicas = 0 in
    # the schema; operators scale up when enabling sandbox-isolated
    # egress. NOT yet consumed by gateway-router (PR-C2 wires that).
    "egress-xds.yaml.j2",
    "egress-proxy.yaml.j2",
    # NetworkPolicies last — they reference workloads via podSelectors,
    # and listing them after the workloads keeps the rendered output
    # naturally ordered for human review (workload, then its policy).
    "network-policies.yaml.j2",
    # Grafana dashboards ConfigMap — auto-discovered by the
    # kube-prometheus-stack sidecar (grafana_dashboard: "1" label).
    # Listed last so removal doesn't break the core apply ordering.
    "grafana-dashboards.yaml.j2",
)

_PERSISTENT_STORAGE_DYNAMIC = "dynamic"
_PERSISTENT_STORAGE_STATIC_HOST_PATH = "static-host-path"
_PERSISTENT_STORAGE_BACKENDS = frozenset({
    _PERSISTENT_STORAGE_DYNAMIC,
    _PERSISTENT_STORAGE_STATIC_HOST_PATH,
})


def _normalise_static_host_path_root(config: ClusterConfig) -> str | None:
    backend = config.persistent_storage_backend
    if backend not in _PERSISTENT_STORAGE_BACKENDS:
        raise ValueError(
            "persistent_storage_backend must be one of "
            f"{sorted(_PERSISTENT_STORAGE_BACKENDS)!r}; got {backend!r}"
        )
    if backend == _PERSISTENT_STORAGE_DYNAMIC:
        return None
    root = config.persistent_storage_host_path_root.strip().rstrip("/")
    if not root:
        raise ValueError(
            "persistent_storage_host_path_root is required when "
            "persistent_storage_backend = 'static-host-path'"
        )
    if not root.startswith("/") or root == "/":
        raise ValueError(
            "persistent_storage_host_path_root must be an absolute host "
            "path below an operator-managed data directory, for example "
            "/data/loom-public-beta"
        )
    return root


def _join_host_path(root: str, child: str) -> str:
    return f"{root.rstrip('/')}/{child}"


def _persistent_storage_context(config: ClusterConfig) -> dict[str, Any]:
    root = _normalise_static_host_path_root(config)
    if root is None:
        return {
            "static_host_path_storage": False,
            "postgres_pv_name": "",
            "minio_pv_name": "",
            "worker_trajectories_pv_name": "",
            "persistent_volumes": [],
        }

    namespace = config.namespace
    postgres_pv_name = f"{namespace}-postgres-data"
    minio_pv_name = f"{namespace}-minio-data"
    worker_pv_name = f"{namespace}-worker-trajectories-data"
    return {
        "static_host_path_storage": True,
        "postgres_pv_name": postgres_pv_name,
        "minio_pv_name": minio_pv_name,
        "worker_trajectories_pv_name": worker_pv_name,
        "persistent_volumes": [
            {
                "name": postgres_pv_name,
                "claim_name": "data-loom-postgres-0",
                "storage_gi": config.postgres_storage_gi,
                "host_path": _join_host_path(root, "postgres"),
            },
            {
                "name": minio_pv_name,
                "claim_name": "data-loom-minio-0",
                "storage_gi": config.minio_storage_gi,
                "host_path": _join_host_path(root, "minio"),
            },
            {
                "name": worker_pv_name,
                "claim_name": "loom-worker-trajectories",
                "storage_gi": config.worker_trajectory_storage_gi,
                "host_path": _join_host_path(root, "trajectories"),
            },
        ],
    }


@dataclass(frozen=True)
class ProviderEgressRule:
    cidr: str
    port: int


_BLOCKED_PROVIDER_EGRESS_CIDRS = (
    ipaddress.ip_network("0.0.0.0/8"),
    ipaddress.ip_network("127.0.0.0/8"),
    ipaddress.ip_network("169.254.0.0/16"),
    ipaddress.ip_network("224.0.0.0/4"),
    ipaddress.ip_network("::1/128"),
    ipaddress.ip_network("fe80::/10"),
)


def _parse_provider_egress_target(raw: str) -> ProviderEgressRule:
    target = raw.strip()
    if not target:
        raise ValueError("provider_egress_allowlist entries must not be empty")

    if target.startswith("["):
        host_end = target.find("]")
        if host_end == -1 or len(target) <= host_end + 2 or target[host_end + 1] != ":":
            raise ValueError(
                f"provider_egress_allowlist entry {raw!r} must be "
                "IP-or-CIDR:TCP-port"
            )
        host = target[1:host_end]
        port_text = target[host_end + 2:]
    else:
        host, sep, port_text = target.rpartition(":")
        if not sep or not host:
            raise ValueError(
                f"provider_egress_allowlist entry {raw!r} must be "
                "IP-or-CIDR:TCP-port"
            )
        if ":" in host:
            raise ValueError(
                f"provider_egress_allowlist entry {raw!r} uses IPv6; wrap IPv6 "
                "CIDRs in brackets, for example [2001:db8::1/128]:8443"
            )

    try:
        port = int(port_text)
    except ValueError as exc:
        raise ValueError(
            f"provider_egress_allowlist entry {raw!r} has invalid TCP port "
            f"{port_text!r}; expected 1-65535"
        ) from exc
    if port < 1 or port > 65535:
        raise ValueError(
            f"provider_egress_allowlist entry {raw!r} has invalid TCP port "
            f"{port}; expected 1-65535"
        )

    try:
        network = ipaddress.ip_network(host, strict=False)
    except ValueError as exc:
        raise ValueError(
            f"provider_egress_allowlist entry {raw!r}: target must be an "
            "IP address or CIDR; Kubernetes NetworkPolicy cannot enforce "
            "DNS hostnames, so resolve the provider host first"
        ) from exc

    if network.prefixlen == 0:
        raise ValueError(
            f"provider_egress_allowlist entry {raw!r} is too broad; "
            "approve a specific provider IP or CIDR instead"
        )
    for blocked in _BLOCKED_PROVIDER_EGRESS_CIDRS:
        if blocked.version != network.version:
            continue
        if network.overlaps(blocked):
            raise ValueError(
                f"provider_egress_allowlist entry {raw!r} overlaps reserved "
                f"range {blocked}; refusing to render provider egress policy"
            )

    return ProviderEgressRule(cidr=str(network), port=port)


def _provider_egress_rules(config: ClusterConfig) -> list[dict[str, int | str]]:
    rules = {
        _parse_provider_egress_target(target)
        for target in config.provider_egress_allowlist
    }
    return [
        {"cidr": rule.cidr, "port": rule.port}
        for rule in sorted(
            rules,
            key=lambda item: (
                ipaddress.ip_network(item.cidr, strict=False).version,
                int(ipaddress.ip_network(item.cidr, strict=False).network_address),
                ipaddress.ip_network(item.cidr, strict=False).prefixlen,
                item.port,
            ),
        )
    ]


def render_manifests(config: ClusterConfig) -> str:
    """Render every template and join with `---` separators. Output is
    valid YAML that can be piped directly into `kubectl apply -f -`.

    Templates are loaded from `loom_cli.templates.k8s` via a Jinja2
    FileSystemLoader so that `{% import "_env.j2" as env_macros %}`
    directives in individual templates can resolve the shared macro.
    The package path is resolved once via `importlib.resources` so
    packaging (sdist + wheel) continues to pick up the templates
    without a separate MANIFEST.in entry.
    """
    try:
        from jinja2 import Environment, FileSystemLoader, StrictUndefined
    except ModuleNotFoundError as exc:
        # jinja2 is a core dep in pyproject.toml; this should never
        # fire in a correctly-installed environment but the error
        # message helps developers running a partial install.
        raise RuntimeError(
            "the 'jinja2' package is required for `loom cluster render`. "
            "if you installed Loom in development mode, run `uv sync` "
            "(or `pip install -e .`) to pick up dependencies.",
        ) from exc

    pkg_path = resources.files("loom_cli.templates.k8s")
    env = Environment(
        # FileSystemLoader is required so that `{% import "_env.j2" %}`
        # in per-service templates can resolve the shared macro file.
        loader=FileSystemLoader(str(pkg_path)),
        # StrictUndefined makes a missing variable error LOUDLY instead
        # of rendering an empty string. Better to fail at render time
        # than silently emit a manifest with `image: loom-service:`
        # (no tag) that operators chase for an hour.
        undefined=StrictUndefined,
        keep_trailing_newline=True,
        # trim_blocks + lstrip_blocks remove the newline after block
        # tags and leading whitespace before them, which is required
        # for the `_env.j2` macro to emit clean YAML without spurious
        # blank lines. Verified against all existing templates — parsed
        # YAML is identical with or without these flags.
        trim_blocks=True,
        lstrip_blocks=True,
    )
    ctx = config.to_render_context()
    ctx.update(_persistent_storage_context(config))
    ctx["provider_egress_rules"] = _provider_egress_rules(config)
    try:
        ipaddress.ip_address(config.ingress_host)
        ctx["ingress_host_is_ip"] = True
    except ValueError:
        ctx["ingress_host_is_ip"] = False
    ctx["schema"] = _load_schema(_REPO_ROOT / "config" / "loom-schema.toml")
    # Load dashboard JSON for the grafana-dashboards.yaml.j2 template.
    # The JSON is read from deploy/grafana/dashboards/ and passed as
    # pre-serialised strings so the Jinja2 `indent` filter can embed
    # them cleanly inside the ConfigMap data block.
    _dashboards_dir = _REPO_ROOT / "deploy" / "grafana" / "dashboards"
    ctx["operator_overview_json"] = (_dashboards_dir / "operator-overview.json").read_text()
    ctx["control_plane_json"] = (_dashboards_dir / "control-plane.json").read_text()
    ctx["llm_gateway_json"] = (_dashboards_dir / "llm-gateway.json").read_text()
    ctx["loom_service_json"] = (_dashboards_dir / "loom-service.json").read_text()
    ctx["worker_json"] = (_dashboards_dir / "worker.json").read_text()
    # Egress proxy bootstrap (#190 Phase C). Mounted as a ConfigMap so
    # operators can pin the Envoy config without a deploy/Dockerfile
    # change. Source of truth lives at deploy/envoy/egress-proxy.yaml;
    # the template embeds it via `| indent` into the ConfigMap data.
    ctx["envoy_egress_bootstrap"] = (
        _REPO_ROOT / "deploy" / "envoy" / "egress-proxy.yaml"
    ).read_text()
    chunks: list[str] = []
    for name in _TEMPLATE_ORDER:
        rendered = env.get_template(name).render(**ctx)
        if not rendered.strip():
            continue
        # Each rendered file is itself one-or-more YAML docs. Splice
        # with `---\n` between files. Files that already end with a
        # trailing newline merge cleanly; the join trims any
        # double-blank-line drift.
        chunks.append(rendered.rstrip() + "\n")
    return "\n---\n".join(chunks)


def _render(args: argparse.Namespace) -> int:
    try:
        cfg_path = Path(args.config).resolve() if args.config else None
        config = load_cluster_config(cfg_path)
    except (FileNotFoundError, ValueError) as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 2

    try:
        manifests = render_manifests(config)
    except (RuntimeError, ValueError) as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 2
    sys.stdout.write(manifests)
    return 0


def _audit(args: argparse.Namespace) -> int:
    """`loom cluster audit` — render manifests and check the
    public/internal boundary (#77). Renders without touching the
    cluster, so it's safe to run anywhere as a static check (CI
    pre-merge, kind smoke, operator dry-run)."""
    from loom_cli.cluster_boundary import audit_boundary, format_violations

    try:
        cfg_path = Path(args.config).resolve() if args.config else None
        config = load_cluster_config(cfg_path)
    except (FileNotFoundError, ValueError) as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 2

    try:
        manifests = render_manifests(config)
    except (RuntimeError, ValueError) as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 2

    violations = audit_boundary(manifests)
    sys.stdout.write(format_violations(violations))
    return 0 if not violations else 1


def _status(args: argparse.Namespace) -> int:
    try:
        apps_v1, net_v1, core_v1, _storage_v1 = _load_clients(args.context)
    except RuntimeError as exc:
        # Install-hint path.
        sys.stderr.write(f"error: {exc}\n")
        return 2
    except Exception as exc:
        # Kubeconfig / context errors. Map to exit 2 (cluster
        # unreachable) per the documented exit-code contract.
        sys.stderr.write(
            f"error: cannot connect to cluster: {type(exc).__name__}: {exc}\n",
        )
        return 2

    try:
        status = collect_status(
            apps_v1,
            net_v1,
            core_v1,
            args.namespace,
            context=args.context,
        )
    except Exception as exc:
        sys.stderr.write(
            f"error: failed to read cluster state: {type(exc).__name__}: {exc}\n",
        )
        return 2

    if args.format == "json":
        sys.stdout.write(_format_json(status))
    else:
        sys.stdout.write(_format_table(status))

    return 0 if status.all_ready else 1


# ──────────────────────────────────────────────────────────────────────
# preflight (#76 Phase 2A — API-side read-only checks)
# ──────────────────────────────────────────────────────────────────────


# Secrets the cluster manifests (post-rendering) require. Phase 1B's
# control-plane.yaml + loom-service.yaml mount `loom-admin-secret`;
# every component reads from `loom-secrets`. Tracking these by name
# here keeps the preflight in sync with the template surface — a
# future PR that adds a new Secret reference must add it here too.
_REQUIRED_SECRETS: tuple[str, ...] = (
    "loom-secrets",
    "loom-admin-secret",
)

# PodSecurityStandard label keys (k8s 1.25+). `restricted` enforce
# blocks the worker's hostPath docker-socket mount (per the spec's
# spike #02). The preflight surfaces this as a warning, not a fail,
# because the operator may have decided to use a different driver
# (sysbox / kata) where restricted enforce is fine.
_PSS_ENFORCE_LABEL = "pod-security.kubernetes.io/enforce"

_PreflightOutcome = str  # "pass" | "fail" | "warn"


@dataclass
class PreflightCheck:
    """One check + its outcome. `detail` is a single line of human-
    readable context; `remediation` is an optional hint operators
    can act on (e.g. how to create the missing Secret)."""

    name: str
    outcome: _PreflightOutcome
    detail: str
    remediation: str | None = None


@dataclass
class PreflightReport:
    namespace: str
    context: str | None
    checks: list[PreflightCheck]

    @property
    def all_pass(self) -> bool:
        return all(c.outcome == "pass" for c in self.checks)

    @property
    def any_fail(self) -> bool:
        return any(c.outcome == "fail" for c in self.checks)


def _check_namespace_exists(
    core_v1: Any,
    namespace: str,
) -> PreflightCheck:
    """First check: does the namespace exist? Every subsequent check
    is namespace-scoped so this gates the rest."""
    try:
        core_v1.read_namespace(name=namespace)
    except Exception as exc:
        return PreflightCheck(
            name="namespace-exists",
            outcome="fail",
            detail=f"namespace {namespace!r} not found ({_exception_to_note(exc)})",
            remediation=(
                f"kubectl create namespace {namespace}\n# Then re-run `loom cluster preflight`."
            ),
        )
    return PreflightCheck(
        name="namespace-exists",
        outcome="pass",
        detail=f"namespace {namespace!r} present",
    )


def _check_required_secrets(
    core_v1: Any,
    namespace: str,
) -> list[PreflightCheck]:
    """One check per Secret listed in `_REQUIRED_SECRETS`. Missing
    Secret = fail with a `kubectl create secret` hint."""
    out: list[PreflightCheck] = []
    for name in _REQUIRED_SECRETS:
        if _secret_present(core_v1, namespace, name):
            out.append(
                PreflightCheck(
                    name=f"secret-{name}",
                    outcome="pass",
                    detail=f"Secret {name!r} present in {namespace}",
                )
            )
        else:
            out.append(
                PreflightCheck(
                    name=f"secret-{name}",
                    outcome="fail",
                    detail=f"Secret {name!r} missing in {namespace}",
                    remediation=(
                        f"# See cluster-deploy.md §Bootstrap for the keys "
                        f"{name!r} expects. Quick example:\n"
                        f"kubectl create secret generic {name} \\\n"
                        f"  --namespace={namespace} \\\n"
                        f"  --from-literal=<key>=<value>"
                    ),
                )
            )
    return out


def _check_ingress_class_installed(
    networking_v1: Any,
) -> PreflightCheck:
    """An IngressClass resource is required for the `loom-ingress`
    Ingress to actually route traffic. Most clusters use `nginx`
    (ingress-nginx) but the check is class-agnostic — we just need
    *something* registered."""
    try:
        result = networking_v1.list_ingress_class()
    except Exception as exc:
        return PreflightCheck(
            name="ingress-class-installed",
            outcome="fail",
            detail=(f"failed to list IngressClass resources: {_exception_to_note(exc)}"),
            remediation=(
                "Check kubectl + cluster networking access; see cluster-deploy.md §Prerequisites."
            ),
        )
    classes = [getattr(it.metadata, "name", "<unknown>") for it in (result.items or [])]
    if not classes:
        return PreflightCheck(
            name="ingress-class-installed",
            outcome="fail",
            detail="no IngressClass resources registered in the cluster",
            remediation=(
                "Install an ingress controller (e.g. ingress-nginx):\n"
                "  helm install ingress-nginx ingress-nginx "
                "--repo https://kubernetes.github.io/ingress-nginx"
            ),
        )
    return PreflightCheck(
        name="ingress-class-installed",
        outcome="pass",
        detail=f"IngressClass(es) present: {', '.join(classes)}",
    )


def _check_default_storage_class(
    storage_v1: Any,
) -> PreflightCheck:
    """At least one StorageClass marked default. Postgres + MinIO
    StatefulSets and the worker trajectories PVC bind against this."""
    try:
        result = storage_v1.list_storage_class()
    except Exception as exc:
        return PreflightCheck(
            name="default-storage-class",
            outcome="fail",
            detail=(f"failed to list StorageClass resources: {_exception_to_note(exc)}"),
        )
    items = result.items or []
    if not items:
        return PreflightCheck(
            name="default-storage-class",
            outcome="fail",
            detail="no StorageClass resources registered",
            remediation=(
                "Install a CSI driver appropriate for your cluster "
                "(e.g. local-path-provisioner for kind:\n"
                "  kubectl apply -f "
                "https://raw.githubusercontent.com/rancher/"
                "local-path-provisioner/master/deploy/local-path-storage.yaml"
                ")"
            ),
        )
    default_names: list[str] = []
    for sc in items:
        anns = getattr(sc.metadata, "annotations", None) or {}
        # Both keys are used in the wild — the in-tree storage class
        # uses the unprefixed key, beta and modern CSI uses the
        # storage.k8s.io prefix.
        is_default = (
            anns.get("storageclass.kubernetes.io/is-default-class") == "true"
            or anns.get("storageclass.beta.kubernetes.io/is-default-class") == "true"
        )
        if is_default:
            default_names.append(getattr(sc.metadata, "name", "<unknown>"))
    if not default_names:
        return PreflightCheck(
            name="default-storage-class",
            outcome="warn",
            detail=(
                "StorageClass resources exist but none is marked "
                "default — PVCs without an explicit storageClassName "
                "will not bind"
            ),
            remediation=(
                "Pick one StorageClass and annotate it as default:\n"
                "  kubectl annotate sc <name> "
                "storageclass.kubernetes.io/is-default-class=true"
            ),
        )
    return PreflightCheck(
        name="default-storage-class",
        outcome="pass",
        detail=f"default StorageClass: {', '.join(default_names)}",
    )


def _storage_class_is_default(sc: Any) -> bool:
    anns = getattr(sc.metadata, "annotations", None) or {}
    return (
        anns.get("storageclass.kubernetes.io/is-default-class") == "true"
        or anns.get("storageclass.beta.kubernetes.io/is-default-class") == "true"
    )


_CRITICAL_STATE_PVCS: tuple[str, ...] = (
    "data-loom-postgres-0",
    "data-loom-minio-0",
    "loom-worker-trajectories",
)


def _object_metadata_name(obj: Any) -> str:
    return str(getattr(getattr(obj, "metadata", None), "name", "") or "")


def _pv_host_path(pv: Any) -> str:
    host_path = getattr(getattr(pv, "spec", None), "host_path", None)
    return str(getattr(host_path, "path", "") or "")


def _pv_local_path(pv: Any) -> str:
    local = getattr(getattr(pv, "spec", None), "local", None)
    return str(getattr(local, "path", "") or "")


def _pv_claim_ref(pv: Any) -> tuple[str, str]:
    claim_ref = getattr(getattr(pv, "spec", None), "claim_ref", None)
    return (
        str(getattr(claim_ref, "namespace", "") or ""),
        str(getattr(claim_ref, "name", "") or ""),
    )


def _storage_class_map(storage_classes: list[Any]) -> dict[str, Any]:
    return {
        _object_metadata_name(sc): sc
        for sc in storage_classes
        if _object_metadata_name(sc)
    }


def _storage_class_provisioner(
    *,
    storage_class_name: str,
    storage_classes_by_name: dict[str, Any],
) -> str:
    sc = storage_classes_by_name.get(storage_class_name)
    return str(getattr(sc, "provisioner", "") or "") if sc is not None else ""


def _check_existing_critical_pvc_storage(
    core_v1: Any,
    storage_classes: list[Any],
    *,
    namespace: str,
    environment: str,
) -> PreflightCheck | None:
    try:
        pvc_result = core_v1.list_namespaced_persistent_volume_claim(
            namespace=namespace,
        )
        pv_result = core_v1.list_persistent_volume()
    except Exception as exc:
        return PreflightCheck(
            name="protected-storage-boundary",
            outcome="fail",
            detail=(
                "failed to list PersistentVolumeClaims/PersistentVolumes "
                f"for protected environment {environment!r}: {_exception_to_note(exc)}"
            ),
        )

    pvcs = {
        _object_metadata_name(pvc): pvc
        for pvc in (pvc_result.items or [])
        if _object_metadata_name(pvc)
    }
    critical_present = [name for name in _CRITICAL_STATE_PVCS if name in pvcs]
    if not critical_present:
        return None

    pvs = {
        _object_metadata_name(pv): pv
        for pv in (pv_result.items or [])
        if _object_metadata_name(pv)
    }
    storage_classes_by_name = _storage_class_map(storage_classes)
    problems: list[str] = []
    ok_bindings: list[str] = []

    for pvc_name in _CRITICAL_STATE_PVCS:
        pvc = pvcs.get(pvc_name)
        if pvc is None:
            problems.append(f"{pvc_name} is missing")
            continue
        pvc_spec = getattr(pvc, "spec", None)
        pvc_status = getattr(pvc, "status", None)
        phase = str(getattr(pvc_status, "phase", "") or "")
        volume_name = str(getattr(pvc_spec, "volume_name", "") or "")
        if phase and phase != "Bound":
            problems.append(f"{pvc_name} phase={phase}")
        if not volume_name:
            problems.append(f"{pvc_name} has no bound volumeName")
            continue
        pv = pvs.get(volume_name)
        if pv is None:
            problems.append(f"{pvc_name}->{volume_name} PV is missing")
            continue

        pv_spec = getattr(pv, "spec", None)
        reclaim_policy = str(
            getattr(pv_spec, "persistent_volume_reclaim_policy", "") or ""
        )
        if reclaim_policy != "Retain":
            problems.append(
                f"{pvc_name}->{volume_name} reclaimPolicy="
                f"{reclaim_policy or '<unset>'}"
            )

        claim_namespace, claim_name = _pv_claim_ref(pv)
        if claim_namespace and claim_namespace != namespace:
            problems.append(
                f"{pvc_name}->{volume_name} claimRef namespace={claim_namespace}"
            )
        if claim_name and claim_name != pvc_name:
            problems.append(
                f"{pvc_name}->{volume_name} claimRef name={claim_name}"
            )

        storage_class_name = str(
            getattr(pv_spec, "storage_class_name", "")
            or getattr(pvc_spec, "storage_class_name", "")
            or ""
        )
        provisioner = _storage_class_provisioner(
            storage_class_name=storage_class_name,
            storage_classes_by_name=storage_classes_by_name,
        )
        host_path = _pv_host_path(pv)
        local_path = _pv_local_path(pv)
        if "local-path" in provisioner:
            problems.append(
                f"{pvc_name}->{volume_name} provisioner={provisioner}"
            )
        if local_path:
            if "local-path-provisioner" in local_path:
                problems.append(
                    f"{pvc_name}->{volume_name} localPath={local_path}"
                )
            else:
                problems.append(
                    f"{pvc_name}->{volume_name} uses local volume path={local_path}"
                )
        if host_path and not host_path.startswith("/data/"):
            problems.append(
                f"{pvc_name}->{volume_name} hostPath={host_path} "
                "is outside /data"
            )
        if not host_path and not local_path and not storage_class_name:
            problems.append(
                f"{pvc_name}->{volume_name} has no hostPath, local volume, "
                "or StorageClass to audit"
            )
        if not problems or not any(pvc_name in problem for problem in problems):
            ok_bindings.append(f"{pvc_name}->{volume_name}")

    if problems:
        return PreflightCheck(
            name="protected-storage-boundary",
            outcome="fail",
            detail=(
                f"protected environment {environment!r} critical PVCs are "
                "not on a durable Retain boundary: " + "; ".join(problems)
            ),
            remediation=(
                "Move Postgres, MinIO, and worker trajectories to external "
                "storage or explicit host-managed Retain PVs under /data "
                "before treating this environment as preproduction durable."
            ),
        )
    return PreflightCheck(
        name="protected-storage-boundary",
        outcome="pass",
        detail=(
            f"protected environment {environment!r} critical PVCs are bound "
            "to audited Retain PVs: " + ", ".join(ok_bindings)
        ),
    )


def _check_configured_static_host_path_storage(
    *,
    cluster_config: ClusterConfig | None,
    environment: str,
) -> PreflightCheck | None:
    if cluster_config is None:
        return None
    try:
        root = _normalise_static_host_path_root(cluster_config)
    except ValueError as exc:
        return PreflightCheck(
            name="protected-storage-boundary",
            outcome="fail",
            detail=(
                f"protected environment {environment!r} has invalid "
                f"persistent storage config: {exc}"
            ),
        )
    if root is None:
        return None
    return PreflightCheck(
        name="protected-storage-boundary",
        outcome="pass",
        detail=(
            f"protected environment {environment!r} render config declares "
            f"static-host-path Retain PVs under {root}; critical PVCs are "
            "not created yet"
        ),
    )


def _is_kind_context(context: str | None) -> bool:
    return bool(context and context.startswith("kind-"))


def _read_kind_node_mounts(context: str | None) -> list[dict[str, Any]] | None:
    if not _is_kind_context(context):
        return None
    assert context is not None
    cluster_name = context.removeprefix("kind-")
    node_name = f"{cluster_name}-control-plane"
    import subprocess

    proc = subprocess.run(
        ["docker", "inspect", "--format", "{{json .Mounts}}", node_name],
        capture_output=True,
        check=False,
        text=True,
    )
    if proc.returncode != 0:
        return None
    try:
        mounts = json.loads(proc.stdout.strip())
    except json.JSONDecodeError:
        return None
    if not isinstance(mounts, list):
        return None
    return [mount for mount in mounts if isinstance(mount, dict)]


def _normalise_mount_path(value: object) -> str:
    return str(value or "").rstrip("/") or "/"


def _kind_bind_mount_covers_static_root(
    mount: dict[str, Any],
    *,
    root: str,
) -> bool:
    if str(mount.get("Type") or mount.get("type") or "").lower() != "bind":
        return False
    source = _normalise_mount_path(mount.get("Source") or mount.get("source"))
    destination = _normalise_mount_path(
        mount.get("Destination") or mount.get("destination"),
    )
    if not source or source == "/":
        return False
    if destination == "/data":
        return root == "/data" or root.startswith("/data/")
    return destination == root or root.startswith(f"{destination}/")


def _check_kind_static_host_path_mount(
    *,
    context: str | None,
    cluster_config: ClusterConfig | None,
    environment: str,
    kind_node_mounts: list[dict[str, Any]] | None,
) -> PreflightCheck | None:
    if not _is_kind_context(context) or cluster_config is None:
        return None
    try:
        root = _normalise_static_host_path_root(cluster_config)
    except ValueError as exc:
        return PreflightCheck(
            name="kind-host-storage-mount",
            outcome="fail",
            detail=(
                f"protected kind environment {environment!r} has invalid "
                f"persistent storage config: {exc}"
            ),
        )
    if root is None:
        return None
    if kind_node_mounts is None:
        return PreflightCheck(
            name="kind-host-storage-mount",
            outcome="fail",
            detail=(
                f"protected kind environment {environment!r} uses "
                f"static-host-path root {root}, but the kind node Docker "
                "mounts could not be inspected"
            ),
            remediation=(
                "Run preflight from a host with Docker access, or rebuild the "
                "kind cluster with an extraMount that binds host /data or "
                f"{root} into the control-plane node."
            ),
        )
    if any(
        _kind_bind_mount_covers_static_root(mount, root=root)
        for mount in kind_node_mounts
        if isinstance(mount, dict)
    ):
        return PreflightCheck(
            name="kind-host-storage-mount",
            outcome="pass",
            detail=(
                f"protected kind environment {environment!r} has a host bind "
                f"mount covering static-host-path root {root}"
            ),
        )
    destinations = [
        _normalise_mount_path(mount.get("Destination") or mount.get("destination"))
        for mount in kind_node_mounts
        if isinstance(mount, dict)
    ]
    return PreflightCheck(
        name="kind-host-storage-mount",
        outcome="fail",
        detail=(
            f"protected kind environment {environment!r} uses static-host-path "
            f"root {root}, but the kind node has no Docker bind mount to /data "
            f"or {root}; mounted destinations={destinations or 'none'}"
        ),
        remediation=(
            "Rebuild the kind cluster with extraMounts mapping host /data "
            f"or {root} into the control-plane node, then restore from a "
            "verified backup before trusting static hostPath PV durability."
        ),
    )


def _check_protected_storage_boundary(
    core_v1: Any,
    storage_v1: Any,
    *,
    environment: str,
    namespace: str,
    cluster_config: ClusterConfig | None = None,
) -> PreflightCheck:
    try:
        result = storage_v1.list_storage_class()
    except Exception as exc:
        return PreflightCheck(
            name="protected-storage-boundary",
            outcome="fail",
            detail=(
                "failed to list StorageClass resources for protected "
                f"environment {environment!r}: {_exception_to_note(exc)}"
            ),
        )
    storage_classes = list(result.items or [])
    existing_check = _check_existing_critical_pvc_storage(
        core_v1,
        storage_classes,
        namespace=namespace,
        environment=environment,
    )
    if existing_check is not None:
        return existing_check

    configured_check = _check_configured_static_host_path_storage(
        cluster_config=cluster_config,
        environment=environment,
    )
    if configured_check is not None:
        return configured_check

    default_classes = [sc for sc in storage_classes if _storage_class_is_default(sc)]
    if not default_classes:
        return PreflightCheck(
            name="protected-storage-boundary",
            outcome="fail",
            detail=(
                f"protected environment {environment!r} has no default "
                "StorageClass to audit"
            ),
        )
    unsafe: list[str] = []
    for sc in default_classes:
        name = getattr(sc.metadata, "name", "<unknown>")
        provisioner = str(getattr(sc, "provisioner", "") or "")
        reclaim_policy = str(getattr(sc, "reclaim_policy", "") or "")
        reasons: list[str] = []
        if "local-path" in provisioner:
            reasons.append(f"provisioner={provisioner}")
        if reclaim_policy == "Delete":
            reasons.append("reclaimPolicy=Delete")
        if reasons:
            unsafe.append(f"{name} ({', '.join(reasons)})")
    if unsafe:
        return PreflightCheck(
            name="protected-storage-boundary",
            outcome="fail",
            detail=(
                f"protected environment {environment!r} uses disposable "
                "storage boundary: " + "; ".join(unsafe)
            ),
            remediation=(
                "Move critical state to external Postgres/MinIO or explicit "
                "host-managed Retain volumes before treating this environment "
                "as preproduction durable."
            ),
        )
    return PreflightCheck(
        name="protected-storage-boundary",
        outcome="pass",
        detail=(
            f"protected environment {environment!r} default StorageClass "
            "does not use local-path or Delete reclaim policy"
        ),
    )


def _check_backup_manifest(
    manifest_path: Path | None,
    *,
    environment: str,
    namespace: str,
    max_age_hours: int,
) -> PreflightCheck:
    problems = validate_backup_manifest(
        manifest_path,
        environment=environment,
        namespace=namespace,
        max_age_hours=max_age_hours,
    )
    if problems:
        return PreflightCheck(
            name="backup-manifest",
            outcome="fail",
            detail="; ".join(problems),
            remediation=(
                "Create a fresh metadata manifest with `loom cluster backup "
                "manifest ...` after dumping Postgres, mirroring MinIO, and "
                "backing up Kubernetes/runtime secrets."
            ),
        )
    assert manifest_path is not None
    return PreflightCheck(
        name="backup-manifest",
        outcome="pass",
        detail=f"recent backup manifest verified: {manifest_path}",
    )


def _check_pss_enforce(
    core_v1: Any,
    namespace: str,
) -> PreflightCheck:
    """The worker Deployment bind-mounts the host docker socket. PSS
    `restricted` (k8s 1.25+ default for new namespaces) rejects this
    at admission. Warn (not fail) so operators using a non-Docker
    driver aren't blocked."""
    try:
        ns = core_v1.read_namespace(name=namespace)
    except Exception:
        return PreflightCheck(
            name="pss-enforce",
            outcome="warn",
            detail="could not read namespace metadata",
        )
    labels = getattr(ns.metadata, "labels", None) or {}
    enforce = labels.get(_PSS_ENFORCE_LABEL)
    if enforce == "restricted":
        return PreflightCheck(
            name="pss-enforce",
            outcome="warn",
            detail=(
                f"namespace {namespace!r} has PSS enforce=restricted; "
                "the worker Deployment's hostPath docker.sock mount "
                "will be rejected"
            ),
            remediation=(
                "Either relax to enforce=privileged (worker docker "
                "driver), or switch worker to a non-Docker driver:\n"
                f"  kubectl label namespace {namespace} "
                f"{_PSS_ENFORCE_LABEL}=privileged --overwrite"
            ),
        )
    if enforce is None:
        return PreflightCheck(
            name="pss-enforce",
            outcome="pass",
            detail=(f"namespace {namespace!r} has no PSS enforce label (no admission restriction)"),
        )
    return PreflightCheck(
        name="pss-enforce",
        outcome="pass",
        detail=(f"namespace {namespace!r} PSS enforce={enforce!r} (non-restricted)"),
    )


def collect_preflight(
    core_v1: Any,
    networking_v1: Any,
    storage_v1: Any,
    namespace: str,
    *,
    context: str | None,
    environment: str | None = None,
    backup_manifest: Path | None = None,
    backup_max_age_hours: int = DEFAULT_BACKUP_MAX_AGE_HOURS,
    cluster_config: ClusterConfig | None = None,
    kind_node_mounts: list[dict[str, Any]] | None = None,
) -> PreflightReport:
    """Pure-collection function — every API client passed in so tests
    can inject fakes. If `namespace-exists` fails, the namespace-
    scoped checks (Secrets, PSS) are skipped to avoid burying the
    real problem in cascade failures."""
    checks: list[PreflightCheck] = []
    ns_check = _check_namespace_exists(core_v1, namespace)
    checks.append(ns_check)

    # Cluster-scoped checks always run, even when the namespace is
    # missing — they're useful diagnostics on their own.
    checks.append(_check_ingress_class_installed(networking_v1))
    checks.append(_check_default_storage_class(storage_v1))
    env_name = infer_environment(environment=environment, namespace=namespace)
    if env_name in {"public-beta", "staging", "production"}:
        checks.append(
            _check_protected_storage_boundary(
                core_v1,
                storage_v1,
                environment=env_name,
                namespace=namespace,
                cluster_config=cluster_config,
            )
        )
        kind_mount_check = _check_kind_static_host_path_mount(
            context=context,
            cluster_config=cluster_config,
            environment=env_name,
            kind_node_mounts=kind_node_mounts,
        )
        if kind_mount_check is not None:
            checks.append(kind_mount_check)
        checks.append(
            _check_backup_manifest(
                backup_manifest,
                environment=env_name,
                namespace=namespace,
                max_age_hours=backup_max_age_hours,
            )
        )

    if ns_check.outcome == "pass":
        checks.extend(_check_required_secrets(core_v1, namespace))
        checks.append(_check_pss_enforce(core_v1, namespace))

    return PreflightReport(
        namespace=namespace,
        context=context,
        checks=checks,
    )


def _format_preflight_table(report: PreflightReport) -> str:
    """Stable human-readable preflight output. Greppable. Each check
    appears on its own line with the outcome marker and detail; failed
    checks include their remediation hint inline (indented)."""
    lines: list[str] = []
    lines.append(
        f"namespace: {report.namespace}"
        + (f" (context: {report.context})" if report.context else ""),
    )
    lines.append("")
    lines.append(f"{'CHECK':<32} {'OUTCOME':<8} DETAIL")
    for c in report.checks:
        lines.append(f"{c.name:<32} {c.outcome:<8} {c.detail}")
        if c.remediation and c.outcome in ("fail", "warn"):
            for rline in c.remediation.splitlines():
                lines.append(f"    {rline}")
    return "\n".join(lines) + "\n"


def _format_preflight_json(report: PreflightReport) -> str:
    obj = {
        "namespace": report.namespace,
        "context": report.context,
        "all_pass": report.all_pass,
        "any_fail": report.any_fail,
        "checks": [
            {
                "name": c.name,
                "outcome": c.outcome,
                "detail": c.detail,
                "remediation": c.remediation,
            }
            for c in report.checks
        ],
    }
    return json.dumps(obj, indent=2) + "\n"


def _append_schema_doctor_check(
    report: PreflightReport,
    doctor_report: DoctorReport,
) -> None:
    if doctor_report.ok:
        report.checks.append(PreflightCheck(
            name="schema-doctor",
            outcome="pass",
            detail="schema reconciliation clean",
        ))
        return
    report.checks.append(PreflightCheck(
        name="schema-doctor",
        outcome="fail",
        detail=f"{len(doctor_report.violations)} schema violation(s)",
        remediation="\n".join(
            f"  - {v.kind}: {v.entry}: {v.detail}"
            for v in doctor_report.violations
        ),
    ))


def _append_target_schema_doctor_check(
    report: PreflightReport,
    *,
    core_v1: Any,
    namespace: str,
    config: ClusterConfig,
    rendered_manifests: str | None = None,
) -> None:
    schema = _load_schema(_REPO_ROOT / "config" / "loom-schema.toml")
    try:
        manifests = rendered_manifests or render_manifests(config)
        doctor_report = _doctor_reconcile_rendered(
            schema,
            core_v1,
            namespace=namespace,
            rendered_manifests=manifests,
        )
    except Exception as exc:
        report.checks.append(PreflightCheck(
            name="schema-doctor",
            outcome="warn",
            detail=f"doctor could not run: {type(exc).__name__}: {exc}",
        ))
    else:
        _append_schema_doctor_check(report, doctor_report)


def _preflight(args: argparse.Namespace) -> int:
    try:
        _apps_v1, net_v1, core_v1, storage_v1 = _load_clients(args.context)
    except RuntimeError as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 2
    except Exception as exc:
        sys.stderr.write(
            f"error: cannot connect to cluster: {type(exc).__name__}: {exc}\n",
        )
        return 2
    try:
        cfg_path = Path(args.config).resolve() if args.config else None
        cluster_config = load_cluster_config(cfg_path)
    except (FileNotFoundError, ValueError) as exc:
        sys.stderr.write(f"error: config invalid: {exc}\n")
        return 2
    try:
        effective_context = _effective_kube_context(args.context)
        kind_node_mounts = _read_kind_node_mounts(effective_context)
        report = collect_preflight(
            core_v1,
            net_v1,
            storage_v1,
            args.namespace,
            context=effective_context,
            environment=args.environment,
            backup_manifest=(
                Path(args.backup_manifest).resolve()
                if args.backup_manifest else None
            ),
            backup_max_age_hours=args.backup_max_age_hours,
            cluster_config=cluster_config,
            kind_node_mounts=kind_node_mounts,
        )
    except Exception as exc:
        sys.stderr.write(
            f"error: failed to read cluster state: {type(exc).__name__}: {exc}\n",
        )
        return 2
    if not args.no_doctor:
        _append_target_schema_doctor_check(
            report,
            core_v1=core_v1,
            namespace=args.namespace,
            config=cluster_config,
        )

    if args.format == "json":
        sys.stdout.write(_format_preflight_json(report))
    else:
        sys.stdout.write(_format_preflight_table(report))
    # Exit 1 only when something explicitly failed; warns alone keep
    # exit 0 so CI scripts don't have to special-case them.
    return 1 if report.any_fail else 0


def _backup_manifest(args: argparse.Namespace) -> int:
    try:
        write_backup_manifest(
            environment=args.environment,
            namespace=args.namespace,
            output_path=Path(args.output).resolve(),
            components={
                "postgres": Path(args.postgres_dump).resolve(),
                "minio": Path(args.minio_snapshot).resolve(),
                "k8s_secrets": Path(args.k8s_secrets).resolve(),
            },
        )
    except (OSError, ValueError, FileNotFoundError) as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 2
    sys.stdout.write(f"backup manifest written: {Path(args.output).resolve()}\n")
    return 0


def _backup_check(args: argparse.Namespace) -> int:
    problems = validate_backup_manifest(
        Path(args.manifest).resolve(),
        environment=args.environment,
        namespace=args.namespace,
        max_age_hours=args.max_age_hours,
    )
    if problems:
        for problem in problems:
            sys.stderr.write(f"error: {problem}\n")
        return 1
    sys.stdout.write(f"backup manifest verified: {Path(args.manifest).resolve()}\n")
    return 0


def _doctor(args: argparse.Namespace) -> int:
    try:
        _apps_v1, _net_v1, core_v1, _storage_v1 = _load_clients(args.context)
    except RuntimeError as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 2
    except Exception as exc:
        sys.stderr.write(
            f"error: cannot connect to cluster: {type(exc).__name__}: {exc}\n",
        )
        return 2
    schema = _load_schema(_REPO_ROOT / "config" / "loom-schema.toml")
    report = _doctor_reconcile(schema, core_v1, namespace=args.namespace)
    if report.ok:
        print("[ok] schema reconciliation clean")
        return 0
    for v in report.violations:
        print(f"  [fail] [{v.kind}] {v.entry}: {v.detail}")
    return 1


# ──────────────────────────────────────────────────────────────────────
# up (#76 Phase 3 — orchestrate preflight → render → apply → wait)
# ──────────────────────────────────────────────────────────────────────

# Default deadline for the whole "wait for ready" loop after apply.
# Heuristic: image pull on a cold cluster can take a few minutes;
# Postgres readiness probe is ~5s after init; MinIO is fast. 10 min
# covers cold-start safely. Operators can override via `--timeout`.
_DEFAULT_UP_TIMEOUT_SEC = 600
_DEFAULT_UP_POLL_INTERVAL_SEC = 5.0


@dataclass
class ApplyResult:
    """Outcome of the kubectl apply step. `summary_lines` captures
    kubectl's own per-object reporting (`deployment.apps/loom-service
    configured`) so operators see what changed."""

    returncode: int
    summary_lines: list[str]
    stderr: str


def apply_manifests(
    yaml_text: str,
    namespace: str,
    *,
    context: str | None,
    extra_args: tuple[str, ...] = (),
) -> ApplyResult:
    """Pipe rendered manifests into `kubectl apply -f -`. We shell out
    to kubectl rather than use the python client's apply path because
    kubectl handles server-side-apply + multi-doc YAML + namespace
    auto-creation natively. The result's returncode passes through;
    callers map it to the right exit code.

    `extra_args` lets tests inject `--dry-run=server` etc. without
    cluttering the route signature. Production callers leave it empty.
    """
    import shutil
    import subprocess

    if shutil.which("kubectl") is None:
        raise RuntimeError(
            "kubectl is required for `loom cluster up`. install from "
            "https://kubernetes.io/docs/tasks/tools/ and ensure it's "
            "on PATH.",
        )

    cmd: list[str] = ["kubectl", "apply", "-n", namespace, "-f", "-"]
    if context:
        cmd.extend(["--context", context])
    cmd.extend(extra_args)

    proc = subprocess.run(
        cmd,
        input=yaml_text,
        capture_output=True,
        text=True,
        check=False,
    )
    summary_lines = [line for line in proc.stdout.splitlines() if line.strip()]
    return ApplyResult(
        returncode=proc.returncode,
        summary_lines=summary_lines,
        stderr=proc.stderr,
    )


def wait_for_ready(
    apps_v1: Any,
    networking_v1: Any,
    core_v1: Any,
    namespace: str,
    *,
    context: str | None,
    timeout_sec: int = _DEFAULT_UP_TIMEOUT_SEC,
    poll_interval_sec: float = _DEFAULT_UP_POLL_INTERVAL_SEC,
    _sleep: Any = None,
    _now: Any = None,
) -> ClusterStatus:
    """Poll `collect_status` until every component is healthy or the
    deadline expires. Returns the final ClusterStatus regardless —
    callers inspect `all_ready` to decide the exit code.

    `_sleep` + `_now` are test seams. Production passes
    `time.sleep` / `time.monotonic`.
    """
    import time

    sleep_fn = _sleep if _sleep is not None else time.sleep
    now_fn = _now if _now is not None else time.monotonic

    deadline = now_fn() + timeout_sec
    while True:
        status = collect_status(
            apps_v1,
            networking_v1,
            core_v1,
            namespace,
            context=context,
        )
        if status.all_ready:
            return status
        if now_fn() >= deadline:
            return status
        sleep_fn(poll_interval_sec)


def _up(args: argparse.Namespace) -> int:
    try:
        apps_v1, net_v1, core_v1, storage_v1 = _load_clients(args.context)
    except RuntimeError as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 2
    except Exception as exc:
        sys.stderr.write(
            f"error: cannot connect to cluster: {type(exc).__name__}: {exc}\n",
        )
        return 2

    try:
        cfg_path = Path(args.config).resolve() if args.config else None
        config = load_cluster_config(cfg_path)
    except (FileNotFoundError, ValueError) as exc:
        sys.stderr.write(f"error: render failed: {exc}\n")
        return 2

    try:
        manifests = render_manifests(config)
    except (FileNotFoundError, ValueError, RuntimeError) as exc:
        sys.stderr.write(f"error: render failed: {exc}\n")
        return 2

    # 1. Preflight
    if not args.skip_preflight:
        try:
            effective_context = _effective_kube_context(args.context)
            kind_node_mounts = _read_kind_node_mounts(effective_context)
            report = collect_preflight(
                core_v1,
                net_v1,
                storage_v1,
                args.namespace,
                context=effective_context,
                environment=args.environment,
                backup_manifest=(
                    Path(args.backup_manifest).resolve()
                    if args.backup_manifest else None
                ),
                backup_max_age_hours=args.backup_max_age_hours,
                cluster_config=config,
                kind_node_mounts=kind_node_mounts,
            )
            _append_target_schema_doctor_check(
                report,
                core_v1=core_v1,
                namespace=args.namespace,
                config=config,
                rendered_manifests=manifests,
            )
        except Exception as exc:
            sys.stderr.write(
                f"error: preflight failed: {type(exc).__name__}: {exc}\n",
            )
            return 2
        if report.any_fail:
            sys.stderr.write(
                "error: preflight checks failed — refusing to apply. "
                "Re-run with `loom cluster preflight` to see details, "
                "or pass `--skip-preflight` if you know what you're "
                "doing.\n",
            )
            sys.stderr.write(_format_preflight_table(report))
            return 1
        sys.stdout.write("Preflight: all checks passed.\n")

    # 2. Render
    # 3. Apply
    try:
        result = apply_manifests(
            manifests,
            args.namespace,
            context=args.context,
        )
    except RuntimeError as exc:
        # kubectl missing.
        sys.stderr.write(f"error: {exc}\n")
        return 2
    if result.returncode != 0:
        sys.stderr.write(
            f"error: kubectl apply failed (exit {result.returncode}):\n{result.stderr}\n",
        )
        return 1
    for line in result.summary_lines:
        sys.stdout.write(f"  {line}\n")

    if args.no_wait:
        sys.stdout.write(
            "Applied. Skipping readiness wait (--no-wait).\n",
        )
        return 0

    # 4. Wait for ready
    sys.stdout.write(
        f"Waiting up to {args.timeout}s for components to become ready...\n",
    )
    final = wait_for_ready(
        apps_v1,
        net_v1,
        core_v1,
        args.namespace,
        context=args.context,
        timeout_sec=args.timeout,
        poll_interval_sec=args.poll_interval,
    )
    sys.stdout.write(_format_table(final))
    if final.all_ready:
        return 0
    sys.stderr.write(
        f"error: components did not reach ready state within {args.timeout}s.\n",
    )
    return 1


@dataclass
class DeleteResult:
    """Outcome of `kubectl delete -f -`. `summary_lines` carries
    kubectl's own per-object reporting; `--ignore-not-found` keeps
    re-runs after a partial delete idempotent."""

    returncode: int
    summary_lines: list[str]
    stderr: str


def delete_manifests(
    yaml_text: str,
    namespace: str,
    *,
    context: str | None,
    extra_args: tuple[str, ...] = (),
) -> DeleteResult:
    """Pipe rendered manifests into
    `kubectl delete -f - --ignore-not-found`. Mirrors `apply_manifests`
    so operators see symmetric output. `--ignore-not-found` lets a
    second `down` run finish cleanly after a previous teardown
    partially completed."""
    import shutil
    import subprocess

    if shutil.which("kubectl") is None:
        raise RuntimeError(
            "kubectl is required for `loom cluster down`. install from "
            "https://kubernetes.io/docs/tasks/tools/ and ensure it's "
            "on PATH.",
        )

    cmd: list[str] = [
        "kubectl",
        "delete",
        "-n",
        namespace,
        "-f",
        "-",
        "--ignore-not-found",
    ]
    if context:
        cmd.extend(["--context", context])
    cmd.extend(extra_args)

    proc = subprocess.run(
        cmd,
        input=yaml_text,
        capture_output=True,
        text=True,
        check=False,
    )
    summary_lines = [line for line in proc.stdout.splitlines() if line.strip()]
    return DeleteResult(
        returncode=proc.returncode,
        summary_lines=summary_lines,
        stderr=proc.stderr,
    )


@dataclass
class PVCDeleteResult:
    """Outcome of `delete_pvcs`. Split so the caller can report what
    succeeded even when something later in the list fails — operators
    need to know whether postgres got wiped before minio failed."""

    deleted: list[str]
    failed: list[tuple[str, str]]  # (name, error-message)


def delete_pvcs(
    core_v1: Any,
    namespace: str,
) -> PVCDeleteResult:
    """Delete every PVC in the namespace. StatefulSet
    volumeClaimTemplates create PVCs (`data-loom-postgres-0`, etc.)
    that survive StatefulSet deletion — operators have to drop them
    explicitly to reclaim disk.

    Per-PVC errors are caught so a failure on one PVC doesn't hide
    what was actually wiped before it. The caller decides whether
    `failed` is fatal (it usually is — partial-wipe state is the
    operator's problem to resolve)."""
    pvcs = core_v1.list_namespaced_persistent_volume_claim(
        namespace=namespace,
    )
    deleted: list[str] = []
    failed: list[tuple[str, str]] = []
    for pvc in pvcs.items:
        name = pvc.metadata.name
        try:
            core_v1.delete_namespaced_persistent_volume_claim(
                name=name,
                namespace=namespace,
            )
            deleted.append(name)
        except Exception as exc:
            # Catch broadly: the k8s client can raise ApiException,
            # connection errors, or programming bugs in the underlying
            # client lib. The caller surfaces (name, repr) to the
            # operator either way.
            failed.append((name, f"{type(exc).__name__}: {exc}"))
    return PVCDeleteResult(deleted=deleted, failed=failed)


def delete_namespace_resource(
    core_v1: Any,
    namespace: str,
) -> None:
    """Delete the namespace itself. Cascades to every resource in it,
    including any objects not produced by `render_manifests` (operator
    one-offs, ad-hoc Secrets). Use with `--delete-namespace` when the
    operator wants the slate fully clean."""
    core_v1.delete_namespace(name=namespace)


def _guard_protected_destructive_down(args: argparse.Namespace) -> int | None:
    if not (args.with_volumes or args.delete_namespace):
        return None
    if not is_protected_environment(
        environment=args.environment,
        namespace=args.namespace,
    ):
        return None
    environment = infer_environment(
        environment=args.environment,
        namespace=args.namespace,
    )
    manifest = (
        Path(args.backup_manifest).resolve()
        if args.backup_manifest else None
    )
    problems = validate_backup_manifest(
        manifest,
        environment=environment,
        namespace=args.namespace,
        max_age_hours=args.backup_max_age_hours,
    )
    if problems:
        sys.stderr.write(
            f"error: refusing destructive operation in protected "
            f"environment {environment!r}; backup manifest is not valid:\n",
        )
        for problem in problems:
            sys.stderr.write(f"  - {problem}\n")
        return 1
    if args.acknowledge_data_loss != environment:
        sys.stderr.write(
            "error: destructive protected-environment operation requires "
            f"`--acknowledge-data-loss {environment}` after verifying restore "
            "readiness.\n",
        )
        return 1
    return None


def _down(args: argparse.Namespace) -> int:
    guard_result = _guard_protected_destructive_down(args)
    if guard_result is not None:
        return guard_result

    try:
        _, _, core_v1, _ = _load_clients(args.context)
    except RuntimeError as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 2
    except Exception as exc:
        sys.stderr.write(
            f"error: cannot connect to cluster: {type(exc).__name__}: {exc}\n",
        )
        return 2

    # 1. Render manifests so we know what to delete. Doing this from
    # the same config keeps `up` and `down` symmetric — if the
    # operator changed cluster-config.toml between the two calls,
    # `down` only removes objects the *current* config would have
    # produced. The `--ignore-not-found` flag on kubectl forgives
    # objects that have already been deleted manually.
    try:
        cfg_path = Path(args.config).resolve() if args.config else None
        config = load_cluster_config(cfg_path)
        manifests = render_manifests(config)
    except (FileNotFoundError, ValueError, RuntimeError) as exc:
        sys.stderr.write(f"error: render failed: {exc}\n")
        return 2

    # 2. Confirm. Teardown is destructive; require explicit `--yes`
    # or an interactive y/N prompt before touching anything.
    if not args.yes:
        prompt = f"This will delete Loom resources in namespace '{args.namespace}'"
        if args.with_volumes:
            prompt += " AND its PersistentVolumeClaims (data loss)"
        if args.delete_namespace:
            prompt += f" AND the '{args.namespace}' namespace itself"
        prompt += ". Continue? [y/N]: "
        sys.stdout.write(prompt)
        sys.stdout.flush()
        try:
            reply = sys.stdin.readline().strip().lower()
        except (KeyboardInterrupt, EOFError):
            # EOFError covers `stdin=None` edge cases; closed-pipe
            # stdin (the common CI case without --yes) returns "" from
            # readline() which falls through to the abort path below.
            sys.stdout.write("\naborted.\n")
            return 1
        if reply not in ("y", "yes"):
            sys.stdout.write("aborted.\n")
            return 1

    # 3. Delete manifests.
    try:
        result = delete_manifests(
            manifests,
            args.namespace,
            context=args.context,
        )
    except RuntimeError as exc:
        # kubectl missing.
        sys.stderr.write(f"error: {exc}\n")
        return 2
    if result.returncode != 0:
        sys.stderr.write(
            f"error: kubectl delete failed (exit {result.returncode}):\n{result.stderr}\n",
        )
        return 1
    for line in result.summary_lines:
        sys.stdout.write(f"  {line}\n")

    # 4. Optional volume teardown.
    if args.with_volumes:
        try:
            pvc_result = delete_pvcs(core_v1, args.namespace)
        except Exception as exc:
            # The initial `list_namespaced_persistent_volume_claim` call
            # failed — we never got to per-PVC deletes. Per-PVC failures
            # are reported below via pvc_result.failed.
            sys.stderr.write(
                f"error: failed to list PVCs in namespace "
                f"'{args.namespace}': {type(exc).__name__}: {exc}\n",
            )
            return 1
        for name in pvc_result.deleted:
            sys.stdout.write(f"  persistentvolumeclaim/{name} deleted\n")
        for name, err in pvc_result.failed:
            sys.stderr.write(
                f"  persistentvolumeclaim/{name} FAILED: {err}\n",
            )
        if not pvc_result.deleted and not pvc_result.failed:
            sys.stdout.write(
                f"  (no PVCs found in namespace '{args.namespace}')\n",
            )
        if pvc_result.failed:
            sys.stderr.write(
                f"error: {len(pvc_result.failed)} PVC(s) failed to "
                f"delete; namespace '{args.namespace}' is now in a "
                f"partial-wipe state.\n",
            )
            return 1

    # 5. Optional namespace teardown.
    if args.delete_namespace:
        try:
            delete_namespace_resource(core_v1, args.namespace)
        except Exception as exc:
            sys.stderr.write(
                f"error: failed to delete namespace: {type(exc).__name__}: {exc}\n",
            )
            return 1
        sys.stdout.write(f"  namespace/{args.namespace} deleted\n")

    sys.stdout.write("Cluster down: complete.\n")
    return 0


def _bootstrap_secrets(args: argparse.Namespace) -> int:
    from loom_config.bootstrap import render_bootstrap_command
    schema = _load_schema(_REPO_ROOT / "config" / "loom-schema.toml")
    print(render_bootstrap_command(
        schema,
        namespace=args.namespace,
        smoke_defaults=args.smoke_defaults,
        rotate=args.rotate,
    ))
    return 0


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
        help=("Show component readiness + ingress endpoints for the configured Loom namespace."),
    )
    p_status.add_argument(
        "--context",
        default=None,
        help="kubeconfig context (default: current context).",
    )
    p_status.add_argument(
        "--namespace",
        default="loom",
        help="Kubernetes namespace (default: loom).",
    )
    p_status.add_argument(
        "--format",
        choices=["table", "json"],
        default="table",
        help="Output format. JSON for CI/scripting.",
    )
    p_status.set_defaults(handler=_status)

    p_render = sub.add_parser(
        "render",
        help=(
            "Render Kubernetes manifests to stdout from a "
            "cluster-config.toml. Apply with `kubectl apply -f -`."
        ),
    )
    p_render.add_argument(
        "--config",
        default=None,
        help=(
            "Path to cluster-config.toml. Omit for all defaults "
            "(produces the same shape as deploy/k8s/*.yaml)."
        ),
    )
    p_render.set_defaults(handler=_render)

    p_preflight = sub.add_parser(
        "preflight",
        help=(
            "Verify the target cluster meets prerequisites: required "
            "Secrets, IngressClass installed, default StorageClass, "
            "PSS labels. Exits 0 on all-pass (warns allowed), 1 on "
            "any-fail, 2 on cluster unreachable."
        ),
    )
    p_preflight.add_argument(
        "--context",
        default=None,
        help="kubeconfig context (default: current context).",
    )
    p_preflight.add_argument(
        "--namespace",
        default="loom",
        help="Kubernetes namespace (default: loom).",
    )
    p_preflight.add_argument(
        "--environment",
        default=None,
        help=(
            "Logical environment name. Protected environments "
            "(public-beta/staging/production) get storage and backup "
            "guard checks. If omitted, inferred from namespace when possible."
        ),
    )
    p_preflight.add_argument(
        "--config",
        default=None,
        help=(
            "Path to cluster-config.toml. Protected preflight uses this "
            "to recognize static host-path Retain PVs before first apply."
        ),
    )
    p_preflight.add_argument(
        "--backup-manifest",
        default=None,
        help=(
            "Path to a recent `loom cluster backup manifest` JSON file "
            "for protected environment checks."
        ),
    )
    p_preflight.add_argument(
        "--backup-max-age-hours",
        type=int,
        default=DEFAULT_BACKUP_MAX_AGE_HOURS,
        help=(
            "Maximum accepted backup manifest age for protected "
            f"environments (default: {DEFAULT_BACKUP_MAX_AGE_HOURS})."
        ),
    )
    p_preflight.add_argument(
        "--format",
        choices=["table", "json"],
        default="table",
        help="Output format. JSON for CI/scripting.",
    )
    p_preflight.add_argument(
        "--no-doctor",
        dest="no_doctor",
        action="store_true",
        help=(
            "Skip schema-vs-cluster reconciliation (use when applying "
            "to an empty cluster where loom-secrets does not yet exist)."
        ),
    )
    p_preflight.set_defaults(handler=_preflight)

    p_backup = sub.add_parser(
        "backup",
        help=(
            "Create or verify metadata manifests for protected "
            "public-beta/staging backups."
        ),
    )
    backup_sub = p_backup.add_subparsers(dest="backup_cmd", required=True)
    p_backup_manifest = backup_sub.add_parser(
        "manifest",
        help=(
            "Write a metadata-only manifest after Postgres, MinIO, and "
            "Kubernetes/runtime secrets have been backed up."
        ),
    )
    p_backup_manifest.add_argument("--environment", required=True)
    p_backup_manifest.add_argument("--namespace", required=True)
    p_backup_manifest.add_argument("--output", required=True)
    p_backup_manifest.add_argument("--postgres-dump", required=True)
    p_backup_manifest.add_argument("--minio-snapshot", required=True)
    p_backup_manifest.add_argument("--k8s-secrets", required=True)
    p_backup_manifest.set_defaults(handler=_backup_manifest)

    p_backup_check = backup_sub.add_parser(
        "check",
        help="Verify a backup manifest is recent and complete.",
    )
    p_backup_check.add_argument("--environment", required=True)
    p_backup_check.add_argument("--namespace", required=True)
    p_backup_check.add_argument("--manifest", required=True)
    p_backup_check.add_argument(
        "--max-age-hours",
        type=int,
        default=DEFAULT_BACKUP_MAX_AGE_HOURS,
    )
    p_backup_check.set_defaults(handler=_backup_check)

    p_up = sub.add_parser(
        "up",
        help=(
            "Bring a Loom cluster up: preflight → render → kubectl "
            "apply → wait for components to become ready. Composes "
            "the read-only `preflight`, `render`, and `status` "
            "commands."
        ),
    )
    p_up.add_argument(
        "--context",
        default=None,
        help="kubeconfig context (default: current context).",
    )
    p_up.add_argument(
        "--namespace",
        default="loom",
        help="Kubernetes namespace (default: loom).",
    )
    p_up.add_argument(
        "--environment",
        default=None,
        help=(
            "Logical environment name passed through to preflight. "
            "Protected environments can require backup manifest checks."
        ),
    )
    p_up.add_argument(
        "--backup-manifest",
        default=None,
        help=(
            "Recent `loom cluster backup manifest` JSON file passed "
            "through to preflight for protected environments."
        ),
    )
    p_up.add_argument(
        "--backup-max-age-hours",
        type=int,
        default=DEFAULT_BACKUP_MAX_AGE_HOURS,
        help=(
            "Maximum accepted backup manifest age during preflight "
            f"(default: {DEFAULT_BACKUP_MAX_AGE_HOURS})."
        ),
    )
    p_up.add_argument(
        "--config",
        default=None,
        help=(
            "Path to cluster-config.toml. Omit for all defaults (see `loom cluster render --help`)."
        ),
    )
    p_up.add_argument(
        "--skip-preflight",
        dest="skip_preflight",
        action="store_true",
        help=(
            "Skip the preflight checks. Use sparingly — usually "
            "intended for re-applying after a known transient "
            "preflight failure."
        ),
    )
    p_up.add_argument(
        "--no-wait",
        dest="no_wait",
        action="store_true",
        help=(
            "Apply manifests and return immediately without waiting "
            "for components to reach ready state."
        ),
    )
    p_up.add_argument(
        "--timeout",
        type=int,
        default=_DEFAULT_UP_TIMEOUT_SEC,
        help=(f"Wait timeout in seconds (default: {_DEFAULT_UP_TIMEOUT_SEC})."),
    )
    p_up.add_argument(
        "--poll-interval",
        dest="poll_interval",
        type=float,
        default=_DEFAULT_UP_POLL_INTERVAL_SEC,
        help=(
            f"Poll interval in seconds during the readiness wait "
            f"(default: {_DEFAULT_UP_POLL_INTERVAL_SEC})."
        ),
    )
    p_up.set_defaults(handler=_up)

    p_down = sub.add_parser(
        "down",
        help=(
            "Tear down a Loom cluster: kubectl delete of the rendered "
            "manifests. PVCs and the namespace itself are preserved "
            "unless `--with-volumes` / `--delete-namespace` are passed."
        ),
    )
    p_down.add_argument(
        "--context",
        default=None,
        help="kubeconfig context (default: current context).",
    )
    p_down.add_argument(
        "--namespace",
        default="loom",
        help="Kubernetes namespace (default: loom).",
    )
    p_down.add_argument(
        "--environment",
        default=None,
        help=(
            "Logical environment name. Protected environments "
            "(public-beta/staging/production) require a recent backup "
            "manifest and acknowledgement before --with-volumes or "
            "--delete-namespace."
        ),
    )
    p_down.add_argument(
        "--config",
        default=None,
        help=(
            "Path to cluster-config.toml. Must match the config used "
            "for `loom cluster up`; resources outside the rendered set "
            "are not touched (use --delete-namespace to nuke them all)."
        ),
    )
    p_down.add_argument(
        "--yes",
        "-y",
        dest="yes",
        action="store_true",
        help=(
            "Skip the destructive-action confirmation prompt. Intended "
            "for CI/scripted teardowns; production operators should "
            "leave the prompt on."
        ),
    )
    p_down.add_argument(
        "--with-volumes",
        dest="with_volumes",
        action="store_true",
        help=(
            "Also delete PersistentVolumeClaims in the namespace. "
            "StatefulSet PVCs survive normal teardown; pass this when "
            "you want to wipe the database + object store too. "
            "DESTRUCTIVE — data is unrecoverable."
        ),
    )
    p_down.add_argument(
        "--backup-manifest",
        default=None,
        help=(
            "Recent verified backup manifest required before destructive "
            "protected-environment operations."
        ),
    )
    p_down.add_argument(
        "--backup-max-age-hours",
        type=int,
        default=DEFAULT_BACKUP_MAX_AGE_HOURS,
        help=(
            "Maximum accepted backup manifest age for destructive protected "
            f"operations (default: {DEFAULT_BACKUP_MAX_AGE_HOURS})."
        ),
    )
    p_down.add_argument(
        "--acknowledge-data-loss",
        default=None,
        metavar="ENVIRONMENT",
        help=(
            "Explicit acknowledgement required for protected "
            "--with-volumes/--delete-namespace operations. Value must match "
            "the logical environment, for example `public-beta`."
        ),
    )
    p_down.add_argument(
        "--delete-namespace",
        dest="delete_namespace",
        action="store_true",
        help=(
            "Also delete the namespace. Cascades to every resource in "
            "it, including objects not produced by `cluster render`."
        ),
    )
    p_down.set_defaults(handler=_down)

    p_audit = sub.add_parser(
        "audit",
        help=(
            "Render manifests and check the public/internal boundary: "
            "no LoadBalancer/NodePort Services, no Ingress backends "
            "outside the allowlist, TLS on public Ingress, explicit "
            "Web/API paths, no unsafe hostPort declarations. Exits "
            "0 on clean, 1 on any violation."
        ),
    )
    p_audit.add_argument(
        "--config",
        default=None,
        help=(
            "Path to cluster-config.toml. Omit for all defaults. "
            "Only the Web/API ingress is public; the LLM gateway "
            "remains internal-only."
        ),
    )
    p_audit.set_defaults(handler=_audit)

    p_doctor = sub.add_parser(
        "doctor",
        help="Reconcile config schema against a live cluster.",
    )
    p_doctor.add_argument(
        "--namespace",
        default="loom",
        help="Kubernetes namespace (default: loom).",
    )
    p_doctor.add_argument(
        "--context",
        default=None,
        help="kubeconfig context (default: current context).",
    )
    p_doctor.set_defaults(handler=_doctor)

    p_boot = sub.add_parser(
        "bootstrap-secrets",
        help="Emit the kubectl command to create loom-secrets from schema.",
    )
    p_boot.add_argument("--namespace", default="loom")
    p_boot.add_argument(
        "--smoke-defaults",
        action="store_true",
        help="Use test-grade placeholder values (for smoke workflows).",
    )
    p_boot.add_argument(
        "--rotate",
        action="store_true",
        help="Run each entry's `generate` command to mint fresh values.",
    )
    p_boot.set_defaults(handler=_bootstrap_secrets)

    args = parser.parse_args(argv)
    return cast(int, args.handler(args))
