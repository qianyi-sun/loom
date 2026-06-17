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
from importlib import resources
from pathlib import Path
from typing import Any, cast

from loom_cli.cluster_config import ClusterConfig, load_cluster_config

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


# ──────────────────────────────────────────────────────────────────────
# render (#76 Phase 1B)
# ──────────────────────────────────────────────────────────────────────


# Order matters: Postgres + MinIO come before the workloads that
# depend on them so `kubectl apply -f - < render` brings the cluster
# up in a topologically sound order. Ingress is last so it doesn't
# accept traffic until the backing Services exist.
_TEMPLATE_ORDER: tuple[str, ...] = (
    "postgres.yaml.j2",
    "minio.yaml.j2",
    "control-plane.yaml.j2",
    "loom-service.yaml.j2",
    "llm-gateway.yaml.j2",
    "worker.yaml.j2",
    "web.yaml.j2",
    "ingress.yaml.j2",
)


def render_manifests(config: ClusterConfig) -> str:
    """Render every template and join with `---` separators. Output is
    valid YAML that can be piped directly into `kubectl apply -f -`.

    Templates are loaded from `loom_cli.templates.k8s` via the
    `importlib.resources` API so packaging (sdist + wheel) picks them
    up without needing a separate MANIFEST.in entry.
    """
    try:
        from jinja2 import Environment, StrictUndefined
    except ModuleNotFoundError as exc:
        # jinja2 is a core dep in pyproject.toml; this should never
        # fire in a correctly-installed environment but the error
        # message helps developers running a partial install.
        raise RuntimeError(
            "the 'jinja2' package is required for `loom cluster render`. "
            "if you installed Loom in development mode, run `uv sync` "
            "(or `pip install -e .`) to pick up dependencies.",
        ) from exc

    env = Environment(
        # StrictUndefined makes a missing variable error LOUDLY instead
        # of rendering an empty string. Better to fail at render time
        # than silently emit a manifest with `image: loom-service:`
        # (no tag) that operators chase for an hour.
        undefined=StrictUndefined,
        keep_trailing_newline=True,
        trim_blocks=False,
        lstrip_blocks=False,
    )
    ctx = config.to_render_context()
    chunks: list[str] = []
    pkg = resources.files("loom_cli.templates.k8s")
    for name in _TEMPLATE_ORDER:
        template_text = (pkg / name).read_text(encoding="utf-8")
        rendered = env.from_string(template_text).render(**ctx)
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
    except RuntimeError as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 2
    sys.stdout.write(manifests)
    return 0


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
    core_v1: Any, namespace: str,
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
                f"kubectl create namespace {namespace}\n"
                f"# Then re-run `loom cluster preflight`."
            ),
        )
    return PreflightCheck(
        name="namespace-exists",
        outcome="pass",
        detail=f"namespace {namespace!r} present",
    )


def _check_required_secrets(
    core_v1: Any, namespace: str,
) -> list[PreflightCheck]:
    """One check per Secret listed in `_REQUIRED_SECRETS`. Missing
    Secret = fail with a `kubectl create secret` hint."""
    out: list[PreflightCheck] = []
    for name in _REQUIRED_SECRETS:
        if _secret_present(core_v1, namespace, name):
            out.append(PreflightCheck(
                name=f"secret-{name}",
                outcome="pass",
                detail=f"Secret {name!r} present in {namespace}",
            ))
        else:
            out.append(PreflightCheck(
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
            ))
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
            detail=(
                f"failed to list IngressClass resources: "
                f"{_exception_to_note(exc)}"
            ),
            remediation=(
                "Check kubectl + cluster networking access; "
                "see cluster-deploy.md §Prerequisites."
            ),
        )
    classes = [
        getattr(it.metadata, "name", "<unknown>")
        for it in (result.items or [])
    ]
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
            detail=(
                f"failed to list StorageClass resources: "
                f"{_exception_to_note(exc)}"
            ),
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
        anns = (getattr(sc.metadata, "annotations", None) or {})
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


def _check_pss_enforce(
    core_v1: Any, namespace: str,
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
            detail=(
                f"namespace {namespace!r} has no PSS enforce label "
                "(no admission restriction)"
            ),
        )
    return PreflightCheck(
        name="pss-enforce",
        outcome="pass",
        detail=(
            f"namespace {namespace!r} PSS enforce={enforce!r} "
            "(non-restricted)"
        ),
    )


def collect_preflight(
    core_v1: Any, networking_v1: Any, storage_v1: Any, namespace: str,
    *, context: str | None,
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

    if ns_check.outcome == "pass":
        checks.extend(_check_required_secrets(core_v1, namespace))
        checks.append(_check_pss_enforce(core_v1, namespace))

    return PreflightReport(
        namespace=namespace, context=context, checks=checks,
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


def _preflight(args: argparse.Namespace) -> int:
    try:
        _apps_v1, net_v1, core_v1, storage_v1 = _load_clients(args.context)
    except RuntimeError as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 2
    except Exception as exc:
        sys.stderr.write(
            f"error: cannot connect to cluster: "
            f"{type(exc).__name__}: {exc}\n",
        )
        return 2
    try:
        report = collect_preflight(
            core_v1, net_v1, storage_v1, args.namespace,
            context=args.context,
        )
    except Exception as exc:
        sys.stderr.write(
            f"error: failed to read cluster state: "
            f"{type(exc).__name__}: {exc}\n",
        )
        return 2
    if args.format == "json":
        sys.stdout.write(_format_preflight_json(report))
    else:
        sys.stdout.write(_format_preflight_table(report))
    # Exit 1 only when something explicitly failed; warns alone keep
    # exit 0 so CI scripts don't have to special-case them.
    return 1 if report.any_fail else 0


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
    yaml_text: str, namespace: str, *, context: str | None,
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
        cmd, input=yaml_text, capture_output=True, text=True, check=False,
    )
    summary_lines = [
        line for line in proc.stdout.splitlines() if line.strip()
    ]
    return ApplyResult(
        returncode=proc.returncode,
        summary_lines=summary_lines,
        stderr=proc.stderr,
    )


def wait_for_ready(
    apps_v1: Any, networking_v1: Any, core_v1: Any,
    namespace: str, *, context: str | None,
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
            apps_v1, networking_v1, core_v1, namespace, context=context,
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
            f"error: cannot connect to cluster: "
            f"{type(exc).__name__}: {exc}\n",
        )
        return 2

    # 1. Preflight
    if not args.skip_preflight:
        try:
            report = collect_preflight(
                core_v1, net_v1, storage_v1, args.namespace,
                context=args.context,
            )
        except Exception as exc:
            sys.stderr.write(
                f"error: preflight failed: "
                f"{type(exc).__name__}: {exc}\n",
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
    try:
        cfg_path = Path(args.config).resolve() if args.config else None
        config = load_cluster_config(cfg_path)
        manifests = render_manifests(config)
    except (FileNotFoundError, ValueError, RuntimeError) as exc:
        sys.stderr.write(f"error: render failed: {exc}\n")
        return 2

    # 3. Apply
    try:
        result = apply_manifests(
            manifests, args.namespace, context=args.context,
        )
    except RuntimeError as exc:
        # kubectl missing.
        sys.stderr.write(f"error: {exc}\n")
        return 2
    if result.returncode != 0:
        sys.stderr.write(
            f"error: kubectl apply failed (exit {result.returncode}):\n"
            f"{result.stderr}\n",
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
        f"Waiting up to {args.timeout}s for components to become "
        f"ready...\n",
    )
    final = wait_for_ready(
        apps_v1, net_v1, core_v1, args.namespace,
        context=args.context, timeout_sec=args.timeout,
        poll_interval_sec=args.poll_interval,
    )
    sys.stdout.write(_format_table(final))
    if final.all_ready:
        return 0
    sys.stderr.write(
        f"error: components did not reach ready state within "
        f"{args.timeout}s.\n",
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
    yaml_text: str, namespace: str, *, context: str | None,
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
        "kubectl", "delete", "-n", namespace, "-f", "-",
        "--ignore-not-found",
    ]
    if context:
        cmd.extend(["--context", context])
    cmd.extend(extra_args)

    proc = subprocess.run(
        cmd, input=yaml_text, capture_output=True, text=True, check=False,
    )
    summary_lines = [
        line for line in proc.stdout.splitlines() if line.strip()
    ]
    return DeleteResult(
        returncode=proc.returncode,
        summary_lines=summary_lines,
        stderr=proc.stderr,
    )


def delete_pvcs(
    core_v1: Any, namespace: str,
) -> list[str]:
    """Delete every PVC in the namespace. StatefulSet
    volumeClaimTemplates create PVCs (`data-loom-postgres-0`, etc.)
    that survive StatefulSet deletion — operators have to drop them
    explicitly to reclaim disk. Returns the list of deleted PVC
    names so the caller can report what got wiped."""
    pvcs = core_v1.list_namespaced_persistent_volume_claim(
        namespace=namespace,
    )
    deleted: list[str] = []
    for pvc in pvcs.items:
        name = pvc.metadata.name
        core_v1.delete_namespaced_persistent_volume_claim(
            name=name, namespace=namespace,
        )
        deleted.append(name)
    return deleted


def delete_namespace_resource(
    core_v1: Any, namespace: str,
) -> None:
    """Delete the namespace itself. Cascades to every resource in it,
    including any objects not produced by `render_manifests` (operator
    one-offs, ad-hoc Secrets). Use with `--delete-namespace` when the
    operator wants the slate fully clean."""
    core_v1.delete_namespace(name=namespace)


def _down(args: argparse.Namespace) -> int:
    try:
        _, _, core_v1, _ = _load_clients(args.context)
    except RuntimeError as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 2
    except Exception as exc:
        sys.stderr.write(
            f"error: cannot connect to cluster: "
            f"{type(exc).__name__}: {exc}\n",
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
        prompt = (
            f"This will delete Loom resources in namespace "
            f"'{args.namespace}'"
        )
        if args.with_volumes:
            prompt += " AND its PersistentVolumeClaims (data loss)"
        if args.delete_namespace:
            prompt += f" AND the '{args.namespace}' namespace itself"
        prompt += ". Continue? [y/N]: "
        sys.stdout.write(prompt)
        sys.stdout.flush()
        try:
            reply = sys.stdin.readline().strip().lower()
        except KeyboardInterrupt:
            sys.stdout.write("\naborted.\n")
            return 1
        if reply not in ("y", "yes"):
            sys.stdout.write("aborted.\n")
            return 1

    # 3. Delete manifests.
    try:
        result = delete_manifests(
            manifests, args.namespace, context=args.context,
        )
    except RuntimeError as exc:
        # kubectl missing.
        sys.stderr.write(f"error: {exc}\n")
        return 2
    if result.returncode != 0:
        sys.stderr.write(
            f"error: kubectl delete failed (exit {result.returncode}):\n"
            f"{result.stderr}\n",
        )
        return 1
    for line in result.summary_lines:
        sys.stdout.write(f"  {line}\n")

    # 4. Optional volume teardown.
    if args.with_volumes:
        try:
            deleted_pvcs = delete_pvcs(core_v1, args.namespace)
        except Exception as exc:
            sys.stderr.write(
                f"error: failed to delete PVCs: "
                f"{type(exc).__name__}: {exc}\n",
            )
            return 1
        for name in deleted_pvcs:
            sys.stdout.write(f"  persistentvolumeclaim/{name} deleted\n")
        if not deleted_pvcs:
            sys.stdout.write(
                f"  (no PVCs found in namespace '{args.namespace}')\n",
            )

    # 5. Optional namespace teardown.
    if args.delete_namespace:
        try:
            delete_namespace_resource(core_v1, args.namespace)
        except Exception as exc:
            sys.stderr.write(
                f"error: failed to delete namespace: "
                f"{type(exc).__name__}: {exc}\n",
            )
            return 1
        sys.stdout.write(f"  namespace/{args.namespace} deleted\n")

    sys.stdout.write("Cluster down: complete.\n")
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

    p_render = sub.add_parser(
        "render",
        help=(
            "Render Kubernetes manifests to stdout from a "
            "cluster-config.toml. Apply with `kubectl apply -f -`."
        ),
    )
    p_render.add_argument(
        "--config", default=None,
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
        "--context", default=None,
        help="kubeconfig context (default: current context).",
    )
    p_preflight.add_argument(
        "--namespace", default="loom",
        help="Kubernetes namespace (default: loom).",
    )
    p_preflight.add_argument(
        "--format", choices=["table", "json"], default="table",
        help="Output format. JSON for CI/scripting.",
    )
    p_preflight.set_defaults(handler=_preflight)

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
        "--context", default=None,
        help="kubeconfig context (default: current context).",
    )
    p_up.add_argument(
        "--namespace", default="loom",
        help="Kubernetes namespace (default: loom).",
    )
    p_up.add_argument(
        "--config", default=None,
        help=(
            "Path to cluster-config.toml. Omit for all defaults "
            "(see `loom cluster render --help`)."
        ),
    )
    p_up.add_argument(
        "--skip-preflight", dest="skip_preflight", action="store_true",
        help=(
            "Skip the preflight checks. Use sparingly — usually "
            "intended for re-applying after a known transient "
            "preflight failure."
        ),
    )
    p_up.add_argument(
        "--no-wait", dest="no_wait", action="store_true",
        help=(
            "Apply manifests and return immediately without waiting "
            "for components to reach ready state."
        ),
    )
    p_up.add_argument(
        "--timeout", type=int, default=_DEFAULT_UP_TIMEOUT_SEC,
        help=(
            f"Wait timeout in seconds "
            f"(default: {_DEFAULT_UP_TIMEOUT_SEC})."
        ),
    )
    p_up.add_argument(
        "--poll-interval", dest="poll_interval", type=float,
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
        "--context", default=None,
        help="kubeconfig context (default: current context).",
    )
    p_down.add_argument(
        "--namespace", default="loom",
        help="Kubernetes namespace (default: loom).",
    )
    p_down.add_argument(
        "--config", default=None,
        help=(
            "Path to cluster-config.toml. Must match the config used "
            "for `loom cluster up`; resources outside the rendered set "
            "are not touched (use --delete-namespace to nuke them all)."
        ),
    )
    p_down.add_argument(
        "--yes", "-y", dest="yes", action="store_true",
        help=(
            "Skip the destructive-action confirmation prompt. Intended "
            "for CI/scripted teardowns; production operators should "
            "leave the prompt on."
        ),
    )
    p_down.add_argument(
        "--with-volumes", dest="with_volumes", action="store_true",
        help=(
            "Also delete PersistentVolumeClaims in the namespace. "
            "StatefulSet PVCs survive normal teardown; pass this when "
            "you want to wipe the database + object store too. "
            "DESTRUCTIVE — data is unrecoverable."
        ),
    )
    p_down.add_argument(
        "--delete-namespace", dest="delete_namespace", action="store_true",
        help=(
            "Also delete the namespace. Cascades to every resource in "
            "it, including objects not produced by `cluster render`."
        ),
    )
    p_down.set_defaults(handler=_down)

    args = parser.parse_args(argv)
    return cast(int, args.handler(args))
