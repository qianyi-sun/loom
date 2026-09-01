"""Single-source kubectl server-side apply contract for staging manifests."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

MANIFEST_FIELD_MANAGER = "loom-staging-rollout"
MANIFEST_REQUEST_TIMEOUT = "60s"
MANIFEST_APPLY_CONTRACT_DIGEST = hashlib.sha256(
    json.dumps(
        {
            "apply": {
                "artifact_namespace_mode": "explicit-per-resource",
                "field_manager": MANIFEST_FIELD_MANAGER,
                "force_conflicts": False,
                "machine_readable_output": True,
                "request_timeout": MANIFEST_REQUEST_TIMEOUT,
                "server_side": True,
                "validate": "strict",
            },
            "diff": {
                "artifact_namespace_mode": "explicit-per-resource",
                "field_manager": MANIFEST_FIELD_MANAGER,
                "force_conflicts": False,
                "request_timeout": MANIFEST_REQUEST_TIMEOUT,
                "server_side": True,
            },
            "schema_validation": {
                "artifact_namespace_mode": "explicit-per-resource",
                "dry_run": "server",
                "field_manager": MANIFEST_FIELD_MANAGER,
                "force_conflicts": True,
                "request_timeout": MANIFEST_REQUEST_TIMEOUT,
                "server_side": True,
                "validate": "strict",
            },
            "version": "v4",
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
).hexdigest()


def server_side_apply_argv(
    namespace: str,
    *,
    kubeconfig: Path | None = None,
    dry_run: bool = False,
    output_json: bool = False,
) -> tuple[str, ...]:
    """Return the exact protected apply command, optionally as a server dry-run."""
    _validate_namespace(namespace)
    return _server_side_apply_argv(
        namespace=namespace,
        kubeconfig=kubeconfig,
        dry_run=dry_run,
        output_json=output_json,
    )


def server_side_all_namespaces_apply_argv(
    *,
    kubeconfig: Path | None = None,
    dry_run: bool = False,
    output_json: bool = False,
) -> tuple[str, ...]:
    """Apply an artifact whose namespaced objects carry admitted explicit namespaces."""

    return _server_side_apply_argv(
        namespace=None,
        kubeconfig=kubeconfig,
        dry_run=dry_run,
        output_json=output_json,
    )


def _server_side_apply_argv(
    *,
    namespace: str | None,
    kubeconfig: Path | None,
    dry_run: bool,
    output_json: bool,
) -> tuple[str, ...]:
    argv = ["kubectl"]
    if kubeconfig is not None:
        if not kubeconfig.is_absolute() or ".." in kubeconfig.parts:
            raise ValueError("manifest kubeconfig path is invalid")
        argv.extend(("--kubeconfig", str(kubeconfig)))
    if namespace is not None:
        argv.extend(("--namespace", namespace))
    argv.extend(("apply", "--server-side=true", f"--field-manager={MANIFEST_FIELD_MANAGER}"))
    if dry_run:
        argv.append("--dry-run=server")
    if output_json:
        argv.extend(("--output", "json"))
    argv.extend(
        (
            "--validate=strict",
            f"--request-timeout={MANIFEST_REQUEST_TIMEOUT}",
            "-f",
            "-",
        )
    )
    return tuple(argv)


def server_side_diff_argv(namespace: str) -> tuple[str, ...]:
    """Return the exact read-only classifier for the protected apply contract."""
    _validate_namespace(namespace)
    return (
        "kubectl",
        "--namespace",
        namespace,
        "diff",
        "--server-side=true",
        f"--field-manager={MANIFEST_FIELD_MANAGER}",
        f"--request-timeout={MANIFEST_REQUEST_TIMEOUT}",
        "-f",
        "-",
    )


def server_side_all_namespaces_diff_argv() -> tuple[str, ...]:
    """Classify an artifact whose namespaced objects carry explicit namespaces."""

    return (
        "kubectl",
        "diff",
        "--server-side=true",
        f"--field-manager={MANIFEST_FIELD_MANAGER}",
        f"--request-timeout={MANIFEST_REQUEST_TIMEOUT}",
        "-f",
        "-",
    )


def server_side_schema_validation_argv(
    namespace: str,
    *,
    kubeconfig: Path | None = None,
) -> tuple[str, ...]:
    """Return a mutation-free schema validation command.

    ``--force-conflicts`` is deliberately confined to a server dry-run.  It
    separates API/schema validation from the independent no-force field-owner
    predicate without creating or changing any Kubernetes object.
    """
    argv = list(server_side_apply_argv(namespace, kubeconfig=kubeconfig, dry_run=True))
    argv.insert(argv.index("--validate=strict"), "--force-conflicts")
    return tuple(argv)


def server_side_all_namespaces_schema_validation_argv(
    *,
    kubeconfig: Path | None = None,
) -> tuple[str, ...]:
    """Server-validate one explicitly namespaced multi-namespace artifact."""

    argv = list(
        server_side_all_namespaces_apply_argv(
            kubeconfig=kubeconfig,
            dry_run=True,
        )
    )
    argv.insert(argv.index("--validate=strict"), "--force-conflicts")
    return tuple(argv)


def _validate_namespace(namespace: str) -> None:
    if not namespace or "\x00" in namespace or namespace.startswith("-") or "/" in namespace:
        raise ValueError("manifest namespace is invalid")


__all__ = [
    "MANIFEST_APPLY_CONTRACT_DIGEST",
    "MANIFEST_FIELD_MANAGER",
    "MANIFEST_REQUEST_TIMEOUT",
    "server_side_all_namespaces_apply_argv",
    "server_side_all_namespaces_diff_argv",
    "server_side_all_namespaces_schema_validation_argv",
    "server_side_apply_argv",
    "server_side_diff_argv",
    "server_side_schema_validation_argv",
]
