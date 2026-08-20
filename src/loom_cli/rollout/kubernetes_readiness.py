"""Single-source, read-only Kubernetes client and target readiness predicate."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

_NAME_RE = re.compile(r"^[a-z0-9](?:[-a-z0-9.]{0,61}[a-z0-9])?$")


class CommandResult(Protocol):
    @property
    def returncode(self) -> int: ...

    @property
    def stdout(self) -> str: ...


CommandRunner = Callable[[Sequence[str]], CommandResult]


@dataclass(frozen=True, slots=True)
class KubernetesClientReadiness:
    current_context: str
    namespace: str
    context_ready: bool
    namespace_ready: bool

    @property
    def ready(self) -> bool:
        return self.context_ready and self.namespace_ready

    @property
    def evidence_digest(self) -> str:
        payload = json.dumps(
            {
                "context_ready": self.context_ready,
                "current_context": self.current_context,
                "namespace": self.namespace,
                "namespace_ready": self.namespace_ready,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        return hashlib.sha256(payload).hexdigest()


def _run(run: CommandRunner, argv: tuple[str, ...]) -> CommandResult | None:
    try:
        result = run(list(argv))
    except Exception:
        return None
    if type(result.returncode) is not int or not isinstance(result.stdout, str):
        return None
    return result


def probe_kubernetes_client(
    run: CommandRunner,
    *,
    kubeconfig: Path,
    cluster_name: str,
    namespace: str,
) -> KubernetesClientReadiness:
    """Probe exact context and namespace without applying or changing resources."""
    if (
        not kubeconfig.is_absolute()
        or ".." in kubeconfig.parts
        or _NAME_RE.fullmatch(cluster_name) is None
        or _NAME_RE.fullmatch(namespace) is None
    ):
        raise ValueError("Kubernetes readiness target is invalid")
    context_result = _run(
        run,
        (
            "kubectl",
            "--kubeconfig",
            str(kubeconfig),
            "config",
            "current-context",
        ),
    )
    context = "unavailable"
    context_ready = False
    if context_result is not None and context_result.returncode == 0:
        candidate = context_result.stdout.strip()
        if candidate == cluster_name and "\n" not in candidate:
            context = candidate
            context_ready = True
    namespace_result = _run(
        run,
        (
            "kubectl",
            "--kubeconfig",
            str(kubeconfig),
            "get",
            "namespace",
            namespace,
            "--request-timeout=10s",
        ),
    )
    namespace_ready = bool(namespace_result is not None and namespace_result.returncode == 0)
    return KubernetesClientReadiness(
        current_context=context,
        namespace=namespace,
        context_ready=context_ready,
        namespace_ready=namespace_ready,
    )


__all__ = ["KubernetesClientReadiness", "probe_kubernetes_client"]
