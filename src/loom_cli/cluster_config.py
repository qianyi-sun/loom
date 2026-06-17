"""Cluster-deploy config: TOML schema + loader (#76 Phase 1B).

`loom cluster render --config FILE` reads a `cluster-config.toml`
to produce a deployable manifest set. Every variable has a sensible
default so an empty file (or no `--config` flag at all) yields a
working manifest set matching the canonical `deploy/k8s/*.yaml`
examples.

The schema is deliberately minimal in this PR; subsequent phases
add fields like `--postgres-url` for external storage, backup
targets, etc.
"""

from __future__ import annotations

import tomllib
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class ReplicaConfig:
    service: int = 2
    control_plane: int = 2
    gateway: int = 2
    # `loom-web` is paused by default per cluster-deploy.md §Component
    # map — operators scale it up when they want the SPA exposed.
    web: int = 0
    worker: int = 3


@dataclass(frozen=True)
class ClusterConfig:
    """Operator inputs that drive `loom cluster render`. All fields
    have defaults; the simplest `--config` is an empty TOML file.

    Field semantics:
    - `namespace`: target k8s namespace. The render command does NOT
      emit a Namespace resource yet (Phase 1B keeps the existing
      manifest shape); namespace selection happens at `kubectl apply
      -n <namespace>` or via `loom cluster up` (Phase 3).
    - `image_tag`: applied to every loom-* image (loom-service,
      loom-control-plane, loom-llm-gateway, loom-worker, loom-web).
      External images (`postgres`, `minio`) are configured separately.
    - `ingress_host`: the single public host. SPA at `/`, API at `/api/v1`.
    - `gateway_public_host`: optional second host for the LLM gateway.
      Empty string ⇒ no public gateway ingress (the default, per #77
      boundary enforcement). Operators on a transitional/legacy path
      can set it to `gateway.<base>` to keep direct gateway access.
    - `postgres_*`, `minio_*`, `worker_trajectory_storage_gi`: storage
      knobs.
    - `worker_max_concurrent`: per-worker process trial concurrency.
      This maps to WorkerSettings.max_concurrent via
      LOOM_WORKER_MAX_CONCURRENT and defaults to the worker's runtime
      default.
    """

    namespace: str = "loom"
    image_tag: str = "0.7"
    ingress_host: str = "loom.example.com"
    gateway_public_host: str = ""
    postgres_image: str = "postgres:16"
    postgres_storage_gi: int = 50
    minio_image: str = "minio/minio"
    minio_storage_gi: int = 500
    worker_trajectory_storage_gi: int = 100
    worker_max_concurrent: int = 5
    replicas: ReplicaConfig = field(default_factory=ReplicaConfig)

    def to_render_context(self) -> dict[str, Any]:
        """Flatten to a dict the jinja2 templates can consume.
        `replicas.service`, `replicas.worker`, etc. become attribute
        access in templates via the nested ReplicaConfig dataclass —
        but dataclasses aren't directly accessible by attribute in
        jinja2, so we hand-flatten with a dict-of-dicts shape."""
        out = asdict(self)
        # `replicas` is already a dict via asdict; jinja2 supports
        # `replicas.service` as dict-key access when undefined=strict.
        return out


def _merge_replicas(raw: object | None) -> ReplicaConfig:
    """Validate the [replicas] table from TOML. Unknown keys fail
    loudly so a typo (`servvice = 4`) doesn't silently use the
    default."""
    if raw is None:
        return ReplicaConfig()
    if not isinstance(raw, dict):
        raise ValueError(
            f"[replicas] must be a TOML table, got {type(raw).__name__}",
        )
    known = {f for f in ReplicaConfig.__dataclass_fields__}
    unknown = set(raw.keys()) - known
    if unknown:
        raise ValueError(
            f"unknown keys under [replicas]: {sorted(unknown)} "
            f"(known: {sorted(known)})",
        )
    return ReplicaConfig(**{k: int(v) for k, v in raw.items()})


def load_cluster_config(path: Path | None) -> ClusterConfig:
    """Load a TOML config file or fall back to all-defaults when path
    is None. Per-field type-check + unknown-key rejection so typos
    surface immediately instead of silently using a default."""
    if path is None:
        return ClusterConfig()
    if not path.exists():
        raise FileNotFoundError(f"cluster config not found: {path}")
    raw = tomllib.loads(path.read_text(encoding="utf-8"))
    known = {f for f in ClusterConfig.__dataclass_fields__}
    unknown = set(raw.keys()) - known
    if unknown:
        raise ValueError(
            f"unknown keys in cluster config: {sorted(unknown)} "
            f"(known: {sorted(known)})",
        )
    replicas = _merge_replicas(raw.pop("replicas", None))
    return ClusterConfig(replicas=replicas, **raw)
