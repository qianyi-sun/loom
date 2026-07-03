"""Rollout run context (#340).

Holds the resolved CLI arguments + resolved-at-launch state (git sha,
config file hashes) that every step needs. Passed to each step's
``run()`` and ``inputs_hash()``. Immutable once constructed — the driver
does not permit inputs to change during a single rollout invocation.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from pathlib import Path


def sha256_of_file(path: Path) -> str:
    """Streaming sha256 of a file's bytes. Returns hex digest."""
    h = hashlib.sha256()
    with path.open("rb") as f:
        while chunk := f.read(65536):
            h.update(chunk)
    return h.hexdigest()


@dataclass(frozen=True, slots=True)
class RolloutContext:
    """Immutable run context passed to every step.

    Attributes:
        image_tag: Target release image tag (e.g. ``public-beta-abc123``).
        target_ref: Git ref the operator asked to roll out (e.g. ``origin/dev``).
        resolved_sha: Full 40-char git sha the ref resolved to at launch.
        cluster_name: Name of the target k8s cluster (kind cluster name).
        namespace: Kubernetes namespace for the release.
        environment: Protected environment name (e.g. ``public-beta``).
            Used by the backup verification and release-gate steps to bind
            evidence to the operator's declared environment.
        cluster_config_path: Path to the operator's cluster-config.toml.
        cluster_config_sha256: sha256 of cluster_config_path contents at launch.
        rollout_root: Root of the evidence directory tree (#174 model).
        backup_manifest_path: Path to a pre-existing backup manifest for the
            protected environment. The dumps themselves are produced by the
            operator per the runbook (needs Postgres/MinIO credentials the
            driver doesn't have); the backup step verifies the manifest via
            ``loom cluster backup check``. Not part of ``inputs_hash`` — the
            manifest changes between rollouts and stale-checks are handled
            by ``backup check`` itself.
        scope: Rollout scope classification.
            One of ``"current-gb10"``, ``"full-cluster"``. Full-cluster asks
            for release-critical acceptance evidence across every managed
            worker pool; current-gb10 targets only the current GB10 pool.
        exclude_oldlab: Operator opt-out of the OLDLAB worker pool.
            Refused when scope=full-cluster per the #340 acceptance criteria
            (you can't claim full-cluster acceptance while excluding a pool).
        resume: Whether this invocation is a resume of a prior run.
    """

    image_tag: str
    target_ref: str
    resolved_sha: str
    cluster_name: str
    namespace: str
    environment: str
    cluster_config_path: Path
    cluster_config_sha256: str
    rollout_root: Path
    backup_manifest_path: Path
    scope: str = "current-gb10"
    exclude_oldlab: bool = False
    resume: bool = False

    # Extra state — not hashed into inputs, just carried for convenience.
    metadata: dict[str, str] = field(default_factory=dict)

    def to_inputs_dict(self) -> dict[str, object]:
        """Return the dict written to ``inputs.json``.

        The keys are stable and sorted so re-writes don't drift.
        """
        return {
            "image_tag": self.image_tag,
            "target_ref": self.target_ref,
            "resolved_sha": self.resolved_sha,
            "cluster_name": self.cluster_name,
            "namespace": self.namespace,
            "environment": self.environment,
            "cluster_config_path": str(self.cluster_config_path),
            "cluster_config_sha256": self.cluster_config_sha256,
            "rollout_root": str(self.rollout_root),
            "scope": self.scope,
            "exclude_oldlab": self.exclude_oldlab,
        }

    def is_full_cluster_scope(self) -> bool:
        return self.scope == "full-cluster"

    def would_falsify_full_cluster_acceptance(self) -> bool:
        """#340 acceptance: exclude-oldlab + full-cluster is contradictory."""
        return self.is_full_cluster_scope() and self.exclude_oldlab
