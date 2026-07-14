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
        image_tag: Target release image tag (e.g. ``staging-abc123``).
        target_ref: Git ref the operator asked to roll out (e.g. ``origin/dev``).
        resolved_sha: Full 40-char git sha the ref resolved to at launch.
        cluster_name: Name of the target k8s cluster (kind cluster name).
        namespace: Kubernetes namespace for the release.
        environment: Protected environment name (e.g. ``staging``).
            Used by the backup verification and release-gate steps to bind
            evidence to the operator's declared environment.
        cp_url: Operator-reachable Control Plane admin base URL. Rollout steps
            that call ``loom admin ...`` must use this instead of the admin
            CLI's localhost default because protected rollouts often run
            against a private tunnel or node-local port.
        admin_token_source: Secret-source reference for protected CP admin
            calls. This is persisted as a source reference only, never the raw
            token value.
        expect_admin_token_fingerprint: Optional redacted fingerprint expected
            for admin_token_source. Steps pass this through to admin commands
            so token drift fails before CP mutation.
        worker_token_source: Optional secret-source reference for protected
            external runner parity checks. This is persisted as a source
            reference only, never the raw token value.
        service_token_source: Optional secret-source reference for protected
            Service API calls made by rollout-owned CLI subcommands. This is
            persisted as a source reference only, never the raw token value.
        smoke_submit_mode: Optional rollout smoke submit mode. When unset, the
            smoke step preserves its legacy environment fallback.
        smoke_api_token_source: Optional secret-source reference for
            user-token smoke mode. This is persisted as a source reference
            only, never the raw token value.
        smoke_task_id: Optional explicit smoke task id.
        smoke_required_worker_pool: Optional worker-pool constraint for the
            smoke submission.
        smoke_agent: Optional smoke agent name.
        smoke_on_behalf_username: Optional represented username for
            admin-on-behalf smoke.
        smoke_on_behalf_team_id: Optional represented team id for
            admin-on-behalf smoke.
        smoke_admin_actor: Optional audited admin actor for admin-on-behalf
            smoke submissions.
        cluster_config_path: Path to the operator's cluster-config.toml.
        cluster_config_sha256: sha256 of cluster_config_path contents at launch.
        rollout_root: Root of the evidence directory tree (#174 model).
        backup_manifest_path: Path to a pre-existing backup manifest for the
            protected environment. Legacy manual rollouts use operator-produced
            dumps because the driver lacks Postgres/MinIO credentials. Brokered
            protected rollouts instead use the broker-owned backup created and
            verified before unit launch. In both paths the backup step verifies
            the manifest via ``loom cluster backup check``. Legacy manual
            contexts omit the path from ``inputs_hash``; broker-created contexts
            bind both the exact path and sha256 into persisted inputs/step
            hashes. Freshness is still enforced independently by ``backup
            check``.
        backup_manifest_min_remaining_hours: Minimum remaining freshness
            window required by the rollout backup step before the manifest
            reaches the protected backup max age. This keeps long GB10 prep
            runs from discovering backup expiry only at the mutation step.
        scope: Rollout scope classification.
            One of ``"current-gb10"``, ``"full-cluster"``. Full-cluster asks
            for release-critical acceptance evidence across every managed
            worker pool; current-gb10 targets only the current GB10 pool.
        exclude_oldlab: Operator opt-out of the OLDLAB worker pool.
            Refused when scope=full-cluster per the #340 acceptance criteria
            (you can't claim full-cluster acceptance while excluding a pool).
        gb10_prep_concurrency: Optional bounded host-level concurrency for
            rollout step 12. ``None`` uses the step's conservative default.
        resume: Whether this invocation is a resume of a prior run.
    """

    image_tag: str
    target_ref: str
    resolved_sha: str
    cluster_name: str
    namespace: str
    environment: str
    cp_url: str
    cluster_config_path: Path
    cluster_config_sha256: str
    rollout_root: Path
    backup_manifest_path: Path
    backup_manifest_min_remaining_hours: int = 2
    backup_manifest_sha256: str | None = None
    runner_config_sha256: str | None = None
    request_id: str | None = None
    initiating_operator: str | None = None
    initiating_uid: int | None = None
    attempt_number: int | None = None
    attempt_operator: str | None = None
    attempt_uid: int | None = None
    request_envelope_path: Path | None = None
    admin_token_source: str = "env:LOOM_CP_ADMIN_TOKEN"
    expect_admin_token_fingerprint: str | None = None
    worker_token_source: str | None = None
    service_token_source: str | None = None
    smoke_submit_mode: str | None = None
    smoke_api_token_source: str | None = None
    smoke_task_id: str | None = None
    smoke_required_worker_pool: str | None = None
    smoke_agent: str | None = None
    smoke_on_behalf_username: str | None = None
    smoke_on_behalf_team_id: str | None = None
    smoke_admin_actor: str | None = None
    scope: str = "current-gb10"
    exclude_oldlab: bool = False
    gb10_prep_concurrency: int | None = None
    resume: bool = False

    # Extra state — not hashed into inputs, just carried for convenience.
    metadata: dict[str, str] = field(default_factory=dict)

    def to_inputs_dict(self) -> dict[str, object]:
        """Return the dict written to ``inputs.json``.

        The keys are stable and sorted so re-writes don't drift.
        """
        inputs: dict[str, object] = {
            "image_tag": self.image_tag,
            "target_ref": self.target_ref,
            "resolved_sha": self.resolved_sha,
            "cluster_name": self.cluster_name,
            "namespace": self.namespace,
            "environment": self.environment,
            "cp_url": self.cp_url,
            "admin_token_source": self.admin_token_source,
            "expect_admin_token_fingerprint": self.expect_admin_token_fingerprint,
            "worker_token_source": self.worker_token_source,
            "service_token_source": self.service_token_source,
            "smoke_submit_mode": self.smoke_submit_mode,
            "smoke_api_token_source": self.smoke_api_token_source,
            "smoke_task_id": self.smoke_task_id,
            "smoke_required_worker_pool": self.smoke_required_worker_pool,
            "smoke_agent": self.smoke_agent,
            "smoke_on_behalf_username": self.smoke_on_behalf_username,
            "smoke_on_behalf_team_id": self.smoke_on_behalf_team_id,
            "smoke_admin_actor": self.smoke_admin_actor,
            "cluster_config_path": str(self.cluster_config_path),
            "cluster_config_sha256": self.cluster_config_sha256,
            "rollout_root": str(self.rollout_root),
            "backup_manifest_min_remaining_hours": (self.backup_manifest_min_remaining_hours),
            "scope": self.scope,
            "exclude_oldlab": self.exclude_oldlab,
            "gb10_prep_concurrency": self.gb10_prep_concurrency,
        }
        if self.request_id is None:
            return inputs
        inputs.update(
            {
                "admin_token_source": _secret_source_identity(self.admin_token_source),
                "worker_token_source": _secret_source_identity(self.worker_token_source),
                "service_token_source": _secret_source_identity(self.service_token_source),
                "smoke_api_token_source": _secret_source_identity(self.smoke_api_token_source),
                "backup_manifest_path": str(self.backup_manifest_path),
                "backup_manifest_sha256": self.backup_manifest_sha256,
                "runner_config_sha256": self.runner_config_sha256,
                "request_id": self.request_id,
                "initiating_operator": self.initiating_operator,
                "initiating_uid": self.initiating_uid,
            }
        )
        return inputs

    def is_full_cluster_scope(self) -> bool:
        return self.scope == "full-cluster"

    def would_falsify_full_cluster_acceptance(self) -> bool:
        """#340 acceptance: exclude-oldlab + full-cluster is contradictory."""
        return self.is_full_cluster_scope() and self.exclude_oldlab


def _secret_source_identity(source: str | None) -> str | None:
    """Persist a change-detecting identity without exposing a protected path."""
    if source is None:
        return None
    return "sha256:" + hashlib.sha256(source.encode("utf-8")).hexdigest()
