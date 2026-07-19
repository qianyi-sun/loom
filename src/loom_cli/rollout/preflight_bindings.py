"""Derive attestation bindings from authoritative preflight evidence once."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from loom_cli.rollout.preflight_contract import (
    AttestationBindings,
    CheckContext,
    CheckExecution,
)


def _string(value: object, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"preflight binding evidence {label} is missing")
    return value


def _string_map(value: object, label: str) -> dict[str, str]:
    if not isinstance(value, Mapping) or not value or not all(
        isinstance(key, str) and isinstance(item, str) and key and item
        for key, item in value.items()
    ):
        raise ValueError(f"preflight binding evidence {label} is missing")
    return dict(value)


def _integer(value: object, label: str) -> int:
    if type(value) is not int or value < 0:
        raise ValueError(f"preflight binding evidence {label} is missing")
    return value


def derive_attestation_bindings(
    context: CheckContext,
    executions: Sequence[CheckExecution],
) -> AttestationBindings:
    """Build one complete binding from passed exact-check evidence and context."""
    if not executions or any(not execution.passed for execution in executions):
        raise ValueError("attestation bindings require complete passing preflight evidence")
    by_id = {execution.check_id: execution for execution in executions}
    if len(by_id) != len(executions):
        raise ValueError("attestation binding evidence contains duplicate checks")
    required = {
        "candidate.identity",
        "runner.install",
        "credentials.metadata",
        "backup.lease-eligibility",
        "images.contract",
        "migration.plan",
        "systemd.render",
        "gb10.shared-mount",
        "gb10.host-readiness",
        "browser.runtime",
        "rehearsal.cleanup",
    }
    if not required <= by_id.keys():
        raise ValueError("attestation binding evidence is incomplete")

    def evidence(check_id: str, field: str) -> object:
        try:
            return by_id[check_id].evidence[field]
        except KeyError as exc:
            raise ValueError(
                f"preflight binding evidence {check_id}.{field} is missing"
            ) from exc

    def binding(name: str) -> object:
        try:
            return context.bindings[name]
        except KeyError as exc:
            raise ValueError(f"preflight context binding {name} is missing") from exc

    candidate_sha = _string(evidence("candidate.identity", "resolved-sha"), "candidate sha")
    context_candidate = _string(binding("candidate.sha"), "context candidate sha")
    if candidate_sha != context_candidate:
        raise ValueError("preflight candidate evidence drifted from context")
    candidate_tree = _string(
        evidence("candidate.identity", "resolved-tree"),
        "candidate tree",
    )
    image_digests = _string_map(
        evidence("images.contract", "image-digests"),
        "image digests",
    )
    browser_image = _string(evidence("browser.runtime", "image-id"), "browser image")
    if browser_image not in image_digests.values():
        raise ValueError("browser image is not part of the exact image artifact set")
    secret_metadata = {
        name: (value if value.startswith("sha256:") else f"sha256:{value}")
        for name, value in _string_map(
            evidence("credentials.metadata", "metadata-fingerprints"),
            "credential metadata",
        ).items()
    }
    cleanup = evidence("rehearsal.cleanup", "cleanup-verified")
    protected_mutation = evidence("rehearsal.cleanup", "protected-mutation")
    if cleanup is not True or protected_mutation is not False:
        raise ValueError("isolated rehearsal cleanup evidence is incomplete")

    return AttestationBindings(
        candidate_sha=candidate_sha,
        candidate_tree=candidate_tree,
        image_digests=image_digests,
        runner_source_sha=candidate_sha,
        runner_source_tree=candidate_tree,
        runner_install_hash=_string(
            evidence("runner.install", "attestation-digest"),
            "runner install",
        ),
        runner_config_hash=_string(binding("runner.config.sha256"), "runner config"),
        staging_mutation_epoch=_integer(
            binding("staging.mutation-epoch"),
            "staging mutation epoch",
        ),
        backup_lease_id=_string(
            evidence("backup.lease-eligibility", "source-request"),
            "backup lease source request",
        ),
        db_snapshot_identity=_string(binding("db.snapshot-identity"), "DB snapshot"),
        schema_revision=_string(binding("schema.revision"), "schema revision"),
        migration_plan_digest=_string(
            evidence("migration.plan", "plan-digest"),
            "migration plan",
        ),
        environment=_string(binding("environment"), "environment"),
        namespace=_string(binding("namespace"), "namespace"),
        route=_string(binding("route"), "route"),
        secret_metadata_fingerprints=secret_metadata,
        gb10_inventory_digest=_string(
            evidence("gb10.host-readiness", "inventory-digest"),
            "GB10 inventory",
        ),
        gb10_boot_ids=_string_map(
            evidence("gb10.host-readiness", "boot-ids"),
            "GB10 boot IDs",
        ),
        gb10_mount_digest=_string(
            evidence("gb10.shared-mount", "mount-digest"),
            "GB10 mount",
        ),
        gb10_unit_digest=_string(
            evidence("systemd.render", "unit-set-digest"),
            "GB10 unit set",
        ),
        browser_image_digest=browser_image,
        browser_report_schema=_string(
            evidence("browser.runtime", "report-schema-digest"),
            "browser report schema",
        ),
    )


__all__ = ["derive_attestation_bindings"]
