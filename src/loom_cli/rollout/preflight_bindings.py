"""Derive attestation bindings from authoritative preflight evidence once."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from loom_cli.rollout.external_supervisor_controller import (
    ExternalSupervisorControllerBinding,
    encode_external_supervisor_controller_bindings,
)
from loom_cli.rollout.external_supervisor_predecessor import GB10_CANONICAL_UNIT_DIR
from loom_cli.rollout.operator.backup_lease import BackupLease, component_set_digest
from loom_cli.rollout.preflight_contract import (
    AttestationBindings,
    CheckContext,
    CheckExecution,
    external_supervisor_transition_digest,
    external_supervisor_unit_set_digest,
    external_supervisor_unit_set_digest_or_empty,
)


def _string(value: object, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"preflight binding evidence {label} is missing")
    return value


def _string_map(value: object, label: str) -> dict[str, str]:
    if (
        not isinstance(value, Mapping)
        or not value
        or not all(
            isinstance(key, str) and isinstance(item, str) and key and item
            for key, item in value.items()
        )
    ):
        raise ValueError(f"preflight binding evidence {label} is missing")
    return dict(value)


def _string_map_allow_empty(value: object, label: str) -> dict[str, str]:
    # An absent external-supervisor predecessor (first introduction of the
    # supervisor) carries an empty unit map; the unit-set-digest recompute is the
    # authoritative gate for that case, so the map itself may legitimately be empty.
    if not isinstance(value, Mapping) or not all(
        isinstance(key, str) and isinstance(item, str) and key and item
        for key, item in value.items()
    ):
        raise ValueError(f"preflight binding evidence {label} is missing")
    return dict(value)


def _integer(value: object, label: str) -> int:
    if type(value) is not int or value < 0:
        raise ValueError(f"preflight binding evidence {label} is missing")
    return value


def _controller_unit_maps(
    values: Mapping[str, str],
    *,
    label: str,
) -> dict[str, dict[str, str]]:
    result: dict[str, dict[str, str]] = {}
    for key, digest in values.items():
        host, separator, unit_name = key.partition("/")
        if not separator or not host or not unit_name:
            raise ValueError(f"preflight binding evidence {label} is invalid")
        result.setdefault(host, {})[unit_name] = digest
    return result


def _controller_predecessors(
    values: Mapping[str, str],
) -> dict[str, dict[str, object]]:
    grouped: dict[str, dict[str, str]] = {}
    for key, value in values.items():
        host, separator, field = key.partition("/")
        if not separator or not host or not field:
            raise ValueError("external supervisor controller predecessor evidence is invalid")
        grouped.setdefault(host, {})[field] = value
    required = {
        "authority-kind",
        "authority-digest",
        "pointer-digest",
        "unit-set-digest",
        "live-evidence-digest",
        "pending-transition-digest",
        "unit-directory",
    }
    result: dict[str, dict[str, object]] = {}
    for host, fields in grouped.items():
        unit_fields = {field: value for field, value in fields.items() if field.startswith("unit/")}
        if set(fields) - set(unit_fields) != required:
            raise ValueError("external supervisor controller predecessor evidence is invalid")
        result[host] = {
            **{field: fields[field] for field in required},
            "units": {field.removeprefix("unit/"): value for field, value in unit_fields.items()},
        }
    return result


def derive_attestation_bindings(
    context: CheckContext,
    executions: Sequence[CheckExecution],
    *,
    backup_lease: BackupLease | None = None,
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
        "external-supervisor.predecessor",
        "images.contract",
        "migration.manifest",
        "migration.plan",
        "systemd.render",
        "gb10.shared-mount",
        "gb10.candidate-source",
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
            raise ValueError(f"preflight binding evidence {check_id}.{field} is missing") from exc

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
    if candidate_tree != _string(binding("candidate.tree"), "context candidate tree"):
        raise ValueError("preflight candidate tree evidence drifted from context")
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

    predecessor_kind = _string(
        evidence("external-supervisor.predecessor", "authority-kind"),
        "external supervisor predecessor kind",
    )
    if predecessor_kind not in {"legacy-manifest", "canonical", "absent"}:
        raise ValueError("external supervisor predecessor evidence is not authoritative")
    predecessor_digest = _string(
        evidence("external-supervisor.predecessor", "authority-digest"),
        "external supervisor predecessor digest",
    )
    predecessor_pointer_digest = _string(
        evidence("external-supervisor.predecessor", "pointer-digest"),
        "external supervisor predecessor pointer",
    )
    predecessor_units = _string_map_allow_empty(
        evidence("external-supervisor.predecessor", "unit-digests"),
        "external supervisor predecessor units",
    )
    predecessor_unit_set_digest = _string(
        evidence("external-supervisor.predecessor", "unit-set-digest"),
        "external supervisor predecessor unit set",
    )
    predecessor_live_evidence_digest = _string(
        evidence("external-supervisor.predecessor", "live-evidence-digest"),
        "external supervisor predecessor live evidence",
    )
    predecessor_pending_transition_digest = _string(
        evidence("external-supervisor.predecessor", "pending-transition-digest"),
        "external supervisor predecessor pending transition",
    )
    if (
        evidence("external-supervisor.predecessor", "transition-clear") is not True
        or evidence("external-supervisor.predecessor", "runtime-ready") is not True
        or external_supervisor_unit_set_digest_or_empty(predecessor_units)
        != predecessor_unit_set_digest
    ):
        raise ValueError("external supervisor predecessor evidence is not authoritative")

    target_artifact_digest = _string(
        evidence("systemd.render", "supervisor-artifact-digest"),
        "external supervisor target artifact",
    )
    target_profile_sha256 = _string(
        evidence("systemd.render", "supervisor-profile-sha256"),
        "external supervisor target profile",
    )
    target_script_sha256 = _string_map(
        evidence("systemd.render", "supervisor-script-digests"),
        "external supervisor target scripts",
    )
    target_unit_sha256 = _string_map(
        evidence("systemd.render", "supervisor-unit-digests"),
        "external supervisor target units",
    )
    target_unit_set_digest = _string(
        evidence("systemd.render", "supervisor-unit-set-digest"),
        "external supervisor target unit set",
    )
    if external_supervisor_unit_set_digest(target_unit_sha256) != target_unit_set_digest:
        raise ValueError("external supervisor target unit evidence drifted")
    supervisor_transition = external_supervisor_transition_digest(
        unit_directory=GB10_CANONICAL_UNIT_DIR,
        candidate_sha=candidate_sha,
        candidate_tree=candidate_tree,
        environment=_string(binding("environment"), "environment"),
        predecessor_kind=predecessor_kind,
        predecessor_digest=predecessor_digest,
        predecessor_pointer_digest=predecessor_pointer_digest,
        predecessor_unit_sha256=predecessor_units,
        predecessor_unit_set_digest=predecessor_unit_set_digest,
        predecessor_live_evidence_digest=predecessor_live_evidence_digest,
        predecessor_pending_transition_digest=predecessor_pending_transition_digest,
        target_artifact_digest=target_artifact_digest,
        target_profile_sha256=target_profile_sha256,
        target_script_sha256=target_script_sha256,
        target_unit_sha256=target_unit_sha256,
        target_unit_set_digest=target_unit_set_digest,
    )
    controller_artifacts = _string_map(
        evidence("systemd.render", "supervisor-controller-artifact-digests"),
        "external supervisor controller artifacts",
    )
    controller_target_units = _controller_unit_maps(
        _string_map(
            evidence("systemd.render", "supervisor-controller-unit-digests"),
            "external supervisor controller units",
        ),
        label="external supervisor controller units",
    )
    controller_target_unit_sets = _string_map(
        evidence("systemd.render", "supervisor-controller-unit-set-digests"),
        "external supervisor controller unit sets",
    )
    controller_predecessors = _controller_predecessors(
        _string_map(
            evidence("external-supervisor.predecessor", "controller-bindings"),
            "external supervisor controller predecessors",
        )
    )
    controller_hosts = set(controller_artifacts)
    if (
        not controller_hosts
        or set(controller_target_units) != controller_hosts
        or set(controller_target_unit_sets) != controller_hosts
        or set(controller_predecessors) != controller_hosts
    ):
        raise ValueError("external supervisor controller coverage drifted")
    controller_bindings: dict[str, ExternalSupervisorControllerBinding] = {}
    for host in sorted(controller_hosts):
        controller_predecessor = controller_predecessors[host]
        controller_units = controller_target_units[host]
        controller_unit_set = controller_target_unit_sets[host]
        predecessor_controller_units = controller_predecessor["units"]
        if (
            not isinstance(predecessor_controller_units, Mapping)
            or external_supervisor_unit_set_digest(controller_units) != controller_unit_set
            or external_supervisor_unit_set_digest_or_empty(predecessor_controller_units)
            != controller_predecessor["unit-set-digest"]
        ):
            raise ValueError("external supervisor controller unit evidence drifted")
        controller_bindings[host] = ExternalSupervisorControllerBinding.build(
            execution_host=host,
            candidate_sha=candidate_sha,
            candidate_tree=candidate_tree,
            environment=_string(binding("environment"), "environment"),
            predecessor_kind=str(controller_predecessor["authority-kind"]),
            predecessor_digest=str(controller_predecessor["authority-digest"]),
            predecessor_pointer_digest=str(controller_predecessor["pointer-digest"]),
            predecessor_unit_sha256=predecessor_controller_units,
            predecessor_unit_set_digest=str(controller_predecessor["unit-set-digest"]),
            predecessor_live_evidence_digest=str(controller_predecessor["live-evidence-digest"]),
            predecessor_pending_transition_digest=str(
                controller_predecessor["pending-transition-digest"]
            ),
            unit_directory=str(controller_predecessor["unit-directory"]),
            target_artifact_digest=controller_artifacts[host],
            target_profile_sha256=target_profile_sha256,
            target_script_sha256=target_script_sha256,
            target_unit_sha256=controller_units,
            target_unit_set_digest=controller_unit_set,
        )
    if backup_lease is None:
        backup_lease_id = _string(
            evidence("backup.lease-eligibility", "source-request"),
            "backup lease source request",
        )
        backup_lease_digest = _string(binding("backup.lease.sha256"), "backup lease digest")
        backup_manifest_sha256 = _string(binding("backup.manifest.sha256"), "backup manifest")
        backup_component_digest = _string(
            binding("backup.component-set.sha256"), "backup component set"
        )
        db_snapshot_identity = _string(binding("db.snapshot-identity"), "DB snapshot")
        schema_revision = _string(binding("schema.revision"), "schema revision")
        object_inventory_root = _string(binding("object.inventory-root"), "object inventory")
    else:
        expected = (
            (backup_lease.environment, binding("environment")),
            (backup_lease.namespace, binding("namespace")),
            (backup_lease.mutation_epoch, binding("staging.mutation-epoch")),
        )
        if any(actual != declared for actual, declared in expected):
            raise ValueError("restore-verified backup lease drifts from preflight context")
        backup_lease_id = backup_lease.lease_id
        backup_lease_digest = backup_lease.evidence_digest
        backup_manifest_sha256 = backup_lease.manifest_sha256
        backup_component_digest = component_set_digest(backup_lease.component_sha256)
        db_snapshot_identity = backup_lease.db_snapshot_identity
        schema_revision = backup_lease.schema_revision
        object_inventory_root = backup_lease.object_inventory_root

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
        backup_lease_id=backup_lease_id,
        backup_lease_digest=backup_lease_digest,
        backup_manifest_sha256=backup_manifest_sha256,
        backup_component_set_digest=backup_component_digest,
        db_snapshot_identity=db_snapshot_identity,
        schema_revision=schema_revision,
        object_inventory_root=object_inventory_root,
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
            evidence("gb10.candidate-source", "source-digest"),
            "GB10 candidate source",
        ),
        browser_image_digest=browser_image,
        browser_report_schema=_string(
            evidence("browser.runtime", "report-schema-digest"),
            "browser report schema",
        ),
        supervisor_predecessor_kind=predecessor_kind,
        supervisor_predecessor_digest=predecessor_digest,
        supervisor_predecessor_pointer_digest=predecessor_pointer_digest,
        supervisor_predecessor_unit_sha256=predecessor_units,
        supervisor_predecessor_unit_set_digest=predecessor_unit_set_digest,
        supervisor_predecessor_live_evidence_digest=predecessor_live_evidence_digest,
        supervisor_predecessor_pending_transition_digest=(predecessor_pending_transition_digest),
        supervisor_transition_digest=supervisor_transition,
        supervisor_controller_bindings=encode_external_supervisor_controller_bindings(
            controller_bindings
        ),
    )


__all__ = ["derive_attestation_bindings"]
