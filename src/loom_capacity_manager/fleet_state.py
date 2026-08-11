"""Single-source fleet-state loading and diagnostic legacy drift inventory."""

from __future__ import annotations

import argparse
import json
import os
import stat
import sys
import tomllib
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from loom_capacity_manager.contracts import (
    MAX_CONTRACT_BYTES,
    MAX_POOLS,
    MAX_SUBJECTS,
    FleetManifestV1,
    ProfileReferenceV1,
    StrictV1Model,
    SubjectConfigurationV1,
    canonical_digest_excluding,
)

_DIGEST_LENGTH = 64
_LEGACY_FIELDS = (
    "actuator",
    "allowed_nodes",
    "container_cpus",
    "container_memory_mib",
    "cpu_arch",
    "cpu_per_slot",
    "exclusive",
    "max_concurrency_per_node",
    "max_jobs",
    "max_slots",
    "memory_mib_per_slot",
    "partition",
    "requested_concurrency",
    "requested_cpus",
    "requested_memory_mib",
    "reserved_cpus",
    "reserved_memory_mib",
    "resource_aware",
    "slurm_cluster_name",
    "slurm_controller_host",
)


class FleetStateError(ValueError):
    """Raised when fleet state is malformed, inconsistent, or noncanonical."""


@dataclass(frozen=True, slots=True)
class TopologyConflict:
    pool_id: str
    fields: tuple[str, ...]
    environments: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class TopologyInventoryReport:
    schema_version: int
    clean: bool
    environments: tuple[str, ...]
    conflicts: tuple[TopologyConflict, ...]

    def to_json(self) -> str:
        payload = {
            "schema_version": self.schema_version,
            "clean": self.clean,
            "environments": list(self.environments),
            "conflicts": [asdict(conflict) for conflict in self.conflicts],
        }
        return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _read_toml(path: Path) -> dict[str, Any]:
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise FleetStateError(f"cannot read fleet-state document {path.name!r}") from exc
    if path.is_symlink() or not stat.S_ISREG(metadata.st_mode):
        raise FleetStateError("fleet-state document must be a regular nonsymlink file")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
        try:
            opened = os.fstat(descriptor)
            if (opened.st_dev, opened.st_ino) != (
                metadata.st_dev,
                metadata.st_ino,
            ) or not stat.S_ISREG(opened.st_mode):
                raise FleetStateError("fleet-state document changed while opening")
            chunks: list[bytes] = []
            total = 0
            while total <= MAX_CONTRACT_BYTES:
                chunk = os.read(
                    descriptor,
                    min(64 * 1024, MAX_CONTRACT_BYTES + 1 - total),
                )
                if not chunk:
                    break
                chunks.append(chunk)
                total += len(chunk)
            payload = b"".join(chunks)
        finally:
            os.close(descriptor)
    except FleetStateError:
        raise
    except OSError as exc:
        raise FleetStateError(f"cannot read fleet-state document {path.name!r}") from exc
    if len(payload) > MAX_CONTRACT_BYTES:
        raise FleetStateError("fleet-state document exceeds maximum byte size")
    try:
        parsed = tomllib.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise FleetStateError(f"invalid TOML in fleet-state document {path.name!r}") from exc
    if not isinstance(parsed, dict):  # pragma: no cover - tomllib guarantee
        raise FleetStateError("fleet-state document root must be a table")
    return parsed


def _digest_without(model: StrictV1Model, field: str) -> str:
    return canonical_digest_excluding(model, field)


def validate_fleet_manifest_digests(manifest: FleetManifestV1) -> None:
    """Validate self-declared immutable pool and fleet generation digests."""

    for pool in manifest.pools:
        computed = _digest_without(pool, "pool_digest")
        if pool.pool_digest != computed:
            raise FleetStateError(f"pool digest mismatch for {pool.pool_id}: expected {computed}")
    computed_fleet = _digest_without(manifest, "fleet_digest")
    if manifest.fleet_digest != computed_fleet:
        raise FleetStateError(f"fleet digest mismatch: expected {computed_fleet}")
    template = manifest.development_subject_template
    if template is not None:
        for profile in template.profiles:
            validate_profile_narrowing(manifest, profile)


def _require_digest(value: Any, *, label: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != _DIGEST_LENGTH
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise FleetStateError(f"{label} digest must be lowercase SHA-256")
    return value


def _model_from_toml(model: type[StrictV1Model], payload: dict[str, Any]) -> StrictV1Model:
    try:
        return model.model_validate_json(json.dumps(payload, sort_keys=True, separators=(",", ":")))
    except (TypeError, ValueError, ValidationError) as exc:
        raise FleetStateError(f"invalid {model.__name__}: {exc}") from exc


def load_fleet_manifest(path: Path) -> FleetManifestV1:
    """Load one immutable fleet generation and verify every supplied digest."""

    raw = _read_toml(path)
    pools = raw.get("pools")
    if not isinstance(pools, list) or not pools:
        raise FleetStateError("fleet state requires at least one physical pool")
    _require_digest(raw.get("fleet_digest"), label="fleet")
    for pool in pools:
        if not isinstance(pool, dict):
            raise FleetStateError("fleet pool entries must be tables")
        _require_digest(pool.get("pool_digest"), label="pool")

    parsed = _model_from_toml(FleetManifestV1, raw)
    assert isinstance(parsed, FleetManifestV1)
    if {pool.pool_id for pool in parsed.pools} != {"gb10", "oldlab"}:
        raise FleetStateError("fleet generation must contain exactly gb10 and oldlab")

    validate_fleet_manifest_digests(parsed)
    return parsed


def load_subject_configuration(path: Path) -> tuple[SubjectConfigurationV1, ...]:
    """Load immutable subject generations without consulting or mutating fleet state."""

    raw = _read_toml(path)
    unknown = set(raw) - {"schema_version", "subjects"}
    if unknown:
        raise FleetStateError(f"unknown subject-state field: {min(unknown)}")
    if raw.get("schema_version") != 1:
        raise FleetStateError("subject-state schema_version must be 1")
    subjects = raw.get("subjects")
    if not isinstance(subjects, list):
        raise FleetStateError("subject-state subjects must be an array")
    if len(subjects) > MAX_SUBJECTS:
        raise FleetStateError("subject-state subject count exceeds the contract limit")

    parsed: list[SubjectConfigurationV1] = []
    for payload in subjects:
        if not isinstance(payload, dict):
            raise FleetStateError("subject entries must be tables")
        subject = _model_from_toml(SubjectConfigurationV1, payload)
        assert isinstance(subject, SubjectConfigurationV1)
        parsed.append(subject)
    identities = [subject.subject_id for subject in parsed]
    if len(identities) != len(set(identities)):
        raise FleetStateError("duplicate subject_id")
    return tuple(sorted(parsed, key=lambda subject: subject.subject_id.hex))


def validate_profile_narrowing(
    manifest: FleetManifestV1,
    profile: ProfileReferenceV1,
) -> None:
    """Prove that an environment profile only narrows fleet-owned topology."""

    computed_profile = _digest_without(profile, "profile_digest")
    if profile.profile_digest != computed_profile:
        raise FleetStateError(
            f"profile digest mismatch for {profile.pool_id}: expected {computed_profile}"
        )

    pool = next((item for item in manifest.pools if item.pool_id == profile.pool_id), None)
    if pool is None:
        raise FleetStateError(f"unknown fleet pool {profile.pool_id!r}")
    if profile.pool_generation != pool.pool_generation:
        raise FleetStateError("profile pool generation does not match fleet state")
    if profile.pool_digest != pool.pool_digest:
        raise FleetStateError("profile pool digest does not match fleet state")
    if profile.protocol_generation != pool.protocol_generation:
        raise FleetStateError("profile protocol generation does not match fleet state")
    if profile.protocol_digest != pool.protocol_digest:
        raise FleetStateError("profile protocol digest does not match fleet state")

    declared_domains = {domain.domain_id for domain in pool.resource_domains}
    unknown_domains = set(profile.eligible_resource_domains) - declared_domains
    if unknown_domains:
        raise FleetStateError(
            f"profile references unknown resource domain {min(unknown_domains)!r}"
        )
    for shape in profile.worker_shapes:
        unknown_shape_domains = set(shape.compatible_domain_ids) - declared_domains
        if unknown_shape_domains:
            raise FleetStateError(
                f"shape references unknown resource domain {min(unknown_shape_domains)!r}"
            )


def _legacy_fingerprint(policy: dict[str, Any]) -> dict[str, Any]:
    actuator = policy.get("actuator_config")
    if not isinstance(actuator, dict):
        actuator = {}
    values: dict[str, Any] = {
        "actuator": policy.get("actuator"),
        "max_slots": policy.get("max_slots"),
    }
    for field in _LEGACY_FIELDS:
        if field in values:
            continue
        value = actuator.get(field)
        if field == "allowed_nodes" and isinstance(value, list):
            value = tuple(sorted(str(node) for node in value))
        values[field] = value
    return values


def inventory_legacy_topology(paths: Sequence[Path]) -> TopologyInventoryReport:
    """Compare legacy copies without selecting, merging, or publishing one."""

    if not paths:
        raise FleetStateError("at least one legacy environment-state path is required")
    by_environment: dict[str, dict[str, dict[str, Any]]] = {}
    for path in paths:
        raw = _read_toml(path)
        environment = raw.get("environment")
        if not isinstance(environment, str) or not environment:
            raise FleetStateError(f"legacy document {path.name!r} has no environment")
        if environment in by_environment:
            raise FleetStateError(f"duplicate legacy environment {environment!r}")
        policies = raw.get("worker_pool_autoscaler_policies")
        if not isinstance(policies, list):
            policies = []
        pool_rows: dict[str, dict[str, Any]] = {}
        for policy in policies:
            if not isinstance(policy, dict):
                continue
            pool_id = policy.get("pool_name")
            if not isinstance(pool_id, str) or not pool_id:
                continue
            if pool_id in pool_rows:
                raise FleetStateError(
                    f"legacy environment {environment!r} repeats pool {pool_id!r}"
                )
            pool_rows[pool_id] = _legacy_fingerprint(policy)
        by_environment[environment] = pool_rows

    environments = tuple(sorted(by_environment))
    pool_ids = tuple(sorted({pool_id for pools in by_environment.values() for pool_id in pools}))
    if len(pool_ids) > MAX_POOLS:
        raise FleetStateError("legacy inventory exceeds maximum pool count")

    conflicts: list[TopologyConflict] = []
    for pool_id in pool_ids:
        different: list[str] = []
        for field in _LEGACY_FIELDS:
            values = []
            for environment in environments:
                pool = by_environment[environment].get(pool_id)
                values.append("<missing-pool>" if pool is None else pool.get(field))
            if any(value != values[0] for value in values[1:]):
                different.append(field)
        if different:
            conflicts.append(
                TopologyConflict(
                    pool_id=pool_id,
                    fields=tuple(sorted(different)),
                    environments=environments,
                )
            )
    return TopologyInventoryReport(
        schema_version=1,
        clean=not conflicts,
        environments=environments,
        conflicts=tuple(conflicts),
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    inventory = subparsers.add_parser(
        "inventory-legacy",
        help="compare legacy environment topology without selecting an authority",
    )
    inventory.add_argument("paths", type=Path, nargs="+")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    if args.command != "inventory-legacy":  # pragma: no cover - argparse guarantee
        raise FleetStateError("unknown command")
    try:
        report = inventory_legacy_topology(args.paths)
    except FleetStateError as exc:
        print(
            json.dumps(
                {"schema_version": 1, "clean": False, "error": str(exc)},
                sort_keys=True,
                separators=(",", ":"),
            )
        )
        return 2
    print(report.to_json())
    return 0 if report.clean else 2


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
