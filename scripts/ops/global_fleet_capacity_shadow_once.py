#!/usr/bin/env python3
"""Produce deterministic, non-executable Package 1 capacity evidence once."""

from __future__ import annotations

import argparse
import os
import stat
import sys
import tempfile
from collections.abc import Sequence
from pathlib import Path
from typing import Annotated, Literal
from uuid import UUID

from pydantic import Field, ValidationError, model_validator

from loom_capacity_manager.allocator import (
    MAX_ALLOCATION_DECISIONS,
    AllocatorSearchBounds,
    ShadowAllocatorError,
    allocate_shadow,
)
from loom_capacity_manager.contracts import (
    MAX_CONTRACT_BYTES,
    MAX_POOLS,
    MAX_SUBJECTS,
    AllocationInputV1,
    CapacityContractError,
    ConfigurationGenerationRefV1,
    ConfigurationSnapshotV1,
    DemandSnapshotV1,
    Digest,
    FairnessCursorV1,
    Identifier,
    InputFreshnessV1,
    ObservedCommitmentV1,
    PoolAllocationInputV1,
    PoolObservationV1,
    PositiveQuantity,
    Quantity,
    ShadowEpochV1,
    StrictV1Model,
    SubjectAllocationInputV1,
    canonical_bytes,
    canonical_digest,
)
from loom_capacity_manager.fleet_state import (
    FleetStateError,
    load_fleet_manifest,
    load_subject_configuration,
    validate_profile_narrowing,
)


class ShadowOnceError(ValueError):
    """One bounded offline-driver failure without a partial publication."""


class _SubjectEvidenceV1(StrictV1Model):
    subject_id: UUID
    subject_incarnation: UUID
    freshness: InputFreshnessV1
    last_demand: DemandSnapshotV1 | None

    @model_validator(mode="after")
    def _binding_and_digest(self) -> _SubjectEvidenceV1:
        if self.last_demand is None:
            if self.freshness.state == "valid":
                raise ValueError("valid subject evidence requires a demand report")
            return self
        if (
            self.last_demand.subject_id != self.subject_id
            or self.last_demand.subject_incarnation != self.subject_incarnation
        ):
            raise ValueError("demand evidence binding mismatch")
        if self.freshness.last_payload_digest != canonical_digest(self.last_demand):
            raise ValueError("demand evidence digest mismatch")
        return self


class _PoolEvidenceV1(StrictV1Model):
    pool_id: Identifier
    freshness: InputFreshnessV1
    last_observation: PoolObservationV1 | None

    @model_validator(mode="after")
    def _binding_and_digest(self) -> _PoolEvidenceV1:
        if self.last_observation is None:
            if self.freshness.state == "valid":
                raise ValueError("valid pool evidence requires an observation")
            return self
        if self.last_observation.pool_id != self.pool_id:
            raise ValueError("pool evidence binding mismatch")
        if self.freshness.last_payload_digest != canonical_digest(self.last_observation):
            raise ValueError("pool evidence digest mismatch")
        return self


class _EvidenceSnapshotV1(StrictV1Model):
    configuration_epoch: PositiveQuantity
    subjects: Annotated[tuple[_SubjectEvidenceV1, ...], Field(max_length=MAX_SUBJECTS)]
    pools: Annotated[tuple[_PoolEvidenceV1, ...], Field(max_length=MAX_POOLS)]
    observed_commitments: tuple[ObservedCommitmentV1, ...] = ()
    fairness_cursors: tuple[FairnessCursorV1, ...] = ()
    existing_pending_slots: Quantity = 0
    existing_pending_jobs: Quantity = 0

    @model_validator(mode="after")
    def _unique_bindings(self) -> _EvidenceSnapshotV1:
        subject_keys = [(item.subject_id, item.subject_incarnation) for item in self.subjects]
        if len(subject_keys) != len(set(subject_keys)):
            raise ValueError("duplicate subject evidence binding")
        pool_ids = [item.pool_id for item in self.pools]
        if len(pool_ids) != len(set(pool_ids)):
            raise ValueError("duplicate pool evidence binding")
        retained = {(item.kind, item.commitment_id): item for item in self.observed_commitments}
        if len(retained) != len(self.observed_commitments):
            raise ValueError("duplicate retained commitment binding")
        for subject in self.subjects:
            if subject.last_demand is None:
                continue
            for claim in subject.last_demand.fixed_claims:
                if claim.state == "unknown":
                    observed_state: Literal["observed", "unknown", "quarantined"] = "unknown"
                elif claim.state == "quarantined":
                    observed_state = "quarantined"
                else:
                    observed_state = "observed"
                expected = ObservedCommitmentV1(
                    kind="claim",
                    commitment_id=claim.claim_id,
                    physical_identity=claim.worker_identity,
                    attempt_id=claim.attempt_id,
                    concurrency_slots=claim.concurrency_slots,
                    subject_id=subject.subject_id,
                    subject_incarnation=subject.subject_incarnation,
                    deployment_generation=claim.deployment_generation,
                    pool_id=claim.pool_id,
                    pool_generation=claim.pool_generation,
                    profile_id=claim.profile_id,
                    profile_generation=claim.profile_generation,
                    profile_digest=claim.profile_digest,
                    shape_id=claim.shape_id,
                    resources=claim.resources,
                    state=observed_state,
                )
                if retained.get(("claim", claim.claim_id)) != expected:
                    raise ValueError("demand claim is absent from retained commitments")
        for pool in self.pools:
            if pool.last_observation is None:
                continue
            for commitment in pool.last_observation.commitments:
                if retained.get(("physical", commitment.commitment_id)) != commitment:
                    raise ValueError("pool commitment is absent from retained commitments")
        return self


class _SearchBoundsEvidenceV1(StrictV1Model):
    max_allocation_decisions: Quantity
    topology_max_states: Quantity
    topology_deadline_milliseconds: Quantity


class _ShadowEvidenceV1(StrictV1Model):
    mode: Literal["shadow"] = "shadow"
    executable: Literal[False] = False
    executable_new_capacity_ceiling: Literal[0] = 0
    configuration_epoch: PositiveQuantity
    configuration_digest: Digest
    input_digest: Digest
    search_bounds: _SearchBoundsEvidenceV1
    shadow_epoch: ShadowEpochV1

    @model_validator(mode="after")
    def _consistent_shadow_epoch(self) -> _ShadowEvidenceV1:
        if self.configuration_epoch != self.shadow_epoch.configuration.configuration_epoch:
            raise ValueError("shadow evidence configuration epoch mismatch")
        if self.configuration_digest != canonical_digest(self.shadow_epoch.configuration):
            raise ValueError("shadow evidence configuration digest mismatch")
        if self.input_digest != self.shadow_epoch.input_digest:
            raise ValueError("shadow evidence input digest mismatch")
        return self


def _read_snapshot(path: Path) -> _EvidenceSnapshotV1:
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise ShadowOnceError("cannot read evidence snapshot") from exc
    if path.is_symlink() or not stat.S_ISREG(metadata.st_mode):
        raise ShadowOnceError("snapshot must be a regular nonsymlink file")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
        try:
            opened = os.fstat(descriptor)
            if (opened.st_dev, opened.st_ino) != (
                metadata.st_dev,
                metadata.st_ino,
            ) or not stat.S_ISREG(opened.st_mode):
                raise ShadowOnceError("snapshot changed while opening")
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
    except ShadowOnceError:
        raise
    except OSError as exc:
        raise ShadowOnceError("cannot read evidence snapshot") from exc
    if len(payload) > MAX_CONTRACT_BYTES:
        raise ShadowOnceError("evidence snapshot exceeds maximum byte size")
    try:
        return _EvidenceSnapshotV1.model_validate_json(payload)
    except (ValidationError, ValueError) as exc:
        raise ShadowOnceError("evidence snapshot contract is invalid") from exc


def _compose_input(*, fleet: Path, subjects: Path, snapshot: Path) -> AllocationInputV1:
    manifest = load_fleet_manifest(fleet)
    configurations = load_subject_configuration(subjects)
    for subject_configuration in configurations:
        for profile in subject_configuration.profiles:
            validate_profile_narrowing(manifest, profile)
    evidence = _read_snapshot(snapshot)

    configured_subjects = {
        (item.subject_id, item.subject_incarnation): item for item in configurations
    }
    evidence_subjects = {
        (item.subject_id, item.subject_incarnation): item for item in evidence.subjects
    }
    if set(configured_subjects) != set(evidence_subjects):
        raise ShadowOnceError("snapshot subject evidence is incomplete")
    configured_pools = {item.pool_id: item for item in manifest.pools}
    evidence_pools = {item.pool_id: item for item in evidence.pools}
    if set(configured_pools) != set(evidence_pools):
        raise ShadowOnceError("snapshot pool evidence is incomplete")

    subject_inputs: list[SubjectAllocationInputV1] = []
    for key, subject_configuration in sorted(
        configured_subjects.items(), key=lambda entry: entry[0][0].hex
    ):
        subject_evidence = evidence_subjects[key]
        demand_report = subject_evidence.last_demand
        if demand_report is not None and (
            demand_report.configuration_generation != subject_configuration.configuration_generation
            or demand_report.deployment_generation != subject_configuration.deployment_generation
            or demand_report.reporter_incarnation
            != subject_configuration.demand_reporter_incarnation
        ):
            raise ShadowOnceError("snapshot demand reporter binding is stale")
        subject_inputs.append(
            SubjectAllocationInputV1(
                configuration=subject_configuration,
                freshness=subject_evidence.freshness,
                last_demand=demand_report,
            )
        )

    pool_inputs: list[PoolAllocationInputV1] = []
    for pool_id, pool_configuration in sorted(configured_pools.items()):
        pool_evidence = evidence_pools[pool_id]
        pool_report = pool_evidence.last_observation
        if pool_report is not None and (
            pool_report.pool_generation != pool_configuration.pool_generation
            or pool_report.reporter_incarnation != pool_configuration.pool_reporter_incarnation
        ):
            raise ShadowOnceError("snapshot pool reporter binding is stale")
        pool_inputs.append(
            PoolAllocationInputV1(
                configuration=pool_configuration,
                freshness=pool_evidence.freshness,
                last_observation=pool_report,
            )
        )

    configuration_snapshot = ConfigurationSnapshotV1(
        configuration_epoch=evidence.configuration_epoch,
        fleet=ConfigurationGenerationRefV1(
            scope="fleet",
            generation=manifest.fleet_generation,
            digest=canonical_digest(manifest),
        ),
        subjects=tuple(
            ConfigurationGenerationRefV1(
                scope="subject",
                generation=item.configuration_generation,
                digest=canonical_digest(item),
                subject_id=item.subject_id,
                subject_incarnation=item.subject_incarnation,
            )
            for item in configurations
        ),
    )
    return AllocationInputV1(
        configuration=configuration_snapshot,
        fleet=manifest,
        subjects=tuple(subject_inputs),
        pools=tuple(pool_inputs),
        observed_commitments=evidence.observed_commitments,
        fairness_cursors=evidence.fairness_cursors,
        existing_pending_slots=evidence.existing_pending_slots,
        existing_pending_jobs=evidence.existing_pending_jobs,
    )


def _atomic_write(path: Path, document: _ShadowEvidenceV1) -> None:
    if not path.is_absolute() or not path.parent.is_dir():
        raise ShadowOnceError("output path must be absolute with an existing parent")
    try:
        existing = path.lstat()
    except FileNotFoundError:
        existing = None
    except OSError as exc:
        raise ShadowOnceError("cannot inspect output path") from exc
    if existing is not None and (path.is_symlink() or not stat.S_ISREG(existing.st_mode)):
        raise ShadowOnceError("output path must be a regular nonsymlink file")
    payload = canonical_bytes(document) + b"\n"
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    replaced = False
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            descriptor = -1
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        replaced = True
        directory_descriptor = os.open(path.parent, os.O_RDONLY | getattr(os, "O_CLOEXEC", 0))
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
    except OSError as exc:
        raise ShadowOnceError("cannot publish shadow evidence") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if not replaced:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass


def run_shadow_once(
    *,
    fleet: Path,
    subjects: Path,
    snapshot: Path,
    output: Path,
    max_allocation_decisions: int = MAX_ALLOCATION_DECISIONS,
    topology_max_states: int = 250_000,
    topology_deadline_milliseconds: int = 500,
) -> int:
    """Validate immutable inputs and atomically publish diagnostic evidence."""

    try:
        if type(topology_deadline_milliseconds) is not int or topology_deadline_milliseconds < 0:
            raise ShadowOnceError("topology_deadline_milliseconds must be a nonnegative integer")
        bounds = AllocatorSearchBounds(
            max_allocation_decisions=max_allocation_decisions,
            topology_max_states=topology_max_states,
            topology_deadline_seconds=topology_deadline_milliseconds / 1000,
        )
        allocation_input = _compose_input(
            fleet=fleet,
            subjects=subjects,
            snapshot=snapshot,
        )
        epoch = allocate_shadow(allocation_input, bounds=bounds)
        document = _ShadowEvidenceV1(
            configuration_epoch=allocation_input.configuration.configuration_epoch,
            configuration_digest=canonical_digest(allocation_input.configuration),
            input_digest=canonical_digest(allocation_input),
            search_bounds=_SearchBoundsEvidenceV1(
                max_allocation_decisions=bounds.max_allocation_decisions,
                topology_max_states=bounds.topology_max_states,
                topology_deadline_milliseconds=topology_deadline_milliseconds,
            ),
            shadow_epoch=epoch,
        )
        _atomic_write(output, document)
    except (
        CapacityContractError,
        FleetStateError,
        OSError,
        ShadowAllocatorError,
        ShadowOnceError,
        ValidationError,
        ValueError,
    ):
        return 2
    return 0


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fleet", required=True, type=Path)
    parser.add_argument("--subjects", required=True, type=Path)
    parser.add_argument("--snapshot", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument(
        "--max-allocation-decisions",
        type=int,
        default=MAX_ALLOCATION_DECISIONS,
    )
    parser.add_argument("--topology-max-states", type=int, default=250_000)
    parser.add_argument("--topology-deadline-milliseconds", type=int, default=500)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    result = run_shadow_once(
        fleet=args.fleet,
        subjects=args.subjects,
        snapshot=args.snapshot,
        output=args.output,
        max_allocation_decisions=args.max_allocation_decisions,
        topology_max_states=args.topology_max_states,
        topology_deadline_milliseconds=args.topology_deadline_milliseconds,
    )
    if result != 0:
        print("global capacity shadow evidence failed safely", file=sys.stderr)
    return result


if __name__ == "__main__":
    raise SystemExit(main())
