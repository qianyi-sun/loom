from __future__ import annotations

import importlib
import os
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import ModuleType

import pytest

from loom_capacity_agent.contracts import AgentRegistrationV1
from loom_capacity_agent.legacy_fence import (
    LegacyCompatibilityFreezeV1,
    LegacyWriterCursorV1,
)
from loom_capacity_manager.executable_contracts import CandidateBindingV2
from tests.loom_cli.rollout.operator.test_installed_execution_authority import _publication


def _ceremony_module() -> ModuleType:
    try:
        return importlib.import_module("loom_cli.rollout.operator.legacy_writer_freeze_ceremony")
    except ModuleNotFoundError:
        pytest.fail("legacy writer freeze ceremony module is unavailable")


def _snapshot(
    module: ModuleType,
    freeze: LegacyCompatibilityFreezeV1,
    *,
    state: str,
    changed_field: tuple[str, object] | None = None,
    active_timer: bool = False,
):
    observations = []
    for index, frozen_cursor in enumerate(freeze.writer_cursors):
        cursor = LegacyWriterCursorV1(
            mutation_path_id=frozen_cursor.mutation_path_id,
            writer_domain=frozen_cursor.writer_domain,
            writer_incarnation=frozen_cursor.writer_incarnation,
            writer_epoch=frozen_cursor.writer_epoch,
            high_water=frozen_cursor.high_water,
            authority_digest=frozen_cursor.authority_digest,
        )
        if index == 0 and changed_field is not None:
            cursor = cursor.model_copy(update={changed_field[0]: changed_field[1]})
        runtime_state = "active" if active_timer and index == 0 else state
        observations.append(
            module.LegacyWriterRuntimeObservation(
                cursor=cursor,
                runtime_kind="timer" if index == 0 else "process",
                runtime_state=runtime_state,
                freeze_acknowledgement_digest=(
                    None
                    if runtime_state == "active"
                    else frozen_cursor.freeze_acknowledgement_digest
                ),
            )
        )
    return module.LegacyWriterRuntimeSnapshot(observations=tuple(observations))


class _Runtime:
    def __init__(self, module: ModuleType, freeze: LegacyCompatibilityFreezeV1) -> None:
        self.module = module
        self.freeze_evidence = freeze
        self.state = "active"
        self.changed_field: tuple[str, object] | None = None
        self.active_timer = False

    def capture(self):
        return _snapshot(
            self.module,
            self.freeze_evidence,
            state=self.state,
            changed_field=self.changed_field,
            active_timer=self.active_timer,
        )

    def freeze(self, _snapshot) -> None:
        self.state = "frozen"


class _FenceStore:
    def __init__(self) -> None:
        self.preparation = None
        self.freeze_evidence = None

    async def prepare(self, preparation):
        if self.preparation is not None and self.preparation != preparation:
            raise RuntimeError("conflicting preparation")
        self.preparation = preparation
        return preparation

    async def freeze(self, freeze):
        if self.freeze_evidence is not None and self.freeze_evidence != freeze:
            raise RuntimeError("conflicting freeze")
        self.freeze_evidence = freeze
        return freeze


def _binding(module: ModuleType, freeze: LegacyCompatibilityFreezeV1):
    registration = AgentRegistrationV1.model_validate(
        {field: getattr(freeze, field) for field in AgentRegistrationV1.model_fields}
    )
    return module.LegacyWriterFreezeBinding(
        registration=registration,
        preparation_id=freeze.preparation_id,
        freeze_id=freeze.freeze_id,
        compatibility_incarnation=freeze.compatibility_incarnation,
        fleet_migration_epoch=freeze.fleet_migration_epoch,
        compatibility_not_after=datetime.now(UTC) + timedelta(hours=1),
    )


def _publication_factory(module: ModuleType, base):
    def build(freeze: LegacyCompatibilityFreezeV1):
        acknowledgement = base.subject_acknowledgements[0]
        candidate = CandidateBindingV2(
            algorithm=freeze.candidate_identity_algorithm,
            identity=freeze.candidate_identity,
            publication_sha256=freeze.candidate_publication_sha256,
        )
        protected_admission = base.subject_protected_admission_sha256[str(freeze.subject_id)]
        acknowledgement = acknowledgement.model_copy(
            update={
                "legacy_writer_high_water": max(
                    cursor.high_water for cursor in freeze.writer_cursors
                ),
                "acknowledgement_sha256": (
                    module.execution_subject_acknowledgement_sha256(
                        freeze,
                        candidate=candidate,
                        protected_admission_sha256=protected_admission,
                    )
                ),
            }
        )
        return replace(
            base,
            subject_acknowledgements=(acknowledgement,),
            subject_freezes=(freeze,),
        )

    return build


def test_runtime_snapshot_rejects_incomplete_mutation_path_coverage(tmp_path: Path) -> None:
    """Catch freezing a subset while an omitted legacy path can still mutate state."""
    module = _ceremony_module()
    freeze = _publication(tmp_path).subject_freezes[0]
    complete = _snapshot(module, freeze, state="active")

    with pytest.raises(ValueError, match="complete mutation inventory"):
        module.LegacyWriterRuntimeSnapshot(observations=complete.observations[1:])


@pytest.mark.asyncio
async def test_ceremony_freezes_stable_writers_and_publishes_exact_owner_evidence(
    tmp_path: Path,
) -> None:
    """Catch publishing before real writers are frozen and database evidence is durable."""
    module = _ceremony_module()
    base = _publication(tmp_path)
    runtime = _Runtime(module, base.subject_freezes[0])
    store = _FenceStore()
    authority_root = tmp_path / "execution-authority"
    authority_root.mkdir(mode=0o700)
    authority_root.chmod(0o700)
    publisher = module.InstalledExecutionAuthorityPublisher(
        path=authority_root / "issue-906.json",
        expected_uid=os.geteuid(),
        expected_gid=os.getegid(),
    )
    ceremony = module.LegacyWriterFreezeCeremony(
        binding=_binding(module, base.subject_freezes[0]),
        runtime_source=runtime,
        runtime_freezer=runtime,
        fence_store=store,
        publication_factory=_publication_factory(module, base),
        publisher=publisher,
    )

    published = await ceremony.execute()

    assert runtime.state == "frozen"
    assert store.preparation is not None
    assert store.freeze_evidence == published.subject_freezes[0]
    assert module.InstalledExecutionAuthorityReader(
        path=authority_root / "issue-906.json",
        expected_uid=os.geteuid(),
        expected_gid=os.getegid(),
    )() == published


@pytest.mark.asyncio
@pytest.mark.parametrize("field,value", [("writer_epoch", 99), ("high_water", 18)])
async def test_ceremony_rejects_writer_epoch_or_high_water_drift(
    tmp_path: Path,
    field: str,
    value: object,
) -> None:
    """Catch admitting a writer that issued or reincarnated during the freeze boundary."""
    module = _ceremony_module()
    base = _publication(tmp_path)
    runtime = _Runtime(module, base.subject_freezes[0])
    store = _FenceStore()

    class _DriftingFreezer:
        def freeze(self, _snapshot) -> None:
            runtime.state = "frozen"
            runtime.changed_field = (field, value)

    ceremony = module.LegacyWriterFreezeCeremony(
        binding=_binding(module, base.subject_freezes[0]),
        runtime_source=runtime,
        runtime_freezer=_DriftingFreezer(),
        fence_store=store,
        publication_factory=_publication_factory(module, base),
        publisher=lambda publication: publication,
    )

    with pytest.raises(ValueError, match="changed while freezing"):
        await ceremony.execute()

    assert store.preparation is None


@pytest.mark.asyncio
async def test_ceremony_rejects_a_timer_that_remains_active(tmp_path: Path) -> None:
    """Catch treating a still-running timer or process as frozen evidence."""
    module = _ceremony_module()
    base = _publication(tmp_path)
    runtime = _Runtime(module, base.subject_freezes[0])
    runtime.active_timer = True
    store = _FenceStore()
    ceremony = module.LegacyWriterFreezeCeremony(
        binding=_binding(module, base.subject_freezes[0]),
        runtime_source=runtime,
        runtime_freezer=runtime,
        fence_store=store,
        publication_factory=_publication_factory(module, base),
        publisher=lambda publication: publication,
    )

    with pytest.raises(ValueError, match=r"timer.*remains active"):
        await ceremony.execute()

    assert store.preparation is None


@pytest.mark.asyncio
async def test_ceremony_rejects_post_freeze_writer_drift_before_publication(
    tmp_path: Path,
) -> None:
    """Catch a writer that changes after database freeze but before owner publication."""
    module = _ceremony_module()
    base = _publication(tmp_path)
    stable = _snapshot(module, base.subject_freezes[0], state="frozen")
    drifted = _snapshot(
        module,
        base.subject_freezes[0],
        state="frozen",
        changed_field=("high_water", 18),
    )

    class _SequenceSource:
        def __init__(self) -> None:
            self.values = iter((stable, stable, drifted))

        def capture(self):
            return next(self.values)

    store = _FenceStore()
    ceremony = module.LegacyWriterFreezeCeremony(
        binding=_binding(module, base.subject_freezes[0]),
        runtime_source=_SequenceSource(),
        runtime_freezer=lambda _snapshot: None,
        fence_store=store,
        publication_factory=_publication_factory(module, base),
        publisher=lambda publication: publication,
    )

    with pytest.raises(ValueError, match="changed after database freeze"):
        await ceremony.execute()


@pytest.mark.asyncio
async def test_ceremony_exact_replay_converges_without_replacing_publication(
    tmp_path: Path,
) -> None:
    """Catch an exact retry minting new fence identities or replacing admitted bytes."""
    module = _ceremony_module()
    base = _publication(tmp_path)
    runtime = _Runtime(module, base.subject_freezes[0])
    store = _FenceStore()
    authority_root = tmp_path / "execution-authority"
    authority_root.mkdir(mode=0o700)
    authority_root.chmod(0o700)
    authority_path = authority_root / "issue-906.json"
    ceremony = module.LegacyWriterFreezeCeremony(
        binding=_binding(module, base.subject_freezes[0]),
        runtime_source=runtime,
        runtime_freezer=runtime,
        fence_store=store,
        publication_factory=_publication_factory(module, base),
        publisher=module.InstalledExecutionAuthorityPublisher(
            path=authority_path,
            expected_uid=os.geteuid(),
            expected_gid=os.getegid(),
        ),
    )

    first = await ceremony.execute()
    first_inode = authority_path.stat(follow_symlinks=False).st_ino
    second = await ceremony.execute()

    assert second == first
    assert authority_path.stat(follow_symlinks=False).st_ino == first_inode
