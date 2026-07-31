from __future__ import annotations

from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest

from loom_cli.rollout.operator.backup import BackupError
from loom_cli.rollout.operator.checkpoint_lease import build_restore_verified_lease
from loom_cli.rollout.operator.rehearsal_attestor import RehearsalLeaseAttestor
from loom_cli.rollout.operator.worker import _backup_failure_code
from loom_cli.rollout.preflight_pipeline import (
    PreflightAssessment,
    PreflightBlocker,
    PreflightPipeline,
    PreflightRehearsal,
)
from tests.loom_cli.rollout.operator.test_checkpoint_coordinator import _request
from tests.loom_cli.rollout.test_rehearsal_restore_evidence import NOW, _checkpoint, _rehearsal


class FakeStore:
    def __init__(self) -> None:
        self.rehearsal: PreflightRehearsal | None = None

    def publish_preflight_rehearsal(
        self,
        _request_id: str,
        rehearsal: PreflightRehearsal,
    ) -> object:
        self.rehearsal = rehearsal
        return object()

    def read_preflight_rehearsal(self, _request_id: str) -> PreflightRehearsal:
        assert self.rehearsal is not None
        return self.rehearsal


class FakePipeline:
    def __init__(self, rehearsal: PreflightRehearsal) -> None:
        self.rehearsal = rehearsal
        self.attested = False

    def rehearse(self, **_kwargs: object) -> PreflightRehearsal:
        return self.rehearsal

    def attest(self, **_kwargs: object) -> object:
        self.attested = True
        return SimpleNamespace(attestation_digest="e" * 64)


def test_attestor_persists_rehearsal_before_restore_and_binds_lease(
    tmp_path: Path,
    monkeypatch,
) -> None:
    checkpoint = _checkpoint(tmp_path)
    rehearsal, context = _rehearsal(checkpoint)
    store = FakeStore()
    pipeline = FakePipeline(rehearsal)
    attestor = RehearsalLeaseAttestor(
        pipeline=cast(PreflightPipeline, pipeline),
        context=context,
        assessment=cast(PreflightAssessment, object()),
        store=store,
        now=lambda: NOW,
    )
    request = replace(
        _request(),
        request_id=checkpoint.request_id,
        mutation_epoch=checkpoint.mutation_epoch,
    )
    # The fixture context uses another candidate; bind it to this request while
    # retaining the exact checkpoint evidence and Tier 3 results.
    bound = dict(context.bindings)
    bound["candidate.sha"] = request.candidate.resolved_sha
    attestor.context = context.__class__(bound)

    restore = attestor.verify_restore(checkpoint, request, lambda: False)
    lease = build_restore_verified_lease(
        checkpoint,
        restore,
        expires_at=NOW + timedelta(hours=2),
    )
    monkeypatch.setattr(
        "loom_cli.rollout.operator.rehearsal_attestor.derive_attestation_bindings",
        lambda *_args, **_kwargs: object(),
    )

    digest = attestor.publish_attestation(checkpoint, lease, request)

    assert store.rehearsal == rehearsal
    assert pipeline.attested
    assert digest == "e" * 64


def test_blocked_rehearsal_raises_typed_backup_error_with_component_codes(
    tmp_path: Path,
) -> None:
    # A blocked restore rehearsal must surface a typed `restore_rehearsal_blocked`
    # code (not a generic `backup_failed`) and preserve the component blocker
    # codes in the secret-safe diagnostic (#924).
    checkpoint = _checkpoint(tmp_path)
    rehearsal, context = _rehearsal(checkpoint)
    blocker = PreflightBlocker(
        check_id="database.restore",
        failure_code="database.runtime-image-binding-failed",
        outcome="blocked",
        blocked_by=(),
        remediation="rebind the runtime image",
        evidence={},
        evidence_hash="0" * 64,
    )
    blocked = replace(rehearsal, blockers=(blocker,))
    assert not blocked.passed
    store = FakeStore()
    pipeline = FakePipeline(blocked)
    attestor = RehearsalLeaseAttestor(
        pipeline=cast(PreflightPipeline, pipeline),
        context=context,
        assessment=cast(PreflightAssessment, object()),
        store=store,
        now=lambda: NOW,
    )
    request = replace(
        _request(),
        request_id=checkpoint.request_id,
        mutation_epoch=checkpoint.mutation_epoch,
    )
    bound = dict(context.bindings)
    bound["candidate.sha"] = request.candidate.resolved_sha
    attestor.context = context.__class__(bound)

    with pytest.raises(BackupError) as excinfo:
        attestor.verify_restore(checkpoint, request, lambda: False)

    error = excinfo.value
    # The typed stage code survives the worker's failure-code extraction, and the
    # component blocker code is carried in the diagnostic — instead of both being
    # collapsed to a bare `backup_failed` (which cost days on #838).
    assert error.code == "restore_rehearsal_blocked"
    assert _backup_failure_code(error) == "restore_rehearsal_blocked"
    assert "database.runtime-image-binding-failed" in (error.diagnostic or "")
    # The rehearsal never publishes when it is blocked.
    assert store.rehearsal is None
