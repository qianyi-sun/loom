from __future__ import annotations

from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import cast

from loom_cli.rollout.operator.checkpoint_lease import build_restore_verified_lease
from loom_cli.rollout.operator.rehearsal_attestor import RehearsalLeaseAttestor
from loom_cli.rollout.preflight_pipeline import (
    PreflightAssessment,
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
