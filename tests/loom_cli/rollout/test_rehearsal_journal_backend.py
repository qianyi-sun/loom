from __future__ import annotations

import os
from pathlib import Path

import pytest

from loom_cli.rollout.rehearsal_action_source import RehearsalPlan, RehearsalResources
from loom_cli.rollout.rehearsal_journal_backend import (
    JournaledRehearsalBackend,
    RehearsalStepOutcome,
)


def _plan(*, candidate_sha: str = "a" * 40) -> RehearsalPlan:
    return RehearsalPlan(
        candidate_sha=candidate_sha,
        candidate_tree="b" * 40,
        checkpoint_evidence_sha256="c" * 64,
        checkpoint_manifest_path=Path("/data/loom-staging/backups/exact/backup-manifest.json"),
        checkpoint_manifest_sha256="d" * 64,
        mutation_epoch=7,
        db_snapshot_identity="pgdump-sha256:" + "e" * 64,
        object_inventory_root="f" * 64,
        schema_revision="0066",
        image_digests={"loom-service": "sha256:" + "1" * 64},
        image_artifact_sha256="2" * 64,
        artifact_bundle_sha256="6" * 64,
        artifact_descriptor_path=Path(
            "/var/lib/loom-staging-rollout/preflight-artifacts/" + "6" * 64 + "/artifact.json"
        ),
        rendered_manifest_path=Path(
            "/var/lib/loom-staging-rollout/preflight-artifacts/" + "6" * 64 + "/rendered.yaml"
        ),
        manifest_artifact_sha256="7" * 64,
        rendered_manifest_sha256="8" * 64,
        migration_plan_sha256="3" * 64,
        browser_report_schema_sha256="4" * 64,
        resources=RehearsalResources.derive(
            "rehearsal-" + "5" * 24,
            route_origin="https://staging.example.test/dev",
        ),
    )


def _root(tmp_path: Path) -> Path:
    root = tmp_path / "journals"
    root.mkdir(mode=0o700)
    return root


def test_backend_publishes_once_and_reuses_exact_record(tmp_path: Path) -> None:
    calls: list[str] = []

    def run(check_id: str, _plan: RehearsalPlan) -> RehearsalStepOutcome:
        calls.append(check_id)
        return RehearsalStepOutcome(passed=True, details={"status": "ready"}, blockers={})

    backend = JournaledRehearsalBackend(
        state_root=_root(tmp_path), service_uid=os.getuid(), run_step=run
    )

    first = backend.execute("rehearsal.namespace", _plan())
    second = backend.execute("rehearsal.namespace", _plan())

    assert first == second
    assert first.blockers == {}
    assert calls == ["rehearsal.namespace"]
    plan_record = next((tmp_path / "journals").rglob("plan.json"))
    assert plan_record.stat().st_mode & 0o777 == 0o600
    assert '"namespace":"loom-staging"' not in plan_record.read_text()


def test_backend_rejects_candidate_drift_against_existing_record(tmp_path: Path) -> None:
    backend = JournaledRehearsalBackend(
        state_root=_root(tmp_path),
        service_uid=os.getuid(),
        run_step=lambda _check_id, _plan: RehearsalStepOutcome(
            passed=True, details={"status": "ready"}, blockers={}
        ),
    )
    backend.execute("rehearsal.namespace", _plan())

    with pytest.raises(ValueError, match="publication collided"):
        backend.execute("rehearsal.namespace", _plan(candidate_sha="6" * 40))


def test_backend_records_failure_without_secret_diagnostics(tmp_path: Path) -> None:
    backend = JournaledRehearsalBackend(
        state_root=_root(tmp_path),
        service_uid=os.getuid(),
        run_step=lambda _check_id, _plan: RehearsalStepOutcome(
            passed=False,
            details={"status": "blocked"},
            blockers={"route": "candidate-mismatch"},
        ),
    )

    observation = backend.execute("rehearsal.browser", _plan())

    assert observation.blockers == {"route": "candidate-mismatch"}
    record = next(
        _root_path
        for _root_path in (tmp_path / "journals").rglob("*.json")
        if _root_path.name != "plan.json"
    )
    assert "candidate-mismatch" in record.read_text()
    assert "details" not in record.read_text()


def test_backend_requires_verified_cleanup(tmp_path: Path) -> None:
    backend = JournaledRehearsalBackend(
        state_root=_root(tmp_path),
        service_uid=os.getuid(),
        run_step=lambda _check_id, _plan: RehearsalStepOutcome(
            passed=True, details={"status": "removed"}, blockers={}
        ),
    )

    observation = backend.execute("rehearsal.cleanup", _plan())

    assert observation.blockers == {"cleanup": "not-verified"}
    assert not observation.cleanup_verified


def test_backend_seals_runner_failure_without_retrying_or_leaking_error(tmp_path: Path) -> None:
    calls = 0

    def fail(_check_id: str, _plan: RehearsalPlan) -> RehearsalStepOutcome:
        nonlocal calls
        calls += 1
        raise RuntimeError("token=must-not-escape")

    root = _root(tmp_path)
    backend = JournaledRehearsalBackend(
        state_root=root,
        service_uid=os.getuid(),
        run_step=fail,
    )

    first = backend.execute("rehearsal.api-smoke", _plan())
    second = backend.execute("rehearsal.api-smoke", _plan())

    assert first == second
    assert first.blockers == {"executor": "isolated-action-failed"}
    assert calls == 1
    assert "must-not-escape" not in next(root.rglob("*.json")).read_text()


def test_backend_rejects_symlinked_or_permissive_root(tmp_path: Path) -> None:
    target = _root(tmp_path)
    link = tmp_path / "link"
    link.symlink_to(target)
    backend = JournaledRehearsalBackend(
        state_root=link,
        service_uid=os.getuid(),
        run_step=lambda _check_id, _plan: RehearsalStepOutcome(
            passed=True, details={"status": "ready"}, blockers={}
        ),
    )
    with pytest.raises(ValueError, match="root authority"):
        backend.execute("rehearsal.namespace", _plan())

    target.chmod(0o755)
    backend = JournaledRehearsalBackend(
        state_root=target,
        service_uid=os.getuid(),
        run_step=backend.run_step,
    )
    with pytest.raises(ValueError, match="root authority"):
        backend.execute("rehearsal.namespace", _plan())
