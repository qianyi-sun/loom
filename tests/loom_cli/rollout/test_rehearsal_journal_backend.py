from __future__ import annotations

import os
from pathlib import Path

import pytest

from loom_cli.rollout.rehearsal_action_source import (
    RehearsalPlan,
    RehearsalResources,
    RehearsalSmokeAuthority,
)
from loom_cli.rollout.rehearsal_journal_backend import (
    JournaledRehearsalBackend,
    RehearsalStepOutcome,
)
from tests.loom_cli.rollout.rehearsal_fixtures import gb10_rehearsal_authority


def _plan(*, candidate_sha: str = "a" * 40) -> RehearsalPlan:
    return RehearsalPlan(
        candidate_sha=candidate_sha,
        candidate_tree="b" * 40,
        cluster_name="loom-staging",
        checkpoint_request_id="req-abcdefgh",
        checkpoint_evidence_sha256="c" * 64,
        checkpoint_manifest_path=Path("/data/loom-staging/backups/exact/backup-manifest.json"),
        checkpoint_manifest_sha256="d" * 64,
        mutation_epoch=7,
        db_snapshot_identity="pgdump-sha256:" + "e" * 64,
        object_inventory_root="f" * 64,
        schema_revision="0066",
        image_digests={
            "loom-control-plane": "sha256:" + "8" * 64,
            "loom-egress-xds": "sha256:" + "3" * 64,
            "loom-family-orchestrator": "sha256:" + "4" * 64,
            "loom-llm-gateway": "sha256:" + "5" * 64,
            "loom-rehearsal-postgres": "sha256:" + "9" * 64,
            "loom-service": "sha256:" + "1" * 64,
            "loom-staging-admin-browser-smoke": "sha256:" + "6" * 64,
            "loom-web": "sha256:" + "2" * 64,
            "loom-worker": "sha256:" + "7" * 64,
        },
        image_tag="staging-aaaaaaaa",
        image_artifact_sha256="2" * 64,
        artifact_bundle_sha256="6" * 64,
        artifact_descriptor_path=Path(
            "/var/lib/loom-staging-rollout/preflight-artifacts/" + "6" * 64 + "/artifact.json"
        ),
        rendered_manifest_path=Path(
            "/var/lib/loom-staging-rollout/preflight-artifacts/" + "6" * 64 + "/rendered.yaml"
        ),
        production_defaults_path=Path(
            "/var/lib/loom-staging-rollout/preflight-artifacts/"
            + "6" * 64
            + "/production-defaults.json"
        ),
        manifest_artifact_sha256="7" * 64,
        rendered_manifest_sha256="8" * 64,
        production_defaults_sha256="9" * 64,
        external_supervisor_artifact_sha256="a" * 64,
        external_supervisor_profile_sha256="b" * 64,
        external_supervisor_script_sha256={
            "scripts/ops/worker_pool_autoscaler_external_once.py": "c" * 64,
        },
        external_supervisor_unit_sha256={
            "loom-autoscaler-gb10-staging.service": "d" * 64,
            "loom-autoscaler-gb10-staging.timer": "e" * 64,
        },
        migration_plan_sha256="3" * 64,
        migration_target_revision="0067",
        browser_report_schema_sha256="4" * 64,
        resources=RehearsalResources.derive(
            "rehearsal-" + "5" * 24,
            route_origin="https://staging.example.test/dev",
        ),
        smoke_authority=RehearsalSmokeAuthority(
            represented_username="devansh",
            team_id="11111111-1111-4111-8111-111111111111",
            admin_actor="loom-staging-rollout",
            task_id="loom-smoke/gb10-oracle-hello-world",
            required_worker_pool="gb10-arm64",
            agent="oracle",
        ),
        gb10_authority=gb10_rehearsal_authority(),
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

    assert observation.blockers == {"cleanup-verification": "not-verified"}
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
