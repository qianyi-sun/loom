from __future__ import annotations

import json
import os
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from loom_cli.rollout.rehearsal_action_source import RehearsalPlan, RehearsalResources
from loom_cli.rollout.rehearsal_helper import _load_plan, _verify_artifact_publication, main


def _plan() -> RehearsalPlan:
    return RehearsalPlan(
        candidate_sha="a" * 40,
        candidate_tree="b" * 40,
        checkpoint_evidence_sha256="c" * 64,
        checkpoint_manifest_path=Path("/data/loom-staging/backups/exact/backup-manifest.json"),
        checkpoint_manifest_sha256="d" * 64,
        mutation_epoch=8,
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


def _write_plan(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> tuple[RehearsalPlan, Path]:
    plan = _plan()
    root = tmp_path / "rehearsals"
    path = root / plan.resources.namespace / "plan.json"
    path.parent.mkdir(parents=True, mode=0o700)
    path.write_text(json.dumps(plan.to_record(), sort_keys=True, separators=(",", ":")) + "\n")
    path.chmod(0o600)
    monkeypatch.setattr(
        "loom_cli.rollout.rehearsal_helper.Path",
        lambda value: root if value == "/var/lib/loom-staging-rollout/rehearsals" else Path(value),
    )
    return plan, path


def test_helper_loads_exact_plan_and_returns_normalized_blocker(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    plan, path = _write_plan(monkeypatch, tmp_path)
    monkeypatch.setattr(
        "loom_cli.rollout.rehearsal_helper._verify_artifact_publication",
        lambda _plan: None,
    )

    assert (
        main(
            [
                "execute",
                "--check-id",
                "rehearsal.namespace",
                "--plan",
                str(path),
                "--plan-sha256",
                plan.plan_digest,
            ]
        )
        == 1
    )
    record = json.loads(capsys.readouterr().out)
    assert record["blockers"] == {"executor": "isolated-action-not-implemented"}
    assert record["plan_digest"] == plan.plan_digest
    assert record["passed"] is False


def test_helper_rejects_plan_drift_and_schema_confusion(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    plan, path = _write_plan(monkeypatch, tmp_path)
    monkeypatch.setattr(
        "loom_cli.rollout.rehearsal_helper._verify_artifact_publication",
        lambda _plan: None,
    )
    assert _load_plan(path, plan.plan_digest) == plan

    path.write_text('{"schema_version":1,"schema_version":1}\n')
    assert (
        main(
            [
                "execute",
                "--check-id",
                "rehearsal.namespace",
                "--plan",
                str(path),
                "--plan-sha256",
                plan.plan_digest,
            ]
        )
        == 2
    )


def test_rehearsal_plan_from_record_rejects_unknown_or_mistyped_fields() -> None:
    record = _plan().to_record()
    record["unknown"] = "value"
    with pytest.raises(ValueError, match="schema"):
        RehearsalPlan.from_record(record)


def test_helper_binds_every_artifact_publication_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = _plan()
    publication = SimpleNamespace(
        candidate_sha=plan.candidate_sha,
        candidate_tree=plan.candidate_tree,
        mutation_epoch=plan.mutation_epoch,
        descriptor_path=plan.artifact_descriptor_path,
        rendered_manifest_path=plan.rendered_manifest_path,
        image_artifact_sha256=plan.image_artifact_sha256,
        manifest_artifact_sha256=plan.manifest_artifact_sha256,
        rendered_manifest_sha256=plan.rendered_manifest_sha256,
        migration_plan_sha256=plan.migration_plan_sha256,
        browser_report_schema_sha256=plan.browser_report_schema_sha256,
    )

    class Store:
        def __init__(self, state_root: Path, *, service_uid: int) -> None:
            assert state_root == Path("/var/lib/loom-staging-rollout")
            assert service_uid == os.geteuid()

        def read(self, digest: str) -> SimpleNamespace:
            assert digest == plan.artifact_bundle_sha256
            return publication

    monkeypatch.setattr("loom_cli.rollout.rehearsal_helper.PreflightArtifactStore", Store)
    _verify_artifact_publication(plan)

    publication.candidate_sha = "9" * 40
    with pytest.raises(ValueError, match="publication drifted"):
        _verify_artifact_publication(replace(plan))

    record = _plan().to_record()
    record["mutation_epoch"] = True
    with pytest.raises(ValueError, match="schema"):
        RehearsalPlan.from_record(record)


def test_wrapper_never_inherits_ambient_environment() -> None:
    wrapper = Path(__file__).parents[3] / "deploy/staging-rollout/loom-staging-rollout-rehearsal"
    payload = wrapper.read_text()
    assert "/usr/bin/env -i" in payload
    assert "rehearsal-kubeconfig" in payload
    assert "LOOM_STAGING_ROLLOUT_CONFIG=/etc/loom/staging-rollout.toml" in payload
    assert os.access(wrapper, os.X_OK)
