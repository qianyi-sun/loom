from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from uuid import UUID

import pytest
from fastapi import HTTPException

from loom.auth import AuthContext
from loom.integrations.behavior.contracts import BehaviorRolloutParametersV1
from loom.pipeline.image_runtime import ImageRuntimeRecord, ImageRuntimeRegistry
from loom.pipeline.keys import canonical_digest
from loom.pipeline.policy_config import PolicyConfigRegistry
from loom.pipeline.resource_profiles import ResourceProfileRegistry
from loom.pipeline.work_protocol import ImageRuntimeContractV1
from loom_service.app import create_app
from loom_service.config import LoomServiceSettings
from loom_service.pipeline_stage1_smoke_authority import (
    RepositoryStage1CandidateAuthority,
    Stage1CandidateSelectionV1,
)
from loom_service.routes.pipeline_stage1_smoke_prepare import _exact_team_and_user
from tests.unit.test_pipeline_stage1_smoke import DIGEST, IMAGE_INDEX, _candidate

REPO_ROOT = Path(__file__).resolve().parents[2]


class _Result:
    def __init__(self, value: Any) -> None:
        self._value = value

    def scalars(self) -> _Result:
        return self

    def scalar_one(self) -> object:
        return self._value

    def __iter__(self):  # type: ignore[no-untyped-def]
        return iter(self._value)


class _ReadOnlySession:
    def __init__(self, artifacts: list[object]) -> None:
        self._results = iter((_Result(artifacts), _Result(0), _Result(4)))

    async def execute(self, _statement: object) -> _Result:
        return next(self._results)


def _artifact(artifact_id: UUID, artifact_type: str) -> SimpleNamespace:
    return SimpleNamespace(
        id=artifact_id,
        artifact_type=artifact_type,
        team_id=UUID("00000000-0000-4000-8000-000000000001"),
        manifest_sha256=DIGEST,
        content_hash="sha256:" + artifact_id.hex[0] * 64,
        stored_size_bytes=100,
        unpacked_size_bytes=200,
        file_count=1,
        safety_state="verified_internal",
        producer_kind=None,
        pipeline_input_import_id=None,
        artifact_upload_session_id=None,
        provenance={},
    )


@pytest.mark.asyncio
async def test_candidate_is_composed_from_db_and_server_registries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile_registry = ResourceProfileRegistry.load()
    image_contract = ImageRuntimeContractV1(
        image_index_digest=IMAGE_INDEX,
        platform="linux/amd64",
        platform_manifest_digest="sha256:" + "3" * 64,
        cpu_arch="x86_64",
        gpu_vendor="nvidia",
        cuda_userspace_version="12.8",
        min_nvidia_driver_version="570.1",
        application_features=["isaac-sim-5.1", "omnigibson-3.8"],
        provider_assets=[],
        preflight_argv=["/opt/loom/bin/stage1-preflight"],
        preflight_digest="sha256:" + "4" * 64,
        sbom_digest="sha256:" + "5" * 64,
        attestation_digest="sha256:" + "6" * 64,
    )
    image_record = ImageRuntimeRecord(image_contract, canonical_digest(image_contract))
    image_registry = ImageRuntimeRegistry({image_record.key: image_record})
    policy_registry = PolicyConfigRegistry.load(
        resource_profiles=profile_registry,
        image_runtime_contracts=image_registry,
    )
    authority = RepositoryStage1CandidateAuthority(
        repo_root=REPO_ROOT,
        loom_commit_sha="a" * 40,
        environment="staging",
    )
    monkeypatch.setattr(
        authority,
        "_registries",
        lambda: (profile_registry, image_registry, policy_registry),
    )
    baseline = _candidate()
    now = datetime(2026, 8, 13, 18, tzinfo=UTC)
    selection = Stage1CandidateSelectionV1(
        team_id=baseline.team_id,
        backend_variant_id="oldlab-rtx5080-2gpu",
        image_index_digest=IMAGE_INDEX,
        task_instance_artifact_id=baseline.inputs[0].artifact_id,
        dataset_artifact_id=baseline.inputs[1].artifact_id,
        policy_artifact_id=baseline.inputs[2].artifact_id,
        parameters=BehaviorRolloutParametersV1.model_validate(baseline.parameters),
        run_budget=baseline.run_budget,
        stage_budget=baseline.stage_budget,
        expected_domain_outcome="rollout_success",
        start_by=now + timedelta(minutes=10),
        cleanup_deadline=now + timedelta(hours=5),
    )
    artifacts = [
        _artifact(item.artifact_id, item.artifact_type) for item in baseline.inputs
    ]
    candidate = await authority.prepare(
        _ReadOnlySession(artifacts),  # type: ignore[arg-type]
        operator_user_id=baseline.operator_user_id,
        selection=selection,
    )

    assert candidate.loom_commit_sha == "a" * 40
    assert candidate.policy_activation_epoch == 5
    assert candidate.platform_child_digest == image_contract.platform_manifest_digest
    assert candidate.image_runtime_contract_sha256 == image_record.snapshot_sha256
    assert candidate.policy_config_sha256 == policy_registry.get(
        "behavior-gpu-oldlab"
    ).policy_config_sha256
    assert [item.manifest_sha256 for item in candidate.inputs] == [DIGEST] * 3


def test_candidate_preparation_hides_cross_team_selection() -> None:
    team = UUID("00000000-0000-4000-8000-000000000001")
    other = UUID("00000000-0000-4000-8000-000000000002")
    ctx = AuthContext(
        token_hash=b"x",
        type="team",
        scopes=["read:own", "submit"],
        team_id=team,
        expires_at=None,
        user_id=UUID("00000000-0000-4000-8000-000000000003"),
    )
    with pytest.raises(HTTPException) as raised:
        _exact_team_and_user((SimpleNamespace(), ctx), other)  # type: ignore[arg-type]
    assert raised.value.status_code == 404


def test_candidate_authority_is_default_off_and_routes_are_hidden(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    for key, value in {
        "LOOM_SVC_DB_URL": "postgresql+psycopg://u:p@h/db",
        "LOOM_SVC_MINIO_ACCESS_KEY": "k",
        "LOOM_SVC_MINIO_SECRET_KEY": "s",
    }.items():
        monkeypatch.setenv(key, value)
    monkeypatch.delenv("LOOM_COMMIT_SHA", raising=False)
    monkeypatch.delenv("LOOM_ENV", raising=False)
    monkeypatch.setattr(
        "loom_service.pipeline_stage1_smoke_authority._IMAGE_BUILD_SHA_PATH",
        tmp_path / "missing-build-sha",
    )
    app = create_app(LoomServiceSettings(_env_file=None))

    assert not hasattr(app.state, "pipeline_stage1_candidate_authority")
    assert not hasattr(app.state, "pipeline_stage1_capacity_preflight_authority")
    assert not hasattr(app.state, "pipeline_stage1_execution_preflight_authority")
    assert not hasattr(app.state, "pipeline_stage1_evidence_authority")
    assert not hasattr(app.state, "pipeline_stage1_cleanup_authority")
    assert not any(
        "pipeline-stage1-smoke-preparation" in path
        for path in app.openapi().get("paths", {})
    )
