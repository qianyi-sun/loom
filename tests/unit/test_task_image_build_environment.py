from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import pytest
from pydantic import ValidationError

from loom_control_plane.task_image_build_environment import (
    BuildEnvironmentDisabledError,
    RootlessBuildResourceRequestV1,
    SlurmBuildEnvironmentPolicyV1,
    SlurmBuildEnvironmentProvider,
    SlurmBuildInventoryV1,
    SlurmBuildRequestIdentityV1,
    issue_slurm_build_grant,
    render_rootless_builder_sbatch_request,
)

_GRANT_ID = UUID("11111111-1111-1111-1111-111111111111")


def _resources() -> RootlessBuildResourceRequestV1:
    return RootlessBuildResourceRequestV1(
        cpus=8,
        memory_mib=32768,
        pids=4096,
        scratch_bytes=107374182400,
        scratch_inodes=1000000,
        wall_time="02:00:00",
        swap_bytes=0,
    )


def _policy(*, enabled: bool = False, blockers: tuple[str, ...] = ("guard_missing",)):
    return SlurmBuildEnvironmentPolicyV1(
        schema="loom.task-image-build-environment-policy/v1",
        enabled=enabled,
        activation_blockers=blockers,
        slurm_cluster_id="gb10",
        cpu_arch="arm64",
        submitting_identity="loom-builder",
        partition="loom-task-builder",
        account="loom-task-builder",
        qos="loom-task-image-builder-rootless-gb10",
        feature_constraint="loom_rootless_buildkit",
        supervisor_path="/usr/local/libexec/loom-task-builder-supervisor",
        sbatch_path="/usr/bin/sbatch",
        resources=_resources(),
    )


def test_held_request_is_ordinary_allocation_without_host_runtime_authority() -> None:
    policy = _policy()
    grant = issue_slurm_build_grant(policy, grant_id=_GRANT_ID)

    request = render_rootless_builder_sbatch_request(policy, grant)

    assert request.args == (
        "/usr/bin/sbatch",
        "--parsable",
        "--hold",
        "--no-requeue",
        "--nodes=1",
        "--ntasks=1",
        "--cpus-per-task=8",
        "--mem=32768M",
        "--time=02:00:00",
        "--partition=loom-task-builder",
        "--account=loom-task-builder",
        "--qos=loom-task-image-builder-rootless-gb10",
        "--constraint=loom_rootless_buildkit",
        "--export=NONE",
        "--comment=loom-task-builder-v1:grant=11111111-1111-1111-1111-111111111111",
    )
    assert request.stdin == (
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n"
        "exec /usr/local/libexec/loom-task-builder-supervisor "
        "--grant-id 11111111-1111-1111-1111-111111111111\n"
    )
    rendered = "\n".join((*request.args, request.stdin))
    for forbidden in (
        "--exclusive",
        "--reservation",
        "--nodelist",
        "docker.sock",
        "DOCKER_HOST",
        "Bearer ",
        "registry_credentials",
    ):
        assert forbidden not in rendered


@pytest.mark.parametrize(
    ("cluster", "architecture", "qos"),
    [
        ("oldlab", "arm64", "loom-task-image-builder-rootless-oldlab"),
        ("gb10", "x86_64", "loom-task-image-builder-rootless-gb10"),
        ("oldlab", "x86_64", "loom-task-image-builder"),
    ],
)
def test_policy_rejects_cluster_architecture_or_legacy_qos_drift(
    cluster: str,
    architecture: str,
    qos: str,
) -> None:
    payload = _policy().model_dump()
    payload.update(slurm_cluster_id=cluster, cpu_arch=architecture, qos=qos)

    with pytest.raises(ValidationError):
        SlurmBuildEnvironmentPolicyV1.model_validate(payload)


def test_contracts_reject_unknown_fields_and_digest_or_comment_drift() -> None:
    resources = _resources().model_dump()
    resources["exclusive"] = False
    with pytest.raises(ValidationError, match="extra_forbidden"):
        RootlessBuildResourceRequestV1.model_validate(resources)

    policy = _policy()
    grant = issue_slurm_build_grant(policy, grant_id=_GRANT_ID)
    bad_digest = grant.model_dump()
    bad_digest["request_sha256"] = "f" * 64
    with pytest.raises(ValidationError, match="digest"):
        type(grant).model_validate(bad_digest)

    bad_comment = grant.model_dump()
    bad_comment["comment"] = "loom-task-builder-v1:grant=22222222-2222-2222-2222-222222222222"
    with pytest.raises(ValidationError, match="comment"):
        type(grant).model_validate(bad_comment)


def test_request_identity_rejects_forbidden_legacy_and_credential_fields() -> None:
    identity = issue_slurm_build_grant(_policy(), grant_id=_GRANT_ID).request.model_dump()
    for forbidden in (
        "reservation",
        "allowed_nodes",
        "nodelist",
        "exclusive",
        "docker_socket",
        "registry_credentials",
        "builder_token",
    ):
        payload = {**identity, forbidden: "forbidden"}
        with pytest.raises(ValidationError, match="extra_forbidden"):
            SlurmBuildRequestIdentityV1.model_validate(payload)


class _RecordingRunner:
    def __init__(self) -> None:
        self.submissions: list[Any] = []

    async def submit(self, request: Any) -> str:
        self.submissions.append(request)
        return "12345"

    async def inventory(self, grant: Any) -> SlurmBuildInventoryV1:
        del grant
        return SlurmBuildInventoryV1(
            controller_authoritative=True,
            accounting_authoritative=True,
            observed_at=datetime.now(UTC),
            jobs=(),
        )

    async def cancel(self, job_id: str) -> None:
        del job_id

    async def release(self, job_id: str) -> None:
        del job_id


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "policy",
    [
        _policy(enabled=False, blockers=()),
        _policy(enabled=False, blockers=("guard_missing",)),
    ],
)
async def test_disabled_or_blocked_provider_fails_before_runner_submission(
    policy: SlurmBuildEnvironmentPolicyV1,
) -> None:
    runner = _RecordingRunner()
    provider = SlurmBuildEnvironmentProvider(policy=policy, runner=runner)
    grant = issue_slurm_build_grant(policy, grant_id=_GRANT_ID)

    with pytest.raises(BuildEnvironmentDisabledError):
        await provider.submit_once(grant)

    assert runner.submissions == []


@pytest.mark.asyncio
async def test_enabled_unblocked_provider_submits_exact_rendered_request_once() -> None:
    policy = _policy(enabled=True, blockers=())
    runner = _RecordingRunner()
    provider = SlurmBuildEnvironmentProvider(policy=policy, runner=runner)
    grant = issue_slurm_build_grant(policy, grant_id=_GRANT_ID)

    assert await provider.submit_once(grant) == "12345"
    assert runner.submissions == [render_rootless_builder_sbatch_request(policy, grant)]
