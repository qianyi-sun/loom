from __future__ import annotations

import asyncio
import os
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import pytest
from pydantic import ValidationError

import loom_capacity_executor.slurm_backend as slurm_backend
from loom_capacity_executor.slurm_backend import (
    SlurmAuthorityError,
    SlurmOutputError,
    SlurmStateConflictError,
    SlurmSubmissionUncertainError,
)
from loom_capacity_executor.slurm_contracts import (
    SlurmCancelRequestV2,
    SlurmExecutableIdentityV2,
    SlurmFileIdentityV2,
    SlurmLaunchRequestV2,
    SlurmResourceV2,
    SlurmTresValueV2,
)
from tests.support.fake_slurm import FakeSlurm, assert_secure_evidence


@pytest.fixture
def fake_slurm(tmp_path: Path) -> FakeSlurm:
    return FakeSlurm(tmp_path / "fake-slurm")


def slurm_launch_request_fixture(fake_slurm: FakeSlurm) -> SlurmLaunchRequestV2:
    return SlurmLaunchRequestV2(
        cluster="oldlab",
        controller_host="ctl.oldlab.internal",
        partition="loom",
        account="loom-executor",
        submitter="loom-oldlab",
        qos="loom",
        job_name="loom-worker",
        operation_id=UUID("00000000-0000-0000-0000-000000000101"),
        nodes=("oldlab-5",),
        features=("x86_64",),
        cpus=16,
        memory_bytes=64 * 1024 * 1024 * 1024,
        gpus=2,
        time_limit_seconds=3600,
        launcher=SlurmExecutableIdentityV2(
            path=str(fake_slurm.launcher),
            sha256=fake_slurm.launcher_sha256,
            owner_uid=fake_slurm.launcher.stat().st_uid,
        ),
        trusted_launcher_config=SlurmFileIdentityV2(
            path=str(fake_slurm.root / "trusted-launcher.json"),
            sha256="d" * 64,
            owner_uid=os.geteuid(),
        ),
        launcher_release_sha256="b" * 64,
        image_digest="registry.internal/loom/worker@sha256:" + "c" * 64,
        ownership_token="A" * 43,
    )


def cancel_request(job_id: str = "101") -> SlurmCancelRequestV2:
    return SlurmCancelRequestV2(
        cluster="oldlab",
        job_id=job_id,
        submitter="loom-oldlab",
        account="loom-executor",
        partition="loom",
        cpus=16,
        memory_bytes=64 * 1024 * 1024 * 1024,
        gpus=2,
        generic_tres=(),
        nodes=("oldlab-5",),
        ownership_token="A" * 43,
        ownership_evidence_sha256="0" * 64,
    )


def test_launch_contract_has_no_candidate_script_or_freeform_argv(
    fake_slurm: FakeSlurm,
) -> None:
    request = slurm_launch_request_fixture(fake_slurm)
    payload = request.model_dump()
    for forbidden in ("candidate", "script", "argv", "command", "shell"):
        with pytest.raises(ValidationError):
            SlurmLaunchRequestV2.model_validate({**payload, forbidden: "$(scancel 1)"})
    with pytest.raises(ValidationError, match="duplicate"):
        SlurmLaunchRequestV2.model_validate({**payload, "nodes": ("oldlab-5", "oldlab-5")})
    with pytest.raises(ValidationError):
        SlurmLaunchRequestV2.model_validate({**payload, "job_name": "a;scancel-1"})


@pytest.mark.parametrize(
    "path",
    (
        "/usr//bin/sbatch",
        "/usr/./bin/sbatch",
        "/usr/local/../bin/sbatch",
        "//usr/bin/sbatch",
        "/usr/bin/sbatch/",
    ),
)
def test_slurm_executable_identity_rejects_noncanonical_absolute_paths(path: str) -> None:
    with pytest.raises(ValidationError, match="canonical"):
        SlurmExecutableIdentityV2(path=path, sha256="a" * 64, owner_uid=0)


@pytest.mark.parametrize(
    "value",
    ("\t", "\x01", "\x1b", "\x7f", "\u0085", "\u009f", "\u2028", "\u2029"),
)
def test_scheduler_output_rejects_non_line_control_data(value: str) -> None:
    with pytest.raises(SlurmOutputError, match="control data"):
        slurm_backend._decode_output(f"record{value}".encode(), command="squeue")


@pytest.mark.parametrize("name", ("billing", "cpu", "mem", "node", "gres/gpu"))
def test_generic_tres_rejects_reserved_scheduler_aggregates(name: str) -> None:
    with pytest.raises(ValidationError, match="reserved"):
        SlurmTresValueV2(name=name, value=1)


def test_generic_tres_contract_rejects_sixty_five_entries() -> None:
    with pytest.raises(ValidationError):
        SlurmResourceV2(
            generic_tres=tuple(
                SlurmTresValueV2(name=f"gres/fpga{index:02d}", value=1) for index in range(65)
            )
        )


@pytest.mark.asyncio
async def test_submit_uses_absolute_argv_without_shell_or_inherited_environment(
    fake_slurm: FakeSlurm, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("LOOM_TEST_UNTRUSTED_SECRET", "must-not-cross-boundary")
    result = await fake_slurm.backend().submit(slurm_launch_request_fixture(fake_slurm))
    assert result.job_id == "101"
    assert result.cluster == "oldlab"
    assert all(call.shell is False for call in fake_slurm.calls)
    assert all(Path(call.executable).is_absolute() for call in fake_slurm.calls)
    call = fake_slurm.sbatch_calls[0]
    assert "LOOM_TEST_UNTRUSTED_SECRET" not in call.environment
    assert call.argv == (
        "--parsable",
        "--clusters=oldlab",
        "--partition=loom",
        "--account=loom-executor",
        "--qos=loom",
        "--job-name=loom-worker",
        "--nodelist=oldlab-5",
        "--nodes=1",
        "--ntasks=1",
        "--cpus-per-task=16",
        "--mem=65536M",
        "--gpus=2",
        "--constraint=x86_64",
        "--time=0-01:00:00",
        "--comment=" + "A" * 43,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("cluster", "host"),
    (("other", None), (None, "other.internal")),
)
async def test_submit_validates_controller_and_cluster_before_mutation(
    fake_slurm: FakeSlurm, cluster: str | None, host: str | None
) -> None:
    fake_slurm.set_controller(cluster=cluster, host=host)
    with pytest.raises(SlurmAuthorityError):
        await fake_slurm.backend().submit(slurm_launch_request_fixture(fake_slurm))
    assert fake_slurm.sbatch_calls == ()


@pytest.mark.asyncio
async def test_submit_rejects_same_scope_request_for_another_controller_host(
    fake_slurm: FakeSlurm,
) -> None:
    request = slurm_launch_request_fixture(fake_slurm).model_copy(
        update={"controller_host": "other.internal"}
    )

    with pytest.raises(SlurmAuthorityError, match="controller"):
        await fake_slurm.backend().submit(request)

    assert fake_slurm.sbatch_calls == ()


@pytest.mark.asyncio
async def test_authority_accepts_standard_config_envelope_and_indexed_controller_list(
    fake_slurm: FakeSlurm,
) -> None:
    authority = await fake_slurm.backend().validate_authority()
    assert authority.cluster == "oldlab"
    assert authority.controller_host == "ctl.oldlab.internal"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "output",
    (
        "ClusterName = oldlab\nClusterName = oldlab\n"
        "SlurmctldHost[0] = ctl.oldlab.internal(192.0.2.10)\n",
        "ClusterName = oldlab\n"
        "SlurmctldHost[0] = ctl.oldlab.internal(192.0.2.10)\n"
        "SlurmctldHost[0] = ctl.oldlab.internal(192.0.2.10)\n",
        "ClusterName oldlab\nSlurmctldHost[0] = ctl.oldlab.internal(192.0.2.10)\n",
        "ClusterName = oldlab\nSlurmctldHost[0] ctl.oldlab.internal(192.0.2.10)\n",
    ),
)
async def test_authority_rejects_duplicate_or_malformed_target_facts(
    fake_slurm: FakeSlurm,
    output: str,
) -> None:
    fake_slurm.set_output("scontrol", output)
    with pytest.raises(SlurmAuthorityError):
        await fake_slurm.backend().validate_authority()


@pytest.mark.asyncio
async def test_authority_uses_fixed_native_association_query(fake_slurm: FakeSlurm) -> None:
    await fake_slurm.backend().validate_authority()
    call = next(item for item in fake_slurm.calls if Path(item.executable).name == "sacctmgr")
    assert call.argv == (
        "--noheader",
        "--parsable2",
        "show",
        "association",
        "where",
        "Cluster=oldlab",
        "Account=loom-executor",
        "User=loom-oldlab",
        "format=Cluster,Account,User,Partition,QOS",
    )


@pytest.mark.asyncio
async def test_submit_rechecks_executable_identity_before_mutation(fake_slurm: FakeSlurm) -> None:
    backend = fake_slurm.backend()
    launcher = SlurmExecutableIdentityV2(
        path=str(fake_slurm.mutable_launcher),
        sha256=fake_slurm._digest(fake_slurm.mutable_launcher),
        owner_uid=os.geteuid(),
    )
    request = slurm_launch_request_fixture(fake_slurm).model_copy(update={"launcher": launcher})
    fake_slurm.mutable_launcher.write_text("#!/bin/sh\nexit 7\n", encoding="utf-8")
    fake_slurm.mutable_launcher.chmod(0o700)
    with pytest.raises(SlurmAuthorityError, match="digest"):
        await backend.submit(request)
    assert fake_slurm.sbatch_calls == ()


@pytest.mark.asyncio
async def test_command_executes_verified_open_inode_when_path_is_replaced(
    fake_slurm: FakeSlurm,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_create = asyncio.create_subprocess_exec
    scontrol = fake_slurm.bin / "scontrol"
    replacement = fake_slurm.bin / "replacement"
    replacement.write_text("#!/usr/bin/python3\nprint('ClusterName = foreign')\n", encoding="utf-8")
    replacement.chmod(0o700)
    replaced = False

    async def replace_before_exec(*argv: str, **kwargs: object) -> asyncio.subprocess.Process:
        nonlocal replaced
        if not replaced and argv[0] == str(scontrol):
            replaced = True
            os.replace(replacement, scontrol)
        return await real_create(*argv, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(
        "loom_capacity_executor.slurm_backend.asyncio.create_subprocess_exec",
        replace_before_exec,
    )
    authority = await fake_slurm.backend().validate_authority()
    assert replaced is True
    assert authority.cluster == "oldlab"


@pytest.mark.asyncio
async def test_submit_rejects_launcher_mutable_by_executor_identity(fake_slurm: FakeSlurm) -> None:
    launcher = SlurmExecutableIdentityV2(
        path=str(fake_slurm.mutable_launcher),
        sha256=fake_slurm._digest(fake_slurm.mutable_launcher),
        owner_uid=os.geteuid(),
    )
    request = slurm_launch_request_fixture(fake_slurm).model_copy(update={"launcher": launcher})
    with pytest.raises(SlurmAuthorityError, match=r"launcher.*immutable"):
        await fake_slurm.backend().submit(request)
    assert fake_slurm.sbatch_calls == ()


@pytest.mark.asyncio
async def test_spooled_verifier_rejects_replaced_launcher_even_when_it_ignores_digest_argument(
    fake_slurm: FakeSlurm,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_uid = os.geteuid()
    executor_uid = real_uid + 1
    backend = fake_slurm.backend()
    backend = slurm_backend.AsyncSlurmBackend(
        backend.authority.model_copy(update={"local_uid": executor_uid})
    )
    monkeypatch.setattr("loom_capacity_executor.slurm_backend.os.geteuid", lambda: executor_uid)

    cache = Path.home() / ".cache"
    with tempfile.TemporaryDirectory(prefix="loom-launcher-", dir=cache) as directory:
        authority_directory = Path(directory) / "authority"
        authority_directory.mkdir(mode=0o700)
        launcher = authority_directory / "trusted-launcher"
        marker = Path(directory) / "executed"
        launcher.write_text(
            "#!/usr/bin/python3\n"
            "from pathlib import Path\n"
            f"Path({str(marker)!r}).write_text('approved', encoding='utf-8')\n",
            encoding="utf-8",
        )
        launcher.chmod(0o700)
        request = slurm_launch_request_fixture(fake_slurm).model_copy(
            update={
                "launcher": SlurmExecutableIdentityV2(
                    path=str(launcher),
                    sha256=fake_slurm._digest(launcher),
                    owner_uid=real_uid,
                )
            }
        )

        approved_submission = await backend.submit(request)
        approved = fake_slurm.run_submitted_job(approved_submission.job_id)
        assert approved.returncode == 0
        assert marker.read_text(encoding="utf-8") == "approved"
        marker.unlink()

        symlink_submission = await backend.submit(request)
        redirected_directory = Path(directory) / "redirected"
        redirected_directory.mkdir(mode=0o700)
        redirected_launcher = redirected_directory / launcher.name
        redirected_launcher.write_bytes(launcher.read_bytes())
        redirected_launcher.chmod(0o700)
        original_directory = Path(directory) / "authority-original"
        authority_directory.rename(original_directory)
        authority_directory.symlink_to(redirected_directory, target_is_directory=True)

        symlinked = fake_slurm.run_submitted_job(symlink_submission.job_id)
        assert symlinked.returncode != 0
        assert not marker.exists()

        authority_directory.unlink()
        original_directory.rename(authority_directory)
        submitted = await backend.submit(request)
        replacement = Path(directory) / "replacement"
        replacement.write_text(
            "#!/usr/bin/python3\n"
            "from pathlib import Path\n"
            f"Path({str(marker)!r}).write_text('malicious', encoding='utf-8')\n",
            encoding="utf-8",
        )
        replacement.chmod(0o700)
        replacement.replace(launcher)

        executed = fake_slurm.run_submitted_job(submitted.job_id)
        assert executed.returncode != 0
        assert not marker.exists()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "output",
    ("101\n", "101;oldlab\n102;oldlab\n", "101;foreign\n", "not-a-job;oldlab\n"),
)
async def test_submit_treats_malformed_duplicate_or_unknown_result_as_uncertain(
    fake_slurm: FakeSlurm, output: str
) -> None:
    fake_slurm.set_output("sbatch", output)
    with pytest.raises(SlurmSubmissionUncertainError):
        await fake_slurm.backend().submit(slurm_launch_request_fixture(fake_slurm))
    assert len(fake_slurm.sbatch_calls) == 1


@pytest.mark.asyncio
async def test_submit_treats_verifier_stdin_failure_as_uncertain(
    fake_slurm: FakeSlurm,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def reject_stdin(stream: asyncio.StreamWriter, payload: bytes) -> None:
        stream.close()
        raise BrokenPipeError

    monkeypatch.setattr(slurm_backend, "_write_bounded", reject_stdin)
    with pytest.raises(SlurmSubmissionUncertainError):
        await fake_slurm.backend().submit(slurm_launch_request_fixture(fake_slurm))


@pytest.mark.asyncio
async def test_subprocess_timeout_and_output_are_bounded(fake_slurm: FakeSlurm) -> None:
    fake_slurm.set_fault("scontrol", "timeout")
    started = time.monotonic()
    with pytest.raises(SlurmAuthorityError, match="timed out"):
        await fake_slurm.backend(command_timeout_seconds=0.1).validate_authority()
    assert time.monotonic() - started < 2.0

    fake_slurm.set_fault("scontrol", "oversize")
    with pytest.raises(SlurmAuthorityError, match="output"):
        await fake_slurm.backend(max_stdout_bytes=128).validate_authority()


@pytest.mark.asyncio
async def test_command_deadline_bounds_descendant_held_pipes(fake_slurm: FakeSlurm) -> None:
    fake_slurm.set_fault("scontrol", "descendant_pipe")
    started = time.monotonic()
    with pytest.raises(SlurmAuthorityError, match="timed out"):
        await fake_slurm.backend(command_timeout_seconds=0.1).validate_authority()
    assert time.monotonic() - started < 2.0


@pytest.mark.asyncio
async def test_inventory_parses_complete_fixed_fields_and_rejects_unknown_data(
    fake_slurm: FakeSlurm,
) -> None:
    fake_slurm.add_job()
    observations = await fake_slurm.backend().inventory()
    assert len(observations) == 1
    observation = observations[0]
    assert observation.job_id == "101"
    assert observation.state == "PENDING"
    assert observation.nodes == ("oldlab-5",)
    assert observation.memory_bytes == 64 * 1024 * 1024 * 1024

    fake_slurm.set_job_state("101", "UNRECOGNIZED")
    with pytest.raises(SlurmOutputError, match="state"):
        await fake_slurm.backend().inventory()
    fake_slurm.set_output("squeue", "101|PENDING|too|few\n")
    with pytest.raises(SlurmOutputError, match="field"):
        await fake_slurm.backend().inventory()


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ("RUNNING", "CONFIGURING"))
async def test_inventory_cross_checks_nonpending_reason_field_as_nodes(
    fake_slurm: FakeSlurm,
    state: str,
) -> None:
    fake_slurm.add_job(state=state)
    observation = (await fake_slurm.backend().inventory())[0]
    assert observation.state == state
    assert observation.nodes == ("oldlab-5",)
    assert observation.pending_reason is None

    fake_slurm.set_output(
        "squeue",
        f"101|{state}|loom-oldlab|loom-executor|loom|16|65536M|gpu:2|"
        f"oldlab-5|foreign-node|{'A' * 43}\n",
    )
    with pytest.raises(SlurmOutputError, match="resource"):
        await fake_slurm.backend().inventory()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("state", "submitter", "account"),
    (
        ("RUNNING", None, None),
        ("PENDING", "foreign-user", None),
        ("PENDING", None, "foreign-account"),
    ),
)
async def test_cancel_pending_rechecks_exact_state_and_association(
    fake_slurm: FakeSlurm,
    state: str,
    submitter: str | None,
    account: str | None,
) -> None:
    fake_slurm.add_job(state=state, submitter=submitter, account=account)
    with pytest.raises(SlurmStateConflictError):
        await fake_slurm.backend().cancel_pending(cancel_request())
    assert fake_slurm.scancel_calls == ()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    (
        {"partition": "loom-debug"},
        {"cpus": 32},
        {"memory_bytes": 128 * 1024 * 1024 * 1024},
        {"gpus": 3, "gres": "gpu:3"},
        {"nodes": ["oldlab-6"]},
        {"ownership_token": "B" * 43},
        {
            "generic_tres": {"gres/fpga:vu9p": 1},
            "gres": "gpu:2,fpga:vu9p:1",
        },
    ),
)
async def test_cancel_pending_rejects_job_reuse_or_scheduler_field_mismatch(
    fake_slurm: FakeSlurm,
    change: dict[str, object],
) -> None:
    fake_slurm.add_job()
    fake_slurm.replace_job("101", **change)
    backend = fake_slurm.backend(
        resource_ceiling=SlurmResourceV2(
            cpus=64,
            memory_bytes=512 * 1024 * 1024 * 1024,
            gpus=8,
            generic_tres=(SlurmTresValueV2(name="gres/fpga:vu9p", value=1),),
        )
    )

    with pytest.raises(SlurmStateConflictError):
        await backend.cancel_pending(cancel_request())

    assert fake_slurm.scancel_calls == ()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "output",
    (
        "",
        "101|UNRECOGNIZED|loom-oldlab|loom-executor|loom|16|65536M|gpu:2|oldlab-5|Resources|"
        + "A" * 43
        + "\n",
        "101|PENDING|loom-oldlab|loom-executor|loom|16|65536M|gpu:2|oldlab-5|Resources|"
        + "A" * 43
        + "\n"
        + "101|PENDING|loom-oldlab|loom-executor|loom|16|65536M|gpu:2|oldlab-5|Resources|"
        + "A" * 43
        + "\n",
    ),
)
async def test_cancel_pending_rejects_missing_unknown_or_duplicate_evidence_before_scancel(
    fake_slurm: FakeSlurm,
    output: str,
) -> None:
    fake_slurm.set_output("squeue", output)

    with pytest.raises(SlurmStateConflictError):
        await fake_slurm.backend().cancel_pending(cancel_request())

    assert fake_slurm.scancel_calls == ()


@pytest.mark.asyncio
async def test_cancel_pending_rejects_proof_digest_mismatch_before_scancel(
    fake_slurm: FakeSlurm,
) -> None:
    fake_slurm.add_job()
    request = cancel_request().model_copy(update={"ownership_evidence_sha256": "1" * 64})

    with pytest.raises(SlurmStateConflictError):
        await fake_slurm.backend().cancel_pending(request)

    assert fake_slurm.scancel_calls == ()


@pytest.mark.asyncio
async def test_cancel_pending_uses_scheduler_predicates_after_exact_reobservation(
    fake_slurm: FakeSlurm,
) -> None:
    fake_slurm.add_job()
    observed = await fake_slurm.backend().cancel_pending(cancel_request())
    assert observed.job_id == "101"
    assert observed.state == "PENDING"
    call = fake_slurm.scancel_calls[0]
    assert call.argv == (
        "--clusters=oldlab",
        "--state=PENDING",
        "--user=loom-oldlab",
        "--account=loom-executor",
        "101",
    )
    assert Path(fake_slurm.calls[-2].executable).name == "squeue"
    assert Path(fake_slurm.calls[-1].executable).name == "scancel"


@pytest.mark.asyncio
async def test_cancel_pending_treats_unexpected_success_output_as_uncertain(
    fake_slurm: FakeSlurm,
) -> None:
    fake_slurm.add_job()
    fake_slurm.set_output("scancel", "unexpected-success-output\n")
    with pytest.raises(slurm_backend.SlurmCancellationUncertainError):
        await fake_slurm.backend().cancel_pending(cancel_request())
    assert len(fake_slurm.scancel_calls) == 1


@pytest.mark.asyncio
async def test_accounting_high_water_returns_only_exact_terminal_evidence(
    fake_slurm: FakeSlurm,
) -> None:
    fake_slurm.add_terminal_job()
    result = await fake_slurm.backend().accounting_high_water(
        since=datetime(2026, 8, 13, 0, 0, tzinfo=UTC)
    )
    assert result.cluster == "oldlab"
    assert result.account == "loom-executor"
    assert result.submitter == "loom-oldlab"
    assert result.since == datetime(2026, 8, 13, 0, 0, tzinfo=UTC)
    assert result.terminal_jobs[0].job_id == "99"
    assert result.terminal_jobs[0].state == "COMPLETED"
    assert result.terminal_jobs[0].partition == "loom"
    assert result.terminal_jobs[0].ended_at == datetime(2026, 8, 13, 12, 3, tzinfo=UTC)

    fake_slurm.add_terminal_job(job_id="100", state="RUNNING")
    with pytest.raises(SlurmOutputError, match="terminal"):
        await fake_slurm.backend().accounting_high_water(
            since=datetime(2026, 8, 13, 0, 0, tzinfo=UTC)
        )


@pytest.mark.asyncio
async def test_accounting_rejects_incomplete_tres_even_with_known_extra_fields(
    fake_slurm: FakeSlurm,
) -> None:
    fake_slurm.set_output(
        "sacct",
        "99|COMPLETED|loom-oldlab|loom-executor|oldlab|"
        "2026-08-13T12:00:00Z|2026-08-13T12:01:00Z|2026-08-13T12:03:00Z|"
        "120|0:0|16|65536M|billing=16,cpu=16,mem=65536M|oldlab-5|" + "A" * 43 + "\n",
    )
    with pytest.raises(SlurmOutputError, match="terminal"):
        await fake_slurm.backend().accounting_high_water(
            since=datetime(2026, 8, 13, 0, 0, tzinfo=UTC)
        )


@pytest.mark.asyncio
async def test_typed_gpu_and_generic_tres_round_trip_submission_inventory_and_accounting(
    fake_slurm: FakeSlurm,
) -> None:
    tres = (
        SlurmTresValueV2(name="gres/fpga:vu9p", value=1),
        SlurmTresValueV2(name="gres/gpu:a100", value=2),
    )
    ceiling = SlurmResourceV2(
        cpus=64,
        memory_bytes=512 * 1024 * 1024 * 1024,
        gpus=8,
        generic_tres=tres,
    )
    request = slurm_launch_request_fixture(fake_slurm).model_copy(update={"generic_tres": tres})
    backend = fake_slurm.backend(resource_ceiling=ceiling)

    submitted = await backend.submit(request)
    observed = (await backend.inventory())[0]
    assert submitted.job_id == observed.job_id
    assert (observed.cpus, observed.memory_bytes, observed.gpus, observed.nodes) == (
        request.cpus,
        request.memory_bytes,
        request.gpus,
        request.nodes,
    )
    assert observed.generic_tres == tres

    fake_slurm.terminalize_job(submitted.job_id)
    terminal = (
        await backend.accounting_high_water(since=datetime(2026, 8, 13, 0, 0, tzinfo=UTC))
    ).terminal_jobs[0]
    assert (terminal.cpus, terminal.memory_bytes, terminal.gpus, terminal.nodes) == (
        request.cpus,
        request.memory_bytes,
        request.gpus,
        request.nodes,
    )
    assert terminal.generic_tres == tres
    assert "--gpus=a100:2" in fake_slurm.sbatch_calls[0].argv
    assert "--gres=fpga:vu9p:1" in fake_slurm.sbatch_calls[0].argv


@pytest.mark.asyncio
async def test_sixty_four_generic_tres_round_trip_with_scheduler_overhead(
    fake_slurm: FakeSlurm,
) -> None:
    generic_tres = tuple(
        SlurmTresValueV2(name=f"gres/fpga{index:02d}", value=1) for index in range(64)
    )
    ceiling = SlurmResourceV2(
        cpus=64,
        memory_bytes=512 * 1024 * 1024 * 1024,
        gpus=8,
        generic_tres=generic_tres,
    )
    request = slurm_launch_request_fixture(fake_slurm).model_copy(
        update={"generic_tres": generic_tres}
    )
    backend = fake_slurm.backend(resource_ceiling=ceiling)

    submitted = await backend.submit(request)
    observed = (await backend.inventory())[0]
    assert observed.generic_tres == generic_tres

    fake_slurm.terminalize_job(submitted.job_id)
    terminal = (
        await backend.accounting_high_water(since=datetime(2026, 8, 13, 0, 0, tzinfo=UTC))
    ).terminal_jobs[0]
    assert terminal.generic_tres == generic_tres


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "gres",
    (
        "gpu:2,foreign:1",
        "gpu:2,fpga00:1,fpga00:1",
        "gpu:2," + ",".join(f"fpga{index:02d}:1" for index in range(64)) + ",fpga00:1",
    ),
)
async def test_inventory_tres_boundary_rejects_unknown_duplicate_and_over_limit_records(
    fake_slurm: FakeSlurm,
    gres: str,
) -> None:
    generic_tres = tuple(
        SlurmTresValueV2(name=f"gres/fpga{index:02d}", value=1) for index in range(64)
    )
    backend = fake_slurm.backend(
        resource_ceiling=SlurmResourceV2(
            cpus=64,
            memory_bytes=512 * 1024 * 1024 * 1024,
            gpus=8,
            generic_tres=generic_tres,
        )
    )
    fake_slurm.set_output(
        "squeue",
        "101|PENDING|loom-oldlab|loom-executor|loom|16|65536M|"
        f"{gres}|oldlab-5|Resources|{'A' * 43}\n",
    )

    with pytest.raises(SlurmOutputError, match="resource"):
        await backend.inventory()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "allocated_tres",
    (
        "billing=16,cpu=16,gres/gpu=2,mem=65536M,node=1,foreign=1",
        "billing=16,cpu=16,gres/gpu=2,mem=65536M,node=1,cpu=16",
        "billing=16,cpu=16,gres/gpu=2,mem=65536M,node=1,"
        + ",".join(f"gres/fpga{index:02d}=1" for index in range(64))
        + ",gres/fpga00=1",
    ),
)
async def test_terminal_tres_boundary_rejects_unknown_duplicate_and_over_limit_records(
    fake_slurm: FakeSlurm,
    allocated_tres: str,
) -> None:
    generic_tres = tuple(
        SlurmTresValueV2(name=f"gres/fpga{index:02d}", value=1) for index in range(64)
    )
    backend = fake_slurm.backend(
        resource_ceiling=SlurmResourceV2(
            cpus=64,
            memory_bytes=512 * 1024 * 1024 * 1024,
            gpus=8,
            generic_tres=generic_tres,
        )
    )
    fake_slurm.set_output(
        "sacct",
        "99|COMPLETED|loom-oldlab|loom-executor|oldlab|"
        "2026-08-13T12:00:00Z|2026-08-13T12:01:00Z|2026-08-13T12:03:00Z|"
        f"120|0:0|16|65536M|{allocated_tres}|oldlab-5|{'A' * 43}\n",
    )

    with pytest.raises(SlurmOutputError, match="terminal"):
        await backend.accounting_high_water(since=datetime(2026, 8, 13, 0, 0, tzinfo=UTC))


@pytest.mark.asyncio
async def test_accounting_rejects_reqmem_allocated_tres_memory_disagreement(
    fake_slurm: FakeSlurm,
) -> None:
    fake_slurm.set_output(
        "sacct",
        "99|COMPLETED|loom-oldlab|loom-executor|oldlab|"
        "2026-08-13T12:00:00Z|2026-08-13T12:01:00Z|2026-08-13T12:03:00Z|"
        "120|0:0|16|65536M|cpu=16,gres/gpu=2,mem=32768M,node=1|oldlab-5|" + "A" * 43 + "\n",
    )
    with pytest.raises(SlurmOutputError, match="terminal"):
        await fake_slurm.backend().accounting_high_water(
            since=datetime(2026, 8, 13, 0, 0, tzinfo=UTC)
        )


@pytest.mark.asyncio
async def test_fake_process_evidence_is_regular_nonsymlink_mode_0600(
    fake_slurm: FakeSlurm,
) -> None:
    await fake_slurm.backend().validate_authority()
    for path in fake_slurm.evidence_paths():
        assert_secure_evidence(path)
