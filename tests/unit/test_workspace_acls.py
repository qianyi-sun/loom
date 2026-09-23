import tarfile
from pathlib import Path, PurePosixPath

import pytest
from pydantic import ValidationError

from loom.models.exec import ExecResult
from loom.models.task import EnvironmentConfig, TaskConfig
from loom.trial.workspace import WorkspaceStagingPolicy
from loom.trial.workspace_snapshot import (
    WorkspaceSnapshotError,
    _export_workspace_archive,
    _import_workspace_archive,
    _validate_workspace_archive,
)


def test_acl_requirement_is_explicit_and_default_serialization_is_unchanged():
    ordinary = EnvironmentConfig(os="linux")
    assert "preserve_acls" not in ordinary.model_dump()
    declared = EnvironmentConfig(os="linux", preserve_acls=True)
    assert declared.preserve_acls
    assert declared.model_dump()["preserve_acls"] is True
    with pytest.raises(ValidationError):
        EnvironmentConfig(os="linux", preserve_acls="optional")


def test_harbor_normalization_retains_acl_requirement():
    from loom.terminal_bench_normalize import normalize_terminal_bench_task_toml
    from tests.unit.test_terminal_bench_normalize import _tb_raw

    task = TaskConfig.model_validate(normalize_terminal_bench_task_toml(_tb_raw(preserve_acls=True)))
    assert task.environment.preserve_acls


async def test_acl_preparation_failure_prevents_model_attempt(tmp_path, monkeypatch):
    from uuid import uuid4

    from loom import service_execution_sandbox_task as module
    from tests.unit.test_service_execution_sandbox_task import Sandbox
    from tests.unit.test_service_execution_terminus_plan import _inputs

    task, trial, _ = _inputs()
    raw = task.model_dump(mode="json")
    raw["environment"]["preserve_acls"] = True
    task = TaskConfig.model_validate(raw)
    driver = Sandbox()
    monkeypatch.setattr(module, "sandbox_driver", lambda *_: driver)
    monkeypatch.setenv("LOOM_GATEWAY_URL", "http://127.0.0.1:9999")
    monkeypatch.setenv("LOOM_TASK_ARTIFACTS_JSON", "[]")

    async def identity(_):
        return uuid4(), uuid4()

    async def unsupported(*_):
        raise WorkspaceSnapshotError("ACL filesystem unsupported")

    async def unexpected_model(**_):
        raise AssertionError("ACL preparation must finish before model execution")

    monkeypatch.setattr(module, "_execution_identity", identity)
    monkeypatch.setattr(module, "run_terminus2", unexpected_model)
    monkeypatch.setattr("loom.trial.workspace_acls.require_acl_support", unsupported)
    with pytest.raises(WorkspaceSnapshotError, match="ACL"):
        await module.run_agent(tmp_path, task, trial)
    assert driver.state == "stopped"
    assert not (tmp_path / ".loom/workspace.tar").exists()


async def test_acl_archive_without_declaration_fails_before_destination_changes(tmp_path):
    archive = tmp_path / "acl.tar"
    with tarfile.open(archive, "w", format=tarfile.PAX_FORMAT) as stream:
        member = tarfile.TarInfo(".")
        member.type = tarfile.DIRTYPE
        member.pax_headers = {"SCHILY.acl.default": "user::rwx\ngroup::r-x\nother::r-x"}
        stream.addfile(member)

    class UntouchedDriver:
        async def exec(self, *_args, **_kwargs):
            raise AssertionError("undeclared ACL must fail before destination changes")

    with pytest.raises(WorkspaceSnapshotError, match="preserve_acls"):
        await _import_workspace_archive(UntouchedDriver(), archive, PurePosixPath("/workspace"))


@pytest.mark.parametrize("key,value,kind", [
    ("SCHILY.acl.unknown", "user::rwx\ngroup::r-x\nother::r-x", tarfile.DIRTYPE),
    ("SCHILY.acl.default", "user::rwx\ngroup::r-x\nother::r-x", tarfile.REGTYPE),
    ("SCHILY.acl.access", "user:alice:rwx\ngroup::r-x\nother::r-x", tarfile.DIRTYPE),
    ("SCHILY.acl.access", "user::rwx\ngroup::r-x\nother::r-x\nother::---", tarfile.DIRTYPE),
    ("SCHILY.acl.access", "user::rwx\nuser:123:rwx\ngroup::r-x\nother::r-x", tarfile.DIRTYPE),
    ("SCHILY.acl.access", "user::rwx\nuser:1:rwx\nuser:01:rwx\ngroup::r-x\nmask::rwx\nother::r-x", tarfile.DIRTYPE),
    ("SCHILY.acl.access", "user::rwx\vgroup::r-x\vother::r-x", tarfile.DIRTYPE),
])
def test_malformed_or_unsupported_acl_metadata_is_rejected(tmp_path, key, value, kind):
    archive = tmp_path / "acl.tar"
    with tarfile.open(archive, "w", format=tarfile.PAX_FORMAT) as stream:
        member = tarfile.TarInfo("entry")
        member.type = kind
        member.pax_headers = {key: value}
        stream.addfile(member)
    with pytest.raises(WorkspaceSnapshotError, match="ACL"):
        _validate_workspace_archive(archive, WorkspaceStagingPolicy((), (), ()))


async def test_missing_acl_tools_fail_before_export(tmp_path: Path):
    class UnsupportedDriver:
        async def exec(self, cmd: str, **_kwargs: object) -> ExecResult:
            return ExecResult(return_code=127, stdout=b"", stderr=b"not found", duration_sec=0)

        async def download(self, *_args):
            raise AssertionError("unsupported metadata must fail before download")

    with pytest.raises(WorkspaceSnapshotError, match="ACL"):
        await _export_workspace_archive(
            UnsupportedDriver(), PurePosixPath("/workspace"), tmp_path / "archive.tar",
            preserve_acls=True,
        )
