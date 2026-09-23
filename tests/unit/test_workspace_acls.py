from pathlib import Path, PurePosixPath

import pytest
from pydantic import ValidationError

from loom.models.exec import ExecResult
from loom.models.task import EnvironmentConfig
from loom.trial.workspace_snapshot import WorkspaceSnapshotError, _export_workspace_archive


def test_acl_requirement_is_explicit_and_default_serialization_is_unchanged():
    ordinary = EnvironmentConfig(os="linux")
    assert "preserve_acls" not in ordinary.model_dump()
    declared = EnvironmentConfig(os="linux", preserve_acls=True)
    assert declared.preserve_acls
    assert declared.model_dump()["preserve_acls"] is True
    with pytest.raises(ValidationError):
        EnvironmentConfig(os="linux", preserve_acls="optional")


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
