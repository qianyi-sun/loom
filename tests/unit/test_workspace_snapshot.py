from __future__ import annotations

import io
import tarfile
from pathlib import Path, PurePosixPath

import pytest

from loom.models.exec import ExecResult
from loom.trial.workspace import (
    TB21_AGENT_WORKSPACE_POLICY,
    WorkspaceStagingPolicy,
)
from loom.trial.workspace_snapshot import (
    WorkspaceSnapshotError,
    _export_workspace_archive,
    _import_workspace_archive,
    _validate_workspace_archive,
)


@pytest.fixture
def policy() -> WorkspaceStagingPolicy:
    return WorkspaceStagingPolicy.from_provenance(TB21_AGENT_WORKSPACE_POLICY)


def _write_archive(path: Path, members: list[tarfile.TarInfo]) -> None:
    with tarfile.open(path, mode="w") as tf:
        for member in members:
            data = None
            if member.isreg():
                payload = b"#!/bin/sh\necho ok\n"
                member.size = len(payload)
                data = io.BytesIO(payload)
            tf.addfile(member, data)


def _member(name: str, kind: bytes, *, mode: int = 0o644, link: str = "") -> tarfile.TarInfo:
    info = tarfile.TarInfo(name)
    info.type = kind
    info.mode = mode
    info.linkname = link
    return info


def test_snapshot_accepts_modes_directories_and_safe_links(
    tmp_path: Path,
    policy: WorkspaceStagingPolicy,
) -> None:
    archive = tmp_path / "valid.tar"
    _write_archive(
        archive,
        [
            _member(".", tarfile.DIRTYPE, mode=0o755),
            _member("./bin", tarfile.DIRTYPE, mode=0o750),
            _member("./bin/tool", tarfile.REGTYPE, mode=0o751),
            _member(
                "./bin/tool-hard",
                tarfile.LNKTYPE,
                mode=0o751,
                link="./bin/tool",
            ),
            _member("./links", tarfile.DIRTYPE, mode=0o755),
            _member(
                "./links/tool",
                tarfile.SYMTYPE,
                mode=0o777,
                link="../bin/tool",
            ),
            _member("./empty", tarfile.DIRTYPE, mode=0o710),
        ],
    )

    _validate_workspace_archive(archive, policy)


@pytest.mark.parametrize(
    ("member", "message"),
    [
        (_member("../escape", tarfile.REGTYPE), "traverses"),
        (_member("/absolute", tarfile.REGTYPE), "traverses"),
        (_member("solution", tarfile.DIRTYPE), "private path"),
        (_member("tests/secret", tarfile.REGTYPE), "private path"),
        (
            _member("public-link", tarfile.SYMTYPE, link="solution/solve.sh"),
            "targets private path",
        ),
        (
            _member("public-link", tarfile.SYMTYPE, link="../outside"),
            "escapes workdir",
        ),
        (
            _member("public-link", tarfile.SYMTYPE, link="/workspace/file"),
            "unsafe target",
        ),
        (
            _member("public-hard", tarfile.LNKTYPE, link="verifier/run.sh"),
            "targets private path",
        ),
        (_member("device", tarfile.CHRTYPE), "unsupported"),
        (_member("fifo", tarfile.FIFOTYPE), "unsupported"),
    ],
)
def test_snapshot_rejects_unsafe_entries(
    tmp_path: Path,
    policy: WorkspaceStagingPolicy,
    member: tarfile.TarInfo,
    message: str,
) -> None:
    archive = tmp_path / "unsafe.tar"
    _write_archive(archive, [_member(".", tarfile.DIRTYPE), member])

    with pytest.raises(WorkspaceSnapshotError, match=message):
        _validate_workspace_archive(archive, policy)


def test_snapshot_rejects_entry_below_symlink(
    tmp_path: Path,
    policy: WorkspaceStagingPolicy,
) -> None:
    archive = tmp_path / "symlink-parent.tar"
    _write_archive(
        archive,
        [
            _member(".", tarfile.DIRTYPE),
            _member("alias", tarfile.SYMTYPE, link="real"),
            _member("alias/payload", tarfile.REGTYPE),
        ],
    )

    with pytest.raises(WorkspaceSnapshotError, match="nested below symlink"):
        _validate_workspace_archive(archive, policy)


async def test_snapshot_export_rejects_socket_before_tar(tmp_path: Path) -> None:
    class _SocketDriver:
        async def exec(self, cmd: str, **_kwargs: object) -> ExecResult:
            assert cmd.startswith("find ")
            return ExecResult(
                return_code=0,
                stdout=b"/workspace/agent.sock\n",
                stderr=b"",
                truncated=False,
                duration_sec=0,
            )

        async def download(self, _src: PurePosixPath, _dst: Path) -> None:
            raise AssertionError("socket workspace must fail before download")

    with pytest.raises(WorkspaceSnapshotError, match="socket"):
        await _export_workspace_archive(
            _SocketDriver(),  # type: ignore[arg-type]
            PurePosixPath("/workspace"),
            tmp_path / "snapshot.tar",
        )


@pytest.mark.parametrize("inventory", [
    ExecResult(return_code=0, stdout=b".\0./private\0", stderr=b"", truncated=True, duration_sec=0),
    ExecResult(return_code=1, stdout=b".\0", stderr=b"find failed", duration_sec=0),
    ExecResult(return_code=0, stdout=b".\0./partial", stderr=b"", duration_sec=0),
    ExecResult(return_code=0, stdout=b".\0../outside\0", stderr=b"", duration_sec=0),
])
async def test_incomplete_destination_inventory_fails_before_deletion_or_upload(
    tmp_path: Path, policy: WorkspaceStagingPolicy, inventory: ExecResult,
) -> None:
    archive = tmp_path / "valid.tar"
    _write_archive(archive, [_member(".", tarfile.DIRTYPE)])

    class InventoryDriver:
        async def exec(self, cmd: str, **_kwargs: object) -> ExecResult:
            if "find . -print0" in cmd:
                return inventory
            # Only destination validation is allowed before the failed inventory.
            assert cmd.startswith("test ! -L "), "invalid inventory must prevent deletion"
            return ExecResult(return_code=0, stdout=b"", stderr=b"", duration_sec=0)

        async def upload(self, _src: Path, _dst: PurePosixPath) -> None:
            raise AssertionError("invalid inventory must prevent archive upload")

    with pytest.raises(WorkspaceSnapshotError):
        await _import_workspace_archive(
            InventoryDriver(), archive, PurePosixPath("/workspace"), policy=policy,  # type: ignore[arg-type]
        )


@pytest.mark.parametrize("destination", ["/", "//", "//workspace", "relative", "/workspace/../other"])
async def test_unsafe_destination_fails_before_any_remote_command(
    tmp_path: Path, policy: WorkspaceStagingPolicy, destination: str,
) -> None:
    archive = tmp_path / "valid.tar"
    _write_archive(archive, [_member(".", tarfile.DIRTYPE)])

    class UntouchedDriver:
        async def exec(self, *_args: object, **_kwargs: object) -> ExecResult:
            raise AssertionError("invalid destination must not execute commands")

    with pytest.raises(WorkspaceSnapshotError, match="destination"):
        await _import_workspace_archive(
            UntouchedDriver(), archive, PurePosixPath(destination), policy=policy,  # type: ignore[arg-type]
        )
