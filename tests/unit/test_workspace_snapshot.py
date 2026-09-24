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


def test_declared_external_leaf_preserves_interpreter_and_internal_aliases(tmp_path, policy):
    archive = tmp_path / "venv.tar"
    _write_archive(archive, [
        _member("bin/python", tarfile.SYMTYPE, link="/usr/local/bin/python3.9"),
        _member("bin/python3", tarfile.SYMTYPE, link="python"),
        _member("bin/python3.9", tarfile.SYMTYPE, link="python"),
    ])
    _validate_workspace_archive(archive, policy, root=PurePosixPath("/cache"),
        external_reference_files=frozenset({PurePosixPath("/usr/local/bin/python3.9")}))
    with tarfile.open(archive) as stream:
        assert [m.linkname for m in stream] == ["/usr/local/bin/python3.9", "python", "python"]


@pytest.mark.parametrize("member", [
    _member("alias", tarfile.SYMTYPE, link="bin/python/../private"),
    _member("alias", tarfile.SYMTYPE, link="bin/python/"),
    _member("alias", tarfile.SYMTYPE, link="bin/python/."),
    _member("alias", tarfile.SYMTYPE, link="/usr/local/bin/python3.9/../private"),
    _member("alias", tarfile.SYMTYPE, link="/usr/local/bin/other"),
    _member("bin/python/child", tarfile.REGTYPE),
    _member("hard", tarfile.LNKTYPE, link="/usr/local/bin/python3.9"),
])
def test_external_reference_is_a_leaf_not_an_escape_prefix(tmp_path, policy, member):
    archive = tmp_path / "invalid.tar"
    _write_archive(archive, [_member("bin/python", tarfile.SYMTYPE,
                                  link="/usr/local/bin/python3.9"), member])
    with pytest.raises(WorkspaceSnapshotError):
        _validate_workspace_archive(archive, policy, root=PurePosixPath("/cache"),
            external_reference_files=frozenset({PurePosixPath("/usr/local/bin/python3.9")}))


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


@pytest.mark.parametrize("target", ["/app/data/file", "/app/data/missing", "/app"])
def test_snapshot_accepts_absolute_link_within_declared_root(
    tmp_path: Path, policy: WorkspaceStagingPolicy, target: str,
) -> None:
    archive = tmp_path / "absolute.tar"
    _write_archive(archive, [
        _member("data", tarfile.DIRTYPE),
        _member("data/file", tarfile.REGTYPE),
        _member("alias", tarfile.SYMTYPE, link=target),
    ])

    _validate_workspace_archive(archive, policy, root=PurePosixPath("/app"))
    with tarfile.open(archive) as stream:
        assert stream.getmember("alias").linkname == target


@pytest.mark.parametrize("target,message", [
    ("/app-other/file", "escapes workdir"),
    ("/etc/passwd", "escapes workdir"),
    ("/app/../outside", "escapes workdir"),
    ("/app/tests/secret", "private path"),
    ("/app/tests/../public", "private path"),
    ("/app/solution", "private path"),
    ("//app/file", "unsafe target"),
])
def test_snapshot_rejects_unsafe_absolute_link(
    tmp_path: Path, policy: WorkspaceStagingPolicy, target: str, message: str,
) -> None:
    archive = tmp_path / "absolute.tar"
    _write_archive(archive, [_member("alias", tarfile.SYMTYPE, link=target)])
    with pytest.raises(WorkspaceSnapshotError, match=message):
        _validate_workspace_archive(archive, policy, root=PurePosixPath("/app"))


@pytest.mark.parametrize("prefix", ["", "/app/"])
@pytest.mark.parametrize("target,message", [
    ("dir/up/../outside", "escapes workdir"),
    ("dir/up/tests/secret", "private path"),
    ("dir/up/tests/../public", "private path"),
    ("alias", "cycle"),
])
def test_snapshot_resolves_links_before_parent_components(
    tmp_path: Path, policy: WorkspaceStagingPolicy, prefix: str, target: str, message: str,
) -> None:
    archive = tmp_path / "chain.tar"
    _write_archive(archive, [
        _member("dir", tarfile.DIRTYPE),
        _member("dir/up", tarfile.SYMTYPE, link=".."),
        _member("alias", tarfile.SYMTYPE, link=prefix + target),
    ])
    with pytest.raises(WorkspaceSnapshotError, match=message):
        _validate_workspace_archive(archive, policy, root=PurePosixPath("/app"))


def test_snapshot_accepts_repeated_noncyclic_directory_link(
    tmp_path: Path, policy: WorkspaceStagingPolicy,
) -> None:
    archive = tmp_path / "chain.tar"
    _write_archive(archive, [
        _member("dir", tarfile.DIRTYPE),
        _member("link", tarfile.SYMTYPE, link="/app/dir"),
        _member("alias", tarfile.SYMTYPE, link="link/../link/file"),
    ])
    _validate_workspace_archive(archive, policy, root=PurePosixPath("/app"))


def test_relative_link_cannot_hide_escape_behind_directory_link(
    tmp_path: Path, policy: WorkspaceStagingPolicy,
) -> None:
    archive = tmp_path / "relative-chain.tar"
    _write_archive(archive, [
        _member("dir", tarfile.DIRTYPE),
        _member("dir/up", tarfile.SYMTYPE, link=".."),
        _member("alias", tarfile.SYMTYPE, link="dir/up/../outside"),
    ])
    with pytest.raises(WorkspaceSnapshotError, match="escapes workdir"):
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


def test_relative_declared_library_reference_retains_literal_target(tmp_path, policy):
    archive = tmp_path / 'libraries.tar'
    _write_archive(archive, [_member('libcrypto.so.3', tarfile.SYMTYPE, link='../../lib/libcrypto.so.3'),
                             _member('libcrypto.so', tarfile.SYMTYPE, link='libcrypto.so.3')])
    _validate_workspace_archive(archive, policy, root=PurePosixPath('/usr/lib'),
        external_reference_files=frozenset({PurePosixPath('/lib/libcrypto.so.3')}),
        allow_relative_references=True)
    with tarfile.open(archive) as stream:
        assert [m.linkname for m in stream] == ['../../lib/libcrypto.so.3', 'libcrypto.so.3']
    # Workspace callers have not qualified root ancestry for relative escapes.
    with pytest.raises(WorkspaceSnapshotError):
        _validate_workspace_archive(archive, policy, root=PurePosixPath('/usr/lib'),
            external_reference_files=frozenset({PurePosixPath('/lib/libcrypto.so.3')}))


@pytest.mark.parametrize('target', ['../../lib/unknown', '../../lib/libcrypto.so.3/',
    '../../lib/libcrypto.so.3/.', '../../lib/libcrypto.so.3/../private',
    '../../../lib/libcrypto.so.3', 'unknown/../../../lib/libcrypto.so.3',
    '../../tests/../lib/libcrypto.so.3'])
def test_relative_references_cannot_normalize_arbitrary_escape_paths(tmp_path, policy, target):
    archive = tmp_path / 'bad-reference.tar'
    _write_archive(archive, [_member('library', tarfile.SYMTYPE, link=target)])
    with pytest.raises(WorkspaceSnapshotError):
        _validate_workspace_archive(archive, policy, root=PurePosixPath('/usr/lib'),
            external_reference_files=frozenset({PurePosixPath('/lib/libcrypto.so.3')}),
            allow_relative_references=True)


@pytest.mark.parametrize('target', ['library/..', 'library/child', 'library/', 'library/.'])
def test_internal_alias_cannot_traverse_after_relative_reference(tmp_path, policy, target):
    archive = tmp_path / 'bad-alias.tar'
    _write_archive(archive, [_member('library', tarfile.SYMTYPE, link='../../lib/libcrypto.so.3'),
                             _member('alias', tarfile.SYMTYPE, link=target)])
    with pytest.raises(WorkspaceSnapshotError):
        _validate_workspace_archive(archive, policy, root=PurePosixPath('/usr/lib'),
            external_reference_files=frozenset({PurePosixPath('/lib/libcrypto.so.3')}),
            allow_relative_references=True)
