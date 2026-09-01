from __future__ import annotations

import json
import os
import signal
import stat
import struct
import subprocess
import sys
from base64 import b64encode
from dataclasses import replace
from pathlib import Path

import pytest
import scripts.ops.install_gb10_autoscaler_controller as controller_installer

ROOT = Path(__file__).resolve().parents[2]
INSTALLER = ROOT / "scripts/ops/install_gb10_autoscaler_controller.py"


class InstallerBackend:
    def __init__(
        self,
        *,
        fail_uv_path: Path | None = None,
        authority_path: Path | None = None,
        system_root: Path | None = None,
        require_blocked_signals: bool = False,
    ):
        self.fail_uv_path = fail_uv_path
        self.authority_path = authority_path
        self.system_root = system_root
        self.require_blocked_signals = require_blocked_signals
        self.host_validated = False

    def validate_host(self) -> None:
        self.host_validated = True

    def validate_kubectl(self, path: Path) -> None:
        assert path.read_bytes() == b"kubectl fixture\n"

    def validate_uv(self, path: Path) -> None:
        if path == self.fail_uv_path:
            raise controller_installer.ControllerInstallError("injected uv readback failure")
        assert path.read_bytes() == b"uv fixture\n"

    def validate_acceptance(self, path: Path) -> None:
        assert path.read_bytes() == b"#!/usr/bin/python3\n"

    def validate_sudoers(self, path: Path) -> None:
        assert path.read_text(encoding="utf-8") == (
            "qianyi ALL=(root) NOPASSWD:NOSETENV: "
            '/usr/local/libexec/loom-gb10-external-supervisor-broker ""\n'
        )

    def publish_authority(
        self,
        broker_path: Path,
        controller_public_key: Path,
        legacy_public_key: Path,
    ) -> None:
        assert self.host_validated
        if self.require_blocked_signals:
            blocked = signal.pthread_sigmask(signal.SIG_BLOCK, set())
            assert signal.SIGINT in blocked
            assert signal.SIGTERM in blocked
        assert broker_path.read_bytes() == b"#!/usr/bin/python3\n"
        assert controller_public_key.read_text(encoding="ascii").endswith(" controller\n")
        assert legacy_public_key.read_text(encoding="ascii").endswith(" legacy\n")
        if self.system_root is not None:
            assert (self.system_root / "usr/local/bin/kubectl").read_bytes() == (
                b"kubectl fixture\n"
            )
            assert (self.system_root / "usr/local/bin/uv").read_bytes() == b"uv fixture\n"
            assert (
                self.system_root / "usr/local/libexec/loom-gb10-slurm-acceptance-authority"
            ).read_bytes() == b"#!/usr/bin/python3\n"
            assert (
                self.system_root / "etc/tmpfiles.d/loom-gb10-slurm-authority.conf"
            ).read_bytes() == (
                b"d /run/loom-gb10-slurm-authority 0700 root root -\n"
                b"d /run/loom-gb10-slurm-authority/jobs 0700 root root -\n"
                b"f /run/loom-gb10-slurm-authority/acceptance.lock 0600 root root -\n"
            )
            assert (
                self.system_root / "etc/sudoers.d/loom-gb10-external-supervisor"
            ).read_bytes() == (
                b"qianyi ALL=(root) NOPASSWD:NOSETENV: "
                b'/usr/local/libexec/loom-gb10-external-supervisor-broker ""\n'
            )
            assert (
                self.system_root / "run/loom-gb10-slurm-authority/acceptance.lock"
            ).read_bytes() == b""
        assert self.authority_path is not None
        self.authority_path.write_text("published\n", encoding="ascii")


def _public_key(seed: int, comment: str) -> bytes:
    algorithm = b"ssh-ed25519"
    raw_key = bytes([seed]) * 32
    blob = struct.pack(">I", len(algorithm)) + algorithm + struct.pack(">I", len(raw_key)) + raw_key
    return algorithm + b" " + b64encode(blob) + b" " + comment.encode("ascii") + b"\n"


def _make_directory(path: Path, *, mode: int) -> None:
    path.mkdir(parents=True, exist_ok=True)
    path.chmod(mode)


def _system_tree_snapshot(root: Path) -> dict[str, tuple[int, int, int, bytes | None]]:
    snapshot: dict[str, tuple[int, int, int, bytes | None]] = {}
    for path in sorted((root, *root.rglob("*"))):
        metadata = path.lstat()
        relative = "." if path == root else str(path.relative_to(root))
        payload = path.read_bytes() if stat.S_ISREG(metadata.st_mode) else None
        snapshot[relative] = (
            stat.S_IMODE(metadata.st_mode),
            metadata.st_uid,
            metadata.st_gid,
            payload,
        )
    return snapshot


def _install_context(tmp_path: Path) -> tuple[controller_installer.InstallContext, Path]:
    source, source_sha = _sealed_source(tmp_path)
    system_root = tmp_path / "system"
    for relative, mode in (
        ("", 0o755),
        ("usr", 0o755),
        ("usr/local", 0o755),
        ("usr/local/bin", 0o755),
        ("usr/local/libexec", 0o755),
        ("etc", 0o755),
        ("etc/sudoers.d", 0o755),
        ("etc/tmpfiles.d", 0o755),
        ("opt", 0o755),
        ("var", 0o755),
        ("var/lib", 0o755),
        ("run", 0o755),
    ):
        _make_directory(system_root / relative, mode=mode)
    staged = tmp_path / "staged"
    staged.mkdir(mode=0o700)
    kubectl = staged / "kubectl"
    uv = staged / "uv"
    controller_key = staged / "controller.pub"
    legacy_key = staged / "legacy.pub"
    kubectl.write_bytes(b"kubectl fixture\n")
    uv.write_bytes(b"uv fixture\n")
    controller_key.write_bytes(_public_key(7, "controller"))
    legacy_key.write_bytes(_public_key(8, "legacy"))
    for path in (kubectl, uv, controller_key, legacy_key):
        path.chmod(0o600)
    authority_uid = os.getuid()
    authority_gid = os.getgid()
    return (
        controller_installer.InstallContext(
            trusted_source_root=source.parent,
            source_root=source,
            source_sha=source_sha,
            kubectl_source=kubectl,
            uv_source=uv,
            controller_public_key=controller_key,
            legacy_public_key=legacy_key,
            system_root=system_root,
            authority_uid=authority_uid,
            authority_gid=authority_gid,
            service_uid=authority_uid,
            service_gid=authority_gid,
        ),
        system_root,
    )


def _git(repository: Path, *arguments: str) -> str:
    completed = subprocess.run(
        ["git", "-C", str(repository), *arguments],
        capture_output=True,
        check=True,
        text=True,
    )
    return completed.stdout.strip()


def _sealed_source(tmp_path: Path) -> tuple[Path, str]:
    trusted_root = tmp_path / "trusted"
    trusted_root.mkdir(mode=0o700)
    source = trusted_root / ("0" * 40)
    source.mkdir(mode=0o700)
    _git(source, "init", "--quiet")
    _git(source, "config", "user.email", "installer-test@example.com")
    _git(source, "config", "user.name", "Installer Test")
    _git(source, "remote", "add", "origin", "https://github.com/qianyi-sun/loom.git")
    assets = {
        "deploy/slurm/install-loom-gb10-autoscaler-controller.sh": "#!/usr/bin/env bash\n",
        "deploy/slurm/loom-gb10-slurm-authority.tmpfiles": (
            "d /run/loom-gb10-slurm-authority 0700 root root -\n"
            "d /run/loom-gb10-slurm-authority/jobs 0700 root root -\n"
            "f /run/loom-gb10-slurm-authority/acceptance.lock 0600 root root -\n"
        ),
        "scripts/ops/gb10_controller_bootstrap.py": "#!/usr/bin/env python3\n",
        "scripts/ops/gb10_external_supervisor_broker.py": "#!/usr/bin/python3\n",
        "scripts/ops/gb10_slurm_acceptance_authority.py": "#!/usr/bin/python3\n",
        "scripts/ops/install_gb10_autoscaler_controller.py": "#!/usr/bin/env python3\n",
    }
    for relative, payload in assets.items():
        path = source / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(payload, encoding="utf-8")
        path.chmod(0o755 if path.suffix == ".py" else 0o644)
    for directory in (
        source / "deploy",
        source / "deploy/slurm",
        source / "scripts",
        source / "scripts/ops",
    ):
        directory.chmod(0o755)
    _git(source, "add", ".")
    _git(source, "commit", "--quiet", "-m", "sealed installer fixture")
    source_sha = _git(source, "rev-parse", "HEAD")
    final = trusted_root / source_sha
    source.rename(final)
    _git(final, "checkout", "--quiet", "--detach", source_sha)
    (final / ".git").chmod(0o700)
    return final, source_sha


def _verify_source(source: Path, source_sha: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            str(INSTALLER),
            "verify-source",
            "--trusted-root",
            str(source.parent),
            "--source-root",
            str(source),
            "--source-sha",
            source_sha,
        ],
        capture_output=True,
        check=False,
        text=True,
        env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
    )


def test_verify_source_accepts_only_the_exact_sealed_checkout(tmp_path: Path) -> None:
    source, source_sha = _sealed_source(tmp_path)

    completed = _verify_source(source, source_sha)

    assert completed.returncode == 0, completed.stderr
    evidence = json.loads(completed.stdout)
    assert evidence == {
        "artifacts": {
            "deploy/slurm/install-loom-gb10-autoscaler-controller.sh": (
                "1d95fc04a80c952f49ce4188627c53b0fbe8c44041b952d592acd1de99861466"
            ),
            "deploy/slurm/loom-gb10-slurm-authority.tmpfiles": (
                "feadb0639f8ead40af36a121eff3f8d0228c5ef43e64cedf7562888a078306eb"
            ),
            "scripts/ops/gb10_external_supervisor_broker.py": (
                "aee50b3d023b039c7116109895cdbe61a93b7869da6ea0984da85862fc5b3ed7"
            ),
            "scripts/ops/gb10_controller_bootstrap.py": (
                "d682cbdb4c8b07518bf486c58990fc391783aa20a916bfb27ad211eb1d5c3642"
            ),
            "scripts/ops/gb10_slurm_acceptance_authority.py": (
                "aee50b3d023b039c7116109895cdbe61a93b7869da6ea0984da85862fc5b3ed7"
            ),
            "scripts/ops/install_gb10_autoscaler_controller.py": (
                "d682cbdb4c8b07518bf486c58990fc391783aa20a916bfb27ad211eb1d5c3642"
            ),
        },
        "source_sha": source_sha,
    }
    assert completed.stderr == ""


def test_verify_source_evidence_binds_every_installer_entrypoint(tmp_path: Path) -> None:
    source, source_sha = _sealed_source(tmp_path)

    completed = _verify_source(source, source_sha)

    assert completed.returncode == 0, completed.stderr
    evidence = json.loads(completed.stdout)
    assert evidence["artifacts"]["deploy/slurm/install-loom-gb10-autoscaler-controller.sh"] == (
        "1d95fc04a80c952f49ce4188627c53b0fbe8c44041b952d592acd1de99861466"
    )
    assert evidence["artifacts"]["scripts/ops/gb10_controller_bootstrap.py"] == (
        "d682cbdb4c8b07518bf486c58990fc391783aa20a916bfb27ad211eb1d5c3642"
    )
    assert evidence["artifacts"]["scripts/ops/install_gb10_autoscaler_controller.py"] == (
        "d682cbdb4c8b07518bf486c58990fc391783aa20a916bfb27ad211eb1d5c3642"
    )


def test_verify_source_rejects_a_writable_source_directory(tmp_path: Path) -> None:
    source, source_sha = _sealed_source(tmp_path)
    source.chmod(0o770)

    completed = _verify_source(source, source_sha)

    assert completed.returncode == 1
    assert completed.stdout == ""
    assert completed.stderr == "error: controller installer source root is unsafe\n"


def test_verify_source_rejects_a_dirty_checkout(tmp_path: Path) -> None:
    source, source_sha = _sealed_source(tmp_path)
    (source / "unexpected").write_text("unreviewed\n", encoding="utf-8")

    completed = _verify_source(source, source_sha)

    assert completed.returncode == 1
    assert completed.stdout == ""
    assert completed.stderr == "error: controller installer Git source identity drifted\n"


def test_verify_source_rejects_git_object_indirection_outside_the_sealed_checkout(
    tmp_path: Path,
) -> None:
    source, source_sha = _sealed_source(tmp_path)
    external_objects = tmp_path / "external-objects"
    (external_objects / "info").mkdir(parents=True)
    (source / ".git/objects/info/alternates").write_text(
        f"{external_objects}\n",
        encoding="utf-8",
    )

    completed = _verify_source(source, source_sha)

    assert completed.returncode == 1
    assert completed.stdout == ""
    assert completed.stderr == "error: controller installer Git indirection is unsupported\n"


def test_verify_source_rejects_assume_unchanged_artifact_drift(tmp_path: Path) -> None:
    source, source_sha = _sealed_source(tmp_path)
    broker = source / "scripts/ops/gb10_external_supervisor_broker.py"
    broker.write_text("#!/usr/bin/python3\nprint('unreviewed')\n", encoding="utf-8")
    _git(source, "update-index", "--assume-unchanged", str(broker.relative_to(source)))
    assert _git(source, "status", "--porcelain=v1", "--untracked-files=all") == ""

    completed = _verify_source(source, source_sha)

    assert completed.returncode == 1
    assert completed.stdout == ""
    assert completed.stderr == "error: controller installer artifact does not match exact commit\n"


def test_verify_source_rejects_git_directory_redirected_outside_sealed_root(
    tmp_path: Path,
) -> None:
    source, source_sha = _sealed_source(tmp_path)
    external_git = tmp_path / "external.git"
    (source / ".git").rename(external_git)
    (source / ".git").symlink_to(external_git, target_is_directory=True)

    completed = _verify_source(source, source_sha)

    assert completed.returncode == 1
    assert completed.stdout == ""
    assert completed.stderr == "error: controller installer Git metadata is unsafe\n"


def test_file_transaction_restores_replaced_and_new_files_after_failure(tmp_path: Path) -> None:
    existing = tmp_path / "existing"
    created = tmp_path / "created"
    existing.write_bytes(b"original\n")
    existing.chmod(0o640)
    uid = os.getuid()
    gid = os.getgid()

    with pytest.raises(RuntimeError, match="injected failure"):
        with controller_installer.AtomicFileTransaction() as transaction:
            transaction.publish(
                existing,
                b"replacement\n",
                mode=0o600,
                uid=uid,
                gid=gid,
            )
            transaction.publish(
                created,
                b"created\n",
                mode=0o644,
                uid=uid,
                gid=gid,
            )
            raise RuntimeError("injected failure")

    assert existing.read_bytes() == b"original\n"
    assert stat.S_IMODE(existing.stat().st_mode) == 0o640
    assert not created.exists()


def test_file_transaction_removes_only_directories_created_before_failure(tmp_path: Path) -> None:
    retained = tmp_path / "retained"
    retained.mkdir(mode=0o750)
    created = retained / "created"
    nested = created / "nested"
    uid = os.getuid()
    gid = os.getgid()

    with pytest.raises(RuntimeError, match="injected failure"):
        with controller_installer.AtomicFileTransaction() as transaction:
            transaction.ensure_directory(
                retained,
                mode=0o750,
                uid=uid,
                gid=gid,
            )
            transaction.ensure_directory(
                created,
                mode=0o700,
                uid=uid,
                gid=gid,
            )
            transaction.ensure_directory(
                nested,
                mode=0o700,
                uid=uid,
                gid=gid,
            )
            raise RuntimeError("injected failure")

    assert retained.is_dir()
    assert stat.S_IMODE(retained.stat().st_mode) == 0o750
    assert not created.exists()


def test_file_transaction_preserves_replacement_directory_during_rollback(
    tmp_path: Path,
) -> None:
    created = tmp_path / "created"
    displaced = tmp_path / "displaced-created"
    uid = os.getuid()
    gid = os.getgid()
    replacement_inode: int | None = None

    with pytest.raises(controller_installer.ControllerInstallError, match="rollback failed"):
        with controller_installer.AtomicFileTransaction() as transaction:
            transaction.ensure_directory(created, mode=0o700, uid=uid, gid=gid)
            created.rename(displaced)
            created.mkdir(mode=0o700)
            replacement_inode = created.stat().st_ino
            raise RuntimeError("injected failure")

    assert displaced.is_dir()
    assert replacement_inode is not None
    assert created.stat().st_ino == replacement_inode


def test_file_transaction_removes_directory_when_metadata_publication_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created = tmp_path / "created"
    uid = os.getuid()
    gid = os.getgid()
    real_chown = os.chown

    def fail_created_chown(path: Path | str, owner: int, group: int) -> None:
        if Path(path).name.startswith(f".loom-directory-{created.name}."):
            raise OSError("injected chown failure")
        real_chown(path, owner, group)

    monkeypatch.setattr(controller_installer.os, "chown", fail_created_chown)

    with pytest.raises(OSError, match="injected chown failure"):
        with controller_installer.AtomicFileTransaction() as transaction:
            transaction.ensure_directory(created, mode=0o700, uid=uid, gid=gid)

    assert not created.exists()


def test_file_transaction_rolls_back_published_directory_when_parent_fsync_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created = tmp_path / "created"
    uid = os.getuid()
    gid = os.getgid()
    real_fsync = controller_installer._fsync_directory
    failed = False

    def fail_first_parent_fsync(path: Path) -> None:
        nonlocal failed
        if path == created.parent and created.exists() and not failed:
            failed = True
            raise OSError("injected parent fsync failure")
        real_fsync(path)

    monkeypatch.setattr(controller_installer, "_fsync_directory", fail_first_parent_fsync)

    with pytest.raises(OSError, match="injected parent fsync failure"):
        with controller_installer.AtomicFileTransaction() as transaction:
            transaction.ensure_directory(created, mode=0o700, uid=uid, gid=gid)

    assert failed is True
    assert not created.exists()


def test_file_transaction_rerun_accepts_hard_stop_after_directory_publication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created = tmp_path / "created"
    uid = os.getuid()
    gid = os.getgid()
    real_rename = controller_installer._rename_noreplace

    class SimulatedHardStop(BaseException):
        pass

    def rename_then_stop(source: Path, destination: Path) -> None:
        real_rename(source, destination)
        raise SimulatedHardStop

    monkeypatch.setattr(controller_installer, "_rename_noreplace", rename_then_stop)

    with pytest.raises(SimulatedHardStop):
        with controller_installer.AtomicFileTransaction() as transaction:
            transaction.ensure_directory(created, mode=0o750, uid=uid, gid=gid)

    assert created.is_dir()
    assert stat.S_IMODE(created.stat().st_mode) == 0o750
    assert created.stat().st_uid == uid
    assert created.stat().st_gid == gid

    rerun = controller_installer.AtomicFileTransaction()
    with rerun:
        rerun.ensure_directory(created, mode=0o750, uid=uid, gid=gid)
    rerun.commit()


def test_file_transaction_can_compensate_after_successful_prepare(tmp_path: Path) -> None:
    existing = tmp_path / "existing"
    existing.write_bytes(b"original\n")
    existing.chmod(0o640)
    uid = os.getuid()
    gid = os.getgid()
    transaction = controller_installer.AtomicFileTransaction()

    with transaction:
        transaction.publish(existing, b"prepared\n", mode=0o600, uid=uid, gid=gid)

    assert existing.read_bytes() == b"prepared\n"
    transaction.rollback()
    assert existing.read_bytes() == b"original\n"
    assert stat.S_IMODE(existing.stat().st_mode) == 0o640


def test_installer_rolls_back_every_file_and_directory_before_authority_on_uv_readback_failure(
    tmp_path: Path,
) -> None:
    context, system_root = _install_context(tmp_path)
    managed_files = {
        "usr/local/bin/kubectl": (b"old kubectl\n", 0o711),
        "usr/local/bin/uv": (b"old uv\n", 0o710),
        "usr/local/libexec/loom-gb10-slurm-acceptance-authority": (
            b"old acceptance\n",
            0o700,
        ),
        "usr/local/libexec/loom-gb10-external-supervisor-broker": (
            b"old broker\n",
            0o701,
        ),
        "etc/tmpfiles.d/loom-gb10-slurm-authority.conf": (b"old tmpfiles\n", 0o600),
        "etc/sudoers.d/loom-gb10-external-supervisor": (b"old sudoers\n", 0o400),
    }
    for relative, (payload, mode) in managed_files.items():
        path = system_root / relative
        path.write_bytes(payload)
        path.chmod(mode)
    authority = tmp_path / "authority-state"
    authority.write_text("legacy authority\n", encoding="ascii")
    before = _system_tree_snapshot(system_root)
    backend = InstallerBackend(
        fail_uv_path=system_root / "usr/local/bin/uv",
        authority_path=authority,
    )

    with pytest.raises(
        controller_installer.ControllerInstallError,
        match="injected uv readback failure",
    ):
        controller_installer.ControllerInstaller(context=context, backend=backend).install()

    assert _system_tree_snapshot(system_root) == before
    assert authority.read_text(encoding="ascii") == "legacy authority\n"


def test_installer_validates_host_and_publishes_authority_only_after_exact_readback(
    tmp_path: Path,
) -> None:
    context, system_root = _install_context(tmp_path)
    authority = tmp_path / "authority-state"
    backend = InstallerBackend(authority_path=authority, system_root=system_root)

    controller_installer.ControllerInstaller(context=context, backend=backend).install()

    assert authority.read_text(encoding="ascii") == "published\n"
    assert stat.S_IMODE((system_root / "usr/local/bin/kubectl").stat().st_mode) == 0o755
    assert (
        stat.S_IMODE((system_root / "etc/sudoers.d/loom-gb10-external-supervisor").stat().st_mode)
        == 0o440
    )
    assert (
        stat.S_IMODE((system_root / "var/lib/loom-rollout/.config/systemd/user").stat().st_mode)
        == 0o750
    )


def test_installer_preserves_existing_acceptance_lock_inode(tmp_path: Path) -> None:
    context, system_root = _install_context(tmp_path)
    runtime = system_root / "run/loom-gb10-slurm-authority"
    jobs = runtime / "jobs"
    runtime.mkdir(mode=0o700)
    jobs.mkdir(mode=0o700)
    lock = runtime / "acceptance.lock"
    lock.write_bytes(b"")
    lock.chmod(0o600)
    inode = lock.stat().st_ino
    authority = tmp_path / "authority-state"
    backend = InstallerBackend(authority_path=authority, system_root=system_root)

    controller_installer.ControllerInstaller(context=context, backend=backend).install()

    assert lock.stat().st_ino == inode
    assert lock.read_bytes() == b""
    assert authority.read_text(encoding="ascii") == "published\n"


def test_installer_blocks_termination_signals_through_authority_commit(tmp_path: Path) -> None:
    context, system_root = _install_context(tmp_path)
    authority = tmp_path / "authority-state"
    previous = signal.pthread_sigmask(signal.SIG_BLOCK, set())
    backend = InstallerBackend(
        authority_path=authority,
        system_root=system_root,
        require_blocked_signals=True,
    )

    controller_installer.ControllerInstaller(context=context, backend=backend).install()

    assert signal.pthread_sigmask(signal.SIG_BLOCK, set()) == previous
    assert authority.read_text(encoding="ascii") == "published\n"


def test_installer_treats_successful_authority_exit_as_final_acknowledgment(
    tmp_path: Path,
) -> None:
    context, system_root = _install_context(tmp_path)
    authority = tmp_path / "authority-state"
    authority_broker = tmp_path / "authority-broker"
    authority_broker.write_text(
        "#!/bin/sh\n"
        f"printf 'committed\\n' > '{authority}'\n"
        "printf 'publication complete\\n'\n"
        "printf 'diagnostic after commit\\n' >&2\n",
        encoding="utf-8",
    )
    authority_broker.chmod(0o700)
    subprocess_backend = controller_installer.SubprocessBackend()

    class CommittingAuthorityBackend(InstallerBackend):
        def publish_authority(
            self,
            broker_path: Path,
            controller_public_key: Path,
            legacy_public_key: Path,
        ) -> None:
            assert self.host_validated
            assert broker_path.read_bytes() == b"#!/usr/bin/python3\n"
            subprocess_backend.publish_authority(
                authority_broker,
                controller_public_key,
                legacy_public_key,
            )

    backend = CommittingAuthorityBackend(system_root=system_root)

    controller_installer.ControllerInstaller(context=context, backend=backend).install()

    assert authority.read_text(encoding="ascii") == "committed\n"
    assert (system_root / "usr/local/bin/kubectl").read_bytes() == b"kubectl fixture\n"
    assert (system_root / "usr/local/bin/uv").read_bytes() == b"uv fixture\n"


@pytest.mark.parametrize(
    ("field", "invalid"),
    [
        ("machine", "x86_64"),
        ("hostname", "gx10-02a1"),
        ("cluster_name", "other"),
        ("service_uid", 996),
        ("service_gid", 2008),
        ("service_home", Path("/home/loom-rollout")),
    ],
)
def test_host_facts_reject_every_wrong_controller_identity(field: str, invalid: object) -> None:
    exact = controller_installer.HostFacts(
        machine="aarch64",
        hostname="gx10-01c7",
        cluster_name="trt-gb10",
        service_uid=995,
        service_gid=2007,
        service_home=Path("/var/lib/loom-rollout"),
    )

    controller_installer.validate_host_facts(exact)
    with pytest.raises(controller_installer.ControllerInstallError, match="host identity"):
        controller_installer.validate_host_facts(replace(exact, **{field: invalid}))


def test_install_cli_rejects_non_root_before_inspecting_install_inputs(tmp_path: Path) -> None:
    if os.geteuid() == 0:
        pytest.skip("non-root CLI contract")

    completed = subprocess.run(
        [
            sys.executable,
            str(INSTALLER),
            "install",
            "--trusted-root",
            str(tmp_path / "trusted"),
            "--source-root",
            str(tmp_path / ("a" * 40)),
            "--source-sha",
            "a" * 40,
            "--kubectl-source",
            str(tmp_path / "kubectl"),
            "--uv-source",
            str(tmp_path / "uv"),
            "--controller-public-key",
            str(tmp_path / "controller.pub"),
            "--legacy-public-key",
            str(tmp_path / "legacy.pub"),
        ],
        capture_output=True,
        check=False,
        text=True,
    )

    assert completed.returncode == 1
    assert completed.stdout == ""
    assert completed.stderr == "error: controller installation requires root\n"


def test_subprocess_backend_validates_exact_tools_and_publishes_through_broker(
    tmp_path: Path,
) -> None:
    kubectl = tmp_path / "kubectl"
    kubectl.write_text(
        "#!/bin/sh\n"
        "[ \"$*\" = 'version --client -o json' ] || exit 9\n"
        'printf \'%s\\n\' \'{"clientVersion":{"gitVersion":"v1.36.2"}}\'\n',
        encoding="utf-8",
    )
    uv = tmp_path / "uv"
    uv.write_text(
        "#!/bin/sh\n"
        "[ \"$*\" = '--version' ] || exit 9\n"
        "printf '%s\\n' 'uv 0.11.26 (aarch64-unknown-linux-gnu)'\n",
        encoding="utf-8",
    )
    acceptance = tmp_path / "acceptance.py"
    acceptance.write_text(
        "import argparse\nargparse.ArgumentParser().parse_args()\n",
        encoding="utf-8",
    )
    visudo = tmp_path / "visudo"
    visudo.write_text(
        "#!/bin/sh\n"
        "[ \"$1\" = '-cf' ] || exit 9\n"
        "grep -qxF 'qianyi ALL=(root) NOPASSWD:NOSETENV: "
        '/usr/local/libexec/loom-gb10-external-supervisor-broker ""\' "$2"\n',
        encoding="utf-8",
    )
    broker_log = tmp_path / "broker.log"
    broker = tmp_path / "broker"
    broker.write_text(
        f"#!/bin/sh\nprintf '%s\\n' \"$@\" > '{broker_log}'\n",
        encoding="utf-8",
    )
    for executable in (kubectl, uv, visudo, broker):
        executable.chmod(0o700)
    sudoers = tmp_path / "sudoers"
    sudoers.write_bytes(
        b"qianyi ALL=(root) NOPASSWD:NOSETENV: "
        b'/usr/local/libexec/loom-gb10-external-supervisor-broker ""\n'
    )
    controller_key = tmp_path / "controller.pub"
    legacy_key = tmp_path / "legacy.pub"
    controller_key.write_bytes(_public_key(7, "controller"))
    legacy_key.write_bytes(_public_key(8, "legacy"))
    facts = controller_installer.HostFacts(
        machine="aarch64",
        hostname="gx10-01c7",
        cluster_name="trt-gb10",
        service_uid=995,
        service_gid=2007,
        service_home=Path("/var/lib/loom-rollout"),
    )
    backend = controller_installer.SubprocessBackend(
        host_facts=facts,
        visudo_path=visudo,
    )

    backend.validate_host()
    backend.validate_kubectl(kubectl)
    backend.validate_uv(uv)
    backend.validate_acceptance(acceptance)
    backend.validate_sudoers(sudoers)
    backend.publish_authority(broker, controller_key, legacy_key)

    assert broker_log.read_text(encoding="utf-8").splitlines() == [
        "--install-authority",
        str(controller_key),
        str(legacy_key),
    ]


def test_installer_rejects_symlinked_unmanaged_destination_parent_before_publication(
    tmp_path: Path,
) -> None:
    context, system_root = _install_context(tmp_path)
    external = tmp_path / "external-bin"
    external.mkdir(mode=0o755)
    (external / "sentinel").write_text("outside\n", encoding="ascii")
    bin_path = system_root / "usr/local/bin"
    bin_path.rmdir()
    bin_path.symlink_to(external, target_is_directory=True)
    authority = tmp_path / "authority-state"
    authority.write_text("legacy authority\n", encoding="ascii")
    backend = InstallerBackend(authority_path=authority)

    with pytest.raises(controller_installer.ControllerInstallError, match="ancestor is unsafe"):
        controller_installer.ControllerInstaller(context=context, backend=backend).install()

    assert sorted(path.name for path in external.iterdir()) == ["sentinel"]
    assert (external / "sentinel").read_text(encoding="ascii") == "outside\n"
    assert authority.read_text(encoding="ascii") == "legacy authority\n"
