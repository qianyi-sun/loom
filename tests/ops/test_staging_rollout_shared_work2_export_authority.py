from __future__ import annotations

import os
from pathlib import Path
from types import SimpleNamespace

import pytest
from scripts.ops import staging_rollout_shared_work2_export_authority as authority


class Result:
    def __init__(self, returncode: int, stdout: str = "", stderr: str = "") -> None:
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


def _policy() -> authority.AuthorityPolicy:
    return authority.AuthorityPolicy(
        source_sha="a" * 40,
        source_tree_sha="b" * 40,
        source_base_sha=authority.APPROVED_BASE_SHA,
        wrapper_sha256="c" * 64,
        validator_sha256="d" * 64,
        sudoers_sha256="e" * 64,
    )


def test_sudoers_exposes_only_two_fixed_no_environment_commands() -> None:
    payload = authority.SOURCE_ROOT.parents[2] / authority.SUDOERS_RELATIVE
    # Read from the repository, not the fixed runtime path.
    payload = Path("deploy/worker-pools/gb10") / payload.name
    lines = payload.read_text(encoding="ascii").splitlines()

    assert lines == [
        "qianyi ALL=(root) NOPASSWD:NOSETENV: "
        "/usr/local/libexec/loom-staging-rollout-shared-work2-export-authority install",
        "qianyi ALL=(root) NOPASSWD:NOSETENV: "
        "/usr/local/libexec/loom-staging-rollout-shared-work2-export-authority check",
    ]
    assert "*" not in "\n".join(lines)


@pytest.mark.parametrize("argument", ["shell", "install extra", "--source-sha"])
def test_runtime_parser_rejects_every_nonfixed_surface(argument: str) -> None:
    with pytest.raises(SystemExit):
        authority._parser().parse_args(argument.split())


def test_policy_is_exact_and_rejects_any_base_or_key_drift() -> None:
    policy = _policy()
    assert policy.source_base_sha == authority.APPROVED_BASE_SHA
    assert b'"source_mode":"sealed-cumulative"' in policy.payload()

    with pytest.raises(authority.AuthorityError, match="approved base"):
        authority.AuthorityPolicy(
            source_sha="a" * 40,
            source_tree_sha="b" * 40,
            source_base_sha="f" * 40,
            wrapper_sha256="c" * 64,
            validator_sha256="d" * 64,
            sudoers_sha256="e" * 64,
        )


def test_journal_accepts_only_exact_sanitized_policy_records(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    policy = _policy()
    valid = {
        "action": "install",
        "changed": True,
        "operator": authority.OPERATOR,
        "source_base_sha": policy.source_base_sha,
        "source_sha": policy.source_sha,
        "source_tree_sha": policy.source_tree_sha,
        "timestamp_ns": 1,
    }
    monkeypatch.setattr(
        authority,
        "_regular_root_file",
        lambda *_args, **_kwargs: (authority.json.dumps(valid) + "\n").encode("ascii"),
    )
    authority._validate_journal(policy)

    valid["source_sha"] = "f" * 40
    with pytest.raises(authority.AuthorityError, match="journal is invalid"):
        authority._validate_journal(policy)


def test_fixed_helper_receives_only_policy_bound_values() -> None:
    calls: list[tuple[tuple[str, ...], dict[str, str]]] = []

    def run(argv, env):  # type: ignore[no-untyped-def]
        calls.append((tuple(argv), dict(env)))
        return Result(0, "changed\n")

    assert authority._invoke_helper("install", _policy(), run) is True
    argv, env = calls[0]
    assert argv == (
        "/usr/bin/python3",
        str(authority.SOURCE_ROOT / authority.HELPER_RELATIVE),
        "install",
        "--sealed-source-sha",
        "a" * 40,
        "--sealed-source-tree",
        "b" * 40,
        "--sealed-approved-base-sha",
        authority.APPROVED_BASE_SHA,
    )
    assert set(env) == {
        "PATH",
        "LANG",
        "LC_ALL",
        "GIT_CONFIG_NOSYSTEM",
        "GIT_CONFIG_GLOBAL",
        "GIT_TERMINAL_PROMPT",
        "GIT_OPTIONAL_LOCKS",
    }


def test_helper_rejects_stderr_unknown_output_and_failure() -> None:
    for result in (Result(1), Result(0, "unexpected\n"), Result(0, "ok\n", "warning\n")):
        with pytest.raises(authority.AuthorityError, match="failed safely"):
            authority._invoke_helper(
                "check", _policy(), lambda _argv, _env, outcome=result: outcome
            )


def test_dispatch_revalidates_identity_assets_and_source_before_helper(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    order: list[str] = []
    lock = tmp_path / "lock"
    lock.write_bytes(b"")
    descriptor = os.open(lock, os.O_RDONLY)
    monkeypatch.setattr(authority, "_validate_invoker", lambda *_args: order.append("identity"))
    monkeypatch.setattr(authority, "_open_lock", lambda **_kwargs: descriptor)
    monkeypatch.setattr(authority, "_read_policy", lambda: order.append("policy") or _policy())
    monkeypatch.setattr(
        authority,
        "_validate_runtime_assets",
        lambda _policy: order.append("assets"),
    )
    monkeypatch.setattr(authority, "_validate_source", lambda _policy: order.append("source"))
    monkeypatch.setattr(
        authority,
        "_invoke_helper",
        lambda _verb, _policy, _run: order.append("helper") or False,
    )
    monkeypatch.setattr(
        authority,
        "_journal_install",
        lambda _policy, **_kwargs: order.append("journal"),
    )

    report = authority.dispatch("check", environ={}, run=lambda _argv, _env: Result(0))
    assert report["status"] == "ok"
    assert order == ["identity", "policy", "assets", "source", "helper"]


def test_install_journals_only_after_successful_fixed_helper(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lock = tmp_path / "lock"
    lock.write_bytes(b"")
    monkeypatch.setattr(authority, "_validate_invoker", lambda *_args: None)
    monkeypatch.setattr(authority, "_open_lock", lambda **_kwargs: os.open(lock, os.O_RDONLY))
    monkeypatch.setattr(authority, "_read_policy", _policy)
    monkeypatch.setattr(authority, "_validate_runtime_assets", lambda _policy: None)
    monkeypatch.setattr(authority, "_validate_source", lambda _policy: None)
    monkeypatch.setattr(authority, "_validate_journal", lambda _policy: None)
    monkeypatch.setattr(authority, "_invoke_helper", lambda *_args: True)
    journaled: list[bool] = []
    monkeypatch.setattr(
        authority,
        "_journal_install",
        lambda _policy, *, changed: journaled.append(changed),
    )

    report = authority.dispatch("install", environ={})
    assert report["changed"] is True
    assert journaled == [True]


def test_bootstrap_refuses_sudo_and_runtime_refuses_direct_root(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(authority.os, "geteuid", lambda: 0)
    monkeypatch.setenv("SUDO_USER", "qianyi")
    with pytest.raises(authority.AuthorityError, match="external root administrator"):
        authority.bootstrap("a" * 40, "b" * 40)

    class Account:
        pw_uid = 1000
        pw_gid = 1000

    monkeypatch.setattr(authority.pwd, "getpwnam", lambda _name: Account())
    with pytest.raises(authority.AuthorityError, match="not approved"):
        authority._validate_invoker("check", {})


def test_invoker_requires_exact_sudo_identity_and_command(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Account:
        pw_uid = 1000
        pw_gid = 1001

    monkeypatch.setattr(authority.os, "geteuid", lambda: 0)
    monkeypatch.setattr(authority.pwd, "getpwnam", lambda _name: Account())
    approved = {
        "SUDO_USER": authority.OPERATOR,
        "SUDO_UID": "1000",
        "SUDO_GID": "1001",
        "SUDO_COMMAND": f"{authority.LIBEXEC} install",
    }
    authority._validate_invoker("install", approved)

    for key, value in (
        ("SUDO_USER", "other"),
        ("SUDO_UID", "0"),
        ("SUDO_GID", "0"),
        ("SUDO_COMMAND", f"{authority.LIBEXEC} install extra"),
    ):
        drifted = dict(approved)
        drifted[key] = value
        with pytest.raises(authority.AuthorityError, match="not approved"):
            authority._validate_invoker("install", drifted)


def test_bootstrap_rolls_back_only_files_created_by_failed_attempt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixed_entrypoint = authority.SOURCE_ROOT / "scripts/ops" / Path(authority.__file__).name
    monkeypatch.setattr(authority, "__file__", str(fixed_entrypoint))
    monkeypatch.setattr(authority.os, "geteuid", lambda: 0)
    monkeypatch.delenv("SUDO_USER", raising=False)
    monkeypatch.delenv("SUDO_UID", raising=False)
    monkeypatch.delenv("SUDO_GID", raising=False)
    monkeypatch.delenv("SUDO_COMMAND", raising=False)
    monkeypatch.setattr(authority, "_safe_root_directory", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(authority, "_ensure_root_directory", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(authority, "_regular_root_file", lambda *_args, **_kwargs: b"asset\n")

    validator = SimpleNamespace(
        SealedSource=lambda *_args, **_kwargs: object(),
        validate_sealed_source=lambda _source: None,
    )
    monkeypatch.setattr(authority, "_load_validator", lambda _path: validator)
    calls = 0

    def install(path, _payload, _mode):  # type: ignore[no-untyped-def]
        nonlocal calls
        calls += 1
        if calls == 2:
            raise authority.AuthorityError("injected publication failure")
        return True

    rolled_back: list[Path] = []
    monkeypatch.setattr(authority, "_atomic_install", install)
    monkeypatch.setattr(
        authority,
        "_rollback_created",
        lambda paths: rolled_back.extend(paths),
    )

    with pytest.raises(authority.AuthorityError, match="injected"):
        authority.bootstrap(
            "a" * 40,
            "b" * 40,
            run=lambda _argv, _env: Result(0),
        )
    assert rolled_back == [authority.LIBEXEC]


def _prepare_bootstrap_transaction(
    monkeypatch: pytest.MonkeyPatch,
) -> SimpleNamespace:
    fixed_entrypoint = authority.SOURCE_ROOT / "scripts/ops" / Path(authority.__file__).name
    monkeypatch.setattr(authority, "__file__", str(fixed_entrypoint))
    monkeypatch.setattr(authority.os, "geteuid", lambda: 0)
    for variable in ("SUDO_USER", "SUDO_UID", "SUDO_GID", "SUDO_COMMAND"):
        monkeypatch.delenv(variable, raising=False)
    monkeypatch.setattr(authority, "_safe_root_directory", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(authority, "_regular_root_file", lambda *_args, **_kwargs: b"asset\n")
    validator = SimpleNamespace(
        SealedSource=lambda *_args, **_kwargs: object(),
        validate_sealed_source=lambda _source: None,
    )
    monkeypatch.setattr(authority, "_load_validator", lambda _path: validator)

    state = SimpleNamespace(
        created_assets=[],
        created_directories=[],
        existing_directories={authority.POLICY.parent},
        publication_order=[],
    )

    def ensure(path: Path, *, mode: int) -> bool:
        assert mode == 0o755
        if path in state.existing_directories:
            return False
        state.created_directories.append(path)
        return True

    def rollback_assets(paths: list[Path]) -> None:
        for path in reversed(paths):
            state.created_assets.remove(path)

    def remove_directory(path: Path) -> None:
        state.created_directories.remove(path)

    monkeypatch.setattr(authority, "_ensure_root_directory", ensure)
    monkeypatch.setattr(authority, "_rollback_created", rollback_assets)
    monkeypatch.setattr(authority.os, "rmdir", remove_directory)
    return state


@pytest.mark.parametrize("failed_publication", range(1, 6))
def test_bootstrap_rolls_back_every_new_asset_and_directory_on_publication_failure(
    monkeypatch: pytest.MonkeyPatch,
    failed_publication: int,
) -> None:
    state = _prepare_bootstrap_transaction(monkeypatch)
    calls = 0

    def install(path: Path, _payload: bytes, _mode: int) -> bool:
        nonlocal calls
        calls += 1
        if calls == failed_publication:
            raise authority.AuthorityError("injected publication failure")
        state.publication_order.append(path)
        state.created_assets.append(path)
        return True

    monkeypatch.setattr(authority, "_atomic_install", install)

    with pytest.raises(authority.AuthorityError, match="injected publication"):
        authority.bootstrap("a" * 40, "b" * 40, run=lambda _argv, _env: Result(0))

    assert state.created_assets == []
    assert state.created_directories == []
    assert state.existing_directories == {authority.POLICY.parent}


@pytest.mark.parametrize("failed_directory", range(1, 4))
def test_bootstrap_rolls_back_prior_directories_when_directory_convergence_fails(
    monkeypatch: pytest.MonkeyPatch,
    failed_directory: int,
) -> None:
    state = _prepare_bootstrap_transaction(monkeypatch)
    calls = 0

    def ensure(path: Path, *, mode: int) -> bool:
        nonlocal calls
        calls += 1
        if calls == failed_directory:
            raise authority.AuthorityError("injected directory failure")
        if path in state.existing_directories:
            return False
        state.created_directories.append(path)
        return True

    monkeypatch.setattr(authority, "_ensure_root_directory", ensure)
    monkeypatch.setattr(
        authority,
        "_atomic_install",
        lambda *_args, **_kwargs: pytest.fail("asset publication must not start"),
    )

    with pytest.raises(authority.AuthorityError, match="injected directory"):
        authority.bootstrap("a" * 40, "b" * 40, run=lambda _argv, _env: Result(0))

    assert state.created_assets == []
    assert state.created_directories == []
    assert state.existing_directories == {authority.POLICY.parent}


def test_bootstrap_rolls_back_sudoers_and_all_dependencies_when_final_validation_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _prepare_bootstrap_transaction(monkeypatch)

    def install(path: Path, _payload: bytes, _mode: int) -> bool:
        state.publication_order.append(path)
        state.created_assets.append(path)
        return True

    monkeypatch.setattr(authority, "_atomic_install", install)
    validations = 0

    def run(_argv, _env):  # type: ignore[no-untyped-def]
        nonlocal validations
        validations += 1
        return Result(0 if validations == 1 else 1)

    with pytest.raises(authority.AuthorityError, match="sudoers is invalid"):
        authority.bootstrap("a" * 40, "b" * 40, run=run)

    assert state.publication_order[-1] == authority.SUDOERS
    assert state.created_assets == []
    assert state.created_directories == []
    assert state.existing_directories == {authority.POLICY.parent}


def test_bootstrap_creates_missing_libexec_inside_the_single_transaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _prepare_bootstrap_transaction(monkeypatch)

    def install(path: Path, _payload: bytes, _mode: int) -> bool:
        state.publication_order.append(path)
        state.created_assets.append(path)
        return True

    monkeypatch.setattr(authority, "_atomic_install", install)
    report = authority.bootstrap("a" * 40, "b" * 40, run=lambda _argv, _env: Result(0))

    assert authority.LIBEXEC.parent in state.created_directories
    assert state.publication_order[-1] == authority.SUDOERS
    assert report["changed"] == [str(path) for path in state.publication_order]


def test_directory_creation_cleans_up_when_metadata_convergence_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / "new-directory"

    def safe_directory(path: Path, *, mode: int) -> None:
        assert mode == 0o755
        if path == target and not target.exists():
            raise authority.AuthorityError("absent")

    monkeypatch.setattr(authority, "_safe_root_directory", safe_directory)
    monkeypatch.setattr(
        authority.os,
        "chown",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("injected chown failure")),
    )

    with pytest.raises(authority.AuthorityError, match="creation failed safely"):
        authority._ensure_root_directory(target, mode=0o755)

    assert not target.exists()


def test_atomic_install_removes_temporary_file_when_payload_write_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    destination = tmp_path / "asset"
    monkeypatch.setattr(authority, "_safe_root_directory", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        authority,
        "_regular_root_file",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(authority.AuthorityError("absent")),
    )
    monkeypatch.setattr(
        authority,
        "_write_all",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            authority.AuthorityError("injected write failure")
        ),
    )

    with pytest.raises(authority.AuthorityError, match="publication failed safely"):
        authority._atomic_install(destination, b"payload", 0o600)

    assert list(tmp_path.iterdir()) == []


def test_atomic_install_removes_published_file_when_directory_fsync_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    destination = tmp_path / "asset"
    monkeypatch.setattr(authority, "_safe_root_directory", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        authority,
        "_regular_root_file",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(authority.AuthorityError("absent")),
    )
    monkeypatch.setattr(authority.os, "fchown", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        authority,
        "_rename_noreplace",
        lambda directory, source, destination: os.rename(
            source,
            destination,
            src_dir_fd=directory,
            dst_dir_fd=directory,
        ),
    )
    real_fsync = authority.os.fsync
    calls = 0

    def fail_first_directory_sync(descriptor: int) -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("injected directory fsync failure")
        real_fsync(descriptor)

    monkeypatch.setattr(authority.os, "fsync", fail_first_directory_sync)

    with pytest.raises(authority.AuthorityError, match="publication failed safely"):
        authority._atomic_install(destination, b"payload", 0o600)

    assert calls == 3
    assert list(tmp_path.iterdir()) == []


def test_check_uses_read_only_shared_lock(monkeypatch: pytest.MonkeyPatch) -> None:
    opened: list[int] = []
    monkeypatch.setattr(authority.os, "open", lambda _path, flags: opened.append(flags) or 41)
    monkeypatch.setattr(
        authority.os, "fstat", lambda _fd: os.stat_result((0o100600, 0, 0, 1, 0, 0, 0, 0, 0, 0))
    )
    monkeypatch.setattr(authority.fcntl, "flock", lambda *_args: None)
    monkeypatch.setattr(authority.os, "close", lambda _fd: None)

    assert authority._open_lock(exclusive=False) == 41
    assert opened[0] & os.O_ACCMODE == os.O_RDONLY
