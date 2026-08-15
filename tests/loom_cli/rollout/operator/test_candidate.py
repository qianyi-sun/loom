from __future__ import annotations

import hashlib
import inspect
import json
import os
import subprocess
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
from scripts.ops import staging_rollout_sealed_source as sealed_source

from loom_cli.rollout.operator import candidate
from loom_cli.rollout.operator import config as operator_config
from loom_cli.rollout.operator.config import OperatorConfig

FRESH_SHA = "abcdef1234567890abcdef1234567890abcdef12"
CACHED_SHA = "1111111111111111111111111111111111111111"
FETCH_URL = "https://github.com/qianyi-sun/loom.git"
FETCHED_AT = datetime(2026, 7, 13, 20, 0, tzinfo=UTC)


def make_config(runner_repo: Path) -> OperatorConfig:
    config_path = runner_repo.parent.parent / "staging-rollout.toml"
    config_sha256 = (
        hashlib.sha256(config_path.read_bytes()).hexdigest() if config_path.is_file() else "1" * 64
    )
    return OperatorConfig(
        schema_version=1,
        service_user="loom-rollout",
        operator_group="loom-staging-operators",
        remote_url=FETCH_URL,
        target_ref="refs/heads/dev",
        runner_repo=runner_repo,
        state_root=Path("/var/lib/loom-staging-rollout"),
        runtime_root=Path("/run/loom-staging-rollout"),
        rollout_root=Path("/data/loom-staging"),
        kubeconfig_path=Path("/var/lib/loom-staging-rollout/kubeconfig"),
        cluster_config_path=(
            runner_repo / "deploy/environments/staging.multinode.cluster.toml"
        ),
        admin_token_source="file:/var/lib/loom-staging-rollout/credentials/admin-token",
        worker_token_source="file:/var/lib/loom-staging-rollout/credentials/worker-token",
        service_token_source="file:/var/lib/loom-staging-rollout/credentials/service-token",
        expect_admin_token_fingerprint="sha256:abc123def456 len=64",
        cluster_name="loom-staging",
        namespace="loom-staging",
        environment="staging",
        cp_url="http://127.0.0.1:18081",
        smoke_on_behalf_username="devansh",
        smoke_on_behalf_team_id="11111111-1111-4111-8111-111111111111",
        scope="current-gb10",
        gb10_prep_concurrency=8,
        config_path=config_path,
        config_sha256=config_sha256,
    )


def fixed_now() -> datetime:
    return FETCHED_AT


def test_cumulative_history_bound_matches_root_sealed_source_validator() -> None:
    assert candidate.MAX_CUMULATIVE_COMMITS == sealed_source.MAX_CUMULATIVE_COMMITS == 512


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), *args],
        check=True,
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "GIT_AUTHOR_NAME": "Sealed Test",
            "GIT_AUTHOR_EMAIL": "sealed@example.invalid",
            "GIT_COMMITTER_NAME": "Sealed Test",
            "GIT_COMMITTER_EMAIL": "sealed@example.invalid",
        },
    ).stdout.strip()


@pytest.fixture
def trusted_repo(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    candidate_root = tmp_path / "candidates"
    runtime_root = candidate_root / FRESH_SHA
    repo = runtime_root / "repo"
    repo.mkdir(parents=True, mode=0o755)
    (runtime_root / "venv").mkdir(mode=0o755)
    (runtime_root / "venv" / ".lock").write_text("", encoding="utf-8")
    (runtime_root / "venv" / ".lock").chmod(0o600)
    (runtime_root / "venv" / "bin").mkdir(mode=0o755)
    (runtime_root / "venv" / "bin" / "python").symlink_to("/usr/bin/python3")
    (repo / ".git").mkdir(mode=0o755)
    git_config = repo / ".git" / "config"
    git_config.write_text("[core]\n\trepositoryformatversion = 0\n", encoding="utf-8")
    config_path = candidate_root / "staging-rollout.toml"
    config_path.write_text("schema_version = 1\n", encoding="utf-8")
    for directory in (
        candidate_root,
        runtime_root,
        repo,
        repo / ".git",
        runtime_root / "venv",
        runtime_root / "venv" / "bin",
    ):
        directory.chmod(0o755)
    git_config.chmod(0o644)
    config_path.chmod(0o600)
    monkeypatch.setattr(
        candidate.pwd,
        "getpwnam",
        lambda username: SimpleNamespace(pw_name=username, pw_uid=os.getuid()),
    )
    monkeypatch.setattr(operator_config, "CANDIDATE_RUNTIME_ROOT", candidate_root)
    original_read_config = operator_config._read_protected_config
    monkeypatch.setattr(
        candidate,
        "_read_protected_config",
        lambda path, _owner: original_read_config(path, os.getuid()),
    )
    real_lstat = os.lstat

    def root_owned_config_lstat(path: os.PathLike[str] | str) -> object:
        metadata = real_lstat(path)
        candidate_path = Path(path)
        if candidate_path == config_path or candidate_path.is_relative_to(candidate_root):
            return SimpleNamespace(st_mode=metadata.st_mode, st_uid=0, st_gid=0)
        return metadata

    monkeypatch.setattr(candidate.os, "lstat", root_owned_config_lstat)
    return repo


class FakeRunner:
    def __init__(self, repo: Path) -> None:
        self.repo = str(repo)
        self.argvs: list[list[str]] = []
        self.remote_returncode = 0
        self.remote_stdout = "origin\n"
        self.url_returncode = 0
        self.url_stdout = f"{FETCH_URL}\n"
        self.url_stderr = ""
        self.pushurl_returncode = 1
        self.pushurl_stdout = ""
        self.pushurl_stderr = ""
        self.status_returncode = 0
        self.status_stdout = ""
        self.symbolic_returncode = 1
        self.symbolic_stdout = ""
        self.symbolic_stderr = ""
        self.rev_parse_returncode = 0
        self.rev_parse_stdout: str | None = None
        self.tree_returncode = 0
        self.tree_stdout = "2" * 40 + "\n"

    def __call__(self, argv: list[str]) -> subprocess.CompletedProcess[str]:
        self.argvs.append(list(argv))
        repo_index = argv.index("-C")
        assert argv[repo_index + 1] == self.repo
        operation = argv[repo_index + 2 :]
        if operation == ["remote"]:
            return self._result(argv, self.remote_returncode, self.remote_stdout)
        if operation == ["remote", "get-url", "--all", "origin"]:
            return self._result(
                argv,
                self.url_returncode,
                self.url_stdout,
                self.url_stderr,
            )
        if operation == ["config", "--get-all", "remote.origin.pushurl"]:
            return self._result(
                argv,
                self.pushurl_returncode,
                self.pushurl_stdout,
                self.pushurl_stderr,
            )
        if operation == ["status", "--porcelain=v1", "--untracked-files=all"]:
            return self._result(argv, self.status_returncode, self.status_stdout)
        if operation == ["symbolic-ref", "-q", "HEAD"]:
            return self._result(
                argv,
                self.symbolic_returncode,
                self.symbolic_stdout,
                self.symbolic_stderr,
            )
        if operation == ["rev-parse", "--verify", "HEAD^{commit}"]:
            stdout = self.rev_parse_stdout
            if stdout is None:
                stdout = f"{FRESH_SHA}\n"
            return self._result(argv, self.rev_parse_returncode, stdout)
        if operation == ["rev-parse", "--verify", "HEAD^{tree}"]:
            return self._result(argv, self.tree_returncode, self.tree_stdout)
        raise AssertionError(f"unexpected command: {argv!r}")

    @staticmethod
    def _result(
        argv: list[str],
        returncode: int,
        stdout: str,
        stderr: str = "",
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(argv, returncode, stdout, stderr)


def test_sealed_binding_uses_exact_detached_local_candidate_without_fetch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    build_repo = tmp_path / "build-repo"
    build_repo.mkdir()
    _git(build_repo, "init")
    _git(build_repo, "remote", "add", "origin", FETCH_URL)
    (build_repo / "value.txt").write_text("base\n", encoding="utf-8")
    _git(build_repo, "add", "value.txt")
    _git(build_repo, "commit", "-m", "base")
    base = _git(build_repo, "rev-parse", "HEAD")
    (build_repo / "value.txt").write_text("sealed\n", encoding="utf-8")
    _git(build_repo, "commit", "-am", "sealed")
    commit = _git(build_repo, "rev-parse", "HEAD")
    tree = _git(build_repo, "rev-parse", "HEAD^{tree}")
    _git(build_repo, "checkout", "--detach", commit)
    candidate_root = tmp_path / "candidates"
    runtime_root = candidate_root / commit
    runtime_root.mkdir(parents=True)
    repo = runtime_root / "repo"
    build_repo.rename(repo)
    venv = runtime_root / "venv"
    venv.mkdir()
    venv_bin = venv / "bin"
    venv_bin.mkdir()
    (venv_bin / "python").symlink_to("/usr/bin/python3")
    for directory in (candidate_root, runtime_root, repo, repo / ".git", venv, venv_bin):
        directory.chmod(0o755)
    (repo / ".git" / "config").chmod(0o644)
    config_path = tmp_path / "staging-rollout.toml"
    config_path.write_text("schema_version = 2\n", encoding="utf-8")
    config_path.chmod(0o600)
    monkeypatch.setattr(
        candidate.pwd,
        "getpwnam",
        lambda username: SimpleNamespace(pw_name=username, pw_uid=os.getuid()),
    )
    monkeypatch.setattr(operator_config, "CANDIDATE_RUNTIME_ROOT", candidate_root)
    original_read_config = operator_config._read_protected_config
    monkeypatch.setattr(
        candidate,
        "_read_protected_config",
        lambda path, _owner: original_read_config(path, os.getuid()),
    )
    real_lstat = os.lstat

    def root_owned_config_lstat(path: os.PathLike[str] | str) -> object:
        metadata = real_lstat(path)
        candidate_path = Path(path)
        if candidate_path == config_path or candidate_path.is_relative_to(candidate_root):
            return SimpleNamespace(st_mode=metadata.st_mode, st_uid=0, st_gid=0)
        return metadata

    monkeypatch.setattr(candidate.os, "lstat", root_owned_config_lstat)
    config = replace(
        make_config(repo),
        config_path=config_path,
        config_sha256=hashlib.sha256(config_path.read_bytes()).hexdigest(),
        source_mode="sealed-cumulative",
        source_commit_sha=commit,
        source_tree_sha=tree,
        source_base_sha=base,
    )
    argvs: list[list[str]] = []

    def run(argv: list[str]) -> subprocess.CompletedProcess[str]:
        argvs.append(argv)
        return subprocess.run(argv, check=False, capture_output=True, text=True)

    binding = candidate.bind_configured_candidate(config, run=run, now=fixed_now)

    assert binding.source_mode == "sealed-cumulative"
    assert binding.resolved_sha == commit
    assert binding.resolved_tree == tree
    assert binding.approved_base_sha == base
    assert not any("fetch" in argv for argv in argvs)

    with pytest.raises(candidate.CandidateBindingError, match="tree identity drifted"):
        candidate.bind_configured_candidate(
            replace(config, source_tree_sha="f" * 40),
            run=run,
            now=fixed_now,
        )


def expected_argvs(repo: Path) -> list[list[str]]:
    return [
        candidate._git_argv(repo, "remote"),
        candidate._git_argv(repo, "remote", "get-url", "--all", "origin"),
        candidate._git_argv(repo, "config", "--get-all", "remote.origin.pushurl"),
        candidate._git_argv(repo, "symbolic-ref", "-q", "HEAD"),
        candidate._git_argv(repo, "rev-parse", "--verify", "HEAD^{commit}"),
        candidate._git_argv(repo, "rev-parse", "--verify", "HEAD^{tree}"),
    ]


def test_binding_uses_exact_installed_sha_without_mutating_git(trusted_repo: Path) -> None:
    runner = FakeRunner(trusted_repo)

    binding = candidate.bind_fresh_origin_dev(
        make_config(trusted_repo),
        run=runner,
        now=fixed_now,
    )

    assert runner.argvs == expected_argvs(trusted_repo)
    assert binding.remote_url == FETCH_URL
    assert binding.target_ref == "origin/dev"
    assert binding.resolved_sha == FRESH_SHA
    assert binding.image_tag == "staging-abcdef1"
    assert binding.fetched_at == "2026-07-13T20:00:00Z"
    # A merged-dev candidate carries the derived git tree (HEAD^{tree}) so the
    # Tier-1 artifact builders can bind it, but never an approved base sha.
    assert binding.source_mode == "merged-dev"
    assert binding.resolved_tree == "2" * 40
    assert binding.approved_base_sha is None
    mutating_operations = {"fetch", "checkout", "switch", "reset", "clean", "update-ref"}
    assert all(mutating_operations.isdisjoint(argv) for argv in runner.argvs)


def test_merged_candidate_binding_decouples_tree_from_approved_base() -> None:
    from loom_cli.rollout.operator.model import CandidateBinding

    tree = "2" * 40
    binding = CandidateBinding(
        remote_url=FETCH_URL,
        target_ref="origin/dev",
        resolved_sha=FRESH_SHA,
        image_tag="staging-abcdef1",
        fetched_at="2026-07-13T20:00:00Z",
        source_mode="merged-dev",
        resolved_tree=tree,
    )
    assert binding.source_mode == "merged-dev"
    assert binding.resolved_tree == tree
    assert binding.approved_base_sha is None

    # the derived tree survives to_dict/from_dict without an approved base
    payload = binding.to_dict()
    assert payload["resolved_tree"] == tree
    assert "approved_base_sha" not in payload
    assert CandidateBinding.from_dict(payload) == binding

    # a merged-dev candidate may still omit the tree (backward compatible)
    treeless = CandidateBinding(
        remote_url=FETCH_URL,
        target_ref="origin/dev",
        resolved_sha=FRESH_SHA,
        image_tag="staging-abcdef1",
        fetched_at="2026-07-13T20:00:00Z",
    )
    assert treeless.resolved_tree is None
    assert CandidateBinding.from_dict(treeless.to_dict()) == treeless

    # but never an approved base sha (the sealed-cumulative approval anchor)
    with pytest.raises(ValueError, match="must not carry an approved base sha"):
        CandidateBinding(
            remote_url=FETCH_URL,
            target_ref="origin/dev",
            resolved_sha=FRESH_SHA,
            image_tag="staging-abcdef1",
            fetched_at="2026-07-13T20:00:00Z",
            source_mode="merged-dev",
            resolved_tree=tree,
            approved_base_sha="1" * 40,
        )


def test_candidate_identity_digest_binds_runtime_and_config_fingerprints(
    trusted_repo: Path,
) -> None:
    config = make_config(trusted_repo)
    identity = candidate.verify_bound_candidate(
        config,
        candidate.bind_fresh_origin_dev(config, run=FakeRunner(trusted_repo), now=fixed_now),
        run=FakeRunner(trusted_repo),
    )
    expected = {
        "approved_base_sha": None,
        "config_sha256": config.config_sha256,
        "image_tag": f"staging-{FRESH_SHA[:7]}",
        "linear_history_count": 0,
        "resolved_sha": FRESH_SHA,
        "resolved_tree": "2" * 40,
        "runtime_root": str(trusted_repo.parent),
        "source_mode": "merged-dev",
    }

    assert (
        identity.evidence_digest
        == hashlib.sha256(
            json.dumps(expected, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
    )


def test_installed_head_drift_prevents_binding(trusted_repo: Path) -> None:
    runner = FakeRunner(trusted_repo)
    runner.rev_parse_stdout = CACHED_SHA + "\n"

    with pytest.raises(candidate.CandidateBindingError, match="commit identity drifted"):
        candidate.bind_fresh_origin_dev(
            make_config(trusted_repo),
            run=runner,
            now=fixed_now,
        )

    assert runner.argvs == expected_argvs(trusted_repo)[:-1]


@pytest.mark.parametrize("remotes", ["upstream\n", "origin\nupstream\n", "", "origin\norigin\n"])
def test_binding_rejects_any_remote_set_except_single_origin(
    trusted_repo: Path,
    remotes: str,
) -> None:
    runner = FakeRunner(trusted_repo)
    runner.remote_stdout = remotes

    with pytest.raises(candidate.CandidateBindingError, match="only remote origin"):
        candidate.bind_fresh_origin_dev(
            make_config(trusted_repo),
            run=runner,
            now=fixed_now,
        )

    assert runner.argvs == expected_argvs(trusted_repo)[:1]


@pytest.mark.parametrize(
    "urls",
    [
        "https://github.com/carinrc/loom.git\n",
        f"{FETCH_URL}\n{FETCH_URL}\n",
        f"{FETCH_URL}\nhttps://example.invalid/loom.git\n",
        "",
    ],
)
def test_binding_rejects_wrong_or_multiple_fetch_urls(trusted_repo: Path, urls: str) -> None:
    runner = FakeRunner(trusted_repo)
    runner.url_stdout = urls

    with pytest.raises(candidate.CandidateBindingError, match="fetch URL"):
        candidate.bind_fresh_origin_dev(
            make_config(trusted_repo),
            run=runner,
            now=fixed_now,
        )

    assert runner.argvs == expected_argvs(trusted_repo)[:2]


@pytest.mark.parametrize(
    ("returncode", "stdout", "stderr"),
    [
        (0, f"{FETCH_URL}\n", ""),
        (0, "", ""),
        (1, "unexpected\n", ""),
        (1, "", "warning"),
        (2, "", ""),
    ],
)
def test_binding_rejects_pushurl_or_unexpected_pushurl_lookup_result(
    trusted_repo: Path,
    returncode: int,
    stdout: str,
    stderr: str,
) -> None:
    runner = FakeRunner(trusted_repo)
    runner.pushurl_returncode = returncode
    runner.pushurl_stdout = stdout
    runner.pushurl_stderr = stderr

    with pytest.raises(candidate.CandidateBindingError, match="pushurl"):
        candidate.bind_fresh_origin_dev(
            make_config(trusted_repo),
            run=runner,
            now=fixed_now,
        )

    assert runner.argvs == expected_argvs(trusted_repo)[:3]


def test_binding_accepts_exit_one_with_empty_pushurl_output(trusted_repo: Path) -> None:
    runner = FakeRunner(trusted_repo)

    candidate.bind_fresh_origin_dev(make_config(trusted_repo), run=runner, now=fixed_now)

    assert runner.argvs == expected_argvs(trusted_repo)


def test_binding_rejects_success_with_git_diagnostics(trusted_repo: Path) -> None:
    runner = FakeRunner(trusted_repo)
    runner.url_stderr = "warning: unexpected config diagnostic\n"

    with pytest.raises(candidate.CandidateBindingError, match="fetch URL inspection failed"):
        candidate.bind_fresh_origin_dev(
            make_config(trusted_repo),
            run=runner,
            now=fixed_now,
        )

    assert runner.argvs == expected_argvs(trusted_repo)[:2]


def test_binding_rejects_non_integer_or_oversized_git_evidence(trusted_repo: Path) -> None:
    invalid_code = FakeRunner(trusted_repo)
    invalid_code.url_returncode = False  # type: ignore[assignment]
    with pytest.raises(candidate.CandidateBindingError, match="invalid evidence"):
        candidate.bind_fresh_origin_dev(
            make_config(trusted_repo),
            run=invalid_code,
            now=fixed_now,
        )

    oversized = FakeRunner(trusted_repo)
    oversized.remote_stdout = "x" * ((1 << 20) + 1)
    with pytest.raises(candidate.CandidateBindingError, match="invalid evidence"):
        candidate.bind_fresh_origin_dev(
            make_config(trusted_repo),
            run=oversized,
            now=fixed_now,
        )


def test_binding_does_not_rescan_runtime_or_worktree(
    trusted_repo: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def reject_walk(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("candidate bind must remain metadata-scale")

    monkeypatch.setattr(candidate.os, "walk", reject_walk)
    runner = FakeRunner(trusted_repo)
    runner.status_stdout = "?? attacker-controlled.py\n"

    candidate.bind_fresh_origin_dev(
        make_config(trusted_repo),
        run=runner,
        now=fixed_now,
    )

    assert runner.argvs == expected_argvs(trusted_repo)
    assert not any("status" in argv for argv in runner.argvs)


@pytest.mark.parametrize(
    "sha_output",
    [
        "",
        "a" * 39 + "\n",
        "a" * 41 + "\n",
        "A" * 40 + "\n",
        " " + "a" * 40 + "\n",
        f"{FRESH_SHA}\n{CACHED_SHA}\n",
    ],
)
def test_binding_rejects_malformed_or_multiple_sha_output(
    trusted_repo: Path,
    sha_output: str,
) -> None:
    runner = FakeRunner(trusted_repo)
    runner.rev_parse_stdout = sha_output

    with pytest.raises(candidate.CandidateBindingError, match="40 lowercase"):
        candidate.bind_fresh_origin_dev(
            make_config(trusted_repo),
            run=runner,
            now=fixed_now,
        )

    assert runner.argvs == expected_argvs(trusted_repo)[:-1]


def test_binding_rejects_attached_head_or_malformed_tree(trusted_repo: Path) -> None:
    attached = FakeRunner(trusted_repo)
    attached.symbolic_returncode = 0
    attached.symbolic_stdout = "refs/heads/dev\n"
    with pytest.raises(candidate.CandidateBindingError, match="detached HEAD"):
        candidate.bind_fresh_origin_dev(
            make_config(trusted_repo),
            run=attached,
            now=fixed_now,
        )

    malformed_tree = FakeRunner(trusted_repo)
    malformed_tree.tree_stdout = f"{'2' * 40}\n{'3' * 40}\n"
    with pytest.raises(candidate.CandidateBindingError, match=r"resolved tree.*40 lowercase"):
        candidate.bind_fresh_origin_dev(
            make_config(trusted_repo),
            run=malformed_tree,
            now=fixed_now,
        )


def test_binding_rejects_symlinked_checkout_before_git(
    trusted_repo: Path,
    tmp_path: Path,
) -> None:
    external_checkout = tmp_path / "external-checkout"
    trusted_repo.rename(external_checkout)
    trusted_repo.symlink_to(external_checkout, target_is_directory=True)
    runner = FakeRunner(trusted_repo)

    with pytest.raises(candidate.CandidateBindingError, match="symlink"):
        candidate.bind_fresh_origin_dev(
            make_config(trusted_repo),
            run=runner,
            now=fixed_now,
        )

    assert runner.argvs == []


def test_binding_rejects_symlinked_git_directory_before_git(
    trusted_repo: Path,
    tmp_path: Path,
) -> None:
    git_dir = trusted_repo / ".git"
    (git_dir / "config").unlink()
    git_dir.rmdir()
    external_git = tmp_path / "external-git"
    external_git.mkdir(mode=0o700)
    git_dir.symlink_to(external_git, target_is_directory=True)
    runner = FakeRunner(trusted_repo)

    with pytest.raises(candidate.CandidateBindingError, match="symlink"):
        candidate.bind_fresh_origin_dev(
            make_config(trusted_repo),
            run=runner,
            now=fixed_now,
        )

    assert runner.argvs == []


@pytest.mark.parametrize(
    ("target_name", "mode"), [("repo", 0o775), ("repo", 0o757), (".git", 0o770), (".git", 0o707)]
)
def test_binding_rejects_group_or_world_writable_paths_before_git(
    trusted_repo: Path,
    target_name: str,
    mode: int,
) -> None:
    target = trusted_repo if target_name == "repo" else trusted_repo / ".git"
    target.chmod(mode)
    runner = FakeRunner(trusted_repo)

    with pytest.raises(candidate.CandidateBindingError, match="group/world writable"):
        candidate.bind_fresh_origin_dev(
            make_config(trusted_repo),
            run=runner,
            now=fixed_now,
        )

    assert runner.argvs == []


def test_binding_rejects_broken_or_unsafe_runtime_python_before_git(
    trusted_repo: Path,
    tmp_path: Path,
) -> None:
    python = trusted_repo.parent / "venv" / "bin" / "python"
    python.unlink()
    python.symlink_to(trusted_repo.parent / "venv" / "missing-python")
    with pytest.raises(candidate.CandidateBindingError, match="Python runtime is unavailable"):
        candidate.bind_fresh_origin_dev(
            make_config(trusted_repo),
            run=FakeRunner(trusted_repo),
            now=fixed_now,
        )

    python.unlink()
    external = tmp_path / "mutable-python"
    external.write_text("#!/bin/sh\n", encoding="utf-8")
    external.chmod(0o755)
    python.symlink_to(external)
    with pytest.raises(candidate.CandidateBindingError, match="Python runtime is unsafe"):
        candidate.bind_fresh_origin_dev(
            make_config(trusted_repo),
            run=FakeRunner(trusted_repo),
            now=fixed_now,
        )


def test_binding_rejects_cross_candidate_config_or_unversioned_runtime(
    trusted_repo: Path,
    tmp_path: Path,
) -> None:
    drifted_cluster = replace(
        make_config(trusted_repo),
        cluster_config_path=tmp_path / "staging.cluster.toml",
    )
    with pytest.raises(candidate.CandidateBindingError, match="cluster config"):
        candidate.bind_fresh_origin_dev(
            drifted_cluster,
            run=FakeRunner(trusted_repo),
            now=fixed_now,
        )

    unversioned = make_config(tmp_path / "repo")
    with pytest.raises(candidate.CandidateBindingError, match="runtime path is not exact"):
        candidate.bind_fresh_origin_dev(
            unversioned,
            run=FakeRunner(tmp_path / "repo"),
            now=fixed_now,
        )


def test_binding_rejects_wrong_path_owner_before_git(
    trusted_repo: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_lstat = os.lstat

    def wrong_owner_lstat(path: os.PathLike[str] | str) -> object:
        metadata = real_lstat(path)
        if Path(path) == trusted_repo:
            return SimpleNamespace(
                st_mode=metadata.st_mode,
                st_uid=os.getuid() + 1000,
                st_gid=0,
            )
        return metadata

    monkeypatch.setattr(candidate.os, "lstat", wrong_owner_lstat)
    runner = FakeRunner(trusted_repo)

    with pytest.raises(candidate.CandidateBindingError, match="root-owned"):
        candidate.bind_fresh_origin_dev(
            make_config(trusted_repo),
            run=runner,
            now=fixed_now,
        )

    assert runner.argvs == []


def test_binding_rejects_non_directory_git_path_before_git(trusted_repo: Path) -> None:
    git_dir = trusted_repo / ".git"
    (git_dir / "config").unlink()
    git_dir.rmdir()
    git_dir.write_text("gitdir: attacker-controlled\n", encoding="utf-8")
    git_dir.chmod(0o600)
    runner = FakeRunner(trusted_repo)

    with pytest.raises(candidate.CandidateBindingError, match="directory"):
        candidate.bind_fresh_origin_dev(
            make_config(trusted_repo),
            run=runner,
            now=fixed_now,
        )

    assert runner.argvs == []


def test_binding_rejects_symlinked_git_config_before_git(
    trusted_repo: Path,
    tmp_path: Path,
) -> None:
    git_config = trusted_repo / ".git" / "config"
    git_config.unlink()
    target = tmp_path / "attacker-git-config"
    target.write_text("[core]\n\tfsmonitor = /tmp/hook\n", encoding="utf-8")
    git_config.symlink_to(target)
    runner = FakeRunner(trusted_repo)

    with pytest.raises(candidate.CandidateBindingError, match=r"git config.*symlink"):
        candidate.bind_fresh_origin_dev(
            make_config(trusted_repo),
            run=runner,
            now=fixed_now,
        )

    assert runner.argvs == []


def test_binding_rejects_non_regular_git_config_before_git(trusted_repo: Path) -> None:
    git_config = trusted_repo / ".git" / "config"
    git_config.unlink()
    git_config.mkdir(mode=0o700)
    runner = FakeRunner(trusted_repo)

    with pytest.raises(candidate.CandidateBindingError, match=r"git config.*regular file"):
        candidate.bind_fresh_origin_dev(
            make_config(trusted_repo),
            run=runner,
            now=fixed_now,
        )

    assert runner.argvs == []


def test_binding_rejects_wrong_git_config_owner_before_git(
    trusted_repo: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    git_config = trusted_repo / ".git" / "config"
    trusted_lstat = candidate.os.lstat

    def wrong_git_config_owner_lstat(path: os.PathLike[str] | str) -> object:
        metadata = trusted_lstat(path)
        if Path(path) == git_config:
            return SimpleNamespace(
                st_mode=metadata.st_mode,
                st_uid=os.getuid() + 1000,
                st_gid=0,
            )
        return metadata

    monkeypatch.setattr(candidate.os, "lstat", wrong_git_config_owner_lstat)
    runner = FakeRunner(trusted_repo)

    with pytest.raises(candidate.CandidateBindingError, match=r"git config.*root-owned"):
        candidate.bind_fresh_origin_dev(
            make_config(trusted_repo),
            run=runner,
            now=fixed_now,
        )

    assert runner.argvs == []


@pytest.mark.parametrize("mode", [0o620, 0o602])
def test_binding_rejects_group_or_world_writable_git_config_before_git(
    trusted_repo: Path,
    mode: int,
) -> None:
    git_config = trusted_repo / ".git" / "config"
    git_config.chmod(mode)
    runner = FakeRunner(trusted_repo)

    with pytest.raises(
        candidate.CandidateBindingError,
        match=r"git config.*group/world writable",
    ):
        candidate.bind_fresh_origin_dev(
            make_config(trusted_repo),
            run=runner,
            now=fixed_now,
        )

    assert runner.argvs == []


def test_binding_rejects_git_config_unreadable_by_service_before_git(
    trusted_repo: Path,
) -> None:
    (trusted_repo / ".git" / "config").chmod(0o600)

    with pytest.raises(candidate.CandidateBindingError, match=r"git config.*not readable"):
        candidate.bind_fresh_origin_dev(
            make_config(trusted_repo),
            run=FakeRunner(trusted_repo),
            now=fixed_now,
        )


def test_binding_rejects_symlinked_config_before_git(
    trusted_repo: Path,
    tmp_path: Path,
) -> None:
    config_path = make_config(trusted_repo).config_path
    config_path.unlink()
    target = tmp_path / "attacker-config.toml"
    target.write_text("schema_version = 1\n", encoding="utf-8")
    config_path.symlink_to(target)
    runner = FakeRunner(trusted_repo)

    with pytest.raises(candidate.CandidateBindingError, match=r"config.*symlink"):
        candidate.bind_fresh_origin_dev(
            make_config(trusted_repo),
            run=runner,
            now=fixed_now,
        )

    assert runner.argvs == []


def test_binding_rejects_non_regular_config_before_git(trusted_repo: Path) -> None:
    config_path = make_config(trusted_repo).config_path
    config_path.unlink()
    config_path.mkdir(mode=0o700)
    runner = FakeRunner(trusted_repo)

    with pytest.raises(candidate.CandidateBindingError, match=r"config.*regular file"):
        candidate.bind_fresh_origin_dev(
            make_config(trusted_repo),
            run=runner,
            now=fixed_now,
        )

    assert runner.argvs == []


def test_binding_rejects_writable_config_before_git(trusted_repo: Path) -> None:
    config_path = make_config(trusted_repo).config_path
    config_path.chmod(0o620)
    runner = FakeRunner(trusted_repo)

    with pytest.raises(candidate.CandidateBindingError, match=r"config.*group/world writable"):
        candidate.bind_fresh_origin_dev(
            make_config(trusted_repo),
            run=runner,
            now=fixed_now,
        )

    assert runner.argvs == []


def test_binding_rejects_non_root_config_owner_before_git(
    trusted_repo: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path = make_config(trusted_repo).config_path
    trusted_lstat = candidate.os.lstat

    def wrong_config_owner_lstat(path: os.PathLike[str] | str) -> object:
        metadata = trusted_lstat(path)
        if Path(path) == config_path:
            return SimpleNamespace(st_mode=metadata.st_mode, st_uid=1000)
        return metadata

    monkeypatch.setattr(candidate.os, "lstat", wrong_config_owner_lstat)
    runner = FakeRunner(trusted_repo)

    with pytest.raises(candidate.CandidateBindingError, match="config owner UID"):
        candidate.bind_fresh_origin_dev(
            make_config(trusted_repo),
            run=runner,
            now=fixed_now,
        )

    assert runner.argvs == []


def test_binding_rejects_protected_config_fingerprint_drift_before_git(
    trusted_repo: Path,
) -> None:
    config = make_config(trusted_repo)
    config.config_path.write_text("schema_version = 2\n", encoding="utf-8")
    config.config_path.chmod(0o600)
    runner = FakeRunner(trusted_repo)

    with pytest.raises(candidate.CandidateBindingError, match="config fingerprint drifted"):
        candidate.bind_fresh_origin_dev(config, run=runner, now=fixed_now)

    assert runner.argvs == []


def test_binding_rejects_non_approved_config_without_git(trusted_repo: Path) -> None:
    runner = FakeRunner(trusted_repo)

    with pytest.raises(candidate.CandidateBindingError, match="approved remote URL"):
        candidate.bind_fresh_origin_dev(
            replace(make_config(trusted_repo), remote_url="https://example.invalid/loom.git"),
            run=runner,
            now=fixed_now,
        )
    with pytest.raises(candidate.CandidateBindingError, match="approved target ref"):
        candidate.bind_fresh_origin_dev(
            replace(make_config(trusted_repo), target_ref="refs/heads/feature"),
            run=runner,
            now=fixed_now,
        )

    assert runner.argvs == []


def test_candidate_binding_api_has_no_ref_tag_remote_or_candidate_overrides() -> None:
    signature = inspect.signature(candidate.bind_fresh_origin_dev)

    assert tuple(signature.parameters) == ("config", "run", "now")
    assert {"ref", "tag", "remote", "candidate", "sha", "image_tag"}.isdisjoint(
        signature.parameters
    )
    assert signature.parameters["run"].kind is inspect.Parameter.KEYWORD_ONLY
    assert signature.parameters["now"].kind is inspect.Parameter.KEYWORD_ONLY
