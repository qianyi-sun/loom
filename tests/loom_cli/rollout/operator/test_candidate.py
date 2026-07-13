from __future__ import annotations

import inspect
import os
import subprocess
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from loom_cli.rollout.operator import candidate
from loom_cli.rollout.operator.config import OperatorConfig

FRESH_SHA = "abcdef1234567890abcdef1234567890abcdef12"
CACHED_SHA = "1111111111111111111111111111111111111111"
FETCH_URL = "https://github.com/qianyi-sun/loom.git"
FETCHED_AT = datetime(2026, 7, 13, 20, 0, tzinfo=UTC)


def make_config(runner_repo: Path) -> OperatorConfig:
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
        cluster_config_path=runner_repo / "deploy/environments/staging.cluster.toml",
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
        config_path=runner_repo.parent / "staging-rollout.toml",
        config_sha256="1" * 64,
    )


def fixed_now() -> datetime:
    return FETCHED_AT


@pytest.fixture
def trusted_repo(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    repo = tmp_path / "repo"
    repo.mkdir(mode=0o755)
    (repo / ".git").mkdir(mode=0o700)
    config_path = tmp_path / "staging-rollout.toml"
    config_path.write_text("schema_version = 1\n", encoding="utf-8")
    repo.chmod(0o755)
    (repo / ".git").chmod(0o700)
    config_path.chmod(0o600)
    monkeypatch.setattr(
        candidate.pwd,
        "getpwnam",
        lambda username: SimpleNamespace(pw_name=username, pw_uid=os.getuid()),
    )
    real_lstat = os.lstat

    def root_owned_config_lstat(path: os.PathLike[str] | str) -> object:
        metadata = real_lstat(path)
        if Path(path) == config_path:
            return SimpleNamespace(st_mode=metadata.st_mode, st_uid=0)
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
        self.pushurl_returncode = 1
        self.pushurl_stdout = ""
        self.pushurl_stderr = ""
        self.status_returncode = 0
        self.status_stdout = ""
        self.fetch_returncode = 0
        self.rev_parse_returncode = 0
        self.rev_parse_stdout: str | None = None
        self.fetched = False

    def __call__(self, argv: list[str]) -> subprocess.CompletedProcess[str]:
        self.argvs.append(list(argv))
        operation = argv[3:]
        if operation == ["remote"]:
            return self._result(argv, self.remote_returncode, self.remote_stdout)
        if operation == ["remote", "get-url", "--all", "origin"]:
            return self._result(argv, self.url_returncode, self.url_stdout)
        if operation == ["config", "--get-all", "remote.origin.pushurl"]:
            return self._result(
                argv,
                self.pushurl_returncode,
                self.pushurl_stdout,
                self.pushurl_stderr,
            )
        if operation == ["status", "--porcelain=v1", "--untracked-files=all"]:
            return self._result(argv, self.status_returncode, self.status_stdout)
        if operation == [
            "fetch",
            "--force",
            "--no-tags",
            "--prune",
            "--no-recurse-submodules",
            "origin",
            "+refs/heads/dev:refs/remotes/origin/dev",
        ]:
            if self.fetch_returncode == 0:
                self.fetched = True
            return self._result(argv, self.fetch_returncode, "")
        if operation == ["rev-parse", "--verify", "refs/remotes/origin/dev^{commit}"]:
            stdout = self.rev_parse_stdout
            if stdout is None:
                stdout = f"{FRESH_SHA if self.fetched else CACHED_SHA}\n"
            return self._result(argv, self.rev_parse_returncode, stdout)
        raise AssertionError(f"unexpected command: {argv!r}")

    @staticmethod
    def _result(
        argv: list[str],
        returncode: int,
        stdout: str,
        stderr: str = "",
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(argv, returncode, stdout, stderr)


def expected_argvs(repo: Path) -> list[list[str]]:
    prefix = ["git", "-C", str(repo)]
    return [
        [*prefix, "remote"],
        [*prefix, "remote", "get-url", "--all", "origin"],
        [*prefix, "config", "--get-all", "remote.origin.pushurl"],
        [*prefix, "status", "--porcelain=v1", "--untracked-files=all"],
        [
            *prefix,
            "fetch",
            "--force",
            "--no-tags",
            "--prune",
            "--no-recurse-submodules",
            "origin",
            "+refs/heads/dev:refs/remotes/origin/dev",
        ],
        [*prefix, "rev-parse", "--verify", "refs/remotes/origin/dev^{commit}"],
    ]


def test_binding_fetches_exact_dev_before_resolving_cached_ref(trusted_repo: Path) -> None:
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
    assert binding.resolved_sha != CACHED_SHA
    assert binding.image_tag == "staging-abcdef1"
    assert binding.fetched_at == "2026-07-13T20:00:00Z"


def test_fetch_failure_prevents_resolve(trusted_repo: Path) -> None:
    runner = FakeRunner(trusted_repo)
    runner.fetch_returncode = 128

    with pytest.raises(candidate.CandidateBindingError, match="fetch"):
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


def test_binding_rejects_dirty_or_untracked_checkout(trusted_repo: Path) -> None:
    runner = FakeRunner(trusted_repo)
    runner.status_stdout = "?? attacker-controlled.py\n"

    with pytest.raises(candidate.CandidateBindingError, match="clean"):
        candidate.bind_fresh_origin_dev(
            make_config(trusted_repo),
            run=runner,
            now=fixed_now,
        )

    assert runner.argvs == expected_argvs(trusted_repo)[:4]


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

    assert runner.argvs == expected_argvs(trusted_repo)


def test_binding_rejects_symlinked_checkout_before_git(
    trusted_repo: Path,
    tmp_path: Path,
) -> None:
    checkout_link = tmp_path / "checkout-link"
    checkout_link.symlink_to(trusted_repo, target_is_directory=True)
    runner = FakeRunner(checkout_link)

    with pytest.raises(candidate.CandidateBindingError, match="symlink"):
        candidate.bind_fresh_origin_dev(
            make_config(checkout_link),
            run=runner,
            now=fixed_now,
        )

    assert runner.argvs == []


def test_binding_rejects_symlinked_git_directory_before_git(
    trusted_repo: Path,
    tmp_path: Path,
) -> None:
    git_dir = trusted_repo / ".git"
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


def test_binding_rejects_wrong_path_owner_before_git(
    trusted_repo: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_lstat = os.lstat

    def wrong_owner_lstat(path: os.PathLike[str] | str) -> object:
        metadata = real_lstat(path)
        if Path(path) == trusted_repo:
            return SimpleNamespace(st_mode=metadata.st_mode, st_uid=os.getuid() + 1000)
        return metadata

    monkeypatch.setattr(candidate.os, "lstat", wrong_owner_lstat)
    runner = FakeRunner(trusted_repo)

    with pytest.raises(candidate.CandidateBindingError, match="owner UID"):
        candidate.bind_fresh_origin_dev(
            make_config(trusted_repo),
            run=runner,
            now=fixed_now,
        )

    assert runner.argvs == []


def test_binding_rejects_non_directory_git_path_before_git(trusted_repo: Path) -> None:
    git_dir = trusted_repo / ".git"
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
