from __future__ import annotations

import hashlib
import os
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from loom_cli.rollout.operator import preflight as preflight_module
from loom_cli.rollout.operator.config import OperatorConfig
from loom_cli.rollout.operator.policy import sanitized_child_environment
from loom_cli.rollout.operator.preflight import (
    PreflightCheck,
    PreflightReport,
    collect_preflight,
)

REPO_ROOT = Path(__file__).resolve().parents[4]
REAL_SHARED_REPOSITORY_BINDING = preflight_module._shared_repository_binding


def _test_shared_repository_binding(*, service_uid: int) -> dict[str, int]:
    return {
        "service_uid": service_uid,
        "service_primary_gid": 2006,
        "consumer_uid": 2005,
        "consumer_primary_gid": 2005,
        "shared_gid": 2007,
        "parent_device": 1,
        "parent_inode": 11,
        "authority_device": 1,
        "authority_inode": 12,
        "repository_device": 1,
        "repository_inode": 13,
    }


@pytest.fixture(autouse=True)
def _fixed_shared_repository_binding(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        preflight_module,
        "_shared_repository_binding",
        _test_shared_repository_binding,
    )


def test_preflight_report_contains_only_named_checks_and_safe_remediation() -> None:
    report = PreflightReport(
        checks=(
            PreflightCheck(name="docker-buildx", passed=True, remediation=None),
            PreflightCheck(
                name="gb10-batch-mode",
                passed=False,
                remediation="restore service SSH trust for every configured GB10 host",
            ),
        )
    )

    assert report.passed is False
    assert report.to_dict() == {
        "checks": [
            {"name": "docker-buildx", "passed": True, "remediation": None},
            {
                "name": "gb10-batch-mode",
                "passed": False,
                "remediation": "restore service SSH trust for every configured GB10 host",
            },
        ],
        "passed": False,
    }


def test_preflight_rejects_secret_shaped_remediation() -> None:
    try:
        PreflightCheck(
            name="credential",
            passed=False,
            remediation="token=super-secret-value",
        )
    except ValueError as exc:
        assert "safe" in str(exc)
    else:  # pragma: no cover - fail-closed assertion
        raise AssertionError("secret-shaped remediation was accepted")


def test_fingerprint_is_bounded_and_does_not_echo_value() -> None:
    from loom_cli.rollout.operator.preflight import safe_fingerprint

    secret = b"test-secret-value"
    rendered = safe_fingerprint(secret)

    assert rendered.startswith("sha256:")
    assert rendered.endswith(f" len={len(secret)}")
    assert len(rendered.split(":", 1)[1].split(" ", 1)[0]) == 12
    assert secret.decode() not in rendered


def make_config(tmp_path: Path) -> OperatorConfig:
    runner = tmp_path / "runner/repo"
    (runner / ".git").mkdir(parents=True)
    (runner / ".git/config").write_text('[remote "origin"]\n', encoding="utf-8")
    (runner / ".git/config").chmod(0o644)
    (runner / "deploy/environments").mkdir(parents=True)
    (runner / "deploy/environment-state").mkdir(parents=True)
    (runner / "deploy/worker-pools/gb10").mkdir(parents=True)
    for directory in (runner, runner / ".git"):
        directory.chmod(0o755)
    identity = tmp_path / "identity"
    identity.write_text("private-key-placeholder", encoding="utf-8")
    identity.chmod(0o600)
    ssh_config = runner / "deploy/worker-pools/gb10/ssh_config"
    ssh_config.write_bytes((REPO_ROOT / "deploy/worker-pools/gb10/ssh_config").read_bytes())
    ssh_config.chmod(0o644)
    cluster = runner / "deploy/environments/staging.cluster.toml"
    hosts = ",\n".join(
        f'  {{ ssh_target = "trt-gb10-{number}" }}' for number in range(1, 16) if number != 7
    )
    cluster.write_text(
        "\n".join(
            [
                'env_state_profile = "../environment-state/staging.toml"',
                "[gb10_pool]",
                'ssh_config = "../worker-pools/gb10/ssh_config"',
                f'ssh_identity_file = "{identity}"',
                "hosts = [",
                hosts,
                "]",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    cluster.chmod(0o644)
    catalog = tmp_path / "catalog.env"
    catalog.write_text("PUBLISHED_SHA=abc\n", encoding="utf-8")
    catalog.chmod(0o600)
    profile = runner / "deploy/environment-state/staging.toml"
    profile.write_text(
        f'[catalog_provisioning]\nenv_file = "{catalog}"\n',
        encoding="utf-8",
    )
    profile.chmod(0o644)
    credentials = tmp_path / "credentials"
    credentials.mkdir(mode=0o700)
    token = b"a" * 64
    for name in ("admin", "worker", "service"):
        path = credentials / name
        path.write_bytes(token)
        path.chmod(0o600)
    data_root = tmp_path / "data"
    data_root.mkdir(mode=0o700)
    for name in preflight_module._REQUIRED_ROLLOUT_SUBDIRECTORIES:  # type: ignore[attr-defined]
        (data_root / name).mkdir(mode=0o700)
    return OperatorConfig(
        schema_version=1,
        service_user="loom-rollout",
        operator_group="loom-staging-operators",
        remote_url="https://github.com/qianyi-sun/loom.git",
        target_ref="refs/heads/dev",
        runner_repo=runner,
        state_root=tmp_path / "state",
        runtime_root=tmp_path / "runtime",
        rollout_root=data_root,
        kubeconfig_path=tmp_path / "kubeconfig",
        cluster_config_path=cluster,
        admin_token_source=f"file:{credentials / 'admin'}",
        worker_token_source=f"file:{credentials / 'worker'}",
        service_token_source=f"file:{credentials / 'service'}",
        expect_admin_token_fingerprint=(
            f"sha256:{hashlib.sha256(token).hexdigest()[:12]} len={len(token)}"
        ),
        cluster_name="loom-staging",
        namespace="loom-staging",
        environment="staging",
        cp_url="http://127.0.0.1:18081",
        smoke_on_behalf_username="devansh",
        smoke_on_behalf_team_id="11111111-1111-4111-8111-111111111111",
        scope="current-gb10",
        gb10_prep_concurrency=8,
        config_path=tmp_path / "staging-rollout.toml",
        config_sha256="1" * 64,
    )


def successful_command(argv: list[str]) -> subprocess.CompletedProcess[str]:
    stdout = ""
    if argv[-1:] == ["remote"]:
        stdout = "origin\n"
    elif "get-url" in argv:
        stdout = "https://github.com/qianyi-sun/loom.git\n"
    elif argv[-2:] == ["--get-all", "remote.origin.pushurl"]:
        return subprocess.CompletedProcess(argv, 1, "", "")
    elif argv[-2:] == ["config", "current-context"]:
        stdout = "kind-loom-staging\n"
    elif argv[:1] == ["ssh"] and argv[-1] != "true":
        mount_type = "ext4" if argv[-2] == "trt-gb10-2" else "nfs4"
        mount_source = (
            "/dev/mapper/shared-work2"
            if argv[-2] == "trt-gb10-2"
            else "192.168.20.12:/shared_work2"
        )
        stdout = (
            "2005;2005,2007;2005;2007;2775;101;201;"
            f"{os.geteuid()};2007;2750;102;202;"
            f"{os.geteuid()};2007;2750;103;203;"
            f"{mount_type};{mount_source};103;203\n"
        )
    return subprocess.CompletedProcess(argv, 0, stdout, "")


def replace_gb10_hosts(config: OperatorConfig, hosts: tuple[str, ...]) -> None:
    cluster = config.cluster_config_path
    prefix, marker, remainder = cluster.read_text(encoding="utf-8").partition("hosts = [\n")
    assert marker
    _old_hosts, closing, suffix = remainder.partition("]\n")
    assert closing
    rendered = ",\n".join(f'  {{ ssh_target = "{host}" }}' for host in hosts)
    cluster.write_text(
        prefix + marker + rendered + "\n" + closing + suffix,
        encoding="utf-8",
    )


def test_collect_preflight_accepts_trusted_readable_repo_configs(tmp_path: Path) -> None:
    config = make_config(tmp_path)

    report = collect_preflight(
        config,
        service_uid=os.geteuid(),
        run=successful_command,
        which=lambda name: f"/usr/bin/{name}",
        importer=lambda name: object(),
    )

    assert report.passed, report.to_dict()


def test_collect_preflight_probes_only_exact_merged_active_gb10_set(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    calls: list[list[str]] = []

    def run(argv: list[str]) -> subprocess.CompletedProcess[str]:
        calls.append(argv)
        return successful_command(argv)

    report = collect_preflight(
        config,
        service_uid=os.geteuid(),
        run=run,
        which=lambda name: f"/usr/bin/{name}",
        importer=lambda name: object(),
    )

    assert report.passed, report.to_dict()
    probed_hosts = tuple(argv[-2] for argv in calls if argv[:1] == ["ssh"] and argv[-1] == "true")
    assert probed_hosts == preflight_module.ACTIVE_GB10_HOSTS
    assert len(probed_hosts) == 14
    assert "trt-gb10-7" not in probed_hosts
    assert len(preflight_module.FULL_GB10_HOSTS) == 15
    assert set(preflight_module.FULL_GB10_HOSTS) - set(probed_hosts) == {"trt-gb10-7"}
    topology = next(check for check in report.checks if check.name == "gb10-topology")
    assert topology.passed is True
    shared_probes = tuple(argv[-2] for argv in calls if argv[:1] == ["ssh"] and argv[-1] != "true")
    assert shared_probes == preflight_module.ACTIVE_GB10_HOSTS
    shared = next(check for check in report.checks if check.name == "gb10-shared-repository")
    assert shared.passed is True
    assert shared.evidence is not None
    assert shared.evidence.endswith("hosts=14")


def test_shared_repository_binding_uses_nss_and_held_directories_not_install_record(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    parent = tmp_path / "qianyi"
    authority = parent / ".loom-staging-rollout"
    repository = authority / "worker-repos"
    repository.mkdir(parents=True)
    parent.chmod(0o2775)
    authority.chmod(0o2750)
    repository.chmod(0o2750)
    shared_gid = os.getegid()
    service_gid = shared_gid + 10000
    consumer_gid = shared_gid + 10001
    uid = os.geteuid()

    def getpwnam(name: str) -> SimpleNamespace:
        if name == "loom-rollout":
            return SimpleNamespace(pw_name=name, pw_uid=uid, pw_gid=service_gid)
        if name == "qianyi":
            return SimpleNamespace(pw_name=name, pw_uid=uid, pw_gid=consumer_gid)
        raise KeyError(name)

    def getgrnam(name: str) -> SimpleNamespace:
        if name == "loom-rollout":
            return SimpleNamespace(gr_name=name, gr_gid=service_gid)
        if name == "sharedwork":
            return SimpleNamespace(gr_name=name, gr_gid=shared_gid)
        raise KeyError(name)

    monkeypatch.setattr(preflight_module.pwd, "getpwnam", getpwnam)
    monkeypatch.setattr(preflight_module.grp, "getgrnam", getgrnam)
    monkeypatch.setattr(
        preflight_module.os,
        "getgrouplist",
        lambda name, primary: [primary, shared_gid] if name == "qianyi" else [primary],
    )
    capability_results = iter((True, False))
    monkeypatch.setattr(
        preflight_module.os,
        "access",
        lambda *args, **kwargs: next(capability_results),
    )
    monkeypatch.setattr(
        preflight_module,
        "_trusted_file_bytes",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("install record must not be read")
        ),
    )
    mountinfo = tmp_path / "mountinfo"
    metadata = tmp_path.stat()
    mountinfo.write_text(
        f"42 1 {os.major(metadata.st_dev)}:{os.minor(metadata.st_dev)} / {tmp_path} "
        "rw,nosuid,nodev,noexec - nfs4 192.168.20.12:/shared_work2 "
        "rw,hard,vers=4.2,proto=tcp,sec=sys,timeo=600,retrans=2\n",
        encoding="utf-8",
    )

    binding = REAL_SHARED_REPOSITORY_BINDING(
        service_uid=uid,
        root=repository,
        mountinfo=mountinfo,
    )

    assert binding is not None
    assert binding["service_uid"] == uid
    assert binding["service_primary_gid"] == service_gid
    assert binding["consumer_uid"] == uid
    assert binding["consumer_primary_gid"] == consumer_gid
    assert binding["shared_gid"] == shared_gid
    assert binding["repository_inode"] == repository.stat().st_ino


@pytest.mark.parametrize(
    "remote_output",
    [
        ("2004;2005,2007;2005;2007;2775;101;201;2006;2007;2750;102;202;2006;2007;2750;103;203\n"),
        ("2005;2005;2005;2007;2775;101;201;2006;2007;2750;102;202;2006;2007;2750;103;203\n"),
        ("2005;2005,2007;2005;2007;2775;101;201;2006;2007;2770;102;202;2006;2007;2750;103;203\n"),
    ],
)
def test_collect_preflight_rejects_shared_repository_identity_membership_or_mode_drift(
    tmp_path: Path,
    remote_output: str,
) -> None:
    config = make_config(tmp_path)

    def run(argv: list[str]) -> subprocess.CompletedProcess[str]:
        if argv[:1] == ["ssh"] and argv[-1] != "true":
            return subprocess.CompletedProcess(argv, 0, remote_output, "")
        return successful_command(argv)

    report = collect_preflight(
        config,
        service_uid=os.geteuid(),
        run=run,
        which=lambda name: f"/usr/bin/{name}",
        importer=lambda name: object(),
    )

    shared = next(check for check in report.checks if check.name == "gb10-shared-repository")
    assert shared.passed is False
    assert shared.evidence is None


@pytest.mark.parametrize(
    ("target", "mount_type", "mount_source", "mount_major", "mount_minor"),
    [
        ("trt-gb10-8", "nfs4", "192.168.20.99:/shared_work2", 103, 203),
        ("trt-gb10-8", "ext4", "/dev/mapper/local", 103, 203),
        ("trt-gb10-8", "nfs4", "192.168.20.12:/shared_work2", 104, 204),
        ("trt-gb10-2", "nfs4", "192.168.20.12:/shared_work2", 103, 203),
    ],
)
def test_collect_preflight_rejects_shared_repository_mount_drift(
    tmp_path: Path,
    target: str,
    mount_type: str,
    mount_source: str,
    mount_major: int,
    mount_minor: int,
) -> None:
    config = make_config(tmp_path)

    def run(argv: list[str]) -> subprocess.CompletedProcess[str]:
        if argv[:1] == ["ssh"] and argv[-2] == target and argv[-1] != "true":
            remote_output = (
                "2005;2005,2007;2005;2007;2775;101;201;"
                f"{os.geteuid()};2007;2750;102;202;"
                f"{os.geteuid()};2007;2750;103;203;"
                f"{mount_type};{mount_source};{mount_major};{mount_minor}\n"
            )
            return subprocess.CompletedProcess(argv, 0, remote_output, "")
        return successful_command(argv)

    report = collect_preflight(
        config,
        service_uid=os.geteuid(),
        run=run,
        which=lambda name: f"/usr/bin/{name}",
        importer=lambda name: object(),
    )

    shared = next(check for check in report.checks if check.name == "gb10-shared-repository")
    assert shared.passed is False
    assert shared.evidence is None


def test_collect_preflight_rejects_one_shared_repository_host_failure(
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path)

    def run(argv: list[str]) -> subprocess.CompletedProcess[str]:
        if argv[:1] == ["ssh"] and argv[-2] == "trt-gb10-8" and argv[-1] != "true":
            return subprocess.CompletedProcess(argv, 1, "", "failed")
        return successful_command(argv)

    report = collect_preflight(
        config,
        service_uid=os.geteuid(),
        run=run,
        which=lambda name: f"/usr/bin/{name}",
        importer=lambda name: object(),
    )

    assert (
        next(check for check in report.checks if check.name == "gb10-shared-repository").passed
        is False
    )


@pytest.mark.parametrize(
    "configured_hosts",
    [
        preflight_module.FULL_GB10_HOSTS,
        preflight_module.ACTIVE_GB10_HOSTS[:-1],
        tuple(reversed(preflight_module.ACTIVE_GB10_HOSTS)),
        (*preflight_module.ACTIVE_GB10_HOSTS[:-1], preflight_module.ACTIVE_GB10_HOSTS[0]),
        ("trt-gb10-7", *preflight_module.ACTIVE_GB10_HOSTS[1:]),
    ],
)
def test_collect_preflight_rejects_non_authoritative_gb10_target_without_ssh(
    tmp_path: Path,
    configured_hosts: tuple[str, ...],
) -> None:
    config = make_config(tmp_path)
    replace_gb10_hosts(config, configured_hosts)
    calls: list[list[str]] = []

    def run(argv: list[str]) -> subprocess.CompletedProcess[str]:
        calls.append(argv)
        return successful_command(argv)

    report = collect_preflight(
        config,
        service_uid=os.geteuid(),
        run=run,
        which=lambda name: f"/usr/bin/{name}",
        importer=lambda name: object(),
    )

    check = next(check for check in report.checks if check.name == "gb10-batch-mode")
    assert check.passed is False
    assert not any(argv[:1] == ["ssh"] for argv in calls)


def test_collect_preflight_rejects_full_topology_digest_drift_without_ssh(
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path)
    ssh_config = config.runner_repo / "deploy/worker-pools/gb10/ssh_config"
    ssh_config.write_text(
        ssh_config.read_text(encoding="utf-8").replace(
            "HostName 192.168.20.17",
            "HostName 192.168.20.117",
        ),
        encoding="utf-8",
    )
    calls: list[list[str]] = []

    def run(argv: list[str]) -> subprocess.CompletedProcess[str]:
        calls.append(argv)
        return successful_command(argv)

    report = collect_preflight(
        config,
        service_uid=os.geteuid(),
        run=run,
        which=lambda name: f"/usr/bin/{name}",
        importer=lambda name: object(),
    )

    topology = next(check for check in report.checks if check.name == "gb10-topology")
    batch_mode = next(check for check in report.checks if check.name == "gb10-batch-mode")
    assert topology.passed is False
    assert batch_mode.passed is False
    assert not any(argv[:1] == ["ssh"] for argv in calls)


def test_collect_preflight_rejects_wrong_origin_url(tmp_path: Path) -> None:
    config = make_config(tmp_path)

    def wrong_origin(argv: list[str]) -> subprocess.CompletedProcess[str]:
        result = successful_command(argv)
        if "get-url" in argv:
            return subprocess.CompletedProcess(argv, 0, "https://example.com/wrong.git\n", "")
        return result

    report = collect_preflight(
        config,
        service_uid=os.geteuid(),
        run=wrong_origin,
        which=lambda name: f"/usr/bin/{name}",
        importer=lambda name: object(),
    )

    checkout = next(check for check in report.checks if check.name == "checkout")
    assert checkout.passed is False


def test_collect_preflight_requires_service_tools_and_all_rollout_imports(
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path)
    executables: list[str] = []
    imports: list[str] = []

    def which(name: str) -> str | None:
        executables.append(name)
        return None if name == "systemd-run" else f"/usr/bin/{name}"

    def importer(name: str) -> object:
        imports.append(name)
        if name == "loom_benchmark_terminal_bench_2.adapter":
            raise ModuleNotFoundError(name)
        return object()

    report = collect_preflight(
        config,
        service_uid=os.geteuid(),
        run=successful_command,
        which=which,
        importer=importer,
    )

    assert "systemd-run" in executables
    assert "loom_benchmark_terminal_bench_2.adapter" in imports
    outcomes = {check.name: check.passed for check in report.checks}
    assert outcomes["executables"] is False
    assert outcomes["python-imports"] is False


@pytest.mark.parametrize("unsafe", ["mode", "symlink"])
def test_checkout_rejects_untrusted_git_config_before_any_git_command(
    tmp_path: Path,
    unsafe: str,
) -> None:
    config = make_config(tmp_path)
    git_config = config.runner_repo / ".git/config"
    git_config.write_text('[remote "origin"]\n', encoding="utf-8")
    if unsafe == "mode":
        git_config.chmod(0o664)
    else:
        target = tmp_path / "git-config-target"
        target.write_text('[remote "origin"]\n', encoding="utf-8")
        git_config.unlink()
        git_config.symlink_to(target)
    calls: list[list[str]] = []

    def run(argv: list[str]) -> subprocess.CompletedProcess[str]:
        calls.append(argv)
        return successful_command(argv)

    report = collect_preflight(
        config,
        service_uid=os.geteuid(),
        run=run,
        which=lambda name: f"/usr/bin/{name}",
        importer=lambda name: object(),
    )

    checkout = next(check for check in report.checks if check.name == "checkout")
    assert checkout.passed is False
    assert calls == []


@pytest.mark.parametrize("unsafe", ["writable-directory", "escaping-symlink"])
def test_checkout_rejects_unsafe_descendant_before_any_git_command(
    tmp_path: Path,
    unsafe: str,
) -> None:
    config = make_config(tmp_path)
    if unsafe == "writable-directory":
        (config.runner_repo / "deploy").chmod(0o775)
    else:
        (config.runner_repo / "escape").symlink_to(tmp_path / "outside")
    calls: list[list[str]] = []

    def run(argv: list[str]) -> subprocess.CompletedProcess[str]:
        calls.append(argv)
        return successful_command(argv)

    report = collect_preflight(
        config,
        service_uid=os.geteuid(),
        run=run,
        which=lambda name: f"/usr/bin/{name}",
        importer=lambda name: object(),
    )

    assert next(check for check in report.checks if check.name == "checkout").passed is False
    assert calls == []


def test_checkout_rejects_symlinked_repository_root(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    linked = tmp_path / "linked-repo"
    linked.symlink_to(config.runner_repo, target_is_directory=True)

    assert (
        preflight_module._checkout_tree_is_trusted(  # type: ignore[attr-defined]
            linked,
            service_uid=os.geteuid(),
        )
        is False
    )


def test_checkout_requires_pushurl_to_be_exactly_absent(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    (config.runner_repo / ".git/config").write_text('[remote "origin"]\n', encoding="utf-8")

    def configured_pushurl(argv: list[str]) -> subprocess.CompletedProcess[str]:
        if argv[-2:] == ["--get-all", "remote.origin.pushurl"]:
            return subprocess.CompletedProcess(argv, 0, "ssh://unapproved.example/repo\n", "")
        return successful_command(argv)

    report = collect_preflight(
        config,
        service_uid=os.geteuid(),
        run=configured_pushurl,
        which=lambda name: f"/usr/bin/{name}",
        importer=lambda name: object(),
    )

    assert next(check for check in report.checks if check.name == "checkout").passed is False


@pytest.mark.parametrize("mode", [0o620, 0o610, 0o604])
def test_credentials_reject_group_write_execute_and_all_world_access(
    tmp_path: Path,
    mode: int,
) -> None:
    config = make_config(tmp_path)
    path = Path(config.admin_token_source.removeprefix("file:"))
    path.chmod(mode)

    report = collect_preflight(
        config,
        service_uid=os.geteuid(),
        run=successful_command,
        which=lambda name: f"/usr/bin/{name}",
        importer=lambda name: object(),
    )

    assert next(check for check in report.checks if check.name == "credentials").passed is False


def test_credentials_and_catalog_accept_qianyi_style_0640_acl_readability(
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path)
    for source in (
        config.admin_token_source,
        config.worker_token_source,
        config.service_token_source,
    ):
        Path(source.removeprefix("file:")).chmod(0o640)
    catalog = tmp_path / "catalog.env"
    catalog.chmod(0o640)

    report = collect_preflight(
        config,
        service_uid=os.geteuid(),
        run=successful_command,
        which=lambda name: f"/usr/bin/{name}",
        importer=lambda name: object(),
    )
    outcomes = {check.name: check.passed for check in report.checks}

    assert outcomes["credentials"] is True
    assert outcomes["catalog-environment"] is True


def test_data_root_needs_only_read_traverse_when_declared_subdirectories_are_writable(
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path)
    config.rollout_root.chmod(0o500)
    try:
        report = collect_preflight(
            config,
            service_uid=os.geteuid(),
            run=successful_command,
            which=lambda name: f"/usr/bin/{name}",
            importer=lambda name: object(),
        )
    finally:
        config.rollout_root.chmod(0o700)

    assert next(check for check in report.checks if check.name == "data-root").passed is True


def test_data_root_requires_preexisting_environment_state_write_access(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    (config.rollout_root / "environment-state").rmdir()

    report = collect_preflight(
        config,
        service_uid=os.geteuid(),
        run=successful_command,
        which=lambda name: f"/usr/bin/{name}",
        importer=lambda name: object(),
    )

    assert next(check for check in report.checks if check.name == "data-root").passed is False


def test_data_root_rejects_symlinked_declared_subdirectory(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    environment_state = config.rollout_root / "environment-state"
    environment_state.rmdir()
    outside = tmp_path / "outside-environment-state"
    outside.mkdir(mode=0o700)
    environment_state.symlink_to(outside, target_is_directory=True)

    report = collect_preflight(
        config,
        service_uid=os.geteuid(),
        run=successful_command,
        which=lambda name: f"/usr/bin/{name}",
        importer=lambda name: object(),
    )

    assert next(check for check in report.checks if check.name == "data-root").passed is False


@pytest.mark.skipif(not hasattr(os, "O_PATH"), reason="Linux O_PATH contract")
def test_credentials_accept_traverse_only_parent_without_directory_listing(
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path)
    credentials = Path(config.admin_token_source.removeprefix("file:")).parent
    credentials.chmod(0o100)
    try:
        report = collect_preflight(
            config,
            service_uid=os.geteuid(),
            run=successful_command,
            which=lambda name: f"/usr/bin/{name}",
            importer=lambda name: object(),
        )
    finally:
        credentials.chmod(0o700)

    assert next(check for check in report.checks if check.name == "credentials").passed is True


def test_qianyi_owner_allowance_is_explicit_not_applied_to_git_config(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    protected = tmp_path / "protected"
    protected.write_text("value", encoding="utf-8")
    protected.chmod(0o640)
    monkeypatch.setattr(
        preflight_module.pwd,
        "getpwnam",
        lambda username: type("Entry", (), {"pw_uid": os.geteuid()})(),
    )

    assert (
        preflight_module._trusted_file_bytes(  # type: ignore[attr-defined]
            protected,
            service_uid=os.geteuid() + 1,
            private=True,
            allow_qianyi_owner=True,
        )
        == b"value"
    )
    assert (
        preflight_module._trusted_file_bytes(  # type: ignore[attr-defined]
            protected,
            service_uid=os.geteuid() + 1,
            private=False,
            allow_qianyi_owner=False,
        )
        is None
    )


def test_catalog_path_rejects_symlinked_intermediate_components(
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path)
    real = tmp_path / "real-catalog"
    real.mkdir()
    (real / "catalog.env").write_text("PUBLISHED_SHA=abc\n", encoding="utf-8")
    (real / "catalog.env").chmod(0o600)
    linked = tmp_path / "linked-catalog"
    linked.symlink_to(real, target_is_directory=True)
    profile = config.runner_repo / "deploy/environment-state/staging.toml"
    profile.write_text(
        f'[catalog_provisioning]\nenv_file = "{linked / "catalog.env"}"\n',
        encoding="utf-8",
    )

    report = collect_preflight(
        config,
        service_uid=os.geteuid(),
        run=successful_command,
        which=lambda name: f"/usr/bin/{name}",
        importer=lambda name: object(),
    )

    assert (
        next(check for check in report.checks if check.name == "catalog-environment").passed
        is False
    )


def test_profile_path_rejects_symlinked_intermediate_components(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    real = tmp_path / "real-profile"
    real.mkdir()
    (real / "staging.toml").write_text(
        f'[catalog_provisioning]\nenv_file = "{tmp_path / "catalog.env"}"\n',
        encoding="utf-8",
    )
    linked = tmp_path / "linked-profile"
    linked.symlink_to(real, target_is_directory=True)
    cluster = config.cluster_config_path
    cluster.write_text(
        cluster.read_text(encoding="utf-8").replace(
            'env_state_profile = "../environment-state/staging.toml"',
            f'env_state_profile = "{linked / "staging.toml"}"',
        ),
        encoding="utf-8",
    )

    report = collect_preflight(
        config,
        service_uid=os.geteuid(),
        run=successful_command,
        which=lambda name: f"/usr/bin/{name}",
        importer=lambda name: object(),
    )

    assert (
        next(check for check in report.checks if check.name == "catalog-environment").passed
        is False
    )


def test_ssh_config_rejects_symlinked_intermediate_components(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    real = tmp_path / "real-ssh"
    real.mkdir()
    (real / "ssh_config").write_text("Host trt-gb10-*\n", encoding="utf-8")
    linked = tmp_path / "linked-ssh"
    linked.symlink_to(real, target_is_directory=True)
    cluster = config.cluster_config_path
    cluster.write_text(
        cluster.read_text(encoding="utf-8").replace(
            'ssh_config = "../worker-pools/gb10/ssh_config"',
            f'ssh_config = "{linked / "ssh_config"}"',
        ),
        encoding="utf-8",
    )

    report = collect_preflight(
        config,
        service_uid=os.geteuid(),
        run=successful_command,
        which=lambda name: f"/usr/bin/{name}",
        importer=lambda name: object(),
    )

    assert next(check for check in report.checks if check.name == "gb10-batch-mode").passed is False


def test_catalog_secret_values_returns_individual_env_values(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    catalog = tmp_path / "catalog.env"
    catalog.write_text(
        "PUBLIC_NAME=staging\nCATALOG_PASSWORD=long-catalog-secret\n"
        "MINIO_SECRET_KEY='another-secret-value'\n",
        encoding="utf-8",
    )
    catalog.chmod(0o640)

    assert hasattr(preflight_module, "catalog_secret_values")
    assert preflight_module.catalog_secret_values(  # type: ignore[attr-defined]
        config, service_uid=os.geteuid()
    ) == (
        "staging",
        "long-catalog-secret",
        "another-secret-value",
    )


def test_default_preflight_subprocesses_use_only_sanitized_environment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = make_config(tmp_path)
    expected = sanitized_child_environment(config, service_uid=os.geteuid())
    environments: list[dict[str, str] | None] = []

    def fake_run(argv: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        environments.append(kwargs.get("env"))  # type: ignore[arg-type]
        return successful_command(argv)

    monkeypatch.setattr(subprocess, "run", fake_run)
    collect_preflight(
        config,
        service_uid=os.geteuid(),
        which=lambda name: f"/usr/bin/{name}",
        importer=lambda name: object(),
    )

    assert environments
    assert all(environment == expected for environment in environments)


def test_default_executable_lookup_uses_only_sanitized_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = make_config(tmp_path)
    expected_path = sanitized_child_environment(config, service_uid=os.geteuid())["PATH"]
    lookup_paths: list[str | None] = []

    def fake_which(name: str, path: str | None = None) -> str:
        lookup_paths.append(path)
        return f"/usr/bin/{name}"

    monkeypatch.setattr(preflight_module.shutil, "which", fake_which)
    collect_preflight(
        config,
        service_uid=os.geteuid(),
        run=successful_command,
        importer=lambda name: object(),
    )

    assert lookup_paths
    assert all(path == expected_path for path in lookup_paths)
