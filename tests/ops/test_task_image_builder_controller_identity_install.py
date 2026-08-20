from __future__ import annotations

import os
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
INSTALLER = ROOT / "deploy/slurm/install-loom-task-image-builder-controller-identity.sh"
EXPECTED_REPORT = (
    '{"certified_nodes":[],"production_certification_allowed":false,'
    '"state":"controller_identity_prepared"}\n'
)


@dataclass(frozen=True)
class Fixture:
    policy: Path
    passwd: Path
    group: Path
    subuid: Path
    subgid: Path
    command_log: Path
    fake_bin: Path


def _write_executable(path: Path, source: str) -> None:
    path.write_text(source, encoding="utf-8")
    path.chmod(0o755)


def _fixture(tmp_path: Path) -> Fixture:
    policy = tmp_path / "prerequisites-v1.toml"
    policy.write_text(
        """
schema = "loom.task-image-builder-prerequisites/v1"
policy_version = "task-image-builder-prerequisites-v1"
production_certification_allowed = false
certified_nodes = []
unconditional_blockers = ["phase2_guard_provider_release_missing"]

[identity]
user = "loom-builder"
group = "loom-task-builder"
uid = 993
gid = 980
subid_start = 3000000
subid_count = 65536
home = "/nonexistent"
shell = "/usr/sbin/nologin"
forbidden_supplementary_groups = ["docker", "root", "sudo"]

[[clusters]]
id = "test"
slurm_cluster = "test-cluster"
architecture = "x86_64"
controller = "test-controller"
builder_nodes = ["node-1"]
""".lstrip(),
        encoding="utf-8",
    )
    passwd = tmp_path / "passwd"
    group = tmp_path / "group"
    subuid = tmp_path / "subuid"
    subgid = tmp_path / "subgid"
    passwd.write_text("root:x:0:0:root:/root:/bin/bash\n", encoding="utf-8")
    group.write_text("root:x:0:\ndocker:x:988:\n", encoding="utf-8")
    subuid.write_text("foreign:100000:65536\n", encoding="utf-8")
    subgid.write_text("foreign:100000:65536\n", encoding="utf-8")
    command_log = tmp_path / "commands.log"
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()

    _write_executable(
        fake_bin / "getent",
        """#!/usr/bin/env bash
set -euo pipefail
if [[ "$#" -ne 2 ]]; then exit 2; fi
case "$1" in
  passwd) database="$LOOM_TEST_PASSWD_FILE"; field=1; numeric_field=3 ;;
  group) database="$LOOM_TEST_GROUP_FILE"; field=1; numeric_field=3 ;;
  *) exit 2 ;;
esac
if [[ "$2" =~ ^[0-9]+$ ]]; then field="$numeric_field"; fi
awk -F: -v field="$field" -v wanted="$2" '$field == wanted { print; found=1 } END { if (!found) exit 2 }' "$database"
""",
    )
    _write_executable(
        fake_bin / "id",
        """#!/usr/bin/env bash
set -euo pipefail
case "$*" in
  "-u") printf '%s\n' "${LOOM_TEST_EFFECTIVE_UID:-0}" ;;
  "-G loom-builder")
    if ! awk -F: '$1 == "loom-builder" { found=1 } END { exit !found }' "$LOOM_TEST_PASSWD_FILE"; then
      exit 1
    fi
    printf '%s\n' "${LOOM_TEST_ID_GROUPS:-980}"
    ;;
  *) exec /usr/bin/id "$@" ;;
esac
""",
    )
    _write_executable(
        fake_bin / "groupadd",
        """#!/usr/bin/env bash
set -euo pipefail
printf 'groupadd %s\n' "$*" >> "$LOOM_TEST_COMMAND_LOG"
if [[ "$*" != "--system --gid 980 loom-task-builder" ]]; then exit 2; fi
printf 'loom-task-builder:x:980:\n' >> "$LOOM_TEST_GROUP_FILE"
""",
    )
    _write_executable(
        fake_bin / "useradd",
        """#!/usr/bin/env bash
set -euo pipefail
printf 'useradd %s\n' "$*" >> "$LOOM_TEST_COMMAND_LOG"
if [[ "$*" != "--system --uid 993 --gid loom-task-builder --home-dir /nonexistent --shell /usr/sbin/nologin --no-create-home loom-builder" ]]; then
  exit 2
fi
printf 'loom-builder:x:993:980::/nonexistent:/usr/sbin/nologin\n' >> "$LOOM_TEST_PASSWD_FILE"
""",
    )
    _write_executable(
        fake_bin / "scontrol",
        """#!/usr/bin/env bash
set -euo pipefail
if [[ "$*" != "show config" ]]; then exit 2; fi
case "${LOOM_TEST_SLURM_MODE:-exact}" in
  wrong-cluster)
    printf 'ClusterName = other-cluster\nSlurmctldHost[0] = test-controller(127.0.0.1)\n'
    ;;
  wrong-controller)
    printf 'ClusterName = test-cluster\nSlurmctldHost[0] = other-controller(127.0.0.1)\n'
    ;;
  *)
    printf 'ClusterName = test-cluster\nSlurmctldHost[0] = test-controller(127.0.0.1)\n'
    ;;
esac
""",
    )
    return Fixture(policy, passwd, group, subuid, subgid, command_log, fake_bin)


def _environment(
    fixture: Fixture,
    *,
    host_arch: str = "x86_64",
    controller_host: str = "test-controller",
    slurm_mode: str = "exact",
    id_groups: str = "980",
    effective_uid: int = 0,
) -> dict[str, str]:
    return {
        **os.environ,
        "PATH": f"{fixture.fake_bin}:{os.environ['PATH']}",
        "LOOM_POLICY_PATH": str(fixture.policy),
        "LOOM_PASSWD_FILE": str(fixture.passwd),
        "LOOM_GROUP_FILE": str(fixture.group),
        "LOOM_HOST_ARCH": host_arch,
        "LOOM_CONTROLLER_HOST": controller_host,
        "LOOM_TEST_PASSWD_FILE": str(fixture.passwd),
        "LOOM_TEST_GROUP_FILE": str(fixture.group),
        "LOOM_TEST_COMMAND_LOG": str(fixture.command_log),
        "LOOM_TEST_SLURM_MODE": slurm_mode,
        "LOOM_TEST_ID_GROUPS": id_groups,
        "LOOM_TEST_EFFECTIVE_UID": str(effective_uid),
    }


def _run(
    fixture: Fixture,
    action: str,
    **environment_overrides: str | int,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            shutil.which("bash") or "bash",
            "-c",
            'source "$1"; "loom_controller_identity_$2" test',
            "controller-identity-test",
            str(INSTALLER),
            action,
        ],
        cwd=ROOT,
        env=_environment(fixture, **environment_overrides),
        check=False,
        capture_output=True,
        text=True,
    )


def _run_main(
    fixture: Fixture,
    action: str,
    **environment_overrides: str | int,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            shutil.which("bash") or "bash",
            "-c",
            'source "$1"; loom_controller_identity_main "$2" test',
            "controller-identity-main-test",
            str(INSTALLER),
            action,
        ],
        cwd=ROOT,
        env=_environment(fixture, **environment_overrides),
        check=False,
        capture_output=True,
        text=True,
    )


def _identity_state(fixture: Fixture) -> dict[Path, bytes]:
    return {
        path: path.read_bytes()
        for path in (fixture.passwd, fixture.group, fixture.subuid, fixture.subgid)
    }


def _install_exact_identity(fixture: Fixture) -> None:
    fixture.passwd.write_text(
        fixture.passwd.read_text(encoding="utf-8")
        + "loom-builder:x:993:980::/nonexistent:/usr/sbin/nologin\n",
        encoding="utf-8",
    )
    fixture.group.write_text(
        fixture.group.read_text(encoding="utf-8") + "loom-task-builder:x:980:\n",
        encoding="utf-8",
    )


def test_installer_parses_and_check_mode_does_not_mutate(tmp_path: Path) -> None:
    parsed = subprocess.run(
        [shutil.which("bash") or "bash", "-n", str(INSTALLER)],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert parsed.returncode == 0, parsed.stderr
    fixture = _fixture(tmp_path)
    before = _identity_state(fixture)

    checked = _run(fixture, "check")

    assert checked.returncode == 1
    assert "incomplete" in checked.stderr
    assert _identity_state(fixture) == before
    assert not fixture.command_log.exists()


@pytest.mark.parametrize(
    "environment_overrides",
    [
        {"host_arch": "aarch64"},
        {"controller_host": "other-controller"},
        {"slurm_mode": "wrong-cluster"},
        {"slurm_mode": "wrong-controller"},
    ],
)
def test_wrong_controller_or_architecture_fails_before_mutation(
    tmp_path: Path,
    environment_overrides: dict[str, str],
) -> None:
    fixture = _fixture(tmp_path)
    before = _identity_state(fixture)

    result = _run(fixture, "apply", **environment_overrides)

    assert result.returncode == 1
    assert "controller" in result.stderr or "architecture" in result.stderr
    assert _identity_state(fixture) == before
    assert not fixture.command_log.exists()


def test_non_root_apply_is_rejected_before_test_override_check(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    before = _identity_state(fixture)

    result = _run_main(fixture, "apply", effective_uid=1000)

    assert result.returncode == 1
    assert "requires root" in result.stderr
    assert _identity_state(fixture) == before
    assert not fixture.command_log.exists()


@pytest.mark.parametrize("conflict", ["user-name", "uid", "group-name", "gid"])
def test_numeric_and_name_collisions_fail_before_mutation(
    tmp_path: Path,
    conflict: str,
) -> None:
    fixture = _fixture(tmp_path)
    if conflict == "user-name":
        fixture.passwd.write_text(
            fixture.passwd.read_text(encoding="utf-8")
            + "loom-builder:x:994:980::/nonexistent:/usr/sbin/nologin\n",
            encoding="utf-8",
        )
    elif conflict == "uid":
        fixture.passwd.write_text(
            fixture.passwd.read_text(encoding="utf-8")
            + "foreign:x:993:2000::/nonexistent:/usr/sbin/nologin\n",
            encoding="utf-8",
        )
    elif conflict == "group-name":
        fixture.group.write_text(
            fixture.group.read_text(encoding="utf-8") + "loom-task-builder:x:981:\n",
            encoding="utf-8",
        )
    else:
        fixture.group.write_text(
            fixture.group.read_text(encoding="utf-8") + "foreign:x:980:\n",
            encoding="utf-8",
        )
    before = _identity_state(fixture)

    result = _run(fixture, "apply")

    assert result.returncode == 1
    assert "conflict" in result.stderr
    assert _identity_state(fixture) == before
    assert not fixture.command_log.exists()


def test_first_apply_creates_only_the_exact_controller_identity(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    subids_before = (fixture.subuid.read_bytes(), fixture.subgid.read_bytes())

    result = _run(fixture, "apply")

    assert result.returncode == 0, result.stderr
    assert result.stdout == EXPECTED_REPORT
    assert fixture.command_log.read_text(encoding="utf-8").splitlines() == [
        "groupadd --system --gid 980 loom-task-builder",
        "useradd --system --uid 993 --gid loom-task-builder --home-dir /nonexistent "
        "--shell /usr/sbin/nologin --no-create-home loom-builder",
    ]
    assert fixture.passwd.read_text(encoding="utf-8").splitlines()[-1] == (
        "loom-builder:x:993:980::/nonexistent:/usr/sbin/nologin"
    )
    assert fixture.group.read_text(encoding="utf-8").splitlines()[-1] == (
        "loom-task-builder:x:980:"
    )
    assert (fixture.subuid.read_bytes(), fixture.subgid.read_bytes()) == subids_before


def test_second_apply_and_check_are_idempotent(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    first = _run(fixture, "apply")
    assert first.returncode == 0, first.stderr
    state_after_first = _identity_state(fixture)
    log_after_first = fixture.command_log.read_bytes()

    second = _run(fixture, "apply")
    checked = _run(fixture, "check")

    assert second.returncode == 0, second.stderr
    assert checked.returncode == 0, checked.stderr
    assert second.stdout == EXPECTED_REPORT
    assert checked.stdout == EXPECTED_REPORT
    assert _identity_state(fixture) == state_after_first
    assert fixture.command_log.read_bytes() == log_after_first


def test_any_supplementary_group_is_fatal(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    _install_exact_identity(fixture)
    before = _identity_state(fixture)

    result = _run(fixture, "apply", id_groups="980 988")

    assert result.returncode == 1
    assert "supplementary" in result.stderr
    assert _identity_state(fixture) == before
    assert not fixture.command_log.exists()


def test_stale_supplementary_membership_fails_before_user_creation(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    fixture.group.write_text(
        fixture.group.read_text(encoding="utf-8").replace(
            "docker:x:988:",
            "docker:x:988:loom-builder",
        ),
        encoding="utf-8",
    )
    before = _identity_state(fixture)

    result = _run(fixture, "apply", id_groups="980 988")

    assert result.returncode == 1
    assert "supplementary" in result.stderr
    assert _identity_state(fixture) == before
    assert not fixture.command_log.exists()
