from __future__ import annotations

import json
import os
import stat
from pathlib import Path

import pytest
from scripts.ops import staging_rollout_host as host

TEAM_ID = "11111111-1111-4111-8111-111111111111"
TEAM_ID_2 = "22222222-2222-4222-8222-222222222222"


class FakeSystem:
    def __init__(self, filesystem: host.LocalFilesystem) -> None:
        self.filesystem = filesystem
        self.group = False
        self.service_user = False
        self.operator_members: set[str] = set()
        self.docker = False
        self.key = False
        self.input_acls: set[Path] = set()
        self.data_acls: set[Path] = set()
        self.linger = False
        self.status = "idle"
        self.validated = 0
        self.candidate_syncs = 0
        self.candidate_sha: str | None = None
        self.revoked = False
        self.revoke_error: str | None = None
        self.ledger_modes: list[str] = []
        self.events: list[str] = []
        self.removed_members: list[str] = []
        self.trust_ready = False
        self.dry_runs = 0
        self.venv = False
        self.venv_lock_mode: int | None = None
        self.venv_lock_hardenings = 0
        self.admission_disabled_at_status = False
        self.maintenance = False
        self.maintenance_begins = 0
        self.maintenance_ends = 0
        self.source_reads: list[str] = []
        self.remote_source_sha = "a" * 40
        self.install_source_sha: str | None = None

    def validate_prerequisites(self) -> None:
        self.validated += 1

    def validate_invocation_checkout(self) -> str:
        self.validated += 1
        return "a" * 40

    def prepare_install_source(self) -> tuple[Path, str]:
        self.validated += 1
        if self.install_source_sha is None:
            self.install_source_sha = self.remote_source_sha
        return host.REPO_ROOT, self.remote_source_sha

    def validate_invocation_merged(self, invocation_head: str, source_sha: str) -> None:
        assert invocation_head == "a" * 40
        assert source_sha == self.remote_source_sha
        self.validated += 1

    def validate_assets(self, source_root: Path, source_sha: str) -> None:
        assert source_root == host.REPO_ROOT
        assert source_sha == self.remote_source_sha
        self.validated += 1

    def source_file(self, source_root: Path, source_sha: str, relative_path: str) -> bytes:
        assert source_root in {host.REPO_ROOT, host.INSTALL_SOURCE}
        assert source_sha in {"a" * 40, "b" * 40}
        self.source_reads.append(relative_path)
        return (host.REPO_ROOT / relative_path).read_bytes()

    def validate_installed_source(self, source_sha: str, *, require_checkout: bool) -> None:
        assert source_sha in {"a" * 40, "b" * 40}
        if require_checkout:
            assert self.install_source_sha == source_sha
        self.validated += 1

    def install_source_ready(self, expected_sha: str) -> bool:
        return self.install_source_sha == expected_sha

    def ensure_install_source_checkout(self, expected_sha: str) -> bool:
        changed = self.install_source_sha != expected_sha
        self.install_source_sha = expected_sha
        return changed

    def ensure_group(self, name: str) -> bool:
        assert name == host.OPERATOR_GROUP
        changed = not self.group
        self.group = True
        return changed

    def group_present(self, name: str) -> bool:
        assert name == host.OPERATOR_GROUP
        return self.group

    def ensure_service_user(self) -> bool:
        changed = not self.service_user
        self.service_user = True
        return changed

    def service_user_present(self) -> bool:
        return self.service_user

    def ensure_operator_membership(self, username: str) -> bool:
        changed = username not in self.operator_members
        self.operator_members.add(username)
        return changed

    def operator_membership_present(self, username: str) -> bool:
        return username in self.operator_members

    def ensure_docker_membership(self) -> bool:
        changed = not self.docker
        self.docker = True
        return changed

    def docker_membership_present(self) -> bool:
        return self.docker

    def ensure_owned_directory(self, path: Path, *, owner: str, mode: int) -> bool:
        assert owner == host.SERVICE_USER
        return self.filesystem.ensure_directory(path, mode)

    def owned_directory_ready(self, path: Path, *, owner: str, mode: int) -> bool:
        assert owner == host.SERVICE_USER
        mapped = self.filesystem.path(path)
        return mapped.is_dir() and (mapped.stat().st_mode & 0o777) == mode

    def ensure_root_directory(self, path: Path, *, mode: int) -> bool:
        return self.filesystem.ensure_directory(path, mode)

    def validate_install_record_authority(self, *, allow_absent: bool) -> None:
        if not allow_absent:
            assert self.filesystem.exists(host.INSTALL_RECORD)

    def create_runtime_directory(self) -> bool:
        return self.filesystem.ensure_directory(host.RUNTIME_ROOT, 0o700)

    def runtime_directory_ready(self) -> bool:
        mapped = self.filesystem.path(host.RUNTIME_ROOT)
        return mapped.is_dir() and (mapped.stat().st_mode & 0o777) == 0o700

    def ensure_candidate(self, expected_sha: str, *, refresh: bool) -> None:
        if refresh or self.candidate_sha != expected_sha:
            self.candidate_syncs += 1
            self.candidate_sha = expected_sha

    def candidate_ready(self, expected_sha: str) -> bool:
        return self.candidate_sha == expected_sha

    def venv_ready(self) -> bool:
        if self.venv_lock_mode not in {None, 0o600}:
            raise host.InstallError("root venv authority is unsafe")
        return self.venv

    def venv_lock_requires_hardening(self) -> bool:
        return self.venv_lock_mode is not None and self.venv_lock_mode != 0o600

    def harden_venv_lock(self) -> None:
        assert self.venv_lock_mode is not None
        self.venv_lock_mode = 0o600
        self.venv_lock_hardenings += 1

    def sync_venv(self, source_root: Path) -> None:
        assert source_root == host.REPO_ROOT
        self.candidate_syncs += 1
        self.venv = True
        self.venv_lock_mode = 0o600

    def ensure_service_key(self) -> bool:
        if self.key:
            return False
        self.filesystem.atomic_write(host.SERVICE_KEY, b"private-key-fixture\n", 0o600)
        self.filesystem.atomic_write(
            Path(str(host.SERVICE_KEY) + ".pub"), b"ssh-ed25519 public-fixture\n", 0o644
        )
        self.key = True
        return True

    def service_key_present(self) -> bool:
        return self.key

    def public_key_fingerprint(self) -> str:
        return "SHA256:6JjXfjyF6JMXDB2Wp4t1YgAzFJPaTv5mQJaqodL6GdU"

    def _trust_ledger(self) -> dict[str, object]:
        try:
            payload = self.filesystem.read_bytes(host.TRUST_REVOCATION_LEDGER, limit=64 * 1024)
            parsed = json.loads(payload)
        except (host.InstallError, json.JSONDecodeError) as exc:
            raise host.InstallError("fake GB10 trust ledger is unavailable") from exc
        path = self.filesystem.path(host.TRUST_REVOCATION_LEDGER)
        if (
            not isinstance(parsed, dict)
            or path.is_symlink()
            or (path.stat().st_mode & 0o777) != 0o600
            or set(parsed)
            != {
                "key_fingerprint",
                "revocation_hosts",
                "schema_version",
                "topology_sha256",
            }
            or type(parsed.get("schema_version")) is not int
            or parsed.get("schema_version") != 1
            or not isinstance(parsed.get("revocation_hosts"), list)
        ):
            raise host.InstallError("fake GB10 trust ledger is invalid")
        return parsed

    def _write_trust_ledger(self, hosts: list[str]) -> None:
        payload = (
            json.dumps(
                {
                    "key_fingerprint": self.public_key_fingerprint(),
                    "revocation_hosts": hosts,
                    "schema_version": 1,
                    "topology_sha256": "b" * 64,
                },
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n"
        ).encode()
        self.filesystem.atomic_write(host.TRUST_REVOCATION_LEDGER, payload, 0o600)

    def prepare_gb10_trust_ledger(self, source_root: Path, *, mode: str) -> None:
        assert source_root == host.REPO_ROOT
        assert mode in {"fresh", "legacy", "existing"}
        self.ledger_modes.append(mode)
        self.events.append(f"trust-ledger:{mode}")
        if not self.filesystem.exists(host.TRUST_REVOCATION_LEDGER):
            if mode == "existing":
                raise host.InstallError("fake GB10 trust ledger is unavailable")
            self._write_trust_ledger([])
        ledger = self._trust_ledger()
        if ledger.get("key_fingerprint") != self.public_key_fingerprint():
            raise host.InstallError("fake GB10 trust ledger key binding is invalid")
        if mode == "legacy":
            self._write_trust_ledger([f"trt-gb10-{number}" for number in range(1, 16)])

    def require_gb10_revocation_complete(self) -> None:
        self.events.append("trust-ledger:finalize-check")
        if self._trust_ledger().get("revocation_hosts") != []:
            raise host.InstallError("fake GB10 trust revocation is incomplete")

    def install_owner(self, path: Path, owner: str, mode: int) -> bool:
        del owner
        mapped = self.filesystem.path(path)
        if mapped.exists():
            mapped.chmod(mode)
        return False

    def file_owner_ready(self, path: Path, *, owner: str, mode: int) -> bool:
        del owner
        mapped = self.filesystem.path(path)
        return mapped.is_file() and (mapped.stat().st_mode & 0o777) == mode

    def gb10_trust_ready(self) -> bool:
        return self.trust_ready

    def run_post_install_dry_run(self) -> None:
        self.dry_runs += 1

    def check_runtime(self, expected_sha: str) -> list[str]:
        return [] if self.candidate_sha == expected_sha else ["candidate-checkout"]

    def export_kubeconfig(self) -> bytes:
        return b"apiVersion: v1\ncurrent-context: loom-staging\n"

    def plan_input_acl(self, path: Path) -> tuple[host.AclPlan, ...]:
        paths = [parent for parent in path.parents if parent != Path("/")]
        paths = paths[: paths.index(Path("/shared_work")) + 1]
        paths.append(path)
        plans = []
        for target in paths:
            if target not in self.input_acls:
                permissions = "r--" if target == path else "--x"
                plans.append(host.AclPlan(host.AclGrant(target), permissions))
        return tuple(plans)

    def plan_data_acl(self, path: Path) -> tuple[host.AclPlan, ...]:
        plans = []
        for default in (False, True):
            key = Path(f"{path}#{default}")
            if key not in self.data_acls:
                plans.append(host.AclPlan(host.AclGrant(path, default=default), "rwx"))
        return tuple(plans)

    def apply_acl(self, plan: host.AclPlan) -> host.AclGrant:
        grant = plan.grant
        if grant.path in host.DATA_DIRECTORIES:
            self.data_acls.add(Path(f"{grant.path}#{grant.default}"))
        else:
            self.input_acls.add(grant.path)
        return grant

    def ensure_input_acl(self, path: Path) -> tuple[host.AclGrant, ...]:
        return tuple(self.apply_acl(plan) for plan in self.plan_input_acl(path))

    def ensure_data_acl(self, path: Path) -> tuple[host.AclGrant, ...]:
        return tuple(self.apply_acl(plan) for plan in self.plan_data_acl(path))

    def ensure_linger(self) -> bool:
        changed = not self.linger
        self.linger = True
        return changed

    def linger_enabled(self) -> bool:
        return self.linger

    def verify_user_manager(self) -> None:
        return None

    def active_status(self) -> str:
        self.admission_disabled_at_status = (
            not self.filesystem.exists(host.SUDOERS_PATH) and self.maintenance
        )
        return self.status

    def begin_maintenance(self) -> None:
        self.maintenance_begins += 1
        self.maintenance = True

    def end_maintenance(self) -> None:
        self.maintenance_ends += 1
        self.maintenance = False

    def revoke_gb10_trust(self) -> None:
        if self.revoke_error is not None:
            raise host.InstallError(self.revoke_error)
        self._trust_ledger()
        self.revoked = True
        self.events.append("trust-ledger:revoke")
        self._write_trust_ledger([])

    def remove_acl(self, grant: host.AclGrant) -> None:
        self.input_acls.discard(grant.path)
        self.data_acls.discard(Path(f"{grant.path}#{grant.default}"))

    def disable_linger(self) -> None:
        self.linger = False

    def remove_operator_membership(self, username: str) -> None:
        self.removed_members.append(username)
        self.operator_members.discard(username)

    def remove_docker_membership(self) -> None:
        self.docker = False


def _write_protected_inputs(filesystem: host.LocalFilesystem) -> None:
    for index, path in enumerate(host.PROTECTED_INPUTS):
        mapped = filesystem.path(path)
        mapped.parent.mkdir(parents=True, exist_ok=True)
        mapped.write_bytes(b"admin-token-fixture\n" if index == 0 else f"value-{index}\n".encode())
        mapped.chmod(0o640)
    for path in host.DATA_DIRECTORIES:
        filesystem.ensure_directory(path, 0o770)


def _installer(tmp_path: Path) -> tuple[host.HostInstaller, FakeSystem]:
    filesystem = host.LocalFilesystem(tmp_path)
    _write_protected_inputs(filesystem)
    system = FakeSystem(filesystem)
    return host.HostInstaller(filesystem, system, 0), system  # type: ignore[arg-type]


def test_install_is_idempotent_and_renders_only_safe_token_metadata(tmp_path: Path) -> None:
    installer, system = _installer(tmp_path)

    first = installer.install(TEAM_ID)
    second = installer.install(TEAM_ID)

    assert first["changed"]
    assert second["changed"] == []
    rendered = installer.filesystem.path(host.CONFIG_PATH).read_text(encoding="utf-8")
    assert TEAM_ID in rendered
    assert "sha256:" in rendered and " len=" in rendered
    assert "admin-token-fixture" not in rendered
    assert "__ADMIN_TOKEN_FINGERPRINT__" not in rendered
    assert "__SMOKE_ON_BEHALF_TEAM_ID__" not in rendered
    assert set(system.operator_members) == set(host.OPERATORS)
    assert system.docker is True
    assert system.candidate_syncs == 2  # candidate convergence and venv sync run only once
    assert system.maintenance_begins == 1
    assert system.maintenance_ends == 1
    assert system.maintenance is False
    assert first["post_install_check"] == "awaiting-gb10-trust"
    assert system.dry_runs == 0
    record = installer.filesystem.load_install_record()
    assert record is not None
    assert record["installation_state"] == "ready"
    assert record["admission_enabled"] is True
    assert record["maintenance_enabled"] is False
    assert record["trust_requires_revocation"] is True
    assert record["trust_ledger_migrated"] is True
    assert record["schema_version"] == 2
    assert record["added_acls"]
    assert system.ledger_modes == ["fresh", "existing"]
    assert set(system.source_reads) >= {
        "deploy/staging-rollout/loom-staging-rollout",
        "deploy/staging-rollout/loom-staging-rollout-broker",
        "deploy/staging-rollout/loom-staging-rollout.sudoers",
        "deploy/staging-rollout/loom-staging-rollout.tmpfiles",
        "deploy/staging-rollout/staging-rollout.toml",
        "scripts/ops/staging_rollout_gb10_trust.py",
    }


def test_install_migrates_legacy_revocation_before_replacing_trust_tool(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    installer, system = _installer(tmp_path)
    installer.install(TEAM_ID)
    installer.filesystem.remove(host.TRUST_REVOCATION_LEDGER)
    record = installer.filesystem.load_install_record()
    assert record is not None
    record["schema_version"] = 1
    record.pop("trust_ledger_migrated")
    installer.filesystem.atomic_write(
        host.INSTALL_RECORD,
        (json.dumps(record, sort_keys=True) + "\n").encode(),
        0o600,
    )
    installer.filesystem.atomic_write(host.TRUST_TOOL_PATH, b"legacy-trust-tool\n", 0o755)
    system.ledger_modes.clear()
    system.events.clear()
    original_atomic_write = host.LocalFilesystem.atomic_write

    def record_authority_write(
        filesystem: host.LocalFilesystem,
        absolute: Path,
        payload: bytes,
        mode: int,
    ) -> bool:
        if absolute == host.TRUST_TOOL_PATH:
            system.events.append("trust-tool:replace")
        return original_atomic_write(filesystem, absolute, payload, mode)

    monkeypatch.setattr(host.LocalFilesystem, "atomic_write", record_authority_write)

    result = installer.install(TEAM_ID)

    assert result["ok"] is True
    assert system.ledger_modes == ["legacy"]
    assert system.events.index("trust-ledger:legacy") < system.events.index("trust-tool:replace")
    assert system._trust_ledger()["revocation_hosts"] == [
        f"trt-gb10-{number}" for number in range(1, 16)
    ]
    migrated = installer.filesystem.load_install_record()
    assert migrated is not None
    assert migrated["schema_version"] == 2
    assert migrated["trust_ledger_migrated"] is True


def test_reinstall_fails_closed_when_migrated_ledger_disappears(tmp_path: Path) -> None:
    installer, system = _installer(tmp_path)
    installer.install(TEAM_ID)
    installer.filesystem.remove(host.TRUST_REVOCATION_LEDGER)
    system.ledger_modes.clear()

    with pytest.raises(host.InstallError, match="ledger is unavailable"):
        installer.install(TEAM_ID)

    assert system.ledger_modes == ["existing"]
    assert installer.filesystem.exists(host.SERVICE_KEY)
    assert installer.filesystem.exists(host.INSTALL_RECORD)
    assert installer.filesystem.exists(host.SUDOERS_PATH)


def test_install_runs_dry_run_only_after_all_gb10_trust_is_ready(tmp_path: Path) -> None:
    installer, system = _installer(tmp_path)
    system.trust_ready = True

    result = installer.install(TEAM_ID)

    assert result["post_install_check"] == "passed"
    assert system.dry_runs == 1


def test_unchanged_reinstall_does_not_repeat_post_install_dry_run(tmp_path: Path) -> None:
    installer, system = _installer(tmp_path)
    system.trust_ready = True

    first = installer.install(TEAM_ID)
    second = installer.install(TEAM_ID)

    assert first["changed"]
    assert second["changed"] == []
    assert system.dry_runs == 1


def test_update_refuses_active_rollout_before_replacing_runtime(tmp_path: Path) -> None:
    installer, system = _installer(tmp_path)
    installer.install(TEAM_ID)
    original_config = installer.filesystem.read_bytes(host.CONFIG_PATH)
    original_sudoers = b"previous-merged-sudoers-authority\n"
    installer.filesystem.atomic_write(host.SUDOERS_PATH, original_sudoers, 0o440)
    system.remote_source_sha = "b" * 40
    system.maintenance_begins = 0
    system.maintenance_ends = 0
    system.status = "running"

    with pytest.raises(host.InstallError, match="active"):
        installer.install(TEAM_ID_2)

    assert installer.filesystem.read_bytes(host.CONFIG_PATH) == original_config
    assert installer.filesystem.exists(host.SUDOERS_PATH)
    assert installer.filesystem.read_bytes(host.SUDOERS_PATH) == original_sudoers
    assert system.admission_disabled_at_status is True
    assert system.maintenance_begins == 1
    assert system.maintenance_ends == 1
    assert system.maintenance is False
    assert system.install_source_sha == "a" * 40
    record = installer.filesystem.load_install_record()
    assert record is not None
    assert record["installation_state"] == "ready"
    assert record["smoke_on_behalf_team_id"] == TEAM_ID


def test_update_failure_stays_fail_closed_with_maintenance_marker(tmp_path: Path) -> None:
    installer, system = _installer(tmp_path)
    installer.install(TEAM_ID)
    system.remote_source_sha = "b" * 40
    system.maintenance_begins = 0
    system.maintenance_ends = 0
    system.status = "done"
    original_ensure_candidate = system.ensure_candidate

    def fail_candidate(expected_sha: str, *, refresh: bool) -> None:
        del expected_sha, refresh
        raise host.InstallError("injected candidate update failure")

    system.ensure_candidate = fail_candidate  # type: ignore[method-assign]
    with pytest.raises(host.InstallError, match="candidate update failure"):
        installer.install(TEAM_ID_2)

    assert not installer.filesystem.exists(host.SUDOERS_PATH)
    assert system.maintenance is True
    assert system.maintenance_begins == 1
    assert system.maintenance_ends == 0
    record = installer.filesystem.load_install_record()
    assert record is not None
    assert record["installation_state"] == "installing"
    assert record["admission_enabled"] is False
    assert record["maintenance_enabled"] is True
    assert record["trust_requires_revocation"] is True
    assert record["smoke_on_behalf_team_id"] == TEAM_ID_2
    assert record["source_sha"] == "b" * 40
    assert system.install_source_sha == "b" * 40

    system.ensure_candidate = original_ensure_candidate  # type: ignore[method-assign]
    installer.uninstall(retain_ledger=True)
    assert system.revoked is True
    assert not installer.filesystem.exists(host.INSTALL_RECORD)


def test_interrupted_install_repairs_uv_lock_before_venv_readiness(
    tmp_path: Path,
) -> None:
    installer, system = _installer(tmp_path)

    def fail_user_manager() -> None:
        raise host.InstallError("injected user-manager failure")

    system.verify_user_manager = fail_user_manager  # type: ignore[method-assign]
    with pytest.raises(host.InstallError, match="user-manager failure"):
        installer.install(TEAM_ID)
    record = installer.filesystem.load_install_record()
    assert record is not None
    assert record["installation_state"] == "installing"

    # Exact live interruption: uv left its root-owned regular lock at mode 0666.
    system.venv_lock_mode = 0o666
    system.verify_user_manager = lambda: None  # type: ignore[method-assign]

    result = installer.install(TEAM_ID)

    assert "venv-lock" in result["changed"]
    assert system.venv_lock_mode == 0o600
    assert system.venv_lock_hardenings == 1
    assert system.candidate_syncs == 2
    record = installer.filesystem.load_install_record()
    assert record is not None
    assert record["installation_state"] == "ready"
    assert record["admission_enabled"] is True


def test_successful_update_reenables_admission_only_after_ready(tmp_path: Path) -> None:
    installer, system = _installer(tmp_path)
    installer.install(TEAM_ID)
    system.remote_source_sha = "b" * 40
    system.maintenance_begins = 0
    system.maintenance_ends = 0
    system.status = "done"

    result = installer.install(TEAM_ID_2)

    assert result["changed"]
    assert installer.filesystem.exists(host.SUDOERS_PATH)
    assert system.maintenance_begins == 1
    assert system.maintenance_ends == 1
    assert system.maintenance is False
    record = installer.filesystem.load_install_record()
    assert record is not None
    assert record["installation_state"] == "ready"
    assert record["admission_enabled"] is True
    assert record["maintenance_enabled"] is False
    assert record["trust_requires_revocation"] is True
    assert record["smoke_on_behalf_team_id"] == TEAM_ID_2
    assert record["source_sha"] == "b" * 40
    assert system.install_source_sha == "b" * 40


def test_check_rejects_candidate_checkout_drift(tmp_path: Path) -> None:
    installer, system = _installer(tmp_path)
    installer.install(TEAM_ID)
    system.candidate_sha = "b" * 40

    result = installer.check()

    assert result["ok"] is False
    assert "candidate-checkout" in result["failures"]


def test_failed_validation_never_replaces_installed_authority_files(tmp_path: Path) -> None:
    installer, system = _installer(tmp_path)
    original = b"existing-client\n"
    installer.filesystem.atomic_write(host.CLIENT_PATH, original, 0o755)

    def fail(source_root: Path, source_sha: str) -> None:
        del source_root, source_sha
        raise host.InstallError("invalid sudoers")

    system.validate_assets = fail  # type: ignore[method-assign]
    with pytest.raises(host.InstallError, match="invalid sudoers"):
        installer.install(TEAM_ID)

    assert installer.filesystem.path(host.CLIENT_PATH).read_bytes() == original


def test_failed_acl_plan_never_replaces_installed_authority_files(tmp_path: Path) -> None:
    installer, system = _installer(tmp_path)
    original = b"existing-client\n"
    installer.filesystem.atomic_write(host.CLIENT_PATH, original, 0o755)

    def fail(path: Path) -> tuple[host.AclPlan, ...]:
        del path
        raise host.InstallError("pre-existing service ACL is insufficient or masked")

    system.plan_input_acl = fail  # type: ignore[method-assign]
    with pytest.raises(host.InstallError, match="insufficient or masked"):
        installer.install(TEAM_ID)

    assert installer.filesystem.path(host.CLIENT_PATH).read_bytes() == original


def test_partial_acl_apply_failure_is_recoverable_from_provisional_ledger(
    tmp_path: Path,
) -> None:
    installer, system = _installer(tmp_path)
    original_apply = system.apply_acl
    calls = 0

    def fail_after_first(plan: host.AclPlan) -> host.AclGrant:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise host.InstallError("injected ACL failure")
        return original_apply(plan)

    system.apply_acl = fail_after_first  # type: ignore[method-assign]
    with pytest.raises(host.InstallError, match="injected ACL failure"):
        installer.install(TEAM_ID)
    record = installer.filesystem.load_install_record()
    assert record is not None
    assert record["installation_state"] == "installing"
    assert system.input_acls

    system.apply_acl = original_apply  # type: ignore[method-assign]

    def unexpected_runtime_dependency() -> None:
        raise AssertionError("pre-admission rollback must not require runtime or broker")

    system.begin_maintenance = unexpected_runtime_dependency  # type: ignore[method-assign]
    system.active_status = unexpected_runtime_dependency  # type: ignore[method-assign]
    installer.uninstall(retain_ledger=True)

    assert system.input_acls == set()
    assert system.data_acls == set()
    assert not installer.filesystem.exists(host.INSTALL_RECORD)


def test_first_mutation_failure_already_has_pre_admission_rollback_ledger(
    tmp_path: Path,
) -> None:
    installer, system = _installer(tmp_path)

    def fail_membership(username: str) -> bool:
        del username
        raise host.InstallError("injected membership failure")

    system.ensure_operator_membership = fail_membership  # type: ignore[method-assign]
    with pytest.raises(host.InstallError, match="injected membership failure"):
        installer.install(TEAM_ID)

    record = installer.filesystem.load_install_record()
    assert record is not None
    assert record["installation_state"] == "installing"
    assert record["admission_enabled"] is False
    assert record["maintenance_enabled"] is False
    assert record["trust_requires_revocation"] is False

    def unexpected_runtime_dependency() -> None:
        raise AssertionError("pre-admission rollback must not require runtime or broker")

    system.begin_maintenance = unexpected_runtime_dependency  # type: ignore[method-assign]
    system.active_status = unexpected_runtime_dependency  # type: ignore[method-assign]
    installer.uninstall(retain_ledger=True)

    assert system.revoked is False
    assert not installer.filesystem.exists(host.INSTALL_RECORD)


def test_install_preserves_existing_shared_file_mode_and_contents(tmp_path: Path) -> None:
    installer, _ = _installer(tmp_path)
    source = installer.filesystem.path(host.PROTECTED_INPUTS[0])
    before = (source.read_bytes(), source.stat().st_mode)

    installer.install(TEAM_ID)

    after = (source.read_bytes(), source.stat().st_mode)
    assert after == before


def test_uninstall_refuses_active_request_and_retains_ledger(tmp_path: Path) -> None:
    installer, system = _installer(tmp_path)
    installer.install(TEAM_ID)
    ledger = installer.filesystem.path(host.STATE_ROOT / "requests/request-a/events.jsonl")
    ledger.parent.mkdir(parents=True, exist_ok=True)
    ledger.write_text("evidence\n", encoding="utf-8")
    system.status = "running"

    with pytest.raises(host.InstallError, match="active"):
        installer.uninstall(retain_ledger=True)
    assert ledger.read_text(encoding="utf-8") == "evidence\n"
    assert system.revoked is False
    assert system.admission_disabled_at_status is True
    assert installer.filesystem.exists(host.SUDOERS_PATH)
    assert system.maintenance is False

    system.status = "done"
    result = installer.uninstall(retain_ledger=True)
    assert result["ok"] is True
    assert ledger.read_text(encoding="utf-8") == "evidence\n"
    assert system.revoked is True
    assert system.removed_members == list(host.OPERATORS)
    assert system.docker is False
    assert not installer.filesystem.exists(host.SERVICE_KEY)
    assert not installer.filesystem.exists(host.GENERATED_ROOT)
    assert not installer.filesystem.exists(host.INSTALL_RECORD)
    assert not installer.filesystem.exists(host.TRUST_REVOCATION_LEDGER)
    assert result["removed"][-2:] == [
        str(host.TRUST_REVOCATION_LEDGER),
        str(host.INSTALL_RECORD),
    ]
    assert system.events[-2:] == [
        "trust-ledger:revoke",
        "trust-ledger:finalize-check",
    ]


def test_uninstall_remote_revocation_failure_retains_key_tool_record_and_ledger(
    tmp_path: Path,
) -> None:
    installer, system = _installer(tmp_path)
    installer.install(TEAM_ID)
    pending_hosts = [f"trt-gb10-{number}" for number in range(1, 16)]
    system._write_trust_ledger(pending_hosts)
    system.status = "done"
    system.revoke_error = "injected remote revocation failure"

    with pytest.raises(host.InstallError, match="remote revocation failure"):
        installer.uninstall(retain_ledger=True)

    assert system._trust_ledger()["revocation_hosts"] == pending_hosts
    assert installer.filesystem.exists(host.SERVICE_KEY)
    assert installer.filesystem.exists(Path(str(host.SERVICE_KEY) + ".pub"))
    assert installer.filesystem.exists(host.TRUST_TOOL_PATH)
    assert installer.filesystem.exists(host.INSTALL_RECORD)
    assert not installer.filesystem.exists(host.SUDOERS_PATH)
    assert system.maintenance is True


def test_uninstall_rejects_unsafe_trust_ledger_without_removing_local_key(
    tmp_path: Path,
) -> None:
    installer, system = _installer(tmp_path)
    installer.install(TEAM_ID)
    installer.filesystem.path(host.TRUST_REVOCATION_LEDGER).chmod(0o644)
    system.status = "done"

    with pytest.raises(host.InstallError, match="ledger is invalid"):
        installer.uninstall(retain_ledger=True)

    assert installer.filesystem.exists(host.SERVICE_KEY)
    assert installer.filesystem.exists(host.TRUST_TOOL_PATH)
    assert installer.filesystem.exists(host.INSTALL_RECORD)
    assert installer.filesystem.exists(host.TRUST_REVOCATION_LEDGER)


def test_uninstall_retries_when_ledger_was_deleted_before_durability_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    installer, system = _installer(tmp_path)
    installer.install(TEAM_ID)
    system.status = "done"
    original_remove_ledger = host.LocalFilesystem.remove_validated_trust_ledger

    def remove_then_fail(filesystem: host.LocalFilesystem, *, expected_fingerprint: str) -> bool:
        original_remove_ledger(
            filesystem,
            expected_fingerprint=expected_fingerprint,
        )
        raise host.InstallError("injected ledger directory fsync failure")

    monkeypatch.setattr(
        host.LocalFilesystem,
        "remove_validated_trust_ledger",
        remove_then_fail,
    )
    with pytest.raises(host.InstallError, match="fsync failure"):
        installer.uninstall(retain_ledger=True)

    interrupted = installer.filesystem.load_install_record()
    assert interrupted is not None
    assert interrupted["installation_state"] == "uninstalling"
    assert interrupted["trust_requires_revocation"] is False
    assert interrupted["trust_ledger_removed"] is False
    assert not installer.filesystem.exists(host.TRUST_REVOCATION_LEDGER)

    monkeypatch.setattr(
        host.LocalFilesystem,
        "remove_validated_trust_ledger",
        original_remove_ledger,
    )
    result = installer.uninstall(retain_ledger=True)

    assert result["ok"] is True
    assert not installer.filesystem.exists(host.INSTALL_RECORD)


def test_uninstall_retries_after_install_record_removal_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    installer, system = _installer(tmp_path)
    installer.install(TEAM_ID)
    system.status = "done"
    original_remove = host.LocalFilesystem.remove
    fail_record_removal = True

    def injected_remove(filesystem: host.LocalFilesystem, absolute: Path) -> bool:
        nonlocal fail_record_removal
        if absolute == host.INSTALL_RECORD and fail_record_removal:
            fail_record_removal = False
            raise host.InstallError("injected install record removal failure")
        return original_remove(filesystem, absolute)

    monkeypatch.setattr(host.LocalFilesystem, "remove", injected_remove)
    with pytest.raises(host.InstallError, match="record removal failure"):
        installer.uninstall(retain_ledger=True)

    interrupted = installer.filesystem.load_install_record()
    assert interrupted is not None
    assert interrupted["installation_state"] == "uninstalling"
    assert interrupted["trust_requires_revocation"] is False
    assert interrupted["trust_ledger_removed"] is True
    assert not installer.filesystem.exists(host.TRUST_REVOCATION_LEDGER)
    with pytest.raises(host.InstallError, match="interrupted uninstall"):
        installer.install(TEAM_ID)

    result = installer.uninstall(retain_ledger=True)

    assert result["ok"] is True
    assert not installer.filesystem.exists(host.INSTALL_RECORD)


def test_trust_ledger_concurrent_replacement_is_reported_as_install_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    installer, system = _installer(tmp_path)
    installer.install(TEAM_ID)
    ledger_path = installer.filesystem.path(host.TRUST_REVOCATION_LEDGER)
    original_lstat = host.os.lstat
    ledger_lstats = 0

    def racing_lstat(path: os.PathLike[str] | str) -> os.stat_result:
        nonlocal ledger_lstats
        if Path(path) == ledger_path:
            ledger_lstats += 1
            if ledger_lstats == 2:
                raise FileNotFoundError("injected concurrent ledger replacement")
        return original_lstat(path)

    monkeypatch.setattr(host.os, "lstat", racing_lstat)

    with pytest.raises(host.InstallError, match="changed before removal"):
        installer.filesystem.remove_validated_trust_ledger(
            expected_fingerprint=system.public_key_fingerprint()
        )

    assert ledger_path.exists()


def test_uninstall_refuses_unmigrated_legacy_revocation_record(tmp_path: Path) -> None:
    installer, _ = _installer(tmp_path)
    installer.install(TEAM_ID)
    record = installer.filesystem.load_install_record()
    assert record is not None
    record["schema_version"] = 1
    record.pop("trust_ledger_migrated")
    installer.filesystem.atomic_write(
        host.INSTALL_RECORD,
        (json.dumps(record, sort_keys=True) + "\n").encode(),
        0o600,
    )

    with pytest.raises(host.InstallError, match="ledger migration"):
        installer.uninstall(retain_ledger=True)

    assert installer.filesystem.exists(host.SUDOERS_PATH)
    assert installer.filesystem.exists(host.SERVICE_KEY)
    assert installer.filesystem.exists(host.INSTALL_RECORD)


def test_uninstall_removes_only_acls_recorded_as_added(tmp_path: Path) -> None:
    installer, system = _installer(tmp_path)
    preexisting = host.PROTECTED_INPUTS[0]
    system.input_acls.add(preexisting)

    installer.install(TEAM_ID)
    system.status = "done"
    installer.uninstall(retain_ledger=True)

    assert preexisting in system.input_acls


def test_uninstall_preserves_preexisting_membership_linger_docker_and_key(
    tmp_path: Path,
) -> None:
    installer, system = _installer(tmp_path)
    system.operator_members.add("hongjian")
    system.docker = True
    system.linger = True
    system.key = True
    installer.filesystem.atomic_write(host.SERVICE_KEY, b"private-key-fixture\n", 0o600)
    installer.filesystem.atomic_write(
        Path(str(host.SERVICE_KEY) + ".pub"), b"ssh-ed25519 public-fixture\n", 0o644
    )

    installer.install(TEAM_ID)
    system.status = "done"
    installer.uninstall(retain_ledger=True)

    assert system.operator_members == {"hongjian"}
    assert system.removed_members == ["qianyi", "devansh"]
    assert system.docker is True
    assert system.linger is True
    assert installer.filesystem.exists(host.SERVICE_KEY)
    assert installer.filesystem.exists(Path(str(host.SERVICE_KEY) + ".pub"))


def test_uninstall_requires_explicit_ledger_retention(tmp_path: Path) -> None:
    installer, _ = _installer(tmp_path)
    with pytest.raises(host.InstallError, match="retain-ledger"):
        installer.uninstall(retain_ledger=False)


def test_cli_rejects_repository_ref_and_host_overrides() -> None:
    parser = host._parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["install", "--smoke-on-behalf-team-id", TEAM_ID, "--ref", "dev"])
    with pytest.raises(SystemExit):
        parser.parse_args(["plan", "--host", "example"])


@pytest.mark.parametrize("version", ["3.11\n", "3.12\n"])
def test_system_python_version_probe_accepts_supported_python(version: str) -> None:
    class VersionRunner:
        def __init__(self) -> None:
            self.calls: list[list[str]] = []

        def run(self, argv, **kwargs):  # type: ignore[no-untyped-def]
            assert kwargs == {"check": False}
            self.calls.append(list(argv))
            return host.CommandResult(0, version)

    runner = VersionRunner()
    python = Path("/usr/bin/python3.12")

    host.HostSystem(runner)._validate_system_python_version(python)

    assert runner.calls == [
        [
            str(python),
            "-I",
            "-S",
            "-c",
            "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')",
        ]
    ]


@pytest.mark.parametrize(
    ("returncode", "version"),
    [(0, "3.10\n"), (0, "4.0\n"), (0, "3.12 extra\n"), (0, ""), (1, "3.12\n")],
)
def test_system_python_version_probe_fails_closed(returncode: int, version: str) -> None:
    class VersionRunner:
        def run(self, argv, **kwargs):  # type: ignore[no-untyped-def]
            del argv
            assert kwargs == {"check": False}
            return host.CommandResult(returncode, version)

    with pytest.raises(host.InstallError, match="Python"):
        host.HostSystem(VersionRunner())._validate_system_python_version(
            Path("/usr/bin/python3.12")
        )


def test_sync_venv_uses_fixed_root_tools_and_candidate_constraints(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []

    class SyncRunner:
        def __init__(self) -> None:
            self.calls: list[tuple[list[str], dict[str, object]]] = []

        def run(self, argv, **kwargs):  # type: ignore[no-untyped-def]
            call = list(argv)
            self.calls.append((call, kwargs))
            if call[1:3] == ["-I", "-S"]:
                return host.CommandResult(0, "3.12\n")
            events.append("uv-sync")
            return host.CommandResult(0)

    resolved = {
        host.SYSTEM_PYTHON: Path("/usr/bin/python3.12"),
        host.UV_BINARY: Path("/usr/local/bin/uv"),
    }
    monkeypatch.setattr(
        host,
        "_safe_root_executable",
        lambda path, *, label: resolved[path],
    )
    runner = SyncRunner()
    system = host.HostSystem(runner)
    monkeypatch.setattr(system, "harden_venv_lock", lambda: events.append("harden-lock"))
    monkeypatch.setattr(
        system,
        "venv_ready",
        lambda: events.append("validate-authority") or True,
    )
    source_root = Path("/opt/loom-staging-runner/source")

    system.sync_venv(source_root)

    sync_call, sync_kwargs = runner.calls[-1]
    assert sync_call == [
        "/usr/local/bin/uv",
        "sync",
        "--project",
        str(source_root),
        "--no-editable",
        "--extra",
        "cluster",
        "--extra",
        "rollout",
        "--python",
        "/usr/bin/python3.12",
    ]
    assert "--frozen" not in sync_call
    assert sync_kwargs["env"] == {
        "PATH": host._ROOT_PATH,
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "UV_PROJECT_ENVIRONMENT": str(host.VENV),
    }
    assert events == ["uv-sync", "harden-lock", "validate-authority"]


def test_venv_lock_hardening_converges_root_regular_file_to_mode_0600(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mode = 0o666
    descriptor = 41
    identity = (17, 23)
    calls: list[tuple[object, ...]] = []

    def metadata(current_mode: int) -> os.stat_result:
        return os.stat_result(
            (stat.S_IFREG | current_mode, identity[1], identity[0], 1, 0, 0, 0, 0, 0, 0)
        )

    def fake_open(path: Path, flags: int) -> int:
        calls.append(("open", path, flags))
        return descriptor

    def fake_fchmod(fd: int, requested_mode: int) -> None:
        nonlocal mode
        calls.append(("fchmod", fd, requested_mode))
        mode = requested_mode

    def fake_lstat(path: Path) -> os.stat_result:
        if path == host.VENV:
            return os.stat_result((stat.S_IFDIR | 0o755, 11, 7, 1, 0, 0, 0, 0, 0, 0))
        assert path == host.VENV / ".lock"
        return metadata(mode)

    monkeypatch.setattr(host.os, "lstat", fake_lstat)
    monkeypatch.setattr(host.os, "open", fake_open)
    monkeypatch.setattr(host.os, "fstat", lambda fd: metadata(mode))
    monkeypatch.setattr(host.os, "fchmod", fake_fchmod)
    monkeypatch.setattr(host.os, "close", lambda fd: calls.append(("close", fd)))

    host.HostSystem(RecordingRunner()).harden_venv_lock()

    assert calls[0][0:2] == ("open", host.VENV / ".lock")
    assert calls[0][2] & getattr(os, "O_NOFOLLOW", 0)
    assert calls[1:] == [("fchmod", descriptor, 0o600), ("close", descriptor)]


@pytest.mark.parametrize(
    ("unsafe_mode", "unsafe_uid", "unsafe_gid"),
    [
        (stat.S_IFLNK | 0o777, 0, 0),
        (stat.S_IFREG | 0o600, 1000, 0),
        (stat.S_IFREG | 0o600, 0, 1000),
        (stat.S_IFIFO | 0o600, 0, 0),
    ],
)
def test_venv_lock_hardening_rejects_unsafe_authority_before_open(
    monkeypatch: pytest.MonkeyPatch,
    unsafe_mode: int,
    unsafe_uid: int,
    unsafe_gid: int,
) -> None:
    metadata = os.stat_result((unsafe_mode, 23, 17, 1, unsafe_uid, unsafe_gid, 0, 0, 0, 0))

    def fake_lstat(path: Path) -> os.stat_result:
        if path == host.VENV:
            return os.stat_result((stat.S_IFDIR | 0o755, 11, 7, 1, 0, 0, 0, 0, 0, 0))
        assert path == host.VENV / ".lock"
        return metadata

    monkeypatch.setattr(host.os, "lstat", fake_lstat)
    monkeypatch.setattr(
        host.os,
        "open",
        lambda path, flags: pytest.fail("unsafe lock must be rejected before open"),
    )

    with pytest.raises(host.InstallError, match="lock authority is unsafe"):
        host.HostSystem(RecordingRunner()).harden_venv_lock()


def test_venv_lock_hardening_rejects_identity_change_after_open(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = os.stat_result((stat.S_IFDIR | 0o755, 11, 7, 1, 0, 0, 0, 0, 0, 0))
    before = os.stat_result((stat.S_IFREG | 0o666, 23, 17, 1, 0, 0, 0, 0, 0, 0))
    after = os.stat_result((stat.S_IFREG | 0o666, 24, 17, 1, 0, 0, 0, 0, 0, 0))
    closed: list[int] = []
    monkeypatch.setattr(host.os, "lstat", lambda path: root if path == host.VENV else before)
    monkeypatch.setattr(host.os, "open", lambda path, flags: 41)
    monkeypatch.setattr(host.os, "fstat", lambda fd: after)
    monkeypatch.setattr(host.os, "close", lambda fd: closed.append(fd))

    with pytest.raises(host.InstallError, match="lock authority is unsafe"):
        host.HostSystem(RecordingRunner()).harden_venv_lock()

    assert closed == [41]


def test_venv_lock_hardening_fails_if_mode_does_not_converge(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = os.stat_result((stat.S_IFDIR | 0o755, 11, 7, 1, 0, 0, 0, 0, 0, 0))
    lock = os.stat_result((stat.S_IFREG | 0o666, 23, 17, 1, 0, 0, 0, 0, 0, 0))
    monkeypatch.setattr(host.os, "lstat", lambda path: root if path == host.VENV else lock)
    monkeypatch.setattr(host.os, "open", lambda path, flags: 41)
    monkeypatch.setattr(host.os, "fstat", lambda fd: lock)
    monkeypatch.setattr(host.os, "fchmod", lambda fd, mode: None)
    monkeypatch.setattr(host.os, "close", lambda fd: None)

    with pytest.raises(host.InstallError, match="hardening did not converge"):
        host.HostSystem(RecordingRunner()).harden_venv_lock()


def test_venv_lock_hardening_converts_fchmod_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = os.stat_result((stat.S_IFDIR | 0o755, 11, 7, 1, 0, 0, 0, 0, 0, 0))
    lock = os.stat_result((stat.S_IFREG | 0o666, 23, 17, 1, 0, 0, 0, 0, 0, 0))
    monkeypatch.setattr(host.os, "lstat", lambda path: root if path == host.VENV else lock)
    monkeypatch.setattr(host.os, "open", lambda path, flags: 41)
    monkeypatch.setattr(host.os, "fstat", lambda fd: lock)
    monkeypatch.setattr(
        host.os,
        "fchmod",
        lambda fd, mode: (_ for _ in ()).throw(OSError("injected chmod failure")),
    )
    monkeypatch.setattr(host.os, "close", lambda fd: None)

    with pytest.raises(host.InstallError, match="lock hardening failed"):
        host.HostSystem(RecordingRunner()).harden_venv_lock()


def test_venv_lock_hardening_converts_close_failure_without_masking_authority_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = os.stat_result((stat.S_IFDIR | 0o755, 11, 7, 1, 0, 0, 0, 0, 0, 0))
    before = os.stat_result((stat.S_IFREG | 0o600, 23, 17, 1, 0, 0, 0, 0, 0, 0))
    changed = os.stat_result((stat.S_IFREG | 0o600, 24, 17, 1, 0, 0, 0, 0, 0, 0))
    monkeypatch.setattr(host.os, "lstat", lambda path: root if path == host.VENV else before)
    monkeypatch.setattr(host.os, "open", lambda path, flags: 41)
    monkeypatch.setattr(host.os, "close", lambda fd: (_ for _ in ()).throw(OSError("close")))

    monkeypatch.setattr(host.os, "fstat", lambda fd: before)
    with pytest.raises(host.InstallError, match="lock close failed"):
        host.HostSystem(RecordingRunner()).harden_venv_lock()

    monkeypatch.setattr(host.os, "fstat", lambda fd: changed)
    with pytest.raises(host.InstallError, match="lock authority is unsafe"):
        host.HostSystem(RecordingRunner()).harden_venv_lock()


def test_verify_user_manager_places_clean_connectivity_probe_inside_sudo() -> None:
    class UserManagerRunner:
        def __init__(self) -> None:
            self.calls: list[tuple[list[str], dict[str, object]]] = []

        def run(self, argv, **kwargs):  # type: ignore[no-untyped-def]
            call = list(argv)
            self.calls.append((call, kwargs))
            if call == ["id", "-u", host.SERVICE_USER]:
                return host.CommandResult(0, "1001\n")
            return host.CommandResult(0, "255.4-1ubuntu8.14\n")

    runner = UserManagerRunner()

    host.HostSystem(runner).verify_user_manager()

    assert runner.calls == [
        (["id", "-u", host.SERVICE_USER], {}),
        (
            [
                "sudo",
                "-n",
                "-u",
                host.SERVICE_USER,
                "--",
                "/usr/bin/env",
                "-i",
                "XDG_RUNTIME_DIR=/run/user/1001",
                "DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/1001/bus",
                "PATH=/usr/bin:/bin",
                "/usr/bin/systemctl",
                "--user",
                "show",
                "--property=Version",
                "--value",
            ],
            {},
        ),
    ]


@pytest.mark.parametrize("version", ["", "degraded\n", "255\nunexpected\n"])
def test_verify_user_manager_rejects_invalid_manager_version(version: str) -> None:
    class UserManagerRunner:
        def run(self, argv, **kwargs):  # type: ignore[no-untyped-def]
            del kwargs
            if list(argv) == ["id", "-u", host.SERVICE_USER]:
                return host.CommandResult(0, "1001\n")
            return host.CommandResult(0, version)

    with pytest.raises(host.InstallError, match="manager version is invalid"):
        host.HostSystem(UserManagerRunner()).verify_user_manager()


def _root_directory_metadata(*, mode: int = 0o700, uid: int = 0) -> os.stat_result:
    return os.stat_result((stat.S_IFDIR | mode, 11, 7, 1, uid, 0, 0, 0, 0, 0))


def _root_kubeconfig_metadata(
    payload: bytes, *, mode: int = 0o600, uid: int = 0, nlink: int = 1
) -> os.stat_result:
    return os.stat_result((stat.S_IFREG | mode, 23, 17, nlink, uid, 0, len(payload), 0, 0, 0))


def test_read_root_kubeconfig_binds_authority_to_one_descriptor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = b"apiVersion: v1\ncurrent-context: loom-staging\n"
    metadata = _root_kubeconfig_metadata(payload)
    reads = iter((payload, b""))
    opened: list[tuple[Path, int]] = []
    closed: list[int] = []
    monkeypatch.setattr(host.os, "lstat", lambda path: _root_directory_metadata())
    monkeypatch.setattr(
        host.os,
        "open",
        lambda path, flags: opened.append((path, flags)) or 41,
    )
    monkeypatch.setattr(host.os, "fstat", lambda fd: metadata)
    monkeypatch.setattr(host.os, "read", lambda fd, size: next(reads))
    monkeypatch.setattr(host.os, "close", lambda fd: closed.append(fd))

    assert host._read_root_kubeconfig_source() == payload
    assert opened[0][0] == host.ROOT_KUBECONFIG
    assert opened[0][1] & getattr(os, "O_NOFOLLOW", 0)
    assert closed == [41]


@pytest.mark.parametrize(
    "metadata",
    [
        _root_directory_metadata(mode=0o720),
        _root_directory_metadata(uid=1000),
        os.stat_result((stat.S_IFLNK | 0o777, 11, 7, 1, 0, 0, 0, 0, 0, 0)),
    ],
)
def test_read_root_kubeconfig_rejects_unsafe_parent(
    monkeypatch: pytest.MonkeyPatch, metadata: os.stat_result
) -> None:
    monkeypatch.setattr(
        host.os,
        "lstat",
        lambda path: (
            metadata if path == host.ROOT_KUBECONFIG.parent else _root_directory_metadata()
        ),
    )

    with pytest.raises(host.InstallError, match="parent authority is unsafe"):
        host._read_root_kubeconfig_source()


@pytest.mark.parametrize(
    "metadata",
    [
        _root_kubeconfig_metadata(b"payload", mode=0o640),
        _root_kubeconfig_metadata(b"payload", uid=1000),
        _root_kubeconfig_metadata(b"payload", nlink=2),
        os.stat_result((stat.S_IFDIR | 0o600, 23, 17, 1, 0, 0, 7, 0, 0, 0)),
    ],
)
def test_read_root_kubeconfig_rejects_unsafe_source_metadata(
    monkeypatch: pytest.MonkeyPatch, metadata: os.stat_result
) -> None:
    monkeypatch.setattr(host.os, "lstat", lambda path: _root_directory_metadata())
    monkeypatch.setattr(host.os, "open", lambda path, flags: 41)
    monkeypatch.setattr(host.os, "fstat", lambda fd: metadata)
    monkeypatch.setattr(host.os, "close", lambda fd: None)

    with pytest.raises(host.InstallError, match="source metadata is unsafe"):
        host._read_root_kubeconfig_source()


def test_read_root_kubeconfig_rejects_descriptor_change(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = b"apiVersion: v1\n"
    before = _root_kubeconfig_metadata(payload)
    after = os.stat_result(
        (
            before.st_mode,
            before.st_ino + 1,
            before.st_dev,
            before.st_nlink,
            before.st_uid,
            before.st_gid,
            before.st_size,
            0,
            0,
            0,
        )
    )
    metadata = iter((before, after))
    reads = iter((payload, b""))
    monkeypatch.setattr(host.os, "lstat", lambda path: _root_directory_metadata())
    monkeypatch.setattr(host.os, "open", lambda path, flags: 41)
    monkeypatch.setattr(host.os, "fstat", lambda fd: next(metadata))
    monkeypatch.setattr(host.os, "read", lambda fd, size: next(reads))
    monkeypatch.setattr(host.os, "close", lambda fd: None)

    with pytest.raises(host.InstallError, match="changed during read"):
        host._read_root_kubeconfig_source()


def test_export_kubeconfig_uses_process_private_snapshot(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    source = b"apiVersion: v1\ncurrent-context: loom-staging\n"
    snapshot_path: Path | None = None

    class KubeconfigRunner:
        def run(self, argv, **kwargs):  # type: ignore[no-untyped-def]
            nonlocal snapshot_path
            call = list(argv)
            assert kwargs == {}
            assert call[0:2] == ["kubectl", "--kubeconfig"]
            snapshot_path = Path(call[2])
            assert snapshot_path.read_bytes() == source
            assert stat.S_IMODE(snapshot_path.stat().st_mode) == 0o600
            assert call[3:] == [
                "config",
                "view",
                "--raw",
                "--minify",
                "--context",
                "loom-staging",
            ]
            return host.CommandResult(0, source.decode())

    monkeypatch.setattr(host, "_read_root_kubeconfig_source", lambda: source)
    monkeypatch.setattr(host, "ROOT_KUBECONFIG_SNAPSHOT_PARENT", tmp_path)

    assert host.HostSystem(KubeconfigRunner()).export_kubeconfig() == source
    assert snapshot_path is not None
    assert not snapshot_path.exists()


def test_installer_fixes_system_python_and_uv_authority_paths() -> None:
    assert host.SYSTEM_PYTHON == Path("/usr/bin/python3")
    assert host.UV_BINARY == Path("/usr/local/bin/uv")


def test_venv_python_minor_drift_converges_through_resync(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class VenvRunner:
        def run(self, argv, **kwargs):  # type: ignore[no-untyped-def]
            call = list(argv)
            if call[1:3] == ["-I", "-S"]:
                assert kwargs == {"check": False}
                return host.CommandResult(0, "3.13\n")
            assert kwargs == {"check": False}
            assert call == ["stat", "-c", "%F:%U:%G:%a", str(host.VENV)]
            return host.CommandResult(0, "directory:root:root:755\n")

    resolved = {
        host.SYSTEM_PYTHON: Path("/usr/bin/python3.13"),
        host.VENV / "bin/python": Path("/usr/bin/python3.12"),
    }
    monkeypatch.setattr(
        host.os.path,
        "lexists",
        lambda path: path in {host.VENV, host.VENV / "bin/python"},
    )
    monkeypatch.setattr(
        host,
        "_safe_root_executable",
        lambda path, *, label: resolved[path],
    )
    monkeypatch.setattr(
        host,
        "_validate_owned_tree",
        lambda root, *, expected_uid, expected_gid, allowed_external_symlink_targets: None,
    )

    assert host.HostSystem(VenvRunner()).venv_ready() is False


def test_dangling_venv_python_converges_only_after_tree_validation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class VenvRunner:
        def run(self, argv, **kwargs):  # type: ignore[no-untyped-def]
            call = list(argv)
            if call[1:3] == ["-I", "-S"]:
                assert kwargs == {"check": False}
                return host.CommandResult(0, "3.12\n")
            assert kwargs == {"check": False}
            return host.CommandResult(0, "directory:root:root:755\n")

    python = host.VENV / "bin/python"
    tree_validated = False

    def validate_tree(*args, **kwargs):  # type: ignore[no-untyped-def]
        nonlocal tree_validated
        del args, kwargs
        tree_validated = True

    def resolve_executable(path: Path, *, label: str) -> Path:
        del label
        if path == host.SYSTEM_PYTHON:
            return Path("/usr/bin/python3.12")
        assert path == python
        assert tree_validated
        raise host.InstallError("dangling venv Python")

    monkeypatch.setattr(host.os.path, "lexists", lambda path: path in {host.VENV, python})
    monkeypatch.setattr(host, "_safe_root_executable", resolve_executable)
    monkeypatch.setattr(host, "_validate_owned_tree", validate_tree)

    assert host.HostSystem(VenvRunner()).venv_ready() is False
    assert tree_validated


def test_owned_tree_allows_only_an_exact_dangling_external_target(tmp_path: Path) -> None:
    uid, gid = os.geteuid(), os.getegid()
    root = tmp_path / "venv"
    bindir = root / "bin"
    bindir.mkdir(parents=True, mode=0o755)
    python = bindir / "python"
    target = Path("/usr/bin/python3.99")
    python.symlink_to(target)

    host._validate_owned_tree(
        root,
        expected_uid=uid,
        expected_gid=gid,
        allowed_external_symlink_targets=(target,),
    )
    with pytest.raises(host.InstallError, match="escapes"):
        host._validate_owned_tree(root, expected_uid=uid, expected_gid=gid)


def test_venv_python_link_rejects_non_system_python_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    venv = tmp_path / "venv"
    bindir = venv / "bin"
    bindir.mkdir(parents=True)
    (bindir / "python").symlink_to("/usr/local/bin/python3.12")

    class VenvRunner:
        def run(self, argv, **kwargs):  # type: ignore[no-untyped-def]
            call = list(argv)
            if call[1:3] == ["-I", "-S"]:
                assert kwargs == {"check": False}
                return host.CommandResult(0, "3.12\n")
            assert kwargs == {"check": False}
            return host.CommandResult(0, "directory:root:root:755\n")

    monkeypatch.setattr(host, "VENV", venv)
    monkeypatch.setattr(
        host,
        "_safe_root_executable",
        lambda path, *, label: Path("/usr/bin/python3.12"),
    )

    with pytest.raises(host.InstallError, match="link is unsafe"):
        host.HostSystem(VenvRunner()).venv_ready()


class RecordingRunner:
    def __init__(self) -> None:
        self.calls: list[list[str]] = []
        self.acls: dict[str, set[str]] = {}

    def run(self, argv, **kwargs):  # type: ignore[no-untyped-def]
        del kwargs
        call = list(argv)
        self.calls.append(call)
        if call[0] == "getfacl":
            entries = sorted(self.acls.get(call[-1], set()))
            return host.CommandResult(
                0,
                "\n".join(["user::rwx", *entries, "group::r-x", "mask::rwx", "other::---"]) + "\n",
            )
        if call[:3] == ["setfacl", "-n", "-m"]:
            raw = call[3]
            if raw.startswith("d:u:"):
                _, _, username, permissions = raw.split(":")
                entry = f"default:user:{username}:{permissions}"
            else:
                _, username, permissions = raw.split(":")
                entry = f"user:{username}:{permissions}"
            self.acls.setdefault(call[-1], set()).add(entry)
        return host.CommandResult(0)


class NoopSourceRunner:
    def __init__(
        self,
        source: Path,
        runner_root: Path,
        sha: str,
        *,
        head_sha: str | None = None,
    ) -> None:
        self.source = source
        self.runner_root = runner_root
        self.sha = sha
        self.head_sha = head_sha or sha
        self.calls: list[list[str]] = []

    def run(self, argv, **kwargs):  # type: ignore[no-untyped-def]
        del kwargs
        call = list(argv)
        self.calls.append(call)
        if call[:3] == ["stat", "-c", "%F:%U:%G:%a"]:
            return host.CommandResult(0, "directory:root:root:755\n")
        if call[:2] == ["test", "-d"]:
            return host.CommandResult(0 if call[-1] == str(self.source / ".git") else 1)
        if call[:4] == ["git", "-C", str(self.source), "remote"]:
            return host.CommandResult(0, "origin\n")
        if call[:5] == ["git", "-C", str(self.source), "config", "--get-all"]:
            if call[-1] == "remote.origin.url":
                return host.CommandResult(0, host.REMOTE_URL + "\n")
            return host.CommandResult(1)
        if call[:5] == ["git", "-C", str(self.source), "status", "--porcelain=v1"]:
            return host.CommandResult(0, "")
        if call[:2] == ["git", "ls-remote"]:
            return host.CommandResult(0, f"{self.sha}\t{host.FETCH_REF}\n")
        if call == ["git", "-C", str(self.source), "rev-parse", "HEAD"]:
            return host.CommandResult(0, self.head_sha + "\n")
        if call[:4] == ["git", "-C", str(self.source), "fetch"]:
            return host.CommandResult(0)
        raise AssertionError(f"unexpected command: {call}")


def test_acl_convergence_uses_named_no_mask_entry_without_owner_or_mode_changes() -> None:
    runner = RecordingRunner()
    system = host.HostSystem(runner)

    grants = system.ensure_input_acl(host.PROTECTED_INPUTS[0])

    assert grants
    flattened = [argument for call in runner.calls for argument in call]
    assert "chown" not in flattened
    assert "chmod" not in flattened
    assert any(call[:4] == ["setfacl", "-n", "-m", "u:loom-rollout:r--"] for call in runner.calls)
    assert all("admin-token-fixture" not in " ".join(call) for call in runner.calls)


def test_unchanged_root_source_performs_no_clone_fetch_checkout_or_install(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner_root = tmp_path / "runner"
    source = runner_root / "source"
    (source / ".git").mkdir(parents=True)
    sha = "a" * 40
    runner = NoopSourceRunner(source, runner_root, sha)
    monkeypatch.setattr(host, "RUNNER_ROOT", runner_root)
    monkeypatch.setattr(host, "INSTALL_SOURCE", source)
    monkeypatch.setattr(host, "_validate_owned_tree", lambda *args, **kwargs: None)

    assert host.HostSystem(runner).prepare_install_source() == (source, sha)

    flattened = [argument for call in runner.calls for argument in call]
    assert "clone" not in flattened
    assert "fetch" not in flattened
    assert "checkout" not in flattened
    assert "install" not in flattened


def test_new_root_source_is_fetched_but_not_checked_out_before_admission_gate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner_root = tmp_path / "runner"
    source = runner_root / "source"
    (source / ".git").mkdir(parents=True)
    old_sha = "a" * 40
    new_sha = "b" * 40
    runner = NoopSourceRunner(source, runner_root, new_sha, head_sha=old_sha)
    monkeypatch.setattr(host, "RUNNER_ROOT", runner_root)
    monkeypatch.setattr(host, "INSTALL_SOURCE", source)
    monkeypatch.setattr(host, "_validate_owned_tree", lambda *args, **kwargs: None)

    assert host.HostSystem(runner).prepare_install_source() == (source, new_sha)
    assert runner.head_sha == old_sha

    assert any(call[:4] == ["git", "-C", str(source), "fetch"] for call in runner.calls)
    assert all("checkout" not in call for call in runner.calls)


def test_dirty_invocation_checkout_is_rejected_before_source_install() -> None:
    class DirtyRunner:
        def run(self, argv, **kwargs):  # type: ignore[no-untyped-def]
            del kwargs
            call = list(argv)
            if call[-1] == "remote":
                return host.CommandResult(0, "origin\n")
            if call[-1] == "remote.origin.url":
                return host.CommandResult(0, host.REMOTE_URL + "\n")
            if call[-1] == "remote.origin.pushurl":
                return host.CommandResult(1)
            if "status" in call:
                return host.CommandResult(0, " M deploy/staging-rollout/staging-rollout.toml\n")
            raise AssertionError(f"unexpected command: {call}")

    with pytest.raises(host.InstallError, match="dirty"):
        host.HostSystem(DirtyRunner()).validate_invocation_checkout()


def test_source_asset_read_is_bound_to_exact_git_object() -> None:
    sha = "a" * 40
    relative = "deploy/staging-rollout/loom-staging-rollout"

    class GitShowRunner:
        def run(self, argv, **kwargs):  # type: ignore[no-untyped-def]
            del kwargs
            assert list(argv) == [
                "git",
                "-C",
                str(host.INSTALL_SOURCE),
                "show",
                f"{sha}:{relative}",
            ]
            return host.CommandResult(0, "asset-from-object\n")

    payload = host.HostSystem(GitShowRunner()).source_file(host.INSTALL_SOURCE, sha, relative)

    assert payload == b"asset-from-object\n"


def test_acl_convergence_preserves_stronger_preexisting_entry() -> None:
    runner = RecordingRunner()
    source = host.PROTECTED_INPUTS[0]
    for parent in source.parents:
        if parent == Path("/"):
            continue
        runner.acls[str(parent)] = {"user:loom-rollout:r-x"}
        if parent == Path("/shared_work"):
            break
    runner.acls[str(source)] = {"user:loom-rollout:rw-"}

    grants = host.HostSystem(runner).ensure_input_acl(source)

    assert grants == ()
    assert all(call[0] != "setfacl" for call in runner.calls)


def test_root_authority_directory_converges_owner_group_and_mode() -> None:
    class RootDirectoryRunner:
        def __init__(self) -> None:
            self.metadata = "directory:operator:operator:755"
            self.calls: list[list[str]] = []

        def run(self, argv, **kwargs):  # type: ignore[no-untyped-def]
            del kwargs
            call = list(argv)
            self.calls.append(call)
            if call[:3] == ["stat", "-c", "%F:%U:%G:%a"]:
                return host.CommandResult(0, self.metadata + "\n")
            if call[:2] == ["test", "-L"]:
                return host.CommandResult(1)
            if call[0] == "install":
                self.metadata = "directory:root:root:755"
                return host.CommandResult(0)
            raise AssertionError(f"unexpected command: {call}")

    runner = RootDirectoryRunner()

    assert host.HostSystem(runner).ensure_root_directory(Path("/etc/loom"), mode=0o755)
    assert any(
        call[:7] == ["install", "-d", "-o", "root", "-g", "root", "-m"] for call in runner.calls
    )


def test_plan_json_contains_only_fixed_scope(tmp_path: Path) -> None:
    installer, _ = _installer(tmp_path)
    payload = json.dumps(installer.plan(), sort_keys=True)
    assert host.REMOTE_URL in payload
    assert host.FETCH_REF in payload
    assert "--ref" not in payload
    assert "admin-token-fixture" not in payload


def test_maintenance_marker_transition_is_locked_and_idempotent(tmp_path: Path) -> None:
    runtime = tmp_path / "runtime"
    runtime.mkdir(mode=0o700)
    uid, gid = os.geteuid(), os.getegid()

    host._maintenance_marker(
        runtime,
        service_uid=uid,
        service_gid=gid,
        authority_uid=uid,
        authority_gid=gid,
        enabled=True,
    )
    host._maintenance_marker(
        runtime,
        service_uid=uid,
        service_gid=gid,
        authority_uid=uid,
        authority_gid=gid,
        enabled=True,
    )
    assert (runtime / "maintenance").is_file()

    host._maintenance_marker(
        runtime,
        service_uid=uid,
        service_gid=gid,
        authority_uid=uid,
        authority_gid=gid,
        enabled=False,
    )
    assert not (runtime / "maintenance").exists()


def test_owned_tree_rejects_writable_descendant_and_escaping_symlink(tmp_path: Path) -> None:
    uid, gid = os.geteuid(), os.getegid()
    root = tmp_path / "authority"
    root.mkdir(mode=0o755)
    child = root / "module.py"
    child.write_text("pass\n", encoding="utf-8")
    child.chmod(0o644)
    host._validate_owned_tree(root, expected_uid=uid, expected_gid=gid)

    child.chmod(0o664)
    with pytest.raises(host.InstallError, match="group/world writable"):
        host._validate_owned_tree(root, expected_uid=uid, expected_gid=gid)
    child.chmod(0o644)
    (root / "escape").symlink_to(tmp_path / "outside")
    with pytest.raises(host.InstallError, match="escapes"):
        host._validate_owned_tree(root, expected_uid=uid, expected_gid=gid)
