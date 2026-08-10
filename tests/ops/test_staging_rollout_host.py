from __future__ import annotations

import contextlib
import hashlib
import json
import os
import stat
import threading
from pathlib import Path
from typing import ClassVar

import pytest
from scripts.ops import staging_rollout_host as host

from loom_cli.rollout.install_attestation import RunnerInstallAttestation

TEAM_ID = "11111111-1111-4111-8111-111111111111"
TEAM_ID_2 = "22222222-2222-4222-8222-222222222222"
SERVICE_FINGERPRINT = "SHA256:6JjXfjyF6JMXDB2Wp4t1YgAzFJPaTv5mQJaqodL6GdU"
OTHER_SERVICE_FINGERPRINT = "SHA256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
TEST_CANDIDATE_SHA = "a" * 40
TEST_CANDIDATE_VENV = host._candidate_venv_path(TEST_CANDIDATE_SHA)


class FakeSystem:
    def __init__(self, filesystem: host.LocalFilesystem) -> None:
        self.filesystem = filesystem
        self.group = False
        self.service_user = False
        self.service_user_requires_convergence = False
        self.operator_members: set[str] = set()
        self.docker = False
        self.key = False
        self.private_key_fingerprint = SERVICE_FINGERPRINT
        self.public_key_fingerprint_value = SERVICE_FINGERPRINT
        self.service_key_generations = 0
        self.input_acls: set[Path] = set()
        self.data_acls: set[Path] = set()
        self.acl_adjustment_states: dict[host.AclGrant, str] = {}
        self.linger = False
        self.status = "idle"
        self.validated = 0
        self.candidate_syncs = 0
        self.sync_safety_snapshots: list[tuple[bool, bool]] = []
        self.candidate_sha: str | None = None
        self.revoked = False
        self.revoke_error: str | None = None
        self.ledger_modes: list[str] = []
        self.ledger_previous_source_shas: list[str | None] = []
        self.previous_topology_drift = False
        self.lifecycle_lock_entries = 0
        self.lifecycle_lock_depth = 0
        self.events: list[str] = []
        self.removed_members: list[str] = []
        self.trust_ready = False
        self.preflights = 0
        self.venv = False
        self.package_ready = True
        self.broker_ready = True
        self.venv_lock_mode: int | None = None
        self.venv_lock_hardenings = 0
        self.admission_disabled_at_status = False
        self.maintenance = False
        self.maintenance_begins = 0
        self.maintenance_ends = 0
        self.source_reads: list[str] = []
        self.remote_source_sha = "a" * 40
        self.install_source_sha: str | None = None
        self.install_owner_calls: list[tuple[Path, str, str, int]] = []
        self.shared_worker_identity_ready = True
        self.shared_work2_mounted = False
        self.preflight_credentials = False
        self.credential_refresh_timer = False
        self.preflight_candidate_source = False
        self.inotify_capacity = False
        self.runtime_venvs: set[Path] = set()

    def _observe_runtime_venv(self, venv: Path) -> None:
        self.runtime_venvs.add(venv)

    def validate_prerequisites(self) -> None:
        self.validated += 1

    @contextlib.contextmanager
    def trust_lifecycle_lock(self):  # type: ignore[no-untyped-def]
        self.lifecycle_lock_entries += 1
        self.lifecycle_lock_depth += 1
        assert self.lifecycle_lock_depth == 1
        try:
            yield
        finally:
            self.lifecycle_lock_depth -= 1

    def validate_invocation_checkout(self) -> str:
        self.validated += 1
        return "a" * 40

    def prepare_install_source(self) -> tuple[Path, str]:
        self.validated += 1
        if self.install_source_sha is None:
            self.install_source_sha = self.remote_source_sha
        return host.REPO_ROOT, self.remote_source_sha

    def prepare_sealed_install_source(
        self,
        source: host.SealedSource,
    ) -> tuple[Path, str]:
        assert source.path == host.REPO_ROOT
        assert source.commit_sha == "a" * 40
        assert source.tree_sha == "b" * 40
        assert source.base_sha == "c" * 40
        self.validated += 1
        self.install_source_sha = source.commit_sha
        return host.INSTALL_SOURCE, source.commit_sha

    def validate_invocation_merged(self, invocation_head: str, source_sha: str) -> None:
        assert invocation_head == "a" * 40
        assert source_sha == self.remote_source_sha
        self.validated += 1

    def validate_assets(self, source_root: Path, source_sha: str) -> None:
        assert source_root in {host.REPO_ROOT, host.INSTALL_SOURCE}
        assert source_sha == self.remote_source_sha
        self.validated += 1

    def source_file(self, source_root: Path, source_sha: str, relative_path: str) -> bytes:
        assert source_root in {host.REPO_ROOT, host.INSTALL_SOURCE}
        assert source_sha in {"a" * 40, "b" * 40}
        self.source_reads.append(relative_path)
        return (host.REPO_ROOT / relative_path).read_bytes()

    def validate_installed_source(
        self,
        source_sha: str,
        *,
        require_checkout: bool,
        source_tree_sha: str | None = None,
        source_base_sha: str | None = None,
    ) -> None:
        assert source_sha in {"a" * 40, "b" * 40}
        if source_tree_sha is not None or source_base_sha is not None:
            assert source_tree_sha == "b" * 40
            assert source_base_sha == "c" * 40
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
        changed = not self.service_user or self.service_user_requires_convergence
        self.service_user = True
        self.service_user_requires_convergence = False
        return changed

    def service_user_present(self) -> bool:
        return self.service_user and not self.service_user_requires_convergence

    def service_user_convergence_needed(self) -> bool:
        return not self.service_user or self.service_user_requires_convergence

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

    def shared_worker_repo_identity(self) -> dict[str, object]:
        if (
            not self.service_user
            or not self.shared_worker_identity_ready
            or not self.shared_work2_mounted
        ):
            raise host.InstallError("shared worker repository identity is unavailable")
        identity: dict[str, object] = {
            "root": str(host.SHARED_WORKER_REPO_ROOT),
            "service_user": host.SERVICE_USER,
            "service_uid": 995,
            "service_primary_group": host.SERVICE_GROUP,
            "service_primary_gid": 982,
            "consumer_user": host.SHARED_WORK_CONSUMER,
            "consumer_uid": 2005,
            "shared_group": host.SHARED_WORK_GROUP,
            "shared_gid": 2007,
        }
        authority = self.filesystem.path(host.SHARED_WORKER_AUTHORITY_ROOT)
        repository = self.filesystem.path(host.SHARED_WORKER_REPO_ROOT)
        if authority.is_dir() and repository.is_dir():
            parent = authority
            parent_metadata = parent.stat()
            authority_metadata = authority.stat()
            repository_metadata = repository.stat()
            identity.update(
                {
                    "schema_version": 1,
                    "parent_mode": "2750",
                    "authority_mode": "2750",
                    "repository_mode": "2750",
                    "parent_device": parent_metadata.st_dev,
                    "parent_inode": parent_metadata.st_ino,
                    "authority_device": authority_metadata.st_dev,
                    "authority_inode": authority_metadata.st_ino,
                    "repository_device": repository_metadata.st_dev,
                    "repository_inode": repository_metadata.st_ino,
                    "service_capability": ("parent-writable;repository-writable-searchable"),
                    "consumer_capability": "repository-readable-searchable-not-writable",
                    "publication_capability": "private-mkdir-publish-verified",
                    "mount": {
                        key: value
                        for key, value in self.shared_work2_mount_identity().items()
                        if key not in {"mount_id", "parent_id", "device_major", "device_minor"}
                    },
                    "created": [],
                }
            )
        return identity

    def shared_worker_repo_root_ready(self) -> bool:
        self.shared_worker_repo_identity()
        return all(
            (mapped := self.filesystem.path(path)).is_dir()
            and not mapped.is_symlink()
            and stat.S_IMODE(mapped.stat().st_mode) == 0o2750
            for path in (host.SHARED_WORKER_AUTHORITY_ROOT, host.SHARED_WORKER_REPO_ROOT)
        )

    def ensure_shared_worker_repo_root(self) -> bool:
        self.shared_worker_repo_identity()
        changed = False
        for path in (host.SHARED_WORKER_AUTHORITY_ROOT, host.SHARED_WORKER_REPO_ROOT):
            changed = self.filesystem.ensure_directory(path, 0o2750) or changed
        return changed

    def shared_work2_mount_identity(self) -> dict[str, object]:
        if not self.shared_work2_mounted:
            raise host.InstallError("shared_work2 mount helper failed safely")
        return {
            "schema_version": 1,
            "mount_point": str(host.SHARED_WORK2_MOUNT_POINT),
            "source": "192.168.20.12:/shared_work2",
            "filesystem_type": "nfs4",
            "mount_id": 42,
            "parent_id": 1,
            "device_major": 0,
            "device_minor": 99,
            "mount_options": ["nodev", "noexec", "nosuid", "rw"],
            "super_options": [
                "hard",
                "proto=tcp",
                "retrans=2",
                "rw",
                "sec=sys",
                "timeo=600",
                "vers=4.2",
            ],
        }

    def shared_work2_mount_ready(self) -> bool:
        return self.shared_work2_mounted

    def ensure_shared_work2_mount(self) -> bool:
        changed = not self.shared_work2_mounted
        self.shared_work2_mounted = True
        self.filesystem.ensure_directory(host.SHARED_WORK2_MOUNT_POINT, 0o755)
        return changed

    def disable_shared_work2_mount(self) -> None:
        self.shared_work2_mounted = False

    def reload_systemd(self) -> None:
        return None

    def credential_refresh_timer_ready(self) -> bool:
        return self.credential_refresh_timer

    def ensure_credential_refresh_timer(self, *, reload_units: bool) -> bool:
        changed = reload_units or not self.credential_refresh_timer
        self.credential_refresh_timer = True
        return changed

    def disable_credential_refresh_timer(self) -> None:
        self.credential_refresh_timer = False

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

    def inotify_capacity_ready(self) -> bool:
        return self.inotify_capacity

    def ensure_inotify_capacity(self) -> bool:
        changed = not self.inotify_capacity
        self.inotify_capacity = True
        return changed

    def ensure_candidate(
        self,
        expected_sha: str,
        *,
        refresh: bool,
        source_tree_sha: str | None = None,
        source_base_sha: str | None = None,
    ) -> bool:
        if source_tree_sha is not None or source_base_sha is not None:
            assert source_tree_sha == "b" * 40
            assert source_base_sha == "c" * 40
        changed = refresh or self.candidate_sha != expected_sha
        if changed:
            self.candidate_syncs += 1
            self.candidate_sha = expected_sha
        return changed

    def candidate_ready(
        self,
        expected_sha: str,
        *,
        source_tree_sha: str | None = None,
        source_base_sha: str | None = None,
    ) -> bool:
        if source_tree_sha is not None or source_base_sha is not None:
            assert source_tree_sha == "b" * 40
            assert source_base_sha == "c" * 40
        return self.candidate_sha == expected_sha

    def venv_ready(self, venv: Path) -> bool:
        self._observe_runtime_venv(venv)
        if self.venv_lock_mode not in {None, 0o600}:
            raise host.InstallError("root venv authority is unsafe")
        return self.venv

    def venv_lock_requires_hardening(self, venv: Path) -> bool:
        self._observe_runtime_venv(venv)
        return self.venv_lock_mode is not None and self.venv_lock_mode != 0o600

    def harden_venv_lock(self, venv: Path) -> None:
        self._observe_runtime_venv(venv)
        assert self.venv_lock_mode is not None
        self.venv_lock_mode = 0o600
        self.venv_lock_hardenings += 1

    def sync_venv(
        self,
        source_root: Path,
        *,
        venv: Path,
    ) -> None:
        self._observe_runtime_venv(venv)
        assert source_root in {host.REPO_ROOT, host.INSTALL_SOURCE}
        self.sync_safety_snapshots.append(
            (self.maintenance, self.filesystem.exists(host.SUDOERS_PATH))
        )
        self.candidate_syncs += 1
        self.venv = True
        self.package_ready = True
        self.broker_ready = True
        self.venv_lock_mode = 0o600

    def broker_runtime_ready(self, venv: Path) -> bool:
        self._observe_runtime_venv(venv)
        return self.broker_ready

    def package_runtime_ready(self, venv: Path) -> bool:
        self._observe_runtime_venv(venv)
        return self.package_ready

    def preflight_credentials_ready(self, venv: Path) -> bool:
        self._observe_runtime_venv(venv)
        return self.preflight_credentials

    def ensure_preflight_credentials(
        self,
        team_id: str,
        *,
        venv: Path,
    ) -> bool:
        self._observe_runtime_venv(venv)
        assert team_id in {TEAM_ID, TEAM_ID_2}
        changed = not self.preflight_credentials
        self.preflight_credentials = True
        for path in (
            host.PREFLIGHT_CREDENTIAL_ROOT / "readonly-kubeconfig",
            host.PREFLIGHT_CREDENTIAL_ROOT / "readonly-probe-token",
            host.PREFLIGHT_CREDENTIAL_ROOT / "readonly-database.json",
            host.PREFLIGHT_CREDENTIAL_ROOT / "rehearsal-kubeconfig",
        ):
            self.filesystem.atomic_write(path, b"credential-fixture\n", 0o600)
        return changed

    def preflight_candidate_source_ready(self, venv: Path) -> bool:
        self._observe_runtime_venv(venv)
        return self.preflight_candidate_source

    def ensure_preflight_candidate_source(self, venv: Path) -> bool:
        self._observe_runtime_venv(venv)
        changed = not self.preflight_candidate_source
        self.preflight_candidate_source = True
        return changed

    def ensure_service_key(self) -> bool:
        if self.service_key_present():
            return False
        self.filesystem.atomic_write(host.SERVICE_KEY, b"private-key-fixture\n", 0o600)
        self.filesystem.atomic_write(
            Path(str(host.SERVICE_KEY) + ".pub"), b"ssh-ed25519 public-fixture\n", 0o644
        )
        self.key = True
        self.private_key_fingerprint = SERVICE_FINGERPRINT
        self.public_key_fingerprint_value = SERVICE_FINGERPRINT
        self.service_key_generations += 1
        return True

    def service_key_present(self) -> bool:
        private_present = self.filesystem.exists(host.SERVICE_KEY)
        public_present = self.filesystem.exists(Path(str(host.SERVICE_KEY) + ".pub"))
        if private_present != public_present:
            raise host.InstallError("service deploy key pair is incomplete")
        if not private_present:
            return False
        if self.private_key_fingerprint != self.public_key_fingerprint_value:
            raise host.InstallError("service deploy private/public key fingerprints do not match")
        return True

    def public_key_fingerprint(self) -> str:
        if not self.service_key_present():
            raise host.InstallError("service deploy key pair is unavailable")
        return self.public_key_fingerprint_value

    def validate_service_key_continuity(self, expected_fingerprint: str) -> None:
        if not self.service_key_present():
            raise host.InstallError("existing GB10 trust authority requires its service key pair")
        if self.public_key_fingerprint() != expected_fingerprint:
            raise host.InstallError(
                "service deploy key fingerprint drifted from the install record"
            )

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
                "active_policy_sha256",
                "key_fingerprint",
                "revocation_hosts",
                "schema_version",
                "topology_sha256",
            }
            or type(parsed.get("schema_version")) is not int
            or parsed.get("schema_version") != 2
            or not isinstance(parsed.get("revocation_hosts"), list)
        ):
            raise host.InstallError("fake GB10 trust ledger is invalid")
        return parsed

    def _write_trust_ledger(self, hosts: list[str]) -> None:
        payload = (
            json.dumps(
                {
                    "active_policy_sha256": "c" * 64,
                    "key_fingerprint": self.public_key_fingerprint(),
                    "revocation_hosts": hosts,
                    "schema_version": 2,
                    "topology_sha256": "b" * 64,
                },
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n"
        ).encode()
        self.filesystem.atomic_write(host.TRUST_REVOCATION_LEDGER, payload, 0o600)

    def prepare_gb10_trust_ledger(
        self,
        source_sha: str,
        *,
        mode: str,
        previous_source_sha: str | None,
    ) -> None:
        assert source_sha in {"a" * 40, "b" * 40}
        assert mode in {"fresh", "legacy", "existing"}
        self.ledger_modes.append(mode)
        self.ledger_previous_source_shas.append(previous_source_sha)
        self.events.append(f"trust-ledger:{mode}")
        if mode == "legacy" and self.previous_topology_drift and previous_source_sha == "a" * 40:
            raise host.InstallError(
                "legacy GB10 trust topology drifted from the previous installed source"
            )
        if not self.filesystem.exists(host.TRUST_REVOCATION_LEDGER):
            if mode == "existing":
                raise host.InstallError("fake GB10 trust ledger is unavailable")
            self._write_trust_ledger([])
        ledger = self._trust_ledger()
        if ledger.get("key_fingerprint") != self.public_key_fingerprint():
            raise host.InstallError("fake GB10 trust ledger key binding is invalid")
        if mode == "legacy":
            self._write_trust_ledger([f"trt-gb10-{number}" for number in range(1, 16)])

    def require_gb10_revocation_complete(self, venv: Path, source_sha: str) -> None:
        self._observe_runtime_venv(venv)
        assert source_sha in {"a" * 40, "b" * 40}
        self.events.append("trust-ledger:finalize-check")
        if self._trust_ledger().get("revocation_hosts") != []:
            raise host.InstallError("fake GB10 trust revocation is incomplete")

    def install_owner(
        self,
        path: Path,
        owner: str,
        mode: int,
        *,
        group: str | None = None,
    ) -> bool:
        target_group = owner if group is None else group
        self.install_owner_calls.append((path, owner, target_group, mode))
        mapped = self.filesystem.path(path)
        if mapped.exists():
            mapped.chmod(mode)
        return False

    def file_owner_ready(
        self,
        path: Path,
        *,
        owner: str,
        mode: int,
        group: str | None = None,
        nlink: int | None = None,
    ) -> bool:
        del owner, group
        mapped = self.filesystem.path(path)
        return bool(
            mapped.is_file()
            and (mapped.stat().st_mode & 0o777) == mode
            and (nlink is None or mapped.stat().st_nlink == nlink)
        )

    def gb10_trust_ready(self, venv: Path, source_sha: str) -> bool:
        self._observe_runtime_venv(venv)
        assert source_sha in {"a" * 40, "b" * 40}
        return self.trust_ready

    def reconcile_gb10_active_hosts(self, venv: Path, source_sha: str) -> bool:
        self._observe_runtime_venv(venv)
        assert source_sha in {"a" * 40, "b" * 40}
        return False

    def run_post_install_preflight(self) -> dict[str, object]:
        self.preflights += 1
        return {
            "assessment_digest": "d" * 64,
            "blocker_codes": [],
            "status": "passed",
        }

    def check_runtime(
        self,
        expected_sha: str,
        *,
        source_tree_sha: str | None = None,
        source_base_sha: str | None = None,
    ) -> list[str]:
        if source_tree_sha is not None or source_base_sha is not None:
            assert source_tree_sha == "b" * 40
            assert source_base_sha == "c" * 40
        failures = [] if self.candidate_sha == expected_sha else ["candidate-checkout"]
        if not self.shared_work2_mount_ready():
            failures.append("shared-work2-mount")
        if not self.shared_worker_repo_root_ready():
            failures.append("shared-worker-repo-root")
        return failures

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
        if plan.mask_adjustment is not None or plan.snapshot_adjustment is not None:
            self.acl_adjustment_states[grant] = "after"
        return grant

    def acl_adjustment_state(
        self,
        adjustment: host.AclMaskAdjustment | host.AclSnapshotAdjustment,
    ) -> str:
        default = adjustment.default if isinstance(adjustment, host.AclMaskAdjustment) else False
        grant = host.AclGrant(adjustment.path, default=default)
        return self.acl_adjustment_states.get(grant, "before")

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

    def maintenance_marker_status(self) -> str:
        return "enabled" if self.maintenance else "disabled"

    def revoke_gb10_trust(self, venv: Path, source_sha: str) -> None:
        self._observe_runtime_venv(venv)
        assert source_sha in {"a" * 40, "b" * 40}
        if self.revoke_error is not None:
            raise host.InstallError(self.revoke_error)
        self._trust_ledger()
        self.revoked = True
        self.events.append("trust-ledger:revoke")
        self._write_trust_ledger([])

    def remove_acl(
        self,
        grant: host.AclGrant,
        adjustment: host.AclMaskAdjustment | host.AclSnapshotAdjustment | None = None,
        *,
        remove_service_entry: bool = True,
    ) -> None:
        if adjustment is not None:
            self.acl_adjustment_states[grant] = "before"
        if remove_service_entry:
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
    legacy_template = filesystem.path(
        host.LEGACY_GB10_ENV_ROOT / "staging-gb10-worker-staging-previous.env"
    )
    legacy_template.parent.mkdir(parents=True, exist_ok=True)
    legacy_template.write_text(
        "\n".join(
            [
                "LOOM_WORKER_CONTROL_PLANE_URL=http://control.example:8080",
                "LOOM_WORKER_GATEWAY_URL=http://control.example:9100",
                "LOOM_WORKER_TOKEN=legacy-worker-token",
                "LOOM_WORKER_MINIO_ENDPOINT=http://control.example:9000",
                "LOOM_WORKER_MINIO_ACCESS_KEY=minio-access",
                "LOOM_WORKER_MINIO_SECRET_KEY=minio-secret",
                "",
            ]
        ),
        encoding="utf-8",
    )
    legacy_template.chmod(0o600)


def _installer(tmp_path: Path) -> tuple[host.HostInstaller, FakeSystem]:
    filesystem = host.LocalFilesystem(tmp_path)
    _write_protected_inputs(filesystem)
    system = FakeSystem(filesystem)
    return host.HostInstaller(filesystem, system, 0), system  # type: ignore[arg-type]


def test_atomic_write_retries_parent_fsync_after_replace_was_not_durable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    filesystem = host.LocalFilesystem(tmp_path)
    destination = Path("/etc/loom/atomic-write-retry")
    payload = b"durable-payload\n"
    original_fsync = host.os.fsync
    directory_fsyncs = 0

    def fail_first_directory_fsync(descriptor: int) -> None:
        nonlocal directory_fsyncs
        if stat.S_ISDIR(os.fstat(descriptor).st_mode):
            directory_fsyncs += 1
            if directory_fsyncs == 1:
                raise OSError("injected parent directory fsync failure")
        original_fsync(descriptor)

    monkeypatch.setattr(host.os, "fsync", fail_first_directory_fsync)

    with pytest.raises(
        (OSError, host.InstallError),
        match="parent directory fsync failure",
    ):
        filesystem.atomic_write(destination, payload, 0o640)

    installed = filesystem.path(destination)
    assert installed.read_bytes() == payload
    assert stat.S_IMODE(installed.stat().st_mode) == 0o640

    assert filesystem.atomic_write(destination, payload, 0o640) is False
    assert directory_fsyncs == 2


def test_installer_known_hosts_authority_rejects_missing_or_malformed_hosts() -> None:
    payload = (host.REPO_ROOT / "deploy/worker-pools/gb10/known_hosts").read_bytes()
    host._validate_known_hosts_authority(payload)

    with pytest.raises(host.InstallError, match="exactly 15"):
        host._validate_known_hosts_authority(b"\n".join(payload.splitlines()[:-1]) + b"\n")
    with pytest.raises(host.InstallError, match="host coverage"):
        host._validate_known_hosts_authority(
            payload.replace(b"192.168.20.77,trt-gb10-7", b"192.168.20.99,trt-gb10-7")
        )


def test_install_is_idempotent_and_renders_only_safe_token_metadata(tmp_path: Path) -> None:
    installer, system = _installer(tmp_path)

    first = installer.install(TEAM_ID)
    maintenance_begins_after_first = system.maintenance_begins
    maintenance_ends_after_first = system.maintenance_ends
    second = installer.install(TEAM_ID)

    assert first["changed"]
    assert second["changed"] == []
    rendered = installer.filesystem.path(host.CONFIG_PATH).read_text(encoding="utf-8")
    assert TEAM_ID in rendered
    assert "sha256:" in rendered and " len=" in rendered
    assert "admin-token-fixture" not in rendered
    assert "__ADMIN_TOKEN_FINGERPRINT__" not in rendered
    assert "__SMOKE_ON_BEHALF_TEAM_ID__" not in rendered
    assert "__SOURCE_SHA__" not in rendered
    candidate_repo = host._candidate_repo_path("a" * 40)
    candidate_venv = host._candidate_venv_path("a" * 40)
    assert f'runner_repo = "{candidate_repo}"' in rendered
    assert (
        f'cluster_config_path = "{candidate_repo}/deploy/environments/'
        'staging.multinode.cluster.toml"'
        in rendered
    )
    assert system.runtime_venvs == {candidate_venv}
    for path in (
        host.ROLLOUT_BROKER_PATH,
        host.REHEARSAL_PATH,
        host.FINAL_GATE_PATH,
        host.CREDENTIAL_REFRESH_PATH,
    ):
        wrapper = installer.filesystem.path(path).read_text(encoding="utf-8")
        assert str(candidate_venv / "bin/python") in wrapper
        assert "PYTHONDONTWRITEBYTECODE=1" in wrapper
        assert "__CANDIDATE_VENV__" not in wrapper
        assert str(host.LEGACY_VENV) not in wrapper
    assert installer.filesystem.path(host.BROKER_PATH).read_text(encoding="utf-8") == (
        '#!/bin/sh\nset -eu\n\nexec /usr/local/libexec/loom-rollout-broker --env staging "$@"\n'
    )
    sudoers = installer.filesystem.path(host.SUDOERS_PATH).read_text(encoding="utf-8")
    assert "--env dev *" in sudoers
    assert "--env staging *" in sudoers
    assert "--env prod *" in sudoers
    assert "/usr/local/libexec/loom-staging-rollout-broker *" in sudoers
    assert str(host.LEGACY_CANDIDATE_REPO) not in rendered
    config_path = installer.filesystem.path(host.CONFIG_PATH)
    assert stat.S_IMODE(config_path.stat().st_mode) == 0o640
    assert (
        host.CONFIG_PATH,
        "root",
        host.SERVICE_GROUP,
        0o640,
    ) in system.install_owner_calls
    known_hosts = installer.filesystem.path(host.KNOWN_HOSTS_PATH)
    assert (
        known_hosts.read_bytes()
        == (host.REPO_ROOT / "deploy/worker-pools/gb10/known_hosts").read_bytes()
    )
    assert stat.S_IMODE(known_hosts.stat().st_mode) == 0o644
    assert (
        installer.filesystem.path(host.SYSCTL_PATH).read_text(encoding="ascii")
        == "fs.inotify.max_user_instances = 1024\n"
    )
    assert system.inotify_capacity is True
    assert "sysctl:fs.inotify.max_user_instances" in first["changed"]
    assert set(system.operator_members) == set(host.OPERATORS)
    assert system.docker is True
    assert system.preflight_credentials is True
    assert system.credential_refresh_timer is True
    assert "preflight-credentials" in first["changed"]
    assert "credential-refresh-timer" in first["changed"]
    assert all(
        stat.S_IMODE(installer.filesystem.path(path).stat().st_mode) == 0o600
        for path in (
            host.PREFLIGHT_CREDENTIAL_ROOT / "readonly-kubeconfig",
            host.PREFLIGHT_CREDENTIAL_ROOT / "readonly-probe-token",
            host.PREFLIGHT_CREDENTIAL_ROOT / "readonly-database.json",
            host.PREFLIGHT_CREDENTIAL_ROOT / "rehearsal-kubeconfig",
        )
    )
    environment_state = Path("/data/loom-staging/environment-state")
    assert Path(f"{environment_state}#False") in system.data_acls
    assert Path(f"{environment_state}#True") in system.data_acls
    assert not any(str(grant).startswith("/data/loom-staging#") for grant in system.data_acls)
    assert system.candidate_syncs == 2  # candidate convergence and venv sync run only once
    assert system.maintenance_begins == 1
    assert system.maintenance_ends == 1
    assert system.maintenance_begins == maintenance_begins_after_first
    assert system.maintenance_ends == maintenance_ends_after_first
    assert system.maintenance is False
    assert first["post_install_check"] == "awaiting-gb10-trust"
    assert system.preflights == 0
    generated_template = installer.filesystem.path(host.GENERATED_GB10_ENV_SEED)
    assert generated_template.read_text(encoding="utf-8").endswith(
        "LOOM_WORKER_MINIO_SECRET_KEY=minio-secret\n"
    )
    assert stat.S_IMODE(generated_template.stat().st_mode) == 0o600
    assert f"worker-env-template:{host.GENERATED_GB10_ENV_SEED}" in first["changed"]
    assert all("minio-secret" not in str(value) for value in first.values())
    record = installer.filesystem.load_install_record()
    assert record is not None
    assert record["schema_version"] == 3
    assert record["installation_state"] == "ready"
    assert record["admission_enabled"] is True
    assert record["maintenance_enabled"] is False
    assert record["trust_requires_revocation"] is True
    assert record["trust_ledger_migrated"] is True
    assert record["shared_worker_repo"] == system.shared_worker_repo_identity()
    assert record["shared_worker_repo"]["schema_version"] == 1
    assert record["shared_worker_repo"]["created"] == []
    install_record = installer.filesystem.path(host.INSTALL_RECORD)
    assert stat.S_IMODE(install_record.stat().st_mode) == 0o600
    assert (host.INSTALL_RECORD, "root", "root", 0o600) in system.install_owner_calls
    install_attestation = installer.filesystem.path(host.INSTALL_ATTESTATION)
    assert stat.S_IMODE(install_attestation.stat().st_mode) == 0o640
    assert (
        host.INSTALL_ATTESTATION,
        "root",
        host.SERVICE_GROUP,
        0o640,
    ) in system.install_owner_calls

    public_statement = json.loads(install_attestation.read_bytes())
    assert public_statement["schema_version"] == 1
    assert public_statement["source_mode"] == "merged-dev"
    assert public_statement["source_sha"] == "a" * 40
    assert public_statement["source_tree_sha"] == "none"
    assert public_statement["source_base_sha"] == "none"
    assert (
        public_statement["install_record_sha256"]
        == hashlib.sha256(install_record.read_bytes()).hexdigest()
    )
    assert set(public_statement["asset_sha256"]) == host._INSTALL_ATTESTATION_ASSETS
    # The root-side producer and service-side strict reader must accept the
    # same exact statement. This is the cross-layer schema gate that prevents
    # a successful install from failing only when broker preflight starts.
    assert RunnerInstallAttestation.from_payload(install_attestation.read_bytes()).source_sha == (
        "a" * 40
    )
    assert all(
        isinstance(value, str) and len(value) == 64
        for value in public_statement["asset_sha256"].values()
    )
    for path in (host.SHARED_WORKER_AUTHORITY_ROOT, host.SHARED_WORKER_REPO_ROOT):
        assert stat.S_IMODE(installer.filesystem.path(path).stat().st_mode) == 0o2750
    assert "trust_legacy_source_sha" not in record
    assert record["added_acls"]
    assert system.ledger_modes == ["fresh", "existing"]
    assert system.ledger_previous_source_shas == [None, None]
    assert system.lifecycle_lock_entries == 2
    assert system.lifecycle_lock_depth == 0
    assert set(system.source_reads) >= {
        "deploy/staging-rollout/loom-rollout",
        "deploy/staging-rollout/loom-rollout-broker",
        "deploy/staging-rollout/loom-staging-rollout",
        "deploy/staging-rollout/loom-staging-rollout-broker",
        "deploy/staging-rollout/loom-staging-rollout-credential-refresh",
        "deploy/staging-rollout/loom-staging-rollout-credential-refresh.service",
        "deploy/staging-rollout/loom-staging-rollout-credential-refresh.timer",
        "deploy/staging-rollout/loom-staging-rollout-final-gate",
        "deploy/staging-rollout/loom-staging-rollout-rehearsal",
        "deploy/staging-rollout/loom-staging-rollout.sudoers",
        "deploy/staging-rollout/loom-staging-rollout.tmpfiles",
        "deploy/staging-rollout/loom-staging-rollout.sysctl",
        "deploy/staging-rollout/shared_work2.mount",
        "deploy/staging-rollout/staging-rollout.toml",
        "deploy/worker-pools/gb10/known_hosts",
        "scripts/ops/staging_rollout_gb10_trust.py",
    }


def test_install_plan_converges_legacy_service_shell_before_strict_readiness(
    tmp_path: Path,
) -> None:
    installer, system = _installer(tmp_path)
    installer.install(TEAM_ID)
    maintenance_begins = system.maintenance_begins
    maintenance_ends = system.maintenance_ends
    system.service_user_requires_convergence = True

    result = installer.install(TEAM_ID)

    assert f"user:{host.SERVICE_USER}" in result["changed"]
    assert system.service_user_present() is True
    assert system.maintenance_begins == maintenance_begins + 1
    assert system.maintenance_ends == maintenance_ends + 1
    assert installer.install(TEAM_ID)["changed"] == []


def test_install_keeps_legacy_repo_and_venv_frozen_across_candidate_updates(
    tmp_path: Path,
) -> None:
    installer, system = _installer(tmp_path)
    legacy_repo = installer.filesystem.path(host.LEGACY_CANDIDATE_REPO)
    legacy_venv = installer.filesystem.path(host.LEGACY_VENV)
    legacy_repo.mkdir(parents=True)
    legacy_venv.mkdir(parents=True)
    repo_sentinel = legacy_repo / "active-pr907-script.py"
    venv_sentinel = legacy_venv / "active-pr907-python"
    repo_sentinel.write_bytes(b"legacy-repo-exact-bytes\n")
    venv_sentinel.write_bytes(b"legacy-venv-exact-bytes\n")

    installer.install(TEAM_ID)
    system.remote_source_sha = "b" * 40
    system.status = "done"
    installer.install(TEAM_ID_2)

    assert repo_sentinel.read_bytes() == b"legacy-repo-exact-bytes\n"
    assert venv_sentinel.read_bytes() == b"legacy-venv-exact-bytes\n"
    assert host.LEGACY_VENV not in system.runtime_venvs
    assert system.runtime_venvs == {
        host._candidate_venv_path("a" * 40),
        host._candidate_venv_path("b" * 40),
    }
    rendered = installer.filesystem.path(host.CONFIG_PATH).read_text(encoding="utf-8")
    assert str(host._candidate_repo_path("b" * 40)) in rendered
    assert str(host._candidate_venv_path("b" * 40)) in installer.filesystem.path(
        host.ROLLOUT_BROKER_PATH
    ).read_text(encoding="utf-8")


def test_explicit_maintenance_is_bounded_idempotent_and_visible_to_check(
    tmp_path: Path,
) -> None:
    installer, system = _installer(tmp_path)
    installer.install(TEAM_ID)

    enabled = installer.maintenance(enabled=True)
    enabled_again = installer.maintenance(enabled=True)

    assert enabled == {
        "ok": True,
        "changed": True,
        "maintenance": "enabled",
        "rollout": "idle",
        "source_sha": "a" * 40,
    }
    assert enabled_again["changed"] is False
    assert system.maintenance is True
    assert "maintenance-marker" in installer.check()["failures"]

    disabled = installer.maintenance(enabled=False)
    disabled_again = installer.maintenance(enabled=False)
    assert disabled["changed"] is True
    assert disabled_again["changed"] is False
    assert system.maintenance is False


def test_explicit_maintenance_enable_rolls_back_new_marker_when_active(
    tmp_path: Path,
) -> None:
    installer, system = _installer(tmp_path)
    installer.install(TEAM_ID)
    system.status = "busy"

    with pytest.raises(host.InstallError, match="rollout is active"):
        installer.maintenance(enabled=True)

    assert system.maintenance is False


def test_explicit_maintenance_disable_preserves_marker_when_active(tmp_path: Path) -> None:
    installer, system = _installer(tmp_path)
    installer.install(TEAM_ID)
    installer.maintenance(enabled=True)
    system.status = "running"

    with pytest.raises(host.InstallError, match="leave maintenance"):
        installer.maintenance(enabled=False)

    assert system.maintenance is True


def test_explicit_maintenance_requires_root(tmp_path: Path) -> None:
    installer, _system = _installer(tmp_path)
    installer.install(TEAM_ID)
    installer.euid = 1000

    with pytest.raises(host.InstallError, match="requires root"):
        installer.maintenance(enabled=True)


def test_install_publishes_attestation_and_ready_record_before_sudoers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    installer, _system = _installer(tmp_path)
    writes: list[Path] = []
    original_atomic_write = host.LocalFilesystem.atomic_write

    def record_write(
        filesystem: host.LocalFilesystem,
        absolute: Path,
        payload: bytes,
        mode: int,
        *,
        expected_nlink: int | None = None,
    ) -> bool:
        writes.append(absolute)
        return original_atomic_write(
            filesystem,
            absolute,
            payload,
            mode,
            expected_nlink=expected_nlink,
        )

    monkeypatch.setattr(host.LocalFilesystem, "atomic_write", record_write)

    installer.install(TEAM_ID)

    assert writes.index(host.INSTALL_ATTESTATION) < max(
        index for index, path in enumerate(writes) if path == host.INSTALL_RECORD
    )
    assert max(index for index, path in enumerate(writes) if path == host.INSTALL_RECORD) < (
        writes.index(host.SUDOERS_PATH)
    )
    assert writes[-1] == host.SUDOERS_PATH


def test_install_attestation_publication_failure_keeps_admission_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    installer, system = _installer(tmp_path)
    original_atomic_write = host.LocalFilesystem.atomic_write

    def fail_attestation(
        filesystem: host.LocalFilesystem,
        absolute: Path,
        payload: bytes,
        mode: int,
        *,
        expected_nlink: int | None = None,
    ) -> bool:
        if absolute == host.INSTALL_ATTESTATION:
            raise host.InstallError("injected install attestation publication failure")
        return original_atomic_write(
            filesystem,
            absolute,
            payload,
            mode,
            expected_nlink=expected_nlink,
        )

    monkeypatch.setattr(host.LocalFilesystem, "atomic_write", fail_attestation)

    with pytest.raises(host.InstallError, match="attestation publication failure"):
        installer.install(TEAM_ID)

    record = installer.filesystem.load_install_record()
    assert record is not None
    assert record["installation_state"] == "installing"
    assert record["admission_enabled"] is False
    assert record["maintenance_enabled"] is True
    assert system.maintenance is True
    assert system.maintenance_begins == 1
    assert system.maintenance_ends == 0
    assert not installer.filesystem.exists(host.INSTALL_ATTESTATION)
    assert not installer.filesystem.exists(host.SUDOERS_PATH)


def test_missing_install_attestation_is_repaired_in_one_controlled_transaction(
    tmp_path: Path,
) -> None:
    installer, system = _installer(tmp_path)
    installer.install(TEAM_ID)
    candidate_syncs = system.candidate_syncs
    installer.filesystem.remove(host.INSTALL_ATTESTATION)
    system.maintenance_begins = 0
    system.maintenance_ends = 0

    result = installer.install(TEAM_ID)

    assert result["ok"] is True
    assert "install-attestation" in result["changed"]
    assert installer.filesystem.exists(host.INSTALL_ATTESTATION)
    assert installer.filesystem.exists(host.SUDOERS_PATH)
    assert system.candidate_syncs == candidate_syncs
    assert system.maintenance_begins == 1
    assert system.maintenance_ends == 1
    assert system.maintenance is False
    record = installer.filesystem.load_install_record()
    assert record is not None
    assert record["installation_state"] == "ready"
    assert record["admission_enabled"] is True
    assert record["maintenance_enabled"] is False


def test_install_activates_ownership_maintenance_only_when_opted_in(tmp_path: Path) -> None:
    # #1085 phase 3: the installer writes the merged-dev (version, policy) opt-in
    # flag only when explicitly requested; the default config omits it entirely.
    default_dir = tmp_path / "default"
    default_dir.mkdir()
    default_installer, _default_system = _installer(default_dir)
    default_installer.install(TEAM_ID)
    default_rendered = default_installer.filesystem.path(host.CONFIG_PATH).read_text(
        encoding="utf-8"
    )
    assert "ownership_maintenance_allowed" not in default_rendered

    opted_dir = tmp_path / "opted"
    opted_dir.mkdir()
    opted_installer, _opted_system = _installer(opted_dir)
    opted_installer.install(TEAM_ID, allow_ownership_maintenance=True)
    rendered = opted_installer.filesystem.path(host.CONFIG_PATH).read_text(encoding="utf-8")
    assert "schema_version = 1" in rendered
    assert "ownership_maintenance_allowed = true" in rendered


def test_sealed_cumulative_install_records_exact_source_and_checks_candidate(
    tmp_path: Path,
) -> None:
    assert host.MAX_CUMULATIVE_COMMITS == 512
    installer, _system = _installer(tmp_path)
    source = host.SealedSource(
        path=host.REPO_ROOT,
        commit_sha="a" * 40,
        tree_sha="b" * 40,
        base_sha="c" * 40,
    )

    result = installer.install(TEAM_ID, sealed_source=source)
    record = installer.filesystem.load_install_record()
    rendered = installer.filesystem.path(host.CONFIG_PATH).read_text(encoding="utf-8")

    assert result["ok"] is True
    assert record is not None
    assert record["schema_version"] == 4
    assert record["source_mode"] == "sealed-cumulative"
    assert record["source_sha"] == source.commit_sha
    assert record["source_tree_sha"] == source.tree_sha
    assert record["source_base_sha"] == source.base_sha
    assert "schema_version = 2" in rendered
    assert 'source_mode = "sealed-cumulative"' in rendered
    assert f'source_commit_sha = "{source.commit_sha}"' in rendered
    assert installer.check() == {"ok": True, "failures": []}


def test_sealed_cumulative_install_rejects_invocation_drift_before_host_mutation(
    tmp_path: Path,
) -> None:
    installer, _system = _installer(tmp_path)
    source = host.SealedSource(
        path=host.REPO_ROOT,
        commit_sha="d" * 40,
        tree_sha="b" * 40,
        base_sha="c" * 40,
    )

    with pytest.raises(host.InstallError, match="installer checkout"):
        installer.install(TEAM_ID, sealed_source=source)

    assert installer.filesystem.load_install_record() is None
    assert not installer.filesystem.path(host.CONFIG_PATH).exists()


def test_worker_env_validation_allows_empty_optional_value() -> None:
    host._validate_gb10_env_payload(
        b"\n".join(
            (
                b"LOOM_WORKER_CONTROL_PLANE_URL=http://control.example:8080",
                b"LOOM_WORKER_GATEWAY_URL=http://control.example:9100",
                b"LOOM_WORKER_TOKEN=worker-token",
                b"LOOM_WORKER_MINIO_ENDPOINT=http://control.example:9000",
                b"LOOM_WORKER_MINIO_ACCESS_KEY=minio-access",
                b"LOOM_WORKER_MINIO_SECRET_KEY=minio-secret",
                b'LOOM_WORKER_SUBPROCESS_GATEWAY_URL=""',
                b"",
            )
        )
    )


class SharedWorkerRepoRunner:
    def __init__(self) -> None:
        self.consumer_present = True
        self.shared_group_present = True
        self.consumer_membership = True
        self.consumer_id_uid = 2005
        self.parent_numeric = "995:2007"
        self.root_numeric = "995:2007"
        self.root_metadata = "directory:loom-rollout:sharedwork:2750"
        self.symlinks: set[str] = set()
        self.service_parent_writable = True
        self.consumer_readable = True
        self.consumer_writable = False
        self.consumer_searchable = True

    def run(self, argv, **kwargs):  # type: ignore[no-untyped-def]
        del kwargs
        call = list(argv)
        mount_report = {
            "schema_version": 1,
            "mount_point": str(host.SHARED_WORK2_MOUNT_POINT),
            "source": "192.168.20.12:/shared_work2",
            "filesystem_type": "nfs4",
            "mount_id": 42,
            "parent_id": 1,
            "device_major": 0,
            "device_minor": 99,
            "mount_options": ["nodev", "noexec", "nosuid", "rw"],
            "super_options": [
                "hard",
                "proto=tcp",
                "retrans=2",
                "rw",
                "sec=sys",
                "timeo=600",
                "vers=4.2",
            ],
        }
        if call == [str(host.SYSTEM_PYTHON), str(host.SHARED_WORK2_MOUNT_HELPER), "check"]:
            return host.CommandResult(0, json.dumps(mount_report) + "\n")
        if call[:2] == [str(host.SYSTEM_PYTHON), str(host.SHARED_WORKER_REPO_HELPER)]:
            if len(call) != 3 or call[2] not in {"check", "ensure"}:
                raise AssertionError(f"unexpected command: {call}")
            helper_ok = bool(
                self.consumer_present
                and self.shared_group_present
                and self.consumer_membership
                and self.consumer_id_uid == 2005
                and self.parent_numeric == "995:2007"
                and self.root_numeric == "995:2007"
                and self.root_metadata == "directory:loom-rollout:sharedwork:2750"
                and not self.symlinks
                and self.service_parent_writable
                and self.consumer_readable
                and self.consumer_searchable
                and not self.consumer_writable
            )
            if not helper_ok:
                return host.CommandResult(1, stderr="safe failure\n")
            return host.CommandResult(
                0,
                json.dumps(
                    {
                        "schema_version": 1,
                        "root": str(host.SHARED_WORKER_REPO_ROOT),
                        "service_user": host.SERVICE_USER,
                        "service_uid": 995,
                        "service_primary_group": host.SERVICE_GROUP,
                        "service_primary_gid": 982,
                        "consumer_user": host.SHARED_WORK_CONSUMER,
                        "consumer_uid": 2005,
                        "shared_group": host.SHARED_WORK_GROUP,
                        "shared_gid": 2007,
                        "parent_mode": "2750",
                        "authority_mode": "2750",
                        "repository_mode": "2750",
                        "parent_device": 1,
                        "parent_inode": 3,
                        "authority_device": 1,
                        "authority_inode": 3,
                        "repository_device": 1,
                        "repository_inode": 4,
                        "service_capability": (
                            "parent-writable;repository-writable-searchable"
                        ),
                        "consumer_capability": ("repository-readable-searchable-not-writable"),
                        "publication_capability": "private-mkdir-publish-verified",
                        "mount": mount_report,
                        "created": [],
                    }
                )
                + "\n",
            )
        if call == ["getent", "passwd", host.SERVICE_USER]:
            return host.CommandResult(
                0,
                f"{host.SERVICE_USER}:x:995:982::{host.STATE_ROOT}:{host.SERVICE_SHELL}\n",
            )
        if call == ["getent", "group", host.SERVICE_GROUP]:
            return host.CommandResult(0, f"{host.SERVICE_GROUP}:x:982:\n")
        if call == ["id", "-u", host.SERVICE_USER]:
            return host.CommandResult(0, "995\n")
        if call == ["id", "-g", host.SERVICE_USER]:
            return host.CommandResult(0, "982\n")
        if call == ["getent", "passwd", host.SHARED_WORK_CONSUMER]:
            if not self.consumer_present:
                return host.CommandResult(1)
            return host.CommandResult(0, "qianyi:x:2005:2005::/home/qianyi:/bin/bash\n")
        if call == ["id", "-u", host.SHARED_WORK_CONSUMER]:
            return host.CommandResult(0, f"{self.consumer_id_uid}\n")
        if call == ["getent", "group", host.SHARED_WORK_GROUP]:
            if not self.shared_group_present:
                return host.CommandResult(1)
            return host.CommandResult(0, "sharedwork:x:2007:qianyi\n")
        if call == ["id", "-nG", host.SHARED_WORK_CONSUMER]:
            groups = "qianyi sharedwork\n" if self.consumer_membership else "qianyi\n"
            return host.CommandResult(0, groups)
        if call[:3] == ["stat", "-c", "%F"]:
            return host.CommandResult(0, "directory\n")
        if call[:3] == ["stat", "-c", "%F:%U:%G:%a"]:
            path = call[-1]
            if path == str(host.SHARED_WORKER_AUTHORITY_ROOT.parent):
                return host.CommandResult(0, "directory:qianyi:sharedwork:2775\n")
            return host.CommandResult(0, self.root_metadata + "\n")
        if call[:3] == ["stat", "-c", "%u:%g"]:
            path = call[-1]
            value = (
                self.parent_numeric
                if path == str(host.SHARED_WORKER_AUTHORITY_ROOT.parent)
                else self.root_numeric
            )
            return host.CommandResult(0, value + "\n")
        if call[:2] == ["test", "-L"]:
            return host.CommandResult(0 if call[-1] in self.symlinks else 1)
        if call[:7] == ["sudo", "-n", "-u", host.SERVICE_USER, "--", "test", "-w"]:
            writable = call[-1] == str(host.SHARED_WORKER_REPO_ROOT)
            if call[-1] == str(host.SHARED_WORKER_AUTHORITY_ROOT.parent):
                writable = self.service_parent_writable
            return host.CommandResult(0 if writable else 1)
        if call[:7] == ["sudo", "-n", "-u", host.SERVICE_USER, "--", "test", "-x"]:
            return host.CommandResult(0)
        if call[:7] == [
            "sudo",
            "-n",
            "-u",
            host.SHARED_WORK_CONSUMER,
            "--",
            "test",
            "-r",
        ]:
            return host.CommandResult(0 if self.consumer_readable else 1)
        if call[:7] == [
            "sudo",
            "-n",
            "-u",
            host.SHARED_WORK_CONSUMER,
            "--",
            "test",
            "-x",
        ]:
            return host.CommandResult(0 if self.consumer_searchable else 1)
        if call[:7] == [
            "sudo",
            "-n",
            "-u",
            host.SHARED_WORK_CONSUMER,
            "--",
            "test",
            "-w",
        ]:
            return host.CommandResult(0 if self.consumer_writable else 1)
        raise AssertionError(f"unexpected command: {call}")


def test_shared_worker_repo_root_capabilities_are_exact_and_read_only() -> None:
    runner = SharedWorkerRepoRunner()

    assert host.HostSystem(runner).shared_worker_repo_root_ready() is True


@pytest.mark.parametrize(
    ("mutation", "message"),
    (
        (lambda runner: setattr(runner, "consumer_present", False), "helper"),
        (lambda runner: setattr(runner, "shared_group_present", False), "helper"),
        (lambda runner: setattr(runner, "consumer_membership", False), "helper"),
        (lambda runner: setattr(runner, "consumer_id_uid", 2006), "helper"),
        (lambda runner: setattr(runner, "parent_numeric", "2005:2999"), "helper"),
        (
            lambda runner: runner.symlinks.add(str(host.SHARED_WORKER_REPO_ROOT)),
            "helper",
        ),
        (
            lambda runner: setattr(
                runner,
                "root_metadata",
                "directory:qianyi:sharedwork:2750",
            ),
            "helper",
        ),
        (
            lambda runner: setattr(
                runner,
                "root_metadata",
                "directory:loom-rollout:loom-rollout:2750",
            ),
            "helper",
        ),
        (
            lambda runner: setattr(
                runner,
                "root_metadata",
                "directory:loom-rollout:sharedwork:2770",
            ),
            "helper",
        ),
        (lambda runner: setattr(runner, "root_numeric", "995:2999"), "helper"),
        (
            lambda runner: setattr(runner, "service_parent_writable", False),
            "helper",
        ),
        (lambda runner: setattr(runner, "consumer_readable", False), "helper"),
        (lambda runner: setattr(runner, "consumer_writable", True), "helper"),
    ),
)
def test_shared_worker_repo_root_fails_closed_on_identity_metadata_or_capability_drift(
    mutation,
    message: str,
) -> None:
    runner = SharedWorkerRepoRunner()
    mutation(runner)

    with pytest.raises(host.InstallError, match=message):
        host.HostSystem(runner).shared_worker_repo_root_ready()


@pytest.mark.parametrize("semantic_empty", (b'""', b"'   '"))
def test_worker_env_validation_rejects_semantically_empty_required_value(
    semantic_empty: bytes,
) -> None:
    payload = b"\n".join(
        (
            b"LOOM_WORKER_CONTROL_PLANE_URL=http://control.example:8080",
            b"LOOM_WORKER_GATEWAY_URL=http://control.example:9100",
            b"LOOM_WORKER_TOKEN=" + semantic_empty,
            b"LOOM_WORKER_MINIO_ENDPOINT=http://control.example:9000",
            b"LOOM_WORKER_MINIO_ACCESS_KEY=minio-access",
            b"LOOM_WORKER_MINIO_SECRET_KEY=minio-secret",
            b"",
        )
    )

    with pytest.raises(host.InstallError, match="empty value"):
        host._validate_gb10_env_payload(payload)


def test_worker_env_validation_rejects_required_interpolation() -> None:
    payload = b"\n".join(
        (
            b"LOOM_WORKER_CONTROL_PLANE_URL=http://control.example:8080",
            b"LOOM_WORKER_GATEWAY_URL=http://control.example:9100",
            b"LOOM_WORKER_TOKEN=${UNSET}",
            b"LOOM_WORKER_MINIO_ENDPOINT=http://control.example:9000",
            b"LOOM_WORKER_MINIO_ACCESS_KEY=minio-access",
            b"LOOM_WORKER_MINIO_SECRET_KEY=minio-secret",
            b"",
        )
    )

    with pytest.raises(host.InstallError, match="cannot use interpolation"):
        host._validate_gb10_env_payload(payload)


def test_worker_env_validation_allows_literal_dollar_value() -> None:
    payload = b"\n".join(
        (
            b"LOOM_WORKER_CONTROL_PLANE_URL=http://control.example:8080",
            b"LOOM_WORKER_GATEWAY_URL=http://control.example:9100",
            b"LOOM_WORKER_TOKEN=worker$literal",
            b"LOOM_WORKER_MINIO_ENDPOINT=http://control.example:9000",
            b"LOOM_WORKER_MINIO_ACCESS_KEY=minio-access",
            b"LOOM_WORKER_MINIO_SECRET_KEY=minio-secret",
            b"",
        )
    )

    host._validate_gb10_env_payload(payload)


def test_install_fails_closed_when_no_legacy_or_generated_worker_env_exists(
    tmp_path: Path,
) -> None:
    installer, _ = _installer(tmp_path)
    installer.filesystem.path(
        host.LEGACY_GB10_ENV_ROOT / "staging-gb10-worker-staging-previous.env"
    ).unlink()

    with pytest.raises(host.InstallError, match="legacy GB10 worker env template"):
        installer.install(TEAM_ID)

    record = installer.filesystem.load_install_record()
    assert record is not None
    assert record["installation_state"] == "installing"
    assert record["admission_enabled"] is False
    assert not installer.filesystem.exists(host.SUDOERS_PATH)


def test_reinstall_preserves_private_template_without_legacy_source(tmp_path: Path) -> None:
    installer, _ = _installer(tmp_path)
    installer.install(TEAM_ID)
    private_before = installer.filesystem.read_bytes(host.GENERATED_GB10_ENV_SEED)
    installer.filesystem.path(
        host.LEGACY_GB10_ENV_ROOT / "staging-gb10-worker-staging-previous.env"
    ).unlink()

    result = installer.install(TEAM_ID)

    assert result["changed"] == []
    assert installer.filesystem.read_bytes(host.GENERATED_GB10_ENV_SEED) == private_before


def test_install_rejects_unsafe_legacy_worker_env_without_copying_secrets(
    tmp_path: Path,
) -> None:
    installer, _ = _installer(tmp_path)
    source = installer.filesystem.path(
        host.LEGACY_GB10_ENV_ROOT / "staging-gb10-worker-staging-previous.env"
    )
    source.chmod(0o622)

    with pytest.raises(host.InstallError, match="metadata is unsafe"):
        installer.install(TEAM_ID)

    assert not installer.filesystem.exists(host.GENERATED_GB10_ENV_SEED)
    record = installer.filesystem.load_install_record()
    assert record is not None
    assert record["installation_state"] == "installing"
    assert record["admission_enabled"] is False
    assert not installer.filesystem.exists(host.SUDOERS_PATH)


def test_check_reports_missing_generated_worker_env_template(tmp_path: Path) -> None:
    installer, _ = _installer(tmp_path)
    installer.install(TEAM_ID)
    installer.filesystem.remove(host.GENERATED_GB10_ENV_SEED)

    result = installer.check()

    assert result["ok"] is False
    assert "generated-gb10-worker-env-template" in result["failures"]


def test_reinstall_closes_admission_before_rejecting_unsafe_generated_template(
    tmp_path: Path,
) -> None:
    installer, system = _installer(tmp_path)
    installer.install(TEAM_ID)
    installer.filesystem.remove(host.GENERATED_GB10_ENV_SEED)
    outside = installer.filesystem.path(Path("/tmp/outside-worker.env"))
    outside.parent.mkdir(parents=True, exist_ok=True)
    outside.write_text("LOOM_WORKER_TOKEN=outside\n", encoding="utf-8")
    installer.filesystem.path(host.GENERATED_GB10_ENV_SEED).symlink_to(outside)
    system.status = "done"

    with pytest.raises(host.InstallError, match="metadata is unsafe"):
        installer.install(TEAM_ID)

    assert not installer.filesystem.exists(host.SUDOERS_PATH)
    assert system.maintenance is True
    record = installer.filesystem.load_install_record()
    assert record is not None
    assert record["installation_state"] == "installing"
    assert record["admission_enabled"] is False


def test_install_atomically_detaches_hardlinked_config_authority(tmp_path: Path) -> None:
    installer, system = _installer(tmp_path)
    installer.install(TEAM_ID)
    config_path = installer.filesystem.path(host.CONFIG_PATH)
    linked_path = config_path.with_name("staging-rollout-attacker-link.toml")
    original_inode = config_path.stat().st_ino
    original_payload = config_path.read_bytes()
    os.link(config_path, linked_path)

    assert config_path.stat().st_nlink == 2
    assert linked_path.stat().st_ino == original_inode

    result = installer.install(TEAM_ID)

    assert f"file:{host.CONFIG_PATH}" in result["changed"]
    assert system.admission_disabled_at_status
    assert config_path.read_bytes() == original_payload
    assert config_path.stat().st_nlink == 1
    assert config_path.stat().st_ino != original_inode
    assert linked_path.read_bytes() == original_payload
    assert linked_path.stat().st_nlink == 1
    assert linked_path.stat().st_ino == original_inode


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
        *,
        expected_nlink: int | None = None,
    ) -> bool:
        if absolute == host.TRUST_TOOL_PATH:
            system.events.append("trust-tool:replace")
        return original_atomic_write(
            filesystem,
            absolute,
            payload,
            mode,
            expected_nlink=expected_nlink,
        )

    monkeypatch.setattr(host.LocalFilesystem, "atomic_write", record_authority_write)

    result = installer.install(TEAM_ID)

    assert result["ok"] is True
    assert system.ledger_modes == ["legacy"]
    assert system.ledger_previous_source_shas[-1] == "a" * 40
    assert system.events.index("trust-ledger:legacy") < system.events.index("trust-tool:replace")
    assert system._trust_ledger()["revocation_hosts"] == [
        f"trt-gb10-{number}" for number in range(1, 16)
    ]
    migrated = installer.filesystem.load_install_record()
    assert migrated is not None
    assert migrated["schema_version"] == 3
    assert migrated["trust_ledger_migrated"] is True
    assert "trust_legacy_source_sha" not in migrated


def test_legacy_migration_rejects_previous_to_candidate_topology_drift(
    tmp_path: Path,
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
    old_trust_tool = installer.filesystem.read_bytes(host.TRUST_TOOL_PATH)
    system.remote_source_sha = "b" * 40
    system.previous_topology_drift = True

    with pytest.raises(host.InstallError, match="topology drifted"):
        installer.install(TEAM_ID)

    assert system.ledger_modes[-1] == "legacy"
    assert system.ledger_previous_source_shas[-1] == "a" * 40
    interrupted = installer.filesystem.load_install_record()
    assert interrupted is not None
    assert interrupted["schema_version"] == 3
    assert interrupted["source_sha"] == "b" * 40
    assert interrupted["trust_legacy_source_sha"] == "a" * 40
    assert interrupted["trust_ledger_migrated"] is False
    assert installer.filesystem.read_bytes(host.TRUST_TOOL_PATH) == old_trust_tool
    assert not installer.filesystem.exists(host.TRUST_REVOCATION_LEDGER)

    with pytest.raises(host.InstallError, match="topology drifted"):
        installer.install(TEAM_ID)

    assert system.ledger_previous_source_shas[-2:] == ["a" * 40, "a" * 40]


def test_legacy_source_binding_survives_post_migration_record_write_failure(
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
    system.remote_source_sha = "b" * 40
    original_atomic_write = host.LocalFilesystem.atomic_write
    install_record_writes = 0

    def fail_second_install_record_write(
        filesystem: host.LocalFilesystem,
        absolute: Path,
        payload: bytes,
        mode: int,
        *,
        expected_nlink: int | None = None,
    ) -> bool:
        nonlocal install_record_writes
        if absolute == host.INSTALL_RECORD:
            install_record_writes += 1
            if install_record_writes == 2:
                raise host.InstallError("injected post-ledger record failure")
        return original_atomic_write(
            filesystem,
            absolute,
            payload,
            mode,
            expected_nlink=expected_nlink,
        )

    monkeypatch.setattr(host.LocalFilesystem, "atomic_write", fail_second_install_record_write)

    with pytest.raises(host.InstallError, match="post-ledger record failure"):
        installer.install(TEAM_ID)

    interrupted = installer.filesystem.load_install_record()
    assert interrupted is not None
    assert interrupted["source_sha"] == "b" * 40
    assert interrupted["trust_legacy_source_sha"] == "a" * 40
    assert interrupted["trust_ledger_migrated"] is False
    assert system._trust_ledger()["revocation_hosts"] == [
        f"trt-gb10-{number}" for number in range(1, 16)
    ]

    monkeypatch.setattr(host.LocalFilesystem, "atomic_write", original_atomic_write)
    result = installer.install(TEAM_ID)

    assert result["ok"] is True
    assert system.ledger_previous_source_shas[-2:] == ["a" * 40, "a" * 40]
    recovered = installer.filesystem.load_install_record()
    assert recovered is not None
    assert recovered["trust_ledger_migrated"] is True
    assert "trust_legacy_source_sha" not in recovered


def test_install_refuses_interrupted_v2_legacy_migration_without_source_binding(
    tmp_path: Path,
) -> None:
    installer, system = _installer(tmp_path)
    installer.install(TEAM_ID)
    record = installer.filesystem.load_install_record()
    assert record is not None
    record.update(
        {
            "schema_version": 2,
            "installation_state": "installing",
            "admission_enabled": False,
            "maintenance_enabled": True,
            "trust_ledger_migrated": False,
        }
    )
    installer.filesystem.atomic_write(
        host.INSTALL_RECORD,
        (json.dumps(record, sort_keys=True) + "\n").encode(),
        0o600,
    )
    record_before = installer.filesystem.read_bytes(host.INSTALL_RECORD)
    ledger_modes_before = list(system.ledger_modes)
    maintenance_begins_before = system.maintenance_begins
    system.remote_source_sha = "b" * 40

    with pytest.raises(host.InstallError, match="lost its legacy source binding"):
        installer.install(TEAM_ID)

    assert installer.filesystem.read_bytes(host.INSTALL_RECORD) == record_before
    assert installer.filesystem.exists(host.SUDOERS_PATH)
    assert system.ledger_modes == ledger_modes_before
    assert system.maintenance_begins == maintenance_begins_before
    assert system.install_source_sha == "a" * 40


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


def test_legacy_ready_record_never_regenerates_a_missing_service_key_pair(
    tmp_path: Path,
) -> None:
    installer, system = _installer(tmp_path)
    installer.install(TEAM_ID)
    record = installer.filesystem.load_install_record()
    assert record is not None
    record["schema_version"] = 1
    record.pop("trust_ledger_migrated")
    record.pop("acl_mask_adjustments", None)
    installer.filesystem.atomic_write(
        host.INSTALL_RECORD,
        (json.dumps(record, sort_keys=True) + "\n").encode(),
        0o600,
    )
    record_before = installer.filesystem.read_bytes(host.INSTALL_RECORD)
    ledger_before = installer.filesystem.read_bytes(host.TRUST_REVOCATION_LEDGER)
    ledger_modes_before = list(system.ledger_modes)
    installer.filesystem.remove(host.SERVICE_KEY)
    installer.filesystem.remove(Path(str(host.SERVICE_KEY) + ".pub"))
    generations = system.service_key_generations

    with pytest.raises(host.InstallError, match="requires its service key pair"):
        installer.install(TEAM_ID)

    assert system.service_key_generations == generations
    assert installer.filesystem.read_bytes(host.INSTALL_RECORD) == record_before
    assert installer.filesystem.read_bytes(host.TRUST_REVOCATION_LEDGER) == ledger_before
    assert system.ledger_modes == ledger_modes_before
    assert installer.filesystem.exists(host.SUDOERS_PATH)


def test_existing_authority_rejects_complete_service_key_replacement(tmp_path: Path) -> None:
    installer, system = _installer(tmp_path)
    installer.install(TEAM_ID)
    record_before = installer.filesystem.read_bytes(host.INSTALL_RECORD)
    ledger_before = installer.filesystem.read_bytes(host.TRUST_REVOCATION_LEDGER)
    ledger_modes_before = list(system.ledger_modes)
    system.private_key_fingerprint = OTHER_SERVICE_FINGERPRINT
    system.public_key_fingerprint_value = OTHER_SERVICE_FINGERPRINT

    with pytest.raises(host.InstallError, match="drifted from the install record"):
        installer.install(TEAM_ID)

    assert installer.filesystem.exists(host.SUDOERS_PATH)
    assert installer.filesystem.read_bytes(host.INSTALL_RECORD) == record_before
    assert installer.filesystem.read_bytes(host.TRUST_REVOCATION_LEDGER) == ledger_before
    assert system.ledger_modes == ledger_modes_before


def test_existing_authority_rejects_private_only_service_key_replacement(
    tmp_path: Path,
) -> None:
    installer, system = _installer(tmp_path)
    installer.install(TEAM_ID)
    system.private_key_fingerprint = OTHER_SERVICE_FINGERPRINT

    with pytest.raises(host.InstallError, match="private/public key fingerprints"):
        installer.install(TEAM_ID)

    assert installer.filesystem.exists(host.SUDOERS_PATH)


@pytest.mark.parametrize("missing_path", [host.SERVICE_KEY, Path(str(host.SERVICE_KEY) + ".pub")])
def test_existing_authority_rejects_partial_service_key_pair_without_mutation(
    tmp_path: Path,
    missing_path: Path,
) -> None:
    installer, system = _installer(tmp_path)
    installer.install(TEAM_ID)
    record_before = installer.filesystem.read_bytes(host.INSTALL_RECORD)
    ledger_before = installer.filesystem.read_bytes(host.TRUST_REVOCATION_LEDGER)
    ledger_modes_before = list(system.ledger_modes)
    generations_before = system.service_key_generations
    installer.filesystem.remove(missing_path)

    with pytest.raises(host.InstallError, match="key pair is incomplete"):
        installer.install(TEAM_ID)

    assert installer.filesystem.read_bytes(host.INSTALL_RECORD) == record_before
    assert installer.filesystem.read_bytes(host.TRUST_REVOCATION_LEDGER) == ledger_before
    assert system.ledger_modes == ledger_modes_before
    assert system.service_key_generations == generations_before
    assert installer.filesystem.exists(host.SUDOERS_PATH)


@pytest.mark.parametrize("legacy_schema", [1, 2])
def test_reinstall_upgrades_pre_acl_record_before_acl_mutation(
    tmp_path: Path,
    legacy_schema: int,
) -> None:
    installer, system = _installer(tmp_path)
    installer.install(TEAM_ID)
    record = installer.filesystem.load_install_record()
    assert record is not None
    record["schema_version"] = legacy_schema
    record.pop("acl_mask_adjustments", None)
    if legacy_schema == 1:
        record.pop("trust_ledger_migrated")
        installer.filesystem.remove(host.TRUST_REVOCATION_LEDGER)
    installer.filesystem.atomic_write(
        host.INSTALL_RECORD,
        (json.dumps(record, sort_keys=True) + "\n").encode(),
        0o600,
    )
    path = host.PROTECTED_INPUTS[3]
    plan = _fake_mask_plan(path, service_preexisting=True)
    system.plan_input_acl = (  # type: ignore[method-assign]
        lambda candidate: (plan,) if candidate == path else ()
    )
    system.plan_data_acl = lambda candidate: ()  # type: ignore[method-assign]
    original_apply = system.apply_acl

    def assert_v3_preimage_before_apply(candidate: host.AclPlan) -> host.AclGrant:
        provisional = installer.filesystem.load_install_record()
        assert provisional is not None
        assert provisional["schema_version"] == 3
        assert provisional["installation_state"] == "installing"
        assert provisional["trust_ledger_migrated"] is True
        assert provisional["acl_mask_adjustments"]
        return original_apply(candidate)

    system.apply_acl = assert_v3_preimage_before_apply  # type: ignore[method-assign]

    installer.install(TEAM_ID)

    upgraded = installer.filesystem.load_install_record()
    assert upgraded is not None
    assert upgraded["schema_version"] == 3


def test_install_runs_requestless_preflight_only_after_all_gb10_trust_is_ready(
    tmp_path: Path,
) -> None:
    installer, system = _installer(tmp_path)
    system.trust_ready = True

    result = installer.install(TEAM_ID)

    assert result["post_install_check"] == "passed"
    assert system.preflights == 1


def test_install_reports_admission_blocker_after_publishing_ready_state(
    tmp_path: Path,
) -> None:
    installer, system = _installer(tmp_path)
    system.trust_ready = True
    system.run_post_install_preflight = lambda: {  # type: ignore[method-assign]
        "assessment_digest": "e" * 64,
        "blocker_codes": ["backup.lease.ineligible"],
        "status": "blocked",
    }

    result = installer.install(TEAM_ID)

    assert result["ok"] is True
    assert result["post_install_check"] == "blocked"
    assert result["post_install_preflight"] == {
        "assessment_digest": "e" * 64,
        "blocker_codes": ["backup.lease.ineligible"],
        "status": "blocked",
    }
    assert installer.filesystem.load_install_record()["installation_state"] == "ready"  # type: ignore[index]


def test_post_install_preflight_invokes_requestless_broker_command() -> None:
    class RecordingRunner:
        def __init__(self) -> None:
            self.calls: list[list[str]] = []

        def run(self, argv, **kwargs):  # type: ignore[no-untyped-def]
            assert kwargs == {"check": False}
            self.calls.append(list(argv))
            return host.CommandResult(
                0,
                json.dumps(
                    {
                        "candidate_sha": "a" * 40,
                        "candidate_tree": "b" * 40,
                        "coverage_sha256": "c" * 64,
                        "mutation_epoch": 8,
                        "preflight_assessment_sha256": "d" * 64,
                        "registry_sha256": "e" * 64,
                        "status": "passed",
                    }
                ),
            )

    runner = RecordingRunner()

    result = host.HostSystem(runner).run_post_install_preflight()

    assert result == {
        "assessment_digest": "d" * 64,
        "blocker_codes": [],
        "status": "passed",
    }

    assert runner.calls == [
        [
            "sudo",
            "-n",
            "-u",
            "qianyi",
            "--",
            str(host.CLIENT_PATH),
            "preflight",
        ]
    ]


def test_post_install_preflight_preserves_normalized_admission_blockers() -> None:
    class BlockedRunner:
        def run(self, argv, **kwargs):  # type: ignore[no-untyped-def]
            assert kwargs == {"check": False}
            return host.CommandResult(
                1,
                stderr=json.dumps(
                    {
                        "assessment_digest": "e" * 64,
                        "blockers": [
                            {"failure_code": "backup.lease.ineligible"},
                        ],
                        "passed": False,
                    }
                ),
            )

    assert host.HostSystem(BlockedRunner()).run_post_install_preflight() == {
        "assessment_digest": "e" * 64,
        "blocker_codes": ["backup.lease.ineligible"],
        "status": "blocked",
    }


def test_post_install_preflight_preserves_legacy_report_blockers() -> None:
    payload = {
        "checks": [
            {"name": "candidate", "passed": True, "remediation": None},
            {
                "name": "gb10-shared-source",
                "passed": False,
                "remediation": "repair exact shared source",
            },
        ],
        "passed": False,
    }

    class BlockedRunner:
        def run(self, argv, **kwargs):  # type: ignore[no-untyped-def]
            assert kwargs == {"check": False}
            return host.CommandResult(1, stderr=json.dumps(payload))

    result = host.HostSystem(BlockedRunner()).run_post_install_preflight()

    assert result == {
        "assessment_digest": hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
        "blocker_codes": ["gb10-shared-source"],
        "status": "blocked",
    }


def test_unchanged_reinstall_does_not_repeat_post_install_preflight(tmp_path: Path) -> None:
    installer, system = _installer(tmp_path)
    system.trust_ready = True

    first = installer.install(TEAM_ID)
    second = installer.install(TEAM_ID)

    assert first["changed"]
    assert second["changed"] == []
    assert system.preflights == 1


def test_unchanged_reinstall_repairs_broken_broker_runtime(tmp_path: Path) -> None:
    installer, system = _installer(tmp_path)
    installer.install(TEAM_ID)
    syncs_before = system.candidate_syncs
    system.package_ready = False
    system.broker_ready = False

    result = installer.install(TEAM_ID)

    assert "venv" in result["changed"]
    assert system.candidate_syncs == syncs_before + 1
    assert system.package_ready is True
    assert system.broker_ready is True
    assert system.sync_safety_snapshots[-1] == (True, False)


def test_same_sha_broker_repair_failure_stays_admission_closed(tmp_path: Path) -> None:
    installer, system = _installer(tmp_path)
    installer.install(TEAM_ID)
    system.package_ready = False
    system.broker_ready = False

    def fail_sync(source_root: Path, *, venv: Path) -> None:
        assert source_root == host.REPO_ROOT
        assert venv == host._candidate_venv_path("a" * 40)
        assert system.maintenance is True
        assert not installer.filesystem.exists(host.SUDOERS_PATH)
        raise host.InstallError("injected same-SHA runtime repair failure")

    system.sync_venv = fail_sync  # type: ignore[method-assign]

    with pytest.raises(host.InstallError, match="same-SHA runtime repair failure"):
        installer.install(TEAM_ID)

    record = installer.filesystem.load_install_record()
    assert record is not None
    assert record["installation_state"] == "installing"
    assert record["admission_enabled"] is False
    assert record["maintenance_enabled"] is True
    assert system.maintenance is True
    assert not installer.filesystem.exists(host.SUDOERS_PATH)


def test_config_probe_failure_stays_admission_closed_without_package_reinstall(
    tmp_path: Path,
) -> None:
    installer, system = _installer(tmp_path)
    installer.install(TEAM_ID)
    syncs_before = system.candidate_syncs
    system.broker_ready = False

    with pytest.raises(host.InstallError, match="broker config probe failed"):
        installer.install(TEAM_ID)

    record = installer.filesystem.load_install_record()
    assert record is not None
    assert record["installation_state"] == "installing"
    assert record["admission_enabled"] is False
    assert record["maintenance_enabled"] is True
    assert system.candidate_syncs == syncs_before
    assert system.maintenance is True
    assert not installer.filesystem.exists(host.SUDOERS_PATH)


def test_existing_venv_with_missing_service_user_converges(tmp_path: Path) -> None:
    installer, system = _installer(tmp_path)
    system.venv = True
    system.package_ready = True
    system.broker_ready = True

    result = installer.install(TEAM_ID)

    assert result["ok"] is True
    assert system.service_user is True


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
    assert not installer.filesystem.exists(host.INSTALL_ATTESTATION)


def test_retry_resyncs_candidate_and_venv_for_non_ready_new_sha_record(
    tmp_path: Path,
) -> None:
    installer, system = _installer(tmp_path)
    installer.install(TEAM_ID)
    system.remote_source_sha = "b" * 40
    system.status = "done"
    original_sync_venv = system.sync_venv

    def fail_before_venv_sync(source_root: Path, *, venv: Path) -> None:
        assert source_root == host.REPO_ROOT
        assert venv == host._candidate_venv_path("b" * 40)
        assert system.candidate_sha == "b" * 40
        raise host.InstallError("injected pre-venv crash")

    system.sync_venv = fail_before_venv_sync  # type: ignore[method-assign]
    with pytest.raises(host.InstallError, match="pre-venv crash"):
        installer.install(TEAM_ID_2)

    interrupted = installer.filesystem.load_install_record()
    assert interrupted is not None
    assert interrupted["installation_state"] == "installing"
    assert interrupted["source_sha"] == "b" * 40
    assert system.install_source_sha == "b" * 40
    assert system.candidate_sha == "b" * 40
    syncs_before_retry = system.candidate_syncs

    system.sync_venv = original_sync_venv  # type: ignore[method-assign]
    result = installer.install(TEAM_ID_2)

    assert "venv" in result["changed"]
    assert system.candidate_syncs == syncs_before_retry + 2
    ready = installer.filesystem.load_install_record()
    assert ready is not None
    assert ready["installation_state"] == "ready"
    assert ready["source_sha"] == "b" * 40


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
    assert system.candidate_syncs == 4
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


def test_check_rejects_installed_known_hosts_drift(tmp_path: Path) -> None:
    installer, _ = _installer(tmp_path)
    installer.install(TEAM_ID)
    installer.filesystem.atomic_write(host.KNOWN_HOSTS_PATH, b"untrusted\n", 0o644)

    result = installer.check()

    assert result["ok"] is False
    assert str(host.KNOWN_HOSTS_PATH) in result["failures"]


def test_check_rejects_host_inotify_capacity_drift(tmp_path: Path) -> None:
    installer, system = _installer(tmp_path)
    installer.install(TEAM_ID)
    system.inotify_capacity = False

    result = installer.check()

    assert result["ok"] is False
    assert "host-inotify-capacity" in result["failures"]


def test_check_rejects_disabled_credential_refresh_timer(tmp_path: Path) -> None:
    installer, system = _installer(tmp_path)
    installer.install(TEAM_ID)
    system.credential_refresh_timer = False

    result = installer.check()

    assert result["ok"] is False
    assert "credential-refresh-timer" in result["failures"]


def test_host_system_converges_only_fixed_inotify_sysctl() -> None:
    class InotifyRunner:
        def __init__(self) -> None:
            self.value = 128
            self.calls: list[list[str]] = []

        def run(self, argv, **kwargs):  # type: ignore[no-untyped-def]
            call = list(argv)
            self.calls.append(call)
            if call == ["sysctl", "-n", "fs.inotify.max_user_instances"]:
                return host.CommandResult(0, f"{self.value}\n")
            if call == ["sysctl", "--load", str(host.SYSCTL_PATH)]:
                assert kwargs == {}
                self.value = host._INOTIFY_MIN_INSTANCES
                return host.CommandResult(0)
            raise AssertionError(call)

    runner = InotifyRunner()
    system = host.HostSystem(runner)

    assert system.inotify_capacity_ready() is False
    assert system.ensure_inotify_capacity() is True
    assert system.inotify_capacity_ready() is True
    assert system.ensure_inotify_capacity() is False
    assert runner.calls.count(["sysctl", "--load", str(host.SYSCTL_PATH)]) == 1


def test_check_rejects_root_install_attestation_drift(tmp_path: Path) -> None:
    installer, _ = _installer(tmp_path)
    installer.install(TEAM_ID)
    statement = json.loads(installer.filesystem.read_bytes(host.INSTALL_ATTESTATION))
    statement["asset_sha256"]["broker"] = "f" * 64
    installer.filesystem.atomic_write(
        host.INSTALL_ATTESTATION,
        (json.dumps(statement, sort_keys=True, separators=(",", ":")) + "\n").encode(),
        0o640,
        expected_nlink=1,
    )

    result = installer.check()

    assert result["ok"] is False
    assert str(host.INSTALL_ATTESTATION) in result["failures"]


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
        raise host.InstallError("pre-existing service ACL is insufficient")

    system.plan_input_acl = fail  # type: ignore[method-assign]
    with pytest.raises(host.InstallError, match="insufficient"):
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
    assert not installer.filesystem.exists(host.KNOWN_HOSTS_PATH)
    assert system.shared_work2_mounted is False
    assert system.credential_refresh_timer is False
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
    tombstone_path = installer.filesystem.path(host.TRUST_REVOCATION_TOMBSTONE)
    original_lstat = host.os.lstat
    tombstone_lstats = 0

    def racing_lstat(path: os.PathLike[str] | str) -> os.stat_result:
        nonlocal tombstone_lstats
        if Path(path) == tombstone_path:
            tombstone_lstats += 1
            if tombstone_lstats == 2:
                raise FileNotFoundError("injected concurrent ledger replacement")
        return original_lstat(path)

    monkeypatch.setattr(host.os, "lstat", racing_lstat)

    with pytest.raises(host.InstallError, match="changed before removal"):
        installer.filesystem.remove_validated_trust_ledger(
            expected_fingerprint=system.public_key_fingerprint()
        )

    assert not ledger_path.exists()
    assert tombstone_path.exists()


def test_trust_ledger_rename_cas_never_deletes_nonempty_replacement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    installer, system = _installer(tmp_path)
    installer.install(TEAM_ID)
    ledger_path = installer.filesystem.path(host.TRUST_REVOCATION_LEDGER)
    tombstone_path = installer.filesystem.path(host.TRUST_REVOCATION_TOMBSTONE)
    replacement = ledger_path.parent / "attacker-valid-nonempty-ledger"
    payload = json.loads(ledger_path.read_text(encoding="utf-8"))
    payload["revocation_hosts"] = ["trt-gb10-2"]
    replacement.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")
    replacement.chmod(0o600)
    original_replace = host.os.replace

    def replace_after_validation(source: os.PathLike[str] | str, target: os.PathLike[str] | str):
        if Path(source) == ledger_path and Path(target) == tombstone_path:
            original_replace(replacement, ledger_path)
        return original_replace(source, target)

    monkeypatch.setattr(host.os, "replace", replace_after_validation)

    with pytest.raises(host.InstallError, match="not safe to finalize"):
        installer.filesystem.remove_validated_trust_ledger(
            expected_fingerprint=system.public_key_fingerprint()
        )

    assert not ledger_path.exists()
    assert tombstone_path.exists()
    retained = json.loads(tombstone_path.read_text(encoding="utf-8"))
    assert retained["revocation_hosts"] == ["trt-gb10-2"]


def test_uninstall_rejects_invalid_maintenance_ledger_before_disabling_admission(
    tmp_path: Path,
) -> None:
    installer, system = _installer(tmp_path)
    installer.install(TEAM_ID)
    record = installer.filesystem.load_install_record()
    assert record is not None
    record["maintenance_enabled"] = "corrupt"
    installer.filesystem.atomic_write(
        host.INSTALL_RECORD,
        (json.dumps(record, sort_keys=True) + "\n").encode(),
        0o600,
    )
    sudoers_before = installer.filesystem.read_bytes(host.SUDOERS_PATH)
    maintenance_begins_before = system.maintenance_begins

    with pytest.raises(host.InstallError, match="maintenance_enabled ledger is invalid"):
        installer.uninstall(retain_ledger=True)

    assert installer.filesystem.read_bytes(host.SUDOERS_PATH) == sudoers_before
    assert system.maintenance_begins == maintenance_begins_before
    assert system.revoked is False


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
    with pytest.raises(SystemExit):
        parser.parse_args(
            [
                "install",
                "--smoke-on-behalf-team-id",
                TEAM_ID,
                "--sealed-source-path",
                "/tmp/checkout",
            ]
        )


def test_cli_sealed_mode_requires_complete_binding_and_merged_mode_rejects_it(
    tmp_path: Path,
) -> None:
    installer, _system = _installer(tmp_path)

    assert (
        host.main(
            [
                "install",
                "--smoke-on-behalf-team-id",
                TEAM_ID,
                "--source-mode",
                "sealed-cumulative",
            ],
            installer=installer,
        )
        == 1
    )
    assert (
        host.main(
            [
                "install",
                "--smoke-on-behalf-team-id",
                TEAM_ID,
                "--sealed-source-sha",
                "a" * 40,
            ],
            installer=installer,
        )
        == 1
    )
    assert installer.filesystem.load_install_record() is None


def test_host_lifecycle_lock_rejects_unsafe_metadata(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    parent = tmp_path / "etc" / "loom"
    parent.mkdir(parents=True)
    parent.chmod(0o755)
    lock_path = parent / "staging-rollout-gb10-trust.lock"
    lock_path.write_text("unsafe\n", encoding="utf-8")
    lock_path.chmod(0o666)
    monkeypatch.setattr(host, "TRUST_LIFECYCLE_LOCK", lock_path)

    with pytest.raises(host.InstallError, match="is unsafe"):
        with host.HostSystem(host.SubprocessRunner()).trust_lifecycle_lock():
            raise AssertionError("unsafe lock must not be entered")


def test_trust_subprocess_inherits_lifecycle_lock_without_ambient_home() -> None:
    system = host.HostSystem(host.SubprocessRunner())
    system._trust_lock_fd = 42

    kwargs = system._trust_command_kwargs()

    assert kwargs["pass_fds"] == (42,)
    environment = kwargs["env"]
    assert environment[host.TRUST_LOCK_FD_ENV] == "42"
    assert environment["HOME"] == str(host.STATE_ROOT)
    assert environment["USER"] == host.SERVICE_USER
    assert environment["LOGNAME"] == host.SERVICE_USER


def test_host_system_derives_private_key_fingerprint_and_rejects_pair_mismatch() -> None:
    class KeyRunner:
        def run(self, argv, **kwargs):  # type: ignore[no-untyped-def]
            call = list(argv)
            if call[:2] == ["test", "-e"]:
                return host.CommandResult(0)
            if call[:2] == ["test", "-L"]:
                return host.CommandResult(1)
            if call[:3] == ["stat", "-c", "%F:%U:%G:%a"]:
                mode = "600" if call[-1] == str(host.SERVICE_KEY) else "644"
                return host.CommandResult(0, f"regular file:loom-rollout:loom-rollout:{mode}\n")
            if call[:2] == ["ssh-keygen", "-y"]:
                return host.CommandResult(0, "ssh-ed25519 test-public-key\n")
            if call == ["ssh-keygen", "-lf", "-"]:
                assert kwargs["input_text"] == "ssh-ed25519 test-public-key\n"
                return host.CommandResult(0, f"256 {SERVICE_FINGERPRINT} stdin (ED25519)\n")
            if call == ["ssh-keygen", "-lf", str(host.SERVICE_KEY) + ".pub"]:
                return host.CommandResult(0, f"256 {OTHER_SERVICE_FINGERPRINT} key (ED25519)\n")
            raise AssertionError(call)

    with pytest.raises(host.InstallError, match="fingerprints do not match"):
        host.HostSystem(KeyRunner()).service_key_present()


@pytest.mark.parametrize("present", [{host.SERVICE_KEY}, {Path(str(host.SERVICE_KEY) + ".pub")}])
def test_host_system_rejects_partial_key_pair(present: set[Path]) -> None:
    class PartialRunner:
        def run(self, argv, **kwargs):  # type: ignore[no-untyped-def]
            del kwargs
            call = list(argv)
            if call[:2] == ["test", "-e"]:
                return host.CommandResult(0 if Path(call[-1]) in present else 1)
            raise AssertionError(call)

    with pytest.raises(host.InstallError, match="key pair is incomplete"):
        host.HostSystem(PartialRunner()).service_key_present()


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


def _runtime_probe_argv(
    service_uid: int,
    program: str,
    *,
    venv: Path = TEST_CANDIDATE_VENV,
) -> list[str]:
    return [
        "sudo",
        "-n",
        "-u",
        host.SERVICE_USER,
        "--",
        "/usr/bin/env",
        "-i",
        f"HOME={host.STATE_ROOT}",
        f"USER={host.SERVICE_USER}",
        f"LOGNAME={host.SERVICE_USER}",
        f"PATH={venv / 'bin'}:{host._ROOT_PATH}",
        "LANG=C.UTF-8",
        "LC_ALL=C.UTF-8",
        "PYTHONDONTWRITEBYTECODE=1",
        f"XDG_RUNTIME_DIR=/run/user/{service_uid}",
        f"DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/{service_uid}/bus",
        f"KUBECONFIG={host.KUBECONFIG_PATH}",
        f"LOOM_STAGING_ROLLOUT_CONFIG={host.CONFIG_PATH}",
        str(venv / "bin/python"),
        "-I",
        "-B",
        "-c",
        program,
    ]


def _candidate_source_publication_argv(
    service_uid: int,
    operation: str,
    *,
    venv: Path = TEST_CANDIDATE_VENV,
) -> list[str]:
    return [
        "sudo",
        "-n",
        "-u",
        host.SERVICE_USER,
        "--",
        "/usr/bin/env",
        "-i",
        f"HOME={host.STATE_ROOT}",
        f"USER={host.SERVICE_USER}",
        f"LOGNAME={host.SERVICE_USER}",
        f"PATH={venv / 'bin'}:{host._ROOT_PATH}",
        "LANG=C.UTF-8",
        "LC_ALL=C.UTF-8",
        "PYTHONDONTWRITEBYTECODE=1",
        f"XDG_RUNTIME_DIR=/run/user/{service_uid}",
        f"DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/{service_uid}/bus",
        f"KUBECONFIG={host.KUBECONFIG_PATH}",
        f"LOOM_STAGING_ROLLOUT_CONFIG={host.CONFIG_PATH}",
        str(venv / "bin/python"),
        "-I",
        "-B",
        "-m",
        "loom_cli.rollout.operator.candidate_source_publication",
        operation,
    ]


def _candidate_source_publication_payload(action: str = "matched") -> str:
    payload: dict[str, object] = {
        "action": action,
        "candidate_sha": "a" * 40,
        "candidate_tree": "b" * 40,
        "image_tag": "staging-aaaaaaa",
        "service_uid": 1001,
        "status": "clean",
    }
    payload["evidence_sha256"] = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return json.dumps(payload, sort_keys=True) + "\n"


def _service_identity_result(
    call: list[str],
    *,
    uid: int = 1001,
    gid: int = 1002,
) -> host.CommandResult | None:
    if call == ["id", "-u", host.SERVICE_USER]:
        return host.CommandResult(0, f"{uid}\n")
    if call == ["getent", "passwd", host.SERVICE_USER]:
        return host.CommandResult(
            0,
            (f"{host.SERVICE_USER}:x:{uid}:{gid}::{host.STATE_ROOT}:{host.SERVICE_SHELL}\n"),
        )
    if call == ["getent", "group", host.SERVICE_GROUP]:
        return host.CommandResult(0, f"{host.SERVICE_GROUP}:x:{gid}:\n")
    if call == ["id", "-g", host.SERVICE_USER]:
        return host.CommandResult(0, f"{gid}\n")
    return None


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
            service_identity = _service_identity_result(call)
            if service_identity is not None:
                return service_identity
            if call == _runtime_probe_argv(1001, host._PACKAGE_RUNTIME_PROBE):
                events.append("broker-import")
                return host.CommandResult(0)
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
    monkeypatch.setattr(
        system,
        "harden_venv_lock",
        lambda _venv: events.append("harden-lock"),
    )
    monkeypatch.setattr(
        system,
        "venv_ready",
        lambda _venv: events.append("validate-authority") or True,
    )
    source_root = Path("/opt/loom-staging-runner/source")

    system.sync_venv(source_root, venv=TEST_CANDIDATE_VENV)

    sync_call, sync_kwargs = next(
        call for call in runner.calls if call[0][0] == "/usr/local/bin/uv"
    )
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
        "--reinstall-package",
        "loom",
        "--python",
        "/usr/bin/python3.12",
    ]
    assert "--frozen" not in sync_call
    assert sync_kwargs["env"] == {
        "PATH": host._ROOT_PATH,
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "UV_PROJECT_ENVIRONMENT": str(TEST_CANDIDATE_VENV),
    }
    assert events == ["uv-sync", "harden-lock", "validate-authority", "broker-import"]
    assert runner.calls[-5:] == [
        (["getent", "passwd", host.SERVICE_USER], {"check": False}),
        (["getent", "group", host.SERVICE_GROUP], {"check": False}),
        (["id", "-u", host.SERVICE_USER], {"check": False}),
        (["id", "-g", host.SERVICE_USER], {"check": False}),
        (_runtime_probe_argv(1001, host._PACKAGE_RUNTIME_PROBE), {"check": False}),
    ]


def test_sync_venv_rejects_broken_installed_broker_import(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class BrokenBrokerRunner:
        def run(self, argv, **kwargs):  # type: ignore[no-untyped-def]
            call = list(argv)
            if call[1:3] == ["-I", "-S"]:
                return host.CommandResult(0, "3.12\n")
            if call[0] == "/usr/local/bin/uv":
                return host.CommandResult(0)
            service_identity = _service_identity_result(call)
            if service_identity is not None:
                return service_identity
            assert call == _runtime_probe_argv(1001, host._PACKAGE_RUNTIME_PROBE)
            assert kwargs == {"check": False}
            return host.CommandResult(1, stderr="missing packaged schema")

    monkeypatch.setattr(
        host,
        "_safe_root_executable",
        lambda path, *, label: {
            host.SYSTEM_PYTHON: Path("/usr/bin/python3.12"),
            host.UV_BINARY: Path("/usr/local/bin/uv"),
        }[path],
    )
    system = host.HostSystem(BrokenBrokerRunner())
    monkeypatch.setattr(system, "harden_venv_lock", lambda _venv: None)
    monkeypatch.setattr(system, "venv_ready", lambda _venv: True)

    with pytest.raises(host.InstallError, match="broker import probe failed"):
        system.sync_venv(
            Path("/opt/loom-staging-runner/source"),
            venv=TEST_CANDIDATE_VENV,
        )


def test_broker_runtime_probe_loads_fixed_config_as_service_user() -> None:
    class ProbeRunner:
        def __init__(self) -> None:
            self.calls: list[tuple[list[str], dict[str, object]]] = []

        def run(self, argv, **kwargs):  # type: ignore[no-untyped-def]
            call = list(argv)
            self.calls.append((call, kwargs))
            service_identity = _service_identity_result(call)
            if service_identity is not None:
                return service_identity
            assert call == _runtime_probe_argv(1001, host._BROKER_RUNTIME_PROBE)
            assert "OperatorConfig.load(fixed_operator_config_path())" in call[-1]
            return host.CommandResult(0)

    runner = ProbeRunner()

    assert host.HostSystem(runner).broker_runtime_ready(TEST_CANDIDATE_VENV) is True
    assert runner.calls[-1][1] == {"check": False}


def test_gb10_trust_probe_binds_exact_candidate_runtime() -> None:
    class ProbeRunner:
        def __init__(self) -> None:
            self.calls: list[tuple[list[str], dict[str, object]]] = []

        def run(self, argv, **kwargs):  # type: ignore[no-untyped-def]
            self.calls.append((list(argv), dict(kwargs)))
            return host.CommandResult(0)

    runner = ProbeRunner()

    assert host.HostSystem(runner).gb10_trust_ready(
        TEST_CANDIDATE_VENV,
        TEST_CANDIDATE_SHA,
    )
    assert runner.calls == [
        (
            [
                str(TEST_CANDIDATE_VENV / "bin/python"),
                "-I",
                "-B",
                str(host._candidate_trust_tool_path(TEST_CANDIDATE_SHA)),
                "--ssh-config",
                str(host._candidate_ssh_config_path(TEST_CANDIDATE_SHA)),
                "check",
            ],
            {
                "check": False,
                "env": {
                    "PATH": host._ROOT_PATH,
                    "LANG": "C.UTF-8",
                    "LC_ALL": "C.UTF-8",
                    "PYTHONDONTWRITEBYTECODE": "1",
                    "HOME": str(host.STATE_ROOT),
                    "USER": host.SERVICE_USER,
                    "LOGNAME": host.SERVICE_USER,
                },
            },
        )
    ]


def test_gb10_active_host_reconcile_binds_exact_candidate_runtime() -> None:
    class ProbeRunner:
        def __init__(self) -> None:
            self.calls: list[tuple[list[str], dict[str, object]]] = []

        def run(self, argv, **kwargs):  # type: ignore[no-untyped-def]
            self.calls.append((list(argv), dict(kwargs)))
            return host.CommandResult(
                0,
                json.dumps(
                    {
                        "action": "reconcile-active-hosts",
                        "hosts": [{"host": "trt-gb10-7", "ok": True, "status": "present"}],
                        "ledger_hosts_remaining": 15,
                        "ok": True,
                        "remote_user": "qianyi",
                    }
                ),
            )

    runner = ProbeRunner()

    assert host.HostSystem(runner).reconcile_gb10_active_hosts(
        TEST_CANDIDATE_VENV,
        TEST_CANDIDATE_SHA,
    )
    assert runner.calls[0][0][-1] == "reconcile-active-hosts"
    assert runner.calls[0][0][0] == str(TEST_CANDIDATE_VENV / "bin/python")
    assert runner.calls[0][1]["env"]["PYTHONDONTWRITEBYTECODE"] == "1"


def test_service_git_disables_optional_locks_and_replace_refs() -> None:
    class GitRunner:
        def run(self, argv, **kwargs):  # type: ignore[no-untyped-def]
            call = list(argv)
            assert "GIT_NO_REPLACE_OBJECTS=1" in call
            assert "GIT_OPTIONAL_LOCKS=0" in call
            assert call[-2:] == ["status", "--porcelain=v1"]
            assert kwargs == {"check": False}
            return host.CommandResult(0)

    result = host.HostSystem(GitRunner())._service_git(
        "status",
        "--porcelain=v1",
        check=False,
    )

    assert result.returncode == 0


def test_candidate_source_publication_uses_fixed_service_user_boundary() -> None:
    class PublicationRunner:
        def __init__(self) -> None:
            self.calls: list[tuple[list[str], dict[str, object]]] = []

        def run(self, argv, **kwargs):  # type: ignore[no-untyped-def]
            call = list(argv)
            self.calls.append((call, kwargs))
            service_identity = _service_identity_result(call)
            if service_identity is not None:
                return service_identity
            assert call == _candidate_source_publication_argv(1001, "check")
            return host.CommandResult(0, _candidate_source_publication_payload())

    runner = PublicationRunner()

    assert host.HostSystem(runner).preflight_candidate_source_ready(TEST_CANDIDATE_VENV) is True
    assert runner.calls[-1][1] == {"check": False}


def test_candidate_source_publication_rejects_tampered_evidence_digest() -> None:
    class TamperedRunner:
        def run(self, argv, **kwargs):  # type: ignore[no-untyped-def]
            del kwargs
            call = list(argv)
            service_identity = _service_identity_result(call)
            if service_identity is not None:
                return service_identity
            payload = json.loads(_candidate_source_publication_payload())
            payload["candidate_tree"] = "c" * 40
            return host.CommandResult(0, json.dumps(payload) + "\n")

    with pytest.raises(host.InstallError, match="evidence digest"):
        host.HostSystem(TamperedRunner()).preflight_candidate_source_ready(TEST_CANDIDATE_VENV)


class ServiceAccountRunner:
    def __init__(self, *, present: bool, shell: str = host.SERVICE_SHELL) -> None:
        self.present = present
        self.shell = shell
        self.calls: list[list[str]] = []

    def run(self, argv, **kwargs):  # type: ignore[no-untyped-def]
        del kwargs
        call = list(argv)
        self.calls.append(call)
        if call == ["getent", "passwd", host.SERVICE_USER]:
            if not self.present:
                return host.CommandResult(2)
            return host.CommandResult(
                0,
                (f"{host.SERVICE_USER}:x:1001:1002::{host.STATE_ROOT}:{self.shell}\n"),
            )
        if call == ["getent", "group", host.SERVICE_GROUP]:
            return host.CommandResult(0, f"{host.SERVICE_GROUP}:x:1002:\n")
        if call == ["id", "-u", host.SERVICE_USER]:
            return host.CommandResult(0, "1001\n")
        if call == ["id", "-g", host.SERVICE_USER]:
            return host.CommandResult(0, "1002\n")
        if call == [
            "useradd",
            "--system",
            "--user-group",
            "--create-home",
            "--home-dir",
            str(host.STATE_ROOT),
            "--shell",
            host.SERVICE_SHELL,
            host.SERVICE_USER,
        ]:
            self.present = True
            self.shell = host.SERVICE_SHELL
            return host.CommandResult(0)
        if call == [
            "usermod",
            "--shell",
            host.SERVICE_SHELL,
            host.SERVICE_USER,
        ]:
            self.shell = host.SERVICE_SHELL
            return host.CommandResult(0)
        raise AssertionError(f"unexpected command: {call}")


def test_service_account_creation_uses_proxyjump_capable_shell() -> None:
    runner = ServiceAccountRunner(present=False)
    system = host.HostSystem(runner)

    assert system.ensure_service_user() is True
    assert runner.shell == host.SERVICE_SHELL
    assert [
        "useradd",
        "--system",
        "--user-group",
        "--create-home",
        "--home-dir",
        str(host.STATE_ROOT),
        "--shell",
        host.SERVICE_SHELL,
        host.SERVICE_USER,
    ] in runner.calls
    calls_after_convergence = list(runner.calls)

    assert system.ensure_service_user() is False
    assert not any(
        call[0] in {"useradd", "usermod"} for call in runner.calls[len(calls_after_convergence) :]
    )


def test_service_account_upgrades_legacy_nologin_shell() -> None:
    runner = ServiceAccountRunner(present=True, shell=host.LEGACY_SERVICE_SHELL)
    system = host.HostSystem(runner)

    assert system.ensure_service_user() is True
    assert [
        "usermod",
        "--shell",
        host.SERVICE_SHELL,
        host.SERVICE_USER,
    ] in runner.calls
    assert system.service_user_present() is True


def test_service_account_rejects_unexpected_shell() -> None:
    runner = ServiceAccountRunner(present=True, shell="/bin/bash")

    with pytest.raises(host.InstallError, match="unexpected identity, home, or shell"):
        host.HostSystem(runner).ensure_service_user()

    assert not any(call[0] in {"useradd", "usermod"} for call in runner.calls)


@pytest.mark.parametrize(
    ("passwd_gid", "group_gid", "id_gid"),
    [
        ("1001", "1002", "1001"),
        ("1001", "1001", "1002"),
        ("1002", "1001", "1001"),
    ],
)
def test_service_account_rejects_inconsistent_primary_gid_authorities(
    passwd_gid: str,
    group_gid: str,
    id_gid: str,
) -> None:
    class PrimaryGroupRunner:
        def run(self, argv, **kwargs):  # type: ignore[no-untyped-def]
            del kwargs
            call = list(argv)
            if call == ["getent", "passwd", host.SERVICE_USER]:
                return host.CommandResult(
                    0,
                    (
                        f"{host.SERVICE_USER}:x:1001:{passwd_gid}:"
                        f":{host.STATE_ROOT}:{host.SERVICE_SHELL}\n"
                    ),
                )
            if call == ["getent", "group", host.SERVICE_GROUP]:
                return host.CommandResult(0, f"{host.SERVICE_GROUP}:x:{group_gid}:\n")
            if call == ["id", "-u", host.SERVICE_USER]:
                return host.CommandResult(0, "1001\n")
            if call == ["id", "-g", host.SERVICE_USER]:
                return host.CommandResult(0, f"{id_gid}\n")
            raise AssertionError(f"unexpected command: {call}")

    with pytest.raises(host.InstallError, match="primary group is inconsistent"):
        host.HostSystem(PrimaryGroupRunner()).service_user_present()


@pytest.mark.parametrize(
    ("passwd_uid", "id_uid"),
    [("1001", "1002"), ("0", "0")],
)
def test_service_ids_reject_uid_mismatch_and_root_uid(
    passwd_uid: str,
    id_uid: str,
) -> None:
    class UidRunner:
        def run(self, argv, **kwargs):  # type: ignore[no-untyped-def]
            del kwargs
            call = list(argv)
            if call == ["getent", "passwd", host.SERVICE_USER]:
                return host.CommandResult(
                    0,
                    (
                        f"{host.SERVICE_USER}:x:{passwd_uid}:1002:"
                        f":{host.STATE_ROOT}:{host.SERVICE_SHELL}\n"
                    ),
                )
            if call == ["getent", "group", host.SERVICE_GROUP]:
                return host.CommandResult(0, f"{host.SERVICE_GROUP}:x:1002:\n")
            if call == ["id", "-u", host.SERVICE_USER]:
                return host.CommandResult(0, f"{id_uid}\n")
            if call == ["id", "-g", host.SERVICE_USER]:
                return host.CommandResult(0, "1002\n")
            raise AssertionError(f"unexpected command: {call}")

    with pytest.raises(host.InstallError, match="UID is inconsistent"):
        host.HostSystem(UidRunner())._service_ids()


def test_service_account_rejects_root_uid() -> None:
    class RootIdentityRunner:
        def run(self, argv, **kwargs):  # type: ignore[no-untyped-def]
            del kwargs
            call = list(argv)
            assert call == ["getent", "passwd", host.SERVICE_USER]
            return host.CommandResult(
                0,
                (f"{host.SERVICE_USER}:x:0:1002::{host.STATE_ROOT}:{host.SERVICE_SHELL}\n"),
            )

    with pytest.raises(host.InstallError, match="unexpected identity"):
        host.HostSystem(RootIdentityRunner()).service_user_present()


def test_config_authority_converges_to_root_owned_service_group_readable() -> None:
    class OwnershipRunner:
        def __init__(self) -> None:
            self.metadata = "root:root:600"
            self.calls: list[list[str]] = []

        def run(self, argv, **kwargs):  # type: ignore[no-untyped-def]
            del kwargs
            call = list(argv)
            self.calls.append(call)
            if call[:2] == ["stat", "-c"]:
                assert call[2] in {"%U:%G:%a", "%U:%G:%a:%h"}
                suffix = ":1" if call[2].endswith(":%h") else ""
                return host.CommandResult(0, self.metadata + suffix + "\n")
            if call == [
                "chown",
                f"root:{host.SERVICE_GROUP}",
                str(host.CONFIG_PATH),
            ]:
                self.metadata = f"root:{host.SERVICE_GROUP}:600"
                return host.CommandResult(0)
            if call == ["chmod", "0640", str(host.CONFIG_PATH)]:
                self.metadata = f"root:{host.SERVICE_GROUP}:640"
                return host.CommandResult(0)
            raise AssertionError(f"unexpected command: {call}")

    runner = OwnershipRunner()
    system = host.HostSystem(runner)

    assert system.install_owner(
        host.CONFIG_PATH,
        "root",
        0o640,
        group=host.SERVICE_GROUP,
    )
    assert system.file_owner_ready(
        host.CONFIG_PATH,
        owner="root",
        group=host.SERVICE_GROUP,
        mode=0o640,
        nlink=1,
    )


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
        if path == TEST_CANDIDATE_VENV:
            return os.stat_result((stat.S_IFDIR | 0o755, 11, 7, 1, 0, 0, 0, 0, 0, 0))
        assert path == TEST_CANDIDATE_VENV / ".lock"
        return metadata(mode)

    monkeypatch.setattr(host.os, "lstat", fake_lstat)
    monkeypatch.setattr(host.os, "open", fake_open)
    monkeypatch.setattr(host.os, "fstat", lambda fd: metadata(mode))
    monkeypatch.setattr(host.os, "fchmod", fake_fchmod)
    monkeypatch.setattr(host.os, "close", lambda fd: calls.append(("close", fd)))

    host.HostSystem(RecordingRunner()).harden_venv_lock(TEST_CANDIDATE_VENV)

    assert calls[0][0:2] == ("open", TEST_CANDIDATE_VENV / ".lock")
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
        if path == TEST_CANDIDATE_VENV:
            return os.stat_result((stat.S_IFDIR | 0o755, 11, 7, 1, 0, 0, 0, 0, 0, 0))
        assert path == TEST_CANDIDATE_VENV / ".lock"
        return metadata

    monkeypatch.setattr(host.os, "lstat", fake_lstat)
    monkeypatch.setattr(
        host.os,
        "open",
        lambda path, flags: pytest.fail("unsafe lock must be rejected before open"),
    )

    with pytest.raises(host.InstallError, match="lock authority is unsafe"):
        host.HostSystem(RecordingRunner()).harden_venv_lock(TEST_CANDIDATE_VENV)


def test_venv_lock_hardening_rejects_identity_change_after_open(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = os.stat_result((stat.S_IFDIR | 0o755, 11, 7, 1, 0, 0, 0, 0, 0, 0))
    before = os.stat_result((stat.S_IFREG | 0o666, 23, 17, 1, 0, 0, 0, 0, 0, 0))
    after = os.stat_result((stat.S_IFREG | 0o666, 24, 17, 1, 0, 0, 0, 0, 0, 0))
    closed: list[int] = []
    monkeypatch.setattr(
        host.os,
        "lstat",
        lambda path: root if path == TEST_CANDIDATE_VENV else before,
    )
    monkeypatch.setattr(host.os, "open", lambda path, flags: 41)
    monkeypatch.setattr(host.os, "fstat", lambda fd: after)
    monkeypatch.setattr(host.os, "close", lambda fd: closed.append(fd))

    with pytest.raises(host.InstallError, match="lock authority is unsafe"):
        host.HostSystem(RecordingRunner()).harden_venv_lock(TEST_CANDIDATE_VENV)

    assert closed == [41]


def test_venv_lock_hardening_fails_if_mode_does_not_converge(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = os.stat_result((stat.S_IFDIR | 0o755, 11, 7, 1, 0, 0, 0, 0, 0, 0))
    lock = os.stat_result((stat.S_IFREG | 0o666, 23, 17, 1, 0, 0, 0, 0, 0, 0))
    monkeypatch.setattr(
        host.os,
        "lstat",
        lambda path: root if path == TEST_CANDIDATE_VENV else lock,
    )
    monkeypatch.setattr(host.os, "open", lambda path, flags: 41)
    monkeypatch.setattr(host.os, "fstat", lambda fd: lock)
    monkeypatch.setattr(host.os, "fchmod", lambda fd, mode: None)
    monkeypatch.setattr(host.os, "close", lambda fd: None)

    with pytest.raises(host.InstallError, match="hardening did not converge"):
        host.HostSystem(RecordingRunner()).harden_venv_lock(TEST_CANDIDATE_VENV)


def test_venv_lock_hardening_converts_fchmod_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = os.stat_result((stat.S_IFDIR | 0o755, 11, 7, 1, 0, 0, 0, 0, 0, 0))
    lock = os.stat_result((stat.S_IFREG | 0o666, 23, 17, 1, 0, 0, 0, 0, 0, 0))
    monkeypatch.setattr(
        host.os,
        "lstat",
        lambda path: root if path == TEST_CANDIDATE_VENV else lock,
    )
    monkeypatch.setattr(host.os, "open", lambda path, flags: 41)
    monkeypatch.setattr(host.os, "fstat", lambda fd: lock)
    monkeypatch.setattr(
        host.os,
        "fchmod",
        lambda fd, mode: (_ for _ in ()).throw(OSError("injected chmod failure")),
    )
    monkeypatch.setattr(host.os, "close", lambda fd: None)

    with pytest.raises(host.InstallError, match="lock hardening failed"):
        host.HostSystem(RecordingRunner()).harden_venv_lock(TEST_CANDIDATE_VENV)


def test_venv_lock_hardening_converts_close_failure_without_masking_authority_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = os.stat_result((stat.S_IFDIR | 0o755, 11, 7, 1, 0, 0, 0, 0, 0, 0))
    before = os.stat_result((stat.S_IFREG | 0o600, 23, 17, 1, 0, 0, 0, 0, 0, 0))
    changed = os.stat_result((stat.S_IFREG | 0o600, 24, 17, 1, 0, 0, 0, 0, 0, 0))
    monkeypatch.setattr(
        host.os,
        "lstat",
        lambda path: root if path == TEST_CANDIDATE_VENV else before,
    )
    monkeypatch.setattr(host.os, "open", lambda path, flags: 41)
    monkeypatch.setattr(host.os, "close", lambda fd: (_ for _ in ()).throw(OSError("close")))

    monkeypatch.setattr(host.os, "fstat", lambda fd: before)
    with pytest.raises(host.InstallError, match="lock close failed"):
        host.HostSystem(RecordingRunner()).harden_venv_lock(TEST_CANDIDATE_VENV)

    monkeypatch.setattr(host.os, "fstat", lambda fd: changed)
    with pytest.raises(host.InstallError, match="lock authority is unsafe"):
        host.HostSystem(RecordingRunner()).harden_venv_lock(TEST_CANDIDATE_VENV)


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


def _status_metadata(mode: int, *, uid: int, gid: int) -> os.stat_result:
    return os.stat_result((mode, 11, 7, 1, uid, gid, 0, 0, 0, 0))


@pytest.mark.parametrize(
    ("unit_result", "expected"),
    [
        (host.CommandResult(0), "idle"),
        (
            host.CommandResult(
                0,
                "loom-staging-rollout-request-1.service loaded active running rollout\n",
            ),
            "busy",
        ),
        (
            host.CommandResult(
                0,
                "loom-staging-rollout-request-1.service loaded maintenance dead rollout\n",
            ),
            "busy",
        ),
        (
            host.CommandResult(
                0,
                "loom-staging-rollout-request-1.service loaded inactive dead rollout\n"
                "loom-staging-rollout-request-2.service loaded failed failed rollout\n",
            ),
            "idle",
        ),
        (host.CommandResult(1, stderr="manager unavailable"), "unknown"),
        (host.CommandResult(0, stderr="manager warning"), "unknown"),
        (host.CommandResult(0, "malformed-unit-output\n"), "unknown"),
    ],
)
def test_active_status_uses_protected_pointer_and_user_units_without_broker_import(
    monkeypatch: pytest.MonkeyPatch,
    unit_result: host.CommandResult,
    expected: str,
) -> None:
    service_uid = 1001
    service_gid = 1002

    class StatusRunner:
        def __init__(self) -> None:
            self.calls: list[tuple[list[str], dict[str, object]]] = []

        def run(self, argv, **kwargs):  # type: ignore[no-untyped-def]
            call = list(argv)
            self.calls.append((call, kwargs))
            service_identity = _service_identity_result(
                call,
                uid=service_uid,
                gid=service_gid,
            )
            if service_identity is not None:
                return service_identity
            return unit_result

    def fake_lstat(path: Path) -> os.stat_result:
        if path == host.MAINTENANCE_MARKER:
            return _status_metadata(stat.S_IFREG | 0o600, uid=0, gid=0)
        if path == host.STATE_ROOT:
            return _status_metadata(
                stat.S_IFDIR | 0o700,
                uid=service_uid,
                gid=service_gid,
            )
        assert path == host.ACTIVE_POINTER
        raise FileNotFoundError(path)

    monkeypatch.setattr(host.os, "lstat", fake_lstat)
    runner = StatusRunner()

    assert host.HostSystem(runner).active_status() == expected
    assert all(str(host.BROKER_PATH) not in call for call, _ in runner.calls)
    systemd_calls = [call for call in runner.calls if "/usr/bin/systemctl" in call[0]]
    assert systemd_calls == [
        (
            [
                "sudo",
                "-n",
                "-u",
                host.SERVICE_USER,
                "--",
                "/usr/bin/env",
                "-i",
                f"XDG_RUNTIME_DIR=/run/user/{service_uid}",
                f"DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/{service_uid}/bus",
                "PATH=/usr/bin:/bin",
                "LANG=C.UTF-8",
                "LC_ALL=C.UTF-8",
                "/usr/bin/systemctl",
                "--user",
                "list-units",
                "--all",
                "--plain",
                "--full",
                "--type=service",
                "--no-legend",
                "--no-pager",
                "loom-staging-rollout-*.service",
            ],
            {"check": False},
        )
    ]


def test_active_status_refuses_safe_active_pointer_without_querying_systemd(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service_uid = 1001
    service_gid = 1002

    class PointerRunner:
        def __init__(self) -> None:
            self.calls: list[list[str]] = []

        def run(self, argv, **kwargs):  # type: ignore[no-untyped-def]
            del kwargs
            call = list(argv)
            self.calls.append(call)
            service_identity = _service_identity_result(
                call,
                uid=service_uid,
                gid=service_gid,
            )
            assert service_identity is not None
            return service_identity

    def fake_lstat(path: Path) -> os.stat_result:
        if path == host.MAINTENANCE_MARKER:
            return _status_metadata(stat.S_IFREG | 0o600, uid=0, gid=0)
        if path == host.STATE_ROOT:
            return _status_metadata(
                stat.S_IFDIR | 0o700,
                uid=service_uid,
                gid=service_gid,
            )
        assert path == host.ACTIVE_POINTER
        return _status_metadata(
            stat.S_IFREG | 0o600,
            uid=service_uid,
            gid=service_gid,
        )

    monkeypatch.setattr(host.os, "lstat", fake_lstat)
    runner = PointerRunner()

    assert host.HostSystem(runner).active_status() == "busy"
    assert runner.calls == [
        ["getent", "passwd", host.SERVICE_USER],
        ["getent", "group", host.SERVICE_GROUP],
        ["id", "-u", host.SERVICE_USER],
        ["id", "-g", host.SERVICE_USER],
    ]


@pytest.mark.parametrize(
    ("unsafe_path", "unsafe_metadata"),
    [
        (
            host.STATE_ROOT,
            _status_metadata(stat.S_IFDIR | 0o755, uid=1001, gid=1002),
        ),
        (
            host.ACTIVE_POINTER,
            _status_metadata(stat.S_IFREG | 0o600, uid=0, gid=0),
        ),
    ],
)
def test_active_status_fails_closed_on_unsafe_state_authority(
    monkeypatch: pytest.MonkeyPatch,
    unsafe_path: Path,
    unsafe_metadata: os.stat_result,
) -> None:
    class StatusRunner:
        def run(self, argv, **kwargs):  # type: ignore[no-untyped-def]
            del kwargs
            call = list(argv)
            service_identity = _service_identity_result(call)
            if service_identity is not None:
                return service_identity
            raise AssertionError(f"unexpected systemd query: {call}")

    def fake_lstat(path: Path) -> os.stat_result:
        if path == unsafe_path:
            return unsafe_metadata
        if path == host.MAINTENANCE_MARKER:
            return _status_metadata(stat.S_IFREG | 0o600, uid=0, gid=0)
        if path == host.STATE_ROOT:
            return _status_metadata(stat.S_IFDIR | 0o700, uid=1001, gid=1002)
        assert path == host.ACTIVE_POINTER
        raise FileNotFoundError(path)

    monkeypatch.setattr(host.os, "lstat", fake_lstat)

    assert host.HostSystem(StatusRunner()).active_status() == "unknown"


def test_active_status_fails_closed_without_safe_maintenance_marker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class StatusRunner:
        def run(self, argv, **kwargs):  # type: ignore[no-untyped-def]
            del kwargs
            return host.CommandResult(0, "1001\n" if list(argv)[1] == "-u" else "1002\n")

    monkeypatch.setattr(
        host.os,
        "lstat",
        lambda path: (_ for _ in ()).throw(FileNotFoundError(path)),
    )

    assert host.HostSystem(StatusRunner()).active_status() == "unknown"


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
    metadata = _root_kubeconfig_metadata(payload, mode=0o644)
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
    source = b"apiVersion: v1\ncurrent-context: default\n"
    canonical = b"apiVersion: v1\ncurrent-context: loom-staging\n"
    snapshot_path: Path | None = None
    calls: list[list[str]] = []

    class KubeconfigRunner:
        def run(self, argv, **kwargs):  # type: ignore[no-untyped-def]
            nonlocal snapshot_path
            call = list(argv)
            calls.append(call)
            assert kwargs == {}
            assert call[0:2] == ["kubectl", "--kubeconfig"]
            snapshot_path = Path(call[2])
            assert snapshot_path.read_bytes() == source
            assert stat.S_IMODE(snapshot_path.stat().st_mode) == 0o600
            command = call[3:]
            if command == ["config", "current-context"]:
                return host.CommandResult(0, "default\n")
            if command == ["config", "rename-context", "default", "loom-staging"]:
                return host.CommandResult(0, "")
            assert command == ["config", "view", "--raw", "--minify"]
            return host.CommandResult(0, canonical.decode())

    monkeypatch.setattr(host, "_read_root_kubeconfig_source", lambda: source)
    monkeypatch.setattr(host, "ROOT_KUBECONFIG_SNAPSHOT_PARENT", tmp_path)

    assert host.HostSystem(KubeconfigRunner()).export_kubeconfig() == canonical
    assert [call[3:] for call in calls] == [
        ["config", "current-context"],
        ["config", "rename-context", "default", "loom-staging"],
        ["config", "view", "--raw", "--minify"],
    ]
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
            assert call == ["stat", "-c", "%F:%U:%G:%a", str(TEST_CANDIDATE_VENV)]
            return host.CommandResult(0, "directory:root:root:755\n")

    resolved = {
        host.SYSTEM_PYTHON: Path("/usr/bin/python3.13"),
        TEST_CANDIDATE_VENV / "bin/python": Path("/usr/bin/python3.12"),
    }
    monkeypatch.setattr(
        host.os.path,
        "lexists",
        lambda path: path in {TEST_CANDIDATE_VENV, TEST_CANDIDATE_VENV / "bin/python"},
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

    assert host.HostSystem(VenvRunner()).venv_ready(TEST_CANDIDATE_VENV) is False


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

    python = TEST_CANDIDATE_VENV / "bin/python"
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

    monkeypatch.setattr(
        host.os.path,
        "lexists",
        lambda path: path in {TEST_CANDIDATE_VENV, python},
    )
    monkeypatch.setattr(host, "_safe_root_executable", resolve_executable)
    monkeypatch.setattr(host, "_validate_owned_tree", validate_tree)

    assert host.HostSystem(VenvRunner()).venv_ready(TEST_CANDIDATE_VENV) is False
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

    monkeypatch.setattr(
        host,
        "_safe_root_executable",
        lambda path, *, label: Path("/usr/bin/python3.12"),
    )

    with pytest.raises(host.InstallError, match="link is unsafe"):
        host.HostSystem(VenvRunner()).venv_ready(venv)


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


class StatefulAclRunner:
    _TAG_NAMES: ClassVar[dict[str, str]] = {
        "u": "user",
        "g": "group",
        "m": "mask",
        "o": "other",
    }

    def __init__(self) -> None:
        self.calls: list[list[str]] = []
        self.acls: dict[str, dict[bool, dict[tuple[str, str], str]]] = {}
        self.passwd = {
            "qianyi": "qianyi:x:2009:2009::/home/qianyi:/bin/bash\n",
            "hongjian": "hongjian:x:2010:2010::/home/hongjian:/bin/bash\n",
            "devansh": "devansh:x:2011:2011::/home/devansh:/bin/bash\n",
        }

    def seed(
        self,
        path: Path,
        *,
        access: tuple[str, ...],
        default: tuple[str, ...] = (),
    ) -> None:
        self.acls[str(path)] = {
            False: host._acl_snapshot_map(access, allow_empty=False),
            True: host._acl_snapshot_map(default, allow_empty=True),
        }

    @staticmethod
    def _intersection(left: str, right: str) -> str:
        return "".join(
            left[index] if left[index] != "-" and right[index] != "-" else "-" for index in range(3)
        )

    def _render(self, path: str) -> str:
        namespaces = self.acls[path]
        lines: list[str] = []
        for default in (False, True):
            entries = namespaces[default]
            mask = entries.get(("mask", ""))
            for raw in host._canonical_acl_snapshot(entries):
                match = host._ACL_SNAPSHOT_ENTRY_RE.fullmatch(raw)
                assert match is not None
                tag, qualifier, permissions = match.groups()
                masked = (tag == "user" and bool(qualifier)) or tag == "group"
                effective = (
                    self._intersection(permissions, mask)
                    if masked and mask is not None
                    else permissions
                )
                prefix = "default:" if default else ""
                suffix = f"\t#effective:{effective}" if effective != permissions else ""
                lines.append(f"{prefix}{raw}{suffix}")
        return "\n".join(lines) + "\n"

    @classmethod
    def _parse_spec(
        cls,
        value: str,
        *,
        permissions_required: bool,
    ) -> tuple[bool, tuple[str, str], str | None]:
        fields = value.split(":")
        default = fields[0] == "d"
        if default:
            fields = fields[1:]
        tag = cls._TAG_NAMES[fields[0]]
        if permissions_required:
            permissions = fields[-1]
            qualifier = fields[1] if len(fields) == 3 else ""
            return default, (tag, qualifier), permissions
        qualifier = fields[1] if len(fields) == 2 else ""
        return default, (tag, qualifier), None

    def run(self, argv, **kwargs):  # type: ignore[no-untyped-def]
        del kwargs
        call = list(argv)
        self.calls.append(call)
        if call[0] == "getfacl":
            return host.CommandResult(0, self._render(call[-1]))
        if call[:2] == ["getent", "passwd"]:
            qualifier = call[2]
            if qualifier in self.passwd:
                return host.CommandResult(0, self.passwd[qualifier])
            for payload in self.passwd.values():
                if payload.split(":")[2] == qualifier:
                    return host.CommandResult(0, payload)
            return host.CommandResult(2)
        if call[:2] == ["setfacl", "-k"]:
            self.acls[call[-1]][True] = {}
            return host.CommandResult(0)
        if call[:2] == ["setfacl", "-n"]:
            path = call[-1]
            if "-m" in call:
                raw_modifiers = call[call.index("-m") + 1]
                for raw in raw_modifiers.split(","):
                    default, key, permissions = self._parse_spec(
                        raw,
                        permissions_required=True,
                    )
                    assert permissions is not None
                    self.acls[path][default][key] = permissions
            if "-x" in call:
                raw_removals = call[call.index("-x") + 1]
                for raw in raw_removals.split(","):
                    default, key, _ = self._parse_spec(raw, permissions_required=False)
                    self.acls[path][default].pop(key, None)
            return host.CommandResult(0)
        raise AssertionError(f"unexpected command: {call}")


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


def test_protected_input_acl_sanitizes_only_undeclared_named_readers() -> None:
    path = host.PROTECTED_INPUTS[0]
    before = (
        "user::rw-",
        "user:2012:r--",
        "user:loom-rollout:r--",
        "group::r--",
        "group:obsolete:r--",
        "mask::r--",
        "other::---",
    )
    runner = StatefulAclRunner()
    runner.seed(path, access=before)
    system = host.HostSystem(runner)

    plan = system._plan_acl(
        path,
        permissions="r--",
        default=False,
        sanitize_named_entries=True,
    )

    assert plan is not None
    assert plan.adds_service_entry is False
    assert plan.mask_adjustment is None
    assert plan.snapshot_adjustment is not None
    assert "group::r--" in plan.after_acl
    assert "user:loom-rollout:r--" in plan.after_acl
    assert all("2012" not in entry and "obsolete" not in entry for entry in plan.after_acl)
    system.apply_acl(plan)
    assert host._canonical_acl_snapshot(runner.acls[str(path)][False]) == plan.after_acl
    assert any(call[:4] == ["setfacl", "-n", "-x", "u:2012,g:obsolete"] for call in runner.calls)

    assert (
        system._plan_acl(
            path,
            permissions="r--",
            default=False,
            sanitize_named_entries=True,
        )
        is None
    )
    system.remove_acl(plan.grant, plan.snapshot_adjustment, remove_service_entry=False)
    assert host._canonical_acl_snapshot(runner.acls[str(path)][False]) == before


def test_protected_input_acl_sanitation_adds_service_and_restores_full_preimage() -> None:
    path = host.PROTECTED_INPUTS[1]
    before = (
        "user::rw-",
        "user:2012:r--",
        "group::r--",
        "mask::r--",
        "other::---",
    )
    runner = StatefulAclRunner()
    runner.seed(path, access=before)
    system = host.HostSystem(runner)

    plan = system._plan_acl(
        path,
        permissions="r--",
        default=False,
        sanitize_named_entries=True,
    )

    assert plan is not None and plan.snapshot_adjustment is not None
    assert plan.adds_service_entry is True
    system.apply_acl(plan)
    assert system._acl_entry(path, default=False) == ("r--", "r--")
    system.remove_acl(plan.grant, plan.snapshot_adjustment, remove_service_entry=True)
    assert host._canonical_acl_snapshot(runner.acls[str(path)][False]) == before


def test_acl_snapshot_adjustment_record_rejects_drift_and_unmanaged_scope() -> None:
    path = host.PROTECTED_INPUTS[0]
    adjustment = host.AclSnapshotAdjustment.from_dict(
        {
            "path": str(path),
            "before_acl": [
                "user::rw-",
                "user:2012:r--",
                "user:loom-rollout:r--",
                "group::r--",
                "mask::r--",
                "other::---",
            ],
            "after_acl": [
                "user::rw-",
                "user:loom-rollout:r--",
                "group::r--",
                "mask::r--",
                "other::---",
            ],
        }
    )
    assert host.AclSnapshotAdjustment.from_dict(adjustment.to_dict()) == adjustment
    assert host.HostInstaller._record_snapshot_adjustments(
        {"acl_snapshot_adjustments": [adjustment.to_dict()]}
    ) == {host.AclGrant(path): adjustment}

    value = adjustment.to_dict()
    value["path"] = "/tmp/unmanaged"
    with pytest.raises(host.InstallError, match="snapshot ledger"):
        host.HostInstaller._record_snapshot_adjustments({"acl_snapshot_adjustments": [value]})

    runner = StatefulAclRunner()
    runner.seed(path, access=adjustment.before_acl)
    system = host.HostSystem(runner)
    plan = host.AclPlan(
        grant=host.AclGrant(path),
        permissions="r--",
        adds_service_entry=False,
        before_acl=adjustment.before_acl,
        after_acl=adjustment.after_acl,
        snapshot_adjustment=adjustment,
    )
    runner.acls[str(path)][False][("user", "racer")] = "r--"
    with pytest.raises(host.InstallError, match="changed before convergence"):
        system.apply_acl(plan)
    assert all(call[0] != "setfacl" for call in runner.calls)


def test_acl_converges_masked_preexisting_service_and_restores_without_removing_it() -> None:
    path = host.PROTECTED_INPUTS[3]
    before = (
        "user::rw-",
        "user:loom-rollout:r--",
        "group::---",
        "mask::---",
        "other::---",
    )
    runner = StatefulAclRunner()
    runner.seed(path, access=before)
    system = host.HostSystem(runner)

    plan = system._plan_acl(path, permissions="r--", default=False)

    assert plan is not None
    assert plan.adds_service_entry is False
    assert plan.mask_adjustment is not None
    assert plan.mask_adjustment.before_mask == "---"
    assert plan.mask_adjustment.after_mask == "r--"
    system.apply_acl(plan)
    assert system._acl_entry(path, default=False) == ("r--", "r--")
    assert any("m::r--" in call for call in runner.calls if call[0] == "setfacl")

    system.remove_acl(
        plan.grant,
        plan.mask_adjustment,
        remove_service_entry=False,
    )

    assert host._canonical_acl_snapshot(runner.acls[str(path)][False]) == before


def test_acl_rollback_removes_ledgered_service_but_preserves_original_mask() -> None:
    path = host.PROTECTED_INPUTS[3]
    before = (
        "user::rw-",
        "user:loom-rollout:r--",
        "group::---",
        "mask::---",
        "other::---",
    )
    runner = StatefulAclRunner()
    runner.seed(path, access=before)
    system = host.HostSystem(runner)
    plan = system._plan_acl(path, permissions="r--", default=False)
    assert plan is not None and plan.mask_adjustment is not None
    system.apply_acl(plan)

    system.remove_acl(
        plan.grant,
        plan.mask_adjustment,
        remove_service_entry=True,
    )

    assert host._canonical_acl_snapshot(runner.acls[str(path)][False]) == (
        "user::rw-",
        "group::---",
        "mask::---",
        "other::---",
    )


def test_acl_mask_expansion_allows_only_declared_operator_gains() -> None:
    path = host.DATA_DIRECTORIES[1]
    runner = StatefulAclRunner()
    runner.seed(
        path,
        access=(
            "user::rwx",
            "user:devansh:rwx",
            "user:hongjian:rwx",
            "group::---",
            "mask::---",
            "other::---",
        ),
    )
    system = host.HostSystem(runner)

    plan = system._plan_acl(path, permissions="rwx", default=False)

    assert plan is not None and plan.mask_adjustment is not None
    assert plan.mask_adjustment.after_mask == "rwx"
    system.apply_acl(plan)
    rendered = runner._render(str(path))
    assert "user:devansh:rwx" in rendered
    assert "user:hongjian:rwx" in rendered
    assert "user:loom-rollout:rwx" in rendered
    assert "#effective" not in rendered


def test_acl_mask_expansion_preserves_uid_2012_without_new_effective_bits() -> None:
    path = host.DATA_DIRECTORIES[1]
    runner = StatefulAclRunner()
    runner.seed(
        path,
        access=(
            "user::rwx",
            "user:2012:r--",
            "group::---",
            "mask::r--",
            "other::---",
        ),
    )
    system = host.HostSystem(runner)

    plan = system._plan_acl(path, permissions="rwx", default=False)

    assert plan is not None and plan.mask_adjustment is not None
    system.apply_acl(plan)
    assert "user:2012:r--" in runner._render(str(path))


@pytest.mark.parametrize(
    "affected_entry",
    [
        "user:2012:r--",
        "user:undeclared:r--",
        "group:undeclared:r--",
        "group::r--",
    ],
)
def test_acl_mask_expansion_rejects_undeclared_principal_gain(
    affected_entry: str,
) -> None:
    path = host.PROTECTED_INPUTS[3]
    runner = StatefulAclRunner()
    base = ["user::rw-", "group::---", "mask::---", "other::---"]
    if affected_entry.startswith("group::"):
        base[1] = affected_entry
    else:
        base.insert(1, affected_entry)
    runner.seed(path, access=tuple(base))

    with pytest.raises(host.InstallError, match="undeclared principal"):
        host.HostSystem(runner)._plan_acl(path, permissions="r--", default=False)

    assert all(call[0] != "setfacl" for call in runner.calls)


def test_acl_mask_expansion_rejects_operator_name_resolved_to_uid_2012() -> None:
    path = host.PROTECTED_INPUTS[3]
    runner = StatefulAclRunner()
    runner.passwd["devansh"] = "devansh:x:2012:2012::/home/devansh:/bin/bash\n"
    runner.seed(
        path,
        access=(
            "user::rw-",
            "user:devansh:r--",
            "group::---",
            "mask::---",
            "other::---",
        ),
    )

    with pytest.raises(host.InstallError, match="undeclared principal"):
        host.HostSystem(runner)._plan_acl(path, permissions="r--", default=False)

    assert all(call[0] != "setfacl" for call in runner.calls)


def test_acl_initializes_and_fully_removes_installer_created_default_acl() -> None:
    path = host.DATA_DIRECTORIES[0]
    access = ("user::rwx", "group::rwx", "mask::---", "other::---")
    runner = StatefulAclRunner()
    runner.seed(path, access=access)
    system = host.HostSystem(runner)

    plan = system._plan_acl(path, permissions="rwx", default=True)

    assert plan is not None and plan.mask_adjustment is not None
    assert plan.before_acl == ()
    assert "group::---" in plan.after_acl
    system.apply_acl(plan)
    assert "default:user:loom-rollout:rwx" in runner._render(str(path))
    system.remove_acl(plan.grant, plan.mask_adjustment, remove_service_entry=True)
    assert runner.acls[str(path)][True] == {}
    assert host._canonical_acl_snapshot(runner.acls[str(path)][False]) == access
    assert ["setfacl", "-k", str(path)] in runner.calls


@pytest.mark.parametrize("default", [False, True])
def test_acl_rollback_removes_installer_created_mask_from_existing_base(
    default: bool,
) -> None:
    path = host.DATA_DIRECTORIES[0] if default else host.PROTECTED_INPUTS[4]
    access = ("user::rwx", "group::---", "other::---")
    baseline = access if default else ("user::rw-", "group::r--", "other::---")
    runner = StatefulAclRunner()
    runner.seed(
        path,
        access=access if default else baseline,
        default=baseline if default else (),
    )
    system = host.HostSystem(runner)
    permissions = "rwx" if default else "r--"

    plan = system._plan_acl(path, permissions=permissions, default=default)

    assert plan is not None and plan.mask_adjustment is not None
    assert plan.mask_adjustment.before_mask is None
    system.apply_acl(plan)
    system.remove_acl(plan.grant, plan.mask_adjustment, remove_service_entry=True)
    restored = system._acl_snapshot(system._acl_entries(path), default=default)
    assert restored == baseline


def test_acl_apply_rejects_plan_time_drift_before_setfacl() -> None:
    path = host.PROTECTED_INPUTS[3]
    runner = StatefulAclRunner()
    runner.seed(
        path,
        access=(
            "user::rw-",
            "user:loom-rollout:r--",
            "group::---",
            "mask::---",
            "other::---",
        ),
    )
    system = host.HostSystem(runner)
    plan = system._plan_acl(path, permissions="r--", default=False)
    assert plan is not None
    runner.acls[str(path)][False][("user", "undeclared")] = "r--"

    with pytest.raises(host.InstallError, match="changed before convergence"):
        system.apply_acl(plan)

    assert all(call[0] != "setfacl" for call in runner.calls)


def test_acl_apply_rejects_unexpected_readback_subject() -> None:
    class InjectingRunner(StatefulAclRunner):
        inject = False

        def run(self, argv, **kwargs):  # type: ignore[no-untyped-def]
            call = list(argv)
            if call[0] == "getfacl" and self.inject:
                self.acls[call[-1]][False][("user", "undeclared")] = "---"
                self.inject = False
            result = super().run(argv, **kwargs)
            if call[0] == "setfacl":
                self.inject = True
            return result

    path = host.PROTECTED_INPUTS[3]
    runner = InjectingRunner()
    runner.seed(path, access=("user::rw-", "group::---", "other::---"))
    system = host.HostSystem(runner)
    plan = system._plan_acl(path, permissions="r--", default=False)
    assert plan is not None

    with pytest.raises(host.InstallError, match="did not converge"):
        system.apply_acl(plan)


def test_acl_mask_adjustment_record_round_trip_and_validation() -> None:
    path = host.PROTECTED_INPUTS[3]
    runner = StatefulAclRunner()
    runner.seed(
        path,
        access=(
            "user::rw-",
            "user:loom-rollout:r--",
            "group::---",
            "mask::---",
            "other::---",
        ),
    )
    plan = host.HostSystem(runner)._plan_acl(path, permissions="r--", default=False)
    assert plan is not None and plan.mask_adjustment is not None
    value = plan.mask_adjustment.to_dict()

    assert host.AclMaskAdjustment.from_dict(value) == plan.mask_adjustment
    value["after_mask"] = "---"
    with pytest.raises(host.InstallError, match="inconsistent"):
        host.AclMaskAdjustment.from_dict(value)


def _fake_mask_plan(path: Path, *, service_preexisting: bool = False) -> host.AclPlan:
    before = ["user::rw-"]
    if service_preexisting:
        before.append("user:loom-rollout:r--")
    before.extend(["group::---", "mask::---", "other::---"])
    after = ["user::rw-", "user:loom-rollout:r--", "group::---", "mask::r--", "other::---"]
    adjustment = host.AclMaskAdjustment.from_dict(
        {
            "path": str(path),
            "default": False,
            "before_mask": "---",
            "after_mask": "r--",
            "before_acl": before,
            "after_acl": after,
        }
    )
    return host.AclPlan(
        grant=host.AclGrant(path),
        permissions="r--",
        adds_service_entry=not service_preexisting,
        before_acl=adjustment.before_acl,
        after_acl=adjustment.after_acl,
        mask_adjustment=adjustment,
    )


def _fake_snapshot_plan(path: Path, *, service_preexisting: bool = False) -> host.AclPlan:
    before = ["user::rw-", "user:2012:r--"]
    if service_preexisting:
        before.append("user:loom-rollout:r--")
    before.extend(["group::r--", "mask::r--", "other::---"])
    after = ["user::rw-", "user:loom-rollout:r--", "group::r--", "mask::r--", "other::---"]
    adjustment = host.AclSnapshotAdjustment.from_dict(
        {
            "path": str(path),
            "before_acl": before,
            "after_acl": after,
        }
    )
    return host.AclPlan(
        grant=host.AclGrant(path),
        permissions="r--",
        adds_service_entry=not service_preexisting,
        before_acl=adjustment.before_acl,
        after_acl=adjustment.after_acl,
        snapshot_adjustment=adjustment,
    )


def test_install_persists_snapshot_preimage_before_acl_sanitation_and_uninstall_restores_it(
    tmp_path: Path,
) -> None:
    installer, system = _installer(tmp_path)
    path = host.PROTECTED_INPUTS[0]
    plan = _fake_snapshot_plan(path)
    assert plan.snapshot_adjustment is not None

    def planned_input(candidate: Path) -> tuple[host.AclPlan, ...]:
        if candidate != path or system.acl_adjustment_states.get(plan.grant) == "after":
            return ()
        return (plan,)

    system.plan_input_acl = planned_input  # type: ignore[method-assign]
    system.plan_data_acl = lambda candidate: ()  # type: ignore[method-assign]
    original_apply = system.apply_acl

    def assert_preimage_persisted(candidate: host.AclPlan) -> host.AclGrant:
        record = installer.filesystem.load_install_record()
        assert record is not None
        assert record["installation_state"] == "installing"
        assert record["acl_snapshot_adjustments"] == [plan.snapshot_adjustment.to_dict()]
        return original_apply(candidate)

    system.apply_acl = assert_preimage_persisted  # type: ignore[method-assign]
    assert installer.install(TEAM_ID)["ok"] is True
    ready = installer.filesystem.load_install_record()
    assert ready is not None
    assert host.HostInstaller._record_snapshot_adjustments(ready) == {
        plan.grant: plan.snapshot_adjustment
    }

    installer.uninstall(retain_ledger=True)
    assert system.acl_adjustment_states[plan.grant] == "before"
    assert path not in system.input_acls


def test_partial_acl_apply_persists_all_mask_preimages_and_retries(tmp_path: Path) -> None:
    installer, system = _installer(tmp_path)
    plans = {
        host.PROTECTED_INPUTS[0]: _fake_mask_plan(host.PROTECTED_INPUTS[0]),
        host.PROTECTED_INPUTS[1]: _fake_mask_plan(host.PROTECTED_INPUTS[1]),
    }

    def planned_input(path: Path) -> tuple[host.AclPlan, ...]:
        plan = plans.get(path)
        if plan is None:
            return ()
        if system.acl_adjustment_states.get(plan.grant) == "after":
            return ()
        return (plan,)

    system.plan_input_acl = planned_input  # type: ignore[method-assign]
    system.plan_data_acl = lambda path: ()  # type: ignore[method-assign]
    original_apply = system.apply_acl
    calls = 0

    def fail_before_second(plan: host.AclPlan) -> host.AclGrant:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise host.InstallError("injected ACL mask failure")
        return original_apply(plan)

    system.apply_acl = fail_before_second  # type: ignore[method-assign]
    with pytest.raises(host.InstallError, match="ACL mask failure"):
        installer.install(TEAM_ID)

    interrupted = installer.filesystem.load_install_record()
    assert interrupted is not None
    assert interrupted["installation_state"] == "installing"
    assert len(interrupted["acl_mask_adjustments"]) == 2
    assert system.acl_adjustment_states[plans[host.PROTECTED_INPUTS[0]].grant] == "after"
    assert plans[host.PROTECTED_INPUTS[1]].grant not in system.acl_adjustment_states

    system.apply_acl = original_apply  # type: ignore[method-assign]
    result = installer.install(TEAM_ID)

    assert result["ok"] is True
    ready = installer.filesystem.load_install_record()
    assert ready is not None
    assert ready["installation_state"] == "ready"
    assert len(ready["acl_mask_adjustments"]) == 2
    assert set(system.acl_adjustment_states.values()) == {"after"}

    installer.uninstall(retain_ledger=True)
    assert set(system.acl_adjustment_states.values()) == {"before"}
    assert system.input_acls == set()


def test_preexisting_masked_service_acl_is_not_claimed_by_install_ledger(
    tmp_path: Path,
) -> None:
    installer, system = _installer(tmp_path)
    path = host.PROTECTED_INPUTS[3]
    plan = _fake_mask_plan(path, service_preexisting=True)
    system.input_acls.add(path)
    system.plan_input_acl = (  # type: ignore[method-assign]
        lambda candidate: (plan,) if candidate == path else ()
    )
    system.plan_data_acl = lambda candidate: ()  # type: ignore[method-assign]

    installer.install(TEAM_ID)
    record = installer.filesystem.load_install_record()
    assert record is not None
    assert plan.grant not in host.HostInstaller._record_grants(record)
    assert plan.grant in host.HostInstaller._record_mask_adjustments(record)

    installer.uninstall(retain_ledger=True)
    assert path in system.input_acls
    assert system.acl_adjustment_states[plan.grant] == "before"


def test_check_reports_recorded_acl_mask_drift(tmp_path: Path) -> None:
    installer, system = _installer(tmp_path)
    path = host.PROTECTED_INPUTS[3]
    plan = _fake_mask_plan(path)

    def planned_input(candidate: Path) -> tuple[host.AclPlan, ...]:
        if candidate != path or system.acl_adjustment_states.get(plan.grant) == "after":
            return ()
        return (plan,)

    system.plan_input_acl = planned_input  # type: ignore[method-assign]
    system.plan_data_acl = lambda candidate: ()  # type: ignore[method-assign]
    installer.install(TEAM_ID)
    assert installer.check()["ok"] is True

    system.acl_adjustment_states[plan.grant] = "drift"
    result = installer.check()

    assert result["ok"] is False
    assert f"acl-mask:access:{path}" in result["failures"]


def test_acl_mask_ledger_rejects_duplicates_and_unmanaged_paths() -> None:
    managed = _fake_mask_plan(host.PROTECTED_INPUTS[0]).mask_adjustment
    assert managed is not None
    value = managed.to_dict()
    with pytest.raises(host.InstallError, match="mask ledger"):
        host.HostInstaller._record_mask_adjustments({"acl_mask_adjustments": [value, value]})

    value["path"] = "/tmp/unmanaged"
    with pytest.raises(host.InstallError, match="mask ledger"):
        host.HostInstaller._record_mask_adjustments({"acl_mask_adjustments": [value]})

    for schema_version in (1, 2):
        assert host.HostInstaller._record_mask_adjustments({"schema_version": schema_version}) == {}
        with pytest.raises(host.InstallError, match="legacy install record"):
            host.HostInstaller._record_mask_adjustments(
                {"schema_version": schema_version, "acl_mask_adjustments": []}
            )


def test_uninstall_rejects_cross_ledger_ownership_drift_before_mutation(
    tmp_path: Path,
) -> None:
    installer, system = _installer(tmp_path)
    path = host.PROTECTED_INPUTS[3]
    plan = _fake_mask_plan(path)
    system.plan_input_acl = (  # type: ignore[method-assign]
        lambda candidate: (plan,) if candidate == path else ()
    )
    system.plan_data_acl = lambda candidate: ()  # type: ignore[method-assign]
    installer.install(TEAM_ID)
    record = installer.filesystem.load_install_record()
    assert record is not None
    record["added_acls"] = [value for value in record["added_acls"] if value["path"] != str(path)]
    installer.filesystem.atomic_write(
        host.INSTALL_RECORD,
        (json.dumps(record, sort_keys=True) + "\n").encode(),
        0o600,
    )

    with pytest.raises(host.InstallError, match="ACL ledgers are inconsistent"):
        installer.uninstall(retain_ledger=True)

    assert installer.filesystem.exists(host.SUDOERS_PATH)
    assert system.maintenance_begins == 1  # install only
    assert system.revoked is False
    assert path in system.input_acls


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
    monkeypatch.setattr(host, "_validate_root_authority_parent_chain", lambda path: None)
    monkeypatch.setattr(host, "_validate_git_checkout_tree", lambda *args, **kwargs: None)

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
    monkeypatch.setattr(host, "_validate_root_authority_parent_chain", lambda path: None)
    monkeypatch.setattr(host, "_validate_git_checkout_tree", lambda *args, **kwargs: None)

    assert host.HostSystem(runner).prepare_install_source() == (source, new_sha)
    assert runner.head_sha == old_sha

    assert any(call[:4] == ["git", "-C", str(source), "fetch"] for call in runner.calls)
    assert all("checkout" not in call for call in runner.calls)


def test_dirty_invocation_checkout_is_rejected_before_source_install(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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

    monkeypatch.setattr(host, "_validate_root_authority_parent_chain", lambda path: None)
    monkeypatch.setattr(host, "_validate_git_checkout_tree", lambda *args, **kwargs: None)

    with pytest.raises(host.InstallError, match="dirty"):
        host.HostSystem(DirtyRunner()).validate_invocation_checkout()


def test_invocation_checkout_rejects_replaceable_root_authority_before_git(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class NoGitRunner:
        def run(self, argv, **kwargs):  # type: ignore[no-untyped-def]
            del argv, kwargs
            raise AssertionError("Git ran before invocation authority validation")

    def reject(path: Path) -> None:
        assert path == host.REPO_ROOT
        raise host.InstallError("root authority parent is unsafe")

    monkeypatch.setattr(host, "_validate_root_authority_parent_chain", reject)

    with pytest.raises(host.InstallError, match="root authority parent is unsafe"):
        host.HostSystem(NoGitRunner()).validate_invocation_checkout()


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


def test_maintenance_marker_waits_for_inflight_launch_guard(tmp_path: Path) -> None:
    runtime = tmp_path / "runtime"
    runtime.mkdir(mode=0o700)
    uid, gid = os.geteuid(), os.getegid()
    lock_fd = os.open(runtime / "launch.lock", os.O_RDWR | os.O_CREAT, 0o600)
    os.fchmod(lock_fd, 0o600)
    host.fcntl.flock(lock_fd, host.fcntl.LOCK_EX)
    started = threading.Event()
    finished = threading.Event()
    failures: list[BaseException] = []

    def publish_marker() -> None:
        started.set()
        try:
            host._maintenance_marker(
                runtime,
                service_uid=uid,
                service_gid=gid,
                authority_uid=uid,
                authority_gid=gid,
                enabled=True,
            )
        except BaseException as exc:  # pragma: no cover - asserted below
            failures.append(exc)
        finally:
            finished.set()

    worker = threading.Thread(target=publish_marker)
    worker.start()
    try:
        assert started.wait(timeout=2)
        assert not finished.wait(timeout=0.1)
    finally:
        host.fcntl.flock(lock_fd, host.fcntl.LOCK_UN)
        os.close(lock_fd)
    assert finished.wait(timeout=2)
    worker.join(timeout=2)

    assert not failures
    assert (runtime / "maintenance").is_file()


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


def test_owned_tree_hardening_removes_only_group_world_write_bits(tmp_path: Path) -> None:
    uid, gid = os.geteuid(), os.getegid()
    root = tmp_path / "candidate"
    nested = root / ".git"
    nested.mkdir(parents=True, mode=0o775)
    config = nested / "config"
    config.write_text('[remote "origin"]\n', encoding="utf-8")
    config.chmod(0o664)
    executable = root / "run.sh"
    executable.write_text("#!/bin/sh\n", encoding="utf-8")
    executable.chmod(0o775)

    assert host._harden_owned_tree(root, expected_uid=uid, expected_gid=gid) is True
    assert stat.S_IMODE(nested.stat().st_mode) == 0o755
    assert stat.S_IMODE(config.stat().st_mode) == 0o644
    assert stat.S_IMODE(executable.stat().st_mode) == 0o755
    assert host._harden_owned_tree(root, expected_uid=uid, expected_gid=gid) is False


def test_git_checkout_authority_rejects_external_worktree_pointer(tmp_path: Path) -> None:
    uid, gid = os.geteuid(), os.getegid()
    root = tmp_path / "checkout"
    root.mkdir(mode=0o700)
    (root / ".git").write_text("gitdir: /outside/repository\n", encoding="utf-8")

    with pytest.raises(host.InstallError, match="Git authority directory is unsafe"):
        host._validate_git_checkout_tree(root, expected_uid=uid, expected_gid=gid)


def test_candidate_convergence_hardens_existing_checkout_and_uses_fixed_umask(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    uid, gid = os.geteuid(), os.getegid()
    sha = "a" * 40
    candidate = tmp_path / sha / "repo"
    git_dir = candidate / ".git"
    git_dir.mkdir(parents=True, mode=0o775)
    config = git_dir / "config"
    config.write_text('[remote "origin"]\n', encoding="utf-8")
    config.chmod(0o664)
    tracked = candidate / "tracked.py"
    tracked.write_text("pass\n", encoding="utf-8")
    tracked.chmod(0o664)

    class CandidateRunner:
        def __init__(self) -> None:
            self.calls: list[list[str]] = []

        def run(self, argv, **kwargs):  # type: ignore[no-untyped-def]
            del kwargs
            call = list(argv)
            self.calls.append(call)
            service_identity = _service_identity_result(call, uid=uid, gid=gid)
            if service_identity is not None:
                return service_identity
            if call[:2] == ["test", "-d"]:
                return host.CommandResult(0)
            git_index = call.index("/usr/bin/git")
            arguments = call[git_index + 1 :]
            if arguments[-1:] == ["remote"]:
                return host.CommandResult(0, "origin\n")
            if arguments[-1:] == ["remote.origin.url"]:
                return host.CommandResult(0, host.REMOTE_URL + "\n")
            if arguments[-1:] == ["remote.origin.pushurl"]:
                return host.CommandResult(1)
            if "status" in arguments:
                return host.CommandResult(0)
            if arguments[-2:] == ["rev-parse", "HEAD"]:
                return host.CommandResult(0, sha + "\n")
            raise AssertionError(f"unexpected command: {call}")

    monkeypatch.setattr(host, "CANDIDATE_RUNTIME_ROOT", tmp_path)
    runner = CandidateRunner()

    assert host.HostSystem(runner).ensure_candidate(sha, refresh=False) is True
    assert stat.S_IMODE(git_dir.stat().st_mode) == 0o755
    assert stat.S_IMODE(config.stat().st_mode) == 0o644
    assert stat.S_IMODE(tracked.stat().st_mode) == 0o644
    assert all(
        {
            "GIT_CONFIG_NOSYSTEM=1",
            "GIT_CONFIG_GLOBAL=/dev/null",
            "GIT_TERMINAL_PROMPT=0",
        }.issubset(call)
        for call in runner.calls
        if "/usr/bin/git" in call
    )


def test_sealed_candidate_fetch_uses_fixed_install_source_upload_pack(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    uid, gid = os.geteuid(), os.getegid()
    sha = "a" * 40
    tree = "b" * 40
    base = "c" * 40
    candidate = tmp_path / sha / "repo"
    (candidate / ".git").mkdir(parents=True)

    class SealedCandidateRunner:
        def __init__(self) -> None:
            self.calls: list[list[str]] = []

        def run(self, argv, **kwargs):  # type: ignore[no-untyped-def]
            del kwargs
            call = list(argv)
            self.calls.append(call)
            service_identity = _service_identity_result(call, uid=uid, gid=gid)
            if service_identity is not None:
                return service_identity
            if call[:2] == ["test", "-d"]:
                return host.CommandResult(0)
            git_index = call.index("/usr/bin/git")
            arguments = call[git_index + 1 :]
            if arguments[-1:] == ["remote"]:
                return host.CommandResult(0, "origin\n")
            if arguments[-1:] == ["remote.origin.url"]:
                return host.CommandResult(0, host.REMOTE_URL + "\n")
            if arguments[-1:] == ["remote.origin.pushurl"]:
                return host.CommandResult(1)
            if "status" in arguments or "checkout" in arguments or "fetch" in arguments:
                return host.CommandResult(0)
            if arguments[-2:] == ["rev-parse", "HEAD"]:
                return host.CommandResult(0, sha + "\n")
            if arguments[-2:] == ["rev-parse", f"{sha}^{{tree}}"]:
                return host.CommandResult(0, tree + "\n")
            if "merge-base" in arguments:
                return host.CommandResult(0, base + "\n")
            if "rev-list" in arguments:
                return host.CommandResult(0, f"{sha} {base}\n")
            raise AssertionError(f"unexpected command: {call}")

    monkeypatch.setattr(host, "CANDIDATE_RUNTIME_ROOT", tmp_path)
    monkeypatch.setattr(host, "_validate_git_checkout_tree", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(host, "_harden_owned_tree", lambda *_args, **_kwargs: False)
    runner = SealedCandidateRunner()

    assert (
        host.HostSystem(runner).ensure_candidate(
            sha,
            refresh=True,
            source_tree_sha=tree,
            source_base_sha=base,
        )
        is True
    )
    fetch = next(call for call in runner.calls if "fetch" in call)
    git_index = fetch.index("/usr/bin/git")
    arguments = fetch[git_index + 1 :]
    assert arguments == [
        "-C",
        str(candidate),
        "fetch",
        "--no-tags",
        "--no-recurse-submodules",
        f"--upload-pack={host.SEALED_SOURCE_UPLOAD_PACK}",
        str(host.INSTALL_SOURCE),
        sha,
    ]
    assert host.SEALED_SOURCE_UPLOAD_PACK == (
        "/usr/bin/git -c safe.directory=/opt/loom-staging-runner/source/.git upload-pack"
    )
    assert all(
        call[call.index("/bin/sh") : call.index("/usr/bin/git") + 1]
        == [
            "/bin/sh",
            "-c",
            'umask 022; exec "$@"',
            "loom-staging-root-git",
            "/usr/bin/git",
        ]
        for call in runner.calls
        if "/usr/bin/git" in call
    )
