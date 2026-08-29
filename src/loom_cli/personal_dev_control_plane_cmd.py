"""Render and observe the zero-capacity personal-development management plane."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import selectors
import stat
import subprocess
import sys
import tempfile
import time
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, BinaryIO, Protocol

import yaml  # type: ignore[import-untyped]

from loom.personal_dev_acceptance_evidence import (
    build_personal_dev_backup_restore_evidence,
    build_personal_dev_scanner_finding_policy,
    build_personal_dev_trusted_launcher_profile,
    load_personal_dev_acceptance_result,
    load_personal_dev_backup_restore_evidence,
    load_personal_dev_rollback_shadow_status,
    validate_personal_dev_policy_evidence,
    validate_personal_dev_rollback_shadow_manifest,
)
from loom.personal_dev_control_plane_config import (
    PersonalDevAcceptancePlan,
    PersonalDevControlPlaneProfile,
    PersonalDevOperationalPlan,
    PersonalDevTrustedRelease,
    load_personal_dev_acceptance_plan,
    load_personal_dev_control_plane_profile,
    load_personal_dev_operational_plan,
    load_personal_dev_trusted_release,
)
from loom.personal_dev_control_plane_render import (
    render_acceptance_personal_dev_control_plane,
    render_operational_personal_dev_control_plane,
    render_shadow_personal_dev_control_plane,
)
from loom.personal_dev_control_plane_status import (
    MAX_PERSONAL_DEV_STATUS_RESPONSE_BYTES,
    observe_personal_dev_acceptance_status,
    observe_personal_dev_operational_status,
    observe_personal_dev_shadow_status,
)
from loom.personal_dev_schema_transition import (
    prepare_personal_dev_schema_transition,
    validate_personal_dev_schema_transition_source_root,
)
from loom_cli.personal_dev_minio_backup_cmd import (
    PersonalDevMinioCommandResult,
    capture_personal_dev_minio_backup,
    restore_personal_dev_minio_backup,
)

_RENDER_ERROR = "error: personal-dev control-plane render inputs are invalid\n"
_STATUS_ERROR = "error: personal-dev control-plane status inputs are invalid\n"
_EVIDENCE_ERROR = "error: personal-dev acceptance evidence inputs are invalid\n"
_SCHEMA_TRANSITION_ERROR = "error: personal-dev schema transition inputs are invalid\n"
_VERIFICATION_ERROR = "error: personal-dev acceptance result inputs are invalid\n"
_MINIO_BACKUP_ERROR = "error: personal-dev MinIO backup inputs are invalid\n"
_KUBECTL_READ_BYTES = 64 * 1024
_MAX_KUBECONFIG_BYTES = 1024 * 1024
_MAX_MINIO_STDERR_BYTES = 64 * 1024
_MINIO_POD_NAME_RE = re.compile(r"[a-z0-9](?:[-a-z0-9.]{0,251}[a-z0-9])?")
_KUBECTL_MINIO_WRAPPER = (
    'export MC_HOST_local="http://${MINIO_ROOT_USER}:${MINIO_ROOT_PASSWORD}'
    '@127.0.0.1:9000"; exec mc "$@"'
)
_DOCKER_MINIO_WRAPPER = (
    'export MC_HOST_restore="http://${MINIO_ROOT_USER}:${MINIO_ROOT_PASSWORD}'
    '@minio-restore:9000"; exec mc "$@"'
)
_DOCKER_PAYLOAD_ROOT = "/loom-payloads"
_MAX_DOCKER_INSPECT_BYTES = 1024 * 1024
_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_STDERR_PRESENT = b"\x01"
_KubeconfigIdentity = tuple[int, int, int, int, int, int, int, int, int]
_DirectoryIdentity = tuple[int, int, int, int, int]


class _BinaryWriteDestination(Protocol):
    def write(self, payload: bytes | memoryview, /) -> int: ...


def _render(args: argparse.Namespace) -> int:
    try:
        profile = load_personal_dev_control_plane_profile(args.file)
        release = load_personal_dev_trusted_release(
            args.trusted_release_file,
            args.trusted_release_sha256,
        )
        rendered = render_shadow_personal_dev_control_plane(profile, release)
        evidence = json.dumps(
            {
                "input_sha256": rendered.input_sha256,
                "mode": "shadow",
                "release_sha256": rendered.release_sha256,
                "resource_count": rendered.resource_count,
                "schema": "loom-personal-dev-control-plane-render-v1",
                "source_sha": release.source_sha,
                "source_tree": release.source_tree,
                "yaml_sha256": hashlib.sha256(rendered.yaml_text.encode("utf-8")).hexdigest(),
            },
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )
    except (OSError, TypeError, ValueError):
        sys.stderr.write(_RENDER_ERROR)
        return 2

    try:
        sys.stdout.write(rendered.yaml_text)
    except BrokenPipeError:
        return 0
    sys.stderr.write(evidence + "\n")
    return 0


def _render_policy_evidence(args: argparse.Namespace) -> int:
    try:
        profile = load_personal_dev_control_plane_profile(args.file)
        release = load_personal_dev_trusted_release(
            args.trusted_release_file,
            args.trusted_release_sha256,
        )
        if args.personal_dev_control_plane_op == "render-trusted-launcher-profile":
            value = build_personal_dev_trusted_launcher_profile(
                profile=profile,
                release=release,
                source_root=args.source_root,
            )
            kind = "trusted-launcher-profile"
        else:
            value = build_personal_dev_scanner_finding_policy(
                profile=profile,
                release=release,
                source_root=args.source_root,
            )
            kind = "scanner-finding-policy"
        payload = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )
        digest = hashlib.sha256(payload.encode("ascii")).hexdigest()
    except (OSError, TypeError, ValueError):
        sys.stderr.write(_EVIDENCE_ERROR)
        return 2
    try:
        sys.stdout.write(payload)
    except BrokenPipeError:
        return 0
    sys.stderr.write(
        json.dumps(
            {
                "kind": kind,
                "schema": "loom-personal-dev-policy-evidence-render-v1",
                "sha256": digest,
                "source_sha": release.source_sha,
                "source_tree": release.source_tree,
            },
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )
        + "\n"
    )
    return 0


def _assert_isolated_restore_cleanup(args: argparse.Namespace) -> None:
    containers = subprocess.run(
        ["docker", "ps", "-a", "--format", "{{.Names}}"],
        check=True,
        capture_output=True,
        text=True,
        timeout=15,
    )
    networks = subprocess.run(
        ["docker", "network", "ls", "--format", "{{.Name}}"],
        check=True,
        capture_output=True,
        text=True,
        timeout=15,
    )
    live_containers = set(containers.stdout.splitlines())
    live_networks = set(networks.stdout.splitlines())
    if (
        args.isolated_postgres_name in live_containers
        or args.isolated_minio_name in live_containers
        or args.isolated_network_name in live_networks
    ):
        raise ValueError("isolated restore cleanup is incomplete")


def _render_backup_restore_evidence(args: argparse.Namespace) -> int:
    try:
        profile = load_personal_dev_control_plane_profile(args.file)
        release = load_personal_dev_trusted_release(
            args.trusted_release_file,
            args.trusted_release_sha256,
        )
        suffix = args.trusted_release_sha256[:12]
        if (
            args.isolated_postgres_name != f"loom-personal-dev-pg-restore-{suffix}"
            or args.isolated_minio_name != f"loom-personal-dev-minio-restore-{suffix}"
            or args.isolated_network_name != f"loom-personal-dev-restore-{suffix}"
        ):
            raise ValueError("isolated restore identity is not release-bound")
        _assert_isolated_restore_cleanup(args)
        value = build_personal_dev_backup_restore_evidence(
            profile=profile,
            release=release,
            release_sha256=args.trusted_release_sha256,
            started_at=args.started_at,
            completed_at=args.completed_at,
            postgres_dump_path=args.postgres_dump_file,
            postgres_source_state_path=args.postgres_source_state_file,
            postgres_restored_state_path=args.postgres_restored_state_file,
            source_schema_head=args.source_schema_head,
            restored_schema_head=args.restored_schema_head,
            minio_source_manifest_path=args.minio_source_manifest_file,
            minio_restored_manifest_path=args.minio_restored_manifest_file,
            minio_payload_root=args.minio_payload_root,
            secret_key_inventory_path=args.secret_key_inventory_file,
            pre_shadow_status_path=args.pre_shadow_status_file,
            post_shadow_status_path=args.post_shadow_status_file,
            storage_inventory_path=args.storage_inventory_file,
        )
        payload = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )
        digest = hashlib.sha256(payload.encode("ascii")).hexdigest()
        _assert_isolated_restore_cleanup(args)
    except (OSError, subprocess.SubprocessError, TypeError, ValueError):
        sys.stderr.write(_EVIDENCE_ERROR)
        return 2
    try:
        sys.stdout.write(payload)
    except BrokenPipeError:
        return 0
    sys.stderr.write(
        json.dumps(
            {
                "kind": "backup-restore-evidence",
                "schema": "loom-personal-dev-backup-restore-evidence-render-v1",
                "sha256": digest,
                "source_sha": release.source_sha,
                "source_tree": release.source_tree,
            },
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )
        + "\n"
    )
    return 0


def _render_schema_transition(args: argparse.Namespace) -> int:
    try:
        source_root = args.source_root.resolve(strict=True)
        if (
            args.source_root != source_root
            or args.file != source_root / "deploy/dev-fleet/personal-dev-control-plane.toml"
            or Path(__file__).resolve(strict=True)
            != source_root / "src/loom_cli/personal_dev_control_plane_cmd.py"
        ):
            raise ValueError
        profile = load_personal_dev_control_plane_profile(args.file)
        current_release = load_personal_dev_trusted_release(
            args.trusted_release_file,
            args.trusted_release_sha256,
        )
        predecessor_release = load_personal_dev_trusted_release(
            args.predecessor_trusted_release_file,
            args.predecessor_trusted_release_sha256,
        )
        validate_personal_dev_schema_transition_source_root(
            args.source_root,
            release=current_release,
            alembic_ini_path=args.alembic_config_file,
        )
        prepared = prepare_personal_dev_schema_transition(
            profile=profile,
            current_release=current_release,
            current_release_sha256=args.trusted_release_sha256,
            predecessor_release=predecessor_release,
            predecessor_release_sha256=args.predecessor_trusted_release_sha256,
            backup_evidence_path=args.backup_restore_evidence_file,
            backup_evidence_sha256=args.backup_restore_evidence_sha256,
            postgres_dump_path=args.postgres_dump_file,
            postgres_source_state_path=args.postgres_source_state_file,
            predecessor_shadow_path=args.predecessor_shadow_manifest_file,
            predecessor_shadow_sha256=args.predecessor_shadow_manifest_sha256,
            alembic_ini_path=args.alembic_config_file,
            expected_predecessor_head=args.expected_predecessor_schema_head,
            expected_target_head=args.expected_target_schema_head,
        )
        validate_personal_dev_schema_transition_source_root(
            args.source_root,
            release=current_release,
            alembic_ini_path=args.alembic_config_file,
        )
    except (OSError, TypeError, ValueError):
        sys.stderr.write(_SCHEMA_TRANSITION_ERROR)
        return 2
    try:
        sys.stdout.write(prepared.migration_job_json.decode("ascii"))
    except BrokenPipeError:
        return 0
    sys.stderr.write(prepared.plan_json.decode("ascii") + "\n")
    return 0


def _validate_bound_acceptance_evidence(
    args: argparse.Namespace,
    *,
    profile: PersonalDevControlPlaneProfile,
    release: PersonalDevTrustedRelease,
    plan: PersonalDevAcceptancePlan | PersonalDevOperationalPlan,
) -> None:
    validate_personal_dev_policy_evidence(
        profile=profile,
        release=release,
        plan=plan,
        source_root=args.source_root,
        trusted_launcher_profile_path=args.trusted_launcher_profile_file,
        scanner_finding_policy_path=args.scanner_finding_policy_file,
    )
    load_personal_dev_backup_restore_evidence(
        args.backup_restore_evidence_file,
        expected_sha256=plan.storage.backup_restore_evidence_sha256,
        release=release,
        release_sha256=args.trusted_release_sha256,
        expected_schema_head=plan.storage.schema_head,
    )


def _render_acceptance(args: argparse.Namespace) -> int:
    try:
        profile = load_personal_dev_control_plane_profile(args.file)
        release = load_personal_dev_trusted_release(
            args.trusted_release_file,
            args.trusted_release_sha256,
        )
        plan = load_personal_dev_acceptance_plan(
            args.acceptance_plan_file,
            args.acceptance_plan_sha256,
        )
        _validate_bound_acceptance_evidence(
            args,
            profile=profile,
            release=release,
            plan=plan,
        )
        rendered = render_acceptance_personal_dev_control_plane(
            profile,
            release,
            plan,
            now=datetime.now(UTC),
        )
        evidence = json.dumps(
            {
                "acceptance_plan_sha256": plan.sha256,
                "input_sha256": rendered.input_sha256,
                "mode": "acceptance",
                "release_sha256": rendered.release_sha256,
                "resource_count": rendered.resource_count,
                "schema": "loom-personal-dev-control-plane-render-v1",
                "source_sha": release.source_sha,
                "source_tree": release.source_tree,
                "yaml_sha256": hashlib.sha256(rendered.yaml_text.encode("utf-8")).hexdigest(),
            },
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )
    except (OSError, TypeError, ValueError):
        sys.stderr.write(_RENDER_ERROR)
        return 2

    try:
        sys.stdout.write(rendered.yaml_text)
    except BrokenPipeError:
        return 0
    sys.stderr.write(evidence + "\n")
    return 0


def _render_operational(args: argparse.Namespace) -> int:
    try:
        profile = load_personal_dev_control_plane_profile(args.file)
        release = load_personal_dev_trusted_release(
            args.trusted_release_file,
            args.trusted_release_sha256,
        )
        plan = load_personal_dev_operational_plan(
            args.operational_plan_file,
            args.operational_plan_sha256,
        )
        _validate_bound_acceptance_evidence(
            args,
            profile=profile,
            release=release,
            plan=plan,
        )
        rendered = render_operational_personal_dev_control_plane(
            profile,
            release,
            plan,
            now=datetime.now(UTC),
        )
        evidence = json.dumps(
            {
                "input_sha256": rendered.input_sha256,
                "mode": "operational",
                "operational_plan_sha256": plan.sha256,
                "release_sha256": rendered.release_sha256,
                "resource_count": rendered.resource_count,
                "schema": "loom-personal-dev-control-plane-render-v1",
                "source_sha": release.source_sha,
                "source_tree": release.source_tree,
                "yaml_sha256": hashlib.sha256(rendered.yaml_text.encode("utf-8")).hexdigest(),
            },
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )
    except (OSError, TypeError, ValueError):
        sys.stderr.write(_RENDER_ERROR)
        return 2

    try:
        sys.stdout.write(rendered.yaml_text)
    except BrokenPipeError:
        return 0
    sys.stderr.write(evidence + "\n")
    return 0


class _SubprocessKubectlRunner:
    def __init__(self, kubeconfig: Path) -> None:
        self._kubeconfig = kubeconfig
        loaded = _load_safe_kubeconfig(kubeconfig)
        if loaded is None:
            raise ValueError("kubeconfig is invalid")
        self._kubeconfig_identity, self._kubeconfig_payload = loaded

    def _validate_kubeconfig(self) -> None:
        if _load_safe_kubeconfig(self._kubeconfig) != (
            self._kubeconfig_identity,
            self._kubeconfig_payload,
        ):
            raise OSError("kubeconfig changed during observation")

    def _open_kubeconfig(self) -> int:
        self._validate_kubeconfig()
        return _anonymous_kubeconfig_snapshot(self._kubeconfig_payload)

    def run(
        self,
        argv: Sequence[str],
        *,
        timeout_seconds: int,
    ) -> subprocess.CompletedProcess[str]:
        if type(timeout_seconds) is not int or not 1 <= timeout_seconds <= 60:
            raise ValueError("kubectl timeout is invalid")
        self._validate_kubeconfig()
        kubeconfig_descriptor = self._open_kubeconfig()
        display_command = ["kubectl", "--kubeconfig", str(self._kubeconfig), *argv]
        command = [
            "kubectl",
            "--kubeconfig",
            f"/proc/self/fd/{kubeconfig_descriptor}",
            *argv,
        ]
        try:
            process = subprocess.Popen(
                command,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                close_fds=True,
                pass_fds=(kubeconfig_descriptor,),
            )
        finally:
            os.close(kubeconfig_descriptor)
        if process.stdout is None or process.stderr is None:  # pragma: no cover
            process.kill()
            process.wait()
            raise OSError("kubectl output pipes are unavailable")
        streams = {
            "stdout": (process.stdout, bytearray()),
            "stderr": (process.stderr, bytearray()),
        }
        selector = selectors.DefaultSelector()
        deadline = time.monotonic() + timeout_seconds
        total = 0
        try:
            for name, (stream, _buffer) in streams.items():
                os.set_blocking(stream.fileno(), False)
                selector.register(stream, selectors.EVENT_READ, name)
            while selector.get_map():
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise subprocess.TimeoutExpired(command, timeout_seconds)
                events = selector.select(remaining)
                if not events:
                    raise subprocess.TimeoutExpired(command, timeout_seconds)
                for key, _mask in events:
                    try:
                        chunk = os.read(
                            key.fd,
                            min(
                                _KUBECTL_READ_BYTES,
                                MAX_PERSONAL_DEV_STATUS_RESPONSE_BYTES + 1 - total,
                            ),
                        )
                    except BlockingIOError:
                        continue
                    if not chunk:
                        selector.unregister(key.fileobj)
                        continue
                    total += len(chunk)
                    if total > MAX_PERSONAL_DEV_STATUS_RESPONSE_BYTES:
                        raise OSError("kubectl output exceeds its size bound")
                    streams[key.data][1].extend(chunk)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise subprocess.TimeoutExpired(command, timeout_seconds)
            returncode = process.wait(timeout=remaining)
            self._validate_kubeconfig()
        except BaseException:
            if process.poll() is None:
                process.kill()
            process.wait()
            raise
        finally:
            selector.close()
            process.stdout.close()
            process.stderr.close()
        return subprocess.CompletedProcess(
            display_command,
            returncode,
            streams["stdout"][1].decode("utf-8"),
            streams["stderr"][1].decode("utf-8"),
        )

    def stream(
        self,
        argv: Sequence[str],
        *,
        destination: _BinaryWriteDestination | None,
        maximum_stdout_bytes: int,
        expected_size: int | None,
        maximum_stderr_bytes: int,
        timeout_seconds: int,
        retain_stderr: bool = True,
    ) -> PersonalDevMinioCommandResult:
        if (
            type(maximum_stdout_bytes) is not int
            or maximum_stdout_bytes < 0
            or type(maximum_stderr_bytes) is not int
            or maximum_stderr_bytes < 0
            or type(timeout_seconds) is not int
            or not 1 <= timeout_seconds <= 3600
            or type(retain_stderr) is not bool
            or (expected_size is not None and (type(expected_size) is not int or expected_size < 0))
        ):
            raise ValueError("kubectl stream bound is invalid")
        self._validate_kubeconfig()
        kubeconfig_descriptor = self._open_kubeconfig()
        command = [
            "kubectl",
            "--kubeconfig",
            f"/proc/self/fd/{kubeconfig_descriptor}",
            *argv,
        ]
        try:
            process = subprocess.Popen(
                command,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                close_fds=True,
                pass_fds=(kubeconfig_descriptor,),
            )
        finally:
            os.close(kubeconfig_descriptor)
        if process.stdout is None or process.stderr is None:  # pragma: no cover
            process.kill()
            process.wait()
            raise OSError("kubectl output pipes are unavailable")
        stdout = bytearray()
        stderr = bytearray()
        stdout_size = 0
        stderr_size = 0
        selector = selectors.DefaultSelector()
        deadline = time.monotonic() + timeout_seconds
        try:
            os.set_blocking(process.stdout.fileno(), False)
            os.set_blocking(process.stderr.fileno(), False)
            selector.register(process.stdout, selectors.EVENT_READ, "stdout")
            selector.register(process.stderr, selectors.EVENT_READ, "stderr")
            while selector.get_map():
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise subprocess.TimeoutExpired(command, timeout_seconds)
                events = selector.select(remaining)
                if not events:
                    raise subprocess.TimeoutExpired(command, timeout_seconds)
                for key, _mask in events:
                    if key.data == "stdout":
                        remaining_bytes = maximum_stdout_bytes - stdout_size
                    else:
                        remaining_bytes = maximum_stderr_bytes - stderr_size
                    try:
                        chunk = os.read(
                            key.fd,
                            min(_KUBECTL_READ_BYTES, max(1, remaining_bytes + 1)),
                        )
                    except BlockingIOError:
                        continue
                    if not chunk:
                        selector.unregister(key.fileobj)
                        continue
                    if len(chunk) > remaining_bytes:
                        raise OSError(f"kubectl {key.data} exceeds its size bound")
                    if key.data == "stderr":
                        stderr_size += len(chunk)
                        if retain_stderr:
                            stderr.extend(chunk)
                        continue
                    stdout_size += len(chunk)
                    if destination is None:
                        stdout.extend(chunk)
                    else:
                        view = memoryview(chunk)
                        while view:
                            written = destination.write(view)
                            if not isinstance(written, int) or written <= 0:
                                raise OSError("kubectl destination write failed")
                            view = view[written:]
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise subprocess.TimeoutExpired(command, timeout_seconds)
            returncode = process.wait(timeout=remaining)
            if expected_size is not None and stdout_size != expected_size:
                raise OSError("kubectl stdout size differs from expected size")
            self._validate_kubeconfig()
        except BaseException:
            if process.poll() is None:
                process.kill()
            process.wait()
            raise
        finally:
            selector.close()
            process.stdout.close()
            process.stderr.close()
        safe_stderr = bytes(stderr) if retain_stderr else (_STDERR_PRESENT if stderr_size else b"")
        return PersonalDevMinioCommandResult(returncode, bytes(stdout), safe_stderr)


class _HashingDestination:
    def __init__(self, destination: BinaryIO | None) -> None:
        self._destination = destination
        self._sha256 = hashlib.sha256()

    def write(self, payload: bytes | memoryview) -> int:
        value = bytes(payload)
        self._sha256.update(value)
        if self._destination is not None:
            written = self._destination.write(value)
            if written != len(value):
                raise OSError("MinIO destination write failed")
        return len(value)

    def hexdigest(self) -> str:
        return self._sha256.hexdigest()


def _strict_json_value(payload: str) -> object:
    def reject_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
        value: dict[str, object] = {}
        for key, item in pairs:
            if key in value:
                raise ValueError("duplicate JSON key")
            value[key] = item
        return value

    try:
        return json.loads(payload, object_pairs_hook=reject_duplicates)
    except (json.JSONDecodeError, RuntimeError, TypeError, ValueError):
        raise ValueError("JSON document is invalid") from None


def _strict_json_mapping(payload: str) -> Mapping[str, object]:
    value = _strict_json_value(payload)
    if not isinstance(value, Mapping):
        raise ValueError("JSON document is invalid")
    return value


class _KubectlMinioTransport:
    def __init__(self, runner: _SubprocessKubectlRunner, *, namespace: str) -> None:
        if namespace != "loom-dev":
            raise ValueError("MinIO namespace is invalid")
        self._runner = runner
        self._namespace = namespace
        observed = runner.run(
            [
                "--namespace",
                namespace,
                "get",
                "pods",
                "--selector",
                "app=loom-dev-minio",
                "--output=json",
            ],
            timeout_seconds=30,
        )
        if observed.returncode != 0 or observed.stderr:
            raise ValueError("MinIO pod discovery failed")
        document = _strict_json_mapping(observed.stdout)
        items = document.get("items")
        if (
            document.get("apiVersion") != "v1"
            or document.get("kind") != "List"
            or not isinstance(items, list)
            or len(items) != 1
        ):
            raise ValueError("MinIO pod cardinality is invalid")
        pod = items[0]
        if not isinstance(pod, Mapping):
            raise ValueError("MinIO pod is invalid")
        metadata = pod.get("metadata")
        status = pod.get("status")
        if not isinstance(metadata, Mapping) or not isinstance(status, Mapping):
            raise ValueError("MinIO pod is invalid")
        labels = metadata.get("labels")
        name = metadata.get("name")
        if (
            not isinstance(labels, Mapping)
            or labels.get("app") != "loom-dev-minio"
            or metadata.get("namespace") != namespace
            or not isinstance(name, str)
            or _MINIO_POD_NAME_RE.fullmatch(name) is None
            or status.get("phase") != "Running"
        ):
            raise ValueError("MinIO pod identity is invalid")
        self._pod_name = name

    def _command(self, arguments: Sequence[str]) -> tuple[str, ...]:
        if not isinstance(arguments, Sequence) or any(type(item) is not str for item in arguments):
            raise TypeError("MinIO arguments are invalid")
        return (
            "--namespace",
            self._namespace,
            "exec",
            self._pod_name,
            "-c",
            "admin",
            "--",
            "/bin/sh",
            "-euc",
            _KUBECTL_MINIO_WRAPPER,
            "sh",
            *arguments,
        )

    def run(
        self,
        arguments: Sequence[str],
        *,
        maximum_stdout_bytes: int,
        timeout_seconds: int,
    ) -> PersonalDevMinioCommandResult:
        return self._runner.stream(
            self._command(arguments),
            destination=None,
            maximum_stdout_bytes=maximum_stdout_bytes,
            expected_size=None,
            maximum_stderr_bytes=_MAX_MINIO_STDERR_BYTES,
            timeout_seconds=timeout_seconds,
        )

    def stream(
        self,
        arguments: Sequence[str],
        *,
        destination: BinaryIO | None,
        expected_size: int,
        timeout_seconds: int,
    ) -> str:
        hashing_destination = _HashingDestination(destination)
        result = self._runner.stream(
            self._command(arguments),
            destination=hashing_destination,
            maximum_stdout_bytes=expected_size,
            expected_size=expected_size,
            maximum_stderr_bytes=_MAX_MINIO_STDERR_BYTES,
            timeout_seconds=timeout_seconds,
            retain_stderr=False,
        )
        if result.returncode != 0 or result.stdout or result.stderr:
            raise ValueError("MinIO stream failed")
        return hashing_destination.hexdigest()


def _stream_docker_command(
    argv: Sequence[str],
    *,
    destination: _BinaryWriteDestination | None,
    maximum_stdout_bytes: int,
    expected_size: int | None,
    maximum_stderr_bytes: int,
    timeout_seconds: int,
    retain_stderr: bool = True,
    pass_fds: tuple[int, ...] = (),
) -> PersonalDevMinioCommandResult:
    if (
        not isinstance(argv, Sequence)
        or any(type(item) is not str for item in argv)
        or type(maximum_stdout_bytes) is not int
        or maximum_stdout_bytes < 0
        or type(maximum_stderr_bytes) is not int
        or maximum_stderr_bytes < 0
        or type(timeout_seconds) is not int
        or not 1 <= timeout_seconds <= 3600
        or type(retain_stderr) is not bool
        or type(pass_fds) is not tuple
        or any(type(descriptor) is not int or descriptor < 0 for descriptor in pass_fds)
        or len(set(pass_fds)) != len(pass_fds)
        or (expected_size is not None and (type(expected_size) is not int or expected_size < 0))
    ):
        raise ValueError("Docker command bound is invalid")
    command = ["docker", *argv]
    process = subprocess.Popen(
        command,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        close_fds=True,
        pass_fds=pass_fds,
    )
    if process.stdout is None or process.stderr is None:  # pragma: no cover
        process.kill()
        process.wait()
        raise OSError("Docker output pipes are unavailable")
    stdout = bytearray()
    stderr = bytearray()
    stdout_size = 0
    stderr_size = 0
    selector = selectors.DefaultSelector()
    deadline = time.monotonic() + timeout_seconds
    try:
        os.set_blocking(process.stdout.fileno(), False)
        os.set_blocking(process.stderr.fileno(), False)
        selector.register(process.stdout, selectors.EVENT_READ, "stdout")
        selector.register(process.stderr, selectors.EVENT_READ, "stderr")
        while selector.get_map():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise subprocess.TimeoutExpired(command, timeout_seconds)
            events = selector.select(remaining)
            if not events:
                raise subprocess.TimeoutExpired(command, timeout_seconds)
            for key, _mask in events:
                if key.data == "stdout":
                    remaining_bytes = maximum_stdout_bytes - stdout_size
                else:
                    remaining_bytes = maximum_stderr_bytes - stderr_size
                try:
                    chunk = os.read(
                        key.fd,
                        min(_KUBECTL_READ_BYTES, max(1, remaining_bytes + 1)),
                    )
                except BlockingIOError:
                    continue
                if not chunk:
                    selector.unregister(key.fileobj)
                    continue
                if len(chunk) > remaining_bytes:
                    raise OSError(f"Docker {key.data} exceeds its size bound")
                if key.data == "stderr":
                    stderr_size += len(chunk)
                    if retain_stderr:
                        stderr.extend(chunk)
                    continue
                stdout_size += len(chunk)
                if destination is None:
                    stdout.extend(chunk)
                else:
                    view = memoryview(chunk)
                    while view:
                        written = destination.write(view)
                        if not isinstance(written, int) or written <= 0:
                            raise OSError("Docker destination write failed")
                        view = view[written:]
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise subprocess.TimeoutExpired(command, timeout_seconds)
        returncode = process.wait(timeout=remaining)
        if expected_size is not None and stdout_size != expected_size:
            raise OSError("Docker stdout size differs from expected size")
    except BaseException:
        if process.poll() is None:
            process.kill()
        process.wait()
        raise
    finally:
        selector.close()
        process.stdout.close()
        process.stderr.close()
    safe_stderr = bytes(stderr) if retain_stderr else (_STDERR_PRESENT if stderr_size else b"")
    return PersonalDevMinioCommandResult(returncode, bytes(stdout), safe_stderr)


def _owner_only_file_identity(path: Path) -> _KubeconfigIdentity | None:
    if not isinstance(path, Path) or not path.is_absolute():
        return None
    try:
        if path.resolve(strict=True) != path:
            return None
        metadata = path.lstat()
    except (OSError, RuntimeError):
        return None
    if (
        not stat.S_ISREG(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_uid != os.geteuid()
        or stat.S_IMODE(metadata.st_mode) != 0o600
        or metadata.st_nlink != 1
        or metadata.st_size <= 0
    ):
        return None
    return _kubeconfig_identity(metadata)


def _owner_only_directory_identity(path: Path) -> _DirectoryIdentity | None:
    if not isinstance(path, Path) or not path.is_absolute():
        return None
    try:
        if path.resolve(strict=True) != path:
            return None
        metadata = path.lstat()
    except (OSError, RuntimeError):
        return None
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_uid != os.geteuid()
        or stat.S_IMODE(metadata.st_mode) != 0o700
    ):
        return None
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_mode,
        metadata.st_uid,
        metadata.st_gid,
    )


def _open_owner_only_file(
    path: Path,
    *,
    expected_identity: _KubeconfigIdentity,
) -> int:
    flags = os.O_RDONLY | os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
    except OSError:
        raise OSError("owner-only file changed") from None
    try:
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != os.geteuid()
            or stat.S_IMODE(metadata.st_mode) != 0o600
            or metadata.st_nlink != 1
            or metadata.st_size <= 0
            or _kubeconfig_identity(metadata) != expected_identity
        ):
            raise OSError("owner-only file changed")
    except BaseException:
        os.close(descriptor)
        raise
    return descriptor


class _DockerMinioTransport:
    def __init__(
        self,
        *,
        client_image: str,
        minio_image: str,
        restore_env_file: Path,
        payload_root: Path,
        isolated_minio_name: str,
        isolated_network_name: str,
    ) -> None:
        env_identity = _owner_only_file_identity(restore_env_file)
        env_parent_identity = _owner_only_directory_identity(restore_env_file.parent)
        if env_identity is None or env_parent_identity is None:
            raise ValueError("restore environment identity is invalid")
        if not isinstance(payload_root, Path) or not payload_root.is_absolute():
            raise ValueError("Docker restore input is invalid")
        try:
            payload_root_is_exact = payload_root.resolve(strict=False) == payload_root
        except (OSError, RuntimeError):
            raise ValueError("Docker restore input is invalid") from None
        if (
            not payload_root_is_exact
            or "," in str(payload_root)
            or not isinstance(client_image, str)
            or not isinstance(minio_image, str)
            or not isinstance(isolated_minio_name, str)
            or not isinstance(isolated_network_name, str)
        ):
            raise ValueError("Docker restore input is invalid")
        self._client_image = client_image
        self._minio_image = minio_image
        self._restore_env_file = restore_env_file
        self._restore_env_identity = env_identity
        self._restore_env_parent_identity = env_parent_identity
        self._payload_root = payload_root
        self._isolated_minio_name = isolated_minio_name
        self._isolated_network_name = isolated_network_name
        self._validate_boundaries()

    def _inspect(self, argv: Sequence[str]) -> object:
        result = _stream_docker_command(
            argv,
            destination=None,
            maximum_stdout_bytes=_MAX_DOCKER_INSPECT_BYTES,
            expected_size=None,
            maximum_stderr_bytes=_MAX_MINIO_STDERR_BYTES,
            timeout_seconds=30,
            retain_stderr=False,
        )
        if result.returncode != 0 or result.stderr:
            raise ValueError("Docker inspect failed")
        try:
            return _strict_json_value(result.stdout.decode("ascii"))
        except UnicodeDecodeError:
            raise ValueError("Docker inspect output is invalid") from None

    def _validate_boundaries(self) -> None:
        if (
            _owner_only_directory_identity(self._restore_env_file.parent)
            != self._restore_env_parent_identity
            or _owner_only_file_identity(self._restore_env_file) != self._restore_env_identity
        ):
            raise OSError("restore environment changed")
        container_values = self._inspect(["inspect", self._isolated_minio_name])
        network_values = self._inspect(["network", "inspect", self._isolated_network_name])
        if (
            not isinstance(container_values, list)
            or len(container_values) != 1
            or not isinstance(container_values[0], Mapping)
            or not isinstance(network_values, list)
            or len(network_values) != 1
            or not isinstance(network_values[0], Mapping)
        ):
            raise ValueError("Docker isolation inspection is invalid")
        container = container_values[0]
        network = network_values[0]
        config = container.get("Config")
        host = container.get("HostConfig")
        settings = container.get("NetworkSettings")
        state = container.get("State")
        container_id = container.get("Id")
        network_id = network.get("Id")
        if (
            not isinstance(config, Mapping)
            or config.get("Image") != self._minio_image
            or not isinstance(host, Mapping)
            or host.get("NetworkMode") != self._isolated_network_name
            or host.get("PortBindings") not in (None, {})
            or not isinstance(settings, Mapping)
            or not isinstance(state, Mapping)
            or state.get("Running") is not True
            or container.get("Name") != f"/{self._isolated_minio_name}"
            or not isinstance(container_id, str)
            or _SHA256_RE.fullmatch(container_id) is None
            or not isinstance(network_id, str)
            or _SHA256_RE.fullmatch(network_id) is None
        ):
            raise ValueError("Docker MinIO container is not trusted")
        ports = settings.get("Ports")
        attachments = settings.get("Networks")
        if (
            not isinstance(ports, Mapping)
            or any(value is not None for value in ports.values())
            or not isinstance(attachments, Mapping)
            or set(attachments) != {self._isolated_network_name}
        ):
            raise ValueError("Docker MinIO network attachment is invalid")
        attachment = attachments[self._isolated_network_name]
        if not isinstance(attachment, Mapping):
            raise ValueError("Docker MinIO network attachment is invalid")
        aliases = attachment.get("Aliases")
        if (
            attachment.get("NetworkID") != network_id
            or not isinstance(aliases, list)
            or any(not isinstance(alias, str) for alias in aliases)
            or aliases.count("minio-restore") != 1
        ):
            raise ValueError("Docker MinIO restore alias is invalid")
        members = network.get("Containers")
        if (
            network.get("Name") != self._isolated_network_name
            or network.get("Internal") is not True
            or network.get("Ingress") is not False
            or not isinstance(members, Mapping)
            or set(members) != {container_id}
            or not isinstance(members[container_id], Mapping)
            or members[container_id].get("Name") != self._isolated_minio_name
        ):
            raise ValueError("Docker restore network is not isolated")
        if _owner_only_file_identity(self._restore_env_file) != self._restore_env_identity:
            raise OSError("restore environment changed")

    def _arguments(self, arguments: Sequence[str]) -> tuple[str, ...]:
        if not isinstance(arguments, Sequence) or any(type(item) is not str for item in arguments):
            raise TypeError("MinIO arguments are invalid")
        prefix = f"{self._payload_root}{os.sep}"
        translated: list[str] = []
        for item in arguments:
            if item.startswith(prefix):
                source = Path(item)
                if source.parent != self._payload_root or _SHA256_RE.fullmatch(source.name) is None:
                    raise ValueError("retained payload argument is invalid")
                translated.append(f"{_DOCKER_PAYLOAD_ROOT}/{source.name}")
            else:
                translated.append(item)
        return tuple(translated)

    def _command(
        self,
        arguments: Sequence[str],
        *,
        env_descriptor: int,
        cid_path: Path,
    ) -> tuple[str, ...]:
        return (
            "run",
            "--rm",
            "--cidfile",
            str(cid_path),
            "--network",
            self._isolated_network_name,
            "--env-file",
            f"/proc/self/fd/{env_descriptor}",
            "--mount",
            f"type=bind,src={self._payload_root},dst={_DOCKER_PAYLOAD_ROOT},readonly",
            "--entrypoint",
            "/bin/sh",
            self._client_image,
            "-euc",
            _DOCKER_MINIO_WRAPPER,
            "sh",
            *self._arguments(arguments),
        )

    @staticmethod
    def _read_client_cid(cid_path: Path) -> str | None:
        flags = os.O_RDONLY | os.O_CLOEXEC
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            descriptor = os.open(cid_path, flags)
        except FileNotFoundError:
            return None
        except OSError:
            raise OSError("Docker client CID is invalid") from None
        try:
            metadata = os.fstat(descriptor)
            payload = os.read(descriptor, 66)
            if (
                not stat.S_ISREG(metadata.st_mode)
                or metadata.st_uid != os.geteuid()
                or metadata.st_nlink != 1
                or len(payload) > 65
                or os.read(descriptor, 1)
            ):
                raise OSError("Docker client CID is invalid")
        finally:
            os.close(descriptor)
        try:
            if payload.endswith(b"\n"):
                payload = payload[:-1]
            value = payload.decode("ascii")
        except UnicodeDecodeError:
            raise OSError("Docker client CID is invalid") from None
        if len(value) != 64 or _SHA256_RE.fullmatch(value) is None:
            raise OSError("Docker client CID is invalid")
        return value

    @classmethod
    def _force_remove_client(cls, cid_path: Path) -> None:
        cid = cls._read_client_cid(cid_path)
        if cid is None:
            return
        try:
            removed = subprocess.run(
                ["docker", "rm", "--force", cid],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=30,
                check=False,
            )
        except (OSError, subprocess.SubprocessError):
            raise OSError("Docker client cleanup failed") from None
        if removed.returncode != 0:
            raise OSError("Docker client cleanup failed")

    def _invoke_client(
        self,
        arguments: Sequence[str],
        *,
        destination: _BinaryWriteDestination | None,
        maximum_stdout_bytes: int,
        expected_size: int | None,
        timeout_seconds: int,
        retain_stderr: bool,
    ) -> PersonalDevMinioCommandResult:
        self._validate_boundaries()
        env_descriptor = _open_owner_only_file(
            self._restore_env_file,
            expected_identity=self._restore_env_identity,
        )
        cid_directory: Path | None = None
        cid_path: Path | None = None
        try:
            cid_directory = Path(
                tempfile.mkdtemp(
                    prefix=".loom-minio-client-",
                )
            )
            cid_path = cid_directory / "cid"
            try:
                result = _stream_docker_command(
                    self._command(
                        arguments,
                        env_descriptor=env_descriptor,
                        cid_path=cid_path,
                    ),
                    destination=destination,
                    maximum_stdout_bytes=maximum_stdout_bytes,
                    expected_size=expected_size,
                    maximum_stderr_bytes=_MAX_MINIO_STDERR_BYTES,
                    timeout_seconds=timeout_seconds,
                    retain_stderr=retain_stderr,
                    pass_fds=(env_descriptor,),
                )
                self._validate_boundaries()
            except BaseException:
                self._force_remove_client(cid_path)
                raise
            return result
        finally:
            os.close(env_descriptor)
            if cid_path is not None and cid_directory is not None:
                try:
                    cid_path.unlink(missing_ok=True)
                    cid_directory.rmdir()
                except OSError:
                    raise OSError("Docker client authority cleanup failed") from None

    def run(
        self,
        arguments: Sequence[str],
        *,
        maximum_stdout_bytes: int,
        timeout_seconds: int,
    ) -> PersonalDevMinioCommandResult:
        return self._invoke_client(
            arguments,
            destination=None,
            maximum_stdout_bytes=maximum_stdout_bytes,
            expected_size=None,
            timeout_seconds=timeout_seconds,
            retain_stderr=True,
        )

    def stream(
        self,
        arguments: Sequence[str],
        *,
        destination: BinaryIO | None,
        expected_size: int,
        timeout_seconds: int,
    ) -> str:
        hashing_destination = _HashingDestination(destination)
        result = self._invoke_client(
            arguments,
            destination=hashing_destination,
            maximum_stdout_bytes=expected_size,
            expected_size=expected_size,
            timeout_seconds=timeout_seconds,
            retain_stderr=False,
        )
        if result.returncode != 0 or result.stdout or result.stderr:
            raise ValueError("MinIO stream failed")
        return hashing_destination.hexdigest()


def _kubeconfig_identity(opened: os.stat_result) -> _KubeconfigIdentity:
    return (
        opened.st_dev,
        opened.st_ino,
        opened.st_mode,
        opened.st_uid,
        opened.st_gid,
        opened.st_nlink,
        opened.st_size,
        opened.st_mtime_ns,
        opened.st_ctime_ns,
    )


def _self_contained_kubeconfig(payload: bytes) -> bool:
    try:
        document = yaml.safe_load(payload)
    except (RecursionError, UnicodeDecodeError, yaml.YAMLError):
        return False
    if (
        not isinstance(document, Mapping)
        or document.get("apiVersion") != "v1"
        or document.get("kind") != "Config"
        or not isinstance(document.get("current-context"), str)
        or not document["current-context"]
    ):
        return False

    def entries(field: str, body_field: str) -> dict[str, Mapping[str, object]] | None:
        values = document.get(field)
        if not isinstance(values, list) or not 1 <= len(values) <= 128:
            return None
        result: dict[str, Mapping[str, object]] = {}
        for value in values:
            if not isinstance(value, Mapping):
                return None
            name = value.get("name")
            body = value.get(body_field)
            if (
                not isinstance(name, str)
                or not name
                or name in result
                or not isinstance(body, Mapping)
            ):
                return None
            result[name] = body
        return result

    clusters = entries("clusters", "cluster")
    contexts = entries("contexts", "context")
    users = entries("users", "user")
    if clusters is None or contexts is None or users is None:
        return False
    current = contexts.get(document["current-context"])
    if current is None:
        return False
    cluster_name = current.get("cluster")
    user_name = current.get("user")
    if (
        not isinstance(cluster_name, str)
        or not isinstance(user_name, str)
        or cluster_name not in clusters
        or user_name not in users
    ):
        return False
    if any("certificate-authority" in cluster for cluster in clusters.values()):
        return False
    external_user_fields = {
        "auth-provider",
        "client-certificate",
        "client-key",
        "exec",
        "tokenFile",
    }
    return all(not external_user_fields.intersection(user) for user in users.values())


def _load_safe_kubeconfig(path: Path) -> tuple[_KubeconfigIdentity, bytes] | None:
    if not path.is_absolute():
        return None
    try:
        if path.resolve(strict=True) != path:
            return None
        path_metadata = path.lstat()
    except (OSError, RuntimeError):
        return None
    if (
        not stat.S_ISREG(path_metadata.st_mode)
        or stat.S_ISLNK(path_metadata.st_mode)
        or path_metadata.st_uid != os.geteuid()
        or stat.S_IMODE(path_metadata.st_mode) != 0o600
        or path_metadata.st_nlink != 1
        or not 0 < path_metadata.st_size <= _MAX_KUBECONFIG_BYTES
    ):
        return None
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError:
        return None
    try:
        opened = os.fstat(descriptor)
        identity = _kubeconfig_identity(opened)
        if identity != _kubeconfig_identity(path_metadata):
            return None
        payload = bytearray()
        while len(payload) <= _MAX_KUBECONFIG_BYTES:
            chunk = os.read(
                descriptor, min(_KUBECTL_READ_BYTES, _MAX_KUBECONFIG_BYTES + 1 - len(payload))
            )
            if not chunk:
                break
            payload.extend(chunk)
        if (
            len(payload) != opened.st_size
            or _kubeconfig_identity(os.fstat(descriptor)) != identity
            or _safe_path_identity(path) != identity
            or not _self_contained_kubeconfig(bytes(payload))
        ):
            return None
        return identity, bytes(payload)
    except OSError:
        return None
    finally:
        os.close(descriptor)


def _safe_path_identity(path: Path) -> _KubeconfigIdentity | None:
    try:
        return _kubeconfig_identity(path.lstat())
    except OSError:
        return None


def _anonymous_kubeconfig_snapshot(payload: bytes) -> int:
    descriptor: int | None = None
    try:
        temporary_directory = tempfile.gettempdir()
        temporary_flag = getattr(os, "O_TMPFILE", 0)
        if temporary_flag:
            try:
                descriptor = os.open(
                    temporary_directory,
                    temporary_flag | os.O_RDWR | getattr(os, "O_CLOEXEC", 0),
                    0o600,
                )
            except OSError:
                descriptor = None
        if descriptor is None:
            descriptor, temporary_path = tempfile.mkstemp(
                prefix="loom-personal-dev-kubeconfig-",
                dir=temporary_directory,
            )
            os.unlink(temporary_path)
        view = memoryview(payload)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise OSError("kubeconfig snapshot write failed")
            view = view[written:]
        os.fchmod(descriptor, 0o400)
        os.lseek(descriptor, 0, os.SEEK_SET)
        read_descriptor = os.open(
            f"/proc/self/fd/{descriptor}",
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0),
        )
        os.close(descriptor)
        return read_descriptor
    except BaseException:
        if descriptor is not None:
            os.close(descriptor)
        raise


def _safe_kubeconfig(path: Path) -> _KubeconfigIdentity | None:
    loaded = _load_safe_kubeconfig(path)
    return None if loaded is None else loaded[0]


def _minio_backup_summary(manifest: Any) -> str:
    return (
        json.dumps(
            {
                "object_count": manifest.object_count,
                "payload_bytes": manifest.total_payload_bytes,
                "schema": "loom-personal-dev-minio-backup-summary-v1",
                "source_manifest_sha256": hashlib.sha256(manifest.canonical_bytes).hexdigest(),
            },
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )
        + "\n"
    )


def _capture_minio_backup(args: argparse.Namespace) -> int:
    try:
        runner = _SubprocessKubectlRunner(args.kubeconfig)
        transport = _KubectlMinioTransport(runner, namespace=args.namespace)
        manifest = capture_personal_dev_minio_backup(
            transport=transport,
            source_manifest_path=args.source_manifest_file,
            payload_root=args.payload_root,
        )
        output = _minio_backup_summary(manifest)
    except (OSError, subprocess.SubprocessError, TypeError, ValueError):
        sys.stderr.write(_MINIO_BACKUP_ERROR)
        return 2
    try:
        sys.stdout.write(output)
    except BrokenPipeError:
        return 0
    return 0


def _restore_minio_backup(args: argparse.Namespace) -> int:
    try:
        release = load_personal_dev_trusted_release(
            args.trusted_release_file,
            args.trusted_release_sha256,
        )
        suffix = args.trusted_release_sha256[:12]
        if (
            args.isolated_minio_name != f"loom-personal-dev-minio-restore-{suffix}"
            or args.isolated_network_name != f"loom-personal-dev-restore-{suffix}"
        ):
            raise ValueError("isolated restore identity is not release-bound")
        transport = _DockerMinioTransport(
            client_image=release.images.minio_client,
            minio_image=release.images.minio,
            restore_env_file=args.restore_env_file,
            payload_root=args.payload_root,
            isolated_minio_name=args.isolated_minio_name,
            isolated_network_name=args.isolated_network_name,
        )
        manifest = restore_personal_dev_minio_backup(
            transport=transport,
            source_manifest_path=args.source_manifest_file,
            payload_root=args.payload_root,
            restored_manifest_path=args.restored_manifest_file,
        )
        output = _minio_backup_summary(manifest)
    except (OSError, subprocess.SubprocessError, TypeError, ValueError):
        sys.stderr.write(_MINIO_BACKUP_ERROR)
        return 2
    try:
        sys.stdout.write(output)
    except BrokenPipeError:
        return 0
    return 0


def _verify_acceptance_result(args: argparse.Namespace) -> int:
    """Validate local two-owner evidence without constructing a runner or client."""
    try:
        plan = load_personal_dev_acceptance_plan(
            args.acceptance_plan_file,
            args.acceptance_plan_sha256,
        )
        result = load_personal_dev_acceptance_result(
            args.acceptance_result_file,
            args.acceptance_result_sha256,
            plan=plan,
            expected_acceptance_manifest_sha256=args.acceptance_manifest_sha256,
        )
        rollback_shadow_status = load_personal_dev_rollback_shadow_status(
            args.rollback_shadow_status_file,
            result.status_sha256s.rollback_shadow,
        )
        validate_personal_dev_rollback_shadow_manifest(
            args.rollback_shadow_manifest_file,
            result.shadow_manifest_sha256,
            expected_input_sha256=rollback_shadow_status["input_sha256"],
            expected_release_sha256=result.release_sha256,
        )
        if rollback_shadow_status["release_sha256"] != result.release_sha256:
            raise ValueError("rollback shadow release binding is invalid")
    except (OSError, TypeError, ValueError):
        sys.stderr.write(_VERIFICATION_ERROR)
        return 2

    record = {
        "acceptance_manifest_sha256": result.acceptance_manifest_sha256,
        "acceptance_plan_sha256": plan.sha256,
        "acceptance_result_sha256": args.acceptance_result_sha256,
        "cross_owner_denial_count": len(result.cross_owner_denials),
        "owner_count": len(result.owners),
        "release_sha256": result.release_sha256,
        "rollback_shadow_status_sha256": result.status_sha256s.rollback_shadow,
        "schema": "loom-personal-dev-zero-capacity-acceptance-verification-v1",
        "shadow_manifest_sha256": result.shadow_manifest_sha256,
        "verified": True,
    }
    sys.stdout.write(
        json.dumps(
            record,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )
        + "\n"
    )
    return 0


def _status(args: argparse.Namespace) -> int:
    try:
        if _safe_kubeconfig(args.kubeconfig) is None:
            raise ValueError("kubeconfig is invalid")
        profile = load_personal_dev_control_plane_profile(args.file)
        release = load_personal_dev_trusted_release(
            args.trusted_release_file,
            args.trusted_release_sha256,
        )
        expected = render_shadow_personal_dev_control_plane(profile, release)
        runner = _SubprocessKubectlRunner(args.kubeconfig)
    except (OSError, TypeError, ValueError):
        sys.stderr.write(_STATUS_ERROR)
        return 2

    status_value = observe_personal_dev_shadow_status(
        runner,
        expected=expected,
        namespace=args.namespace,
    )
    output = (
        json.dumps(
            status_value.to_dict(),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )
        + "\n"
    )
    try:
        sys.stdout.write(output)
    except BrokenPipeError:
        return 0 if status_value.ready else 1
    return 0 if status_value.ready else 1


def _status_acceptance(args: argparse.Namespace) -> int:
    try:
        if _safe_kubeconfig(args.kubeconfig) is None:
            raise ValueError("kubeconfig is invalid")
        profile = load_personal_dev_control_plane_profile(args.file)
        release = load_personal_dev_trusted_release(
            args.trusted_release_file,
            args.trusted_release_sha256,
        )
        plan = load_personal_dev_acceptance_plan(
            args.acceptance_plan_file,
            args.acceptance_plan_sha256,
        )
        _validate_bound_acceptance_evidence(
            args,
            profile=profile,
            release=release,
            plan=plan,
        )
        # Reconstruct the immutable target even after expiry; the live observer
        # reports actual window state as a blocker instead of hiding cluster drift.
        expected = render_acceptance_personal_dev_control_plane(
            profile,
            release,
            plan,
            now=plan.window.started_at,
        )
        runner = _SubprocessKubectlRunner(args.kubeconfig)
    except (OSError, TypeError, ValueError):
        sys.stderr.write(_STATUS_ERROR)
        return 2

    status_value = observe_personal_dev_acceptance_status(
        runner,
        expected=expected,
        plan=plan,
        namespace=args.namespace,
    )
    output = (
        json.dumps(
            status_value.to_dict(),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )
        + "\n"
    )
    try:
        sys.stdout.write(output)
    except BrokenPipeError:
        return 0 if status_value.ready else 1
    return 0 if status_value.ready else 1


def _status_operational(args: argparse.Namespace) -> int:
    try:
        if _safe_kubeconfig(args.kubeconfig) is None:
            raise ValueError("kubeconfig is invalid")
        profile = load_personal_dev_control_plane_profile(args.file)
        release = load_personal_dev_trusted_release(
            args.trusted_release_file,
            args.trusted_release_sha256,
        )
        plan = load_personal_dev_operational_plan(
            args.operational_plan_file,
            args.operational_plan_sha256,
        )
        _validate_bound_acceptance_evidence(
            args,
            profile=profile,
            release=release,
            plan=plan,
        )
        expected = render_operational_personal_dev_control_plane(
            profile,
            release,
            plan,
            now=datetime.now(UTC),
        )
        runner = _SubprocessKubectlRunner(args.kubeconfig)
    except (OSError, TypeError, ValueError):
        sys.stderr.write(_STATUS_ERROR)
        return 2

    status_value = observe_personal_dev_operational_status(
        runner,
        expected=expected,
        plan=plan,
        namespace=args.namespace,
    )
    output = (
        json.dumps(
            status_value.to_dict(),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )
        + "\n"
    )
    try:
        sys.stdout.write(output)
    except BrokenPipeError:
        return 0 if status_value.ready else 1
    return 0 if status_value.ready else 1


def _add_bound_evidence_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--source-root",
        type=Path,
        required=True,
        help="Absolute clean checkout root for source-derived evidence validation.",
    )
    parser.add_argument(
        "--trusted-launcher-profile-file",
        type=Path,
        required=True,
        help="Owner-only canonical source-derived trusted-launcher profile.",
    )
    parser.add_argument(
        "--scanner-finding-policy-file",
        type=Path,
        required=True,
        help="Owner-only canonical source-derived scanner finding policy.",
    )
    parser.add_argument(
        "--backup-restore-evidence-file",
        type=Path,
        required=True,
        help="Owner-only canonical completed backup/restore drill evidence.",
    )


def add_personal_dev_control_plane_subparser(subparsers: Any) -> None:
    """Register the bounded personal-dev observation and recovery surface."""

    parent = subparsers.add_parser(
        "personal-dev-control-plane",
        allow_abbrev=False,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        help="Render, observe, or recover the personal-development management plane.",
        description=(
            "render-only and read-only live personal-development management-plane shadow, "
            "acceptance, or operational mode\n"
            "recovery mutates only a release-bound isolated Docker store, never live resources\n"
            "physical capacity unchanged"
        ),
    )
    operations = parent.add_subparsers(
        dest="personal_dev_control_plane_op",
        required=True,
    )
    capture_minio = operations.add_parser(
        "capture-minio-backup",
        allow_abbrev=False,
        help="Capture live MinIO read-only into retained payload authority.",
    )
    capture_minio.add_argument(
        "--namespace",
        choices=["loom-dev"],
        required=True,
        help="Exact shared infrastructure namespace.",
    )
    capture_minio.add_argument("--kubeconfig", type=Path, required=True)
    capture_minio.add_argument("--source-manifest-file", type=Path, required=True)
    capture_minio.add_argument("--payload-root", type=Path, required=True)
    capture_minio.set_defaults(handler=_capture_minio_backup)

    restore_minio = operations.add_parser(
        "restore-minio-backup",
        allow_abbrev=False,
        help="Restore retained MinIO payloads into a release-bound isolated store.",
    )
    restore_minio.add_argument("--trusted-release-file", type=Path, required=True)
    restore_minio.add_argument("--trusted-release-sha256", required=True)
    restore_minio.add_argument("--source-manifest-file", type=Path, required=True)
    restore_minio.add_argument("--payload-root", type=Path, required=True)
    restore_minio.add_argument("--restored-manifest-file", type=Path, required=True)
    restore_minio.add_argument("--restore-env-file", type=Path, required=True)
    restore_minio.add_argument("--isolated-minio-name", required=True)
    restore_minio.add_argument("--isolated-network-name", required=True)
    restore_minio.set_defaults(handler=_restore_minio_backup)
    render = operations.add_parser(
        "render",
        allow_abbrev=False,
        help="Render exact Kubernetes YAML and canonical evidence.",
    )
    render.add_argument(
        "--file",
        type=Path,
        required=True,
        help="Strict non-secret personal-dev management-plane TOML profile.",
    )
    render.add_argument(
        "--trusted-release-file",
        type=Path,
        required=True,
        help="Owner-only canonical trusted-release JSON document.",
    )
    render.add_argument(
        "--trusted-release-sha256",
        required=True,
        help="Exact SHA-256 of the canonical trusted-release document.",
    )
    render.set_defaults(handler=_render)

    for operation, description in (
        (
            "render-trusted-launcher-profile",
            "Render the exact source-derived trusted-launcher profile.",
        ),
        (
            "render-scanner-finding-policy",
            "Render the exact source-derived scanner finding policy.",
        ),
    ):
        policy = operations.add_parser(
            operation,
            allow_abbrev=False,
            help=description,
        )
        policy.add_argument(
            "--file",
            type=Path,
            required=True,
            help="Strict non-secret personal-dev management-plane TOML profile.",
        )
        policy.add_argument(
            "--trusted-release-file",
            type=Path,
            required=True,
            help="Owner-only canonical trusted-release JSON document.",
        )
        policy.add_argument(
            "--trusted-release-sha256",
            required=True,
            help="Exact SHA-256 of the canonical trusted-release document.",
        )
        policy.add_argument(
            "--source-root",
            type=Path,
            required=True,
            help="Absolute clean checkout root whose source files are bound.",
        )
        policy.set_defaults(handler=_render_policy_evidence)

    backup = operations.add_parser(
        "render-backup-restore-evidence",
        allow_abbrev=False,
        help="Derive canonical evidence from one completed isolated restore drill.",
    )
    backup.add_argument("--file", type=Path, required=True)
    backup.add_argument("--trusted-release-file", type=Path, required=True)
    backup.add_argument("--trusted-release-sha256", required=True)
    backup.add_argument("--started-at", required=True)
    backup.add_argument("--completed-at", required=True)
    backup.add_argument("--postgres-dump-file", type=Path, required=True)
    backup.add_argument("--postgres-source-state-file", type=Path, required=True)
    backup.add_argument("--postgres-restored-state-file", type=Path, required=True)
    backup.add_argument("--source-schema-head", required=True)
    backup.add_argument("--restored-schema-head", required=True)
    backup.add_argument("--minio-source-manifest-file", type=Path, required=True)
    backup.add_argument("--minio-restored-manifest-file", type=Path, required=True)
    backup.add_argument("--minio-payload-root", type=Path, required=True)
    backup.add_argument("--secret-key-inventory-file", type=Path, required=True)
    backup.add_argument("--pre-shadow-status-file", type=Path, required=True)
    backup.add_argument("--post-shadow-status-file", type=Path, required=True)
    backup.add_argument("--storage-inventory-file", type=Path, required=True)
    backup.add_argument("--isolated-postgres-name", required=True)
    backup.add_argument("--isolated-minio-name", required=True)
    backup.add_argument("--isolated-network-name", required=True)
    backup.set_defaults(handler=_render_backup_restore_evidence)

    transition = operations.add_parser(
        "render-schema-transition",
        allow_abbrev=False,
        help="Bind an exact migration Job to a proven predecessor restore boundary.",
    )
    transition.add_argument("--file", type=Path, required=True)
    transition.add_argument("--trusted-release-file", type=Path, required=True)
    transition.add_argument("--trusted-release-sha256", required=True)
    transition.add_argument("--source-root", type=Path, required=True)
    transition.add_argument(
        "--predecessor-trusted-release-file",
        type=Path,
        required=True,
    )
    transition.add_argument("--predecessor-trusted-release-sha256", required=True)
    transition.add_argument(
        "--backup-restore-evidence-file",
        type=Path,
        required=True,
    )
    transition.add_argument("--backup-restore-evidence-sha256", required=True)
    transition.add_argument("--postgres-dump-file", type=Path, required=True)
    transition.add_argument(
        "--postgres-source-state-file",
        type=Path,
        required=True,
    )
    transition.add_argument(
        "--predecessor-shadow-manifest-file",
        type=Path,
        required=True,
    )
    transition.add_argument("--predecessor-shadow-manifest-sha256", required=True)
    transition.add_argument("--alembic-config-file", type=Path, required=True)
    transition.add_argument("--expected-predecessor-schema-head", required=True)
    transition.add_argument("--expected-target-schema-head", required=True)
    transition.set_defaults(handler=_render_schema_transition)

    render_acceptance = operations.add_parser(
        "render-acceptance",
        allow_abbrev=False,
        help="Render exact zero-capacity acceptance YAML and canonical evidence.",
    )
    render_acceptance.add_argument(
        "--file",
        type=Path,
        required=True,
        help="Strict non-secret personal-dev management-plane TOML profile.",
    )
    render_acceptance.add_argument(
        "--trusted-release-file",
        type=Path,
        required=True,
        help="Owner-only canonical trusted-release JSON document.",
    )
    render_acceptance.add_argument(
        "--trusted-release-sha256",
        required=True,
        help="Exact SHA-256 of the canonical trusted-release document.",
    )
    render_acceptance.add_argument(
        "--acceptance-plan-file",
        type=Path,
        required=True,
        help="Owner-only canonical zero-capacity acceptance plan.",
    )
    render_acceptance.add_argument(
        "--acceptance-plan-sha256",
        required=True,
        help="Exact SHA-256 of the canonical acceptance plan.",
    )
    _add_bound_evidence_arguments(render_acceptance)
    render_acceptance.set_defaults(handler=_render_acceptance)

    render_operational = operations.add_parser(
        "render-operational",
        allow_abbrev=False,
        help="Render exact durable zero-capacity operational YAML and evidence.",
    )
    render_operational.add_argument(
        "--file",
        type=Path,
        required=True,
        help="Strict non-secret personal-dev management-plane TOML profile.",
    )
    render_operational.add_argument(
        "--trusted-release-file",
        type=Path,
        required=True,
        help="Owner-only canonical trusted-release JSON document.",
    )
    render_operational.add_argument(
        "--trusted-release-sha256",
        required=True,
        help="Exact SHA-256 of the canonical trusted-release document.",
    )
    render_operational.add_argument(
        "--operational-plan-file",
        type=Path,
        required=True,
        help="Owner-only canonical durable operational plan.",
    )
    render_operational.add_argument(
        "--operational-plan-sha256",
        required=True,
        help="Exact SHA-256 of the canonical operational plan.",
    )
    _add_bound_evidence_arguments(render_operational)
    render_operational.set_defaults(handler=_render_operational)

    verify_acceptance_result = operations.add_parser(
        "verify-acceptance-result",
        allow_abbrev=False,
        help="Verify one canonical two-owner acceptance result without mutation.",
    )
    verify_acceptance_result.add_argument(
        "--acceptance-plan-file",
        type=Path,
        required=True,
        help="Owner-only canonical two-owner acceptance plan.",
    )
    verify_acceptance_result.add_argument(
        "--acceptance-plan-sha256",
        required=True,
        help="Exact SHA-256 of the canonical acceptance plan.",
    )
    verify_acceptance_result.add_argument(
        "--acceptance-result-file",
        type=Path,
        required=True,
        help="Owner-only canonical two-owner acceptance result.",
    )
    verify_acceptance_result.add_argument(
        "--acceptance-result-sha256",
        required=True,
        help="Exact SHA-256 of the canonical acceptance result.",
    )
    verify_acceptance_result.add_argument(
        "--acceptance-manifest-sha256",
        required=True,
        help="Exact SHA-256 of the acceptance manifest bound by the result.",
    )
    verify_acceptance_result.add_argument(
        "--rollback-shadow-manifest-file",
        type=Path,
        required=True,
        help="Owner-only exact rollback shadow manifest bound by the result.",
    )
    verify_acceptance_result.add_argument(
        "--rollback-shadow-status-file",
        type=Path,
        required=True,
        help="Owner-only canonical rollback-shadow status bound by the result.",
    )
    verify_acceptance_result.set_defaults(handler=_verify_acceptance_result)

    status_parser = operations.add_parser(
        "status",
        allow_abbrev=False,
        help="Compare bounded live state with one exact trusted shadow render.",
    )
    status_parser.add_argument(
        "--namespace",
        choices=["loom-dev"],
        default="loom-dev",
        help="Fixed shared infrastructure namespace (default: loom-dev).",
    )
    status_parser.add_argument(
        "--kubeconfig",
        type=Path,
        required=True,
        help="Absolute non-symlink path to the reviewed kubeconfig.",
    )
    status_parser.add_argument(
        "--file",
        type=Path,
        required=True,
        help="Strict non-secret personal-dev management-plane TOML profile.",
    )
    status_parser.add_argument(
        "--trusted-release-file",
        type=Path,
        required=True,
        help="Owner-only canonical trusted-release JSON document.",
    )
    status_parser.add_argument(
        "--trusted-release-sha256",
        required=True,
        help="Exact SHA-256 of the canonical trusted-release document.",
    )
    status_parser.set_defaults(handler=_status)

    status_acceptance = operations.add_parser(
        "status-acceptance",
        allow_abbrev=False,
        help="Observe one exact zero-capacity acceptance without mutation.",
    )
    status_acceptance.add_argument(
        "--namespace",
        choices=["loom-dev"],
        default="loom-dev",
        help="Fixed shared infrastructure namespace (default: loom-dev).",
    )
    status_acceptance.add_argument(
        "--kubeconfig",
        type=Path,
        required=True,
        help="Absolute non-symlink path to the reviewed kubeconfig.",
    )
    status_acceptance.add_argument(
        "--file",
        type=Path,
        required=True,
        help="Strict non-secret personal-dev management-plane TOML profile.",
    )
    status_acceptance.add_argument(
        "--trusted-release-file",
        type=Path,
        required=True,
        help="Owner-only canonical trusted-release JSON document.",
    )
    status_acceptance.add_argument(
        "--trusted-release-sha256",
        required=True,
        help="Exact SHA-256 of the canonical trusted-release document.",
    )
    status_acceptance.add_argument(
        "--acceptance-plan-file",
        type=Path,
        required=True,
        help="Owner-only canonical zero-capacity acceptance plan.",
    )
    status_acceptance.add_argument(
        "--acceptance-plan-sha256",
        required=True,
        help="Exact SHA-256 of the canonical acceptance plan.",
    )
    _add_bound_evidence_arguments(status_acceptance)
    status_acceptance.set_defaults(handler=_status_acceptance)

    status_operational = operations.add_parser(
        "status-operational",
        allow_abbrev=False,
        help="Observe one exact durable zero-capacity operational binding.",
    )
    status_operational.add_argument(
        "--namespace",
        choices=["loom-dev"],
        default="loom-dev",
        help="Fixed shared infrastructure namespace (default: loom-dev).",
    )
    status_operational.add_argument(
        "--kubeconfig",
        type=Path,
        required=True,
        help="Absolute non-symlink path to the reviewed kubeconfig.",
    )
    status_operational.add_argument(
        "--file",
        type=Path,
        required=True,
        help="Strict non-secret personal-dev management-plane TOML profile.",
    )
    status_operational.add_argument(
        "--trusted-release-file",
        type=Path,
        required=True,
        help="Owner-only canonical trusted-release JSON document.",
    )
    status_operational.add_argument(
        "--trusted-release-sha256",
        required=True,
        help="Exact SHA-256 of the canonical trusted-release document.",
    )
    status_operational.add_argument(
        "--operational-plan-file",
        type=Path,
        required=True,
        help="Owner-only canonical durable operational plan.",
    )
    status_operational.add_argument(
        "--operational-plan-sha256",
        required=True,
        help="Exact SHA-256 of the canonical operational plan.",
    )
    _add_bound_evidence_arguments(status_operational)
    status_operational.set_defaults(handler=_status_operational)


__all__ = ["add_personal_dev_control_plane_subparser"]
