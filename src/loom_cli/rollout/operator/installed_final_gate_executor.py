"""Production composition for the fixed installed final-gate helper."""

from __future__ import annotations

import json
import os
import subprocess
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from loom_cli.cluster_config import load_cluster_config
from loom_cli.rollout.external_supervisor_readiness import (
    STAGING_ROLLOUT_EXECUTION_HOST,
)
from loom_cli.rollout.final_gate_readiness import FinalGateResult
from loom_cli.rollout.install_attestation import VerifiedRunnerInstall, verify_runner_install
from loom_cli.rollout.preflight_contract import CheckOperation

from .config import OperatorConfig, candidate_sha_from_runner_repo, environment_authority
from .final_browser_executor import FinalBrowserExecutor
from .final_capacity_executor import FinalCapacityExecutor
from .final_gate_plan import FinalGatePlan
from .final_smoke_executor import FinalSmokeExecutor
from .final_summary_executor import FinalSummaryExecutor
from .protected_apply_executor import (
    KubernetesProtectedConvergenceExecutor,
    MigrationEpochProtectedApplyExecutor,
    SubprocessProtectedApplyCommandRunner,
)
from .protected_environment_state_component import (
    HttpxProtectedEnvironmentStateTransport,
)
from .protected_external_supervisor_transport import (
    ProtectedExternalSupervisorTransport,
    build_fixed_external_supervisor_transport,
)
from .protected_gb10_external_supervisor_transport import (
    GB10_CONTROLLER_EXECUTION_HOST,
    build_fixed_gb10_external_supervisor_transport,
)
from .protected_gb10_transport import build_fixed_gb10_ssh_transport
from .resume_runtime_upgrade import (
    ResumeRuntimeUpgradeAuthority,
    build_installed_resume_runtime_upgrade_authority,
)
from .staging_smoke_authority import staging_smoke_authority

_CONFIG_PATH = Path("/etc/loom/staging-rollout.toml")
_MAX_HTTP_BODY = 1024 * 1024
_MAX_COMMAND_OUTPUT = 64 * 1024
_MAX_COMMAND_INPUT = 4 * 1024 * 1024


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(
        self,
        req: urllib.request.Request,
        fp: Any,
        code: int,
        msg: str,
        headers: Any,
        newurl: str,
    ) -> urllib.request.Request | None:
        return None


@dataclass(frozen=True, slots=True)
class BoundedStagingSmokeTransport:
    base_url: str
    timeout_seconds: float = 30.0

    def __post_init__(self) -> None:
        parsed = urllib.parse.urlsplit(self.base_url)
        if (
            parsed.scheme != "https"
            or not parsed.netloc
            or parsed.query
            or parsed.fragment
            or not parsed.path
            or parsed.path.endswith("/")
            or not 0 < self.timeout_seconds <= 60
        ):
            raise ValueError("installed smoke transport route is invalid")

    def __call__(
        self,
        method: str,
        path: str,
        token: str,
        payload: Mapping[str, object] | None,
        headers: Mapping[str, str] | None,
    ) -> tuple[int, bytes]:
        parsed = urllib.parse.urlsplit(path)
        if (
            method not in {"GET", "POST"}
            or not path.startswith("/api/v1/")
            or parsed.scheme
            or parsed.netloc
            or parsed.fragment
            or not token
            or len(token) > 64 * 1024
            or (payload is None) is not (method == "GET")
            or (headers is not None and set(headers) != {"X-Loom-Admin-Actor"})
        ):
            raise ValueError("installed smoke request authority is invalid")
        body = None
        request_headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {token}",
        }
        if payload is not None:
            body = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
            if len(body) > _MAX_HTTP_BODY:
                raise ValueError("installed smoke request is too large")
            request_headers["Content-Type"] = "application/json"
        if headers is not None:
            request_headers.update(headers)
        request = urllib.request.Request(
            self.base_url + path,
            data=body,
            headers=request_headers,
            method=method,
        )
        opener = urllib.request.build_opener(_NoRedirect())
        try:
            with opener.open(request, timeout=self.timeout_seconds) as response:
                status = int(response.status)
                response_body = response.read(_MAX_HTTP_BODY + 1)
        except urllib.error.HTTPError as exc:
            status = int(exc.code)
            response_body = exc.read(_MAX_HTTP_BODY + 1)
        if len(response_body) > _MAX_HTTP_BODY:
            raise ValueError("installed smoke response is too large")
        return status, response_body


@dataclass(frozen=True, slots=True)
class InstalledFinalGateExecutor:
    config: OperatorConfig
    service_uid: int
    service_gid: int
    verify_install: Callable[..., VerifiedRunnerInstall] = verify_runner_install
    resume_runtime_upgrade: ResumeRuntimeUpgradeAuthority | None = None

    def __post_init__(self) -> None:
        if (
            self.config.config_path != _CONFIG_PATH
            or self.config.environment != "staging"
            or self.config.namespace != "loom-staging"
            or self.service_uid <= 0
            or self.service_gid <= 0
            or os.geteuid() != self.service_uid
            or not callable(self.verify_install)
        ):
            raise ValueError("installed final gate authority is invalid")

    def __call__(
        self,
        check_id: str,
        operation: CheckOperation,
        plan: FinalGatePlan,
    ) -> FinalGateResult:
        installed, effective_config = self._validate_plan(plan)
        if check_id in {"final.protected-apply", "final.convergence"}:
            gb10_controller = build_fixed_gb10_external_supervisor_transport(
                candidate_sha=plan.candidate_sha,
                candidate_tree=plan.candidate_tree,
                run=self._supervisor_ssh_run,
            )
            container_registry = str(
                load_cluster_config(effective_config.cluster_config_path).container_registry
            )
            protected_runner = SubprocessProtectedApplyCommandRunner(
                kubeconfig=effective_config.kubeconfig_path
            )
            gb10 = build_fixed_gb10_ssh_transport(
                effective_config.cluster_config_path,
                expected_hosts=tuple(plan.gb10_boot_ids),
                run=self._ssh_run,
                max_concurrency=effective_config.gb10_prep_concurrency,
            )
            external_supervisors: dict[str, ProtectedExternalSupervisorTransport] = {
                GB10_CONTROLLER_EXECUTION_HOST: gb10_controller,
                STAGING_ROLLOUT_EXECUTION_HOST: (
                    build_fixed_external_supervisor_transport(service_uid=self.service_uid)
                ),
            }
            environment_state = HttpxProtectedEnvironmentStateTransport(
                candidate_root=effective_config.runner_repo,
                admin_token_path=Path(effective_config.admin_token_source.removeprefix("file:")),
                worker_token_path=Path(effective_config.worker_token_source.removeprefix("file:")),
                expected_env_template_sha256=installed.attestation.asset_sha256[
                    "worker-env-template"
                ],
                cp_url=effective_config.cp_url,
                service_uid=self.service_uid,
            )
        if check_id == "final.protected-apply":
            return MigrationEpochProtectedApplyExecutor(
                state_root=effective_config.state_root,
                service_uid=self.service_uid,
                runner=protected_runner,
                gb10_transport=gb10,
                environment_state_transport=environment_state,
                candidate_root=effective_config.runner_repo,
                external_supervisor_transports=external_supervisors,
                container_registry=container_registry,
            )(check_id, operation, plan)
        if check_id == "final.convergence":
            return KubernetesProtectedConvergenceExecutor(
                service_uid=self.service_uid,
                runner=protected_runner,
                gb10_transport=gb10,
                environment_state_transport=environment_state,
                candidate_root=effective_config.runner_repo,
                external_supervisor_transports=external_supervisors,
                container_registry=container_registry,
            )(check_id, operation, plan)
        if check_id == "final.capacity":
            return FinalCapacityExecutor(
                transport_factory=lambda: build_fixed_gb10_external_supervisor_transport(
                    candidate_sha=plan.candidate_sha,
                    candidate_tree=plan.candidate_tree,
                    run=self._capacity_ssh_run,
                )
            )(check_id, operation, plan)
        if check_id == "final.smoke":
            return FinalSmokeExecutor(
                service_uid=self.service_uid,
                token_path=Path(effective_config.admin_token_source.removeprefix("file:")),
                expected_token_fingerprint=effective_config.expect_admin_token_fingerprint,
                authority=staging_smoke_authority(effective_config),
                request=BoundedStagingSmokeTransport(plan.route),
            )(check_id, operation, plan)
        if check_id == "final.browser":
            return FinalBrowserExecutor(
                state_root=effective_config.state_root,
                service_uid=self.service_uid,
                service_gid=self.service_gid,
                token_path=Path(effective_config.admin_token_source.removeprefix("file:")),
                expected_token_fingerprint=effective_config.expect_admin_token_fingerprint,
                run=self._browser_run,
            )(check_id, operation, plan)
        if check_id == "final.summary":
            return FinalSummaryExecutor(effective_config.state_root, self.service_uid)(
                check_id, operation, plan
            )
        raise ValueError("installed final gate check has no fixed executor")

    def _validate_plan(
        self,
        plan: FinalGatePlan,
    ) -> tuple[VerifiedRunnerInstall, OperatorConfig]:
        try:
            installed = self.verify_install(service_uid=self.service_uid)
        except (OSError, ValueError) as exc:
            raise ValueError("installed final gate runner install drifted") from exc
        statement = installed.attestation
        effective_config = self.config
        runtime_upgraded = plan.runner_config_hash != self.config.config_sha256
        if runtime_upgraded:
            if self.resume_runtime_upgrade is None or plan.source_mode != "merged-dev":
                raise ValueError("installed final gate plan drifted from runner config")
            authority = environment_authority(self.config.short_name)
            historical_repo = authority.candidate_runtime_root / plan.candidate_sha / "repo"
            historical_cluster_config = historical_repo / authority.candidate_cluster_config
            try:
                effective_config = self.resume_runtime_upgrade.resolve(
                    self.config,
                    candidate_sha=plan.candidate_sha,
                    candidate_tree=plan.candidate_tree,
                    runner_config_sha256=plan.runner_config_hash,
                    cluster_config_path=str(historical_cluster_config),
                )
                current_sha = candidate_sha_from_runner_repo(
                    self.config.runner_repo,
                    authority=authority,
                )
            except Exception as exc:
                raise ValueError("installed final gate plan drifted from runner config") from exc
            if (
                effective_config.runner_repo != historical_repo
                or effective_config.cluster_config_path != historical_cluster_config
                or statement.asset_sha256["config"] != self.config.config_sha256
                or statement.source_mode != self.config.source_mode
                or statement.source_sha != current_sha
                or statement.source_tree_sha != "none"
                or statement.source_base_sha != "none"
            ):
                raise ValueError("installed final gate plan drifted from runner config")
        expected_tree = (
            plan.runner_source_tree if plan.source_mode == "sealed-cumulative" else "none"
        )
        expected_base = (
            plan.approved_base_sha if plan.source_mode == "sealed-cumulative" else "none"
        )
        if (
            plan.runner_config_hash != effective_config.config_sha256
            or plan.environment != effective_config.environment
            or plan.namespace != effective_config.namespace
            or plan.source_mode != effective_config.source_mode
            or plan.runner_source_sha != plan.candidate_sha
            or plan.runner_source_tree != plan.candidate_tree
            or not installed.ready
            or (
                not runtime_upgraded
                and (
                    statement.payload_digest != plan.runner_install_hash
                    or statement.source_mode != plan.source_mode
                    or statement.source_sha != plan.runner_source_sha
                    or statement.source_tree_sha != expected_tree
                    or statement.source_base_sha != expected_base
                    or statement.asset_sha256["config"] != effective_config.config_sha256
                )
            )
            or (
                plan.source_mode == "sealed-cumulative"
                and (
                    plan.runner_source_sha != effective_config.source_commit_sha
                    or plan.runner_source_tree != effective_config.source_tree_sha
                    or plan.approved_base_sha != effective_config.source_base_sha
                    or plan.runner_source_tree != plan.candidate_tree
                )
            )
        ):
            raise ValueError("installed final gate plan drifted from runner config")
        return installed, effective_config

    @staticmethod
    def _ssh_run(argv: Sequence[str]) -> subprocess.CompletedProcess[str]:
        return _run_command(argv, timeout=180, capture_output=True)

    @staticmethod
    def _supervisor_ssh_run(
        argv: Sequence[str],
        input_payload: str,
    ) -> subprocess.CompletedProcess[str]:
        return _run_command(
            argv,
            timeout=180,
            capture_output=True,
            input_payload=input_payload,
        )

    @staticmethod
    def _capacity_ssh_run(
        argv: Sequence[str],
        input_payload: str,
    ) -> subprocess.CompletedProcess[str]:
        return _run_command(
            argv,
            timeout=1500,
            capture_output=True,
            input_payload=input_payload,
        )

    @staticmethod
    def _browser_run(argv: Sequence[str]) -> subprocess.CompletedProcess[str]:
        return _run_command(argv, timeout=900, capture_output=False)


def _run_command(
    argv: Sequence[str],
    *,
    timeout: int,
    capture_output: bool,
    input_payload: str | None = None,
) -> subprocess.CompletedProcess[str]:
    command = tuple(argv)
    if (
        not command
        # A newline is NOT rejected: the GB10 fleet observation and other remote
        # probes are dispatched as a single `python3 -c <multi-line source>` argv
        # element (identical to the read-only gb10_readiness probe), and every
        # command here runs via subprocess argv with no shell, so an embedded
        # newline is literal argument text, not an injection vector. A NUL byte
        # is still rejected (it cannot appear in an execve argument).
        or any(not item or "\x00" in item for item in command)
        or not 1 <= timeout <= 1800
    ):
        raise ValueError("installed final command is invalid")
    if input_payload is not None and (
        type(input_payload) is not str or len(input_payload.encode()) > _MAX_COMMAND_INPUT
    ):
        raise ValueError("installed final command input is too large")
    environment = {
        "HOME": "/var/lib/loom-staging-rollout",
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "PATH": "/usr/local/bin:/usr/bin:/bin",
        "PYTHONDONTWRITEBYTECODE": "1",
        "XDG_RUNTIME_DIR": f"/run/user/{os.geteuid()}",
    }
    result = subprocess.run(
        command,
        check=False,
        shell=False,
        capture_output=capture_output,
        stdout=None if capture_output else subprocess.DEVNULL,
        stderr=None if capture_output else subprocess.DEVNULL,
        text=True,
        input=input_payload,
        timeout=timeout,
        env=environment,
    )
    if capture_output and (
        len(result.stdout.encode()) > _MAX_COMMAND_OUTPUT
        or len(result.stderr.encode()) > _MAX_COMMAND_OUTPUT
    ):
        raise RuntimeError("installed final command output is too large")
    return result


def build_installed_final_gate_executor() -> InstalledFinalGateExecutor:
    configured = os.environ.get("LOOM_STAGING_ROLLOUT_CONFIG")
    if configured != str(_CONFIG_PATH):
        raise ValueError("installed final gate config path is invalid")
    config = OperatorConfig.load(_CONFIG_PATH)
    resume_runtime_upgrade = (
        build_installed_resume_runtime_upgrade_authority(
            config,
            service_uid=os.geteuid(),
            run=lambda argv: _run_command(argv, timeout=60, capture_output=True),
        )
        if config.source_mode == "merged-dev"
        else None
    )
    return InstalledFinalGateExecutor(
        config=config,
        service_uid=os.geteuid(),
        service_gid=os.getegid(),
        resume_runtime_upgrade=resume_runtime_upgrade,
    )


__all__ = [
    "BoundedStagingSmokeTransport",
    "InstalledFinalGateExecutor",
    "build_installed_final_gate_executor",
]
