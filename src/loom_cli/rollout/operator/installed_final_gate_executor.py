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

from .config import OperatorConfig
from .final_browser_executor import FinalBrowserExecutor
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
    build_fixed_external_supervisor_transport,
)
from .protected_gb10_transport import build_fixed_gb10_ssh_transport
from .staging_smoke_authority import staging_smoke_authority

_CONFIG_PATH = Path("/etc/loom/staging-rollout.toml")
_MAX_HTTP_BODY = 1024 * 1024
_MAX_COMMAND_OUTPUT = 64 * 1024


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
        self._validate_plan(plan)
        if check_id in {"final.protected-apply", "final.convergence"}:
            container_registry = str(
                load_cluster_config(self.config.cluster_config_path).container_registry
            )
            protected_runner = SubprocessProtectedApplyCommandRunner(
                kubeconfig=self.config.kubeconfig_path
            )
            gb10 = build_fixed_gb10_ssh_transport(
                self.config.cluster_config_path,
                expected_hosts=tuple(plan.gb10_boot_ids),
                run=self._ssh_run,
                max_concurrency=self.config.gb10_prep_concurrency,
            )
            external_supervisors = build_fixed_external_supervisor_transport(
                service_uid=self.service_uid
            )
            environment_state = HttpxProtectedEnvironmentStateTransport(
                candidate_root=self.config.runner_repo,
                admin_token_path=Path(self.config.admin_token_source.removeprefix("file:")),
                cp_url=self.config.cp_url,
                service_uid=self.service_uid,
            )
        if check_id == "final.protected-apply":
            return MigrationEpochProtectedApplyExecutor(
                state_root=self.config.state_root,
                service_uid=self.service_uid,
                runner=protected_runner,
                gb10_transport=gb10,
                environment_state_transport=environment_state,
                candidate_root=self.config.runner_repo,
                external_supervisor_transport=external_supervisors,
                external_supervisor_execution_host=STAGING_ROLLOUT_EXECUTION_HOST,
                container_registry=container_registry,
            )(check_id, operation, plan)
        if check_id == "final.convergence":
            return KubernetesProtectedConvergenceExecutor(
                service_uid=self.service_uid,
                runner=protected_runner,
                gb10_transport=gb10,
                environment_state_transport=environment_state,
                candidate_root=self.config.runner_repo,
                external_supervisor_transport=external_supervisors,
                external_supervisor_execution_host=STAGING_ROLLOUT_EXECUTION_HOST,
                container_registry=container_registry,
            )(check_id, operation, plan)
        if check_id == "final.smoke":
            return FinalSmokeExecutor(
                service_uid=self.service_uid,
                token_path=Path(self.config.admin_token_source.removeprefix("file:")),
                expected_token_fingerprint=self.config.expect_admin_token_fingerprint,
                authority=staging_smoke_authority(self.config),
                request=BoundedStagingSmokeTransport(plan.route),
            )(check_id, operation, plan)
        if check_id == "final.browser":
            return FinalBrowserExecutor(
                state_root=self.config.state_root,
                service_uid=self.service_uid,
                service_gid=self.service_gid,
                token_path=Path(self.config.admin_token_source.removeprefix("file:")),
                expected_token_fingerprint=self.config.expect_admin_token_fingerprint,
                run=self._browser_run,
            )(check_id, operation, plan)
        if check_id == "final.summary":
            return FinalSummaryExecutor(self.config.state_root, self.service_uid)(
                check_id, operation, plan
            )
        raise ValueError("installed final gate check has no fixed executor")

    def _validate_plan(self, plan: FinalGatePlan) -> None:
        try:
            installed = self.verify_install(service_uid=self.service_uid)
        except (OSError, ValueError) as exc:
            raise ValueError("installed final gate runner install drifted") from exc
        statement = installed.attestation
        expected_tree = (
            plan.runner_source_tree if plan.source_mode == "sealed-cumulative" else "none"
        )
        expected_base = (
            plan.approved_base_sha if plan.source_mode == "sealed-cumulative" else "none"
        )
        if (
            plan.runner_config_hash != self.config.config_sha256
            or plan.environment != self.config.environment
            or plan.namespace != self.config.namespace
            or plan.source_mode != self.config.source_mode
            or not installed.ready
            or statement.payload_digest != plan.runner_install_hash
            or statement.source_mode != plan.source_mode
            or statement.source_sha != plan.runner_source_sha
            or statement.source_sha != plan.candidate_sha
            or statement.source_tree_sha != expected_tree
            or statement.source_base_sha != expected_base
            or statement.asset_sha256["config"] != self.config.config_sha256
            or (
                plan.source_mode == "sealed-cumulative"
                and (
                    plan.runner_source_sha != self.config.source_commit_sha
                    or plan.runner_source_tree != self.config.source_tree_sha
                    or plan.approved_base_sha != self.config.source_base_sha
                    or plan.runner_source_tree != plan.candidate_tree
                )
            )
        ):
            raise ValueError("installed final gate plan drifted from runner config")

    @staticmethod
    def _ssh_run(argv: Sequence[str]) -> subprocess.CompletedProcess[str]:
        return _run_command(argv, timeout=180, capture_output=True)

    @staticmethod
    def _browser_run(argv: Sequence[str]) -> subprocess.CompletedProcess[str]:
        return _run_command(argv, timeout=900, capture_output=False)


def _run_command(
    argv: Sequence[str],
    *,
    timeout: int,
    capture_output: bool,
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
        or not 1 <= timeout <= 900
    ):
        raise ValueError("installed final command is invalid")
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
        capture_output=capture_output,
        stdout=None if capture_output else subprocess.DEVNULL,
        stderr=None if capture_output else subprocess.DEVNULL,
        text=True,
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
    return InstalledFinalGateExecutor(
        config=config,
        service_uid=os.geteuid(),
        service_gid=os.getegid(),
    )


__all__ = [
    "BoundedStagingSmokeTransport",
    "InstalledFinalGateExecutor",
    "build_installed_final_gate_executor",
]
