"""Candidate-bound browser runtime contract exercised before backup."""

from __future__ import annotations

import hashlib
import json
import os
import re
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from loom_cli.rollout.credential_authority import read_trusted_file
from loom_cli.rollout.image_readiness import BROWSER_IMAGE, ImageArtifactSet

BROWSER_REPORT_SCHEMA_VERSION = 4
_IMAGE_ID_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_RUNTIME_OUTPUT = {"runtime": "ready", "schema_version": BROWSER_REPORT_SCHEMA_VERSION}
_NODE_PROBE = (
    "const fs=require('fs');"
    "const p='/run/secrets/admin-token';"
    "const s=fs.statSync(p);"
    "if(!s.isFile()||(s.mode&0o022)!==0)process.exit(2);"
    f"process.stdout.write('{json.dumps(_RUNTIME_OUTPUT, separators=(',', ':'))}');"
)


class CommandResult(Protocol):
    @property
    def returncode(self) -> int: ...

    @property
    def stdout(self) -> str: ...


CommandRunner = Callable[[Sequence[str]], CommandResult]


@dataclass(frozen=True, slots=True)
class BrowserRuntimeEvidence:
    image_id: str
    token_metadata_fingerprint: str
    token_acl_fingerprint: str
    report_schema_digest: str
    launch_ready: bool

    def __post_init__(self) -> None:
        if (
            _IMAGE_ID_RE.fullmatch(self.image_id) is None
            or any(
                len(value) != 64 or any(char not in "0123456789abcdef" for char in value)
                for value in (
                    self.token_metadata_fingerprint,
                    self.token_acl_fingerprint,
                    self.report_schema_digest,
                )
            )
            or not self.launch_ready
        ):
            raise ValueError("browser runtime evidence is invalid")


def browser_report_schema_digest() -> str:
    contract = {
        "cleanup": ["logout_status", "auth_me_after_logout_status"],
        "deployment_identity": [
            "expected_deployed_sha",
            "observed_deployed_sha",
            "matched",
        ],
        "failure_code": "string-or-null",
        "rollout_binding": [
            "request_id",
            "attempt_number",
            "request_envelope_sha256",
            "resolved_sha",
        ],
        "rehearsal_binding": [
            "plan_sha256",
            "isolation_id",
            "resolved_sha",
        ],
        "schema_version": BROWSER_REPORT_SCHEMA_VERSION,
        "status": "pass-or-fail",
    }
    return hashlib.sha256(
        json.dumps(contract, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def browser_runtime_command(
    *,
    image_id: str,
    token_path: Path,
    service_uid: int,
    service_gid: int,
) -> tuple[str, ...]:
    if (
        _IMAGE_ID_RE.fullmatch(image_id) is None
        or not token_path.is_absolute()
        or ".." in token_path.parts
        or any(character in str(token_path) for character in (",", "\n", "\r", "\x00"))
        or service_uid < 0
        or service_gid < 0
    ):
        raise ValueError("browser runtime launch binding is invalid")
    return (
        "docker",
        "run",
        "--rm",
        "--pull=never",
        "--read-only",
        "--cap-drop=ALL",
        "--security-opt=no-new-privileges",
        "--network=none",
        "--pids-limit=64",
        "--memory=256m",
        "--cpus=1",
        "--user",
        f"{service_uid}:{service_gid}",
        "--env",
        "HOME=/tmp",
        "--tmpfs",
        "/tmp:rw,nosuid,nodev,size=32m,mode=0700",
        "--mount",
        f"type=bind,src={token_path},dst=/run/secrets/admin-token,readonly,bind-propagation=rprivate",
        "--entrypoint",
        "node",
        image_id,
        "-e",
        _NODE_PROBE,
    )


def probe_browser_runtime(
    run: CommandRunner,
    *,
    image_artifact: ImageArtifactSet,
    token_path: Path,
    service_uid: int | None = None,
    service_gid: int | None = None,
) -> BrowserRuntimeEvidence:
    """Prove token authority and a network-isolated exact-image launch."""
    uid = os.geteuid() if service_uid is None else service_uid
    gid = os.getegid() if service_gid is None else service_gid
    token = read_trusted_file(
        token_path,
        service_uid=uid,
        private=True,
        allow_qianyi_owner=True,
        max_bytes=64 * 1024,
        require_nonempty=True,
    )
    image_id = image_artifact.descriptors[BROWSER_IMAGE].image_id
    result = run(
        browser_runtime_command(
            image_id=image_id,
            token_path=token_path,
            service_uid=uid,
            service_gid=gid,
        )
    )
    try:
        output = json.loads(result.stdout)
    except (json.JSONDecodeError, TypeError) as exc:
        raise ValueError("browser runtime launch evidence is invalid") from exc
    if result.returncode != 0 or output != _RUNTIME_OUTPUT:
        raise ValueError("browser runtime launch failed")
    return BrowserRuntimeEvidence(
        image_id=image_id,
        token_metadata_fingerprint=token.metadata_fingerprint,
        token_acl_fingerprint=token.acl_fingerprint,
        report_schema_digest=browser_report_schema_digest(),
        launch_ready=True,
    )


__all__ = [
    "BROWSER_REPORT_SCHEMA_VERSION",
    "BrowserRuntimeEvidence",
    "browser_report_schema_digest",
    "browser_runtime_command",
    "probe_browser_runtime",
]
