from __future__ import annotations

import hashlib
import json
import os
import subprocess
from pathlib import Path

import pytest

from loom_cli.rollout.browser_runtime_readiness import (
    BROWSER_REPORT_SCHEMA_VERSION,
    browser_report_schema_digest,
    probe_browser_runtime,
)
from loom_cli.rollout.image_readiness import (
    ALL_BUILD_IMAGES,
    BROWSER_ENTRYPOINT,
    BROWSER_IMAGE,
    ImageArtifactSet,
    ImageDescriptor,
    image_plan_digest,
)


def _artifact() -> ImageArtifactSet:
    descriptors = {
        name: ImageDescriptor(
            image_id=f"sha256:{hashlib.sha256(name.encode()).hexdigest()}",
            revision="a" * 40,
            os="linux",
            architecture="amd64",
            entrypoint=BROWSER_ENTRYPOINT if name == BROWSER_IMAGE else (),
        )
        for name, _path in ALL_BUILD_IMAGES
    }
    return ImageArtifactSet(
        descriptors=descriptors,
        plan_digest=image_plan_digest(),
        artifact_digest="b" * 64,
    )


def _token(path: Path) -> None:
    path.write_text("not-persisted-in-evidence", encoding="utf-8")
    path.chmod(0o600)


def test_browser_runtime_launches_exact_image_without_network(tmp_path: Path) -> None:
    token = tmp_path / "admin-token"
    _token(token)
    calls: list[tuple[str, ...]] = []

    def run(argv):
        calls.append(tuple(argv))
        return subprocess.CompletedProcess(
            argv,
            0,
            json.dumps({"runtime": "ready", "schema_version": 4}, separators=(",", ":")),
            "",
        )

    evidence = probe_browser_runtime(
        run,
        image_artifact=_artifact(),
        token_path=token,
        service_uid=os.geteuid(),
        service_gid=os.getegid(),
    )

    command = calls[0]
    assert "--network=none" in command
    assert "--read-only" in command
    assert "--cap-drop=ALL" in command
    assert _artifact().descriptors[BROWSER_IMAGE].image_id in command
    assert token.read_text() not in " ".join(command)
    assert evidence.report_schema_digest == browser_report_schema_digest()
    assert BROWSER_REPORT_SCHEMA_VERSION == 4


def test_browser_runtime_rejects_unsafe_token_before_docker(tmp_path: Path) -> None:
    token = tmp_path / "admin-token"
    _token(token)
    token.chmod(0o666)
    calls: list[object] = []

    with pytest.raises(ValueError, match="metadata is unsafe"):
        probe_browser_runtime(
            lambda _argv: calls.append(object()),  # type: ignore[arg-type,return-value]
            image_artifact=_artifact(),
            token_path=token,
            service_uid=os.geteuid(),
            service_gid=os.getegid(),
        )
    assert calls == []


def test_browser_runtime_rejects_malformed_probe_output(tmp_path: Path) -> None:
    token = tmp_path / "admin-token"
    _token(token)

    with pytest.raises(ValueError, match="launch evidence is invalid"):
        probe_browser_runtime(
            lambda argv: subprocess.CompletedProcess(argv, 0, "secret=leak", ""),
            image_artifact=_artifact(),
            token_path=token,
            service_uid=os.geteuid(),
            service_gid=os.getegid(),
        )
