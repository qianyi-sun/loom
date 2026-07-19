from __future__ import annotations

import hashlib
import json
import subprocess
from pathlib import Path

import pytest

from loom_cli.rollout.image_readiness import (
    ALL_BUILD_IMAGES,
    BROWSER_ENTRYPOINT,
    BROWSER_IMAGE,
    REHEARSAL_POSTGRES_ENTRYPOINT,
    REHEARSAL_POSTGRES_IMAGE,
    REVISION_LABEL,
    build_exact_images,
    verify_image_contract,
)


def _inspect_payload(name: str, revision: str, *, image_id: str | None = None) -> str:
    return json.dumps(
        [
            {
                "Id": image_id or f"sha256:{hashlib.sha256(name.encode()).hexdigest()}",
                "Os": "linux",
                "Architecture": "amd64",
                "Config": {
                    "Labels": {REVISION_LABEL: revision},
                    "Entrypoint": (
                        list(BROWSER_ENTRYPOINT)
                        if name == BROWSER_IMAGE
                        else (
                            list(REHEARSAL_POSTGRES_ENTRYPOINT)
                            if name == REHEARSAL_POSTGRES_IMAGE
                            else []
                        )
                    ),
                },
            }
        ]
    )


def test_build_exact_images_reuses_matching_and_rebuilds_drifted(tmp_path: Path) -> None:
    revision = "a" * 40
    calls: list[tuple[tuple[str, ...], Path | None]] = []
    rebuilt: set[str] = set()

    def run(argv, cwd):
        command = tuple(argv)
        calls.append((command, cwd))
        if command[:3] == ("docker", "image", "inspect"):
            tag = command[-1]
            name = tag.split(":", 1)[0]
            observed = revision if name != "loom-worker" or name in rebuilt else "b" * 40
            return subprocess.CompletedProcess(argv, 0, _inspect_payload(name, observed), "")
        name = command[command.index("-t") + 1].split(":", 1)[0]
        rebuilt.add(name)
        return subprocess.CompletedProcess(argv, 0, "", "")

    result = build_exact_images(
        run,
        candidate_root=tmp_path,
        image_tag="staging-aaaaaaaa",
        resolved_sha=revision,
    )

    assert set(result.image_digests) == {name for name, _path in ALL_BUILD_IMAGES}
    assert rebuilt == {"loom-worker"}
    build = next(command for command, _cwd in calls if command[:2] == ("docker", "build"))
    assert f"{REVISION_LABEL}={revision}" in build
    assert len(result.artifact_digest) == 64


def test_verify_image_contract_rejects_digest_drift_without_build() -> None:
    revision = "a" * 40
    expected = {
        name: f"sha256:{hashlib.sha256(name.encode()).hexdigest()}"
        for name, _path in ALL_BUILD_IMAGES
    }

    def run(argv, cwd):
        assert cwd is None
        name = argv[-1].split(":", 1)[0]
        image_id = "sha256:" + "f" * 64 if name == "loom-worker" else expected[name]
        return subprocess.CompletedProcess(
            argv, 0, _inspect_payload(name, revision, image_id=image_id), ""
        )

    with pytest.raises(ValueError, match="contract drifted for loom-worker"):
        verify_image_contract(
            run,
            image_tag="staging-aaaaaaaa",
            resolved_sha=revision,
            expected_digests=expected,
        )


def test_browser_entrypoint_is_part_of_contract() -> None:
    revision = "a" * 40
    expected = {
        name: f"sha256:{hashlib.sha256(name.encode()).hexdigest()}"
        for name, _path in ALL_BUILD_IMAGES
    }

    def run(argv, _cwd):
        name = argv[-1].split(":", 1)[0]
        payload = json.loads(_inspect_payload(name, revision, image_id=expected[name]))
        if name == BROWSER_IMAGE:
            payload[0]["Config"]["Entrypoint"] = ["node", "/wrong.js"]
        return subprocess.CompletedProcess(argv, 0, json.dumps(payload), "")

    with pytest.raises(ValueError, match="browser-smoke"):
        verify_image_contract(
            run,
            image_tag="staging-aaaaaaaa",
            resolved_sha=revision,
            expected_digests=expected,
        )


def test_rehearsal_postgres_entrypoint_is_part_of_contract() -> None:
    revision = "a" * 40
    expected = {
        name: f"sha256:{hashlib.sha256(name.encode()).hexdigest()}"
        for name, _path in ALL_BUILD_IMAGES
    }

    def run(argv, _cwd):
        name = argv[-1].split(":", 1)[0]
        payload = json.loads(_inspect_payload(name, revision, image_id=expected[name]))
        if name == REHEARSAL_POSTGRES_IMAGE:
            payload[0]["Config"]["Entrypoint"] = ["/bin/sh"]
        return subprocess.CompletedProcess(argv, 0, json.dumps(payload), "")

    with pytest.raises(ValueError, match="rehearsal-postgres"):
        verify_image_contract(
            run,
            image_tag="staging-aaaaaaaa",
            resolved_sha=revision,
            expected_digests=expected,
        )
