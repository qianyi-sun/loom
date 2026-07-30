from __future__ import annotations

import hashlib
import io
import json
import tarfile
from pathlib import Path
from types import SimpleNamespace

import pytest

from loom_cli.rollout.steps import s04_gb10_prep
from loom_cli.rollout.steps.base import VerifyOutcome
from loom_cli.rollout.steps.candidate_source import CandidateToolingError
from loom_cli.rollout.steps.s04_gb10_prep import GB10Host
from loom_cli.rollout.steps.subprocess_util import SubprocessResult


def _write_image_evidence(env_state_dir: Path) -> str:
    candidate_sha = "b" * 40
    image_tag = "staging-bbbbbbb"
    config = json.dumps(
        {
            "architecture": "arm64",
            "os": "linux",
            "config": {
                "Labels": {
                    "org.opencontainers.image.revision": candidate_sha,
                    "loom.source-archive.sha256": "c" * 64,
                },
                "Cmd": ["python", "-m", "loom_worker"],
                "Entrypoint": None,
            },
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    config_digest = hashlib.sha256(config).hexdigest()
    layer = b"exact layer"
    layer_digest = hashlib.sha256(layer).hexdigest()
    manifest = json.dumps(
        [
            {
                "Config": f"{config_digest}.json",
                "RepoTags": [f"loom-worker:{image_tag}-arm64"],
                "Layers": [f"{layer_digest}.tar"],
            }
        ],
        separators=(",", ":"),
    ).encode()
    archive = env_state_dir / "staging-gb10-worker-arm64.tar"
    with tarfile.open(archive, "w") as output:
        for name, payload in (
            ("manifest.json", manifest),
            (f"{config_digest}.json", config),
            (f"{layer_digest}.tar", layer),
        ):
            member = tarfile.TarInfo(name)
            member.size = len(payload)
            member.mode = 0o600
            output.addfile(member, io.BytesIO(payload))
    archive.chmod(0o600)
    from loom_cli.rollout.steps.s10_env_state import (
        _inspect_gb10_arm64_worker_archive,
    )

    evidence = _inspect_gb10_arm64_worker_archive(
        archive,
        candidate_sha=candidate_sha,
        image_tag=image_tag,
    )
    (env_state_dir / "staging-gb10-worker-arm64.json").write_text(
        json.dumps(evidence, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return str(evidence["image_id"])


def test_s04_resolves_worker_image_from_sibling_env_state_and_binds_id(
    tmp_path: Path,
) -> None:
    env_state_dir = tmp_path / "11-env-state"
    host_dir = tmp_path / "12-gb10-prep" / "host-trt-gb10-1"
    env_state_dir.mkdir()
    host_dir.mkdir(parents=True)
    image_id = _write_image_evidence(env_state_dir)
    ctx = SimpleNamespace(
        resolved_sha="b" * 40,
        image_tag="staging-bbbbbbb",
    )

    archive, evidence = s04_gb10_prep._external_worker_image_artifact(
        ctx,
        host_dir=host_dir,
    )

    assert archive == env_state_dir / "staging-gb10-worker-arm64.tar"
    assert evidence["image_id"] == image_id

    manifest = env_state_dir / "staging-gb10-worker-arm64.json"
    payload = json.loads(manifest.read_text(encoding="utf-8"))
    payload["image_id"] = "sha256:" + ("c" * 64)
    manifest.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(CandidateToolingError, match="evidence drifted"):
        s04_gb10_prep._external_worker_image_artifact(
            ctx,
            host_dir=host_dir,
        )


def test_s04_load_streams_archive_to_fixed_docker_image_load(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    archive = tmp_path / "image.tar"
    archive.write_bytes(b"archive bytes")
    host = GB10Host(
        ssh_target="trt-gb10-1",
        repo_path="/home/qianyi/loom-worker-build-staging",
        env_file_path="/home/qianyi/loom-worker-build-staging/.env",
    )
    captured: dict[str, object] = {}

    def fake_ssh_argv(selected, remote_cmd):  # type: ignore[no-untyped-def]
        captured["host"] = selected.ssh_target
        captured["remote_cmd"] = remote_cmd
        return [
            "/bin/sh",
            "-c",
            'test "$(cat)" = "archive bytes" && printf "Loaded image\\n"',
        ]

    monkeypatch.setattr(s04_gb10_prep, "_ssh_argv", fake_ssh_argv)

    assert s04_gb10_prep._load_external_worker_image(host, archive=archive)
    assert captured == {
        "host": "trt-gb10-1",
        "remote_cmd": "/usr/bin/env docker image load",
    }


def test_s04_load_rejects_bounded_remote_diagnostic(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    archive = tmp_path / "image.tar"
    archive.write_bytes(b"archive bytes")
    host = GB10Host(
        ssh_target="trt-gb10-1",
        repo_path="/home/qianyi/loom-worker-build-staging",
        env_file_path="/home/qianyi/loom-worker-build-staging/.env",
    )
    monkeypatch.setattr(
        s04_gb10_prep,
        "_ssh_argv",
        lambda *_args, **_kwargs: [
            "/bin/sh",
            "-c",
            "cat >/dev/null; head -c 65537 /dev/zero >&2",
        ],
    )

    assert not s04_gb10_prep._load_external_worker_image(host, archive=archive)


def test_s04_done_verification_binds_expected_artifact_id(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected_id = "sha256:" + ("a" * 64)
    host = GB10Host(
        ssh_target="trt-gb10-1",
        repo_path="/home/qianyi/loom-worker-build-staging",
        env_file_path="/home/qianyi/loom-worker-build-staging/.env",
        node_agent_service="loom-gb10-node-agent.service",
    )
    captured: dict[str, str | None] = {}
    monkeypatch.setattr(
        s04_gb10_prep,
        "_external_worker_image_artifact",
        lambda *_args, **_kwargs: (tmp_path / "image.tar", {"image_id": expected_id}),
    )
    monkeypatch.setattr(
        s04_gb10_prep,
        "_ssh",
        lambda *_args, **_kwargs: SubprocessResult(
            argv=("ssh",),
            returncode=0,
            stdout=json.dumps(
                {
                    "baseline_ready": True,
                    "legacy_absent": True,
                    "service_timer_exact": True,
                }
            ),
            stderr="",
        ),
    )

    def exact(_host, _plan, *, expected_image_id=None):  # type: ignore[no-untyped-def]
        captured["expected_image_id"] = expected_image_id
        return expected_image_id == expected_id

    monkeypatch.setattr(s04_gb10_prep, "_external_worker_image_exact", exact)

    outcome = s04_gb10_prep._verify_external_user_authority(
        SimpleNamespace(
            resolved_sha="b" * 40,
            resolved_tree="c" * 40,
        ),
        host,
        host_dir=tmp_path / "12-gb10-prep" / "host-trt-gb10-1",
    )

    assert outcome is VerifyOutcome.MATCH
    assert captured["expected_image_id"] == expected_id
