from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
IMAGE = "loom-pipeline-core-fixture:pytest"
BASE_IMAGE = (
    "python:3.12.13-slim-bookworm@sha256:"
    "4766d8b510c428e595d74b9cc5bbb2fae8e26316fffb4adc89908d79aacd58a2"
)
pytestmark = pytest.mark.docker


def _docker(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["docker", *args],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=check,
    )


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )


def _mkdir_container_output(path: Path) -> None:
    path.mkdir()
    # The fixture intentionally runs as uid 65532.  pytest's process umask can
    # otherwise silently narrow mkdir(0o777) to a host-only 0o755 bind mount.
    path.chmod(0o777)


def _locked_stage(image: str, stage: str, *, inputs: Path, outputs: Path) -> dict[str, str]:
    result = _docker(
        "run",
        "--rm",
        "--network",
        "none",
        "--read-only",
        "--cap-drop",
        "ALL",
        "--security-opt",
        "no-new-privileges",
        "--mount",
        f"type=bind,source={inputs},target=/inputs,readonly",
        "--mount",
        f"type=bind,source={outputs},target=/outputs",
        image,
        stage,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


@pytest.fixture(scope="module")
def fixture_image() -> str:
    version = _docker("version", "--format", "{{.Server.Version}}", check=False)
    if version.returncode != 0:
        pytest.skip("Docker daemon is unavailable")
    build = _docker(
        "build",
        "--file",
        "deploy/Dockerfile.pipeline-core-fixture",
        "--tag",
        IMAGE,
        ".",
        check=False,
    )
    assert build.returncode == 0, build.stderr
    try:
        yield IMAGE
    finally:
        _docker("image", "rm", IMAGE, check=False)


def test_fixture_base_pin_is_a_two_native_platform_manifest() -> None:
    inspected = _docker("buildx", "imagetools", "inspect", "--raw", BASE_IMAGE, check=False)
    assert inspected.returncode == 0, inspected.stderr
    manifest = json.loads(inspected.stdout)
    platforms = {
        (item["platform"]["os"], item["platform"]["architecture"])
        for item in manifest["manifests"]
        if item.get("platform", {}).get("os") == "linux"
    }
    assert {("linux", "amd64"), ("linux", "arm64")} <= platforms


def test_fixture_image_declares_non_root_default_user(fixture_image: str) -> None:
    inspected = _docker("image", "inspect", fixture_image, "--format", "{{json .Config.User}}")
    assert json.loads(inspected.stdout) == "65532:65532"


def test_fixture_image_runs_locked_self_check_without_privilege_or_network(
    fixture_image: str,
) -> None:
    result = _docker(
        "run",
        "--rm",
        "--network",
        "none",
        "--read-only",
        "--cap-drop",
        "ALL",
        "--security-opt",
        "no-new-privileges",
        fixture_image,
        "--self-check",
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {
        "fixture": "pipeline-core-fixture@1",
        "status": "ok",
    }


def test_fixture_image_runs_seed_with_read_only_inputs_and_writable_outputs(
    fixture_image: str, tmp_path: Path
) -> None:
    inputs = tmp_path / "inputs"
    outputs = tmp_path / "outputs"
    inputs.mkdir(mode=0o755)
    _mkdir_container_output(outputs)
    assert _locked_stage(fixture_image, "seed_set", inputs=inputs, outputs=outputs) == {
        "domain_outcome": "seeded"
    }
    artifact = json.loads((outputs / "seed/artifact.json").read_bytes())
    assert artifact["schema_version"] == "loom.pipeline-core-seed.v1"


def test_fixture_image_runs_every_closed_stage_with_locked_mounts(
    fixture_image: str, tmp_path: Path
) -> None:
    seed_inputs = tmp_path / "seed-inputs"
    seed_outputs = tmp_path / "seed-outputs"
    seed_inputs.mkdir(mode=0o755)
    _mkdir_container_output(seed_outputs)
    assert _locked_stage(fixture_image, "seed_set", inputs=seed_inputs, outputs=seed_outputs) == {
        "domain_outcome": "seeded"
    }

    index_outputs = tmp_path / "index-outputs"
    _mkdir_container_output(index_outputs)
    assert _locked_stage(
        fixture_image, "produce_index", inputs=seed_outputs, outputs=index_outputs
    ) == {"domain_outcome": "indexed"}
    assert {path.parent.name for path in index_outputs.glob("*/artifact.json")} == {
        "index",
        "item-000",
        "item-001",
    }

    transform_outputs: list[Path] = []
    for item_name in ("item-000", "item-001"):
        item_outputs = tmp_path / f"{item_name}-outputs"
        _mkdir_container_output(item_outputs)
        assert _locked_stage(
            fixture_image,
            "transform",
            inputs=index_outputs / item_name,
            outputs=item_outputs,
        ) == {"domain_outcome": "transformed"}
        transform_outputs.append(item_outputs)

    aggregate_inputs = tmp_path / "aggregate-inputs"
    aggregate_outputs = tmp_path / "aggregate-outputs"
    aggregate_inputs.mkdir(mode=0o755)
    _mkdir_container_output(aggregate_outputs)
    _write_json(
        aggregate_inputs / "stage-request.json",
        {
            "schema_version": "loom.pipeline-core-aggregate-request.v1",
            "transforms": [
                {
                    "artifact_sha256": json.loads(
                        (path / "transformed/artifact.json").read_bytes()
                    )["payload"]["value_sha256"],
                    "shard_key": f"item-{ordinal:03d}",
                }
                for ordinal, path in enumerate(transform_outputs)
            ],
        },
    )
    assert _locked_stage(
        fixture_image, "aggregate", inputs=aggregate_inputs, outputs=aggregate_outputs
    ) == {"domain_outcome": "pass"}

    receipt_outputs = tmp_path / "receipt-outputs"
    _mkdir_container_output(receipt_outputs)
    assert _locked_stage(
        fixture_image,
        "local_artifact_readback",
        inputs=aggregate_outputs,
        outputs=receipt_outputs,
    ) == {"domain_outcome": "verified"}
    assert (receipt_outputs / "receipt/artifact.json").is_file()


def test_fixture_image_rejects_unknown_command_before_side_effect(
    fixture_image: str, tmp_path: Path
) -> None:
    outputs = tmp_path / "outputs"
    _mkdir_container_output(outputs)
    result = _docker(
        "run",
        "--rm",
        "--network",
        "none",
        "--read-only",
        "--cap-drop",
        "ALL",
        "--security-opt",
        "no-new-privileges",
        "--mount",
        f"type=bind,source={outputs},target=/outputs",
        fixture_image,
        "publisher",
        check=False,
    )
    assert result.returncode == 64
    assert not any(outputs.iterdir())
