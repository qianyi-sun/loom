"""CI placement and unique cross-language test ownership after Nebius cutover."""

import json
import os
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize("name", ["ci", "images", "cluster-smoke", "staging-smoke"])
def test_ci_has_no_shared_host_placement_or_local_cache_dependency(name: str) -> None:
    source = (ROOT / f".github/workflows/{name}.yml").read_text()
    assert "oldlab" not in source.lower()
    assert "ci-runner-route" not in source
    workflow = yaml.safe_load(source)
    for job in workflow["jobs"].values():
        placement = str(job["runs-on"])
        assert "needs." not in placement
        assert "self-hosted" not in placement
        for step in job.get("steps", []):
            if str(step.get("uses", "")).startswith("astral-sh/setup-uv@"):
                assert "manifest-file" not in step.get("with", {})


@pytest.mark.parametrize("publishing", [False, True])
def test_pr_matrix_runs_amd64_and_preserves_declared_publication_contract(
    tmp_path: Path, publishing: bool,
) -> None:
    workflow = yaml.safe_load((ROOT / ".github/workflows/images.yml").read_text())
    step = next(item for item in workflow["jobs"]["plan"]["steps"]
                if item.get("id") == "build-matrices")
    rows = [
        {"image": image, "architecture": arch, "platform": f"linux/{arch}"}
        for image in ("service", "control-plane")
        for arch in ("amd64", "arm64")
    ]
    output = tmp_path / "output"
    result = subprocess.run(
        ["bash", "-c", step["run"]], text=True, capture_output=True,
        env={**os.environ, "PUBLISHING": str(publishing).lower(),
             "NATIVE_BUILDS": json.dumps(rows), "GITHUB_OUTPUT": str(output)},
    )
    assert result.returncode == 0, result.stderr
    matrices = dict(line.split("=", 1) for line in output.read_text().splitlines())
    selected = json.loads(matrices["ordinary_builds"])
    assert selected == [row for row in rows if publishing or row["architecture"] == "amd64"]


def test_linux_runtime_dependency_check_is_amd64_only() -> None:
    workflow = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())
    rows = workflow["jobs"]["locked-environments"]["strategy"]["matrix"]["include"]
    assert [row["target"] for row in rows] == ["linux-x86_64"]
