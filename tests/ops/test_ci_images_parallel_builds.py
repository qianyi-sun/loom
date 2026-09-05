from __future__ import annotations

import json
import os
import re
import subprocess
from pathlib import Path
from typing import Any

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]


def _condition(expression: str, values: dict[str, str], *, cancelled: bool = False) -> bool:
    """Evaluate the Boolean-only job guards, including GitHub's implicit success guard."""
    status_function = "cancelled()" in expression or "always()" in expression
    if not status_function and any(
        value != "success" for key, value in values.items() if key.endswith(".result")
    ):
        return False
    expression = re.sub(
        r"(?:github|needs)\.[A-Za-z0-9_.-]+", lambda match: repr(values[match[0]]), expression
    )
    expression = expression.replace("cancelled()", str(cancelled)).replace("always()", "True")
    expression = expression.replace("&&", " and ").replace("||", " or ")
    expression = re.sub(r"!(?!=)", " not ", expression).strip()
    return bool(eval(f"({expression})", {"__builtins__": {}, "contains": lambda a, b: b in a}))


def _jobs() -> dict[str, Any]:
    return yaml.safe_load((ROOT / ".github/workflows/images.yml").read_text())["jobs"]


def test_ordinary_images_start_without_scanner_assets() -> None:
    jobs = _jobs()
    assert "personal-dev-scanner-cache-assets" not in jobs["build"]["needs"]
    assert "personal-dev-scanner-cache-assets" in jobs["scanner-cache-build"]["needs"]
    assert "scanner-cache-build" in jobs["images-gate"]["needs"]


@pytest.mark.parametrize(
    "images",
    [[], ["service"], ["personal-dev-scanner-cache"], ["service", "personal-dev-scanner-cache"]],
)
def test_matrix_partition_preserves_each_selected_native_build(
    tmp_path: Path, images: list[str]
) -> None:
    native = [
        {"image": image, "architecture": arch, "platform": f"linux/{arch}"}
        for image in images
        for arch in ("amd64", "arm64")
    ]
    step = next(step for step in _jobs()["plan"]["steps"] if step.get("id") == "build-matrices")
    output = tmp_path / "output"
    result = subprocess.run(
        ["bash"],
        input=step["run"],
        text=True,
        capture_output=True,
        env={**os.environ, "NATIVE_BUILDS": json.dumps(native), "GITHUB_OUTPUT": str(output)},
        check=False,
    )
    assert result.returncode == 0, result.stderr
    partition = dict(line.split("=", 1) for line in output.read_text().splitlines())
    ordinary = json.loads(partition["ordinary_builds"])
    scanner = json.loads(partition["scanner_cache_builds"])
    assert ordinary == [row for row in native if row["image"] != "personal-dev-scanner-cache"]
    assert scanner == [row for row in native if row["image"] == "personal-dev-scanner-cache"]
    assert len(ordinary) + len(scanner) == len(native)


@pytest.mark.parametrize("event", ["pull_request", "merge_group", "push", "workflow_dispatch"])
def test_docs_plan_emits_empty_native_matrix_before_partition(tmp_path: Path, event: str) -> None:
    """Exercise the real checked-out image planner when it selects no image work."""
    steps = _jobs()["plan"]["steps"]
    select = next(step for step in steps if step.get("id") == "plan")
    partition = next(step for step in steps if step.get("id") == "build-matrices")
    changed = tmp_path / "changed"
    changed.write_text("docs/architecture/personal-dev-scanner-cache-preparation.md\n")
    output = tmp_path / "output"
    env = {
        **os.environ,
        "EVENT_NAME": event,
        "REQUIRED": "false",
        "UNOWNED_RUNTIME": "false",
        "TRUSTED_PUBLISH": "true" if event == "workflow_dispatch" else "false",
        "CHANGED_FILES": str(changed),
        "GITHUB_OUTPUT": str(output),
    }
    planned = subprocess.run(
        ["bash"],
        input=select["run"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        env=env,
        check=False,
    )
    assert planned.returncode == 0, planned.stderr
    values = dict(line.split("=", 1) for line in output.read_text().splitlines())
    assert values["required"] == "false"
    assert values["images"] == values["native_builds"] == "[]"
    split = subprocess.run(
        ["bash"],
        input=partition["run"],
        text=True,
        capture_output=True,
        env={**env, "NATIVE_BUILDS": values["native_builds"]},
        check=False,
    )
    assert split.returncode == 0, split.stderr
    values = dict(line.split("=", 1) for line in output.read_text().splitlines())
    assert values["ordinary_builds"] == values["scanner_cache_builds"] == "[]"


@pytest.mark.parametrize(
    "images",
    [[], ["service"], ["personal-dev-scanner-cache"], ["service", "personal-dev-scanner-cache"]],
)
@pytest.mark.parametrize("event", ["pull_request", "merge_group", "workflow_dispatch", "push"])
@pytest.mark.parametrize("fault", [None, "ordinary", "scanner", "publish"])
def test_gate_enforces_both_selected_build_results(
    images: list[str], event: str, fault: str | None
) -> None:
    ordinary_selected = any(image != "personal-dev-scanner-cache" for image in images)
    scanner_selected = "personal-dev-scanner-cache" in images
    publish = event == "push"
    ordinary = "success" if ordinary_selected and not publish else "skipped"
    scanner = "success" if scanner_selected and not publish else "skipped"
    publish_result = "success" if images and publish else "skipped"
    env = {
        **os.environ,
        "EVENT_NAME": event,
        "TRUSTED_PUBLISH": "false",
        "PLAN_RESULT": "success",
        "GATE_MODE": "full",
        "REQUIRED": "true" if images else "false",
        "STANDARD_IMAGES": json.dumps([{"image": image} for image in images]),
        "BUILD_RESULT": "failure" if fault == "ordinary" else ordinary,
        "SCANNER_BUILD_RESULT": "failure" if fault == "scanner" else scanner,
        "PUBLISH_RESULT": "failure" if fault == "publish" else publish_result,
        "MANIFEST_RESULT": publish_result,
        "PERSONAL_DEV_RELEASE_RESULT": "skipped",
    }
    step = _jobs()["images-gate"]["steps"][0]
    result = subprocess.run(
        ["bash"], input=step["run"], text=True, capture_output=True, env=env, check=False
    )
    assert (result.returncode == 0) == (fault is None), result.stderr


@pytest.mark.parametrize("job", ["build", "scanner-cache-build"])
def test_parallel_builds_keep_native_scan_and_untrusted_permissions(job: str) -> None:
    jobs = _jobs()
    build = jobs[job]
    assert build["permissions"] == {"contents": "read"}
    assert build["strategy"]["fail-fast"] is False
    assert "github.event_name != 'push'" in build["if"]
    assert "needs.plan.outputs.trusted_publish != 'true'" in build["if"]
    assert build["steps"] == jobs["build"]["steps"]
    scripts = "\n".join(step.get("run", "") for step in build["steps"])
    assert "scripts/validate_trivy_release_report.py" in scripts
    assert 'test "$(verify_scanner_cache_assets)" = "$scanner_cache_before"' in scripts
    assert "--cache-to" not in scripts
    assert "--push" not in scripts


@pytest.mark.parametrize("result_name", ["BUILD_RESULT", "SCANNER_BUILD_RESULT"])
@pytest.mark.parametrize("result", ["failure", "cancelled", "skipped"])
def test_gate_rejects_missing_selected_matrix_after_dependency_failure(
    result_name: str, result: str
) -> None:
    env = {
        **os.environ,
        "EVENT_NAME": "pull_request",
        "TRUSTED_PUBLISH": "false",
        "PLAN_RESULT": "success",
        "GATE_MODE": "full",
        "REQUIRED": "true",
        "STANDARD_IMAGES": '[{"image":"service"},{"image":"personal-dev-scanner-cache"}]',
        "BUILD_RESULT": "success",
        "SCANNER_BUILD_RESULT": "success",
        "PUBLISH_RESULT": "skipped",
        "MANIFEST_RESULT": "skipped",
        "PERSONAL_DEV_RELEASE_RESULT": "skipped",
        result_name: result,
    }
    completed = subprocess.run(
        ["bash"],
        input=_jobs()["images-gate"]["steps"][0]["run"],
        text=True,
        capture_output=True,
        env=env,
        check=False,
    )
    assert completed.returncode != 0


@pytest.mark.parametrize("scanner_selected", [False, True])
@pytest.mark.parametrize("asset_result", ["success", "skipped", "failure", "cancelled"])
@pytest.mark.parametrize("prerequisite", [None, "plan", "trivy-binary"])
def test_trusted_publish_requires_successful_selected_dependencies(
    scanner_selected: bool, asset_result: str, prerequisite: str | None
) -> None:
    images = ["service", "personal-dev-scanner-cache"] if scanner_selected else ["service"]
    values = {
        "github.event_name": "push",
        "github.ref": "refs/heads/dev",
        "needs.plan.result": "success",
        "needs.trivy-binary.result": "success",
        "needs.personal-dev-scanner-cache-assets.result": asset_result,
        "needs.plan.outputs.trusted_publish": "false",
        "needs.plan.outputs.gate_mode": "full",
        "needs.plan.outputs.required": "true",
        "needs.plan.outputs.images": json.dumps(
            [{"image": image} for image in images], separators=(",", ":")
        ),
    }
    expression = _jobs()["publish"]["if"]
    expected = asset_result == "success" or (asset_result == "skipped" and not scanner_selected)
    for result in ("failure", "skipped", "cancelled"):
        if prerequisite:
            values[f"needs.{prerequisite}.result"] = result
        assert _condition(expression, values) == (expected and prerequisite is None)
        assert not _condition(expression, values, cancelled=True)


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("github.event_name", "pull_request"),
        ("github.event_name", "merge_group"),
        ("github.event_name", "workflow_dispatch"),
        ("github.ref", "refs/heads/feature"),
        ("needs.plan.outputs.gate_mode", "filtered"),
        ("needs.plan.outputs.required", "false"),
        ("needs.plan.outputs.images", "[]"),
    ],
)
def test_optional_assets_do_not_bypass_existing_publish_authority(key: str, value: str) -> None:
    values = {
        "github.event_name": "push",
        "github.ref": "refs/heads/dev",
        "needs.plan.result": "success",
        "needs.trivy-binary.result": "success",
        "needs.personal-dev-scanner-cache-assets.result": "skipped",
        "needs.plan.outputs.trusted_publish": "false",
        "needs.plan.outputs.gate_mode": "full",
        "needs.plan.outputs.required": "true",
        "needs.plan.outputs.images": '[{"image":"service"}]',
        key: value,
    }
    assert not _condition(_jobs()["publish"]["if"], values)


@pytest.mark.parametrize("event", ["pull_request", "push", "workflow_dispatch", "merge_group"])
def test_asset_preparation_is_absent_when_scanner_image_is_not_selected(event: str) -> None:
    values = {
        "github.event_name": event,
        "needs.plan.result": "success",
        "needs.trivy-binary.result": "success",
        "needs.plan.outputs.gate_mode": "full",
        "needs.plan.outputs.required": "true",
        "needs.plan.outputs.images": '[{"image":"service"}]',
    }
    assert not _condition(_jobs()["personal-dev-scanner-cache-assets"]["if"], values)


@pytest.mark.parametrize("job", ["build", "scanner-cache-build"])
@pytest.mark.parametrize("failed_dependency", ["plan", "trivy-binary", "specific"])
def test_untrusted_builds_do_not_run_after_required_dependency_failure(
    job: str, failed_dependency: str
) -> None:
    selected = _jobs()[job]
    values = {f"needs.{dependency}.result": "success" for dependency in selected["needs"]}
    values.update(
        {
            "github.event_name": "pull_request",
            "needs.plan.outputs.trusted_publish": "false",
            "needs.plan.outputs.gate_mode": "full",
            "needs.plan.outputs.required": "true",
            "needs.plan.outputs.ordinary_builds": '[{"image":"service"}]',
            "needs.plan.outputs.scanner_cache_builds": '[{"image":"personal-dev-scanner-cache"}]',
        }
    )
    dependency = failed_dependency
    if dependency == "specific":
        dependency = "image-route" if job == "build" else "personal-dev-scanner-cache-assets"
    assert _condition(selected["if"], values)
    for result in ("failure", "skipped", "cancelled"):
        values[f"needs.{dependency}.result"] = result
        assert not _condition(selected["if"], values)


@pytest.mark.parametrize("publish_result", ["success", "failure", "cancelled", "skipped"])
def test_manifest_requires_publish_success_when_optional_assets_are_skipped(
    publish_result: str,
) -> None:
    values = {
        "github.event_name": "workflow_dispatch",
        "github.ref": "refs/heads/main",
        "needs.plan.result": "success",
        "needs.publish.result": publish_result,
        "needs.plan.outputs.trusted_publish": "true",
        "needs.plan.outputs.gate_mode": "full",
        "needs.plan.outputs.required": "true",
        "needs.plan.outputs.images": '[{"image":"service"}]',
        # Include the indirect skipped dependency to protect against implicit status propagation.
        "needs.personal-dev-scanner-cache-assets.result": "skipped",
    }
    condition = _jobs()["publish-manifest"]["if"]
    assert _condition(condition, values) == (publish_result == "success")
    assert not _condition(condition, values, cancelled=True)
