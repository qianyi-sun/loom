from __future__ import annotations

from pathlib import Path

import yaml

PINNED_ROUTE_SHA = "0d336557f0a3570490bd21bf85485c3e4c4a2d73"
PINNED_CHECKOUT = "actions/checkout@93cb6efe18208431cddfb8368fd83d5badbf9bfd"
LOCAL_ROUTE_ACTION = "./.loom-ci-route-action/.github/actions/ci-runner-route"


def _workflow(name: str) -> dict[str, object]:
    repo_root = Path(__file__).resolve().parents[2]
    return yaml.safe_load((repo_root / f".github/workflows/{name}.yml").read_text(encoding="utf-8"))


def _route_step(job: dict[str, object]) -> dict[str, object]:
    return next(step for step in job["steps"] if step.get("id") == "route")


def test_route_planners_are_hosted_read_only_and_pin_the_merged_action() -> None:
    specs = [
        ("ci", "ci-route", "CI", "302898379"),
        ("images", "image-route", "images", "302898384"),
        ("cluster-smoke", "cluster-route", "cluster-smoke", "302898381"),
        ("staging-smoke", "staging-route", "staging-smoke", "302898388"),
    ]

    for workflow_name, job_name, expected_name, expected_id in specs:
        job = _workflow(workflow_name)["jobs"][job_name]
        step = _route_step(job)
        checkout = job["steps"][0]
        assert job["runs-on"] == "ubuntu-latest"
        assert job["permissions"] == {"checks": "read", "contents": "read"}
        assert checkout["uses"] == PINNED_CHECKOUT
        assert checkout["with"] == {
            "repository": "qianyi-sun/loom",
            "ref": PINNED_ROUTE_SHA,
            "sparse-checkout": ".github/actions/ci-runner-route",
            "path": ".loom-ci-route-action",
            "persist-credentials": False,
        }
        assert step["uses"] == LOCAL_ROUTE_ACTION
        assert step["with"]["workflow-name"] == expected_name
        assert step["with"]["workflow-id"] == expected_id
        assert step["with"]["route-mode"] == ("${{ vars.LOOM_CI_ROUTE_MODE || 'disabled' }}")
        assert step["with"]["github-token"] == "${{ github.token }}"


def test_every_normal_job_consumes_its_ci_route_dependency() -> None:
    jobs = _workflow("ci")["jobs"]
    expected = {
        "lint-and-static": "['lint-and-static']",
        "tests-root": "format('tests-root-{0}', matrix.shard)",
        "tests-packages": "['tests-packages']",
        "runtime-payload": "['runtime-payload']",
        "go-checks": "['go-checks']",
        "web-checks": "['web-checks']",
        "integration": "format('integration-{0}', matrix.shard)",
        "integration-docker": "['integration-docker']",
    }

    for job_name, route_key in expected.items():
        job = jobs[job_name]
        assert set(job["needs"]) == {"workflow-plan", "ci-route"}
        assert "needs.ci-route.outputs.routes" in job["runs-on"]
        assert route_key in job["runs-on"]
        assert "LOOM_CI_" not in job["runs-on"]


def test_image_and_smoke_jobs_consume_only_their_route_maps() -> None:
    images = _workflow("images")["jobs"]
    build = images["build"]
    assert set(build["needs"]) == {"plan", "image-route"}
    assert "ubuntu-24.04-arm" in build["runs-on"]
    assert "needs.image-route.outputs.routes" in build["runs-on"]

    smoke_specs = [
        ("cluster-smoke", "cluster-contract", "cluster-route", "cluster-contract"),
        ("staging-smoke", "system-smoke", "staging-route", "system-smoke"),
    ]
    for workflow_name, job_name, route_job, route_key in smoke_specs:
        job = _workflow(workflow_name)["jobs"][job_name]
        assert set(job["needs"]) == {"plan", route_job}
        assert f"needs.{route_job}.outputs.routes" in job["runs-on"]
        assert f"['{route_key}']" in job["runs-on"]


def test_static_repository_route_variables_are_no_longer_job_placement_inputs() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    for workflow_path in (repo_root / ".github/workflows").glob("*.yml"):
        text = workflow_path.read_text(encoding="utf-8")
        assert "LOOM_CI_NORMAL_RUNS_ON" not in text
        assert "LOOM_CI_IMAGE_RUNS_ON" not in text
        assert "LOOM_CI_SMOKE_RUNS_ON" not in text
        assert "LOOM_CI_ACCELERATOR_RUNS_ON" not in text
