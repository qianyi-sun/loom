from __future__ import annotations

import argparse
import json
from collections.abc import Collection, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path

HEAVY_CHECKS = (
    "integration",
    "integration_docker",
    "images",
    "cluster_smoke",
    "staging_smoke",
)

LABEL_TO_CHECK = {
    "ci:integration": "integration",
    "ci:integration-docker": "integration_docker",
    "ci:images": "images",
    "cluster-smoke": "cluster_smoke",
    "staging-smoke": "staging_smoke",
    "ci:coverage-summary": "coverage_summary",
}

DOC_METADATA_PATHS = {
    ".github/CODEOWNERS",
    ".github/PULL_REQUEST_TEMPLATE.md",
    ".gitignore",
    ".editorconfig",
    "LICENSE",
}

PLANNER_PATHS = {
    "scripts/plan_ci_validations.py",
    "tests/ops/test_plan_ci_validations.py",
}


@dataclass(frozen=True)
class ValidationPlan:
    docs_only: bool
    integration: bool
    integration_docker: bool
    images: bool
    cluster_smoke: bool
    staging_smoke: bool
    coverage_summary: bool
    reasons: dict[str, tuple[str, ...]]

    def selected_heavy_checks(self) -> set[str]:
        return {name for name in HEAVY_CHECKS if getattr(self, name)}

    def github_outputs(self) -> dict[str, str]:
        outputs = {
            name: str(bool(getattr(self, name))).lower()
            for name in (
                "docs_only",
                *HEAVY_CHECKS,
                "coverage_summary",
            )
        }
        outputs["reasons_json"] = json.dumps(
            self.reasons, sort_keys=True, separators=(",", ":")
        )
        return outputs


def _is_documentation_path(path: str) -> bool:
    return (
        path.endswith(".md")
        or path.startswith("docs/")
        or path.startswith(".github/ISSUE_TEMPLATE/")
        or path in DOC_METADATA_PATHS
    )


def _matches(path: str, *, exact: set[str], prefixes: tuple[str, ...]) -> bool:
    return path in exact or path.startswith(prefixes)


def plan_validations(
    *,
    changed_paths: Sequence[str],
    labels: Collection[str],
    event_name: str,
) -> ValidationPlan:
    paths = tuple(dict.fromkeys(path.strip() for path in changed_paths if path.strip()))
    docs_only = bool(paths) and all(_is_documentation_path(path) for path in paths)
    selected = {name: False for name in (*HEAVY_CHECKS, "coverage_summary")}
    reasons: dict[str, list[str]] = {name: [] for name in selected}

    def select(name: str, reason: str) -> None:
        selected[name] = True
        reasons[name].append(reason)

    if event_name == "merge_group":
        for name in HEAVY_CHECKS:
            select(name, "merge_group")

    for label in sorted(labels):
        if check := LABEL_TO_CHECK.get(label):
            select(check, f"label:{label}")

    if any(path in PLANNER_PATHS for path in paths):
        for name in HEAVY_CHECKS:
            select(name, "planner-change")

    integration_exact = {
        ".github/workflows/ci.yml",
        "config/loom-schema.toml",
    }
    integration_prefixes = (
        "src/",
        "packages/",
        "migrations/",
        "config/",
        "tests/integration/",
        "tests/contract/",
    )
    docker_exact = {
        "src/loom/driver/docker.py",
        ".github/workflows/ci.yml",
    }
    docker_prefixes = (
        "src/loom_worker/",
        "src/loom/sandbox",
        "packages/loom-launcher/",
        "tests/integration/test_docker",
        "tests/integration/test_trial_e2e",
    )
    image_exact = {
        ".dockerignore",
        ".github/workflows/images.yml",
        "pyproject.toml",
        "go.mod",
        "go.sum",
        "web/index.html",
        "web/package.json",
        "web/package-lock.json",
        "web/vite.config.ts",
        "deploy/nginx-spa.conf",
        "deploy/web-runtime-config.sh",
    }
    image_prefixes = (
        "deploy/Dockerfile.",
        "src/",
        "packages/",
        "web/src/",
        "cmd/",
        "migrations/",
    )
    cluster_exact = {
        ".github/workflows/cluster-smoke.yml",
        "src/loom_cli/cluster_cmd.py",
        "src/loom_cli/cluster_config.py",
        "config/loom-schema.toml",
    }
    cluster_prefixes = (
        "src/loom_cli/templates/k8s/",
        "deploy/k8s/",
        "deploy/environments/",
    )
    staging_exact = {
        ".github/workflows/staging-smoke.yml",
        "pyproject.toml",
        "config/loom-schema.toml",
        "src/loom/driver/docker.py",
        "deploy/nginx-spa.conf",
        "deploy/web-runtime-config.sh",
    }
    staging_prefixes = (
        "deploy/Dockerfile.",
        "deploy/environments/",
        "src/loom_service/",
        "src/loom_control_plane/",
        "src/loom_llm_gateway/",
        "src/loom_worker/",
        "src/loom_family_orchestrator/",
        "src/loom_cli/templates/k8s/",
        "packages/",
        "web/",
    )

    for path in paths:
        if _is_documentation_path(path):
            continue
        if _matches(path, exact=integration_exact, prefixes=integration_prefixes):
            select("integration", f"path:{path}")
        else:
            select("integration", f"non-doc-path:{path}")
        if _matches(path, exact=docker_exact, prefixes=docker_prefixes):
            select("integration_docker", f"path:{path}")
        if _matches(
            path,
            exact=image_exact,
            prefixes=image_prefixes,
        ) and not path.startswith("src/loom_cli/templates/k8s/"):
            select("images", f"path:{path}")
        if _matches(path, exact=cluster_exact, prefixes=cluster_prefixes):
            select("cluster_smoke", f"path:{path}")
        if _matches(path, exact=staging_exact, prefixes=staging_prefixes):
            select("staging_smoke", f"path:{path}")

    if selected["coverage_summary"]:
        select("integration", "coverage-summary-requires-integration")

    return ValidationPlan(
        docs_only=docs_only,
        integration=selected["integration"],
        integration_docker=selected["integration_docker"],
        images=selected["images"],
        cluster_smoke=selected["cluster_smoke"],
        staging_smoke=selected["staging_smoke"],
        coverage_summary=selected["coverage_summary"],
        reasons={name: tuple(values) for name, values in reasons.items()},
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--changed-files", type=Path, required=True)
    parser.add_argument("--labels-json", default="[]")
    parser.add_argument("--event-name", required=True)
    parser.add_argument("--github-output", type=Path, required=True)
    args = parser.parse_args()
    labels = json.loads(args.labels_json or "[]")
    if not isinstance(labels, list) or not all(isinstance(label, str) for label in labels):
        raise SystemExit("--labels-json must be a JSON array of strings")
    plan = plan_validations(
        changed_paths=args.changed_files.read_text(encoding="utf-8").splitlines(),
        labels=set(labels),
        event_name=args.event_name,
    )
    with args.github_output.open("a", encoding="utf-8") as handle:
        for name, value in plan.github_outputs().items():
            handle.write(f"{name}={value}\n")
    print(json.dumps(asdict(plan), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
