from __future__ import annotations

import argparse
import json
from collections.abc import Collection, Sequence
from dataclasses import asdict, dataclass
from functools import lru_cache
from pathlib import Path

if __package__:
    from scripts.component_ownership import Manifest, load_manifest
else:
    from component_ownership import Manifest, load_manifest

HEAVY_CHECKS = (
    "integration",
    "integration_docker",
    "images",
    "cluster_smoke",
    "staging_smoke",
)

SUPPORTED_EVENTS = {"merge_group", "pull_request", "push", "workflow_dispatch"}

LABEL_TO_CHECK = {
    "ci:integration": "integration",
    "ci:integration-docker": "integration_docker",
    "ci:images": "images",
    "cluster-smoke": "cluster_smoke",
    "staging-smoke": "staging_smoke",
    "ci:coverage-summary": "coverage_summary",
}

DOC_METADATA_PATHS = {
    ".github/PULL_REQUEST_TEMPLATE.md",
    ".editorconfig",
    ".gitignore",
    "CONTRIBUTING.md",
    "LICENSE",
    "README.md",
    "SECURITY.md",
}

DOCS_STATIC_SUFFIXES = {
    ".csv",
    ".gif",
    ".jpeg",
    ".jpg",
    ".json",
    ".jsonl",
    ".md",
    ".mdx",
    ".png",
    ".rst",
    ".svg",
    ".txt",
}

PLANNER_PATHS = {
    "scripts/plan_ci_validations.py",
    "tests/ops/test_plan_ci_validations.py",
}

OWNERSHIP_AUTHORITY_PATHS = {
    "config/component-ownership.toml",
    "scripts/component_ownership.py",
    "tests/ops/test_component_ownership_manifest.py",
}

NEBIUS_IAC_EXACT = {
    "scripts/check_nebius_iac.py",
    "tests/ops/test_nebius_iac.py",
}

NEBIUS_IAC_PREFIXES = ("deploy/terraform/nebius/",)

PROTECTED_STAGING_ROLLOUT_EXACT = {
    "deploy/environments/staging.cluster.toml",
    "deploy/environment-state/staging.toml",
    "deploy/worker-pools/gb10/known_hosts",
    "deploy/worker-pools/gb10/loom-staging-rollout-platform-dev.exports",
    "deploy/worker-pools/gb10/loom-staging-rollout-shared-work2-export-authority.sudoers",
    "deploy/worker-pools/gb10/ssh_config",
    "scripts/ops/verify_staging_rollout_secret_boundary.py",
    "src/loom_cli/rollout/steps/s04_gb10_prep.py",
    "src/loom_cli/rollout/steps/s10_env_state.py",
    "tests/loom_cli/rollout/steps/test_env_state_external_prereqs.py",
    "tests/loom_cli/test_cluster_render.py",
    "tests/loom_cli/test_environment_state.py",
}

PROTECTED_STAGING_ROLLOUT_PREFIXES = (
    "deploy/staging-rollout/",
    "scripts/ops/staging_rollout_",
    "src/loom_cli/rollout/operator/",
    "tests/loom_cli/rollout/operator/",
    "tests/ops/test_staging_rollout_",
)

PROTECTED_NATIVE_AUTHORITY_EXACT = {
    "deploy/worker-pools/gb10/README.md",
    "docs/architecture/2026-08-31-personal-dev-native-runtime-authority-design.md",
    "docs/architecture/2026-09-01-personal-dev-native-operator-material-authority-design.md",
    "docs/implementation-plans/2026-09-01-personal-dev-native-operator-material-authority.md",
    "docs/runbooks/personal-dev-native-builder-acceptance.md",
    "docs/runbooks/personal-dev-native-builder-runtime.md",
    "scripts/ops/converge_personal_dev_native_builder_release.py",
    "scripts/ops/install_personal_dev_native_builder_runtime.py",
    "scripts/ops/install_personal_dev_native_builder_runtime_authority.py",
    "scripts/ops/personal_dev_native_builder_conformance.py",
    "scripts/ops/personal_dev_native_builder_runtime_crypto.py",
    "scripts/ops/personal_dev_native_builder_runtime_authority.py",
    "scripts/ops/personal_dev_native_builder_runtime_authority_client.py",
    "scripts/ops/personal_dev_native_builder_runtime_authority_launcher.py",
    "scripts/ops/personal_dev_native_builder_runtime_authority_material_client.py",
    "scripts/ops/personal_dev_native_builder_runtime_authority_protocol.py",
    "scripts/ops/personal_dev_native_builder_runtime_profile.py",
    "tests/ops/test_converge_personal_dev_native_builder_release.py",
    "tests/ops/test_install_personal_dev_native_builder_runtime.py",
    "tests/ops/test_install_personal_dev_native_builder_runtime_authority.py",
    "tests/ops/test_personal_dev_native_builder_conformance.py",
    "tests/ops/test_personal_dev_native_builder_runbooks.py",
    "tests/ops/test_personal_dev_native_builder_runtime_authority.py",
    "tests/ops/test_personal_dev_native_builder_runtime_authority_protocol.py",
    "tests/ops/test_personal_dev_native_builder_runtime_profile.py",
    # Retired path tombstones keep removal diffs inside the protected lane.
    "scripts/ops/personal_dev_native_builder_conformance.sh",
    "scripts/ops/personal_dev_native_runtime_authority.py",
    "tests/ops/test_personal_dev_native_runtime_authority.py",
}

PROTECTED_NATIVE_AUTHORITY_PREFIXES = ("deploy/personal-dev-native-builder/",)

REPO_ROOT = Path(__file__).resolve().parents[1]
COMPONENT_OWNERSHIP_MANIFEST = REPO_ROOT / "config/component-ownership.toml"


@dataclass(frozen=True)
class ValidationPlan:
    event_relevant: bool
    full_gate: bool
    gate_mode: str
    docs_only: bool
    unowned_runtime: bool
    integration: bool
    integration_docker: bool
    images: bool
    cluster_smoke: bool
    staging_smoke: bool
    coverage_summary: bool
    web_checks: bool
    reasons: dict[str, tuple[str, ...]]

    def selected_heavy_checks(self) -> set[str]:
        return {name for name in HEAVY_CHECKS if getattr(self, name)}

    def github_outputs(self) -> dict[str, str]:
        outputs = {
            name: str(bool(getattr(self, name))).lower()
            for name in (
                "event_relevant",
                "full_gate",
                "docs_only",
                "unowned_runtime",
                *HEAVY_CHECKS,
                "coverage_summary",
                "web_checks",
            )
        }
        outputs["gate_mode"] = self.gate_mode
        outputs["reasons_json"] = json.dumps(self.reasons, sort_keys=True, separators=(",", ":"))
        return outputs


def _pull_request_gate_mode(
    *,
    draft: bool,
) -> tuple[bool, bool, str]:
    """Return event relevance, full-gate eligibility, and the gate context mode.

    Every non-draft PR event emits the four protected contexts. Drafts are
    filtered. Validation labels remain additive selectors, but no label,
    author, reviewer, or coordinator grants merge authority.
    """

    if draft:
        return False, False, "filtered"
    return True, True, "full"


def _is_documentation_path(path: str) -> bool:
    return (
        path in DOC_METADATA_PATHS
        or path.startswith(".github/ISSUE_TEMPLATE/")
        or (path.startswith("docs/") and Path(path).suffix in DOCS_STATIC_SUFFIXES)
    )


def _matches(path: str, *, exact: set[str], prefixes: tuple[str, ...]) -> bool:
    return path in exact or path.startswith(prefixes)


def _is_dependency_authority_path(path: str) -> bool:
    return path in {"config/uv-toolchain.toml", "pyproject.toml", "uv.lock"} or (
        path.startswith("packages/") and path.endswith("/pyproject.toml")
    )


def _is_protected_staging_rollout_path(path: str) -> bool:
    return _matches(
        path,
        exact=PROTECTED_STAGING_ROLLOUT_EXACT,
        prefixes=PROTECTED_STAGING_ROLLOUT_PREFIXES,
    )


def _is_protected_native_authority_path(path: str) -> bool:
    return _matches(
        path,
        exact=PROTECTED_NATIVE_AUTHORITY_EXACT,
        prefixes=PROTECTED_NATIVE_AUTHORITY_PREFIXES,
    )


@lru_cache(maxsize=512)
def _test_owner_lanes(path: str) -> tuple[str, ...]:
    return tuple(owner.lane for owner in _component_ownership_manifest().test_owners_for_path(path))


@lru_cache(maxsize=1)
def _component_ownership_manifest() -> Manifest:
    return load_manifest(COMPONENT_OWNERSHIP_MANIFEST)


def plan_validations(
    *,
    changed_paths: Sequence[str],
    labels: Collection[str],
    event_name: str,
    pull_request_action: str = "opened",
    pull_request_action_label: str = "",
    pull_request_draft: bool = False,
    pull_request_base_changed: bool = False,
) -> ValidationPlan:
    if event_name not in SUPPORTED_EVENTS:
        raise ValueError(f"unsupported event_name: {event_name}")

    event_relevant = True
    full_gate = True
    gate_mode = "full"
    if event_name == "pull_request":
        event_relevant, full_gate, gate_mode = _pull_request_gate_mode(
            draft=pull_request_draft,
        )

    paths = tuple(dict.fromkeys(path.strip() for path in changed_paths if path.strip()))
    docs_only = bool(paths) and all(_is_documentation_path(path) for path in paths)
    unowned_runtime = False
    selected = {name: False for name in (*HEAVY_CHECKS, "coverage_summary", "web_checks")}
    reasons: dict[str, list[str]] = {name: [] for name in selected}

    def select(name: str, reason: str) -> None:
        selected[name] = True
        reasons[name].append(reason)

    if event_name == "merge_group":
        for name in HEAVY_CHECKS:
            select(name, "merge_group")
        select("web_checks", "merge_group")

    for label in sorted(labels):
        if check := LABEL_TO_CHECK.get(label):
            select(check, f"label:{label}")
        if event_name == "pull_request" and label == "ci:integration":
            select("coverage_summary", f"label:{label}")

    if any(path in PLANNER_PATHS for path in paths):
        for name in HEAVY_CHECKS:
            select(name, "planner-change")

    if any(path in OWNERSHIP_AUTHORITY_PATHS for path in paths):
        for name in HEAVY_CHECKS:
            select(name, "ownership-authority-change")

    if any(_is_protected_staging_rollout_path(path) for path in paths):
        for name in HEAVY_CHECKS:
            select(name, "protected-staging-rollout")

    if any(_is_protected_native_authority_path(path) for path in paths):
        for name in HEAVY_CHECKS:
            select(name, "protected-native-authority")

    integration_exact = {
        ".github/workflows/ci.yml",
        "config/loom-schema.toml",
    }
    integration_prefixes = (
        "capacity_guard_migrations/",
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
        "tests/integration/",
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
        "web/tailwind.config.js",
        "web/vite.config.ts",
        "deploy/nginx-spa.conf",
        "deploy/nginx-spa-security-headers.conf",
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
        ".github/workflows/release-promotion-gate.yml",
        "scripts/ops/deploy_staging_k3s.sh",
        "src/loom_cli/cluster_cmd.py",
        "src/loom_cli/cluster_config.py",
        "web/src/__tests__/AuthContext.test.tsx",
        "web/src/auth/AuthContext.tsx",
        "config/loom-schema.toml",
    }
    cluster_prefixes = (
        "src/loom_cli/templates/k8s/",
        "deploy/k8s/",
        "deploy/environments/",
        "deploy/staging-k3s/",
    )
    staging_exact = {
        ".github/workflows/staging-smoke.yml",
        ".github/workflows/release-promotion-gate.yml",
        "pyproject.toml",
        "config/loom-schema.toml",
        "src/loom/driver/docker.py",
        "deploy/Dockerfile.web",
        "deploy/nginx-spa.conf",
        "deploy/nginx-spa-security-headers.conf",
        "deploy/web-runtime-config.sh",
        "scripts/ops/frontend_security_headers.py",
        "scripts/ops/frontend_route_smoke.py",
        "scripts/ops/deploy_staging_k3s.sh",
        "src/loom_cli/templates/k8s/ingress.yaml.j2",
        "tests/ops/test_frontend_security_headers.py",
        "web/package-lock.json",
        "web/package.json",
        "web/scripts/frontend-route-browser-smoke.mjs",
        "web/scripts/frontend-route-browser-smoke.test.mjs",
        "web/src/__tests__/AuthContext.test.tsx",
        "web/src/auth/AuthContext.tsx",
        "web/src/main.tsx",
    }
    staging_prefixes = (
        "deploy/Dockerfile.",
        "deploy/environments/",
        "deploy/staging-k3s/",
        "src/loom_service/",
        "src/loom_control_plane/",
        "src/loom_llm_gateway/",
        "src/loom_worker/",
        "src/loom_family_orchestrator/",
        "src/loom_cli/templates/k8s/",
        "migrations/",
        "packages/",
        "web/",
    )
    web_quality_exact = {
        ".github/workflows/ci.yml",
        "config/component-ownership.toml",
        "deploy/Dockerfile.web",
        "deploy/nginx-spa.conf",
        "deploy/nginx-spa-security-headers.conf",
        "deploy/web-runtime-config.sh",
        "scripts/component_ownership.py",
    }

    for path in paths:
        if _is_documentation_path(path):
            continue
        test_owner_lanes = _test_owner_lanes(path)
        matched_owner = (
            path in PLANNER_PATHS
            or path in OWNERSHIP_AUTHORITY_PATHS
            or _matches(path, exact=NEBIUS_IAC_EXACT, prefixes=NEBIUS_IAC_PREFIXES)
            or _is_protected_staging_rollout_path(path)
            or _is_protected_native_authority_path(path)
            or bool(test_owner_lanes)
        )
        for lane in test_owner_lanes:
            reason = f"test-owner:{lane}:{path}"
            if lane == "integration":
                select("integration", reason)
            elif lane == "integration-docker":
                # Retain the existing tests/integration contract: changes to a
                # Docker-owned module exercise both integration tiers.
                select("integration", reason)
                select("integration_docker", reason)
            elif lane == "cluster-smoke":
                select("cluster_smoke", reason)
            elif lane == "system-smoke":
                select("staging_smoke", reason)
        if _is_dependency_authority_path(path):
            for name in HEAVY_CHECKS:
                select(name, f"dependency-authority:{path}")
            matched_owner = True
        if _matches(path, exact=integration_exact, prefixes=integration_prefixes):
            select("integration", f"path:{path}")
            matched_owner = True
        elif not test_owner_lanes:
            select("integration", f"non-doc-path:{path}")
        if _matches(path, exact=docker_exact, prefixes=docker_prefixes):
            select("integration_docker", f"path:{path}")
            matched_owner = True
        image_match = _matches(
            path,
            exact=image_exact,
            prefixes=image_prefixes,
        ) and not path.startswith("src/loom_cli/templates/k8s/")
        if image_match:
            select("images", f"path:{path}")
            matched_owner = True
        if _matches(path, exact=cluster_exact, prefixes=cluster_prefixes):
            select("cluster_smoke", f"path:{path}")
            matched_owner = True
        if _matches(path, exact=staging_exact, prefixes=staging_prefixes):
            select("staging_smoke", f"path:{path}")
            matched_owner = True
        if path.startswith("web/") or path in web_quality_exact:
            select("web_checks", f"path:{path}")
            matched_owner = True
        if not matched_owner:
            unowned_runtime = True
            reason = f"unowned-runtime-path:{path}"
            for name in HEAVY_CHECKS:
                select(name, reason)

    if selected["coverage_summary"]:
        select("integration", "coverage-summary-requires-integration")

    if any(selected[name] for name in ("integration", "integration_docker", "coverage_summary")):
        docs_only = False

    return ValidationPlan(
        event_relevant=event_relevant,
        full_gate=full_gate,
        gate_mode=gate_mode,
        docs_only=docs_only,
        unowned_runtime=unowned_runtime,
        integration=selected["integration"],
        integration_docker=selected["integration_docker"],
        images=selected["images"],
        cluster_smoke=selected["cluster_smoke"],
        staging_smoke=selected["staging_smoke"],
        coverage_summary=selected["coverage_summary"],
        web_checks=selected["web_checks"],
        reasons={name: tuple(values) for name, values in reasons.items()},
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--changed-files", type=Path, required=True)
    parser.add_argument("--labels-json", default="[]")
    parser.add_argument("--event-name", required=True)
    parser.add_argument("--pull-request-action", default="")
    parser.add_argument("--pull-request-action-label", default="")
    parser.add_argument(
        "--pull-request-draft",
        choices=("true", "false"),
        default="false",
    )
    parser.add_argument(
        "--pull-request-base-changed",
        choices=("true", "false"),
        default="false",
    )
    parser.add_argument("--github-output", type=Path, required=True)
    args = parser.parse_args()
    labels = json.loads(args.labels_json or "[]")
    if not isinstance(labels, list) or not all(isinstance(label, str) for label in labels):
        raise SystemExit("--labels-json must be a JSON array of strings")
    plan = plan_validations(
        changed_paths=args.changed_files.read_text(encoding="utf-8").splitlines(),
        labels=set(labels),
        event_name=args.event_name,
        pull_request_action=args.pull_request_action,
        pull_request_action_label=args.pull_request_action_label,
        pull_request_draft=args.pull_request_draft == "true",
        pull_request_base_changed=args.pull_request_base_changed == "true",
    )
    with args.github_output.open("a", encoding="utf-8") as handle:
        for name, value in plan.github_outputs().items():
            handle.write(f"{name}={value}\n")
    print(json.dumps(asdict(plan), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
