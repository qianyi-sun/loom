from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping
from uuid import uuid4

from agentic_data_platform.benchmark_wrappers.smoke import (
    BenchmarkWrapperSmokeConfig,
    run_benchmark_wrapper_smoke,
)
from agentic_data_platform.benchmarks.fixtures import BenchmarkFixtureCatalog, load_fixture_catalog
from agentic_data_platform.benchmarks.upstream_sources import (
    MaterializedSkillFlowTaskAssets,
    MaterializedUpstreamSource,
    SkillFlowTaskAssetsSpec,
    UpstreamSourceSpec,
    materialize_skillflow_task_assets as default_materialize_skillflow_task_assets,
    materialize_upstream_source,
)

_DEFAULT_SKILLFLOW_DATASET_REPO_ID = "zhang-ziao/SkillFlow-Task"
_DEFAULT_SKILLFLOW_DATASET_REVISION = "ecaadb0e25d5d5cfd87bd86d81e77b4abe3a00bc"


@dataclass(frozen=True)
class RealUpstreamSmokeConfig:
    suite_name: str
    source_type: str
    source_uri: str
    source_version: str
    task_family: str
    instance_id: str
    cache_root: Path
    workspace_root: Path
    run_id: str
    timeout_seconds: int = 3600
    force_refresh: bool = False
    skillflow_dataset_repo_id: str = _DEFAULT_SKILLFLOW_DATASET_REPO_ID
    skillflow_dataset_revision: str = _DEFAULT_SKILLFLOW_DATASET_REVISION


MaterializeSource = Callable[..., MaterializedUpstreamSource]
MaterializeSkillFlowTaskAssets = Callable[..., MaterializedSkillFlowTaskAssets]
WrapperSmokeRunner = Callable[[BenchmarkWrapperSmokeConfig], dict[str, Any]]


def run_real_upstream_smoke(
    config: RealUpstreamSmokeConfig,
    *,
    materialize_source: MaterializeSource = materialize_upstream_source,
    materialize_skillflow_task_assets: MaterializeSkillFlowTaskAssets = default_materialize_skillflow_task_assets,
    wrapper_smoke_runner: WrapperSmokeRunner = run_benchmark_wrapper_smoke,
) -> dict[str, Any]:
    suite_name = _canonical_suite(config.suite_name)
    materialized = materialize_source(
        UpstreamSourceSpec(
            suite_name=suite_name,
            source_type=config.source_type,
            source_uri=config.source_uri,
            source_version=config.source_version,
        ),
        cache_root=config.cache_root,
        force_refresh=config.force_refresh,
    )
    dataset_result = None
    if suite_name == "SkillFlow":
        dataset_assets = materialize_skillflow_task_assets(
            SkillFlowTaskAssetsSpec(
                repo_id=config.skillflow_dataset_repo_id,
                revision=config.skillflow_dataset_revision,
                allow_patterns=[f"test_tasks/{config.task_family}/**"],
            ),
            local_dir=materialized.root,
            force_refresh=config.force_refresh,
        )
        dataset_result = _skillflow_task_assets_payload(dataset_assets)

    wrapper_result = wrapper_smoke_runner(
        BenchmarkWrapperSmokeConfig(
            suite_name=suite_name,
            task_family=config.task_family,
            instance_id=config.instance_id,
            workspace_root=config.workspace_root,
            run_id=config.run_id,
            upstream_root=materialized.root,
            dry_run=False,
            timeout_seconds=config.timeout_seconds,
        )
    )
    return {
        "run_id": config.run_id,
        "suite_name": suite_name,
        "task_family": config.task_family,
        "instance_id": config.instance_id,
        "status": wrapper_result.get("status", "failed"),
        "source": _source_payload(materialized),
        "skillflow_dataset": dataset_result,
        "wrapper_smoke": wrapper_result,
    }


def main(argv: list[str] | None = None) -> int:
    result = run_real_upstream_smoke(_config_from_args(argv))
    print(json.dumps(result, sort_keys=True), flush=True)
    return 0 if result["status"] == "succeeded" else 1


def _config_from_args(argv: list[str] | None = None) -> RealUpstreamSmokeConfig:
    parser = argparse.ArgumentParser(description="Run a real-upstream SkillFlow/SkillLearnBench wrapper smoke.")
    parser.add_argument("--suite", default=os.environ.get("BENCHMARK_REAL_UPSTREAM_SMOKE_SUITE", "SkillFlow"))
    parser.add_argument("--source-type", default=os.environ.get("BENCHMARK_REAL_UPSTREAM_SMOKE_SOURCE_TYPE", "git"))
    parser.add_argument("--source-uri", default=os.environ.get("BENCHMARK_REAL_UPSTREAM_SMOKE_SOURCE_URI"))
    parser.add_argument("--source-version", default=os.environ.get("BENCHMARK_REAL_UPSTREAM_SMOKE_SOURCE_VERSION"))
    parser.add_argument("--task-family", default=os.environ.get("BENCHMARK_REAL_UPSTREAM_SMOKE_TASK_FAMILY", ""))
    parser.add_argument("--instance-id", default=os.environ.get("BENCHMARK_REAL_UPSTREAM_SMOKE_INSTANCE_ID", ""))
    parser.add_argument(
        "--cache-root",
        default=os.environ.get("BENCHMARK_REAL_UPSTREAM_SMOKE_CACHE_ROOT", ".runtime/benchmark-real-upstream-cache"),
    )
    parser.add_argument(
        "--workspace-root",
        default=os.environ.get("BENCHMARK_REAL_UPSTREAM_SMOKE_WORKSPACE_ROOT", ".runtime/benchmark-real-upstream-smoke"),
    )
    parser.add_argument(
        "--run-id",
        default=os.environ.get("BENCHMARK_REAL_UPSTREAM_SMOKE_RUN_ID", f"real_upstream_smoke_{uuid4().hex}"),
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=int(os.environ.get("BENCHMARK_REAL_UPSTREAM_SMOKE_TIMEOUT_SECONDS", "3600")),
    )
    parser.add_argument(
        "--force-refresh",
        action="store_true",
        default=_env_bool("BENCHMARK_REAL_UPSTREAM_SMOKE_FORCE_REFRESH", False),
    )
    parser.add_argument(
        "--skillflow-dataset-repo-id",
        default=os.environ.get("SKILLFLOW_DATASET_REPO_ID", _DEFAULT_SKILLFLOW_DATASET_REPO_ID),
    )
    parser.add_argument(
        "--skillflow-dataset-revision",
        default=os.environ.get("SKILLFLOW_DATASET_REVISION", _DEFAULT_SKILLFLOW_DATASET_REVISION),
    )
    args = parser.parse_args(argv)
    return _config_from_env(vars(args))


def _config_from_env(values: Mapping[str, object] | None = None) -> RealUpstreamSmokeConfig:
    data = os.environ if values is None else values
    suite_name = _canonical_suite(_string(data, "suite", "BENCHMARK_REAL_UPSTREAM_SMOKE_SUITE", "SkillFlow"))
    catalog = load_fixture_catalog(suite_name)
    task_family, instance_id = _task_selection(
        catalog=catalog,
        task_family=_string(data, "task_family", "BENCHMARK_REAL_UPSTREAM_SMOKE_TASK_FAMILY", ""),
        instance_id=_string(data, "instance_id", "BENCHMARK_REAL_UPSTREAM_SMOKE_INSTANCE_ID", ""),
    )
    return RealUpstreamSmokeConfig(
        suite_name=suite_name,
        source_type=_string(data, "source_type", "BENCHMARK_REAL_UPSTREAM_SMOKE_SOURCE_TYPE", "git"),
        source_uri=_string(
            data,
            "source_uri",
            "BENCHMARK_REAL_UPSTREAM_SMOKE_SOURCE_URI",
            _default_runner_uri(catalog),
        ),
        source_version=_string(
            data,
            "source_version",
            "BENCHMARK_REAL_UPSTREAM_SMOKE_SOURCE_VERSION",
            _default_runner_revision(catalog),
        ),
        task_family=task_family,
        instance_id=instance_id,
        cache_root=Path(_string(data, "cache_root", "BENCHMARK_REAL_UPSTREAM_SMOKE_CACHE_ROOT", ".runtime/benchmark-real-upstream-cache")),
        workspace_root=Path(
            _string(data, "workspace_root", "BENCHMARK_REAL_UPSTREAM_SMOKE_WORKSPACE_ROOT", ".runtime/benchmark-real-upstream-smoke")
        ),
        run_id=_string(data, "run_id", "BENCHMARK_REAL_UPSTREAM_SMOKE_RUN_ID", f"real_upstream_smoke_{uuid4().hex}"),
        timeout_seconds=int(_string(data, "timeout_seconds", "BENCHMARK_REAL_UPSTREAM_SMOKE_TIMEOUT_SECONDS", "3600")),
        force_refresh=_bool(data, "force_refresh", "BENCHMARK_REAL_UPSTREAM_SMOKE_FORCE_REFRESH", False),
        skillflow_dataset_repo_id=_string(
            data,
            "skillflow_dataset_repo_id",
            "SKILLFLOW_DATASET_REPO_ID",
            _default_skillflow_task_asset_repo_id(catalog),
        ),
        skillflow_dataset_revision=_string(
            data,
            "skillflow_dataset_revision",
            "SKILLFLOW_DATASET_REVISION",
            _default_skillflow_task_asset_revision(catalog),
        ),
    )


def _source_payload(materialized: MaterializedUpstreamSource) -> dict[str, object]:
    return {
        "suite_name": materialized.suite_name,
        "source_type": materialized.source_type,
        "source_uri": materialized.source_uri,
        "source_version": materialized.source_version,
        "root": str(materialized.root),
        "lock_path": str(materialized.lock_path),
        "reused": materialized.reused,
        "applied_patches": materialized.applied_patches,
    }


def _skillflow_task_assets_payload(materialized: MaterializedSkillFlowTaskAssets) -> dict[str, object]:
    return {
        "source_type": materialized.source_type,
        "repo_id": materialized.repo_id,
        "revision": materialized.revision,
        "allow_patterns": list(materialized.allow_patterns),
        "local_dir": str(materialized.local_dir),
        "lock_path": str(materialized.lock_path),
        "file_count": materialized.file_count,
        "reused": materialized.reused,
    }


def _task_selection(*, catalog: BenchmarkFixtureCatalog, task_family: str, instance_id: str) -> tuple[str, str]:
    if task_family and instance_id:
        return task_family, instance_id
    first_instance = catalog.task_instances()[0]
    return task_family or first_instance.task_family, instance_id or first_instance.instance_id


def _default_runner_uri(catalog: BenchmarkFixtureCatalog) -> str:
    value = catalog.metadata.get("upstream_runner_uri")
    return value if isinstance(value, str) and value.strip() else catalog.source_uri


def _default_runner_revision(catalog: BenchmarkFixtureCatalog) -> str:
    value = catalog.metadata.get("upstream_runner_revision")
    return value if isinstance(value, str) and value.strip() else catalog.source_version


def _default_skillflow_task_asset_repo_id(catalog: BenchmarkFixtureCatalog) -> str:
    value = catalog.metadata.get("task_asset_repo_id")
    return value if catalog.suite_name == "SkillFlow" and isinstance(value, str) and value.strip() else _DEFAULT_SKILLFLOW_DATASET_REPO_ID


def _default_skillflow_task_asset_revision(catalog: BenchmarkFixtureCatalog) -> str:
    value = catalog.metadata.get("task_asset_revision")
    return value if catalog.suite_name == "SkillFlow" and isinstance(value, str) and value.strip() else _DEFAULT_SKILLFLOW_DATASET_REVISION


def _canonical_suite(suite_name: str) -> str:
    normalized = suite_name.strip().lower()
    if normalized == "skillflow":
        return "SkillFlow"
    if normalized == "skilllearnbench":
        return "SkillLearnBench"
    raise ValueError(f"Unsupported suite: {suite_name}")


def _string(data: Mapping[str, object], arg_key: str, env_key: str, default: str) -> str:
    value = data.get(arg_key, data.get(env_key, default))
    if isinstance(value, str) and value.strip():
        return value.strip()
    return default


def _bool(data: Mapping[str, object], arg_key: str, env_key: str, default: bool) -> bool:
    value = data.get(arg_key, data.get(env_key))
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _env_bool(key: str, default: bool) -> bool:
    value = os.environ.get(key)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


if __name__ == "__main__":
    raise SystemExit(main())
