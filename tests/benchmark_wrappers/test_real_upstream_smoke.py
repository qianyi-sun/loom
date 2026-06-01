import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from agentic_data_platform.benchmark_wrappers.real_upstream_smoke import (
    RealUpstreamSmokeConfig,
    _config_from_env,
    run_real_upstream_smoke,
)
from agentic_data_platform.benchmarks import (
    MaterializedSkillFlowTaskAssets,
    MaterializedUpstreamSource,
    SkillFlowTaskAssetsSpec,
    UpstreamSourceSpec,
)


class RealUpstreamSmokeTest(unittest.TestCase):
    def test_skillflow_smoke_materializes_source_downloads_dataset_and_runs_wrapper(self):
        with TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            materialized_root = temp_path / "upstream"
            materialized_root.mkdir()
            calls = _FakeRealUpstreamSmokeCalls(materialized_root)

            result = run_real_upstream_smoke(
                RealUpstreamSmokeConfig(
                    suite_name="SkillFlow",
                    source_type="git",
                    source_uri="https://github.com/ZhangZi-a/SkillFlow",
                    source_version="runner-commit",
                    task_family="OCR-Data-Extraction",
                    instance_id="task_family_invoice_images",
                    cache_root=temp_path / "cache",
                    workspace_root=temp_path / "workspace",
                    run_id="real_skillflow_smoke_unit",
                    timeout_seconds=120,
                ),
                materialize_source=calls.materialize,
                materialize_skillflow_task_assets=calls.materialize_skillflow_task_assets,
                wrapper_smoke_runner=calls.run_wrapper_smoke,
            )

        self.assertEqual(result["status"], "succeeded")
        self.assertEqual(result["run_id"], "real_skillflow_smoke_unit")
        self.assertEqual(result["suite_name"], "SkillFlow")
        self.assertEqual(result["source"]["source_version"], "runner-commit")
        self.assertEqual(result["source"]["applied_patches"], ["skillflow-test-patch"])
        self.assertEqual(
            calls.materialize_specs,
            [
                UpstreamSourceSpec(
                    suite_name="SkillFlow",
                    source_type="git",
                    source_uri="https://github.com/ZhangZi-a/SkillFlow",
                    source_version="runner-commit",
                )
            ],
        )
        self.assertEqual(
            calls.task_asset_specs,
            [
                SkillFlowTaskAssetsSpec(
                    repo_id="zhang-ziao/SkillFlow-Task",
                    revision="ecaadb0e25d5d5cfd87bd86d81e77b4abe3a00bc",
                    allow_patterns=["test_tasks/OCR-Data-Extraction/**"],
                )
            ],
        )
        self.assertEqual(result["skillflow_dataset"]["source_type"], "huggingface-dataset")
        self.assertEqual(result["skillflow_dataset"]["revision"], "ecaadb0e25d5d5cfd87bd86d81e77b4abe3a00bc")
        self.assertEqual(calls.wrapper_configs[0].suite_name, "SkillFlow")
        self.assertEqual(calls.wrapper_configs[0].upstream_root, materialized_root)
        self.assertEqual(calls.wrapper_configs[0].dry_run, False)

    def test_skilllearnbench_smoke_uses_materialized_root_without_dataset_download(self):
        with TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            materialized_root = temp_path / "skilllearnbench-upstream"
            materialized_root.mkdir()
            calls = _FakeRealUpstreamSmokeCalls(materialized_root)

            result = run_real_upstream_smoke(
                RealUpstreamSmokeConfig(
                    suite_name="SkillLearnBench",
                    source_type="git",
                    source_uri="https://github.com/cxcscmu/SkillLearnBench",
                    source_version="638284",
                    task_family="financial-analysis",
                    instance_id="financial-analysis-2",
                    cache_root=temp_path / "cache",
                    workspace_root=temp_path / "workspace",
                    run_id="real_slb_smoke_unit",
                    timeout_seconds=120,
                ),
                materialize_source=calls.materialize,
                materialize_skillflow_task_assets=calls.materialize_skillflow_task_assets,
                wrapper_smoke_runner=calls.run_wrapper_smoke,
            )

        self.assertEqual(result["status"], "succeeded")
        self.assertEqual(result["suite_name"], "SkillLearnBench")
        self.assertEqual(calls.task_asset_specs, [])
        self.assertEqual(calls.wrapper_configs[0].suite_name, "SkillLearnBench")
        self.assertEqual(calls.wrapper_configs[0].upstream_root, materialized_root)

    def test_default_env_config_uses_skillflow_catalog_runner_pin(self):
        with TemporaryDirectory() as temp_dir:
            config = _config_from_env(
                {
                    "BENCHMARK_REAL_UPSTREAM_SMOKE_CACHE_ROOT": str(Path(temp_dir) / "cache"),
                    "BENCHMARK_REAL_UPSTREAM_SMOKE_WORKSPACE_ROOT": str(Path(temp_dir) / "workspace"),
                    "BENCHMARK_REAL_UPSTREAM_SMOKE_RUN_ID": "real_upstream_smoke_unit",
                }
            )

        self.assertEqual(config.suite_name, "SkillFlow")
        self.assertEqual(config.source_type, "git")
        self.assertEqual(config.source_uri, "https://github.com/ZhangZi-a/SkillFlow")
        self.assertEqual(config.source_version, "7b49ff5a7e26cd7706e959bfa0dba4746d18440d")
        self.assertEqual(config.task_family, "OCR-Data-Extraction")
        self.assertEqual(config.instance_id, "task_family_invoice_images")
        self.assertEqual(config.run_id, "real_upstream_smoke_unit")
        self.assertEqual(config.skillflow_dataset_repo_id, "zhang-ziao/SkillFlow-Task")
        self.assertEqual(config.skillflow_dataset_revision, "ecaadb0e25d5d5cfd87bd86d81e77b4abe3a00bc")


class _FakeRealUpstreamSmokeCalls:
    def __init__(self, materialized_root: Path) -> None:
        self.materialized_root = materialized_root
        self.materialize_specs: list[UpstreamSourceSpec] = []
        self.task_asset_specs: list[SkillFlowTaskAssetsSpec] = []
        self.wrapper_configs = []

    def materialize(self, spec: UpstreamSourceSpec, *, cache_root: Path, force_refresh: bool = False):
        self.materialize_specs.append(spec)
        return MaterializedUpstreamSource(
            suite_name=spec.suite_name,
            source_type=spec.source_type,
            source_uri=spec.source_uri,
            source_version=spec.source_version,
            root=self.materialized_root,
            lock_path=self.materialized_root / "adp-upstream-source-lock.json",
            reused=False,
            applied_patches=["skillflow-test-patch"] if spec.suite_name == "SkillFlow" else [],
        )

    def materialize_skillflow_task_assets(
        self,
        spec: SkillFlowTaskAssetsSpec,
        *,
        local_dir: Path,
        force_refresh: bool = False,
    ) -> MaterializedSkillFlowTaskAssets:
        self.task_asset_specs.append(spec)
        return MaterializedSkillFlowTaskAssets(
            repo_id=spec.repo_id,
            revision=spec.revision,
            allow_patterns=spec.allow_patterns,
            local_dir=local_dir,
            lock_path=local_dir / "adp-skillflow-task-assets-lock.json",
            file_count=3,
            reused=False,
        )

    def run_wrapper_smoke(self, config):
        self.wrapper_configs.append(config)
        return {
            "run_id": config.run_id,
            "suite_name": config.suite_name,
            "status": "succeeded",
            "exit_code": 0,
        }
