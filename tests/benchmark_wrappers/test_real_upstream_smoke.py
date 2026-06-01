import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from agentic_data_platform.benchmark_wrappers.real_upstream_smoke import (
    RealUpstreamSmokeConfig,
    _config_from_env,
    run_real_upstream_smoke,
)
from agentic_data_platform.benchmarks import MaterializedUpstreamSource, UpstreamSourceSpec


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
                skillflow_dataset_downloader=calls.download_skillflow_dataset,
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
            calls.dataset_downloads,
            [
                {
                    "repo_id": "zhang-ziao/SkillFlow-Task",
                    "repo_type": "dataset",
                    "revision": "main",
                    "local_dir": materialized_root,
                    "allow_patterns": ["test_tasks/OCR-Data-Extraction/**"],
                }
            ],
        )
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
                skillflow_dataset_downloader=calls.download_skillflow_dataset,
                wrapper_smoke_runner=calls.run_wrapper_smoke,
            )

        self.assertEqual(result["status"], "succeeded")
        self.assertEqual(result["suite_name"], "SkillLearnBench")
        self.assertEqual(calls.dataset_downloads, [])
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


class _FakeRealUpstreamSmokeCalls:
    def __init__(self, materialized_root: Path) -> None:
        self.materialized_root = materialized_root
        self.materialize_specs: list[UpstreamSourceSpec] = []
        self.dataset_downloads: list[dict[str, object]] = []
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

    def download_skillflow_dataset(
        self,
        *,
        repo_id: str,
        repo_type: str,
        revision: str,
        local_dir: Path,
        allow_patterns: list[str],
    ) -> dict[str, object]:
        self.dataset_downloads.append(
            {
                "repo_id": repo_id,
                "repo_type": repo_type,
                "revision": revision,
                "local_dir": local_dir,
                "allow_patterns": allow_patterns,
            }
        )
        return {"repo_id": repo_id, "revision": revision, "file_count": 3}

    def run_wrapper_smoke(self, config):
        self.wrapper_configs.append(config)
        return {
            "run_id": config.run_id,
            "suite_name": config.suite_name,
            "status": "succeeded",
            "exit_code": 0,
        }
