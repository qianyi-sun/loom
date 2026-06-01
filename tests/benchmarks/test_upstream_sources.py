import json
import subprocess
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from agentic_data_platform.benchmarks import UpstreamSourceSpec, materialize_upstream_source
from agentic_data_platform.benchmarks.manifests import catalog_from_local_tree


class UpstreamSourceMaterializationTest(unittest.TestCase):
    def test_materializes_local_tree_source_into_cache_with_lock_file(self):
        with TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            source_root = temp_path / "skilllearnbench-source"
            _write(source_root / "tasks/financial-analysis/financial-analysis-1/instruction.md")
            _write(source_root / "tasks/financial-analysis/financial-analysis-1/task.toml")
            _write(source_root / "tasks/financial-analysis/financial-analysis-1/environment/input.xlsx")
            _write(source_root / "tasks/financial-analysis/financial-analysis-1/tests/test_outputs.py")

            materialized = materialize_upstream_source(
                UpstreamSourceSpec(
                    suite_name="SkillLearnBench",
                    source_type="local-tree",
                    source_uri=str(source_root),
                    source_version="local-snapshot-001",
                ),
                cache_root=temp_path / "cache",
            )

            lock = json.loads(materialized.lock_path.read_text(encoding="utf-8"))
            catalog = catalog_from_local_tree(
                suite_name="SkillLearnBench",
                source_uri=materialized.source_uri,
                source_version=materialized.source_version,
                root=materialized.root,
            )
            copied_instruction_exists = (
                materialized.root / "tasks/financial-analysis/financial-analysis-1/instruction.md"
            ).exists()
            task_family = catalog.to_task_spec(
                task_family="financial-analysis",
                instance_id="financial-analysis-1",
            ).task_family

        self.assertFalse(materialized.reused)
        self.assertEqual(materialized.root.name, "tree")
        self.assertTrue(copied_instruction_exists)
        self.assertEqual(lock["suite_name"], "SkillLearnBench")
        self.assertEqual(lock["source_type"], "local-tree")
        self.assertEqual(lock["source_version"], "local-snapshot-001")
        self.assertEqual(lock["root"], str(materialized.root))
        self.assertEqual(task_family, "financial-analysis")

    def test_reuses_existing_materialized_source_when_lock_matches(self):
        with TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            source_root = temp_path / "skilllearnbench-source"
            _write(source_root / "tasks/financial-analysis/financial-analysis-1/instruction.md")
            _write(source_root / "tasks/financial-analysis/financial-analysis-1/task.toml")

            spec = UpstreamSourceSpec(
                suite_name="SkillLearnBench",
                source_type="local-tree",
                source_uri=str(source_root),
                source_version="local-snapshot-001",
            )
            first = materialize_upstream_source(spec, cache_root=temp_path / "cache")
            _write(source_root / "tasks/financial-analysis/financial-analysis-1/new-file.txt")

            second = materialize_upstream_source(spec, cache_root=temp_path / "cache")
            new_file_exists_in_cached_tree = (
                second.root / "tasks/financial-analysis/financial-analysis-1/new-file.txt"
            ).exists()

        self.assertFalse(first.reused)
        self.assertTrue(second.reused)
        self.assertEqual(second.root, first.root)
        self.assertFalse(new_file_exists_in_cached_tree)

    def test_materializes_git_source_at_requested_revision(self):
        with TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            repo_root = temp_path / "repo"
            _init_git_repo(repo_root)
            _write(repo_root / "tasks/demo/demo-1/instruction.md")
            _write(repo_root / "tasks/demo/demo-1/task.toml")
            _git(repo_root, "add", ".")
            _git(repo_root, "-c", "user.name=ADP Test", "-c", "user.email=adp@example.com", "commit", "-m", "seed")
            revision = _git(repo_root, "rev-parse", "HEAD").stdout.strip()

            materialized = materialize_upstream_source(
                UpstreamSourceSpec(
                    suite_name="SkillLearnBench",
                    source_type="git",
                    source_uri=str(repo_root),
                    source_version=revision,
                ),
                cache_root=temp_path / "cache",
            )
            checked_out = _git(materialized.root, "rev-parse", "HEAD").stdout.strip()
            lock = json.loads(materialized.lock_path.read_text(encoding="utf-8"))
            has_git_metadata = (materialized.root / ".git").exists()

        self.assertEqual(checked_out, revision)
        self.assertEqual(lock["source_type"], "git")
        self.assertEqual(lock["source_version"], revision)
        self.assertTrue(has_git_metadata)

    def test_applies_skillflow_harbor_api_patch_and_records_lock_metadata(self):
        with TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            source_root = temp_path / "skillflow-source"
            _write_skillflow_runner_sources(source_root)

            materialized = materialize_upstream_source(
                UpstreamSourceSpec(
                    suite_name="SkillFlow",
                    source_type="local-tree",
                    source_uri=str(source_root),
                    source_version="skillflow-test-snapshot",
                ),
                cache_root=temp_path / "cache",
            )

            lock = json.loads(materialized.lock_path.read_text(encoding="utf-8"))
            family_runner = (materialized.root / "family_job_runner.py").read_text(encoding="utf-8")
            iterative_runner = (materialized.root / "iterative_shared_skills_runner.py").read_text(encoding="utf-8")

        self.assertEqual(
            materialized.applied_patches,
            ["skillflow-harbor-api-compat-20260601"],
        )
        self.assertEqual(
            [patch["id"] for patch in lock["applied_patches"]],
            ["skillflow-harbor-api-compat-20260601"],
        )
        self.assertRegex(lock["applied_patches"][0]["sha256"], r"^[0-9a-f]{64}$")
        self.assertIn("from harbor.models.task.task import Task", family_runner)
        self.assertIn("Task.is_valid_dir(path, disable_verification=disable_verification)", family_runner)
        self.assertIn("job = await Job.create(group_config)", family_runner)
        self.assertIn("from harbor.models.task.task import Task", iterative_runner)
        self.assertIn("Task.is_valid_dir(path, disable_verification=disable_verification)", iterative_runner)
        self.assertIn("job = await Job.create(group_config)", iterative_runner)

    def test_rejects_unknown_source_type(self):
        with TemporaryDirectory() as temp_dir:
            with self.assertRaisesRegex(ValueError, "Unsupported upstream source type"):
                materialize_upstream_source(
                    UpstreamSourceSpec(
                        suite_name="SkillLearnBench",
                        source_type="s3",
                        source_uri="s3://bucket/path",
                        source_version="v1",
                    ),
                    cache_root=Path(temp_dir) / "cache",
                )


def _write(path: Path, text: str = "fixture\n") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _write_skillflow_runner_sources(root: Path) -> None:
    _write_numbered_file(
        root / "family_job_runner.py",
        line_count=420,
        lines={
            34: "from harbor.models.task.paths import TaskPaths\n",
            123: "            if TaskPaths(path).is_valid(disable_verification=disable_verification)\n",
            416: "        job = Job(config=group_config)\n",
            417: "        job.on_trial_ended(on_trial_ended_hook)\n",
            418: "        result = asyncio.run(job.run())\n",
        },
    )
    _write_numbered_file(
        root / "iterative_shared_skills_runner.py",
        line_count=786,
        lines={
            40: "from harbor.models.task.paths import TaskPaths\n",
            148: "            if TaskPaths(path).is_valid(disable_verification=disable_verification)\n",
            781: "        job = Job(config=group_config)\n",
            782: "        job.on_trial_ended(on_trial_ended_hook)\n",
            784: "        # Harbor Job.run 是 async，需要在同步入口中执行\n",
            785: "        result = asyncio.run(job.run())\n",
        },
    )


def _write_numbered_file(path: Path, *, line_count: int, lines: dict[int, str]) -> None:
    content = ["\n"] * line_count
    for line_number, line in lines.items():
        content[line_number - 1] = line
    _write(path, "".join(content))


def _init_git_repo(path: Path) -> None:
    path.mkdir(parents=True)
    _git(path, "init")


def _git(cwd: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args],
        cwd=cwd,
        capture_output=True,
        text=True,
        check=True,
    )
