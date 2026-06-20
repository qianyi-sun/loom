"""LiveCodeBench adapter contract (Plan 15 Phase 9)."""

from __future__ import annotations

import base64
import json
import pickle
import subprocess
import sys
import tomllib
import zlib
from pathlib import Path

from loom_benchmarks.adapters.livecodebench import LiveCodeBenchAdapter
from loom_benchmarks.base import BenchmarkInstance

from loom.models.task import TaskConfig

FIXTURE = Path(__file__).parent / "fixtures" / "livecodebench" / "sample.json"


def test_livecodebench_emits_io_pytest(tmp_path: Path) -> None:
    rec = json.loads(FIXTURE.read_text())[0]
    inst = BenchmarkInstance(
        instance_id=rec["question_id"], split="test", raw=rec,
    )
    LiveCodeBenchAdapter().convert_instance(inst, out_dir=tmp_path)
    cfg = TaskConfig.model_validate(
        tomllib.loads((tmp_path / "task.toml").read_text()),
    )
    assert cfg.verifier.name == "pytest"
    assert cfg.task.id == "livecodebench/lcb-9001"
    tests = list((tmp_path / "tests").glob("test_lcb_*.py"))
    assert len(tests) == 3  # 2 public + 1 private
    assert "55" in (tmp_path / "tests" / "test_lcb_2.py").read_text()


def test_livecodebench_license_is_cc_by_nc(tmp_path: Path) -> None:
    """Spec §7: LiveCodeBench tasks must carry `CC-BY-NC-4.0` so the
    Plan 13 license-allowlist keeps them out of the default allowlist
    until an operator opts in for non-commercial use (audit license-bypass)."""
    rec = json.loads(FIXTURE.read_text())[0]
    inst = BenchmarkInstance(
        instance_id=rec["question_id"], split="test", raw=rec,
    )
    converted = LiveCodeBenchAdapter().convert_instance(inst, out_dir=tmp_path)
    assert converted.license_spdx == "CC-BY-NC-4.0"
    assert LiveCodeBenchAdapter.license_spdx == "CC-BY-NC-4.0"


def test_livecodebench_solution_passes_subprocess_run(tmp_path: Path) -> None:
    """End-to-end: the canonical Fibonacci solution passes its own
    stdin tests when run via pytest in a subprocess."""
    rec = json.loads(FIXTURE.read_text())[0]
    inst = BenchmarkInstance(
        instance_id=rec["question_id"], split="test", raw=rec,
    )
    LiveCodeBenchAdapter().convert_instance(inst, out_dir=tmp_path)
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "tests/", "-q"],
        cwd=tmp_path, capture_output=True, text=True,
    )
    assert result.returncode == 0, (
        f"stdout={result.stdout}\nstderr={result.stderr}"
    )


def test_livecodebench_decodes_compressed_private_cases(tmp_path: Path) -> None:
    rec = json.loads(FIXTURE.read_text())[0]
    private_cases = [
        {"input": "2\n", "output": "1\n", "testtype": "stdin"},
        {"input": "10\n", "output": "55\n", "testtype": "stdin"},
    ]
    rec["private_test_cases"] = base64.b64encode(
        zlib.compress(pickle.dumps(json.dumps(private_cases))),
    ).decode("ascii")
    inst = BenchmarkInstance(
        instance_id=rec["question_id"], split="test", raw=rec,
    )

    LiveCodeBenchAdapter().convert_instance(inst, out_dir=tmp_path)

    tests = sorted((tmp_path / "tests").glob("test_lcb_*.py"))
    assert len(tests) == 4  # 2 public + 2 compressed private
    assert "55" in tests[-1].read_text()


def test_livecodebench_functional_cases_call_solution_method(tmp_path: Path) -> None:
    rec = {
        "question_id": "lcb-functional-1",
        "question_content": "Return how many passenger ages are above 60.",
        "starter_code": "class Solution:\n    def countSeniors(self, details):\n        pass\n",
        "code": (
            "class Solution:\n"
            "    def countSeniors(self, details):\n"
            "        return sum(int(item[11:13]) > 60 for item in details)\n"
        ),
        "public_test_cases": json.dumps([
            {
                "input": (
                    '["7868190130M7522", "5303914400F9211", '
                    '"9273338290F4010"]'
                ),
                "output": "2",
                "testtype": "functional",
            }
        ]),
        "private_test_cases": "[]",
        "metadata": json.dumps({"func_name": "countSeniors"}),
        "platform": "leetcode",
        "difficulty": "easy",
    }
    inst = BenchmarkInstance(
        instance_id=rec["question_id"], split="test", raw=rec,
    )
    LiveCodeBenchAdapter().convert_instance(inst, out_dir=tmp_path)

    result = subprocess.run(
        [sys.executable, "-m", "pytest", "tests/", "-q"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, (
        f"stdout={result.stdout}\nstderr={result.stderr}"
    )


def test_livecodebench_functional_cases_support_multiline_args(
    tmp_path: Path,
) -> None:
    rec = {
        "question_id": "lcb-functional-2",
        "question_content": "Return the maximum OR after shifting one value.",
        "starter_code": "class Solution:\n    def maximumOr(self, nums, k):\n        pass\n",
        "code": (
            "class Solution:\n"
            "    def maximumOr(self, nums, k):\n"
            "        return max((nums[i] << k) | "
            "sum(nums[j] for j in range(len(nums)) if j != i) "
            "for i in range(len(nums)))\n"
        ),
        "public_test_cases": json.dumps([
            {"input": "[12, 9]\n1", "output": "30", "testtype": "functional"}
        ]),
        "private_test_cases": "[]",
        "metadata": json.dumps({"func_name": "maximumOr"}),
        "platform": "leetcode",
        "difficulty": "medium",
    }
    inst = BenchmarkInstance(
        instance_id=rec["question_id"], split="test", raw=rec,
    )
    LiveCodeBenchAdapter().convert_instance(inst, out_dir=tmp_path)

    result = subprocess.run(
        [sys.executable, "-m", "pytest", "tests/", "-q"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, (
        f"stdout={result.stdout}\nstderr={result.stderr}"
    )
