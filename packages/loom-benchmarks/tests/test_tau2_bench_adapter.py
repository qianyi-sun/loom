from __future__ import annotations

import gzip
import json
import os
import subprocess
import sys
import tomllib
from pathlib import Path

from loom_benchmarks.adapters.tau2_bench import Tau2BenchAdapter

from loom.models.task import TaskConfig
from loom.models.verifier import VerifierResult


def _write_tau2_fixture(root: Path) -> None:
    repo = root / "repo"
    for domain in ("airline", "retail", "telecom"):
        domain_dir = repo / "domains" / domain
        domain_dir.mkdir(parents=True)
        (domain_dir / "policy.md").write_text(f"# {domain} policy\nFollow policy.\n")
        (domain_dir / "db.json").write_text(json.dumps({"domain": domain}))

    (repo / "domains" / "airline" / "tasks.json").write_text(
        json.dumps(
            [
                {
                    "id": "cancel_refusal",
                    "user_scenario": {"instructions": "Cancel reservation ABC."},
                    "evaluation_criteria": {
                        "actions": [],
                        "communicate_info": [],
                        "nl_assertions": ["Agent should refuse cancellation."],
                    },
                }
            ],
        ),
    )
    (repo / "domains" / "retail" / "tasks.json").write_text(
        json.dumps(
            [
                {
                    "id": "exchange_keyboard",
                    "ticket": "Exchange the keyboard.",
                    "user_scenario": {"instructions": {"known_info": "Yusuf Rossi"}},
                    "evaluation_criteria": {
                        "actions": [
                            {
                                "action_id": "0_0",
                                "name": "get_order_details",
                                "arguments": {"order_id": "#W2378156"},
                            }
                        ],
                        "communicate_info": [],
                    },
                }
            ],
        ),
    )
    (repo / "domains" / "telecom" / "tasks.json").write_text(
        json.dumps(
            [
                {
                    "id": "mobile_data_issue",
                    "ticket": "Fix mobile data.",
                    "user_scenario": {"instructions": {"domain": "telecom"}},
                    "evaluation_criteria": {
                        "actions": [
                            {
                                "action_id": "toggle_data_0",
                                "requestor": "user",
                                "name": "toggle_data",
                                "arguments": {},
                            }
                        ],
                        "env_assertions": [
                            {"func_name": "assert_mobile_data_status", "arguments": {}}
                        ],
                    },
                }
            ],
        ),
    )


def test_tau2_bench_lists_default_leaderboard_domains(tmp_path: Path) -> None:
    _write_tau2_fixture(tmp_path)

    instances = list(Tau2BenchAdapter().list_instances(source_dir=tmp_path, split="test"))

    assert [i.instance_id for i in instances] == [
        "airline/000",
        "retail/000",
        "telecom/000",
    ]
    assert instances[0].tags == {
        "domain": "airline",
        "task_set": "default_leaderboard",
        "upstream_task_id": "cancel_refusal",
    }


def test_tau2_bench_convert_writes_task_bundle(tmp_path: Path) -> None:
    source = tmp_path / "source"
    _write_tau2_fixture(source)
    instance = list(Tau2BenchAdapter().list_instances(source_dir=source, split="test"))[1]
    out = tmp_path / "out"

    converted = Tau2BenchAdapter().convert_instance(instance, out_dir=out)

    cfg = TaskConfig.model_validate(tomllib.loads((out / "task.toml").read_text()))
    assert converted.task_id == "tau2-bench/retail/000"
    assert cfg.task.id == "tau2-bench/retail/000"
    assert cfg.steps[0].artifacts == ["agent_output.json"]
    assert "Exchange the keyboard." in (out / "instruction.md").read_text()
    assert json.loads((out / "ground_truth.json").read_text())["actions"][0]["name"] == (
        "get_order_details"
    )
    with gzip.open(out / "domain" / "db.json.gz", "rt") as fh:
        assert json.load(fh) == {"domain": "retail"}


def test_tau2_bench_verifier_scores_required_actions(tmp_path: Path) -> None:
    source = tmp_path / "source"
    _write_tau2_fixture(source)
    instance = list(Tau2BenchAdapter().list_instances(source_dir=source, split="test"))[1]
    out = tmp_path / "out"
    Tau2BenchAdapter().convert_instance(instance, out_dir=out)
    (out / "agent_output.json").write_text(
        json.dumps(
            {
                "actions": [
                    {"name": "get_order_details", "arguments": {"order_id": "#W2378156"}}
                ],
                "messages": ["I found the order."],
            },
        ),
    )
    verifier_out = tmp_path / "verifier-output.json"

    env = dict(os.environ)
    env["PYTHON"] = sys.executable
    env["LOOM_VERIFIER_OUTPUT"] = str(verifier_out)
    result = subprocess.run(
        ["sh", str(out / "verifier" / "run.sh")],
        env=env,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    parsed = VerifierResult.model_validate_json(verifier_out.read_text())
    assert parsed.rewards == {"score": 1.0}
    assert parsed.checks[0].passed is True
