"""One-shot manifest editor for #419 Layer 2 / batch 2.

Adds Layer 2 evidence for the 5 benchmarks whose adapters already ship
the replay tests the gate needs: gpqa, mmlu-pro, livecodebench,
skillflow, skilllearnbench. Follows the same shape as batch 1.

Remaining v1.0 benchmarks not in this batch:
- math-500 — no Loom adapter exists yet; tracked separately (a missing-
  benchmark issue is filed alongside this PR).
- terminal-bench-2 — blocked on the codex/issue426-tb2-followup branch;
  Layer 2 lands cleanly once that work merges.
- swe-bench-verified — needs new patch-replay infrastructure; will be
  its own batch.
"""

from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
MANIFEST = ROOT / "docs" / "benchmark-score-alignment.json"


_BATCH: dict[str, dict[str, object]] = {
    "gpqa": {
        "harbor_support": {
            "status": "not_supported",
            "parity_target": (
                "exact letter match against the GPQA Extended dataset row's "
                "correct-answer letter"
            ),
            "decision": (
                "Harbor (coder-harbor-cloud) ships no GPQA verifier. "
                "Loom's adapter (`GPQAAdapter.convert_instance`) emits a "
                "script verifier that extracts the answer letter from "
                "`final_answer.txt` and compares to the canonical letter "
                "stored in `answer_key.json` (derived from the dataset row). "
                "Equivalence is proven by replay: known correct letter → "
                "reward 1.0."
            ),
        },
        "layer2_evidence": {
            "status": "replay_validated",
            "parity_kind": "replay",
            "replay_tests": [
                {
                    "case_id": "gpqa-correct-letter-scores-1",
                    "test_id": (
                        "packages/loom-benchmarks/tests/test_gpqa_adapter.py"
                        "::test_gpqa_verifier_scores_correct_letter"
                    ),
                    "asserts": (
                        "reward={'score': 1.0} when `final_answer.txt` "
                        "ends with the canonical letter from "
                        "`answer_key.json`"
                    ),
                },
            ],
        },
    },
    "mmlu-pro": {
        "harbor_support": {
            "status": "not_supported",
            "parity_target": (
                "exact letter match against the MMLU-Pro dataset row's "
                "answer field"
            ),
            "decision": (
                "Harbor ships no MMLU-Pro verifier. Loom's adapter "
                "(`MMLUProAdapter.convert_instance`) emits a script "
                "verifier that extracts the last standalone letter from "
                "`final_answer.txt` and compares to the canonical answer "
                "letter on the dataset row. Equivalence is proven by replay."
            ),
        },
        "layer2_evidence": {
            "status": "replay_validated",
            "parity_kind": "replay",
            "replay_tests": [
                {
                    "case_id": "mmlu-pro-correct-letter-scores-1",
                    "test_id": (
                        "packages/loom-benchmarks/tests/"
                        "test_mmlu_pro_adapter.py"
                        "::test_mmlu_pro_verifier_scores_last_standalone_letter"
                    ),
                    "asserts": (
                        "reward={'score': 1.0} when `final_answer.txt` "
                        "ends with the canonical letter; pins the "
                        "extraction rule against multi-letter reasoning "
                        "(e.g. \"Reasoning mentions A. Final answer: D\")"
                    ),
                },
            ],
        },
    },
    "livecodebench": {
        "harbor_support": {
            "status": "not_supported",
            "parity_target": (
                "upstream LiveCodeBench evaluator semantics — pytest "
                "executing IO/functional test cases against the agent's "
                "submitted code"
            ),
            "decision": (
                "Harbor ships no LiveCodeBench verifier. Loom's adapter "
                "(`LiveCodeBenchAdapter.convert_instance`) decodes the "
                "compressed IO/functional test cases from the upstream "
                "dataset row and emits them verbatim as pytest test files, "
                "so Loom's pytest verifier IS the canonical LiveCodeBench "
                "scorer by construction. Functional and IO formats both "
                "covered."
            ),
        },
        "layer2_evidence": {
            "status": "replay_validated",
            "parity_kind": "construction",
            "replay_tests": [
                {
                    "case_id": "livecodebench-io-tests-pass",
                    "test_id": (
                        "packages/loom-benchmarks/tests/"
                        "test_livecodebench_adapter.py"
                        "::test_livecodebench_solution_passes_subprocess_run"
                    ),
                    "asserts": (
                        "the bundled solution passes pytest end-to-end "
                        "against the decoded upstream test cases"
                    ),
                },
                {
                    "case_id": "livecodebench-private-cases-decoded",
                    "test_id": (
                        "packages/loom-benchmarks/tests/"
                        "test_livecodebench_adapter.py"
                        "::test_livecodebench_decodes_compressed_private_cases"
                    ),
                    "asserts": (
                        "the adapter correctly decodes the upstream's "
                        "compressed private test cases before emitting "
                        "them — guards against the parity claim "
                        "regressing if the upstream format changes"
                    ),
                },
                {
                    "case_id": "livecodebench-functional-format",
                    "test_id": (
                        "packages/loom-benchmarks/tests/"
                        "test_livecodebench_adapter.py"
                        "::test_livecodebench_functional_cases_call_solution_method"
                    ),
                    "asserts": (
                        "functional-format cases invoke the solution "
                        "method correctly, matching the upstream "
                        "evaluator's invocation"
                    ),
                },
            ],
        },
    },
    "skillflow": {
        "harbor_support": {
            "status": "not_supported",
            "parity_target": (
                "upstream SkillFlow bundle's bundled pytest suite — the "
                "verifier semantics ARE the canonical scorer"
            ),
            "decision": (
                "Harbor ships no SkillFlow verifier. Loom's SkillFlow "
                "adapter passes through the upstream task bundle's "
                "structure unchanged: the bundled pre-baked solution + "
                "tests pass under pytest end-to-end. The Loom pytest "
                "verifier is the canonical scorer by construction."
            ),
        },
        "layer2_evidence": {
            "status": "replay_validated",
            "parity_kind": "construction",
            "replay_tests": [
                {
                    "case_id": "skillflow-solution-runs",
                    "test_id": (
                        "packages/loom-benchmarks/tests/"
                        "test_skillflow_and_skilllearnbench_adapter.py"
                        "::test_skill_solution_runs[SkillFlowAdapter-skillflow]"
                    ),
                    "asserts": (
                        "the upstream-bundled solution passes pytest "
                        "end-to-end after `SkillFlowAdapter.convert_instance` "
                        "materializes the task"
                    ),
                },
            ],
        },
    },
    "skilllearnbench": {
        "harbor_support": {
            "status": "not_supported",
            "parity_target": (
                "upstream SkillLearnBench bundle's bundled pytest suite — "
                "the verifier semantics ARE the canonical scorer"
            ),
            "decision": (
                "Same construction as skillflow — the SkillLearnBench "
                "adapter passes through the upstream task bundle's "
                "structure unchanged and pytest is the canonical "
                "evaluator."
            ),
        },
        "layer2_evidence": {
            "status": "replay_validated",
            "parity_kind": "construction",
            "replay_tests": [
                {
                    "case_id": "skilllearnbench-solution-runs",
                    "test_id": (
                        "packages/loom-benchmarks/tests/"
                        "test_skillflow_and_skilllearnbench_adapter.py"
                        "::test_skill_solution_runs[SkillLearnBenchAdapter-skilllearnbench]"
                    ),
                    "asserts": (
                        "the upstream-bundled solution passes pytest "
                        "end-to-end after "
                        "`SkillLearnBenchAdapter.convert_instance` "
                        "materializes the task"
                    ),
                },
            ],
        },
    },
}


def main() -> int:
    manifest = json.loads(MANIFEST.read_text())
    by_id = {b["benchmark_id"]: b for b in manifest["benchmarks"]}
    for bid, updates in _BATCH.items():
        if bid not in by_id:
            raise SystemExit(f"benchmark {bid!r} missing from manifest")
        by_id[bid].update(updates)
    MANIFEST.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False) + "\n",
    )
    print(f"updated {len(_BATCH)} entries in {MANIFEST}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
