"""One-shot manifest editor for #419 Layer 2 / batch 4 — final two.

After this batch: 12/12 v1.0 benchmarks have Layer 2 evidence.

- math-500 — codex's #480 shipped `MATH500Adapter` (inherits from
  `HendrycksMATHAdapter`) before my #512 misfiling. Verifier path
  extracts the boxed answer from `final_answer.txt` and compares
  via the math-equivalence checker. Replay-validated by the shared
  hendrycks-math verifier tests + math500-specific listing/convert
  tests.
- terminal-bench-2 — codex's TB2 work (#469, #487, etc.) has landed
  the adapter with a verifier shim that wraps the upstream TB2
  bundled `tests/` directory. Parity is by construction; oracle
  smoke EXECUTES end-to-end in CI via the `test_convert_renders_
  solution_yaml_for_oracle_smoke` test.
"""

from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
MANIFEST = ROOT / "docs" / "benchmark-score-alignment.json"


_BATCH: dict[str, dict[str, object]] = {
    "math-500": {
        "harbor_support": {
            "status": "not_supported",
            "parity_target": (
                "HuggingFaceH4/MATH-500 canonical row + boxed-answer "
                "equivalence checker (the math-eval `is_equiv` algorithm "
                "the upstream MATH paper uses)"
            ),
            "decision": (
                "Harbor (coder-harbor-cloud) ships no MATH-500 verifier. "
                "Loom's adapter (`MATH500Adapter`, inherits from "
                "`HendrycksMATHAdapter`) emits a script verifier that "
                "extracts the boxed final answer from `final_answer.txt` "
                "and compares it to the canonical row's `answer` field "
                "via the boxed-answer equivalence routine the MATH paper "
                "and HuggingFaceH4/MATH-500 use. Replay-validated by "
                "the shared hendrycks-math verifier test (same code path) "
                "+ math500-specific listing/convert tests."
            ),
        },
        "layer2_evidence": {
            "status": "replay_validated",
            "parity_kind": "replay",
            "replay_tests": [
                {
                    "case_id": "math-500-pinned-test-split",
                    "test_id": (
                        "packages/loom-benchmarks/tests/"
                        "test_hendrycks_math_adapter.py"
                        "::test_math500_lists_public_500_problem_test_split"
                    ),
                    "asserts": (
                        "MATH500Adapter lists the pinned 500-row test "
                        "split from `HuggingFaceH4/MATH-500@6e4ed1a`, "
                        "matching the canonical math-500 benchmark "
                        "definition"
                    ),
                },
                {
                    "case_id": "math-500-convert-namespaces-task-id",
                    "test_id": (
                        "packages/loom-benchmarks/tests/"
                        "test_hendrycks_math_adapter.py"
                        "::test_math500_convert_writes_math500_task_id"
                    ),
                    "asserts": (
                        "the materialized bundle namespaces task ids "
                        "under `math-500/...`, distinguishing math-500 "
                        "trials from the broader (non-v1) hendrycks-math "
                        "catalog"
                    ),
                },
                {
                    "case_id": "math-500-verifier-equivalent-answer-scores-1",
                    "test_id": (
                        "packages/loom-benchmarks/tests/"
                        "test_hendrycks_math_adapter.py"
                        "::test_hendrycks_math_verifier_accepts_equivalent_boxed_output"
                    ),
                    "asserts": (
                        "the shared math verifier (used by both "
                        "hendrycks-math and math-500) scores reward 1.0 "
                        "when the agent's boxed answer is mathematically "
                        "equivalent to the canonical answer (e.g. "
                        "`\\frac{1}{3}` vs `1/3`) — the same equivalence "
                        "routine the upstream MATH evaluator uses"
                    ),
                },
                {
                    "case_id": "math-500-extracts-nested-boxed",
                    "test_id": (
                        "packages/loom-benchmarks/tests/"
                        "test_hendrycks_math_adapter.py"
                        "::test_hendrycks_math_extracts_nested_boxed_answer"
                    ),
                    "asserts": (
                        "the boxed-answer extractor handles nested "
                        "structures and picks the LAST boxed answer "
                        "when multiple are present (matching upstream "
                        "convention)"
                    ),
                },
            ],
        },
    },
    "terminal-bench-2": {
        "harbor_support": {
            "status": "not_supported",
            "parity_target": (
                "upstream Terminal-Bench-2 bundled `tests/` directory — "
                "the verifier shim wraps the upstream test suite verbatim"
            ),
            "decision": (
                "Harbor (coder-harbor-cloud) ships no Terminal-Bench-2 "
                "verifier. Loom's adapter (in "
                "`packages/loom-benchmark-terminal-bench-2/`) emits a "
                "verifier shim (`verifier/run.sh`) that wraps the "
                "upstream TB2 `tests/` directory and emits a "
                "VerifierResult JSON matching Loom's contract. The "
                "shim's TEST_DIR defaults to `/app/environment/tb2-tests` "
                "where the upstream tests live in the per-task image. "
                "Loom's verifier code path IS the upstream TB2 test "
                "runner, so parity is by construction. Oracle smoke "
                "EXECUTES end-to-end in CI (not just structurally "
                "asserted) via the yaml-solution replay test, which is "
                "stronger evidence than the other by-construction "
                "benchmarks."
            ),
        },
        "layer2_evidence": {
            "status": "replay_validated",
            "parity_kind": "construction",
            "replay_tests": [
                {
                    "case_id": "tb2-oracle-solution-staged",
                    "test_id": (
                        "packages/loom-benchmark-terminal-bench-2/tests/"
                        "test_adapter_convert_instance.py"
                        "::test_convert_copies_reference_solution_for_oracle_smoke"
                    ),
                    "asserts": (
                        "the upstream `reference.sh` is staged + wired "
                        "into `solve.sh` for oracle agents, so oracle "
                        "trials execute the upstream-supplied canonical "
                        "solution against the verifier"
                    ),
                },
                {
                    "case_id": "tb2-oracle-yaml-solution-executes",
                    "test_id": (
                        "packages/loom-benchmark-terminal-bench-2/tests/"
                        "test_adapter_convert_instance.py"
                        "::test_convert_renders_solution_yaml_for_oracle_smoke"
                    ),
                    "asserts": (
                        "the YAML-solution oracle path EXECUTES "
                        "end-to-end (`subprocess.run(['bash', solve.sh], "
                        "check=True)` + asserts the output file matches "
                        "expected content). Stronger live-runtime "
                        "evidence than the by-construction benchmarks "
                        "above — the actual oracle script runs in CI"
                    ),
                },
                {
                    "case_id": "tb2-verifier-shim-emits-loom-result",
                    "test_id": (
                        "packages/loom-benchmark-terminal-bench-2/tests/"
                        "test_adapter_convert_instance.py"
                        "::test_generated_verifier_shim_emits_loom_verifier_result"
                    ),
                    "asserts": (
                        "the verifier shim emits a well-formed "
                        "VerifierResult JSON that Loom's verifier "
                        "machinery can ingest, proving the TB2-to-Loom "
                        "result-shape mapping is sound"
                    ),
                },
                {
                    "case_id": "tb2-verifier-shim-wraps-upstream-tests",
                    "test_id": (
                        "packages/loom-benchmark-terminal-bench-2/tests/"
                        "test_adapter_convert_instance.py"
                        "::test_convert_writes_verifier_shim"
                    ),
                    "asserts": (
                        "the verifier shim's TEST_DIR defaults to "
                        "`/app/environment/tb2-tests` (the upstream "
                        "TB2 test location) so the shim runs the "
                        "upstream test suite verbatim, not a Loom-built "
                        "proxy"
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
