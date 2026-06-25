"""One-shot manifest editor for #419 Layer 2 / batch 3.

Adds Layer 2 evidence for swe-bench-verified. Same parity pattern as
the by-construction batch 1/2 entries (mbpp, humaneval, livecodebench,
skill*): Loom's emitted test code IS the canonical upstream harness,
the construction is proven by adapter unit tests, and the live-runtime
gap (no in-CI execution of the per-instance multi-GB SWE-Bench Docker
image) is documented as a known limitation rather than treated as a
Layer 2 blocker.

After this batch: 10/12 v1.0 benchmarks have Layer 2 evidence. Two
remain genuinely blocked:
- math-500 (no Loom adapter, #512)
- terminal-bench-2 (depends on codex/issue426-tb2-followup)
"""

from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
MANIFEST = ROOT / "docs" / "benchmark-score-alignment.json"


_BATCH: dict[str, dict[str, object]] = {
    "swe-bench-verified": {
        "harbor_support": {
            "status": "not_supported",
            "parity_target": (
                "official SWE-Bench evaluation harness — pytest executing "
                "the union of FAIL_TO_PASS + PASS_TO_PASS node ids inside "
                "the per-instance SWE-Bench Docker image's /testbed checkout"
            ),
            "decision": (
                "Harbor (coder-harbor-cloud / voyager) ships no SWE-Bench "
                "verifier. Loom's adapter "
                "(`SWEBenchVerifiedAdapter.convert_instance`) emits the "
                "official harness verbatim: `solve.sh` runs `git apply "
                "--3way` of the canonical patch inside `/testbed`, then "
                "`tests/test_swebench.py` shells out to `pytest -x -q` "
                "with the union of the upstream `FAIL_TO_PASS` + "
                "`PASS_TO_PASS` node ids and asserts rc=0. The container "
                "image is the upstream `swebench/sweb.eval.x86_64.<slug>` "
                "per-instance evaluation image. Loom's verifier code path "
                "IS the upstream harness, so the parity claim is by "
                "construction; equivalence is proven by the adapter unit "
                "tests that pin the emitted structure."
            ),
        },
        "layer2_evidence": {
            "status": "replay_validated",
            "parity_kind": "construction",
            "replay_tests": [
                {
                    "case_id": "swe-bench-verified-emits-canonical-harness",
                    "test_id": (
                        "packages/loom-benchmarks/tests/"
                        "test_swe_bench_verified_adapter.py"
                        "::test_convert_emits_valid_task"
                    ),
                    "asserts": (
                        "the emitted bundle ships `solve.sh` with `git "
                        "apply` of the canonical patch, `tests/test_"
                        "swebench.py` with the exact upstream node ids, "
                        "and the per-instance `swebench/sweb.eval.x86_64` "
                        "Docker image — together this IS the upstream "
                        "SWE-Bench evaluation harness"
                    ),
                },
                {
                    "case_id": "swe-bench-verified-empty-test-ids-graceful",
                    "test_id": (
                        "packages/loom-benchmarks/tests/"
                        "test_swe_bench_verified_adapter.py"
                        "::test_empty_test_node_lists_emit_script_verifier_reward_zero"
                    ),
                    "asserts": (
                        "the degenerate `FAIL_TO_PASS=[] PASS_TO_PASS=[]` "
                        "case (which would silently 'pass' under pytest "
                        "with no tests collected) is detected at convert "
                        "time and emits a script verifier reporting "
                        "reward=0 with reason `no_upstream_test_node_ids`. "
                        "Guards against the #388 false-positive class — "
                        "no test ids must NOT score 1.0"
                    ),
                },
                {
                    "case_id": "swe-bench-verified-per-instance-image",
                    "test_id": (
                        "packages/loom-benchmarks/tests/"
                        "test_swe_bench_verified_adapter.py"
                        "::test_image_slug_replaces_double_underscore"
                    ),
                    "asserts": (
                        "the per-instance image name follows the upstream "
                        "slug-encoding convention (`repo__name-N` → "
                        "`swebench/sweb.eval.x86_64.repo_1776_name-N`), "
                        "so the runtime pulls the correct upstream eval "
                        "image instead of a Loom-built proxy"
                    ),
                },
            ],
            "known_limitations": [
                {
                    "limitation": "live_runtime_not_unit_tested",
                    "description": (
                        "No CI test pulls the multi-GB per-instance "
                        "`swebench/sweb.eval.x86_64.<slug>` image to "
                        "execute solve.sh + pytest end-to-end. Image "
                        "size (GBs) × image count (500 in SWE-Bench "
                        "Verified) makes per-instance CI execution "
                        "impractical. The construction claim above is "
                        "the verifier-semantics parity proof; live "
                        "runtime parity is observed when operators "
                        "submit oracle batches against SWE-Bench Verified "
                        "and the trials reach succeeded with reward 1.0."
                    ),
                    "mitigation": (
                        "Operator smoke: submit an oracle batch with N=5 "
                        "SWE-Bench Verified instances and verify all 5 "
                        "reach state=succeeded with the bundled patch. "
                        "Any verifier-semantics regression would surface "
                        "as reward!=1.0 on the oracle path."
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
