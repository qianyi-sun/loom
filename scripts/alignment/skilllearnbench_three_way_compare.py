"""Three-way SkillLearnBench alignment comparator.

Joins per-task results from three sources and produces a CSV +
Markdown report that surfaces variance attribution between:

  1. **official** — upstream SkillLearnBench reference harness output
     (a directory of `result.json` files from
     `evaluation_log_*/<shard>/<method>/<family>/<task>/<timestamp>/`).
  2. **loom_arm** — an existing Loom batch's per-task comparison CSV
     produced by the original two-way `compare_full100_results.py`
     run; we read the joined CSV (which already contains the
     `loom_aggregate_reward`, `loom_worker_capabilities`, etc.
     columns) and pull the Loom side out.
  3. **loom_x86** — a fresh Loom batch on a local x86 cluster,
     queried directly via `loom eval trial list --format json`.

Each row of the output reports:
  - task_id
  - official_reward, loom_arm_reward, loom_x86_reward
  - pairwise concordance flags (arm_vs_official, x86_vs_official,
    arm_vs_x86)
  - architecture (from worker_capabilities, where available)
  - any failure reason on either Loom side

Aggregates:
  - per-source totals + mean reward + pass rate
  - 3 confusion matrices (one per pair)
  - count of fully concordant rows (all three agree)
  - count of arch-specific divergences
    (arm==official != x86, or x86==official != arm)

Refs: #6 #49 #129.
"""

from __future__ import annotations

import argparse
import csv
import json
import subprocess
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any


def _task_key(task_id: str) -> str:
    prefix = "skilllearnbench/"
    return task_id[len(prefix):] if task_id.startswith(prefix) else task_id


def _reward(value: Any) -> float | None:
    if value is None or value == "":
        return None
    if isinstance(value, dict):
        return _reward(value.get("score"))
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def load_official(output_dir: Path) -> dict[str, dict[str, Any]]:
    """Read per-task `result.json` files from the official runner's
    output dir."""
    rows: dict[str, dict[str, Any]] = {}
    for path in sorted(output_dir.rglob("result.json")):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            continue
        task_id = str(data.get("task_id") or "")
        if not task_id:
            continue
        key = _task_key(task_id)
        rows[key] = {
            "task_id": key,
            "official_reward": _reward(data.get("reward")),
            "official_agent_exit": data.get("agent_exit"),
            "official_agent_timed_out": data.get("agent_timed_out"),
            "official_verifier_exit": data.get("verifier_exit"),
            "official_result_path": str(path),
        }
    return rows


def load_loom_arm_from_csv(csv_path: Path) -> dict[str, dict[str, Any]]:
    """Load the ARM Loom side from an existing two-way comparison
    CSV (column shape: `loom_aggregate_reward`, `loom_state`,
    `loom_worker_capabilities`, `loom_worker_hostname`,
    `loom_worker_pool_name`, etc.)."""
    rows: dict[str, dict[str, Any]] = {}
    with csv_path.open() as fh:
        for record in csv.DictReader(fh):
            key = _task_key(str(record.get("task_id") or ""))
            if not key:
                continue
            rows[key] = {
                "loom_arm_reward": _reward(record.get("loom_aggregate_reward")),
                "loom_arm_state": record.get("loom_state"),
                "loom_arm_trial_id": record.get("loom_trial_id"),
                "loom_arm_worker_pool": record.get("loom_worker_pool_name"),
                "loom_arm_worker_caps": record.get("loom_worker_capabilities"),
                "loom_arm_failure": record.get("loom_failure_reason"),
                "loom_arm_input_tokens": record.get("loom_input_tokens"),
                "loom_arm_output_tokens": record.get("loom_output_tokens"),
                "loom_arm_llm_calls": record.get("loom_llm_calls_count"),
            }
    return rows


def load_loom_x86_from_local(batch_id: str) -> dict[str, dict[str, Any]]:
    """Query the local Loom Postgres directly for one batch's trials.
    `loom eval trial list` doesn't filter by batch_id; going through
    docker exec + psql keeps the join trivially scoped."""
    sql = (
        "SELECT id::text, task_id, state, failure_reason, "
        "(result->>'aggregate_reward')::float AS aggregate_reward, "
        "result->'capabilities_snapshot' AS capabilities, "
        "(result->>'total_prompt_tokens')::int AS prompt_tokens, "
        "(result->>'total_completion_tokens')::int AS completion_tokens "
        "FROM trials WHERE batch_id = '" + batch_id + "'::uuid"
    )
    result = subprocess.run(
        [
            "sg", "docker", "-c",
            f'docker exec deploy-postgres-1 psql -U loom -d loom -t -A -F"|" -c "{sql}"',
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    trials: list[dict[str, Any]] = []
    for line in result.stdout.strip().splitlines():
        if not line.strip():
            continue
        parts = line.split("|")
        if len(parts) < 8:
            continue
        trials.append({
            "id": parts[0],
            "task_id": parts[1],
            "state": parts[2],
            "failure_reason": parts[3] or None,
            "aggregate_reward": float(parts[4]) if parts[4] else None,
            "capabilities_raw": parts[5] or None,
            "total_prompt_tokens": int(parts[6]) if parts[6] else 0,
            "total_completion_tokens": int(parts[7]) if parts[7] else 0,
        })
    rows: dict[str, dict[str, Any]] = {}
    for t in trials:
        task_id = str(t.get("task_id") or "")
        if not task_id.startswith("skilllearnbench/"):
            continue
        key = _task_key(task_id)
        caps_raw = t.get("capabilities_raw")
        arch = None
        if caps_raw:
            try:
                caps = json.loads(caps_raw)
                if isinstance(caps, list) and caps:
                    arch = caps[0].get("cpu_arch")
            except Exception:
                pass
        rows[key] = {
            "loom_x86_reward": t.get("aggregate_reward"),
            "loom_x86_state": t.get("state"),
            "loom_x86_trial_id": t.get("id"),
            "loom_x86_cpu_arch": arch,
            "loom_x86_failure": t.get("failure_reason"),
            "loom_x86_input_tokens": t.get("total_prompt_tokens"),
            "loom_x86_output_tokens": t.get("total_completion_tokens"),
        }
    return rows


@dataclass
class Cell:
    task_id: str
    official_reward: float | None
    loom_arm_reward: float | None
    loom_x86_reward: float | None
    loom_arm_failure: str | None
    loom_x86_failure: str | None
    loom_arm_arch: str | None
    loom_x86_arch: str | None

    def concordance(self) -> str:
        o, a, x = self.official_reward, self.loom_arm_reward, self.loom_x86_reward
        present = sum(v is not None for v in (o, a, x))
        if present < 3:
            return "incomplete"
        if a == o == x:
            return "three_way_match"
        if a == o:
            return "x86_dissents"
        if x == o:
            return "arm_dissents"
        if a == x:
            return "loom_agrees_official_dissents"
        return "all_three_dissent"


def join(
    official: dict[str, dict[str, Any]],
    loom_arm: dict[str, dict[str, Any]],
    loom_x86: dict[str, dict[str, Any]],
) -> list[Cell]:
    all_keys = sorted(set(official) | set(loom_arm) | set(loom_x86))
    cells: list[Cell] = []
    for k in all_keys:
        o = official.get(k, {})
        a = loom_arm.get(k, {})
        x = loom_x86.get(k, {})
        arm_arch = None
        caps = a.get("loom_arm_worker_caps")
        if caps:
            try:
                arm_arch = json.loads(caps.replace("'", '"'))[0].get("cpu_arch")
            except Exception:
                arm_arch = None
        cells.append(Cell(
            task_id=k,
            official_reward=o.get("official_reward"),
            loom_arm_reward=a.get("loom_arm_reward"),
            loom_x86_reward=x.get("loom_x86_reward"),
            loom_arm_failure=a.get("loom_arm_failure"),
            loom_x86_failure=x.get("loom_x86_failure"),
            loom_arm_arch=arm_arch,
            loom_x86_arch=x.get("loom_x86_cpu_arch"),
        ))
    return cells


def _safe_sum(values: list[float | None]) -> float:
    return sum(v for v in values if v is not None)


def _safe_count(values: list[float | None]) -> int:
    return sum(1 for v in values if v is not None)


def aggregate(cells: list[Cell]) -> dict[str, Any]:
    o_rewards = [c.official_reward for c in cells]
    a_rewards = [c.loom_arm_reward for c in cells]
    x_rewards = [c.loom_x86_reward for c in cells]
    concordance_counts = Counter(c.concordance() for c in cells)
    pairwise = {}
    for label, (lefts, rights) in (
        ("arm_vs_official", (a_rewards, o_rewards)),
        ("x86_vs_official", (x_rewards, o_rewards)),
        ("arm_vs_x86", (a_rewards, x_rewards)),
    ):
        matches = 0
        total = 0
        for left, right in zip(lefts, rights, strict=False):
            if left is None or right is None:
                continue
            total += 1
            if left == right:
                matches += 1
        pairwise[label] = {
            "total": total,
            "matches": matches,
            "match_rate": matches / total if total else None,
        }
    return {
        "n": len(cells),
        "official_sum": _safe_sum(o_rewards),
        "loom_arm_sum": _safe_sum(a_rewards),
        "loom_x86_sum": _safe_sum(x_rewards),
        "official_present": _safe_count(o_rewards),
        "loom_arm_present": _safe_count(a_rewards),
        "loom_x86_present": _safe_count(x_rewards),
        "concordance": dict(concordance_counts),
        "pairwise": pairwise,
    }


def write_csv(cells: list[Cell], path: Path) -> None:
    fieldnames = [
        "task_id", "official_reward", "loom_arm_reward", "loom_x86_reward",
        "concordance", "loom_arm_arch", "loom_x86_arch",
        "loom_arm_failure", "loom_x86_failure",
    ]
    with path.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fieldnames)
        w.writeheader()
        for c in cells:
            w.writerow({
                "task_id": c.task_id,
                "official_reward": c.official_reward,
                "loom_arm_reward": c.loom_arm_reward,
                "loom_x86_reward": c.loom_x86_reward,
                "concordance": c.concordance(),
                "loom_arm_arch": c.loom_arm_arch,
                "loom_x86_arch": c.loom_x86_arch,
                "loom_arm_failure": c.loom_arm_failure,
                "loom_x86_failure": c.loom_x86_failure,
            })


def write_report(
    cells: list[Cell], agg: dict[str, Any], path: Path,
) -> None:
    lines: list[str] = []
    lines.append("# SkillLearnBench three-way alignment report")
    lines.append("")
    lines.append(f"- N tasks joined: **{agg['n']}**")
    lines.append(f"- Official: {agg['official_sum']:.1f} / {agg['official_present']} "
                 f"({(agg['official_sum']/agg['official_present'] if agg['official_present'] else 0):.3f} mean)")
    lines.append(f"- Loom ARM: {agg['loom_arm_sum']:.1f} / {agg['loom_arm_present']} "
                 f"({(agg['loom_arm_sum']/agg['loom_arm_present'] if agg['loom_arm_present'] else 0):.3f} mean)")
    lines.append(f"- Loom x86: {agg['loom_x86_sum']:.1f} / {agg['loom_x86_present']} "
                 f"({(agg['loom_x86_sum']/agg['loom_x86_present'] if agg['loom_x86_present'] else 0):.3f} mean)")
    lines.append("")
    lines.append("## Concordance buckets")
    lines.append("")
    lines.append("| Bucket | Count |")
    lines.append("|---|---:|")
    for k in sorted(agg["concordance"]):
        lines.append(f"| {k} | {agg['concordance'][k]} |")
    lines.append("")
    lines.append("## Pairwise reward match rates")
    lines.append("")
    lines.append("| Pair | Matches / Total | Match rate |")
    lines.append("|---|---:|---:|")
    for k, v in agg["pairwise"].items():
        rate = f"{v['match_rate']*100:.2f}%" if v["match_rate"] is not None else "n/a"
        lines.append(f"| {k} | {v['matches']} / {v['total']} | {rate} |")
    lines.append("")
    lines.append("## Per-task table (mismatches first)")
    lines.append("")
    lines.append("| Task | Official | Loom ARM | Loom x86 | Concordance |")
    lines.append("|---|---:|---:|---:|---|")
    mismatch = [c for c in cells if c.concordance() not in ("three_way_match",)]
    match = [c for c in cells if c.concordance() == "three_way_match"]
    for c in sorted(mismatch, key=lambda c: c.task_id):
        lines.append(
            f"| {c.task_id} | {c.official_reward} | {c.loom_arm_reward} | {c.loom_x86_reward} | {c.concordance()} |",
        )
    lines.append("")
    lines.append(f"_{len(match)} three-way matched rows omitted from the table; see CSV._")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--official-output-dir", required=True, type=Path,
                        help="Directory of upstream `result.json` files.")
    parser.add_argument("--arm-comparison-csv", required=True, type=Path,
                        help="Existing two-way comparison CSV with `loom_*` columns.")
    parser.add_argument("--x86-loom-batch-id", required=True,
                        help="Local Loom batch id (UUID).")
    parser.add_argument("--out-csv", required=True, type=Path)
    parser.add_argument("--out-md", required=True, type=Path)
    args = parser.parse_args()

    official = load_official(args.official_output_dir)
    loom_arm = load_loom_arm_from_csv(args.arm_comparison_csv)
    loom_x86 = load_loom_x86_from_local(args.x86_loom_batch_id)

    cells = join(official, loom_arm, loom_x86)
    agg = aggregate(cells)

    write_csv(cells, args.out_csv)
    write_report(cells, agg, args.out_md)

    print(json.dumps({
        "n": agg["n"],
        "official_sum": agg["official_sum"],
        "loom_arm_sum": agg["loom_arm_sum"],
        "loom_x86_sum": agg["loom_x86_sum"],
        "pairwise": agg["pairwise"],
        "concordance": agg["concordance"],
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
