"""Build a repo-only SkillLearnBench three-way dissent replay plan.

This helper consumes the checked-in three-way comparison CSV produced by
``skilllearnbench_three_way_compare.py``. It does not query Loom, Docker,
providers, databases, artifact stores, or live infrastructure.

The output is a deterministic triage plan for rows that need later replay or
inspection, with labels that describe the next evidence to gather rather than
claiming a root cause from score deltas alone.

Refs: #6 #32 #110.
"""

from __future__ import annotations

import argparse
import csv
import json
import shlex
from collections import Counter
from collections.abc import Sequence
from pathlib import Path
from typing import Any

SCHEMA_VERSION = "slb-three-way-replay-plan-v1"

REQUIRED_COLUMNS = {
    "task_id",
    "official_reward",
    "loom_arm_reward",
    "loom_x86_reward",
    "concordance",
    "loom_arm_arch",
    "loom_x86_arch",
    "loom_arm_failure",
    "loom_x86_failure",
}


def _reward(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _format_reward(value: float | None) -> str:
    return "" if value is None else f"{value:.1f}"


def _nonempty(value: Any) -> bool:
    return str(value or "").strip() != ""


def _read_rows(csv_path: Path) -> list[dict[str, str]]:
    with csv_path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        missing = REQUIRED_COLUMNS - set(reader.fieldnames or [])
        if missing:
            columns = ", ".join(sorted(missing))
            raise ValueError(f"{csv_path} is missing required columns: {columns}")
        return [dict(row) for row in reader]


def _safe_next_commands(csv_path: Path, task_id: str) -> list[str]:
    script = "scripts/alignment/skilllearnbench_three_way_replay_plan.py"
    return [
        "rg -n --fixed-strings -- "
        f"{shlex.quote(task_id)} {shlex.quote(str(csv_path))}",
        "uv run python "
        f"{script} --three-way-csv {shlex.quote(str(csv_path))} "
        f"--task-id {shlex.quote(task_id)} --out-md /tmp/slb-replay-plan.md",
    ]


def _base_evidence_requirements() -> list[str]:
    return [
        "Collect official output, Loom artifacts, verifier stdout/stderr, "
        "required_files or artifact manifest, and reward JSON for this task.",
        "Record provider/model metadata and trial/reference ids without secrets.",
    ]


def _classify_row(row: dict[str, str]) -> tuple[str, str, str, list[str]]:
    official = _reward(row.get("official_reward"))
    arm = _reward(row.get("loom_arm_reward"))
    x86 = _reward(row.get("loom_x86_reward"))
    concordance = row.get("concordance", "")
    arm_failure = row.get("loom_arm_failure")
    x86_failure = row.get("loom_x86_failure")

    if (
        official is None
        or arm is None
        or x86 is None
        or concordance == "incomplete"
        or _nonempty(arm_failure)
        or _nonempty(x86_failure)
    ):
        return (
            "incomplete_or_missing_evidence",
            "missing_or_failed_leg",
            (
                "One or more rewards or Loom failure fields are missing/non-empty; "
                "do not interpret the row as score semantics until evidence is complete."
            ),
            [
                "Recover the missing numeric rewards or failure classification for every leg.",
                "Preserve per-leg trial/reference ids and failure diagnostics without secrets.",
            ],
        )

    if concordance == "loom_agrees_official_dissents":
        if arm == x86 and official > arm:
            return (
                "likely_verifier_artifact_replay_needed",
                "loom_zero_official_one",
                (
                    "Both Loom legs scored lower than official. This is a replay target "
                    "for artifact collection, verifier inputs, and required-output handling."
                ),
                [
                    *_base_evidence_requirements(),
                    "Replay the Loom final output/artifact bundle through the official verifier when authorized.",
                    "Inspect whether required outputs were generated, collected, and passed to the verifier.",
                ],
            )
        if arm == x86 and arm > official:
            return (
                "official_semantics_drift_candidate",
                "loom_one_official_zero",
                (
                    "Both Loom legs scored higher than official. Treat as a semantics-drift "
                    "candidate until identical outputs/artifacts are replayed through both verifiers."
                ),
                [
                    *_base_evidence_requirements(),
                    "Replay the official final output/artifact bundle through the Loom verifier when authorized.",
                    "Compare task instructions, verifier assumptions, and denominator/reward parsing.",
                ],
            )
        return (
            "likely_verifier_artifact_replay_needed",
            "loom_agrees_official_differs",
            (
                "Both Loom legs agree and official differs, but the reward direction is not "
                "the common binary 0/1 pattern; replay before assigning root cause."
            ),
            _base_evidence_requirements(),
        )

    if concordance in {"arm_dissents", "x86_dissents"}:
        focus = "arm" if concordance == "arm_dissents" else "x86"
        return (
            "architecture_specific_rerun_needed",
            focus,
            (
                f"The {focus} leg disagrees while the other Loom leg matches official; "
                "future validation should rerun or replay this task on that architecture."
            ),
            [
                f"Collect a fresh same-task {focus} rerun when live validation is authorized.",
                "Record worker architecture, image/source revision, verifier inputs, artifacts, and reward JSON.",
            ],
        )

    if concordance == "all_three_dissent":
        return (
            "manual_replay_review_needed",
            "all_three_dissent",
            "All three legs differ; classify only after per-leg outputs and verifier inputs are inspected.",
            _base_evidence_requirements(),
        )

    return (
        "three_way_match_no_replay",
        "none",
        "All three legs agree in the checked-in CSV.",
        ["No replay needed unless this task is selected as a control row."],
    )


def build_replay_plan(
    csv_path: Path,
    *,
    include_matches: bool = False,
    task_ids: set[str] | None = None,
) -> dict[str, Any]:
    rows = _read_rows(csv_path)
    concordance_counts = Counter(row.get("concordance", "") for row in rows)
    planned_rows: list[dict[str, Any]] = []
    category_counts: Counter[str] = Counter()

    for row in rows:
        task_id = row["task_id"]
        if task_ids is not None and task_id not in task_ids:
            continue
        category, replay_focus, reason, evidence_requirements = _classify_row(row)
        if category == "three_way_match_no_replay" and not include_matches:
            continue

        official = _reward(row.get("official_reward"))
        arm = _reward(row.get("loom_arm_reward"))
        x86 = _reward(row.get("loom_x86_reward"))
        category_counts[category] += 1
        planned_rows.append(
            {
                "task_id": task_id,
                "concordance": row.get("concordance", ""),
                "official_reward": official,
                "loom_arm_reward": arm,
                "loom_x86_reward": x86,
                "loom_arm_arch": row.get("loom_arm_arch", ""),
                "loom_x86_arch": row.get("loom_x86_arch", ""),
                "loom_arm_failure": row.get("loom_arm_failure", ""),
                "loom_x86_failure": row.get("loom_x86_failure", ""),
                "category": category,
                "replay_focus": replay_focus,
                "reason": reason,
                "evidence_requirements": evidence_requirements,
                "safe_next_commands": _safe_next_commands(csv_path, task_id),
            },
        )

    return {
        "schema_version": SCHEMA_VERSION,
        "source_csv": str(csv_path),
        "summary": {
            "total_rows": len(rows),
            "planned_rows": len(planned_rows),
            "concordance_counts": dict(sorted(concordance_counts.items())),
            "category_counts": dict(sorted(category_counts.items())),
            "caveat": (
                "Classifications are deterministic triage labels from checked-in CSV evidence; "
                "they are not root-cause conclusions without replay artifacts."
            ),
        },
        "rows": planned_rows,
    }


def write_json(plan: dict[str, Any], path: Path) -> None:
    path.write_text(json.dumps(plan, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_markdown(plan: dict[str, Any], path: Path) -> None:
    lines: list[str] = []
    summary = plan["summary"]
    lines.append("# SkillLearnBench three-way dissent replay plan")
    lines.append("")
    lines.append(
        "Repo-only triage generated from the checked-in three-way CSV. "
        "It does not run providers, Docker, databases, artifact stores, or live infrastructure.",
    )
    lines.append("")
    lines.append(f"- Source CSV: `{plan['source_csv']}`")
    lines.append(f"- Total rows: **{summary['total_rows']}**")
    lines.append(f"- Planned rows: **{summary['planned_rows']}**")
    lines.append(f"- Caveat: {summary['caveat']}")
    lines.append("")
    lines.append("## Category counts")
    lines.append("")
    lines.append("| Category | Count |")
    lines.append("|---|---:|")
    for category, count in summary["category_counts"].items():
        lines.append(f"| {category} | {count} |")
    lines.append("")
    lines.append("## Replay rows")
    lines.append("")
    lines.append("| Task | Concordance | Official | Loom ARM | Loom x86 | Category |")
    lines.append("|---|---|---:|---:|---:|---|")
    for row in plan["rows"]:
        lines.append(
            "| {task_id} | {concordance} | {official} | {arm} | {x86} | {category} |".format(
                task_id=row["task_id"],
                concordance=row["concordance"],
                official=_format_reward(row["official_reward"]),
                arm=_format_reward(row["loom_arm_reward"]),
                x86=_format_reward(row["loom_x86_reward"]),
                category=row["category"],
            ),
        )
    lines.append("")
    lines.append("## Replay evidence requirements")
    lines.append("")
    for row in plan["rows"]:
        lines.append(f"### {row['task_id']}")
        lines.append("")
        lines.append(f"- Category: `{row['category']}`")
        lines.append(f"- Reason: {row['reason']}")
        for requirement in row["evidence_requirements"]:
            lines.append(f"- Evidence: {requirement}")
        for command in row["safe_next_commands"]:
            lines.append(f"- Safe command: `{command}`")
        lines.append("")

    path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Build a repo-only replay plan from a SkillLearnBench three-way CSV.",
    )
    parser.add_argument("--three-way-csv", required=True, type=Path)
    parser.add_argument("--out-json", type=Path)
    parser.add_argument("--out-md", type=Path)
    parser.add_argument(
        "--include-matches",
        action="store_true",
        help="Include three_way_match rows as no-replay control rows.",
    )
    parser.add_argument(
        "--task-id",
        action="append",
        default=None,
        help="Limit output to one task_id. May be repeated.",
    )
    args = parser.parse_args(argv)

    plan = build_replay_plan(
        args.three_way_csv,
        include_matches=args.include_matches,
        task_ids=set(args.task_id) if args.task_id else None,
    )

    if args.out_json is not None:
        write_json(plan, args.out_json)
    if args.out_md is not None:
        write_markdown(plan, args.out_md)
    if args.out_json is None and args.out_md is None:
        print(json.dumps(plan, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
