"""
Replace invalid old instances in 2025-challenge-task-instances with valid new instances.

Priority order for replacement sources:
1. Same-ID valid new instance (from behavior-1k-assets)
2. Valid spare instance from 900-950 range (renamed)
3. "Free" valid new instance whose ID has a successful rollout (renamed)

Instances with successful rollouts are NEVER touched.

Usage:
    python replace_invalid_instances.py --dry_run      # preview what would be replaced
    python replace_invalid_instances.py                 # execute replacements
"""

import json
import os
import shutil
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--dry_run", action="store_true", help="Preview replacements without copying files")
parser.add_argument("--old_validation", type=str, default="/tmp/validation_old_350_800.json")
parser.add_argument("--new_validation", type=str, default="/tmp/validation_new_350_800.json")
parser.add_argument("--spare_validation", type=str, default="/tmp/validation_results_900_950.json")
parser.add_argument("--successful_rollouts", type=str,
                    default=os.path.join(os.path.dirname(__file__), "successful_rollout_instances.txt"))
parser.add_argument("--start_idx", type=int, default=351)
parser.add_argument("--end_idx", type=int, default=800)
parser.add_argument("--scene_model", type=str, default="house_double_floor_lower")
parser.add_argument("--activity", type=str, default="picking_up_trash")
parser.add_argument("--report_file", type=str, default="/tmp/replacement_report.json")


def load_validation(path):
    """Load validation JSON and return (set of valid IDs, set of invalid IDs)."""
    with open(path, "r") as f:
        data = json.load(f)
    valid = set(data["valid_instances"])
    invalid = set(data["invalid_instances"])
    return valid, invalid


def load_successful_rollouts(path):
    """Load successful rollout instance IDs from txt file."""
    ids = set()
    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if line:
                # Handle format "302" or with line numbers like "1\t302"
                parts = line.split()
                ids.add(int(parts[-1]))
    return ids


def make_filename(scene_model, activity, instance_id):
    """Build the TRO state filename for a given instance ID."""
    return f"{scene_model}_task_{activity}_0_{instance_id}_template-tro_state.json"


def main():
    args = parser.parse_args()

    base_datasets = "/home/user/BEHAVIOR-1K/datasets"
    old_dir = os.path.join(
        base_datasets, "2025-challenge-task-instances", "scenes",
        args.scene_model, "json",
        f"{args.scene_model}_task_{args.activity}_instances"
    )
    new_dir = os.path.join(
        base_datasets, "behavior-1k-assets", "scenes",
        args.scene_model, "json",
        f"{args.scene_model}_task_{args.activity}_instances"
    )
    backup_dir = os.path.join(old_dir, "backup")

    # 1. Load data
    old_valid, old_invalid = load_validation(args.old_validation)
    new_valid, new_invalid = load_validation(args.new_validation)
    spare_valid, spare_invalid = load_validation(args.spare_validation)
    successful_ids = load_successful_rollouts(args.successful_rollouts)

    # Filter to the target range
    range_ids = set(range(args.start_idx, args.end_idx + 1))
    old_invalid_in_range = old_invalid & range_ids
    successful_in_range = successful_ids & range_ids
    old_valid_in_range = old_valid & range_ids

    print(f"Range: {args.start_idx}-{args.end_idx}")
    print(f"Old invalid in range: {len(old_invalid_in_range)}")
    print(f"Old valid in range: {len(old_valid_in_range)}")
    print(f"Successful rollouts in range: {len(successful_in_range)}")
    print(f"Successful rollouts that are old-invalid: {len(old_invalid_in_range & successful_ids)}")
    print()

    # 2. Determine which instances need replacement
    # Invalid AND no successful rollout
    needs_replacement = sorted(old_invalid_in_range - successful_ids)
    print(f"Instances needing replacement: {len(needs_replacement)}")

    # 3. Build source pools
    # Spare pool: valid 900-950 instances
    spare_pool = sorted(spare_valid)

    # Free pool: valid new instances whose IDs have successful rollouts
    # (their content is "free" because the old instance at that ID is preserved)
    free_pool = sorted(new_valid & successful_ids & range_ids)

    print(f"Spare pool (900-950 valid): {len(spare_pool)}")
    print(f"Free pool (valid new + has rollout): {len(free_pool)}")
    print()

    # 4. Assign sources
    replacements = {}  # target_id -> (source_id, source_type)
    unfilled = []

    for target_id in needs_replacement:
        if target_id in new_valid:
            replacements[target_id] = (target_id, "same_id_new")
        elif spare_pool:
            source_id = spare_pool.pop(0)
            replacements[target_id] = (source_id, "spare_900_950")
        elif free_pool:
            source_id = free_pool.pop(0)
            replacements[target_id] = (source_id, "free_new")
        else:
            unfilled.append(target_id)

    # Count by source type
    same_id_count = sum(1 for _, (_, t) in replacements.items() if t == "same_id_new")
    spare_count = sum(1 for _, (_, t) in replacements.items() if t == "spare_900_950")
    free_count = sum(1 for _, (_, t) in replacements.items() if t == "free_new")

    print(f"=== Replacement Plan ===")
    print(f"  Same-ID new (valid):  {same_id_count}")
    print(f"  Spare (900-950):      {spare_count}")
    print(f"  Free (new+rollout):   {free_count}")
    print(f"  Total replacements:   {len(replacements)}")
    print(f"  Unfilled:             {len(unfilled)}")
    if unfilled:
        print(f"  Unfilled IDs: {unfilled}")
    print()

    # Print detailed replacement plan
    for target_id in sorted(replacements.keys()):
        source_id, source_type = replacements[target_id]
        if source_type == "same_id_new":
            print(f"  {target_id} <- new:{source_id} (same ID)")
        elif source_type == "spare_900_950":
            print(f"  {target_id} <- spare:{source_id} (900-950)")
        else:
            print(f"  {target_id} <- free:{source_id} (new+rollout)")

    if args.dry_run:
        print(f"\n[DRY RUN] No files were copied.")
        return

    # 5. Execute copies
    os.makedirs(backup_dir, exist_ok=True)
    copied = 0
    errors = []

    for target_id in sorted(replacements.keys()):
        source_id, source_type = replacements[target_id]

        target_filename = make_filename(args.scene_model, args.activity, target_id)
        source_filename = make_filename(args.scene_model, args.activity, source_id)

        target_path = os.path.join(old_dir, target_filename)
        source_path = os.path.join(new_dir, source_filename)
        backup_path = os.path.join(backup_dir, target_filename)

        if not os.path.exists(source_path):
            errors.append(f"Source not found: {source_path}")
            continue

        # Backup original
        if os.path.exists(target_path):
            shutil.copy2(target_path, backup_path)

        # Copy source to target (rename if IDs differ)
        shutil.copy2(source_path, target_path)

        # If source_id != target_id, we need to rename the content inside
        # Actually, the filename itself IS the rename - the JSON content
        # references object positions, not instance IDs, so no content change needed.
        # But the filename must match the target ID.
        # shutil.copy2 already copied to the target filename, so we're good.

        copied += 1
        print(f"  Copied: {source_filename} -> {target_filename}")

    print(f"\n=== Done ===")
    print(f"  Files copied: {copied}")
    print(f"  Errors: {len(errors)}")
    for e in errors:
        print(f"    {e}")

    # 6. Save report
    report = {
        "range": [args.start_idx, args.end_idx],
        "total_in_range": len(range_ids & (old_valid | old_invalid)),
        "old_valid": len(old_valid_in_range),
        "old_invalid": len(old_invalid_in_range),
        "successful_rollouts_in_range": sorted(successful_in_range),
        "needs_replacement": len(needs_replacement),
        "replaced": copied,
        "unfilled": unfilled,
        "source_breakdown": {
            "same_id_new": same_id_count,
            "spare_900_950": spare_count,
            "free_new": free_count,
        },
        "replacements": {
            str(tid): {"source_id": sid, "source_type": stype}
            for tid, (sid, stype) in sorted(replacements.items())
        },
        "errors": errors,
    }
    with open(args.report_file, "w") as f:
        json.dump(report, f, indent=2)
    print(f"Report saved to: {args.report_file}")


if __name__ == "__main__":
    main()
