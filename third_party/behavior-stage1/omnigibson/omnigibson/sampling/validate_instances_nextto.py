"""
Validation script to check if task objects or robot are too close to scene furniture/walls.

Uses PhysX overlap_sphere on collision geometry at multiple radii (0.05 to --radius in 0.05
increments). An instance is marked INVALID if ANY object overlaps at ANY of these radii,
which handles sampling noise where a specific radius might miss a nearby object.

Usage:
    python validate_instances_nextto.py --activity picking_up_trash --start_idx 300 --end_idx 400 --radius 0.2
"""

import os
import argparse
import json
import numpy as np
import torch as th

# Must set macros BEFORE importing omnigibson
from omnigibson.macros import gm, macros

gm.HEADLESS = True
gm.USE_GPU_DYNAMICS = False
gm.ENABLE_FLATCACHE = True
gm.ENABLE_OBJECT_STATES = True
gm.ENABLE_TRANSITION_RULES = False

import omnigibson as og
from omnigibson.objects import DatasetObject
from omnigibson.object_states.aabb import AABB
from omnigibson.utils.constants import STRUCTURE_CATEGORIES

parser = argparse.ArgumentParser()
parser.add_argument("--scene_model", type=str, default="house_double_floor_lower")
parser.add_argument("--activity", type=str, required=True)
parser.add_argument("--start_idx", type=int, default=301)
parser.add_argument("--end_idx", type=int, default=700)
parser.add_argument("--radius", type=float, default=0.2,
                    help="Max inflate radius (meters). Checks all radii from 0.05 to this value in 0.05 increments.")
parser.add_argument("--max_surface_points", type=int, default=200,
                    help="Max number of surface points to sample per object")
parser.add_argument("--output_file", type=str, default=None,
                    help="Output file for validation results (JSON)")
parser.add_argument("--dataset_path", type=str, default=None,
                    help="Override base dataset path for TRO files (e.g. /home/user/BEHAVIOR-1K/datasets/2025-challenge-task-instances/scenes)")

# Load task custom lists for room types
with open(os.path.join(os.path.dirname(__file__), "task_custom_lists.json"), "r") as f:
    TASK_CUSTOM_LISTS = json.load(f)


def check_proximity_overlap_sphere(obj, inflate_radius, excluded_prim_prefixes, furniture_prim_map):
    """
    Check if any collision surface point of obj, inflated by inflate_radius,
    overlaps with furniture collision geometry using PhysX overlap_sphere.

    Args:
        obj: The object to check (task object or robot)
        inflate_radius: Radius to inflate each surface point (meters)
        excluded_prim_prefixes: Set of prim path prefixes to exclude (self, task objects, structural)
        furniture_prim_map: Dict mapping prim path prefix -> (furniture_name, furniture_category)

    Returns:
        tuple: (has_overlap, hit_furniture_dict)
            hit_furniture_dict: {furniture_name: {"count": int, "category": str}}
    """
    # Get collision surface points
    try:
        points = obj.collision_points_world
    except Exception:
        return False, {}

    if points is None or len(points) == 0:
        return False, {}

    # Subsample if too many points
    max_pts = 200
    if len(points) > max_pts:
        indices = th.randperm(len(points))[:max_pts]
        points = points[indices]

    # Track which furniture objects we hit
    hit_furniture = {}  # furniture_name -> count

    def make_callback(excluded_prefixes, furn_map, hits_dict):
        def overlap_callback(hit):
            body_path = hit.rigid_body
            # Skip if it's one of the excluded objects (self, task objects, robot, structural)
            for prefix in excluded_prefixes:
                if body_path.startswith(prefix):
                    return True  # continue traversal
            # Check if it belongs to a known furniture object
            for furn_prefix, (furn_name, furn_cat) in furn_map.items():
                if body_path.startswith(furn_prefix):
                    hits_dict[furn_name] = hits_dict.get(furn_name, 0) + 1
                    return True  # continue traversal to find all hits
            return True  # continue
        return overlap_callback

    callback = make_callback(excluded_prim_prefixes, furniture_prim_map, hit_furniture)

    for pt in points:
        og.sim.psqi.overlap_sphere(inflate_radius, pt.cpu().tolist(), callback, False)

    if hit_furniture:
        # Build result dict with categories
        result = {}
        for furn_name, count in hit_furniture.items():
            cat = "unknown"
            for furn_prefix, (fn, fc) in furniture_prim_map.items():
                if fn == furn_name:
                    cat = fc
                    break
            result[furn_name] = {"count": count, "category": cat}
        return True, result

    return False, {}


def compute_proximity_results(env, task_objects, robot, inflate_radii):
    """
    For each task object and robot, check proximity to furniture at multiple
    inflate radii using overlap_sphere on collision geometry.

    Args:
        env: The OmniGibson environment
        task_objects: List of task-relevant objects to check
        robot: The robot object to check
        inflate_radii: List of radii to check (meters)

    Returns:
        list[dict]: Per-object results with overlap info at each radius
    """
    # Build set of excluded objects
    excluded_objects = set(task_objects)
    if robot is not None:
        excluded_objects.add(robot)

    # Build prim prefix sets for exclusion
    excluded_prim_prefixes = set()
    for obj in excluded_objects:
        excluded_prim_prefixes.add(obj.prim_path)
    # Also exclude structural objects
    for obj in env.scene.objects:
        if hasattr(obj, 'category') and (
            obj.category in STRUCTURE_CATEGORIES or
            obj.category.startswith('agent')
        ):
            excluded_prim_prefixes.add(obj.prim_path)

    # Build furniture prim map: prim_path_prefix -> (name, category)
    furniture_prim_map = {}
    for obj in env.scene.objects:
        if (obj not in excluded_objects and
            hasattr(obj, 'category') and
            obj.category not in STRUCTURE_CATEGORIES and
            not obj.category.startswith('agent') and
            'robot' not in obj.name.lower()):
            furniture_prim_map[obj.prim_path] = (obj.name, getattr(obj, 'category', 'unknown'))

    # Objects to check: task objects + robot
    objects_to_check = []
    for obj in task_objects:
        objects_to_check.append(("task_object", obj))
    if robot is not None:
        objects_to_check.append(("robot", robot))

    results = []
    for obj_type, obj in objects_to_check:
        obj_pos = obj.get_position_orientation()[0].tolist()
        item = {
            "type": obj_type,
            "object": obj.name,
            "position": obj_pos,
            "overlaps": {},  # radius -> {"has_overlap": bool, "furniture": str, "category": str}
        }
        if obj_type == "task_object":
            item["object_category"] = getattr(obj, 'category', 'unknown')

        all_hit_furniture = {}  # furniture_name -> category (union across all radii)
        per_radius_overlaps = {}
        for radius in inflate_radii:
            has_overlap, hit_dict = check_proximity_overlap_sphere(
                obj, radius, excluded_prim_prefixes, furniture_prim_map
            )
            per_radius_overlaps[f"{radius:.2f}"] = {
                "has_overlap": has_overlap,
                "hit_furniture": {fn: info["category"] for fn, info in hit_dict.items()},
            }
            for fn, info in hit_dict.items():
                all_hit_furniture[fn] = info["category"]

        item["overlaps"] = per_radius_overlaps
        item["has_any_overlap"] = len(all_hit_furniture) > 0
        item["all_hit_furniture"] = all_hit_furniture  # union across all radii

        results.append(item)

    return results



def main():
    args = parser.parse_args()

    # Path to TRO state files
    base_path = args.dataset_path if args.dataset_path else "/home/user/BEHAVIOR-1K/datasets/behavior-1k-assets/scenes"
    tro_dir = os.path.join(
        base_path,
        args.scene_model,
        "json",
        f"{args.scene_model}_task_{args.activity}_instances"
    )

    if not os.path.exists(tro_dir):
        print(f"Error: Directory not found: {tro_dir}")
        return

    # Get room types for this activity
    if args.activity not in TASK_CUSTOM_LISTS:
        print(f"Error: Activity {args.activity} not found in task_custom_lists.json")
        return
    room_types = TASK_CUSTOM_LISTS[args.activity]["room_types"]

    # Initialize OmniGibson environment
    cfg = {
        "env": {
            "action_frequency": 30,
            "physics_frequency": 120,
        },
        "scene": {
            "type": "InteractiveTraversableScene",
            "scene_model": args.scene_model,
            "load_room_types": room_types,
        },
        "robots": [
            {
                "type": "R1Pro",
                "obs_modalities": ["rgb"],
                "grasping_mode": "physical",
                "default_arm_pose": "diagonal30",
                "default_reset_mode": "untuck",
                "position": np.ones(3) * -50.0,  # Place robot far away initially
            },
        ],
        "task": {
            "type": "BehaviorTask",
            "online_object_sampling": False,
            "activity_name": args.activity,
            "activity_instance_id": 0,
        },
    }

    print("Initializing OmniGibson environment...")
    env = og.Environment(cfg)

    # Get robot reference
    robot = env.robots[0] if env.robots else None

    # Store initial scene state to reset between instances
    initial_state = og.sim.dump_state()

    # Build radii from 0.05 to args.radius in 0.05 increments
    num_steps = max(1, round(args.radius / 0.05))
    inflate_radii = [round(i * 0.05, 2) for i in range(1, num_steps + 1)]
    print(f"Checking radii: {inflate_radii}")

    # Collect per-instance results
    all_instance_results = []  # list of (instance_id, proximity_results)

    for instance_id in range(args.start_idx, args.end_idx + 1):
        tro_filename = f"{args.scene_model}_task_{args.activity}_0_{instance_id}_template-tro_state.json"
        tro_path = os.path.join(tro_dir, tro_filename)

        if not os.path.exists(tro_path):
            continue

        # Reset to initial state
        og.sim.load_state(initial_state)
        og.sim.step()

        # Load TRO state
        with open(tro_path, "r") as f:
            tro_data = json.load(f)

        # Apply TRO state to task objects
        task_objects = []
        for tro_key, tro_state in tro_data.items():
            if tro_key == "robot_poses":
                # Apply robot pose - format is {"R1Pro": [{"position": [...], "orientation": [...]}], ...}
                if robot is not None and isinstance(tro_state, dict):
                    robot_type = robot.__class__.__name__
                    if robot_type in tro_state:
                        pose_list = tro_state[robot_type]
                        if isinstance(pose_list, list) and len(pose_list) > 0:
                            pose_data = pose_list[0]
                            pos = pose_data.get("position", None)
                            ori = pose_data.get("orientation", None)
                            if pos is not None and ori is not None:
                                robot.set_position_orientation(position=pos, orientation=ori)
                continue

            if tro_key.startswith("floor."):
                continue

            # Find the object in the task's object scope
            if tro_key in env.task.object_scope:
                obj_entity = env.task.object_scope[tro_key]
                if hasattr(obj_entity, 'wrapped_obj') and isinstance(obj_entity.wrapped_obj, DatasetObject):
                    obj = obj_entity.wrapped_obj
                    # Load the state for this object
                    if isinstance(tro_state, dict) and "root_link" in tro_state:
                        pos = tro_state["root_link"].get("pos", None)
                        ori = tro_state["root_link"].get("ori", None)
                        if pos is not None and ori is not None:
                            obj.set_position_orientation(position=pos, orientation=ori)
                            task_objects.append(obj)

        # Step physics to settle
        for _ in range(5):
            og.sim.step()

        # Compute proximity using overlap_sphere at all radii
        proximity_results = compute_proximity_results(env, task_objects, robot, inflate_radii)
        all_instance_results.append((instance_id, proximity_results))

        # Print per-instance results (union across all radii)
        for pr in proximity_results:
            status = "OVERLAP" if pr["has_any_overlap"] else "OK"
            furns = ", ".join(f"{fn} ({cat})" for fn, cat in pr["all_hit_furniture"].items()) or "none"
            print(f"  Instance {instance_id} [{pr['type']}] {pr['object']}: {status} -> {furns}")

    # Summary using union-of-radii logic
    print(f"\n{'='*70}")
    print(f"Proximity Validation Summary for {args.activity}")
    print(f"Radii checked: {inflate_radii}")
    print(f"Instances checked: {len(all_instance_results)}")
    print(f"{'='*70}")

    valid_ids = []
    invalid_ids = []
    invalid_details = {}  # instance_id -> list of (obj_type, obj_name, {furniture: category})

    for instance_id, prox_results in all_instance_results:
        instance_invalid = False
        details = []
        for pr in prox_results:
            if pr["has_any_overlap"]:
                instance_invalid = True
                details.append((pr["type"], pr["object"], pr["all_hit_furniture"]))
        if instance_invalid:
            invalid_ids.append(instance_id)
            invalid_details[instance_id] = details
        else:
            valid_ids.append(instance_id)

    print(f"\n  VALID:   {len(valid_ids)} instances")
    print(f"  INVALID: {len(invalid_ids)} instances")
    print(f"\n  Valid IDs: {valid_ids}")
    print(f"  Invalid IDs: {invalid_ids}")

    # Print detailed invalid info
    for iid in invalid_ids:
        reasons = []
        for obj_type, obj_name, furns in invalid_details[iid]:
            for fn, cat in furns.items():
                reasons.append(f"[{obj_type}] {obj_name} -> {fn} ({cat})")
        print(f"\n  Instance {iid}: INVALID")
        for r in reasons:
            print(f"    - {r}")

    # Save results if output file specified
    if args.output_file:
        output_data = {
            "activity": args.activity,
            "scene_model": args.scene_model,
            "validation_type": "collision_proximity_union",
            "radius": args.radius,
            "inflate_radii": [f"{r:.2f}" for r in inflate_radii],
            "total_checked": len(all_instance_results),
            "valid_count": len(valid_ids),
            "invalid_count": len(invalid_ids),
            "valid_instances": valid_ids,
            "invalid_instances": invalid_ids,
            "invalid_details": {
                str(iid): [
                    {"type": obj_type, "object": obj_name, "furniture": furns}
                    for obj_type, obj_name, furns in details
                ]
                for iid, details in invalid_details.items()
            },
            "per_instance_results": [
                {"instance_id": iid, "results": res}
                for iid, res in all_instance_results
            ],
        }
        with open(args.output_file, "w") as f:
            json.dump(output_data, f, indent=2)
        print(f"\nResults saved to: {args.output_file}")

    # Shutdown
    og.shutdown()


if __name__ == "__main__":
    main()
