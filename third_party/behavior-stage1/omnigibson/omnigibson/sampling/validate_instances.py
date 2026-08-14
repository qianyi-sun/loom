"""
Post-validation script to check if task objects are placed under furniture.
This script loads instances in OmniGibson and uses the Under state (ray-casting)
to check if any task-relevant objects have furniture above them.

Usage:
    python validate_instances.py --activity picking_up_trash --start_idx 301 --end_idx 700

This will output a list of instances where objects are under furniture.
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
from omnigibson.object_states import Under
from omnigibson.object_states.aabb import AABB
from omnigibson.utils.asset_utils import get_dataset_path
from omnigibson.utils.constants import STRUCTURE_CATEGORIES
from omnigibson.utils.sampling_utils import raytest

parser = argparse.ArgumentParser()
parser.add_argument("--scene_model", type=str, default="house_double_floor_lower")
parser.add_argument("--activity", type=str, required=True)
parser.add_argument("--start_idx", type=int, default=301)
parser.add_argument("--end_idx", type=int, default=700)
parser.add_argument("--output_file", type=str, default=None,
                    help="Output file for validation results (JSON)")

# Load task custom lists for room types
with open(os.path.join(os.path.dirname(__file__), "task_custom_lists.json"), "r") as f:
    TASK_CUSTOM_LISTS = json.load(f)


# Height difference threshold for stairs - objects with height difference > this are considered reachable
STAIRS_HEIGHT_THRESHOLD = 1.8  # meters
MAX_RAYCAST_DISTANCE = 5.0  # meters (same as adjacency.py MAX_DISTANCE_VERTICAL)


def get_height_above_object(task_obj, furniture):
    """
    Cast a ray upward from the task object to find the actual height of the furniture
    directly above the object. This handles cases like stairs where the height varies
    along the structure.

    Args:
        task_obj: The task object to check from
        furniture: The furniture object above

    Returns:
        float or None: Height difference between the hit point and task object, or None if no hit
    """
    # Get task object's AABB center as the ray start point
    if hasattr(task_obj, 'states') and AABB in task_obj.states:
        aabb_lower, aabb_higher = task_obj.states[AABB].get_value()
        start_pos = (aabb_lower + aabb_higher) / 2.0
    else:
        start_pos = task_obj.get_position()

    # Cast ray upward
    end_pos = start_pos.clone()
    end_pos[2] += MAX_RAYCAST_DISTANCE

    # Get the furniture's link paths to check for hits
    furniture_link_paths = set(furniture.link_prim_paths)

    # Cast ray and find hit on the specific furniture
    results = raytest(
        start_point=start_pos,
        end_point=end_pos,
        only_closest=False,
    )

    if not isinstance(results, list):
        results = [results]

    for result in results:
        if result.get("hit", False):
            # Check if this hit is on our target furniture
            rigid_body = result.get("rigidBody", "")
            if rigid_body in furniture_link_paths:
                hit_pos = result["position"]
                height_diff = hit_pos[2] - start_pos[2].item()
                return height_diff

    return None


def validate_objects_not_under_furniture(env, task_objects):
    """
    Check if any task-relevant objects are under furniture using the Under state.
    Excludes structural elements like ceilings, walls, floors, etc.
    Also excludes robots and applies height-based filtering for stairs.

    Args:
        env: The OmniGibson environment
        task_objects: List of task-relevant objects to check

    Returns:
        tuple: (is_valid, flagged_objects) - True if no objects are under furniture,
               flagged_objects is a list of dicts with object info
    """
    flagged_objects = []

    # Get all scene objects that are not task objects and not structural elements
    # STRUCTURE_CATEGORIES includes: floors, walls, ceilings, lawn, driveway, fence, roof, background
    # Also exclude robots (they have 'robot' in their name or category)
    scene_furniture = [
        obj for obj in env.scene.objects
        if (obj not in task_objects and
            hasattr(obj, 'states') and
            Under in obj.states and
            hasattr(obj, 'category') and
            obj.category not in STRUCTURE_CATEGORIES and
            not obj.category.startswith('agent') and
            'robot' not in obj.name.lower())
    ]

    for task_obj in task_objects:
        if not hasattr(task_obj, 'states') or Under not in task_obj.states:
            continue

        for furniture in scene_furniture:
            try:
                # Check if task_obj is under furniture
                if task_obj.states[Under].get_value(furniture):
                    task_obj_pos = task_obj.get_position()
                    furniture_pos = furniture.get_position()
                    height_diff = None

                    # Height-based filtering for stairs using raycast to get actual height
                    # The stairs are like a ramp, so we need the height at the object's position
                    if 'stair' in furniture.category.lower() or 'stair' in furniture.name.lower():
                        height_diff = get_height_above_object(task_obj, furniture)
                        if height_diff is not None and height_diff > STAIRS_HEIGHT_THRESHOLD:
                            # Object is in open space beneath stairs, not trapped
                            continue

                    # Calculate height difference if not already computed (for non-stairs)
                    if height_diff is None:
                        height_diff = float(furniture_pos[2] - task_obj_pos[2])
                    else:
                        height_diff = float(height_diff)  # Ensure it's a Python float

                    flagged_objects.append({
                        "object": task_obj.name,
                        "position": task_obj_pos.tolist(),
                        "under_furniture": furniture.name,
                        "furniture_position": furniture_pos.tolist(),
                        "height_diff": height_diff
                    })
                    break  # Only report once per task object
            except Exception:
                # Skip if the check fails (e.g., cloth objects)
                continue

    return len(flagged_objects) == 0, flagged_objects


def main():
    args = parser.parse_args()

    # Path to TRO state files
    tro_dir = os.path.join(
        "/home/user/BEHAVIOR-1K/datasets/2025-challenge-task-instances/scenes",
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

    # Initialize OmniGibson environment (similar to multiply_b1k_tasks_random_pose.py)
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
                "type": "R1",
                "obs_modalities": ["rgb"],
                "grasping_mode": "physical",
                "default_arm_pose": "diagonal30",
                "default_reset_mode": "untuck",
                "position": np.ones(3) * -50.0,  # Place robot far away
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

    # Store initial scene state to reset between instances
    initial_state = og.sim.dump_state()

    all_results = []
    invalid_instances = []
    valid_instances = []

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

        # Validate using Under state
        is_valid, flagged_objects = validate_objects_not_under_furniture(env, task_objects)

        result = {
            "file": tro_path,
            "instance_id": instance_id,
            "valid": is_valid,
            "flagged_objects": flagged_objects
        }
        all_results.append(result)

        if is_valid:
            valid_instances.append(instance_id)
        else:
            invalid_instances.append(instance_id)
            print(f"Instance {instance_id}: INVALID - Objects under furniture:")
            for flagged in flagged_objects:
                print(f"  - {flagged['object']} at [{flagged['position'][0]:.2f}, {flagged['position'][1]:.2f}] under {flagged['under_furniture']}")

    # Summary
    print(f"\n{'='*50}")
    print(f"Validation Summary for {args.activity}")
    print(f"{'='*50}")
    print(f"Total instances checked: {len(all_results)}")
    print(f"Valid instances: {len(valid_instances)}")
    print(f"Invalid instances: {len(invalid_instances)}")

    if invalid_instances:
        print(f"\nInvalid instance IDs: {invalid_instances}")

    # Save results if output file specified
    if args.output_file:
        output_data = {
            "activity": args.activity,
            "scene_model": args.scene_model,
            "total_checked": len(all_results),
            "valid_count": len(valid_instances),
            "invalid_count": len(invalid_instances),
            "valid_instances": valid_instances,
            "invalid_instances": invalid_instances,
            "detailed_results": all_results
        }
        with open(args.output_file, "w") as f:
            json.dump(output_data, f, indent=2)
        print(f"\nResults saved to: {args.output_file}")

    # Shutdown
    og.shutdown()


if __name__ == "__main__":
    main()
