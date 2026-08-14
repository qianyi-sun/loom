"""
Modified version of multiply_b1k_tasks.py that samples different robot positions for each instance.
This script creates task instances with unique robot starting positions by sampling from the traversable map.
"""
import os
import argparse
import omnigibson as og
from omnigibson.macros import gm, macros
import json
from omnigibson.objects import DatasetObject
from omnigibson.utils.asset_utils import get_dataset_path
from omnigibson.object_states import Under
from omnigibson.object_states.aabb import AABB
from omnigibson.utils.constants import STRUCTURE_CATEGORIES
from omnigibson.utils.sampling_utils import raytest
import math
import cv2
import numpy as np
import torch as th
from utils import validate_task


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
        tuple: (is_valid, error_msg) - True if no objects are under furniture, False with error message otherwise
    """
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

                    return False, f"Object {task_obj.name} is under {furniture.name} (height_diff: {height_diff:.2f}m)"
            except Exception:
                # Skip if the check fails (e.g., cloth objects)
                continue

    return True, None


def _check_overlap_sphere(obj, inflate_radius, excluded_prim_prefixes, furniture_prim_map):
    """
    Check if any collision surface point of obj, inflated by inflate_radius,
    overlaps with furniture collision geometry using PhysX overlap_sphere.

    Args:
        obj: The object to check (task object or robot)
        inflate_radius: Radius to inflate each surface point (meters)
        excluded_prim_prefixes: Set of prim path prefixes to exclude
        furniture_prim_map: Dict mapping prim path prefix -> (furniture_name, furniture_category)

    Returns:
        tuple: (has_overlap, hit_furniture_names)
    """
    try:
        points = obj.collision_points_world
    except Exception:
        return False, []

    if points is None or len(points) == 0:
        return False, []

    # Subsample if too many points
    max_pts = 200
    if len(points) > max_pts:
        indices = th.randperm(len(points))[:max_pts]
        points = points[indices]

    hit_furniture = {}

    def make_callback(excluded_prefixes, furn_map, hits_dict):
        def overlap_callback(hit):
            body_path = hit.rigid_body
            for prefix in excluded_prefixes:
                if body_path.startswith(prefix):
                    return True
            for furn_prefix, (furn_name, furn_cat) in furn_map.items():
                if body_path.startswith(furn_prefix):
                    hits_dict[furn_name] = furn_cat
                    return True
            return True
        return overlap_callback

    callback = make_callback(excluded_prim_prefixes, furniture_prim_map, hit_furniture)

    for pt in points:
        og.sim.psqi.overlap_sphere(inflate_radius, pt.cpu().tolist(), callback, False)

    if hit_furniture:
        return True, list(hit_furniture.keys())
    return False, []


def _build_proximity_maps(env, task_objects):
    """
    Build the excluded prim prefixes and furniture prim map for proximity checks.

    Returns:
        tuple: (excluded_prim_prefixes, furniture_prim_map)
    """
    excluded_objects = set(task_objects)
    # Also add robot
    for robot in env.robots:
        excluded_objects.add(robot)

    excluded_prim_prefixes = set()
    for obj in excluded_objects:
        excluded_prim_prefixes.add(obj.prim_path)
    # Exclude structural objects
    for obj in env.scene.objects:
        if hasattr(obj, 'category') and (
            obj.category in STRUCTURE_CATEGORIES or
            obj.category.startswith('agent')
        ):
            excluded_prim_prefixes.add(obj.prim_path)

    furniture_prim_map = {}
    for obj in env.scene.objects:
        if (obj not in excluded_objects and
            hasattr(obj, 'category') and
            obj.category not in STRUCTURE_CATEGORIES and
            not obj.category.startswith('agent') and
            'robot' not in obj.name.lower()):
            furniture_prim_map[obj.prim_path] = (obj.name, getattr(obj, 'category', 'unknown'))

    return excluded_prim_prefixes, furniture_prim_map


def validate_objects_proximity(env, task_objects, inflate_radii):
    """
    Check if any task objects are too close to scene furniture using
    PhysX overlap_sphere at multiple radii (union logic).

    Args:
        env: The OmniGibson environment
        task_objects: List of task-relevant objects to check
        inflate_radii: List of radii to check (meters)

    Returns:
        tuple: (is_valid, error_msg)
    """
    excluded_prim_prefixes, furniture_prim_map = _build_proximity_maps(env, task_objects)

    for obj in task_objects:
        for radius in inflate_radii:
            has_overlap, hit_names = _check_overlap_sphere(
                obj, radius, excluded_prim_prefixes, furniture_prim_map
            )
            if has_overlap:
                furns = ", ".join(hit_names)
                return False, f"Object {obj.name} overlaps furniture at radius {radius:.2f}m: {furns}"

    return True, None


def validate_robot_pose_proximity(env, robot, position, orientation, task_objects, inflate_radii):
    """
    Validate a single robot pose against furniture proximity using overlap_sphere.
    Moves robot to the given pose, checks, then moves robot back.

    Args:
        env: The OmniGibson environment
        robot: The robot object
        position: [x, y, z] position to check
        orientation: [x, y, z, w] quaternion orientation
        task_objects: List of task-relevant objects (excluded from checks)
        inflate_radii: List of radii to check (meters)

    Returns:
        tuple: (is_valid, error_msg)
    """
    # Move robot to pose and let it settle on the floor
    robot.set_position_orientation(position=position, orientation=orientation)
    for _ in range(10):
        og.sim.step()

    excluded_prim_prefixes, furniture_prim_map = _build_proximity_maps(env, task_objects)

    for radius in inflate_radii:
        has_overlap, hit_names = _check_overlap_sphere(
            robot, radius, excluded_prim_prefixes, furniture_prim_map
        )
        if has_overlap:
            furns = ", ".join(hit_names)
            return False, f"Robot overlaps furniture at radius {radius:.2f}m: {furns}"

    return True, None


parser = argparse.ArgumentParser()
parser.add_argument("--scene_model", type=str, default=None, help="Scene model to sample tasks in")
parser.add_argument(
    "--activity",
    type=str,
    default=None,
    help="Activity to be sampled.",
)
parser.add_argument(
    "--seed",
    type=int,
    default=0,
    help="Instance ID to use as seed",
)
parser.add_argument(
    "--start_idx",
    type=int,
    default=1,
    help="Instance ID to start (inclusive)",
)
parser.add_argument(
    "--end_idx",
    type=int,
    default=100,
    help="Instance ID to end (inclusive)",
)
parser.add_argument(
    "--partial_save",
    action="store_true",
    help="Whether to only the task-relevant object scope states instead of the entire scene json",
)
parser.add_argument(
    "--num_robot_pose_samples",
    type=int,
    default=1,
    help="Number of robot pose samples to generate per robot type per instance",
)
parser.add_argument(
    "--proximity_radius",
    type=float,
    default=0.2,
    help="Max proximity check radius (meters). Checks 0.05 to this value in 0.05 increments.",
)
parser.add_argument(
    "--erosion_radius",
    type=float,
    default=0.35,
    help="Erosion radius (meters) applied to traversability map before sampling robot poses.",
)

with open("task_custom_lists.json", "r") as f:
    TASK_CUSTOM_LISTS = json.load(f)

gm.HEADLESS = False
gm.USE_GPU_DYNAMICS = False
gm.ENABLE_FLATCACHE = True
gm.ENABLE_OBJECT_STATES = True
gm.ENABLE_TRANSITION_RULES = True

macros.systems.micro_particle_system.MICRO_PARTICLE_SYSTEM_MAX_VELOCITY = 0.5
macros.systems.macro_particle_system.MACRO_PARTICLE_SYSTEM_MAX_DENSITY = 200.0
macros.utils.object_state_utils.DEFAULT_HIGH_LEVEL_SAMPLING_ATTEMPTS = 5
macros.utils.object_state_utils.DEFAULT_LOW_LEVEL_SAMPLING_ATTEMPTS = 5

# Robot types to sample poses for
ROBOT_TYPES = ["R1Pro", "Fetch", "R1", "Stretch", "Tiago"]


def build_eroded_trav_map(env, erosion_radius, floor=0):
    """
    Build a traversability map eroded by the given radius (in meters).
    Points sampled from this map are guaranteed to be at least erosion_radius
    away from any non-traversable pixel (walls, furniture, etc.).

    Args:
        env: The OmniGibson environment
        erosion_radius (float): Erosion radius in meters
        floor (int): Floor index

    Returns:
        tuple: (eroded_map, trav_map_obj) where eroded_map is a th.Tensor and
               trav_map_obj is the TraversableMap (for coordinate conversion)
    """
    trav_map_obj = env.scene._trav_map
    trav_map = th.clone(trav_map_obj.floor_map[floor])
    radius_pixel = int(math.ceil(erosion_radius / trav_map_obj.map_resolution))
    kernel = np.ones((radius_pixel * 2 + 1, radius_pixel * 2 + 1), dtype=np.uint8)
    eroded = th.tensor(cv2.erode(trav_map.cpu().numpy(), kernel))
    return eroded, trav_map_obj


def sample_from_eroded_map(eroded_map, trav_map_obj, seg_map, valid_room_sem_ids, floor=0):
    """
    Sample a single random point from the eroded traversability map that falls
    within one of the valid room types.

    Args:
        eroded_map (th.Tensor): The eroded traversability map
        trav_map_obj: TraversableMap object (for coordinate conversion)
        seg_map: SegmentationMap object (for room type lookup)
        valid_room_sem_ids (set): Set of valid room semantic IDs
        floor (int): Floor index

    Returns:
        tuple: (success: bool, position: list[float] or None) - world [x, y, z]
    """
    trav_space = th.where(eroded_map == 255)
    if trav_space[0].shape[0] == 0:
        return False, None
    idx = th.randint(0, high=trav_space[0].shape[0], size=(1,)).item()
    xy_map = th.tensor([trav_space[0][idx], trav_space[1][idx]])
    x, y = trav_map_obj.map_to_world(xy_map)
    z = trav_map_obj.floor_heights[floor]

    # Check room type
    map_coords = seg_map.world_to_map(th.tensor([float(x), float(y)]))
    map_x, map_y = int(map_coords[0].item()), int(map_coords[1].item())
    if (0 <= map_x < seg_map.room_sem_map.shape[0] and
            0 <= map_y < seg_map.room_sem_map.shape[1]):
        room_sem_id = seg_map.room_sem_map[map_x, map_y].item()
        if room_sem_id in valid_room_sem_ids:
            return True, [float(x), float(y), float(z)]
    return False, None


def sample_robot_poses(env, room_types, num_samples=5, max_attempts_per_sample=50, eroded_map=None, trav_map_obj=None):
    """
    Sample random robot poses within specific room types using traversable map.
    Uses the traversable map to ensure robot doesn't spawn on top of or too close to furniture.

    Args:
        env: The OmniGibson environment
        room_types: List of room types to sample poses from (e.g., ["living_room"])
        num_samples: Number of pose samples to generate per robot type
        max_attempts_per_sample: Max attempts to find a valid point in the room

    Returns:
        dict: Mapping from robot type to list of pose dicts with 'position' and 'orientation'
    """
    robot_poses = {}
    scene = env.scene
    seg_map = scene.seg_map

    # Get valid room semantic IDs
    valid_room_sem_ids = set()
    for room_type in room_types:
        if room_type in seg_map.room_sem_name_to_sem_id:
            valid_room_sem_ids.add(seg_map.room_sem_name_to_sem_id[room_type])
        else:
            print(f"Warning: room_type [{room_type}] does not exist.")

    if not valid_room_sem_ids:
        print(f"Warning: No valid room types found for {room_types}")
        return robot_poses

    for robot_type in ROBOT_TYPES:
        poses = []
        for _ in range(num_samples):
            # Try to find a valid point in the desired room types
            for attempt in range(max_attempts_per_sample):
                try:
                    if eroded_map is not None and trav_map_obj is not None:
                        success, pos_list = sample_from_eroded_map(
                            eroded_map, trav_map_obj, seg_map, valid_room_sem_ids, floor=0
                        )
                        if not success:
                            continue
                        yaw = np.random.uniform(0, 2 * np.pi)
                        orientation = [0.0, 0.0, float(np.sin(yaw / 2)), float(np.cos(yaw / 2))]
                        pose = {"position": pos_list, "orientation": orientation}
                        poses.append(pose)
                        break
                    else:
                        _, pos = scene.get_random_point(floor=0)
                        map_coords = seg_map.world_to_map(pos[:2])
                        map_x, map_y = int(map_coords[0].item()), int(map_coords[1].item())
                        if (0 <= map_x < seg_map.room_sem_map.shape[0] and
                            0 <= map_y < seg_map.room_sem_map.shape[1]):
                            room_sem_id = seg_map.room_sem_map[map_x, map_y].item()
                            if room_sem_id in valid_room_sem_ids:
                                yaw = np.random.uniform(0, 2 * np.pi)
                                orientation = [0.0, 0.0, float(np.sin(yaw / 2)), float(np.cos(yaw / 2))]
                                pose = {
                                    "position": [float(pos[0]), float(pos[1]), float(pos[2])],
                                    "orientation": orientation,
                                }
                                poses.append(pose)
                                break
                except Exception as e:
                    print(f"Warning: Failed to sample pose for {robot_type} (attempt {attempt}): {e}")
                    continue
            else:
                print(f"Warning: Could not find valid pose for {robot_type} in {room_types} after {max_attempts_per_sample} attempts")

        if poses:
            robot_poses[robot_type] = poses
        else:
            print(f"Warning: No poses sampled for {robot_type}")

    return robot_poses


def main():
    args = parser.parse_args()

    # Set random seed for reproducibility
    np.random.seed(args.seed)
    th.manual_seed(args.seed)

    # Define the configuration to load -- we'll use a R1
    cfg = {
        # Use default frequency
        "env": {
            "action_frequency": 30,
            "physics_frequency": 120,
        },
        "scene": {
            "type": "InteractiveTraversableScene",
            "scene_model": args.scene_model,
            "seg_map_resolution": 0.1,
            "load_room_types": TASK_CUSTOM_LISTS[args.activity]["room_types"],
        },
        "robots": [
            {
                "type": "R1",
                "obs_modalities": ["rgb"],
                "grasping_mode": "physical",
                "default_arm_pose": "diagonal30",
                "default_reset_mode": "untuck",
                "position": np.ones(3) * -50.0,
            },
        ],
        "task": {
            "type": "BehaviorTask",
            "online_object_sampling": False,
            "activity_name": args.activity,
            "activity_instance_id": args.seed,
        },
    }
    env = og.Environment(cfg)

    # Define where to save instances
    save_dir = os.path.join(
        get_dataset_path("behavior-1k-assets"),
        "scenes",
        env.task.scene_name,
        "json",
        f"{env.task.scene_name}_task_{args.activity}_instances",
    )

    # If we want to create a stable scene config, do that now
    default_scene_fpath = os.path.join(
        get_dataset_path("behavior-1k-assets"), "scenes", args.scene_model, "json", f"{args.scene_model}_stable.json"
    )
    # Get the default scene instance
    assert os.path.exists(default_scene_fpath), "Did not find default stable scene json!"
    with open(default_scene_fpath, "r") as f:
        default_scene_dict = json.load(f)

    # Needed for _sample_initial_conditions_final()
    env.task.sampler._parse_inroom_object_room_assignment()
    env.task.sampler._build_sampling_order()

    # Clear all the system particles
    for system in env.scene.active_systems.values():
        system.remove_all_particles()

    og.sim.step()

    # Store the state without any particles
    initial_state = og.sim.dump_state()

    # Build proximity check radii from 0.05 to args.proximity_radius in 0.05 increments
    num_radius_steps = max(1, round(args.proximity_radius / 0.05))
    inflate_radii = [round(i * 0.05, 2) for i in range(1, num_radius_steps + 1)]
    print(f"Proximity check radii: {inflate_radii}")

    # Build eroded traversability map for robot pose sampling
    # Eroding by erosion_radius ensures sampled points are at least that far from walls/furniture
    eroded_map, trav_map_obj = build_eroded_trav_map(env, erosion_radius=args.erosion_radius, floor=0)
    eroded_trav_count = (eroded_map == 255).sum().item()
    orig_trav_count = (env.scene._trav_map.floor_map[0] == 255).sum().item()
    print(f"Eroded traversability map: {eroded_trav_count} traversable pixels (was {orig_trav_count}, {100*eroded_trav_count/max(orig_trav_count,1):.1f}% remaining)")

    # Save before/after traversability map images
    trav_map_dir = os.path.join(save_dir, "trav_maps")
    os.makedirs(trav_map_dir, exist_ok=True)
    orig_map_np = env.scene._trav_map.floor_map[0].cpu().numpy().astype(np.uint8)
    eroded_map_np = eroded_map.cpu().numpy().astype(np.uint8)
    cv2.imwrite(os.path.join(trav_map_dir, "trav_map_original.png"), orig_map_np)
    cv2.imwrite(os.path.join(trav_map_dir, "trav_map_eroded.png"), eroded_map_np)
    # Also save a side-by-side comparison
    side_by_side = np.hstack([orig_map_np, eroded_map_np])
    cv2.imwrite(os.path.join(trav_map_dir, "trav_map_comparison.png"), side_by_side)
    print(f"Saved traversability map images to {trav_map_dir}")

    num_trials = 50
    for activity_instance_id in range(args.start_idx, args.end_idx + 1):
        # Set unique seed for this instance to ensure different robot poses
        instance_seed = args.seed * 10000 + activity_instance_id
        np.random.seed(instance_seed)
        th.manual_seed(instance_seed)

        # Sample unique robot poses for this instance within the task-relevant room types
        room_types = TASK_CUSTOM_LISTS[args.activity]["room_types"]
        instance_robot_poses = sample_robot_poses(env, room_types=room_types, num_samples=args.num_robot_pose_samples, eroded_map=eroded_map, trav_map_obj=trav_map_obj)
        print(f"Sampled robot poses for instance {activity_instance_id}: {len(instance_robot_poses)} robot types in rooms {room_types}")

        for i in range(num_trials):
            og.sim.load_state(initial_state)
            og.sim.step()

            # Will sample new particles to satisfy states like Filled
            error_msg = env._task.sampler._sample_initial_conditions_final()

            if error_msg is not None:
                print(f"instance {activity_instance_id} trial {i} sampling failed: {error_msg}")
                continue

            for _ in range(10):
                og.sim.step()

            for obj in env._task.object_scope.values():
                if isinstance(obj, DatasetObject):
                    obj.keep_still()

            for _ in range(10):
                og.sim.step()

            task_final_state = env.scene.dump_state()
            task_scene_dict = {"state": task_final_state}
            validated, error_msg = validate_task(env.task, task_scene_dict, default_scene_dict)
            if not validated:
                print(f"instance {activity_instance_id} trial {i} validation failed: {error_msg}")
                continue

            # Check that no task objects are under furniture
            # Exclude floor/structural objects - they are always "under" furniture but that's expected
            task_objects = [
                obj.wrapped_obj for obj in env._task.object_scope.values()
                if hasattr(obj, 'wrapped_obj') and isinstance(obj.wrapped_obj, DatasetObject)
                and obj.wrapped_obj.category not in STRUCTURE_CATEGORIES
            ]
            under_valid, under_error = validate_objects_not_under_furniture(env, task_objects)
            if not under_valid:
                print(f"instance {activity_instance_id} trial {i} under-furniture validation failed: {under_error}")
                continue

            # Check task objects are not too close to furniture
            proximity_valid, proximity_error = validate_objects_proximity(env, task_objects, inflate_radii)
            if not proximity_valid:
                print(f"instance {activity_instance_id} trial {i} proximity validation failed: {proximity_error}")
                continue

            env.scene.load_state(task_final_state)
            env.scene.update_initial_file()
            print(f"instance {activity_instance_id} trial {i} succeeded.")

            env.task.activity_instance_id = activity_instance_id

            # Save task - save both the full template AND the TRO state file
            # First save the full template (task_relevant_only=False)
            env.task.save_task(env=env, save_dir=save_dir, override=True, task_relevant_only=False)
            # Also save the TRO state file (task_relevant_only=True)
            env.task.save_task(env=env, save_dir=save_dir, override=True, task_relevant_only=True)

            # Then update the saved files with our sampled robot poses
            task_name = env.task.get_cached_activity_scene_filename(
                scene_model=args.scene_model,
                activity_name=args.activity,
                activity_definition_id=0,
                activity_instance_id=activity_instance_id,
            )

            # Update template file with robot_poses in metadata
            # Note: task_name already includes "_template" suffix from get_cached_activity_scene_filename
            template_fpath = os.path.join(save_dir, f"{task_name}.json")
            print(f"DEBUG: Looking for template at: {template_fpath}")
            print(f"DEBUG: instance_robot_poses R1Pro[0] position: {instance_robot_poses.get('R1Pro', [{}])[0].get('position', 'NONE')}")
            if os.path.exists(template_fpath):
                with open(template_fpath, "r") as f:
                    template_data = json.load(f)
                if "metadata" not in template_data:
                    template_data["metadata"] = {}
                template_data["metadata"]["robot_poses"] = instance_robot_poses
                with open(template_fpath, "w") as f:
                    json.dump(template_data, f, indent=4)
                print(f"Updated template with robot_poses: {template_fpath}")
            else:
                print(f"WARNING: Template file not found at {template_fpath}")

            # Update TRO state file with robot_poses
            tro_fpath = os.path.join(save_dir, f"{task_name}-tro_state.json")
            if os.path.exists(tro_fpath):
                with open(tro_fpath, "r") as f:
                    tro_data = json.load(f)
                # Remove agent entry if present (wrong robot type)
                if "agent.n.01_1" in tro_data:
                    del tro_data["agent.n.01_1"]
                tro_data["robot_poses"] = instance_robot_poses
                with open(tro_fpath, "w") as f:
                    json.dump(tro_data, f, indent=4)
                print(f"Updated TRO state with robot_poses: {tro_fpath}")

            print(f"instance {activity_instance_id} trial {i} saved")
            break

    print("Successful shutdown!")
    og.shutdown()


if __name__ == "__main__":
    main()
