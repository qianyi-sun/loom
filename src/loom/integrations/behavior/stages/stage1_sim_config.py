"""Closed simulator configuration used by the Stage 1 image.

The historical evaluator imported its scene and robot configuration from an
interactive teleoperation package.  Stage 1 owns the much smaller production
surface below instead: the selected scene and initial pose are supplied by the
signed dataset/TRO record, while all remaining values are fixed by this module.
"""

from __future__ import annotations

from typing import Any

from omnigibson.transition_rules import (  # type: ignore[import-not-found]
    CookingSystemRule,
    MixingToolRule,
    ToggleableMachineRule,
)

DISABLED_TRANSITION_RULES = [
    ToggleableMachineRule,
    MixingToolRule,
    CookingSystemRule,
]

_ROOM_DEPENDENCIES = {
    "house_single_floor": {
        "dining_room": ["kitchen", "living_room"],
        "kitchen": ["living_room", "dining_room"],
        "living_room": ["kitchen", "dining_room"],
    },
    "house_double_floor_lower": {
        "kitchen": ["living_room", "corridor"],
        "living_room": ["kitchen", "corridor"],
        "garage": ["corridor"],
    },
    "house_double_floor_upper": {},
}

_TASK_SPECIFIC_EXTRA_ROOMS = {
    "bringing_in_kindling": {"house_double_floor_lower": ["corridor"]},
    "bringing_newspaper_in": {"house_double_floor_lower": ["corridor"]},
    "chopping_wood": {"house_double_floor_lower": ["garage"]},
}

_EXTERNAL_CAMERAS = (
    (
        "external_sensor0",
        [-0.4, 0.0, 2.0],
        [0.2706, -0.2706, -0.6533, 0.6533],
    ),
    (
        "external_sensor1",
        [-0.2, 0.6, 2.0],
        [-0.1930, 0.4163, 0.8062, -0.3734],
    ),
    (
        "external_sensor2",
        [-0.2, -0.6, 2.0],
        [0.4164, -0.1929, -0.3737, 0.8060],
    ),
)


def get_task_relevant_room_types(activity_name: str) -> list[str]:
    from bddl.activity import Conditions  # type: ignore[import-not-found]

    conditions = Conditions(
        activity_name,
        0,
        simulator_name="omnigibson",
        predefined_problem=None,
    )
    values = {
        condition[2]
        for condition in conditions.parsed_initial_conditions
        if len(condition) == 3 and condition[0] == "inroom"
    }
    return sorted(values)


def augment_rooms(
    relevant_rooms: list[str],
    scene_model: str,
    task_name: str,
) -> list[str]:
    dependencies = _ROOM_DEPENDENCIES.get(scene_model)
    if dependencies is None:
        raise ValueError("signed task selected an unsupported Stage 1 scene")
    values = set(relevant_rooms)
    for room in tuple(values):
        values.update(dependencies.get(room, ()))
    values.update(_TASK_SPECIFIC_EXTRA_ROOMS.get(task_name, {}).get(scene_model, ()))
    return sorted(values)


def _camera(
    name: str,
    position: list[float],
    orientation: list[float],
) -> dict[str, Any]:
    return {
        "sensor_type": "VisionSensor",
        "name": name,
        "relative_prim_path": f"/controllable__r1pro__robot_r1/base_link/{name}",
        "modalities": [],
        "sensor_kwargs": {
            "viewport_name": "Viewport",
            "image_height": 1080,
            "image_width": 1080,
        },
        "position": position,
        "orientation": orientation,
        "pose_frame": "parent",
        "include_in_obs": False,
    }


def generate_basic_environment_config(
    task_name: str | None = None,
    task_cfg: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if task_name is None or task_cfg is None:
        raise ValueError("Stage 1 requires one authoritative task configuration")
    if set(task_cfg) != {
        "robot_start_orientation",
        "robot_start_position",
        "scene_model",
    }:
        raise ValueError("Stage 1 task configuration keys are not closed")
    scene_model = task_cfg["scene_model"]
    if not isinstance(scene_model, str) or scene_model not in _ROOM_DEPENDENCIES:
        raise ValueError("Stage 1 task scene is unsupported")
    return {
        "env": {
            "action_frequency": 30,
            "rendering_frequency": 30,
            "physics_frequency": 120,
            "external_sensors": [
                _camera(name, position, orientation)
                for name, position, orientation in _EXTERNAL_CAMERAS
            ],
        },
        "scene": {
            "type": "InteractiveTraversableScene",
            "scene_model": scene_model,
            "load_room_types": None,
            "load_room_instances": None,
            "include_robots": False,
        },
        "task": {
            "type": "BehaviorTask",
            "activity_name": task_name,
            "activity_definition_id": 0,
            "activity_instance_id": 0,
            "predefined_problem": None,
            "online_object_sampling": False,
            "debug_object_sampling": False,
            "highlight_task_relevant_objects": False,
            "termination_config": {"max_steps": 50_000},
            "reward_config": {"r_potential": 1.0},
            "include_obs": False,
        },
    }


def generate_robot_config(
    task_name: str | None = None,
    task_cfg: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if task_name is None or task_cfg is None:
        raise ValueError("Stage 1 requires one authoritative robot pose")
    torch = __import__("torch")
    reset_joint_pos = torch.zeros(28)
    reset_joint_pos[-4:] = 0.05
    upright = torch.tensor([0.45, -0.4, 0.0, 0.0], dtype=torch.float32)
    downward = torch.tensor([1.6, -2.5, -0.94, 0.0], dtype=torch.float32)
    reset_joint_pos[6:10] = (upright + downward) / 2
    return {
        "type": "R1Pro",
        "name": "robot_r1",
        "action_normalize": False,
        "controller_config": {
            "arm_left": {
                "name": "JointController",
                "motor_type": "position",
                "pos_kp": 150,
                "command_input_limits": None,
                "command_output_limits": None,
                "use_impedances": False,
                "use_delta_commands": False,
            },
            "arm_right": {
                "name": "JointController",
                "motor_type": "position",
                "pos_kp": 150,
                "command_input_limits": None,
                "command_output_limits": None,
                "use_impedances": False,
                "use_delta_commands": False,
            },
            "gripper_left": {
                "name": "MultiFingerGripperController",
                "mode": "smooth",
                "command_input_limits": "default",
                "command_output_limits": "default",
                "use_impedances": False,
                "use_delta_commands": False,
            },
            "gripper_right": {
                "name": "MultiFingerGripperController",
                "mode": "smooth",
                "command_input_limits": "default",
                "command_output_limits": "default",
                "use_impedances": False,
                "use_delta_commands": False,
            },
            "base": {
                "name": "HolonomicBaseJointController",
                "motor_type": "velocity",
                "vel_kp": 150,
                "command_input_limits": [-torch.ones(3), torch.ones(3)],
                "command_output_limits": [
                    -torch.tensor([0.75, 0.75, 1.0]),
                    torch.tensor([0.75, 0.75, 1.0]),
                ],
                "use_impedances": False,
            },
            "trunk": {
                "name": "JointController",
                "motor_type": "position",
                "pos_kp": 150,
                "command_input_limits": None,
                "command_output_limits": None,
                "use_impedances": False,
                "use_delta_commands": False,
            },
            "camera": {"name": "NullJointController"},
        },
        "self_collisions": True,
        "obs_modalities": [],
        "position": task_cfg["robot_start_position"],
        "orientation": task_cfg["robot_start_orientation"],
        "grasping_mode": "assisted",
        "sensor_config": {
            "VisionSensor": {
                "sensor_kwargs": {"image_height": 1080, "image_width": 1080},
            },
        },
        "reset_joint_pos": reset_joint_pos,
    }
