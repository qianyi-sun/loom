from __future__ import annotations

from dataclasses import dataclass, field
import logging
import math
import os
import pickle
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Literal, Optional, Sequence, Tuple, Union

import numpy as np


logger = logging.getLogger(__name__)
ArmName = Literal["left", "right", "auto"]
PoseLike = Union[Tuple[Sequence[float], Sequence[float]], Dict[str, Sequence[float]]]


@dataclass
class PrimitiveResult:
    success: bool
    primitive: str
    message: str = ""
    object_name: Optional[str] = None
    arm: Optional[str] = None
    data: Dict[str, Any] = field(default_factory=dict)


class LowPrimitivesForAgent:
    def __init__(
        self,
        env: Any,
        robot: Any,
        *,
        task: Optional[Any] = None,
        grasp_library_path: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.env = env
        self.robot = robot
        self.task = task
        self.config = config or {}
        self.grasp_library: Optional[Dict[str, Any]] = None
        self.grasp_library_path: Optional[str] = None
        self._motion_generator: Any = None
        if grasp_library_path is not None:
            self.load_grasp_library(grasp_library_path)

    # ---------------------------------------------------------------------
    # Library / object state
    # ---------------------------------------------------------------------
    def load_grasp_library(self, pkl_path: str) -> PrimitiveResult:
        if not os.path.exists(pkl_path):
            return PrimitiveResult(
                success=False,
                primitive="load_grasp_library",
                message=f"grasp library not found: {pkl_path}",
                data={"path": pkl_path},
            )

        with open(pkl_path, "rb") as f:
            library = pickle.load(f)

        if not isinstance(library, dict):
            return PrimitiveResult(
                success=False,
                primitive="load_grasp_library",
                message=f"expected dict grasp library, got {type(library).__name__}",
                data={"path": pkl_path},
            )
        if "records" not in library:
            return PrimitiveResult(
                success=False,
                primitive="load_grasp_library",
                message="grasp library missing required key: records",
                data={"path": pkl_path, "keys": sorted(library.keys())},
            )

        library.setdefault("index", self._build_grasp_library_index(library["records"]))
        self.grasp_library = library
        self.grasp_library_path = pkl_path
        return PrimitiveResult(
            success=True,
            primitive="load_grasp_library",
            message=f"loaded {len(library['records'])} grasp records",
            data={
                "path": pkl_path,
                "num_records": len(library["records"]),
                "task_name": library.get("task_name"),
                "task_dir": library.get("task_dir"),
            },
        )

    @staticmethod
    def _object_category_from_name(object_name: str) -> str:
        parts = object_name.rsplit("_", 1)
        return parts[0] if len(parts) == 2 and parts[1].isdigit() else object_name

    @classmethod
    def _relation_key(cls, from_object: Optional[str]) -> Optional[str]:
        if from_object is None:
            return None
        category = cls._object_category_from_name(from_object)
        if category.startswith("floor"):
            return "from_floor"
        if category.startswith("bed"):
            return "from_bed"
        if category.startswith("table") or category.startswith("nightstand"):
            return "from_table"
        return f"from_{category}"

    @classmethod
    def _build_grasp_library_index(cls, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        by_object: Dict[str, List[int]] = {}
        by_category: Dict[str, List[int]] = {}
        by_category_relation: Dict[str, Dict[str, List[int]]] = {}

        for idx, record in enumerate(records):
            object_name = record.get("object_name")
            object_category = record.get("object_category") or (
                cls._object_category_from_name(object_name) if object_name else None
            )
            relation = record.get("relation_key") or cls._relation_key(record.get("from_object"))

            if object_name:
                by_object.setdefault(object_name, []).append(idx)
            if object_category:
                by_category.setdefault(object_category, []).append(idx)
                if relation:
                    by_category_relation.setdefault(object_category, {}).setdefault(relation, []).append(idx)

        return {
            "by_object": by_object,
            "by_category": by_category,
            "by_category_relation": by_category_relation,
        }

    @staticmethod
    def _record_matches_filters(
        record: Dict[str, Any],
        *,
        arm: ArmName = "auto",
        task_name: Optional[str] = None,
    ) -> bool:
        if arm != "auto" and record.get("arm") != arm:
            return False
        if task_name is not None and record.get("task_name") not in (None, task_name):
            return False
        return True

    @staticmethod
    def _pose_to_arrays(pose: PoseLike) -> Tuple[np.ndarray, np.ndarray]:
        if isinstance(pose, dict):
            pos = pose.get("pos", None)
            if pos is None:
                pos = pose.get("position", None)
            quat = pose.get("quat", None)
            if quat is None:
                quat = pose.get("orientation", None)
        else:
            pos, quat = pose
        if pos is None or quat is None:
            raise ValueError(f"Invalid pose: {pose!r}")
        return np.asarray(pos, dtype=np.float64), np.asarray(quat, dtype=np.float64)

    @staticmethod
    def _quat_multiply(q1: Sequence[float], q2: Sequence[float]) -> np.ndarray:
        x1, y1, z1, w1 = np.asarray(q1, dtype=np.float64)
        x2, y2, z2, w2 = np.asarray(q2, dtype=np.float64)
        return np.asarray(
            [
                w1 * x2 + x1 * w2 + y1 * z2 - z1 * y2,
                w1 * y2 - x1 * z2 + y1 * w2 + z1 * x2,
                w1 * z2 + x1 * y2 - y1 * x2 + z1 * w2,
                w1 * w2 - x1 * x2 - y1 * y2 - z1 * z2,
            ],
            dtype=np.float64,
        )

    @staticmethod
    def _quat_conjugate(quat: Sequence[float]) -> np.ndarray:
        x, y, z, w = np.asarray(quat, dtype=np.float64)
        return np.asarray([-x, -y, -z, w], dtype=np.float64)

    @staticmethod
    def _quat_normalize(quat: Sequence[float]) -> np.ndarray:
        q = np.asarray(quat, dtype=np.float64)
        norm = np.linalg.norm(q)
        if norm < 1e-12:
            raise ValueError(f"Cannot normalize near-zero quaternion: {quat!r}")
        return q / norm

    @classmethod
    def _quat_similarity(cls, q1: Sequence[float], q2: Sequence[float]) -> float:
        q1_np = cls._quat_normalize(q1)
        q2_np = cls._quat_normalize(q2)
        return float(abs(np.dot(q1_np, q2_np)))

    @classmethod
    def _quat_angle_diff(cls, q1: Sequence[float], q2: Sequence[float]) -> float:
        similarity = min(1.0, max(-1.0, cls._quat_similarity(q1, q2)))
        return float(2.0 * math.acos(similarity))

    @classmethod
    def _rotate_vector(cls, quat: Sequence[float], vec: Sequence[float]) -> np.ndarray:
        q = cls._quat_normalize(quat)
        vq = np.asarray([vec[0], vec[1], vec[2], 0.0], dtype=np.float64)
        return cls._quat_multiply(cls._quat_multiply(q, vq), cls._quat_conjugate(q))[:3]

    @classmethod
    def _pose_inverse(cls, pos: Sequence[float], quat: Sequence[float]) -> Tuple[np.ndarray, np.ndarray]:
        quat_inv = cls._quat_conjugate(cls._quat_normalize(quat))
        pos_inv = -cls._rotate_vector(quat_inv, pos)
        return pos_inv, quat_inv

    @classmethod
    def _pose_multiply(
        cls,
        pos_a: Sequence[float],
        quat_a: Sequence[float],
        pos_b: Sequence[float],
        quat_b: Sequence[float],
    ) -> Tuple[np.ndarray, np.ndarray]:
        quat_a = cls._quat_normalize(quat_a)
        quat_b = cls._quat_normalize(quat_b)
        pos = np.asarray(pos_a, dtype=np.float64) + cls._rotate_vector(quat_a, pos_b)
        quat = cls._quat_normalize(cls._quat_multiply(quat_a, quat_b))
        return pos, quat

    @classmethod
    def _object_relative_pose_to_world_arrays(
        cls,
        object_pose: PoseLike,
        *,
        rel_pos: Sequence[float],
        rel_quat: Sequence[float],
    ) -> Tuple[np.ndarray, np.ndarray]:
        obj_pos, obj_quat = cls._pose_to_arrays(object_pose)
        obj_quat = obj_quat / np.linalg.norm(obj_quat)
        rel_pos = np.asarray(rel_pos, dtype=np.float64)
        rel_quat = np.asarray(rel_quat, dtype=np.float64)
        rel_quat = rel_quat / np.linalg.norm(rel_quat)
        world_pos = obj_pos + cls._rotate_vector(obj_quat, rel_pos)
        world_quat = cls._quat_multiply(obj_quat, rel_quat)
        world_quat = world_quat / np.linalg.norm(world_quat)
        return world_pos, world_quat

    @staticmethod
    def _yaw_from_quat(quat: Sequence[float]) -> float:
        x, y, z, w = np.asarray(quat, dtype=np.float64)
        return float(np.arctan2(2.0 * (w * z + x * y), 1.0 - 2.0 * (y * y + z * z)))

    @staticmethod
    def _wrap_angle(angle: float) -> float:
        return float((angle + np.pi) % (2.0 * np.pi) - np.pi)

    def _get_nav_primitives(self) -> Any:
        primitives = getattr(self, "_nav_primitives", None)
        if primitives is None:
            from omnigibson.action_primitives.starter_semantic_action_primitives import (
                StarterSemanticActionPrimitives,
            )

            # Reuse the ONE motion generator this process already owns: a second
            # CuRoboMotionGenerator on the shared GPU OOMs and corrupts the first
            # stack's solves (every post-nav arm plan failed, observed live).
            motion_generator = self._get_motion_generator()
            primitives = StarterSemanticActionPrimitives(
                env=self.env,
                robot=self.robot,
                enable_head_tracking=False,
                task_relevant_objects_only=False,
                curobo_batch_size=motion_generator.batch_size,
                skip_curobo_initilization=True,
            )
            primitives._motion_generator = motion_generator
            self._nav_primitives = primitives
        return primitives

    def _get_motion_generator(self) -> Any:
        if self._motion_generator is None:
            from omnigibson.action_primitives.curobo import CuRoboMotionGenerator

            self._motion_generator = CuRoboMotionGenerator(
                robot=self.robot,
                batch_size=int(
                    self.config.get("arm_curobo_batch_size", os.environ.get("MOP_ARM_CUROBO_BATCH_SIZE", 1))
                ),
                collision_activation_distance=float(self.config.get("arm_collision_activation_distance", 0.005)),
                # NOTE (embodiedClaw_recovery modification, 2026-06-12): honor the same
                # MOP_CUROBO_USE_CUDA_GRAPH=0 gate as the recovery backend's waist-locked
                # generator — disabling cuda graphs frees ~0.5 GB (graph pools + warmup),
                # needed when curobo shares the sim's GPU.
                use_cuda_graph=os.environ.get("MOP_CUROBO_USE_CUDA_GRAPH", "1") != "0",
            )
        return self._motion_generator

    def _resolve_object(self, object_name: str) -> Any:
        try:
            from omnigibson.learning.embodiedClaw.mop_controller import resolve_target_object

            obj = resolve_target_object(self.env, [object_name])
            if obj is not None:
                return obj
        except Exception:
            pass

        object_scope = getattr(getattr(self.env, "task", None), "object_scope", {})
        if object_name in object_scope:
            return object_scope[object_name]
        for scope_name, entity in object_scope.items():
            if getattr(entity, "name", None) == object_name or scope_name == object_name:
                return entity
        raise KeyError(f"Could not resolve object {object_name!r}")

    def _pose_to_2d(self, pose: Union[PoseLike, Sequence[float]]) -> np.ndarray:
        if isinstance(pose, dict) or (isinstance(pose, tuple) and len(pose) == 2):
            pos, quat = self._pose_to_arrays(pose)  # type: ignore[arg-type]
            return np.asarray([pos[0], pos[1], self._yaw_from_quat(quat)], dtype=np.float32)

        values = np.asarray(pose, dtype=np.float64)
        if values.shape[0] < 3:
            raise ValueError(f"Navigation pose must be (x, y, yaw) or (pos, quat), got {pose!r}")
        return np.asarray([values[0], values[1], values[2]], dtype=np.float32)

    def get_grasp_pose(
        self,
        object_name: str,
        *,
        object_pose: PoseLike,
        gripper_pos: Sequence[float],
        gripper_quat: Optional[Sequence[float]] = None,
        arm: ArmName = "auto",
        from_object: Optional[str] = None,
        task_name: Optional[str] = None,
    ) -> PoseLike:
        if self.grasp_library is None:
            raise RuntimeError("No grasp library loaded. Call load_grasp_library(pkl_path) first.")

        index = self.grasp_library.get("index", {})
        records = self.grasp_library["records"]
        object_category = self._object_category_from_name(object_name)
        relation = self._relation_key(from_object)
        gripper_pos_np = np.asarray(gripper_pos, dtype=np.float64)

        candidate_indices: List[int] = []
        lookup_source = None

        exact_candidates = index.get("by_object", {}).get(object_name, [])
        if exact_candidates:
            candidate_indices = exact_candidates
            lookup_source = f"object:{object_name}"

        if not candidate_indices and relation is not None:
            relation_candidates = index.get("by_category_relation", {}).get(object_category, {}).get(relation, [])
            if relation_candidates:
                candidate_indices = relation_candidates
                lookup_source = f"category_relation:{object_category}:{relation}"

        if not candidate_indices:
            category_candidates = index.get("by_category", {}).get(object_category, [])
            if category_candidates:
                candidate_indices = category_candidates
                lookup_source = f"category:{object_category}"

        if not candidate_indices:
            raise KeyError(
                f"No grasp candidates found for object={object_name!r}, "
                f"category={object_category!r}, from_object={from_object!r}"
            )

        filtered = [
            records[idx]
            for idx in candidate_indices
            if self._record_matches_filters(records[idx], arm=arm, task_name=task_name)
        ]
        if not filtered and arm != "auto":
            filtered = [
                records[idx]
                for idx in candidate_indices
                if self._record_matches_filters(records[idx], arm="auto", task_name=task_name)
            ]
        if not filtered:
            raise KeyError(
                "No grasp record matched filters "
                f"arm={arm!r}, task_name={task_name!r}"
            )

        best_record = None
        best_pos_world = None
        best_quat_world = None
        best_distance = float("inf")
        for record in filtered:
            pos_world, quat_world = self._object_relative_pose_to_world_arrays(
                object_pose,
                rel_pos=record["pos_obj"],
                rel_quat=record["quat_obj"],
            )
            distance = float(np.linalg.norm(pos_world - gripper_pos_np))
            if distance < best_distance:
                best_record = record
                best_pos_world = pos_world
                best_quat_world = quat_world
                best_distance = distance

        assert best_record is not None
        selected_orientation_variant = "original"
        original_similarity = None
        z180_similarity = None
        if gripper_quat is not None:
            z180_quat = np.asarray([0.0, 0.0, 1.0, 0.0], dtype=np.float64)
            z180_world_quat = self._quat_multiply(best_quat_world, z180_quat)
            z180_world_quat = self._quat_normalize(z180_world_quat)
            original_similarity = self._quat_similarity(best_quat_world, gripper_quat)
            z180_similarity = self._quat_similarity(z180_world_quat, gripper_quat)
            if z180_similarity > original_similarity:
                best_quat_world = z180_world_quat
                selected_orientation_variant = "z180"

            debug_msg = (
                f"[low-grasp-debug] orientation variant selection object={object_name} "
                f"arm={arm} source={lookup_source} "
                f"original_similarity={original_similarity} z180_similarity={z180_similarity} "
                f"selected={selected_orientation_variant}"
            )
            logger.info(debug_msg)
            print(debug_msg, flush=True)

        return {
            "pos": best_pos_world,
            "quat": best_quat_world,
            "pos_obj": best_record["pos_obj"],
            "quat_obj": best_record["quat_obj"],
            "distance_to_gripper": best_distance,
            "orientation_variant": selected_orientation_variant,
            "orientation_similarity": (
                original_similarity if selected_orientation_variant == "original" else z180_similarity
            ),
            "orientation_similarity_original": original_similarity,
            "orientation_similarity_z180": z180_similarity,
            "arm": best_record.get("arm"),
            "object_name": best_record.get("object_name"),
            "object_category": best_record.get("object_category"),
            "from_object": best_record.get("from_object"),
            "relation_key": best_record.get("relation_key"),
            "episode_id": best_record.get("episode_id"),
            "grasp_frame": best_record.get("grasp_frame"),
            "source": lookup_source,
            "record": best_record,
        }

    # ---------------------------------------------------------------------
    # Navigation
    # ---------------------------------------------------------------------
    def _plan_starter_navigation_trajectory(
        self,
        primitives: Any,
        pose_2d: Any,
    ) -> Tuple[Any, Any, Any]:
        import torch as th
        import omnigibson.utils.transform_utils as T
        from omnigibson.action_primitives.curobo import CuRoboEmbodimentSelection

        nav_pos, nav_quat = primitives._get_robot_pose_from_2d_pose(pose_2d)
        emb_sel = CuRoboEmbodimentSelection.BASE
        target_link = primitives._motion_generator.ee_link[emb_sel]

        # Build the goal IN THE PLANNING FRAME from base-joint values. The BASE chain
        # only moves x/y/rz; z/rx/ry stay at their locked live values, so a goal built
        # from the live WORLD z sits off the (tilted) root plane by ~tilt*distance —
        # millimetres at metres, above the IK position threshold: far goals could
        # never converge (probe: world-frame IK_FAIL vs 3s success with this goal).
        base_j = self.robot.get_joint_positions()[self.robot.base_idx]
        rel = primitives._world_pose_2d_to_base_joint_values(pose_2d)
        local_pos = th.tensor([float(rel[0]), float(rel[1]), float(base_j[2])], dtype=th.float32)
        local_quat = T.mat2quat(T.euler_intrinsic2mat(
            th.tensor([float(base_j[3]), float(base_j[4]), float(rel[2])], dtype=th.float32)))
        max_attempts = int(os.environ.get("MOP_CUROBO_MAX_ATTEMPTS", self.config.get("nav_curobo_max_attempts", 30)))
        curobo_timeout_s = float(os.environ.get("MOP_CUROBO_TIMEOUT_S", self.config.get("nav_curobo_timeout", 20.0)))
        print(
            "[low-nav-debug] CuRobo BASE planning start "
            f"link={target_link} local_pos={local_pos.tolist()} local_quat={local_quat.tolist()} "
            f"world_pose_2d={[float(v) for v in pose_2d]} max_attempts={max_attempts} "
            f"timeout_s={curobo_timeout_s}",
            flush=True,
        )
        successes, paths = primitives._motion_generator.compute_trajectories(
            target_pos={target_link: local_pos.view(1, 3)},
            target_quat={target_link: local_quat.view(1, 4)},
            is_local=True,
            max_attempts=max_attempts,
            timeout=curobo_timeout_s,
            ik_fail_return=int(os.environ.get("MOP_CUROBO_IK_FAIL_RETURN", self.config.get("nav_curobo_ik_fail_return", 50))),
            enable_finetune_trajopt=True,
            finetune_attempts=int(self.config.get("nav_curobo_finetune_attempts", 1)),
            return_full_result=False,
            success_ratio=1.0 / primitives._motion_generator.batch_size,
            # keep the stance-gate world (_sample_navigation_pose_near_object wrote it:
            # structural excluded, held objects excluded) — the probe-proven setup
            skip_obstacle_update=True,
            ik_only=False,
            ik_world_collision_check=False,
            emb_sel=emb_sel,
        )
        success_idx = th.where(successes)[0].cpu()
        print(
            "[low-nav-debug] CuRobo BASE planning done "
            f"successes={successes.detach().cpu().tolist()} num_paths={len(paths)}",
            flush=True,
        )
        if len(success_idx) == 0:
            raise RuntimeError("CuRobo BASE planning failed: no successful trajectory")

        best_i = int(success_idx[0].item())
        traj_path = paths[best_i]
        q_traj = primitives._motion_generator.path_to_joint_trajectory(
            traj_path,
            get_full_js=True,
            emb_sel=emb_sel,
        ).cpu().float()
        # 0.05 not 0.01: base joints are metres, and _execute_motion_plan paces one
        # waypoint per convergence (or 10 steps) — 1cm-spaced waypoints crawl the base
        # at ~1.2mm/step (live), timing out any multi-metre traverse
        q_traj = primitives._motion_generator.add_linearly_interpolated_waypoints(
            traj=q_traj,
            max_inter_dist=0.05,
        )

        # q_traj's base entries stay ROOT-RELATIVE (curobo native): _execute_motion_plan
        # calls robot.q_to_action per step (which itself maps joint-frame -> local action)
        # and its convergence check compares against get_joint_positions() — both expect
        # joint values. The old world-frame rewrite here made the tracker chase a target
        # metres away from any reachable joint value: EXECUTION_ERROR every time.
        print(
            "[low-nav-debug] planned navigation trajectory "
            f"shape={tuple(q_traj.shape)} nav_pos={nav_pos} nav_quat={nav_quat}",
            flush=True,
        )
        try:
            base_ctrl_idx = self.robot.base_control_idx
            first = q_traj[0][base_ctrl_idx].detach().cpu().tolist()
            last = q_traj[-1][base_ctrl_idx].detach().cpu().tolist()
            print(f"[low-nav-debug] base target first={first} last={last}", flush=True)
        except Exception as e:
            print(f"[low-nav-debug] failed to log base target first/last: {e}", flush=True)
        return q_traj, nav_pos, nav_quat

    def _execute_navigation_trajectory(
        self,
        primitives: Any,
        q_traj: Any,
        *,
        nav_pos: Any,
        nav_quat: Any,
        timeout_steps: int,
    ) -> Tuple[int, bool]:
        import torch as th
        import omnigibson.utils.transform_utils as T
        import omnigibson.action_primitives.starter_semantic_action_primitives as sap

        executed_steps = 0
        timed_out = False
        close_enough_hits = 0
        close_enough_required = int(self.config.get("nav_close_enough_required", 3))
        check_every_steps = int(self.config.get("nav_check_every_steps", 30))
        close_enough_dist = float(sap.m.DEFAULT_DIST_THRESHOLD) + float(self.config.get("nav_close_enough_extra", 0.02))
        close_enough_yaw = float(sap.m.DEFAULT_ANGLE_THRESHOLD)
        for action in primitives._execute_motion_plan(q_traj):
            if action is None:
                continue
            self.env.step(action, n_render_iterations=1)
            executed_steps += 1
            if executed_steps >= int(timeout_steps):
                timed_out = True
                break
            if check_every_steps > 0 and executed_steps % check_every_steps == 0:
                cur_pos, cur_quat = self.robot.get_position_orientation()
                pos_err = th.max(th.abs(nav_pos[:2] - cur_pos[:2])).item()
                cur_yaw = float(T.quat2euler(cur_quat)[2].item())
                goal_yaw = float(T.quat2euler(nav_quat)[2].item())
                yaw_err = abs(self._wrap_angle(goal_yaw - cur_yaw))
                if pos_err <= close_enough_dist and yaw_err <= close_enough_yaw:
                    close_enough_hits += 1
                    print(
                        "[low-nav-debug] nav close-enough "
                        f"hit={close_enough_hits}/{close_enough_required} "
                        f"pos_err={pos_err:.4f} yaw_err={yaw_err:.4f}",
                        flush=True,
                    )
                    if close_enough_hits >= close_enough_required:
                        break
        return executed_steps, timed_out

    def _navigation_result_from_pose(
        self,
        *,
        primitive: str,
        pose_2d_np: np.ndarray,
        distance_tol: float,
        yaw_tol: float,
        timeout_steps: int,
        executed_steps: int,
        timed_out: bool,
        object_name: Optional[str] = None,
        target_obj: Any = None,
        trajectory: Any = None,
    ) -> PrimitiveResult:
        robot_pos, robot_quat = self.robot.get_position_orientation()
        robot_pos_np = self._to_numpy(robot_pos).astype(np.float64)
        robot_quat_np = self._to_numpy(robot_quat).astype(np.float64)
        pos_err = float(np.linalg.norm(robot_pos_np[:2] - pose_2d_np[:2]))
        yaw_err = abs(self._wrap_angle(self._yaw_from_quat(robot_quat_np) - float(pose_2d_np[2])))
        success = bool(pos_err <= distance_tol and yaw_err <= yaw_tol and not timed_out)
        # [diag] surface the ACTUAL arrival error so a "did not reach target pose" can be told
        # apart: a near-miss (base landed cm-close, gate too tight) vs a real drive gap (landed
        # far short). Which term (pos vs yaw) trips it, and by how much, decides the fix.
        _fail = "pos" if pos_err > distance_tol else ("yaw" if yaw_err > yaw_tol else ("timeout" if timed_out else "-"))
        print(
            f"[low-nav-debug] arrival: pos_err={pos_err:.4f}m (tol={distance_tol:.4f}) "
            f"yaw_err={yaw_err:.4f}rad (tol={yaw_tol:.4f}) timed_out={timed_out} "
            f"-> {'REACHED' if success else 'MISS[' + _fail + ']'} | "
            f"robot_xy={[round(float(v), 3) for v in robot_pos_np[:2]]} "
            f"goal_xy={[round(float(v), 3) for v in pose_2d_np[:2]]}",
            flush=True,
        )
        data = {
            "target_pose_2d": pose_2d_np.tolist(),
            "robot_xy": robot_pos_np[:2].tolist(),
            "pos_err": pos_err,
            "yaw_err": yaw_err,
            "steps": executed_steps,
            "timeout_steps": int(timeout_steps),
            "distance_tol": distance_tol,
            "yaw_tol": yaw_tol,
        }
        if target_obj is not None:
            data["resolved_object_name"] = getattr(target_obj, "name", None)
            data["nav_pose_2d"] = pose_2d_np.tolist()
        if trajectory is not None:
            try:
                data["nav_traj_shape"] = list(trajectory.shape)
                data["nav_traj_first"] = self._to_numpy(trajectory[0]).astype(np.float32).tolist()
                data["nav_traj_last"] = self._to_numpy(trajectory[-1]).astype(np.float32).tolist()
            except Exception:
                data["nav_traj_repr"] = repr(trajectory)
        return PrimitiveResult(
            success=success,
            primitive=primitive,
            object_name=object_name,
            message="navigation reached target pose" if success else "navigation did not reach target pose",
            data=data,
        )

    def _sample_navigation_pose_near_object(
        self,
        primitives: Any,
        target_obj: Any,
        *,
        sampling_attempts: int,
    ) -> Any:
        # Base-pose gate world: the mobile base always touches the floor and structural
        # meshes enclose the robot, so with them as obstacles EVERY correctly-placed
        # stance is rejected (probe on seeded 315: 0/48 vs 34/48 without). Held objects
        # ride with the robot — they are not world obstacles for a stance check.
        held = [o for o in getattr(self.robot, "_ag_obj_in_hand", {}).values() if o is not None]
        self._get_motion_generator().update_obstacles(
            ignore_objects=held or None, include_structural=False)
        return primitives._sample_pose_near_object(
            target_obj,
            sampling_attempts=sampling_attempts,
            skip_obstacle_update=True,
            # navigate just needs the base NEAR the object (to then place a held item / reach it
            # with a planned arm motion); it must NOT be gated on reaching an auto-sampled GRASP
            # pose on the object. That gate rejected every collision-free near-stance for a tall
            # hall tree (grasp pose high/unreachable) -> sampler returned None -> nav failed.
            # require_eef_reachability was added back to SSAP._sample_pose_near_object (2026-06-25);
            # the engine was originally vendored against a MoP-forked SSAP that had it.
            require_eef_reachability=False,
        )

    def _maybe_save_low_level_head_rgb_debug(self, obs: Any, *, executed_steps: int, prefix: str) -> None:
        if os.getenv("MOP_DEBUG", "0") not in ("1", "true", "True"):
            return

        def _find_head_rgb(value: Any) -> Any:
            if not isinstance(value, dict):
                return None
            camera_key = "robot_r1::robot_r1:zed_link:Camera:0::rgb"
            if camera_key in value:
                return value[camera_key]
            for key, child in value.items():
                if isinstance(key, str) and key.endswith("::rgb") and "zed_link:Camera:0" in key:
                    return child
                found = _find_head_rgb(child)
                if found is not None:
                    return found
            if "rgb" in value:
                return value["rgb"]
            return None

        rgb_value = _find_head_rgb(obs)
        source = "obs"
        if rgb_value is None:
            try:
                for sensor_name, sensor in self.robot.sensors.items():
                    if "zed_link:Camera:0" not in str(sensor_name) and "zed" not in str(sensor_name):
                        continue
                    if not hasattr(sensor, "get_obs"):
                        continue
                    sensor_obs, _sensor_info = sensor.get_obs()
                    if isinstance(sensor_obs, dict) and "rgb" in sensor_obs:
                        rgb_value = sensor_obs["rgb"]
                        source = f"sensor:{sensor_name}"
                        break
            except Exception as e:
                cnt = int(getattr(self, "_low_level_head_rgb_sensor_fail_count", 0))
                if cnt < 5:
                    print(f"[low-nav-debug] failed to read head rgb from sensor: {e}", flush=True)
                setattr(self, "_low_level_head_rgb_sensor_fail_count", cnt + 1)
        if rgb_value is None:
            cnt = int(getattr(self, "_low_level_head_rgb_missing_count", 0))
            if cnt < 5:
                try:
                    keys = list(obs.keys())[:20] if isinstance(obs, dict) else type(obs).__name__
                except Exception:
                    keys = "<unavailable>"
                print(f"[low-nav-debug] head rgb not found for debug save obs_keys={keys}", flush=True)
            setattr(self, "_low_level_head_rgb_missing_count", cnt + 1)
            return

        import cv2
        import torch as th

        if isinstance(rgb_value, th.Tensor):
            rgb_arr = rgb_value.detach().cpu().numpy()
        else:
            rgb_arr = np.asarray(rgb_value)
        if rgb_arr.ndim == 3 and rgb_arr.shape[0] == 3 and rgb_arr.shape[-1] != 3:
            rgb_arr = np.transpose(rgb_arr, (1, 2, 0))
        if rgb_arr.dtype != np.uint8:
            rgb_arr = np.clip(rgb_arr, 0, 255).astype(np.uint8)

        debug_dir = os.environ.get("MOP_DEBUG_DIR")
        if debug_dir is None:
            nav_debug_dir = os.environ.get("OG_NAV_SAMPLING_DEBUG_DIR")
            if nav_debug_dir:
                nav_debug_path = Path(nav_debug_dir).expanduser()
                debug_root = nav_debug_path.parent if nav_debug_path.name == "nav_sampling" else nav_debug_path
            else:
                debug_root = Path("/tmp/embodiedclaw_low_level_debug/debug")
        else:
            debug_root = Path(debug_dir).expanduser()
        out_dir = debug_root / "low_level_head_rgb"
        out_dir.mkdir(parents=True, exist_ok=True)
        bgr = cv2.cvtColor(rgb_arr, cv2.COLOR_RGB2BGR)
        out_path = out_dir / f"{prefix}_step{int(executed_steps):06d}.png"
        if cv2.imwrite(str(out_path), bgr):
            cnt = int(getattr(self, "_low_level_head_rgb_saved_count", 0))
            if cnt < 5 or executed_steps % 50 == 0:
                print(
                    f"[low-nav-debug] saved low-level head rgb source={source} path={out_path}",
                    flush=True,
                )
            setattr(self, "_low_level_head_rgb_saved_count", cnt + 1)
        else:
            print(f"[low-nav-debug] failed to write low-level head rgb path={out_path}", flush=True)

    def _debug_root(self) -> Path:
        debug_dir = os.environ.get("MOP_DEBUG_DIR")
        if debug_dir is None:
            nav_debug_dir = os.environ.get("OG_NAV_SAMPLING_DEBUG_DIR")
            if nav_debug_dir:
                nav_debug_path = Path(nav_debug_dir).expanduser()
                return nav_debug_path.parent if nav_debug_path.name == "nav_sampling" else nav_debug_path
            return Path("/tmp/embodiedclaw_low_level_debug/debug")
        return Path(debug_dir).expanduser()

    def _maybe_save_move_gripper_debug_plot(
        self,
        *,
        arm: str,
        direction: str,
        distance: float,
        orient_tol: Optional[float],
        start_pos: np.ndarray,
        target_pos: np.ndarray,
        final_pos: np.ndarray,
        actual_positions: List[np.ndarray],
        world_dir: np.ndarray,
        pos_err: float,
        orient_err: float,
        orient_ok: bool,
        success: bool,
    ) -> None:
        if os.getenv("MOP_DEBUG", "0") not in ("1", "true", "True"):
            return

        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        planned_positions = np.asarray([start_pos, target_pos], dtype=np.float64)
        actual_positions_np = np.asarray(actual_positions or [start_pos, final_pos], dtype=np.float64)

        out_dir = self._debug_root() / "move_gripper"
        out_dir.mkdir(parents=True, exist_ok=True)
        idx = int(getattr(self, "_move_gripper_debug_plot_count", 0))
        setattr(self, "_move_gripper_debug_plot_count", idx + 1)
        out_path = out_dir / f"move_gripper_{idx:03d}_{arm}_{direction}_{float(distance):.3f}m.png"

        fig, ax = plt.subplots(figsize=(6, 6))
        ax.plot(planned_positions[:, 0], planned_positions[:, 1], "k--", linewidth=1.5, label="planned straight path")
        ax.scatter(start_pos[0], start_pos[1], c="green", s=60, label="start gripper")
        ax.scatter(target_pos[0], target_pos[1], c="black", marker="x", s=80, label="target gripper")
        ax.plot(actual_positions_np[:, 0], actual_positions_np[:, 1], "tab:blue", linewidth=2.0, label="actual gripper")
        ax.scatter(final_pos[0], final_pos[1], c="tab:blue", s=50, label="final gripper")

        arrow_scale = max(float(distance), 1e-3)
        ax.arrow(
            start_pos[0],
            start_pos[1],
            world_dir[0] * arrow_scale,
            world_dir[1] * arrow_scale,
            width=0.002,
            head_width=0.025,
            length_includes_head=True,
            color="tab:orange",
            label="command direction",
        )

        info = (
            f"direction: {direction}\n"
            f"distance: {float(distance):.3f} m\n"
            f"final orient err: {orient_err:.4f} rad\n"
            f"orient tol: {orient_tol}\n"
            f"pos err: {pos_err:.4f} m\n"
            f"success: {success}, orient_ok: {orient_ok}"
        )
        ax.text(
            0.02,
            0.98,
            info,
            transform=ax.transAxes,
            va="top",
            ha="left",
            fontsize=9,
            bbox={"facecolor": "white", "alpha": 0.85, "edgecolor": "0.7"},
        )

        ax.set_aspect("equal", adjustable="box")
        ax.set_xlabel("world x (m)")
        ax.set_ylabel("world y (m)")
        ax.set_title(f"Move gripper debug ({arm}, top-down XY)")
        ax.grid(True, alpha=0.3)
        ax.legend(loc="best", fontsize=8)
        fig.tight_layout()
        fig.savefig(out_path, dpi=160)
        plt.close(fig)
        logger.info("[low-gripper-debug] saved move_gripper debug plot: %s", out_path)
        print(f"[low-gripper-debug] saved move_gripper debug plot: {out_path}", flush=True)

    def navigate_to_pose(
        self,
        pose: Union[PoseLike, Sequence[float]],
        *,
        distance_tol: float = 0.3,
        yaw_tol: float = 0.3,
        timeout_steps: int = 1000,
    ) -> PrimitiveResult:
        if self.env is None:
            return PrimitiveResult(
                success=False,
                primitive="navigate_to_pose",
                message="env is required to execute navigation",
            )

        pose_2d_np = self._pose_to_2d(pose)
        primitives = self._get_nav_primitives()
        import torch as th

        # Distance-gated nav: CuRobo BASE trajopt cannot plan multi-metre base navs (it returns
        # "no successful trajectory"). For long distances use MoP's RRT-Connect base transit
        # (curobo handles short ones) — same split as mop_generation/path_execution/curobo.py.
        _cur = self._to_numpy(self.robot.get_position_orientation()[0]).astype(np.float64)
        _navd = float(np.hypot(pose_2d_np[0] - _cur[0], pose_2d_np[1] - _cur[1]))
        _rrt_min = float(os.environ.get("MOP_NAV_RRT_MIN_DIST", self.config.get("nav_rrt_min_dist", 0.5)) or 0.5)
        if _navd >= _rrt_min:
            return self._navigate_via_rrt(
                primitives, pose_2d_np,
                distance_tol=distance_tol, yaw_tol=yaw_tol, timeout_steps=timeout_steps,
            )

        pose_2d = th.tensor(pose_2d_np, dtype=th.float32)
        q_traj = None
        try:
            q_traj, _nav_pos, _nav_quat = self._plan_starter_navigation_trajectory(primitives, pose_2d)
            # Same execution semantics as navigate_to_object: budget scales with path
            # length (the 0.05-spaced base interp paces ~4mm/step), and per-waypoint
            # tracking misses must not abort mid-drive — the outcome is gated by the
            # final distance_tol/yaw_tol check below, not by 5mm waypoint tracking.
            timeout_steps = max(int(timeout_steps), 12 * len(q_traj))
            print(
                "[low-nav-debug] trajectory planned; executing motion plan "
                f"shape={tuple(q_traj.shape)} target_pose_2d={pose_2d_np.tolist()} "
                f"timeout_steps={int(timeout_steps)} "
                f"first5_base={q_traj[:5, self.robot.base_control_idx].detach().cpu().tolist()}",
                flush=True,
            )
            executed_steps = 0
            timed_out = False
            for action in primitives._execute_motion_plan(q_traj, ignore_failure=True):
                if action is None:
                    continue
                obs, reward, terminated, truncated, info = self.env.step(action)
                executed_steps += 1
                self._maybe_save_low_level_head_rgb_debug(
                    obs,
                    executed_steps=executed_steps,
                    prefix="navigate_to_pose",
                )
                if executed_steps <= 5 or executed_steps % 50 == 0:
                    robot_pos, robot_quat = self.robot.get_position_orientation()
                    print(
                        "[low-nav-debug] "
                        f"executed_steps={executed_steps} "
                        f"action={self._to_numpy(action).astype(np.float32).tolist()} "
                        f"robot_pos={self._to_numpy(robot_pos).astype(np.float32).tolist()} ",
                        flush=True,
                    )
                if terminated or truncated:
                    print(
                        "[low-nav-debug] navigation execution stopped by episode state "
                        f"executed_steps={executed_steps} terminated={terminated} truncated={truncated}",
                        flush=True,
                    )
                    break
                if executed_steps >= int(timeout_steps):
                    timed_out = True
                    print(
                        "[low-nav-debug] navigation execution timed out "
                        f"executed_steps={executed_steps} timeout_steps={int(timeout_steps)}",
                        flush=True,
                    )
                    break
        except Exception as e:
            return PrimitiveResult(
                success=False,
                primitive="navigate_to_pose",
                message=f"navigation failed: {e}",
                data={
                    "target_pose_2d": pose_2d_np.tolist(),
                    "nav_traj_shape": list(q_traj.shape) if q_traj is not None else None,
                    "nav_traj_first": self._to_numpy(q_traj[0]).astype(np.float32).tolist() if q_traj is not None else None,
                    "nav_traj_last": self._to_numpy(q_traj[-1]).astype(np.float32).tolist() if q_traj is not None else None,
                },
            )

        return self._navigation_result_from_pose(
            primitive="navigate_to_pose",
            pose_2d_np=pose_2d_np,
            distance_tol=distance_tol,
            yaw_tol=yaw_tol,
            timeout_steps=timeout_steps,
            executed_steps=executed_steps,
            timed_out=timed_out,
            trajectory=q_traj,
        )

    def _navigate_via_rrt(
        self,
        primitives: Any,
        pose_2d_np: np.ndarray,
        *,
        distance_tol: float,
        yaw_tol: float,
        timeout_steps: int,
    ) -> PrimitiveResult:
        """Drive the base to a far 2D pose via MoP's RRT-Connect base transit.

        CuRobo BASE trajopt cannot plan multi-metre navs; the RRT searches continuous SE(2)
        with the exact oriented-footprint collision oracle and drives the base kinematically,
        yielding one hold-action per env.step. The held object is attached for the footprint
        check (it never adds collision when carried compact) so the carry path is a base path.
        """
        from omnigibson.learning.embodiedClaw_recovery.executor.nav_transit import make_base_transit

        class _NavCtrl:
            """NavController for the nav toolbox: the robot + curobo motion generator + the
            action post-processor (make_base_transit / _stream_traj read these)."""

            def __init__(self, robot, motion_generator, postprocess_action):
                self.robot = robot
                self.motion_generator = motion_generator
                self.postprocess_action = postprocess_action

        # Prefer the wrapping backend's (warmed, place-proven) curobo if it set it on the engine;
        # the freshly-built nav curobo from _get_nav_primitives is more crash-prone for the RRT's
        # batched collision sweep (the proven RRT drive ran off the backend curobo).
        cmg = getattr(self, "_motion_generator", None) or getattr(primitives, "_motion_generator", None)
        navctrl = _NavCtrl(self.robot, cmg, getattr(primitives, "_postprocess_action", lambda a: a))
        joint_names = list(getattr(cmg, "robot_joint_names", []) or list(self.robot.joints.keys()))
        held_names = tuple(
            o.name for o in (getattr(self.robot, "_ag_obj_in_hand", None) or {}).values() if o is not None
        )
        # Goal yaw: the sampler faces the object (grasp-oriented), but for a held-object PLACE the
        # base need NOT face the object (curobo plans the arm to the shelf from any base yaw), and
        # facing the object near a tall rack forces the carried-arm footprint to sweep into collision
        # — the RRT then finds no path. MOP_NAV_GOAL_YAW_MODE=keep_current navigates to the position
        # at the robot's current (collision-free carry) yaw instead. (2026-06-26)
        _yaw_mode = os.environ.get("MOP_NAV_GOAL_YAW_MODE", "sampler")
        if _yaw_mode == "keep_current":
            import omnigibson.utils.transform_utils as _Tu
            _cyaw = float(_Tu.quat2euler(self.robot.get_position_orientation()[1])[2])
            goal = (float(pose_2d_np[0]), float(pose_2d_np[1]), _cyaw)
            print(f"[low-nav-debug] goal yaw -> current carry yaw {_cyaw:.2f} (place handles the arm)", flush=True)
        else:
            goal = (float(pose_2d_np[0]), float(pose_2d_np[1]), float(pose_2d_np[2]))
        executed_steps = 0
        timed_out = False
        print(f"[low-nav-debug] RRT base transit -> goal={goal} ignore_held={held_names}", flush=True)
        try:
            for action in make_base_transit(
                controller=navctrl,
                goal_xytheta=goal,
                env=self.env,
                planner="rrt",
                ignore_object_names=held_names,
                base_z=None,
            ):
                if action is None:
                    continue
                self.env.step(action)
                executed_steps += 1
                if executed_steps >= int(timeout_steps):
                    timed_out = True
                    break
        except Exception as e:
            return PrimitiveResult(
                success=False,
                primitive="navigate_to_pose",
                message=f"navigation failed (rrt): {e}",
                data={"target_pose_2d": pose_2d_np.tolist(), "executed_steps": executed_steps},
            )

        # Judge success against the ACTUAL goal we drove to (goal yaw may have been overridden to the
        # carry yaw), not the original sampler pose — else a correct drive reads as a yaw failure.
        goal_2d_np = np.asarray([goal[0], goal[1], goal[2]], dtype=np.float64)
        return self._navigation_result_from_pose(
            primitive="navigate_to_pose",
            pose_2d_np=goal_2d_np,
            distance_tol=distance_tol,
            yaw_tol=yaw_tol,
            timeout_steps=timeout_steps,
            executed_steps=executed_steps,
            timed_out=timed_out,
            trajectory=None,
        )

    def navigate_to_object(
        self,
        object_name: str,
        *,
        distance_tol: float = 0.3,
        timeout_steps: int = 1000,
        reach_target_world: Optional[Tuple[float, float, float]] = None,
        step_cb: Optional[Callable[..., None]] = None,
    ) -> PrimitiveResult:
        if self.env is None:
            return PrimitiveResult(
                success=False,
                primitive="navigate_to_object",
                object_name=object_name,
                message="env is required to execute navigation",
            )

        try:
            target_obj = self._resolve_object(object_name)
            print(f"[low-nav-debug] resolved target_obj={target_obj!r}", flush=True)
            primitives = self._get_nav_primitives()

            sampling_attempts = int(
                self.config.get(
                    "nav_sampling_attempts",
                    os.environ.get("MOP_NAV_SAMPLING_ATTEMPTS", 50),
                )
            )
            max_pose_tries = int(
                self.config.get(
                    "nav_max_pose_tries",
                    os.environ.get("MOP_NAV_MAX_POSE_TRIES", 5),
                )
            )
            nav_goal_mode = os.environ.get("MOP_NAV_GOAL_MODE", "free_dock").strip().lower()
            nav_goal_distance = float(os.environ.get("MOP_NAV_GOAL_DISTANCE", "1.5"))
            if nav_goal_mode in ("towards_target", "toward_target", "robot_to_target"):
                max_pose_tries = 1
                print(
                    "[low-nav-debug] fixed nav goal mode "
                    f"mode={nav_goal_mode} max_pose_tries={max_pose_tries}",
                    flush=True,
                )

            # free_dock (DEFAULT): INFORMED docking — enumerate ranked dock poses out of the
            # FREE graspable region (occupancy inflated by the FROZEN-arm FK radius), instead of
            # blind-sample-then-reject. The arm stays FROZEN — NO carry-tuck: if the free annulus
            # is empty we widen the standoff, never tuck. Each candidate is still exact-checked
            # (curobo chassis+arm) by _navigate_via_rrt downstream; this just stops us forming
            # poses behind walls / inside the object / where the frozen arm cannot fit.
            _dock_cands: List[Tuple[float, float, float]] = []
            if nav_goal_mode == "free_dock":
                from omnigibson.learning.embodiedClaw_recovery.executor.nav_search import (
                    free_dock_candidates,
                )
                _cmg = getattr(self, "_motion_generator", None) or getattr(primitives, "_motion_generator", None)
                _jn = list(getattr(_cmg, "robot_joint_names", []) or list(self.robot.joints.keys()))
                _dmax = float(os.environ.get("MOP_DOCK_DMAX", "0.78"))
                _dock_cands = free_dock_candidates(self.env, self.robot, target_obj,
                                                   cmg=_cmg, joint_names=_jn, d_max=_dmax,
                                                   reach_target_world=reach_target_world)
                if not _dock_cands:   # fallback rung: widen the standoff (NEVER tuck the arm)
                    _dock_cands = free_dock_candidates(self.env, self.robot, target_obj,
                                                       cmg=_cmg, joint_names=_jn, d_max=_dmax + 0.40,
                                                       reach_target_world=reach_target_world)
                max_pose_tries = max(1, len(_dock_cands))
                print(f"[low-nav-debug] free_dock: {len(_dock_cands)} ranked free-space dock "
                      f"candidates (verifying best-first)", flush=True)

            q_traj = None
            nav_pos = None
            nav_quat = None
            pose_2d = None
            pose_try_errors: List[str] = []

            import torch as th

            for pose_try in range(max_pose_tries):
                force_pose_2d = os.environ.get("MOP_NAV_FORCE_POSE2D")
                if nav_goal_mode == "free_dock":
                    pose_2d_th = (th.tensor(_dock_cands[pose_try], dtype=th.float32)
                                  if pose_try < len(_dock_cands) else None)
                    if pose_2d_th is not None:
                        print(f"[low-nav-debug] pose_try={pose_try} free_dock candidate "
                              f"{_dock_cands[pose_try]}", flush=True)
                elif nav_goal_mode in ("towards_target", "toward_target", "robot_to_target"):
                    robot_pos_now, _ = self.robot.get_position_orientation()
                    target_pos_now, _ = target_obj.get_position_orientation()
                    robot_xy = th.as_tensor(robot_pos_now[:2], dtype=th.float32)
                    target_xy = th.as_tensor(target_pos_now[:2], dtype=th.float32)
                    delta_xy = target_xy - robot_xy
                    dist_xy = th.norm(delta_xy)
                    if float(dist_xy.item()) < 1e-6:
                        pose_try_errors.append(f"pose_try={pose_try}: robot and target XY are coincident")
                        continue
                    direction_xy = delta_xy / dist_xy
                    goal_xy = robot_xy + direction_xy * nav_goal_distance
                    yaw = th.atan2(delta_xy[1], delta_xy[0])
                    pose_2d_th = th.stack([goal_xy[0], goal_xy[1], yaw]).to(dtype=th.float32)
                    print(
                        "[low-nav-debug] "
                        f"pose_try={pose_try} using {nav_goal_mode} pose_2d={pose_2d_th.detach().cpu().tolist()} "
                        f"robot_xy={robot_xy.detach().cpu().tolist()} target_xy={target_xy.detach().cpu().tolist()} "
                        f"distance={nav_goal_distance:.3f}",
                        flush=True,
                    )
                elif force_pose_2d:
                    try:
                        parts = [p.strip() for p in force_pose_2d.replace("(", "").replace(")", "").split(",")]
                        if len(parts) != 3:
                            raise ValueError("expected 3 comma-separated floats: x,y,yaw")
                        pose_2d_th = th.tensor([float(parts[0]), float(parts[1]), float(parts[2])], dtype=th.float32)
                        print(
                            "[low-nav-debug] "
                            f"pose_try={pose_try} using forced pose_2d={pose_2d_th.detach().cpu().tolist()} "
                            "from MOP_NAV_FORCE_POSE2D",
                            flush=True,
                        )
                    except Exception as e:
                        print(
                            "[low-nav-debug] "
                            f"pose_try={pose_try} invalid MOP_NAV_FORCE_POSE2D={force_pose_2d!r} ({e}); falling back to sampling",
                            flush=True,
                        )
                        pose_2d_th = self._sample_navigation_pose_near_object(
                            primitives,
                            target_obj,
                            sampling_attempts=sampling_attempts,
                        )
                        print(
                            "[low-nav-debug] "
                            f"pose_try={pose_try} _sample_pose_near_object attempts={sampling_attempts} -> {pose_2d_th}",
                            flush=True,
                        )
                else:
                    pose_2d_th = self._sample_navigation_pose_near_object(
                        primitives,
                        target_obj,
                        sampling_attempts=sampling_attempts,
                    )
                    print(
                        "[low-nav-debug] "
                        f"pose_try={pose_try} _sample_pose_near_object attempts={sampling_attempts} -> {pose_2d_th}",
                        flush=True,
                    )

                if pose_2d_th is None:
                    pose_try_errors.append(f"pose_try={pose_try}: sample_pose_near_object returned None")
                    continue

                pose_2d = self._to_numpy(pose_2d_th).astype(np.float32)
                print(
                    "[low-nav-debug] "
                    f"pose_try={pose_try} sampled pose_2d={pose_2d.tolist()}",
                    flush=True,
                )

                # Distance-gated: CuRobo BASE trajopt cannot plan multi-metre navs. For a far
                # sampled stance, drive via MoP's RRT-Connect base transit (curobo for short).
                _cur_xy = self._to_numpy(self.robot.get_position_orientation()[0]).astype(np.float64)
                _navd = float(np.hypot(float(pose_2d[0]) - _cur_xy[0], float(pose_2d[1]) - _cur_xy[1]))
                _rrt_min = float(os.environ.get("MOP_NAV_RRT_MIN_DIST", self.config.get("nav_rrt_min_dist", 0.5)) or 0.5)
                if _navd >= _rrt_min:
                    print(
                        f"[low-nav-debug] pose_try={pose_try} nav dist {_navd:.2f}m >= {_rrt_min}m "
                        f"-> RRT base transit to {pose_2d.tolist()}",
                        flush=True,
                    )
                    res = self._navigate_via_rrt(
                        primitives, pose_2d,
                        distance_tol=distance_tol, yaw_tol=distance_tol, timeout_steps=timeout_steps,
                    )
                    if getattr(res, "success", False):
                        res.primitive = "navigate_to_object"
                        res.object_name = object_name
                        return res
                    pose_try_errors.append(f"pose_try={pose_try}: rrt nav failed: {getattr(res, 'message', '')}")
                    print(
                        f"[low-nav-debug] pose_try={pose_try} RRT nav failed; trying another pose: "
                        f"{getattr(res, 'message', '')}",
                        flush=True,
                    )
                    continue

                try:
                    q_traj, nav_pos, nav_quat = self._plan_starter_navigation_trajectory(primitives, pose_2d_th)
                except Exception as e:
                    pose_try_errors.append(f"pose_try={pose_try}: {e}")
                    print(
                        "[low-nav-debug] "
                        f"pose_try={pose_try} planning failed; trying another pose if available: {e}",
                        flush=True,
                    )
                    continue

                break

            if q_traj is None or nav_pos is None or nav_quat is None or pose_2d is None:
                msg = f"could not plan navigation after {max_pose_tries} pose tries"
                if pose_try_errors:
                    msg += " (" + "; ".join(pose_try_errors[-5:]) + ")"
                return PrimitiveResult(
                    success=False,
                    primitive="navigate_to_object",
                    object_name=object_name,
                    message=msg,
                    data={
                        "sampling_attempts": sampling_attempts,
                        "max_pose_tries": max_pose_tries,
                        "pose_try_errors": pose_try_errors,
                    },
                )

            # scale the step budget with the planned path: _execute_motion_plan paces
            # up to ~10 steps per waypoint, so a fixed cap dies mid-room on multi-metre
            # traverses regardless of tracking health
            timeout_steps = max(int(timeout_steps), 12 * len(q_traj))
            print(
                "[low-nav-debug] trajectory planned; executing motion plan "
                f"shape={tuple(q_traj.shape)} target_object={object_name} "
                f"target_pose_2d={pose_2d.tolist()} timeout_steps={int(timeout_steps)} "
                f"first5_base={q_traj[:5, self.robot.base_control_idx].detach().cpu().tolist()}",
                flush=True,
            )
            executed_steps = 0
            timed_out = False
            # ignore_failure: a waypoint the base cannot reach within 10 steps to 5mm
            # (stock MAX_STEPS_FOR_JOINT_MOTION/DEFAULT_DIST_THRESHOLD) must not abort
            # the whole navigation (observed: killed at 5.8mm) — the outcome is gated
            # below by _navigation_result_from_pose (distance_tol) instead.
            for action in primitives._execute_motion_plan(q_traj, ignore_failure=True):
                if action is None:
                    continue
                obs, reward, terminated, truncated, info = self.env.step(action)
                executed_steps += 1
                if step_cb is not None:
                    step_cb(action=action, obs=obs, reward=reward,
                            terminated=terminated, truncated=truncated, info=info)
                self._maybe_save_low_level_head_rgb_debug(
                    obs,
                    executed_steps=executed_steps,
                    prefix="navigate_to_object",
                )
                if executed_steps <= 5 or executed_steps % 50 == 0:
                    robot_pos, robot_quat = self.robot.get_position_orientation()
                    print(
                        "[low-nav-debug] "
                        f"executed_steps={executed_steps} "
                        f"action={self._to_numpy(action).astype(np.float32).tolist()} "
                        f"robot_pos={self._to_numpy(robot_pos).astype(np.float32).tolist()} "
                        f"robot_quat={self._to_numpy(robot_quat).astype(np.float32).tolist()}",
                        flush=True,
                    )
                if terminated or truncated:
                    print(
                        "[low-nav-debug] navigation execution stopped by episode state "
                        f"executed_steps={executed_steps} terminated={terminated} truncated={truncated}",
                        flush=True,
                    )
                    break
                if executed_steps >= int(timeout_steps):
                    timed_out = True
                    print(
                        "[low-nav-debug] navigation execution timed out "
                        f"executed_steps={executed_steps} timeout_steps={int(timeout_steps)}",
                        flush=True,
                    )
                    break
            return self._navigation_result_from_pose(
                primitive="navigate_to_object",
                object_name=object_name,
                target_obj=target_obj,
                pose_2d_np=pose_2d,
                distance_tol=distance_tol,
                yaw_tol=float(self.config.get("nav_yaw_tol", 0.3)),
                timeout_steps=timeout_steps,
                executed_steps=executed_steps,
                timed_out=timed_out,
                trajectory=q_traj,
            )
        except Exception as e:
            return PrimitiveResult(
                success=False,
                primitive="navigate_to_object",
                object_name=object_name,
                message=f"navigation failed: {e}",
                data={
                    "nav_traj_shape": list(q_traj.shape) if "q_traj" in locals() and q_traj is not None else None,
                    "nav_traj_first": self._to_numpy(q_traj[0]).astype(np.float32).tolist()
                    if "q_traj" in locals() and q_traj is not None
                    else None,
                    "nav_traj_last": self._to_numpy(q_traj[-1]).astype(np.float32).tolist()
                    if "q_traj" in locals() and q_traj is not None
                    else None,
                },
            )

    def iter_navigate_to_object(
        self,
        object_name: str,
        *,
        distance_tol: float = 0.3,
        timeout_steps: int = 1000,
    ) -> Iterator[Any]:
        if self.env is None:
            raise RuntimeError("env is required to execute navigation")

        target_obj = self._resolve_object(object_name)
        print(f"[low-nav-debug] resolved target_obj={target_obj!r}", flush=True)
        primitives = self._get_nav_primitives()

        sampling_attempts = int(
            self.config.get(
                "nav_sampling_attempts",
                os.environ.get("MOP_NAV_SAMPLING_ATTEMPTS", 50),
            )
        )
        max_pose_tries = int(
            self.config.get(
                "nav_max_pose_tries",
                os.environ.get("MOP_NAV_MAX_POSE_TRIES", 5),
            )
        )
        nav_goal_mode = os.environ.get("MOP_NAV_GOAL_MODE", "sample").strip().lower()
        nav_goal_distance = float(os.environ.get("MOP_NAV_GOAL_DISTANCE", "1.5"))
        if nav_goal_mode in ("towards_target", "toward_target", "robot_to_target"):
            max_pose_tries = 1
            print(
                "[low-nav-debug] fixed nav goal mode "
                f"mode={nav_goal_mode} max_pose_tries={max_pose_tries}",
                flush=True,
            )

        q_traj = None
        nav_pos = None
        nav_quat = None
        pose_2d = None
        pose_try_errors: List[str] = []

        import torch as th

        for pose_try in range(max_pose_tries):
            force_pose_2d = os.environ.get("MOP_NAV_FORCE_POSE2D")
            if nav_goal_mode in ("towards_target", "toward_target", "robot_to_target"):
                robot_pos_now, _ = self.robot.get_position_orientation()
                target_pos_now, _ = target_obj.get_position_orientation()
                robot_xy = th.as_tensor(robot_pos_now[:2], dtype=th.float32)
                target_xy = th.as_tensor(target_pos_now[:2], dtype=th.float32)
                delta_xy = target_xy - robot_xy
                dist_xy = th.norm(delta_xy)
                if float(dist_xy.item()) < 1e-6:
                    pose_try_errors.append(f"pose_try={pose_try}: robot and target XY are coincident")
                    continue
                direction_xy = delta_xy / dist_xy
                goal_xy = robot_xy + direction_xy * nav_goal_distance
                yaw = th.atan2(delta_xy[1], delta_xy[0])
                pose_2d_th = th.stack([goal_xy[0], goal_xy[1], yaw]).to(dtype=th.float32)
                print(
                    "[low-nav-debug] "
                    f"pose_try={pose_try} using {nav_goal_mode} pose_2d={pose_2d_th.detach().cpu().tolist()} "
                    f"robot_xy={robot_xy.detach().cpu().tolist()} target_xy={target_xy.detach().cpu().tolist()} "
                    f"distance={nav_goal_distance:.3f}",
                    flush=True,
                )
            elif force_pose_2d:
                try:
                    parts = [p.strip() for p in force_pose_2d.replace("(", "").replace(")", "").split(",")]
                    if len(parts) != 3:
                        raise ValueError("expected 3 comma-separated floats: x,y,yaw")
                    pose_2d_th = th.tensor([float(parts[0]), float(parts[1]), float(parts[2])], dtype=th.float32)
                    print(
                        "[low-nav-debug] "
                        f"pose_try={pose_try} using forced pose_2d={pose_2d_th.detach().cpu().tolist()} "
                        "from MOP_NAV_FORCE_POSE2D",
                        flush=True,
                    )
                except Exception as e:
                    print(
                        "[low-nav-debug] "
                        f"pose_try={pose_try} invalid MOP_NAV_FORCE_POSE2D={force_pose_2d!r} ({e}); falling back to sampling",
                        flush=True,
                    )
                    pose_2d_th = self._sample_navigation_pose_near_object(
                        primitives,
                        target_obj,
                        sampling_attempts=sampling_attempts,
                    )
                    print(
                        "[low-nav-debug] "
                        f"pose_try={pose_try} _sample_pose_near_object attempts={sampling_attempts} -> {pose_2d_th}",
                        flush=True,
                    )
            else:
                pose_2d_th = self._sample_navigation_pose_near_object(
                    primitives,
                    target_obj,
                    sampling_attempts=sampling_attempts,
                )
                print(
                    "[low-nav-debug] "
                    f"pose_try={pose_try} _sample_pose_near_object attempts={sampling_attempts} -> {pose_2d_th}",
                    flush=True,
                )

            if pose_2d_th is None:
                pose_try_errors.append(f"pose_try={pose_try}: sample_pose_near_object returned None")
                continue

            pose_2d = self._to_numpy(pose_2d_th).astype(np.float32)
            print(
                "[low-nav-debug] "
                f"pose_try={pose_try} sampled pose_2d={pose_2d.tolist()}",
                flush=True,
            )

            try:
                q_traj, nav_pos, nav_quat = self._plan_starter_navigation_trajectory(primitives, pose_2d_th)
            except Exception as e:
                pose_try_errors.append(f"pose_try={pose_try}: {e}")
                print(
                    "[low-nav-debug] "
                    f"pose_try={pose_try} planning failed; trying another pose if available: {e}",
                    flush=True,
                )
                continue

            break

        if q_traj is None or nav_pos is None or nav_quat is None or pose_2d is None:
            msg = f"could not plan navigation after {max_pose_tries} pose tries"
            if pose_try_errors:
                msg += " (" + "; ".join(pose_try_errors[-5:]) + ")"
            raise RuntimeError(msg)

        print(
            "[low-nav-debug] trajectory planned; yielding motion plan "
            f"shape={tuple(q_traj.shape)} target_object={object_name} "
            f"target_pose_2d={pose_2d.tolist()} timeout_steps={int(timeout_steps)} "
            f"first5_base={q_traj[:5, self.robot.base_control_idx].detach().cpu().tolist()}",
            flush=True,
        )

        executed_steps = 0
        timed_out = False
        for action in primitives._execute_motion_plan(q_traj):
            if action is None:
                continue
            yield action
            executed_steps += 1
            if executed_steps <= 5 or executed_steps % 50 == 0:
                robot_pos, robot_quat = self.robot.get_position_orientation()
                print(
                    "[low-nav-debug] "
                    f"yielded_steps={executed_steps} "
                    f"action={self._to_numpy(action).astype(np.float32).tolist()} "
                    f"robot_pos={self._to_numpy(robot_pos).astype(np.float32).tolist()} "
                    f"robot_quat={self._to_numpy(robot_quat).astype(np.float32).tolist()}",
                    flush=True,
                )
            if executed_steps >= int(timeout_steps):
                timed_out = True
                print(
                    "[low-nav-debug] navigation execution timed out "
                    f"executed_steps={executed_steps} timeout_steps={int(timeout_steps)}",
                    flush=True,
                )
                break

        result = self._navigation_result_from_pose(
            primitive="navigate_to_object",
            object_name=object_name,
            target_obj=target_obj,
            pose_2d_np=pose_2d,
            distance_tol=distance_tol,
            yaw_tol=float(self.config.get("nav_yaw_tol", 0.3)),
            timeout_steps=timeout_steps,
            executed_steps=executed_steps,
            timed_out=timed_out,
            trajectory=q_traj,
        )
        print(f"[low-nav-debug] yielded navigation result={result}", flush=True)
        if not result.success:
            raise RuntimeError(result.message or "navigate_to_object failed")

    # ---------------------------------------------------------------------
    # Arm / gripper control
    # ---------------------------------------------------------------------
    def move_eef_to_pose(
        self,
        arm: ArmName,
        pose: PoseLike,
        *,
        timeout_steps: int = 500,
        avoid_collision: bool = True,
        motion_constraint: Optional[Sequence[float]] = None,
    ) -> PrimitiveResult:
        arm = self._validate_arm(arm)
        if self.env is None:
            return PrimitiveResult(
                success=False,
                primitive="move_eef_to_pose",
                message="env is required to execute arm motion",
                arm=arm,
            )

        steps = 0
        try:
            ignore_objects = [] if avoid_collision else list(self.config.get("arm_ignore_objects", []))
            for action in self.iter_move_eef_to_pose(
                arm,
                pose,
                ignore_objects=ignore_objects,
                motion_constraint=motion_constraint,
            ):
                self.env.step(action, n_render_iterations=1)
                steps += 1
                if steps >= int(timeout_steps):
                    return PrimitiveResult(
                        success=False,
                        primitive="move_eef_to_pose",
                        message="timed out while moving eef to pose",
                        arm=arm,
                        data={"steps": steps, "timeout_steps": timeout_steps},
                    )
        except Exception as e:
            return PrimitiveResult(
                success=False,
                primitive="move_eef_to_pose",
                message=f"arm motion failed: {e}",
                arm=arm,
                data={"steps": steps},
            )

        target_pos, _ = self._pose_to_arrays(pose)
        current_pos = self._to_numpy(self.robot.get_eef_position(arm)).astype(np.float64)
        pos_err = float(np.linalg.norm(current_pos - target_pos))
        tol = float(self.config.get("eef_pos_tol", 0.03))
        return PrimitiveResult(
            success=pos_err <= tol,
            primitive="move_eef_to_pose",
            message="eef reached target pose" if pos_err <= tol else "eef did not reach target pose",
            arm=arm,
            data={"steps": steps, "pos_err": pos_err, "pos_tol": tol},
        )

    def move_eef_along_direction(
        self,
        arm: ArmName,
        *,
        direction: Sequence[float],
        distance: float,
        timeout_steps: int = 300,
    ) -> PrimitiveResult:
        arm = self._validate_arm(arm)
        current_pos = self._to_numpy(self.robot.get_eef_position(arm)).astype(np.float64)
        current_quat = self._to_numpy(self.robot.get_eef_orientation(arm)).astype(np.float64)
        direction_np = np.asarray(direction, dtype=np.float64)
        norm = np.linalg.norm(direction_np)
        if norm < 1e-8:
            return PrimitiveResult(
                success=False,
                primitive="move_eef_along_direction",
                message="direction must be nonzero",
                arm=arm,
            )
        target_pos = current_pos + direction_np / norm * float(distance)
        return self.move_eef_to_pose(
            arm,
            {"pos": target_pos, "quat": current_quat},
            timeout_steps=timeout_steps,
            avoid_collision=False,
        )

    def move_gripper(
        self,
        arm: ArmName,
        *,
        direction: Optional[str] = None,
        distance: Optional[float] = None,
        target_pose: Optional[PoseLike] = None,
        target_pos: Optional[Sequence[float]] = None,
        target_quat: Optional[Sequence[float]] = None,
        orient_tol: Optional[float] = 0.10,
        timeout_steps: int = 300,
        avoid_collision: bool = False,
    ) -> PrimitiveResult:
        arm = self._validate_arm(arm)
        if self.env is None:
            return PrimitiveResult(
                success=False,
                primitive="move_gripper",
                message="env is required to execute gripper motion",
                arm=arm,
            )

        direction_map = {
            "front": np.asarray([1.0, 0.0, 0.0], dtype=np.float64),
            "back": np.asarray([-1.0, 0.0, 0.0], dtype=np.float64),
            "left": np.asarray([0.0, 1.0, 0.0], dtype=np.float64),
            "right": np.asarray([0.0, -1.0, 0.0], dtype=np.float64),
            "up": np.asarray([0.0, 0.0, 1.0], dtype=np.float64),
            "down": np.asarray([0.0, 0.0, -1.0], dtype=np.float64),
        }
        _, base_quat = self.robot.get_position_orientation()
        base_quat = self._quat_normalize(self._to_numpy(base_quat).astype(np.float64))
        current_pos = self._to_numpy(self.robot.get_eef_position(arm)).astype(np.float64)
        current_quat = self._quat_normalize(self._to_numpy(self.robot.get_eef_orientation(arm)).astype(np.float64))
        mode = "direction"
        if target_pose is not None:
            target_pos, target_quat = self._pose_to_arrays(target_pose)
            mode = "target_pose"
        elif target_pos is not None:
            target_pos = np.asarray(target_pos, dtype=np.float64)
            target_quat = current_quat if target_quat is None else self._quat_normalize(target_quat)
            mode = "target_pose"
        else:
            if direction is None:
                direction = "front"
            if distance is None:
                distance = 0.05
            if direction not in direction_map:
                return PrimitiveResult(
                    success=False,
                    primitive="move_gripper",
                    message=f"direction must be one of {sorted(direction_map)}, got {direction!r}",
                    arm=arm,
                )
            world_dir = self._rotate_vector(base_quat, direction_map[direction])
            world_dir = world_dir / max(np.linalg.norm(world_dir), 1e-8)
            target_pos = current_pos + world_dir * float(distance)
            target_quat = current_quat

        target_pos = np.asarray(target_pos, dtype=np.float64)
        target_quat = self._quat_normalize(target_quat)
        move_vec = target_pos - current_pos
        move_dist = float(np.linalg.norm(move_vec))
        world_dir = move_vec / max(move_dist, 1e-8)
        if direction is None:
            direction = "target_pose"
        if distance is None:
            distance = move_dist

        debug_msg = (
            f"[low-gripper-debug] move_gripper start arm={arm} mode={mode} direction={direction} "
            f"distance={float(distance)} orient_tol={orient_tol} "
            f"current_pos={current_pos.tolist()} target_pos={target_pos.tolist()} "
            f"current_quat={current_quat.tolist()} target_quat={target_quat.tolist()} "
            f"world_dir={world_dir.tolist()}"
        )
        logger.info(debug_msg)
        print(debug_msg, flush=True)

        steps = 0
        actual_positions = [current_pos.copy()]
        planner_success = True
        planner_message = "eef reached target pose"
        try:
            ignore_objects = [] if avoid_collision else list(self.config.get("arm_ignore_objects", []))
            for action in self.iter_move_eef_to_pose(arm, {"pos": target_pos, "quat": target_quat}, ignore_objects=ignore_objects):
                self.env.step(action, n_render_iterations=1)
                steps += 1
                actual_positions.append(self._to_numpy(self.robot.get_eef_position(arm)).astype(np.float64))
                if steps >= int(timeout_steps):
                    planner_success = False
                    planner_message = "timed out while moving gripper"
                    break
        except Exception as e:
            planner_success = False
            planner_message = f"gripper motion failed: {e}"

        final_pos = self._to_numpy(self.robot.get_eef_position(arm)).astype(np.float64)
        final_quat = self._quat_normalize(self._to_numpy(self.robot.get_eef_orientation(arm)).astype(np.float64))
        pos_err = float(np.linalg.norm(final_pos - target_pos))
        pos_tol = float(self.config.get("eef_pos_tol", 0.03))
        pos_ok = pos_err <= pos_tol
        orient_err = self._quat_angle_diff(final_quat, target_quat)
        orient_ok = True if orient_tol is None else orient_err <= float(orient_tol)
        success = bool(planner_success and pos_ok and orient_ok)
        if success:
            message = "gripper moved to requested target pose" if mode == "target_pose" else "gripper moved along requested direction"
        elif not planner_success:
            message = planner_message
        elif not pos_ok:
            message = "gripper did not reach target position"
        else:
            message = "gripper did not satisfy orientation tolerance"
        self._maybe_save_move_gripper_debug_plot(
            arm=arm,
            direction=direction,
            distance=float(distance),
            orient_tol=orient_tol,
            start_pos=current_pos,
            target_pos=target_pos,
            final_pos=final_pos,
            actual_positions=actual_positions,
            world_dir=world_dir,
            pos_err=pos_err,
            orient_err=orient_err,
            orient_ok=orient_ok,
            success=success,
        )
        done_msg = (
            f"[low-gripper-debug] move_gripper done arm={arm} success={success} "
            f"planner_success={planner_success} pos_err={pos_err} pos_tol={pos_tol} orient_err={orient_err} "
            f"orient_ok={orient_ok} final_pos={final_pos.tolist()} final_quat={final_quat.tolist()}"
        )
        logger.info(done_msg)
        print(done_msg, flush=True)

        data = {}
        data.update(
            {
                "steps": steps,
                "mode": mode,
                "direction": direction,
                "distance": float(distance),
                "orient_tol": orient_tol,
                "start_pos": current_pos.tolist(),
                "start_quat": current_quat.tolist(),
                "target_pos": target_pos.tolist(),
                "target_quat": target_quat.tolist(),
                "world_dir": world_dir.tolist(),
                "final_pos": final_pos.tolist(),
                "final_quat": final_quat.tolist(),
                "pos_err": pos_err,
                "pos_tol": pos_tol,
                "pos_ok": pos_ok,
                "orient_err": orient_err,
                "orient_ok": orient_ok,
                "planner_success": planner_success,
            }
        )
        return PrimitiveResult(
            success=success,
            primitive="move_gripper",
            message=message,
            arm=arm,
            data=data,
        )

    def move_grasped_object_to_pose(
        self,
        object_name: str,
        *,
        target_pose: PoseLike,
        arm: ArmName = "auto",
        orient_tol: Optional[float] = 0.10,
        timeout_steps: int = 300,
        avoid_collision: bool = False,
        require_holding: bool = True,
    ) -> PrimitiveResult:
        if arm == "auto":
            candidate_arms = ("left", "right")
            holding_arms = [candidate for candidate in candidate_arms if self.is_holding(object_name, arm=candidate)]
            if not holding_arms:
                return PrimitiveResult(
                    success=False,
                    primitive="move_grasped_object_to_pose",
                    object_name=object_name,
                    message=f"object is not held by either arm: {object_name}",
                )
            arm = holding_arms[0]
        else:
            arm = self._validate_arm(arm)
            if require_holding and not self.is_holding(object_name, arm=arm):
                return PrimitiveResult(
                    success=False,
                    primitive="move_grasped_object_to_pose",
                    object_name=object_name,
                    arm=arm,
                    message=f"object is not held by {arm} arm: {object_name}",
                )

        obj = self._resolve_object(object_name)
        current_obj_pos, current_obj_quat = obj.get_position_orientation()
        current_obj_pos = self._to_numpy(current_obj_pos).astype(np.float64)
        current_obj_quat = self._quat_normalize(self._to_numpy(current_obj_quat).astype(np.float64))
        current_eef_pos = self._to_numpy(self.robot.get_eef_position(arm)).astype(np.float64)
        current_eef_quat = self._quat_normalize(self._to_numpy(self.robot.get_eef_orientation(arm)).astype(np.float64))
        target_obj_pos, target_obj_quat = self._pose_to_arrays(target_pose)
        target_obj_quat = self._quat_normalize(target_obj_quat)

        obj_inv_pos, obj_inv_quat = self._pose_inverse(current_obj_pos, current_obj_quat)
        obj_to_eef_pos, obj_to_eef_quat = self._pose_multiply(
            obj_inv_pos,
            obj_inv_quat,
            current_eef_pos,
            current_eef_quat,
        )
        target_eef_pos, target_eef_quat = self._pose_multiply(
            target_obj_pos,
            target_obj_quat,
            obj_to_eef_pos,
            obj_to_eef_quat,
        )

        debug_msg = (
            f"[low-object-debug] move_grasped_object_to_pose start object={object_name} arm={arm} "
            f"current_obj_pos={current_obj_pos.tolist()} current_obj_quat={current_obj_quat.tolist()} "
            f"target_obj_pos={target_obj_pos.tolist()} target_obj_quat={target_obj_quat.tolist()} "
            f"current_eef_pos={current_eef_pos.tolist()} current_eef_quat={current_eef_quat.tolist()} "
            f"obj_to_eef_pos={obj_to_eef_pos.tolist()} obj_to_eef_quat={obj_to_eef_quat.tolist()} "
            f"target_eef_pos={target_eef_pos.tolist()} target_eef_quat={target_eef_quat.tolist()}"
        )
        logger.info(debug_msg)
        print(debug_msg, flush=True)

        gripper_result = self.move_gripper(
            arm,
            target_pose={"pos": target_eef_pos, "quat": target_eef_quat},
            orient_tol=orient_tol,
            timeout_steps=timeout_steps,
            avoid_collision=avoid_collision,
        )

        final_obj_pos, final_obj_quat = obj.get_position_orientation()
        final_obj_pos = self._to_numpy(final_obj_pos).astype(np.float64)
        final_obj_quat = self._quat_normalize(self._to_numpy(final_obj_quat).astype(np.float64))
        obj_pos_err = float(np.linalg.norm(final_obj_pos - target_obj_pos))
        obj_pos_tol = float(self.config.get("object_pos_tol", self.config.get("eef_pos_tol", 0.03)))
        obj_orient_err = self._quat_angle_diff(final_obj_quat, target_obj_quat)
        obj_orient_ok = True if orient_tol is None else obj_orient_err <= float(orient_tol)
        obj_pos_ok = obj_pos_err <= obj_pos_tol
        success = bool(gripper_result.success and obj_pos_ok and obj_orient_ok)
        if success:
            message = "grasped object reached target pose"
        elif not gripper_result.success:
            message = gripper_result.message or "gripper failed to move object"
        elif not obj_pos_ok:
            message = "grasped object did not reach target position"
        else:
            message = "grasped object did not satisfy orientation tolerance"

        done_msg = (
            f"[low-object-debug] move_grasped_object_to_pose done object={object_name} arm={arm} "
            f"success={success} gripper_success={gripper_result.success} "
            f"obj_pos_err={obj_pos_err} obj_pos_tol={obj_pos_tol} "
            f"obj_orient_err={obj_orient_err} obj_orient_ok={obj_orient_ok} "
            f"final_obj_pos={final_obj_pos.tolist()} final_obj_quat={final_obj_quat.tolist()}"
        )
        logger.info(done_msg)
        print(done_msg, flush=True)

        data = dict(gripper_result.data)
        data.update(
            {
                "object_name": object_name,
                "target_object_pos": target_obj_pos.tolist(),
                "target_object_quat": target_obj_quat.tolist(),
                "current_object_pos": current_obj_pos.tolist(),
                "current_object_quat": current_obj_quat.tolist(),
                "final_object_pos": final_obj_pos.tolist(),
                "final_object_quat": final_obj_quat.tolist(),
                "object_pos_err": obj_pos_err,
                "object_pos_tol": obj_pos_tol,
                "object_pos_ok": obj_pos_ok,
                "object_orient_err": obj_orient_err,
                "object_orient_ok": obj_orient_ok,
                "obj_to_eef_pos": obj_to_eef_pos.tolist(),
                "obj_to_eef_quat": obj_to_eef_quat.tolist(),
                "target_eef_pos": target_eef_pos.tolist(),
                "target_eef_quat": target_eef_quat.tolist(),
            }
        )
        return PrimitiveResult(
            success=success,
            primitive="move_grasped_object_to_pose",
            object_name=object_name,
            arm=arm,
            message=message,
            data=data,
        )

    @staticmethod
    def _to_numpy(value: Any) -> np.ndarray:
        if hasattr(value, "detach"):
            return value.detach().cpu().numpy()
        return np.asarray(value)

    @staticmethod
    def _to_torch(value: Any) -> Any:
        import torch as th

        if hasattr(value, "detach"):
            return value.detach().clone().float()
        return th.tensor(value, dtype=th.float32)

    @staticmethod
    def _validate_arm(arm: ArmName) -> Literal["left", "right"]:
        if arm not in ("left", "right"):
            raise ValueError(f"Expected arm to be 'left' or 'right', got {arm!r}")
        return arm

    def _choose_arm_near_object(self, object_name: str, arm: ArmName = "auto") -> Literal["left", "right"]:
        if arm != "auto":
            return self._validate_arm(arm)

        obj = self._resolve_object(object_name)
        obj_pos, _ = obj.get_position_orientation()
        obj_pos_np = self._to_numpy(obj_pos).astype(np.float64)

        best_arm = None
        best_dist = float("inf")
        for candidate in ("left", "right"):
            if candidate not in getattr(self.robot, "arm_names", ()):
                continue
            eef_pos = self.robot.get_eef_position(candidate)
            dist = float(np.linalg.norm(self._to_numpy(eef_pos).astype(np.float64) - obj_pos_np))
            if dist < best_dist:
                best_arm = candidate
                best_dist = dist

        if best_arm is None:
            raise ValueError("Could not choose an arm; robot has no left/right arm names")
        return best_arm

    def _support_bbox_pose_and_extent(self, support: Any) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        try:
            bbox_center, bbox_quat, bbox_extent, _ = support.get_base_aligned_bbox()
            return (
                self._to_numpy(bbox_center).astype(np.float64),
                self._to_numpy(bbox_quat).astype(np.float64),
                self._to_numpy(bbox_extent).astype(np.float64),
            )
        except Exception:
            support_pos, support_quat = support.get_position_orientation()
            return (
                self._to_numpy(support_pos).astype(np.float64),
                self._to_numpy(support_quat).astype(np.float64),
                self._to_numpy(getattr(support, "aabb_extent", np.asarray([0.5, 0.5, 0.05]))).astype(np.float64),
            )

    def _distance_to_support_edge_along_push(
        self,
        *,
        obj_pos: np.ndarray,
        push_dir3: np.ndarray,
        support: Any,
    ) -> float:
        support_center, support_quat, support_extent = self._support_bbox_pose_and_extent(support)
        support_quat = support_quat / np.linalg.norm(support_quat)
        inv_support_quat = self._quat_conjugate(support_quat)
        obj_local = self._rotate_vector(inv_support_quat, obj_pos - support_center)
        dir_local = self._rotate_vector(inv_support_quat, push_dir3)

        half_x = max(float(support_extent[0]) * 0.5, 1e-6)
        half_y = max(float(support_extent[1]) * 0.5, 1e-6)
        x, y = float(obj_local[0]), float(obj_local[1])
        dx, dy = float(dir_local[0]), float(dir_local[1])

        candidates: List[float] = []
        eps = 1e-8
        if abs(dx) > eps:
            for edge_x in (half_x, -half_x):
                t = (edge_x - x) / dx
                edge_y = y + t * dy
                if t >= 0.0 and -half_y - eps <= edge_y <= half_y + eps:
                    candidates.append(float(t))
        if abs(dy) > eps:
            for edge_y in (half_y, -half_y):
                t = (edge_y - y) / dy
                edge_x = x + t * dx
                if t >= 0.0 and -half_x - eps <= edge_x <= half_x + eps:
                    candidates.append(float(t))

        if candidates:
            return min(candidates)

        # If the object is already outside the support rectangle or numerical issues occur,
        # do not push farther by geometry-derived distance.
        return 0.0

    def _object_extent_along_direction(self, obj: Any, direction_xy: np.ndarray) -> float:
        obj_extent = self._to_numpy(getattr(obj, "aabb_extent", np.asarray([0.08, 0.08, 0.08]))).astype(np.float64)
        direction_xy = np.asarray(direction_xy, dtype=np.float64)
        direction_xy = direction_xy / max(np.linalg.norm(direction_xy), 1e-8)
        return float(abs(direction_xy[0]) * obj_extent[0] + abs(direction_xy[1]) * obj_extent[1])

    def _plan_eef_joint_trajectory(
        self,
        arm: Literal["left", "right"],
        pose: PoseLike,
        *,
        ignore_objects: Optional[Sequence[str]] = None,
        motion_constraint: Optional[Sequence[float]] = None,
    ) -> Any:
        from omnigibson.action_primitives.curobo import CuRoboEmbodimentSelection

        pos, quat = self._pose_to_arrays(pose)
        motion_generator = self._get_motion_generator()
        batch_size = int(getattr(motion_generator, "batch_size", 1))
        link_name = self.robot.eef_link_names[arm]
        target_pos = {link_name: self._to_torch(pos).repeat(batch_size, 1)}
        target_quat = {link_name: self._to_torch(quat).repeat(batch_size, 1)}
        debug = os.environ.get("MOP_DEBUG", "0") in ("1", "true", "True")
        if debug:
            debug_msg = (
                f"[low-arm-debug] planning start arm={arm} link={link_name} batch_size={batch_size} "
                f"target_pos={pos.tolist()} target_quat={quat.tolist()} "
                f"ignore_objects={list(ignore_objects or [])} motion_constraint={motion_constraint}"
            )
            logger.info(debug_msg)
            print(debug_msg, flush=True)

        motion_generator.update_obstacles(ignore_objects=list(ignore_objects or []))
        successes, traj_paths = motion_generator.compute_trajectories(
            target_pos=target_pos,
            target_quat=target_quat,
            initial_joint_pos=None,
            is_local=False,
            max_attempts=int(self.config.get("arm_planning_max_attempts", 10)),
            timeout=float(self.config.get("arm_planning_timeout", 60.0)),
            ik_fail_return=int(self.config.get("arm_ik_fail_return", 10)),
            enable_finetune_trajopt=True,
            finetune_attempts=int(self.config.get("arm_finetune_attempts", 1)),
            return_full_result=False,
            success_ratio=1.0 / batch_size,
            attached_obj=None,
            attached_obj_scale=None,
            motion_constraint=motion_constraint,
            skip_obstacle_update=True,
            ik_only=False,
            ik_world_collision_check=True,
            emb_sel=CuRoboEmbodimentSelection.ARM,
        )

        import torch as th

        success_idx = th.where(successes)[0].cpu()
        if len(success_idx) == 0:
            raise RuntimeError(f"No arm trajectory found for {arm} eef target pose")
        if debug:
            debug_msg = (
                f"[low-arm-debug] planning result arm={arm} successes={successes.detach().cpu().float().tolist()} "
                f"num_paths={len(traj_paths)} selected_idx={int(success_idx[0])}"
            )
            logger.info(debug_msg)
            print(debug_msg, flush=True)

        q_traj = (
            motion_generator.path_to_joint_trajectory(
                traj_paths[success_idx[0]],
                get_full_js=True,
                emb_sel=CuRoboEmbodimentSelection.ARM,
            )
            .cpu()
            .float()
        )
        q_traj = motion_generator.add_linearly_interpolated_waypoints(
            traj=q_traj,
            max_inter_dist=float(self.config.get("arm_max_inter_waypoint_dist", 0.01)),
        )
        if debug:
            active_idx = th.cat([self.robot.trunk_control_idx, self.robot.arm_control_idx[arm]]).cpu()
            current_q = self.robot.get_joint_positions().detach().cpu().float()
            first_active = q_traj[0][active_idx].detach().cpu().numpy().round(5).tolist()
            last_active = q_traj[-1][active_idx].detach().cpu().numpy().round(5).tolist()
            current_active = current_q[active_idx].detach().cpu().numpy().round(5).tolist()
            debug_msg = (
                f"[low-arm-debug] planned q_traj arm={arm} shape={tuple(q_traj.shape)} "
                f"active_count={len(active_idx)} active_idx={active_idx.tolist()} "
                f"current_active={current_active} first_active={first_active} last_active={last_active}"
            )
            logger.info(debug_msg)
            print(debug_msg, flush=True)
        return q_traj

    def _solve_eef_ik_target(
        self,
        arm: Literal["left", "right"],
        pose: PoseLike,
        *,
        ignore_objects: Optional[Sequence[str]] = None,
    ) -> Any:
        from omnigibson.action_primitives.curobo import CuRoboEmbodimentSelection

        pos, quat = self._pose_to_arrays(pose)
        motion_generator = self._get_motion_generator()
        batch_size = int(getattr(motion_generator, "batch_size", 1))
        link_name = self.robot.eef_link_names[arm]
        target_pos = {link_name: self._to_torch(pos).repeat(batch_size, 1)}
        target_quat = {link_name: self._to_torch(quat).repeat(batch_size, 1)}

        motion_generator.update_obstacles(ignore_objects=list(ignore_objects or []))
        successes, joint_states = motion_generator.compute_trajectories(
            target_pos=target_pos,
            target_quat=target_quat,
            initial_joint_pos=self.robot.get_joint_positions().detach().cpu().float(),
            is_local=False,
            max_attempts=int(self.config.get("arm_ik_max_attempts", os.environ.get("MOP_ARM_IK_MAX_ATTEMPTS", 3))),
            timeout=float(self.config.get("arm_ik_timeout", os.environ.get("MOP_ARM_IK_TIMEOUT_S", 5.0))),
            ik_fail_return=batch_size,
            enable_finetune_trajopt=False,
            finetune_attempts=0,
            return_full_result=False,
            success_ratio=1.0 / batch_size,
            attached_obj=None,
            attached_obj_scale=None,
            motion_constraint=None,
            skip_obstacle_update=True,
            ik_only=True,
            ik_world_collision_check=False,
            emb_sel=CuRoboEmbodimentSelection.ARM,
        )

        import torch as th

        success_idx = th.where(successes)[0].cpu()
        if len(success_idx) == 0:
            raise RuntimeError(f"No IK solution found for {arm} eef target pose")

        candidates = []
        current_q = self.robot.get_joint_positions().detach().cpu().float()
        active_idx = th.cat([self.robot.trunk_control_idx, self.robot.arm_control_idx[arm]]).cpu()
        for idx in success_idx:
            q_traj = motion_generator.path_to_joint_trajectory(
                joint_states[idx],
                get_full_js=True,
                emb_sel=CuRoboEmbodimentSelection.ARM,
            ).cpu().float()
            q_target = q_traj[-1] if q_traj.ndim == 2 else q_traj
            delta = q_target[active_idx] - current_q[active_idx]
            score = float(th.norm(delta).item())
            candidates.append((score, q_target))
        candidates.sort(key=lambda item: item[0])
        return candidates[0][1]

    def _q_to_arm_action(self, joint_pos: Any, arm: Literal["left", "right"]) -> Any:
        import torch as th

        action = th.zeros(self.robot.action_dim, dtype=th.float32)
        control_dict = self.robot.get_control_dict()
        controllers = getattr(self.robot, "controllers", getattr(self.robot, "_controllers", {}))
        debug = os.environ.get("MOP_DEBUG", "0") in ("1", "true", "True")
        if debug and not getattr(self, "_logged_arm_action_controllers", False):
            commanded = [name for name in controllers if name == "trunk" or name == f"arm_{arm}"]
            no_op = [name for name in controllers if name not in commanded]
            debug_msg = (
                f"[low-arm-debug] arm-only action builder arm={arm} action_dim={self.robot.action_dim} "
                f"commanded_controllers={commanded} noop_controllers={no_op}"
            )
            logger.info(debug_msg)
            print(debug_msg, flush=True)
            self._logged_arm_action_controllers = True

        for name, controller in controllers.items():
            if name == "trunk" or name == f"arm_{arm}":
                partial_action = controller._reverse_preprocess_command(joint_pos[controller.dof_idx])
            else:
                partial_action = controller.compute_no_op_action(control_dict)
            if not isinstance(partial_action, th.Tensor):
                partial_action = th.tensor(partial_action, dtype=th.float32)
            action_idx = self.robot.controller_action_idx[name]
            action[action_idx] = partial_action.float().cpu()

        return action

    def _iter_joint_trajectory(
        self,
        q_traj: Any,
        *,
        arm: Literal["left", "right"],
        max_steps_per_waypoint: Optional[int] = None,
        joint_tol: Optional[float] = None,
    ) -> Iterator[Any]:
        import torch as th

        active_idx = th.cat([self.robot.trunk_control_idx, self.robot.arm_control_idx[arm]])
        max_steps = int(max_steps_per_waypoint or self.config.get("arm_max_steps_per_waypoint", 30))
        tol = float(joint_tol or self.config.get("arm_joint_tol", 0.02))
        debug = os.environ.get("MOP_DEBUG", "0") in ("1", "true", "True")
        if debug:
            debug_msg = (
                f"[low-arm-debug] executing q_traj arm={arm} shape={tuple(q_traj.shape)} "
                f"active_count={len(active_idx)} active_idx={active_idx.detach().cpu().tolist()} "
                f"max_steps_per_waypoint={max_steps} joint_tol={tol}"
            )
            logger.info(debug_msg)
            print(debug_msg, flush=True)

        for waypoint_idx, joint_pos in enumerate(q_traj):
            last_err = None
            reached = False
            steps_used = 0
            for step_idx in range(max_steps):
                yield self._q_to_arm_action(joint_pos, arm)
                steps_used = step_idx + 1
                current_q = self.robot.get_joint_positions()
                max_err = th.max(th.abs(joint_pos[active_idx] - current_q[active_idx])).item()
                last_err = max_err
                if max_err <= tol:
                    reached = True
                    break
            if debug and (waypoint_idx < 5 or waypoint_idx == len(q_traj) - 1 or waypoint_idx % 50 == 0 or not reached):
                current_q = self.robot.get_joint_positions()
                target_active = joint_pos[active_idx].detach().cpu().numpy().round(5).tolist()
                current_active = current_q[active_idx].detach().cpu().numpy().round(5).tolist()
                debug_msg = (
                    f"[low-arm-debug] waypoint arm={arm} idx={waypoint_idx + 1}/{len(q_traj)} "
                    f"steps_used={steps_used} reached={reached} max_active_err={last_err} "
                    f"target_active={target_active} current_active={current_active}"
                )
                logger.info(debug_msg)
                print(debug_msg, flush=True)

    def iter_move_eef_to_pose(
        self,
        arm: ArmName,
        pose: PoseLike,
        *,
        ignore_objects: Optional[Sequence[str]] = None,
        motion_constraint: Optional[Sequence[float]] = None,
    ) -> Iterator[Any]:
        arm = self._validate_arm(arm)
        q_traj = self._plan_eef_joint_trajectory(
            arm,
            pose,
            ignore_objects=ignore_objects,
            motion_constraint=motion_constraint,
        )
        yield from self._iter_joint_trajectory(q_traj, arm=arm)

    def _gripper_target_joint_positions(self, arm: Literal["left", "right"], limit_type: str) -> Any:
        target_joint_positions = self.robot.get_joint_positions()
        gripper_ctrl_idx = self.robot.gripper_control_idx[arm]
        if limit_type == getattr(self.robot, "_grasping_direction", "lower"):
            finger_joint_limits = self.robot.joint_lower_limits[gripper_ctrl_idx]
        else:
            finger_joint_limits = self.robot.joint_upper_limits[gripper_ctrl_idx]
        target_joint_positions[gripper_ctrl_idx] = finger_joint_limits
        return target_joint_positions

    def _move_gripper_to_limit(
        self,
        arm: ArmName,
        *,
        primitive: str,
        limit_type: Literal["lower", "upper"],
        steps: int,
        object_name: Optional[str] = None,
    ) -> PrimitiveResult:
        arm = self._validate_arm(arm)
        if self.env is None:
            return PrimitiveResult(
                success=False,
                primitive=primitive,
                message="env is required to execute gripper motion",
                arm=arm,
            )

        target_joint_positions = self._gripper_target_joint_positions(arm, limit_type)
        gripper_ctrl_idx = self.robot.gripper_control_idx[arm]
        target_gripper_q = self._to_numpy(target_joint_positions[gripper_ctrl_idx]).astype(np.float64)
        action = self.robot.q_to_action(target_joint_positions)
        joint_tol = float(self.config.get("gripper_joint_tol", 1e-3))
        start_gripper_q = self._to_numpy(self.robot.get_joint_positions()[gripper_ctrl_idx]).astype(np.float64)

        debug_msg = (
            f"[low-gripper-debug] start primitive={primitive} arm={arm} "
            f"limit_type={limit_type} steps={int(steps)} "
            f"object_name={object_name} "
            f"start_q={start_gripper_q.tolist()} target_q={target_gripper_q.tolist()} "
            f"joint_tol={joint_tol}"
        )
        logger.info(debug_msg)
        print(debug_msg, flush=True)

        executed_steps = 0
        for _ in range(int(steps)):
            self.env.step(action, n_render_iterations=1)
            executed_steps += 1
            current_joint_positions = self.robot.get_joint_positions()
            current_gripper_q = self._to_numpy(current_joint_positions[gripper_ctrl_idx]).astype(np.float64)
            if np.allclose(current_gripper_q, target_gripper_q, atol=joint_tol):
                break

        current_joint_positions = self.robot.get_joint_positions()
        current_gripper_q = self._to_numpy(current_joint_positions[gripper_ctrl_idx]).astype(np.float64)
        reached_limit = bool(np.allclose(current_gripper_q, target_gripper_q, atol=joint_tol))
        holding_object = False
        if primitive == "close_gripper" and object_name:
            holding_object = self.is_holding(object_name, arm=arm)
        reached = bool(holding_object or reached_limit)
        success_reason = "holding object" if holding_object else "reached gripper limit"
        done_msg = (
            f"[low-gripper-debug] done primitive={primitive} arm={arm} "
            f"success={reached} executed_steps={executed_steps} "
            f"holding_object={holding_object} reached_limit={reached_limit} "
            f"current_q={current_gripper_q.tolist()} target_q={target_gripper_q.tolist()}"
        )
        logger.info(done_msg)
        print(done_msg, flush=True)
        return PrimitiveResult(
            success=reached,
            primitive=primitive,
            message=success_reason if reached else "gripper did not reach target within step budget",
            arm=arm,
            data={
                "steps": executed_steps,
                "target_q": target_gripper_q.tolist(),
                "current_q": current_gripper_q.tolist(),
                "joint_tol": joint_tol,
                "object_name": object_name,
                "holding_object": holding_object,
                "reached_limit": reached_limit,
            },
        )

    def open_gripper(self, arm: ArmName, *, steps: int = 20) -> PrimitiveResult:
        return self._move_gripper_to_limit(arm, primitive="open_gripper", limit_type="upper", steps=steps)

    def close_gripper(
        self,
        arm: ArmName,
        *,
        steps: int = 40,
        object_name: Optional[str] = None,
    ) -> PrimitiveResult:
        return self._move_gripper_to_limit(
            arm,
            primitive="close_gripper",
            limit_type="lower",
            steps=steps,
            object_name=object_name,
        )

    def lift_eef(self, arm: ArmName, *, dz: float = 0.10, timeout_steps: int = 300) -> PrimitiveResult:
        raise NotImplementedError

    # ---------------------------------------------------------------------
    # Object manipulation
    # ---------------------------------------------------------------------
    def execute_grasp_pose(
        self,
        object_name: str,
        *,
        grasp_pose: PoseLike,
        arm: ArmName = "auto",
        pregrasp_distance: float = 0.08,
        lift_distance: float = 0.10,
    ) -> PrimitiveResult:
        raise NotImplementedError

    def pick_object(
        self,
        object_name: str,
        *,
        from_object: Optional[str] = None,
        arm: ArmName = "auto",
        grasp_candidate_idx: int = 0,
    ) -> PrimitiveResult:
        raise NotImplementedError

    def place_on(
        self,
        object_name: str,
        *,
        target_object: str,
        arm: ArmName = "auto",
        release_height: float = 0.03,
    ) -> PrimitiveResult:
        raise NotImplementedError

    def place_next_to(
        self,
        object_name: str,
        *,
        reference_object: str,
        side: Literal["left", "right", "front", "back", "in_front_of"] = "left",
        arm: ArmName = "auto",
        clearance: float = 0.05,
    ) -> PrimitiveResult:
        raise NotImplementedError

    def push_to_edge(
        self,
        object_name: str,
        *,
        support_object: str,
        arm: ArmName = "auto",
        push_distance: Optional[float] = None,
    ) -> PrimitiveResult:
        if self.env is None:
            return PrimitiveResult(
                success=False,
                primitive="push_to_edge",
                object_name=object_name,
                message="env is required to execute push",
            )

        resolved_arm = self._choose_arm_near_object(object_name, arm)
        steps = 0
        try:
            for action in self.iter_push_to_edge(
                object_name,
                support_object=support_object,
                arm=resolved_arm,
                push_distance=push_distance,
            ):
                self.env.step(action, n_render_iterations=1)
                steps += 1
        except Exception as e:
            return PrimitiveResult(
                success=False,
                primitive="push_to_edge",
                object_name=object_name,
                arm=resolved_arm,
                message=f"push failed: {e}",
                data={"steps": steps, "support_object": support_object},
            )

        obj = self._resolve_object(object_name)
        support = self._resolve_object(support_object)
        obj_pos = self._to_numpy(obj.get_position_orientation()[0]).astype(np.float64)
        support_pos = self._to_numpy(support.get_position_orientation()[0]).astype(np.float64)
        moved_outward = float(np.linalg.norm((obj_pos - support_pos)[:2]))
        return PrimitiveResult(
            success=True,
            primitive="push_to_edge",
            object_name=object_name,
            arm=resolved_arm,
            message="push trajectory executed",
            data={
                "steps": steps,
                "support_object": support_object,
                "requested_push_distance": None if push_distance is None else float(push_distance),
                "object_support_xy_distance": moved_outward,
            },
        )

    def iter_push_to_edge(
        self,
        object_name: str,
        *,
        support_object: str,
        arm: ArmName = "auto",
        push_distance: Optional[float] = None,
    ) -> Iterator[Any]:
        arm = self._choose_arm_near_object(object_name, arm)
        obj = self._resolve_object(object_name)
        support = self._resolve_object(support_object)

        obj_pos = self._to_numpy(obj.get_position_orientation()[0]).astype(np.float64)
        robot_pos = self._to_numpy(self.robot.get_position_orientation()[0]).astype(np.float64)
        push_dir = obj_pos[:2] - robot_pos[:2]
        if np.linalg.norm(push_dir) < 1e-6:
            push_dir = np.asarray([1.0, 0.0], dtype=np.float64)
        push_dir = push_dir / np.linalg.norm(push_dir)
        push_dir3 = np.asarray([push_dir[0], push_dir[1], 0.0], dtype=np.float64)

        obj_extent_along_push = self._object_extent_along_direction(obj, push_dir)
        obj_radius_along_push = 0.5 * obj_extent_along_push
        if push_distance is None:
            edge_distance = self._distance_to_support_edge_along_push(
                obj_pos=obj_pos,
                push_dir3=push_dir3,
                support=support,
            )
            overhang_fraction = float(self.config.get("push_target_overhang_fraction", 0.25))
            margin = float(self.config.get("push_distance_margin", 0.0))
            push_distance = max(0.0, edge_distance - overhang_fraction * obj_extent_along_push + margin)

        approach_margin = float(self.config.get("push_approach_margin", 0.12))
        contact_margin = float(self.config.get("push_contact_margin", 0.02))
        z_offset = float(self.config.get("push_eef_z_offset", 0.02))

        current_quat = self._to_numpy(self.robot.get_eef_orientation(arm)).astype(np.float64)
        push_z = float(obj_pos[2] + z_offset)
        contact_pos = obj_pos - push_dir3 * (obj_radius_along_push + contact_margin)
        contact_pos[2] = push_z
        approach_pos = obj_pos - push_dir3 * (obj_radius_along_push + approach_margin)
        approach_pos[2] = push_z
        final_pos = contact_pos + push_dir3 * float(push_distance)

        ignore_objects = [getattr(obj, "name", object_name), getattr(support, "name", support_object)]

        yield from self.iter_move_eef_to_pose(
            arm,
            {"pos": approach_pos, "quat": current_quat},
            ignore_objects=ignore_objects,
        )
        yield from self.iter_move_eef_to_pose(
            arm,
            {"pos": contact_pos, "quat": current_quat},
            ignore_objects=ignore_objects,
        )
        yield from self.iter_move_eef_to_pose(
            arm,
            {"pos": final_pos, "quat": current_quat},
            ignore_objects=ignore_objects,
        )

    # ---------------------------------------------------------------------
    # Verification / recovery
    # ---------------------------------------------------------------------
    def is_holding(self, object_name: str, *, arm: ArmName = "auto") -> bool:
        arms = ("left", "right") if arm == "auto" else (self._validate_arm(arm),)
        try:
            obj = self._resolve_object(object_name)
            candidate_obj = getattr(obj, "wrapped_obj", obj)
        except Exception as e:
            logger.warning("is_holding object resolve failed for object=%s: %s", object_name, e)
            return False

        for check_arm in arms:
            try:
                state = self.robot.is_grasping(arm=check_arm, candidate_obj=candidate_obj)
            except Exception as e:
                logger.warning(
                    "is_holding object-specific check failed for object=%s arm=%s: %s",
                    object_name,
                    check_arm,
                    e,
                )
                continue

            state_name = getattr(state, "name", None)
            holding = bool(state is True or state_name == "TRUE" or str(state).endswith("TRUE"))
            logger.info(
                "[low-gripper-debug] is_holding object=%s arm=%s state=%s holding=%s",
                object_name,
                check_arm,
                state,
                holding,
            )
            print(
                f"[low-gripper-debug] is_holding object={object_name} arm={check_arm} state={state} holding={holding}",
                flush=True,
            )
            if holding:
                return True
        return False

    def check_on(self, object_name: str, target_object: str) -> bool:
        raise NotImplementedError

    def check_next_to(
        self,
        object_name: str,
        reference_object: str,
        *,
        side: Optional[str] = None,
    ) -> bool:
        raise NotImplementedError

    def check_task_success(self) -> bool:
        raise NotImplementedError

    def wait(self, *, steps: int = 20) -> PrimitiveResult:
        raise NotImplementedError
