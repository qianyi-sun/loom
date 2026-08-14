"""
eval_data_gen_par.py - Parallel/Direct Data Generation

This script combines eval_data_gen.py and replay_obs.py to directly output raw data
(parquet files and videos) during policy evaluation, bypassing the intermediate HDF5 step.

This is much faster than the two-step process of:
1. eval_data_gen.py -> HDF5
2. replay_obs.py -> parquet + videos
"""

import csv
import cv2
import hydra
import importlib.util
import json
import logging
import math
import numpy as np
import omnigibson as og
import omnigibson.utils.transform_utils as T
import os
import pandas as pd
import shutil
import sys
import time
import torch as th
import traceback
from av.container import Container
from av.stream import Stream
from loom.integrations.behavior.stages.stage1_sim_config import (
    augment_rooms,
    generate_robot_config,
    get_task_relevant_room_types,
)
from loom.integrations.behavior.stages.stage1_sim_config import DISABLED_TRANSITION_RULES
from hydra.utils import instantiate
from inspect import getsourcefile
from omegaconf import DictConfig, OmegaConf
import h5py
from omnigibson.envs.env_wrapper import EnvironmentWrapper
try:
    from omnigibson.envs.data_wrapper import HDF5CollectionWrapper
except ImportError:
    from omnigibson.envs.data_wrapper import DataCollectionWrapper as HDF5CollectionWrapper
from omnigibson.learning.utils.config_utils import register_omegaconf_resolvers
from omnigibson.learning.utils.dataset_utils import makedirs_with_mode
from omnigibson.learning.utils.eval_utils import (
    ROBOT_CAMERA_NAMES,
    PROPRIOCEPTION_INDICES,
    generate_basic_environment_config,
    flatten_obs_dict,
    TASK_NAMES_TO_INDICES,
    HEAD_RESOLUTION,
    WRIST_RESOLUTION,
)
from omnigibson.learning.utils.obs_utils import (
    create_video_writer,
    write_video,
)
from omnigibson.macros import gm, create_module_macros, macros
from omnigibson.metrics import MetricBase, AgentMetric, TaskMetric
from omnigibson.sensors.vision_sensor import VisionSensor
from omnigibson.robots import BaseRobot
from omnigibson.object_states import Inside, OnTop, NextTo, Touching, Under, Open, ToggledOn, Cooked, Frozen
from omnigibson.utils.asset_utils import get_task_instance_path
from omnigibson.utils.python_utils import recursively_convert_to_torch
from pathlib import Path
from signal import signal, SIGINT
from typing import Any, Tuple, List, Dict, Optional


def _load_at_edge_class():
    """Load the worktree state even when OG is editable-installed from the 4.5 checkout."""
    try:
        from omnigibson.object_states import AtEdge

        return AtEdge
    except ImportError:
        module_path = Path(__file__).parents[1] / "object_states" / "at_edge.py"
        spec = importlib.util.spec_from_file_location("omnigibson.object_states.at_edge_diagnostic", module_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module.AtEdge


AtEdge = _load_at_edge_class()
try:
    from omnigibson.object_states.at_edge import MIN_MISS_HIT_RATIO
except ImportError:
    MIN_MISS_HIT_RATIO = 0.1

class PolicyEvalDataCollectionWrapper(HDF5CollectionWrapper):
    """
    A modified HDF5CollectionWrapper that does NOT disable camera render products.
    This allows the policy to receive correct observations during evaluation
    while still recording state dumps for replay compatibility.
    """

    def _optimize_sim_for_data_collection(self, viewport_camera_path):
        """
        Override to NOT disable camera render products.
        We skip the sensor disabling so observations remain valid for policy evaluation.
        """
        og.sim.viewer_camera.active_camera_path = viewport_camera_path
        if self._enable_dump_filters:
            self.enable_dump_filters()


try:
    m = create_module_macros(module_path=__file__)
except ValueError:
    # OmniGibson 3.7 requires macro paths to live under the editable package
    # root, even when the executable diagnostic script lives in a worktree.
    m = create_module_macros(module_path=Path(og.__file__).parent / "learning" / Path(__file__).name)
m.NUM_EVAL_EPISODES = 1
m.NUM_TRAIN_INSTANCES = 200
m.NUM_EVAL_INSTANCES = 1000

# set global variables to boost performance
gm.ENABLE_FLATCACHE = True
gm.USE_GPU_DYNAMICS = False
gm.ENABLE_TRANSITION_RULES = True

# Set grasp window to larger value to account for hard grasps
with macros.unlocked():
    macros.robots.manipulation_robot.GRASP_WINDOW = 0.75

# create module logger
logger = logging.getLogger("evaluator_par")
logger.setLevel(20)  # info


class BDDLStateTracker:
    """
    Tracks BDDL predicate state transitions during an episode.

    Records the exact timestep of every state change for goal conditions,
    grasping, pairwise spatial predicates, and unary object states.

    Two cross-cutting behaviors applied to every emitted transition:

    1. **50-step persistence debounce.** A flip is committed only when the
       new value has held for ``_STABLE_TRANSITION_FRAMES`` consecutive
       frames; jitter that reverts faster is dropped. Matches the gate
       used by ``embodiedClaw/sim/bddl_state_tracker.py``.

    2. **Grasp schema.** Grasp transitions are emitted in the same
       ``args=[{scope_name, scene_name}, ...]`` shape as binary spatial
       predicates, with boolean ``old_value``/``new_value`` and the
       grasped object's BDDL scope_name. The ``grasp_history`` list keeps
       a separate raw-scene record for consumers that need the per-arm
       per-instance trail.
    """

    BINARY_STATE_CLASSES = {
        "Inside": Inside,
        "OnTop": OnTop,
        "NextTo": NextTo,
        "Touching": Touching,
        "Under": Under,
    }

    UNARY_STATE_CLASSES = {
        "Open": Open,
        "ToggledOn": ToggledOn,
        "Cooked": Cooked,
        "Frozen": Frozen,
    }

    # Persistence gate for committed transitions (matches the embodiedClaw
    # runtime tracker so online and offline emit identical timing).
    _STABLE_TRANSITION_FRAMES = 50

    def __init__(
        self,
        diagnostic_at_edge_pairs=None,
        diagnostic_stop_on_at_edge=False,
        diagnostic_global_at_edge=False,
        diagnostic_at_edge_profile_interval=0,
    ):
        self._configured_at_edge_pairs = []
        for pair in diagnostic_at_edge_pairs or []:
            if len(pair) != 2:
                raise ValueError(f"AtEdge pair must contain exactly two object identifiers, got: {pair}")
            self._configured_at_edge_pairs.append(tuple(pair))
        self._diagnostic_stop_on_at_edge = diagnostic_stop_on_at_edge
        self._diagnostic_global_at_edge = diagnostic_global_at_edge
        self._at_edge_profile_interval = max(0, int(diagnostic_at_edge_profile_interval or 0))
        self.transitions = []
        self.tracked_predicates = []
        self.prev_states = {}
        self._goal_heads = []
        self._task_objects = []
        self._scene_to_scope = {}  # obj.name -> BDDL scope_name (inst_name)
        self._at_edge_task_objects = []
        self._at_edge_detail_predicate_ids = set()
        self._at_edge_diagnostic_log_cache = {}
        self._at_edge_profile = self._new_at_edge_profile()
        self._diagnostic_stop_predicate_id = None
        self._robot = None
        # Hysteresis bookkeeping; cleared when prev_states is reset.
        self._pending_values = {}
        self._agreement_counters = {}
        # Cached commit closures alongside pending values. Needed so that
        # `flush_pending` (called at episode-end on a successful terminate)
        # can commit late flips that didn't accumulate the full 50-step
        # stability window before the sim exited. Without this, the goal
        # predicate that satisfied the task is lost from the recording.
        self._pending_commit_fns = {}
        # Per-transition grasp trail (raw scene names) — preserved for
        # consumers that want scene-level identity, separate from the
        # canonical transition stream.
        self.grasp_history = []

    def start(self, env, robot):
        """Initialize tracking for a new episode."""
        self.transitions = []
        self.tracked_predicates = []
        self.prev_states = {}
        self._goal_heads = []
        self._task_objects = []
        self._scene_to_scope = {}
        self._at_edge_task_objects = []
        self._at_edge_detail_predicate_ids = set()
        self._at_edge_diagnostic_log_cache = {}
        self._at_edge_profile = self._new_at_edge_profile()
        self._diagnostic_stop_predicate_id = None
        self._pending_values = {}
        self._agreement_counters = {}
        self._pending_commit_fns = {}
        self.grasp_history = []
        self._robot = robot
        task = env.task

        # 1. Goal conditions intentionally NOT tracked. They duplicate the
        # underlying spatial/grasp predicates (e.g. goal_2.inside fires at the
        # same step as Inside_can__of__soda.n.01_3_ashcan.n.01_1) and add
        # lowercase-name noise to the offline predicate window. The offline
        # judge derives goal satisfaction from the canonical predicate rows.

        # 2. Grasping per arm
        try:
            for arm in robot.arm_names:
                pred_id = f"grasp_{arm}"
                self.tracked_predicates.append({
                    "id": pred_id,
                    "type": "grasp",
                    "arm": arm,
                })
                obj = robot._ag_obj_in_hand.get(arm)
                self.prev_states[pred_id] = obj.name if obj is not None else None
        except Exception as e:
            logger.warning(f"BDDLStateTracker: Failed to parse grasp states: {e}")

        # 3. Collect task objects (non-system, existing entities). Also build
        # a scene_name -> BDDL scope_name map so grasp transitions can emit
        # the canonical scope identity.
        try:
            if hasattr(task, "object_scope"):
                for inst_name, entity in task.object_scope.items():
                    if entity is None or entity.is_system or not entity.exists:
                        continue
                    obj = entity.wrapped_obj
                    if obj is not None and inst_name != "agent.n.01_1":
                        self._task_objects.append((inst_name, obj))
                        scene_name = getattr(obj, "name", None)
                        if isinstance(scene_name, str) and scene_name:
                            self._scene_to_scope[scene_name] = inst_name
        except Exception as e:
            logger.warning(f"BDDLStateTracker: Failed to collect task objects: {e}")

        # AtEdge uses collision-mesh projection and raycasts. By default we
        # track only explicitly requested diagnostic pairs; global tracking is
        # guarded by diagnostic_global_at_edge so profiling can measure the
        # rollout cost before making it part of the normal predicate surface.
        objects_by_identifier = {}
        configured_pair_keys = set()
        at_edge_states_by_obj = {}
        for inst_name, obj in self._task_objects:
            objects_by_identifier[inst_name] = (inst_name, obj)
            scene_name = getattr(obj, "name", None)
            if isinstance(scene_name, str) and scene_name:
                objects_by_identifier[scene_name] = (inst_name, obj)
        for obj_identifier, support_identifier in self._configured_at_edge_pairs:
            obj_entry = objects_by_identifier.get(obj_identifier)
            support_entry = objects_by_identifier.get(support_identifier)
            if obj_entry is None or support_entry is None:
                logger.warning(
                    "BDDLStateTracker: Could not resolve diagnostic AtEdge pair "
                    f"({obj_identifier}, {support_identifier})"
                )
                continue
            inst_a, obj_a = obj_entry
            inst_b, obj_b = support_entry
            pred_id = self._add_at_edge_predicate(inst_a, obj_a, inst_b, obj_b, at_edge_states_by_obj)
            if pred_id is not None:
                configured_pair_keys.add((inst_a, inst_b))
                self._at_edge_detail_predicate_ids.add(pred_id)

        if self._diagnostic_global_at_edge:
            start_time = time.perf_counter()
            added = 0
            for inst_a, obj_a in self._task_objects:
                for inst_b, obj_b in self._task_objects:
                    if inst_a == inst_b or (inst_a, inst_b) in configured_pair_keys:
                        continue
                    if self._add_at_edge_predicate(inst_a, obj_a, inst_b, obj_b, at_edge_states_by_obj) is not None:
                        added += 1
            elapsed_ms = (time.perf_counter() - start_time) * 1000.0
            logger.info(
                "BDDLStateTracker: Global AtEdge enabled, added %d candidates "
                "(%d total AtEdge predicates, init %.1f ms, footprint cache %.1f ms)",
                added,
                len(self._at_edge_task_objects),
                elapsed_ms,
                sum(
                    state.footprint_cache_seconds
                    for state in {entry[2] for entry in self._at_edge_task_objects}
                ) * 1000.0,
            )

        # 4. Pairwise binary spatial predicates between task objects
        for inst_a, obj_a in self._task_objects:
            for inst_b, obj_b in self._task_objects:
                if inst_a == inst_b:
                    continue
                for state_name, state_cls in self.BINARY_STATE_CLASSES.items():
                    if state_cls not in obj_a.states:
                        continue
                    pred_id = f"{state_name}_{inst_a}_{inst_b}"
                    self.tracked_predicates.append({
                        "id": pred_id,
                        "type": "binary_state",
                        "predicate": state_name,
                        "args": [inst_a, inst_b],
                    })
                    try:
                        self.prev_states[pred_id] = bool(obj_a.states[state_cls].get_value(obj_b))
                    except Exception:
                        self.prev_states[pred_id] = False

        # 5. Unary predicates on task objects
        for inst_name, obj in self._task_objects:
            for state_name, state_cls in self.UNARY_STATE_CLASSES.items():
                if state_cls not in obj.states:
                    continue
                pred_id = f"{state_name}_{inst_name}"
                self.tracked_predicates.append({
                    "id": pred_id,
                    "type": "unary_state",
                    "predicate": state_name,
                    "args": [inst_name],
                })
                try:
                    self.prev_states[pred_id] = bool(obj.states[state_cls].get_value())
                except Exception:
                    self.prev_states[pred_id] = False

        logger.info(
            f"BDDLStateTracker: Tracking {len(self.tracked_predicates)} predicates "
            f"({len(self._goal_heads)} goal, {len(self._task_objects)} task objects)"
        )

    def _get_or_create_at_edge_state(self, inst_name, obj, cache):
        scene_name = getattr(obj, "name", inst_name)
        cache_key = scene_name or inst_name
        if cache_key in cache:
            return cache[cache_key]
        if AtEdge in obj.states:
            at_edge_state = obj.states[AtEdge]
        else:
            compatible, reason = AtEdge.is_compatible(obj)
            if not compatible:
                logger.warning(f"BDDLStateTracker: {inst_name} does not support AtEdge: {reason}")
                cache[cache_key] = None
                return None
            at_edge_state = AtEdge(obj=obj)
            at_edge_state.initialize()
        cache[cache_key] = at_edge_state
        return at_edge_state

    @staticmethod
    def _new_at_edge_profile():
        return {
            "steps": 0,
            "checks": 0,
            "seconds": 0.0,
            "on_top_seconds": 0.0,
            "world_transform_seconds": 0.0,
            "raycast_seconds": 0.0,
            "world_transforms": 0,
            "raycast_pairs": 0,
        }

    def _add_at_edge_predicate(self, inst_a, obj_a, inst_b, obj_b, state_cache):
        at_edge_state = self._get_or_create_at_edge_state(inst_a, obj_a, state_cache)
        if at_edge_state is None:
            return None
        pred_id = f"AtEdge_{inst_a}_{inst_b}"
        if pred_id in self.prev_states:
            return pred_id
        self.tracked_predicates.append({
            "id": pred_id,
            "type": "binary_state",
            "predicate": "AtEdge",
            "args": [inst_a, inst_b],
        })
        self._at_edge_task_objects.append((pred_id, inst_a, at_edge_state, inst_b, obj_b))
        try:
            diagnostic = at_edge_state.get_diagnostic(obj_b)
            self.prev_states[pred_id] = bool(diagnostic.get("is_at_edge", False))
        except Exception:
            self.prev_states[pred_id] = False
        return pred_id

    def step(self, env, robot, step_idx):
        """Check all tracked predicates for state changes after an env step.

        Every flip goes through ``_commit_stable_transition`` which enforces
        a 50-step persistence gate before recording into ``self.transitions``.
        """
        # 1. Goal conditions
        for i, head in enumerate(self._goal_heads):
            pred_id = f"goal_{i}"
            try:
                new_val = bool(head.evaluate())
            except Exception:
                continue
            self._commit_stable_transition(
                step_idx, pred_id, new_val,
                lambda old_val, new_val, i=i, pred_id=pred_id: self._record_goal_transition(
                    step_idx, pred_id, i, old_val, new_val,
                ),
            )

        # 2. Grasping per arm
        for arm in robot.arm_names:
            pred_id = f"grasp_{arm}"
            if pred_id not in self.prev_states:
                continue
            try:
                obj = robot._ag_obj_in_hand.get(arm)
                new_val = obj.name if obj is not None else None
            except Exception:
                continue
            self._commit_stable_transition(
                step_idx, pred_id, new_val,
                lambda old_val, new_val, arm=arm, pred_id=pred_id: self._record_grasp_transition(
                    step_idx, pred_id, arm, old_val, new_val,
                ),
            )

        # 3. Binary spatial predicates
        for inst_a, obj_a in self._task_objects:
            for inst_b, obj_b in self._task_objects:
                if inst_a == inst_b:
                    continue
                for state_name, state_cls in self.BINARY_STATE_CLASSES.items():
                    pred_id = f"{state_name}_{inst_a}_{inst_b}"
                    if pred_id not in self.prev_states:
                        continue
                    try:
                        new_val = bool(obj_a.states[state_cls].get_value(obj_b))
                    except Exception:
                        continue
                    self._commit_stable_transition(
                        step_idx, pred_id, new_val,
                        lambda old_val, new_val, state_name=state_name,
                            inst_a=inst_a, inst_b=inst_b,
                            pred_id=pred_id: self._record_binary_transition(
                            step_idx, pred_id, state_name, inst_a, inst_b,
                            old_val, new_val,
                        ),
                    )

        # 3b. Selective AtEdge predicates requested for diagnostic rollouts.
        at_edge_start = time.perf_counter() if self._at_edge_profile_interval and self._at_edge_task_objects else None
        at_edge_checks = 0
        for pred_id, inst_a, at_edge_state, inst_b, obj_b in self._at_edge_task_objects:
            try:
                diagnostic = at_edge_state.get_diagnostic(obj_b)
                timings = diagnostic.get("timings", {})
                self._at_edge_profile["on_top_seconds"] += timings.get("on_top_seconds", 0.0)
                self._at_edge_profile["world_transform_seconds"] += timings.get(
                    "world_transform_seconds", 0.0
                )
                self._at_edge_profile["raycast_seconds"] += timings.get("raycast_seconds", 0.0)
                self._at_edge_profile["world_transforms"] += int(
                    diagnostic.get("world_transform_performed", False)
                )
                self._at_edge_profile["raycast_pairs"] += int(diagnostic.get("reason") == "ray_result")
                if pred_id in self._at_edge_detail_predicate_ids:
                    self._log_at_edge_diagnostic(step_idx, pred_id, inst_a, inst_b, diagnostic)
                new_val = bool(diagnostic.get("is_at_edge", False))
                at_edge_checks += 1
            except Exception:
                continue
            self._commit_stable_transition(
                step_idx, pred_id, new_val,
                lambda old_val, new_val, inst_a=inst_a, inst_b=inst_b,
                    pred_id=pred_id: self._record_binary_transition(
                    step_idx, pred_id, "AtEdge", inst_a, inst_b,
                    old_val, new_val,
                ),
            )
        if at_edge_start is not None:
            elapsed = time.perf_counter() - at_edge_start
            self._at_edge_profile["steps"] += 1
            self._at_edge_profile["checks"] += at_edge_checks
            self._at_edge_profile["seconds"] += elapsed
            if step_idx % self._at_edge_profile_interval == 0:
                steps = self._at_edge_profile["steps"]
                checks = self._at_edge_profile["checks"]
                seconds = self._at_edge_profile["seconds"]
                on_top_seconds = self._at_edge_profile["on_top_seconds"]
                transform_seconds = self._at_edge_profile["world_transform_seconds"]
                raycast_seconds = self._at_edge_profile["raycast_seconds"]
                measured_seconds = on_top_seconds + transform_seconds + raycast_seconds
                logger.info(
                    "AtEdge profile step=%d candidates=%d checked=%d "
                    "avg_step_ms=%.3f avg_pair_ms=%.3f on_top_ms=%.3f "
                    "transform_ms=%.3f raycast_ms=%.3f other_ms=%.3f "
                    "world_transforms=%d raycast_pairs=%d total_ms=%.1f",
                    step_idx,
                    len(self._at_edge_task_objects),
                    at_edge_checks,
                    (seconds / steps) * 1000.0 if steps else 0.0,
                    (seconds / checks) * 1000.0 if checks else 0.0,
                    (on_top_seconds / steps) * 1000.0 if steps else 0.0,
                    (transform_seconds / steps) * 1000.0 if steps else 0.0,
                    (raycast_seconds / steps) * 1000.0 if steps else 0.0,
                    ((seconds - measured_seconds) / steps) * 1000.0 if steps else 0.0,
                    self._at_edge_profile["world_transforms"],
                    self._at_edge_profile["raycast_pairs"],
                    seconds * 1000.0,
                )

    def _log_at_edge_diagnostic(self, step_idx, pred_id, inst_a, inst_b, diagnostic):
        reason = diagnostic.get("reason")
        sample_count = diagnostic.get("sample_count")
        hit_count = diagnostic.get("hit_count")
        miss_count = diagnostic.get("miss_count")
        ratio = diagnostic.get("ratio")
        result = bool(diagnostic.get("is_at_edge", False))
        ratio_for_cache = round(ratio, 3) if isinstance(ratio, float) and math.isfinite(ratio) else ratio
        signature = (reason, sample_count, hit_count, miss_count, ratio_for_cache, result)
        if self._at_edge_diagnostic_log_cache.get(pred_id) == signature:
            return
        self._at_edge_diagnostic_log_cache[pred_id] = signature

        if ratio is None:
            ratio_text = "n/a"
        else:
            ratio_text = f"{ratio:.3f}" if math.isfinite(ratio) else "inf"
        logger.info(
            "AtEdge diagnostic step=%d pred=%s obj=%s support=%s reason=%s "
            "samples=%s hits=%s misses=%s miss_hit_ratio=%s threshold=%.3f result=%s",
            step_idx,
            pred_id,
            inst_a,
            inst_b,
            reason,
            sample_count,
            hit_count,
            miss_count,
            ratio_text,
            MIN_MISS_HIT_RATIO,
            result,
        )

        # 4. Unary predicates
        for inst_name, obj in self._task_objects:
            for state_name, state_cls in self.UNARY_STATE_CLASSES.items():
                pred_id = f"{state_name}_{inst_name}"
                if pred_id not in self.prev_states:
                    continue
                try:
                    new_val = bool(obj.states[state_cls].get_value())
                except Exception:
                    continue
                self._commit_stable_transition(
                    step_idx, pred_id, new_val,
                    lambda old_val, new_val, state_name=state_name,
                        inst_name=inst_name,
                        pred_id=pred_id: self._record_unary_transition(
                        step_idx, pred_id, state_name, inst_name,
                        old_val, new_val,
                    ),
                )

    # ------------------------------------------------------------------
    # Debounce + per-source recording helpers
    # ------------------------------------------------------------------

    def _commit_stable_transition(self, step_idx, pred_id, new_val, commit_fn):
        """Commit a flip only after ``_STABLE_TRANSITION_FRAMES`` agreements."""
        old_val = self.prev_states.get(pred_id)
        if new_val == old_val:
            self._pending_values.pop(pred_id, None)
            self._agreement_counters.pop(pred_id, None)
            self._pending_commit_fns.pop(pred_id, None)
            return
        if self._pending_values.get(pred_id) == new_val:
            self._agreement_counters[pred_id] = self._agreement_counters.get(pred_id, 0) + 1
        else:
            self._pending_values[pred_id] = new_val
            self._agreement_counters[pred_id] = 1
        # Keep the latest commit_fn so flush_pending can call it at
        # episode termination if the counter never reached the stability gate.
        self._pending_commit_fns[pred_id] = commit_fn
        if self._agreement_counters[pred_id] >= self._STABLE_TRANSITION_FRAMES:
            commit_fn(old_val, new_val)
            self._pending_values.pop(pred_id, None)
            self._agreement_counters.pop(pred_id, None)
            self._pending_commit_fns.pop(pred_id, None)

    def flush_pending(self):
        """Commit every still-pending flip with at least one agreement frame.

        Called at episode termination so a predicate that flipped just before
        the sim exited (e.g. the final goal condition that triggered
        terminated=True) makes it into ``self.transitions`` even though it
        never accumulated ``_STABLE_TRANSITION_FRAMES`` of persistence.

        Returns a list of (pred_id, agreement_count) for logging.
        """
        flushed = []
        for pred_id in list(self._pending_values.keys()):
            pending_val = self._pending_values[pred_id]
            counter = self._agreement_counters.get(pred_id, 0)
            commit_fn = self._pending_commit_fns.get(pred_id)
            if counter < 1 or commit_fn is None:
                continue
            old_val = self.prev_states.get(pred_id)
            try:
                commit_fn(old_val, pending_val)
                flushed.append((pred_id, counter))
            except Exception as e:  # noqa: BLE001
                logger.debug(f"flush_pending: commit failed for {pred_id}: {e}")
            self._pending_values.pop(pred_id, None)
            self._agreement_counters.pop(pred_id, None)
            self._pending_commit_fns.pop(pred_id, None)
        return flushed

    def _record_goal_transition(self, step_idx, pred_id, goal_idx, old_val, new_val):
        info = self.tracked_predicates[goal_idx]
        self.transitions.append({
            "step_idx": step_idx,
            "predicate_id": pred_id,
            "predicate_name": info.get("predicate", "unknown"),
            "args": info.get("args", []),
            "old_value": old_val,
            "new_value": new_val,
        })
        self.prev_states[pred_id] = new_val

    def _record_grasp_transition(self, step_idx, pred_id, arm, old_val, new_val):
        """Emit a grasp in the same schema as binary spatial predicates.

        ``args[0]`` is the robot arm scope (``robot.r1.<arm>``); ``args[1]``
        is the grasped object's BDDL scope_name + scene_name. ``old_value``
        / ``new_value`` are booleans (true = grasping). The full per-arm
        per-instance trail stays in ``self.grasp_history`` for any consumer
        that needs scene-level identity.
        """
        self.grasp_history.append({
            "step_idx": step_idx,
            "arm": arm,
            "old_value": old_val,
            "new_value": new_val,
        })
        grasped_scene = new_val if new_val is not None else old_val
        grasped_scope = (
            self._scene_to_scope.get(grasped_scene, grasped_scene)
            if isinstance(grasped_scene, str)
            else ""
        )
        self.transitions.append({
            "step_idx": step_idx,
            "predicate_id": pred_id,
            "predicate_name": "IsGrasping",
            "args": [
                {"scope_name": f"robot.r1.{arm}", "scene_name": f"robot.r1.{arm}"},
                {"scope_name": str(grasped_scope or ""),
                 "scene_name": str(grasped_scene or "")},
            ],
            "old_value": old_val is not None,
            "new_value": new_val is not None,
        })
        self.prev_states[pred_id] = new_val

    def _record_binary_transition(self, step_idx, pred_id, state_name,
                                  inst_a, inst_b, old_val, new_val):
        self.transitions.append({
            "step_idx": step_idx,
            "predicate_id": pred_id,
            "predicate_name": state_name,
            "obj_a": inst_a,
            "obj_b": inst_b,
            "old_value": old_val,
            "new_value": new_val,
        })
        self.prev_states[pred_id] = new_val
        if state_name == "AtEdge" and new_val and self._diagnostic_stop_on_at_edge:
            self._diagnostic_stop_predicate_id = pred_id

    @property
    def diagnostic_stop_predicate_id(self):
        return self._diagnostic_stop_predicate_id

    def _record_unary_transition(self, step_idx, pred_id, state_name,
                                 inst_name, old_val, new_val):
        self.transitions.append({
            "step_idx": step_idx,
            "predicate_id": pred_id,
            "predicate_name": state_name,
            "obj_a": inst_name,
            "old_value": old_val,
            "new_value": new_val,
        })
        self.prev_states[pred_id] = new_val

    def get_results(self, task_name, instance_id, demo_id, total_steps, success):
        """Return JSON-serializable dict of per-episode tracking data.

        The predicate catalog (``tracked_predicates``) is deterministic in
        ``(task_name, instance_id)`` and therefore lives in a sibling file
        written once per (task, instance); see ``get_predicate_catalog``.
        """
        return {
            "task_name": task_name,
            "instance_id": instance_id,
            "demo_id": demo_id,
            "total_steps": total_steps,
            "success": success,
            "transitions": self.transitions,
            "grasp_history": self.grasp_history,
        }

    def get_predicate_catalog(self, task_name, instance_id):
        """Return the (task, instance)-scoped predicate catalog.

        Same content for every episode of a given (task, instance), so the
        recorder writes it once per (task, instance) rather than embedding
        a copy in each ``episode_*_bddl_transitions.json``.
        """
        return {
            "task_name": task_name,
            "instance_id": instance_id,
            "tracked_predicates": self.tracked_predicates,
        }


class DirectDataRecorder:
    """
    Records data directly to parquet and video files during evaluation,
    bypassing the intermediate HDF5 step.
    """

    def __init__(
        self,
        output_folder: str,
        task_name: str,
        task_id: int,
        demo_id: int,
        camera_names: Dict[str, str] = ROBOT_CAMERA_NAMES["R1Pro"],
        record_rgb: bool = True,
        record_depth: bool = True,
        only_successes: bool = False,
    ):
        self.output_folder = output_folder
        self.task_name = task_name
        self.task_id = task_id
        self.demo_id = demo_id
        self.camera_names = camera_names
        self.record_rgb = record_rgb
        self.record_depth = record_depth
        self.only_successes = only_successes

        # Data buffers for parquet
        self.actions = []
        self.proprios = []
        self.cam_rel_poses = []
        self.task_infos = []

        # Video writers
        self.video_writers: Dict[str, Tuple[Container, Stream]] = {}

        # Frame buffers for video (to batch write)
        self.rgb_buffers: Dict[str, List[np.ndarray]] = {}
        self.depth_buffers: Dict[str, List[np.ndarray]] = {}

        self.step_count = 0
        self.is_recording = False

        # Create output directories
        self._setup_directories()

    def _setup_directories(self):
        """Create output directories for parquet, videos, and trajectories under both success/ and failure/."""
        for outcome in ("success", "failure"):
            # Parquet directory
            parquet_dir = os.path.join(
                self.output_folder, outcome, "2025-challenge-demos", "data", f"task-{self.task_id:04d}"
            )
            makedirs_with_mode(parquet_dir)

            # Metadata directory
            meta_dir = os.path.join(
                self.output_folder, outcome, "2025-challenge-demos", "meta", "episodes", f"task-{self.task_id:04d}"
            )
            makedirs_with_mode(meta_dir)

            # Trajectories (HDF5) directory
            traj_dir = os.path.join(
                self.output_folder, outcome, "2025-challenge-demos", "trajectories", f"task-{self.task_id:04d}"
            )
            makedirs_with_mode(traj_dir)

            # Video directories
            if self.record_rgb or self.record_depth:
                for camera_id, camera_name in self.camera_names.items():
                    if self.record_rgb:
                        rgb_dir = os.path.join(
                            self.output_folder,
                            outcome,
                            "2025-challenge-demos",
                            "videos",
                            f"task-{self.task_id:04d}",
                            f"observation.images.rgb.{camera_id}",
                        )
                        makedirs_with_mode(rgb_dir)

                    if self.record_depth:
                        depth_dir = os.path.join(
                            self.output_folder,
                            outcome,
                            "2025-challenge-demos",
                            "videos",
                            f"task-{self.task_id:04d}",
                            f"observation.images.depth.{camera_id}",
                        )
                        makedirs_with_mode(depth_dir)

    def _get_outcome_folder(self, success: bool) -> str:
        """Return the outcome subfolder path ('success' or 'failure')."""
        return os.path.join(self.output_folder, "success" if success else "failure")

    def start_episode(self):
        """Start recording a new episode.

        Files are written to a _staging/ subfolder first, then moved to
        success/ or failure/ at the end of the episode.
        """
        self.actions = []
        self.proprios = []
        self.cam_rel_poses = []
        self.task_infos = []
        self.step_count = 0
        self.is_recording = True

        # Staging folder for in-progress episode
        self._staging_dir = os.path.join(self.output_folder, "_staging")
        self._staging_video_paths: List[str] = []

        # Initialize video writers (writing to staging area)
        for camera_id, camera_name in self.camera_names.items():
            resolution = HEAD_RESOLUTION if "zed" in camera_name else WRIST_RESOLUTION

            if self.record_rgb:
                rgb_dir = os.path.join(
                    self._staging_dir,
                    "2025-challenge-demos",
                    "videos",
                    f"task-{self.task_id:04d}",
                    f"observation.images.rgb.{camera_id}",
                )
                makedirs_with_mode(rgb_dir)
                rgb_path = os.path.join(rgb_dir, f"episode_{self.demo_id:08d}.mp4")
                self._staging_video_paths.append(rgb_path)
                self.video_writers[f"{camera_name}::rgb"] = create_video_writer(
                    fpath=rgb_path,
                    resolution=resolution,
                    codec_name="libx265",
                    pix_fmt="yuv420p",
                    stream_options={"x265-params": "log-level=none"},
                )
                self.rgb_buffers[camera_name] = []

            if self.record_depth:
                depth_dir = os.path.join(
                    self._staging_dir,
                    "2025-challenge-demos",
                    "videos",
                    f"task-{self.task_id:04d}",
                    f"observation.images.depth.{camera_id}",
                )
                makedirs_with_mode(depth_dir)
                depth_path = os.path.join(depth_dir, f"episode_{self.demo_id:08d}.mp4")
                self._staging_video_paths.append(depth_path)
                self.video_writers[f"{camera_name}::depth_linear"] = create_video_writer(
                    fpath=depth_path,
                    resolution=resolution,
                    codec_name="libx265",
                    pix_fmt="yuv420p10le",
                    stream_options={"x265-params": "lossless=1:log-level=none"},
                )
                self.depth_buffers[camera_name] = []

    def record_step(
        self,
        action: np.ndarray,
        proprio: np.ndarray,
        cam_rel_poses: np.ndarray,
        task_info: Optional[np.ndarray],
        obs: Dict[str, th.Tensor],
    ):
        """Record a single step of data."""
        if not self.is_recording:
            return

        # Debug: log obs keys on first step
        if self.step_count == 0:
            logger.info(f"Observation keys: {list(obs.keys())}")
            for camera_id, camera_name in self.camera_names.items():
                logger.info(f"Looking for camera {camera_id}: {camera_name}::rgb")

        # Record low-dim data
        self.actions.append(action.copy() if isinstance(action, np.ndarray) else action.cpu().numpy().copy())
        self.proprios.append(proprio.copy() if isinstance(proprio, np.ndarray) else proprio.cpu().numpy().copy())
        self.cam_rel_poses.append(
            cam_rel_poses.copy() if isinstance(cam_rel_poses, np.ndarray) else cam_rel_poses.cpu().numpy().copy()
        )
        if task_info is not None:
            self.task_infos.append(
                task_info.copy() if isinstance(task_info, np.ndarray) else task_info.cpu().numpy().copy()
            )

        # Record video frames
        for camera_id, camera_name in self.camera_names.items():
            if self.record_rgb and f"{camera_name}::rgb" in obs:
                rgb_frame = obs[f"{camera_name}::rgb"]
                if isinstance(rgb_frame, th.Tensor):
                    rgb_frame = rgb_frame.cpu().numpy()
                self.rgb_buffers[camera_name].append(rgb_frame[..., :3].astype(np.uint8))

            if self.record_depth and f"{camera_name}::depth_linear" in obs:
                depth_frame = obs[f"{camera_name}::depth_linear"]
                if isinstance(depth_frame, th.Tensor):
                    depth_frame = depth_frame.cpu().numpy()
                self.depth_buffers[camera_name].append(depth_frame)

        self.step_count += 1

        # Periodically flush video buffers to avoid memory issues
        if self.step_count % 500 == 0:
            self._flush_video_buffers()
            logger.info(f"Flushed video buffers at step {self.step_count}, rgb_buffer_lens: {[len(v) for v in self.rgb_buffers.values()]}")

    def _flush_video_buffers(self):
        """Flush video buffers to disk."""
        for camera_name in self.camera_names.values():
            if self.record_rgb and camera_name in self.rgb_buffers and len(self.rgb_buffers[camera_name]) > 0:
                rgb_data = np.stack(self.rgb_buffers[camera_name], axis=0)
                write_video(
                    rgb_data,
                    video_writer=self.video_writers[f"{camera_name}::rgb"],
                    batch_size=len(rgb_data),
                    mode="rgb",
                )
                self.rgb_buffers[camera_name] = []

            if self.record_depth and camera_name in self.depth_buffers and len(self.depth_buffers[camera_name]) > 0:
                depth_data = np.stack(self.depth_buffers[camera_name], axis=0)
                write_video(
                    depth_data,
                    video_writer=self.video_writers[f"{camera_name}::depth_linear"],
                    batch_size=len(depth_data),
                    mode="depth",
                )
                self.depth_buffers[camera_name] = []

    def end_episode(self, success: bool, env=None, bddl_transitions=None, predicate_catalog=None, skill_annotation=None) -> bool:
        """
        End the current episode and save data.

        Data is saved to success/ or failure/ subfolder based on the outcome.
        When only_successes is True, only successful episodes are saved.
        When only_successes is False, all episodes (success and failure) are saved.

        Args:
            success: Whether the episode was successful.
            env: The OmniGibson environment, used to extract rich metadata (config, scene_file,
                 task_obs_keys, ins_id_mapping, unique_ins_ids per camera).
            bddl_transitions: Optional dict of BDDL state transitions to save as JSON.

        Returns:
            bool: Whether data was saved
        """
        if not self.is_recording:
            return False

        self.is_recording = False
        self._success = success

        # Check if we should save
        if self.only_successes and not success:
            logger.info(f"Episode failed, discarding data (only_successes={self.only_successes})")
            self._close_video_writers()
            self._cleanup_staging()
            return False

        # Flush remaining video buffers
        self._flush_video_buffers()

        # Close video writers
        self._close_video_writers()

        # Determine destination folder
        outcome_folder = self._get_outcome_folder(success)

        # Move staged video files to the outcome folder
        self._move_staged_videos(outcome_folder)

        # Save parquet directly to outcome folder
        self._save_parquet(outcome_folder)

        # Save metadata directly to outcome folder
        self._save_metadata(outcome_folder, success=success, env=env)

        # Save BDDL state transitions if provided
        if bddl_transitions is not None:
            meta_dir = os.path.join(
                outcome_folder, "2025-challenge-demos", "meta", "episodes", f"task-{self.task_id:04d}"
            )
            makedirs_with_mode(meta_dir)
            bddl_path = os.path.join(meta_dir, f"episode_{self.demo_id:08d}_bddl_transitions.json")
            with open(bddl_path, "w") as f:
                json.dump(bddl_transitions, f, indent=2)
            logger.info(f"Saved BDDL transitions to {bddl_path}")

            # Catalog is deterministic in (task_id, instance_id); write once
            # per (task, instance) and skip if already present. Idempotent so
            # parallel writers and re-runs are safe.
            if predicate_catalog is not None:
                instance_id = predicate_catalog.get("instance_id")
                if instance_id is None:
                    instance_id = (self.demo_id % 10000) // 10
                cat_path = os.path.join(
                    meta_dir,
                    f"task_{self.task_id:04d}_instance_{int(instance_id):04d}_predicate_catalog.json",
                )
                if not os.path.exists(cat_path):
                    with open(cat_path, "w") as f:
                        json.dump(predicate_catalog, f, indent=2)
                    logger.info(f"Saved predicate catalog to {cat_path}")

        # Save HF-format skill/primitive annotation if provided
        if skill_annotation is not None:
            ann_dir = os.path.join(
                outcome_folder, "2025-challenge-demos", "annotations", f"task-{self.task_id:04d}"
            )
            makedirs_with_mode(ann_dir)
            ann_path = os.path.join(ann_dir, f"episode_{self.demo_id:08d}.json")
            with open(ann_path, "w") as f:
                json.dump(skill_annotation, f, indent=2)
            logger.info(f"Saved skill annotation to {ann_path}")

        logger.info(f"Saved episode data to {outcome_folder} (success={success})")
        return True

    def _close_video_writers(self):
        """Close all video writers."""
        for key, (container, stream) in self.video_writers.items():
            try:
                # Flush any remaining packets
                for packet in stream.encode():
                    container.mux(packet)
                container.close()
            except Exception as e:
                logger.warning(f"Error closing video writer {key}: {e}")
        self.video_writers = {}

    def _cleanup_staging(self):
        """Remove staged video files for discarded episodes."""
        for path in getattr(self, "_staging_video_paths", []):
            if os.path.exists(path):
                os.remove(path)

    def _move_staged_videos(self, outcome_folder: str):
        """Move staged video files to the appropriate outcome folder."""
        for src_path in getattr(self, "_staging_video_paths", []):
            if not os.path.exists(src_path):
                continue
            # Compute the relative path from the staging dir
            rel_path = os.path.relpath(src_path, self._staging_dir)
            dst_path = os.path.join(outcome_folder, rel_path)
            makedirs_with_mode(os.path.dirname(dst_path))
            shutil.move(src_path, dst_path)

    def _save_parquet(self, outcome_folder: str):
        """Save low-dimensional data to parquet file."""
        if len(self.actions) == 0:
            logger.warning("No data to save to parquet")
            return

        T = len(self.actions)
        actions = np.array(self.actions, dtype=np.float32)
        proprios = np.array(self.proprios, dtype=np.float32)
        cam_rel_poses = np.array(self.cam_rel_poses, dtype=np.float32)

        data = {
            "index": np.arange(T, dtype=np.int64),
            "episode_index": np.zeros(T, dtype=np.int64) + self.demo_id,
            "task_index": np.zeros(T, dtype=np.int64) + self.task_id,
            "timestamp": np.arange(T, dtype=np.float64) / 30.0,  # 30 fps
            "observation.state": list(proprios),
            "observation.cam_rel_poses": list(cam_rel_poses),
            "action": list(actions),
        }

        if len(self.task_infos) > 0:
            task_infos = np.array(self.task_infos, dtype=np.float32)
            data["observation.task_info"] = list(task_infos)

        df = pd.DataFrame(data)
        parquet_dir = os.path.join(
            outcome_folder, "2025-challenge-demos", "data", f"task-{self.task_id:04d}"
        )
        makedirs_with_mode(parquet_dir)
        parquet_path = os.path.join(parquet_dir, f"episode_{self.demo_id:08d}.parquet")
        df.to_parquet(parquet_path, index=False)
        logger.info(f"Saved parquet to {parquet_path}")

    def _save_metadata(self, outcome_folder: str, success: bool = False, env=None):
        """Save metadata JSON file matching the format produced by replay_obs.py.

        When env is provided, the metadata includes:
          - config: full environment config (JSON string)
          - scene_file: saved scene state (JSON string)
          - n_episodes, n_steps, num_samples: episode statistics
          - robot_type: "R1Pro"
          - task_obs_keys: task observation key names
          - ins_id_mapping: JSON-serialized VisionSensor.INSTANCE_ID_REGISTRY
          - {camera_name}::unique_ins_ids: unique instance IDs per camera
          - success: whether the episode was successful
        """
        n_steps = len(self.actions)
        metadata = {
            "n_episodes": 1,
            "n_steps": n_steps,
            "num_samples": n_steps,
            "robot_type": "R1Pro",
            "success": success,
        }

        if env is not None:
            try:
                # Unwrap to get the base OG environment
                base_env = env
                while hasattr(base_env, "env"):
                    base_env = base_env.env

                # Config
                from copy import deepcopy
                config = deepcopy(base_env.config)
                metadata["config"] = json.dumps(config, default=str)

                # Scene file
                scene_file = base_env.scene.save()
                metadata["scene_file"] = json.dumps(scene_file, default=str)

                # Task observation keys
                if hasattr(base_env, "task") and hasattr(base_env.task, "low_dim_obs_keys"):
                    metadata["task_obs_keys"] = list(base_env.task.low_dim_obs_keys)

                # Instance ID mapping
                metadata["ins_id_mapping"] = json.dumps(VisionSensor.INSTANCE_ID_REGISTRY)

                # Unique instance IDs per camera
                camera_names_set = set(self.camera_names.values())
                for sensor_name in base_env.robots[0].sensors:
                    full_name = f"robot_r1::{sensor_name}"
                    if full_name in camera_names_set:
                        sensor = base_env.robots[0].sensors[sensor_name]
                        if hasattr(sensor, "get_obs") and callable(sensor.get_obs):
                            try:
                                obs = sensor.get_obs()
                                if "seg_instance_id" in obs:
                                    unique_ids = th.unique(obs["seg_instance_id"]).to(th.uint32).tolist()
                                    metadata[f"{full_name}::unique_ins_ids"] = unique_ids
                            except Exception:
                                pass
            except Exception as e:
                logger.warning(f"Could not extract rich metadata from env: {e}")

        meta_dir = os.path.join(
            outcome_folder, "2025-challenge-demos", "meta", "episodes", f"task-{self.task_id:04d}"
        )
        makedirs_with_mode(meta_dir)
        meta_path = os.path.join(meta_dir, f"episode_{self.demo_id:08d}.json")
        with open(meta_path, "w") as f:
            json.dump(metadata, f, indent=4)
        logger.info(f"Saved metadata to {meta_path}")


class EvaluatorWithDirectRecording:
    """
    Evaluator class that directly records data to parquet and video files
    during policy evaluation, bypassing the intermediate HDF5 step.
    """

    def __init__(self, cfg: DictConfig) -> None:
        self.cfg = cfg

        # record total number and success number of trials and trial time
        self.n_trials = 0
        self.n_success_trials = 0
        self.total_time = 0
        self.robot_action = dict()
        self.data_recorder: Optional[DirectDataRecorder] = None
        self.hdf5_recorder = None
        self.output_folder: Optional[str] = None
        diagnostic_at_edge_pairs = OmegaConf.select(cfg, "diagnostic_at_edge_pairs", default=[])
        if OmegaConf.is_config(diagnostic_at_edge_pairs):
            diagnostic_at_edge_pairs = OmegaConf.to_container(diagnostic_at_edge_pairs, resolve=True)
        self.bddl_tracker = BDDLStateTracker(
            diagnostic_at_edge_pairs=diagnostic_at_edge_pairs,
            diagnostic_stop_on_at_edge=bool(
                OmegaConf.select(cfg, "diagnostic_stop_on_at_edge", default=False)
            ),
            diagnostic_global_at_edge=bool(
                OmegaConf.select(cfg, "diagnostic_global_at_edge", default=False)
            ),
            diagnostic_at_edge_profile_interval=int(
                OmegaConf.select(cfg, "diagnostic_at_edge_profile_interval", default=0)
            ),
        )
        self._diagnostic_post_at_edge_steps = int(
            OmegaConf.select(cfg, "diagnostic_post_at_edge_steps", default=0)
        )
        if self._diagnostic_post_at_edge_steps < 0:
            raise ValueError("diagnostic_post_at_edge_steps must be non-negative")
        self._diagnostic_stop_at_step = None

        self.env = self.load_env(env_wrapper=self.cfg.env_wrapper)
        self.policy = self.load_policy()
        if hasattr(self.policy, "attach_env"):
            self.policy.attach_env(self.env)
        self.robot = self.load_robot()
        self.metrics = self.load_metrics()

        self.reset()
        # manually reset environment episode number
        self.env._current_episode = 0
        self._video_writer = None

    def load_env(self, env_wrapper: DictConfig) -> EnvironmentWrapper:
        """
        Read the environment config file and create the environment.
        """
        # Disable a subset of transition rules for data collection
        for rule in DISABLED_TRANSITION_RULES:
            rule.ENABLED = False
        # Load config file
        task_name = self.cfg.task.name
        task_cfg = OmegaConf.to_container(
            self.cfg.authoritative_task_config,
            resolve=True,
        )
        if not isinstance(task_cfg, dict):
            raise ValueError("authoritative task configuration is unavailable")
        # Now, get human stats of the task
        task_idx = TASK_NAMES_TO_INDICES[task_name]
        self.human_stats = {
            "length": [],
            "distance_traveled": [],
            "left_eef_displacement": [],
            "right_eef_displacement": [],
        }
        with open(os.path.join(gm.DATA_PATH, "2025-challenge-task-instances", "metadata", "episodes.jsonl"), "r") as f:
            episodes = [json.loads(line) for line in f]
        for episode in episodes:
            if episode["episode_index"] // 1e4 == task_idx:
                for k in self.human_stats.keys():
                    self.human_stats[k].append(episode[k])
        # take a mean
        for k in self.human_stats.keys():
            self.human_stats[k] = sum(self.human_stats[k]) / len(self.human_stats[k])

        # Load the seed instance by default
        robot_type = self.cfg.robot.type
        assert robot_type == "R1Pro", f"Got invalid robot type: {robot_type}, only R1Pro is supported."
        cfg = generate_basic_environment_config(task_name=task_name, task_cfg=task_cfg)
        if self.cfg.partial_scene_load:
            relevant_rooms = get_task_relevant_room_types(activity_name=task_name)
            relevant_rooms = augment_rooms(relevant_rooms, task_cfg["scene_model"], task_name)
            cfg["scene"]["load_room_types"] = relevant_rooms

        cfg["robots"] = [
            generate_robot_config(
                task_name=task_name,
                task_cfg=task_cfg,
            )
        ]
        # Update observation modalities - include depth for recording
        cfg["robots"][0]["obs_modalities"] = ["proprio", "rgb", "depth_linear"]
        cfg["robots"][0]["proprio_obs"] = list(PROPRIOCEPTION_INDICES["R1Pro"].keys())
        if self.cfg.robot.controllers is not None:
            cfg["robots"][0]["controller_config"].update(self.cfg.robot.controllers)
        if self.cfg.max_steps is None:
            logger.info(
                f"Setting timeout to be 2x the average length of human demos: {int(self.human_stats['length'] * 2)}"
            )
            cfg["task"]["termination_config"]["max_steps"] = int(self.human_stats["length"] * 2)
        else:
            logger.info(f"Setting timeout to be {self.cfg.max_steps} steps through config.")
            cfg["task"]["termination_config"]["max_steps"] = self.cfg.max_steps
        cfg["task"]["include_obs"] = False
        # ──────────────────────────────────────────────────────────────────────
        # Optional 3rd-person observer camera for the behavior1k_mp pipeline.
        # Enabled by passing `++external_camera_enabled=true` from the CLI.
        # Captured into the obs dict as `external_sensor0::rgb` and recorded
        # to a separate video by the existing DirectDataRecorder loop.
        # ──────────────────────────────────────────────────────────────────────
        if getattr(self.cfg, "external_camera_enabled", False):
            ext_pos = list(getattr(self.cfg, "external_camera_position", [1.5, 2.0, 1.5]))
            ext_ori = list(getattr(self.cfg, "external_camera_orientation",
                                   [0.5, -0.5, 0.5, -0.5]))   # xyzw
            # Resolution is square to match WRIST_RESOLUTION (480, 480),
            # which is what the existing DirectDataRecorder uses for any camera
            # whose name does not contain "zed". Using a non-square resolution
            # would cause a video-encoder size mismatch.
            ext_h = int(getattr(self.cfg, "external_camera_height", 480))
            ext_w = int(getattr(self.cfg, "external_camera_width", 480))
            cfg["env"]["use_external_obs"] = True
            cfg["env"]["external_sensors"] = [{
                "sensor_type": "VisionSensor",
                "name": "external_sensor0",
                "relative_prim_path": "/external_sensor0",
                "modalities": ["rgb"],
                "sensor_kwargs": {"image_height": ext_h, "image_width": ext_w},
                "position": ext_pos,
                "orientation": ext_ori,
                "pose_frame": "scene",
            }]
            logger.info(
                f"External 3rd-person camera ENABLED at pos={ext_pos}, "
                f"ori(xyzw)={ext_ori}, size={ext_w}x{ext_h}"
            )
        env = og.Environment(configs=cfg)
        # instantiate env wrapper
        env = instantiate(env_wrapper, env=env)
        # Wrap with DataCollectionWrapper for HDF5 recording if recording_path is specified
        record_path = getattr(self.cfg, "recording_path", None)
        if record_path is not None:
            record_path = Path(record_path).expanduser()
            only_successes = getattr(self.cfg, "only_successes", False)
            logger.info(f"Recording rollouts to HDF5: {record_path} (only_successes={only_successes})")
            env = PolicyEvalDataCollectionWrapper(
                env=env,
                output_path=str(record_path),
                only_successes=only_successes,
                flush_every_n_traj=1,
            )
            self.hdf5_recorder = env
        return env

    def load_robot(self) -> BaseRobot:
        """Loads and returns the robot instance from the environment."""
        robot = self.env.scene.object_registry("name", "robot_r1")
        return robot

    def load_policy(self) -> Any:
        """Loads and returns the policy instance."""
        policy = instantiate(self.cfg.model)
        logger.info("")
        logger.info("=" * 50)
        logger.info(f"Loaded policy: {self.cfg.policy_name}")
        logger.info("=" * 50)
        logger.info("")
        return policy

    def load_metrics(self) -> List[MetricBase]:
        """Load agent and task metrics."""
        return [AgentMetric(self.human_stats), TaskMetric(self.human_stats)]

    def setup_recorder(self, output_folder: str, demo_id: int, record_rgb: bool = True, record_depth: bool = True):
        """Setup the direct data recorder for the current episode."""
        task_name = self.cfg.task.name
        task_id = TASK_NAMES_TO_INDICES[task_name]
        # Extend ROBOT_CAMERA_NAMES with the external observer when enabled.
        camera_names = dict(ROBOT_CAMERA_NAMES["R1Pro"])
        if getattr(self.cfg, "external_camera_enabled", False):
            # The OmniGibson env prefixes external-sensor obs keys with
            # `external::`, so the matching key is `external::external_sensor0::rgb`.
            camera_names["external"] = "external::external_sensor0"
        self.data_recorder = DirectDataRecorder(
            output_folder=output_folder,
            task_name=task_name,
            task_id=task_id,
            demo_id=demo_id,
            camera_names=camera_names,
            record_rgb=record_rgb,
            record_depth=record_depth,
            only_successes=getattr(self.cfg, "only_successes", False),
        )

    def step(self) -> Tuple[bool, bool]:
        """
        Performs a single step of the task by executing the policy, interacting with the environment,
        processing observations, updating metrics, and tracking trial success.
        """
        self.robot_action = self.policy.forward(obs=self.obs)

        obs, _, terminated, truncated, info = self.env.step(self.robot_action, n_render_iterations=1)

        # Flatten obs for recording (this gives us keys like "robot_r1::robot_r1:zed_link:Camera:0::rgb")
        flat_obs = flatten_obs_dict(obs)

        # process obs (adds cam_rel_poses, task_id)
        self.obs = self._preprocess_obs(obs)

        # Record data if recorder is active
        if self.data_recorder is not None and self.data_recorder.is_recording:
            # Extract proprio from preprocessed obs
            proprio = self.obs.get("robot_r1::proprio", None)
            if proprio is None:
                proprio = flat_obs.get("robot_r1::proprio", None)

            cam_rel_poses = self.obs.get("robot_r1::cam_rel_poses", None)
            task_info = self.obs.get("task::low_dim", flat_obs.get("task::low_dim", None))

            # Get action
            action = self.robot_action
            if isinstance(action, dict):
                # Flatten action dict if needed
                action = np.concatenate([v.cpu().numpy() if isinstance(v, th.Tensor) else v for v in action.values()])
            elif isinstance(action, th.Tensor):
                action = action.cpu().numpy()

            # Record step with flattened observations for videos
            self.data_recorder.record_step(
                action=action,
                proprio=proprio,
                cam_rel_poses=cam_rel_poses,
                task_info=task_info,
                obs=flat_obs,
            )

        # Track BDDL state transitions
        if self.data_recorder is not None:
            try:
                self.bddl_tracker.step(self.env, self.robot, self.data_recorder.step_count)
            except Exception as e:
                logger.debug(f"BDDL tracker step error: {e}")

        diagnostic_predicate_id = self.bddl_tracker.diagnostic_stop_predicate_id
        if diagnostic_predicate_id is not None and not (terminated or truncated):
            if self._diagnostic_stop_at_step is None:
                self._diagnostic_stop_at_step = (
                    self.data_recorder.step_count + self._diagnostic_post_at_edge_steps
                )
                logger.info(
                    "Diagnostic stable predicate detected: "
                    f"{diagnostic_predicate_id}; stopping at step "
                    f"{self._diagnostic_stop_at_step}"
                )
            if self.data_recorder.step_count >= self._diagnostic_stop_at_step:
                truncated = True
                logger.info(
                    "Diagnostic rollout stopping after stable predicate: "
                    f"{diagnostic_predicate_id}"
                )

        if terminated or truncated:
            self.n_trials += 1
            if info["done"]["success"]:
                self.n_success_trials += 1

        for metric in self.metrics:
            metric.step_callback(self.env)
        return terminated, truncated, info

    @property
    def video_writer(self) -> Tuple[Container, Stream]:
        """Returns the video writer for the current evaluation step."""
        return self._video_writer

    @video_writer.setter
    def video_writer(self, video_writer: Tuple[Container, Stream]) -> None:
        if self._video_writer is not None:
            (container, stream) = self._video_writer
            for packet in stream.encode():
                container.mux(packet)
            container.close()
        self._video_writer = video_writer

    def load_task_instance(self, instance_id: int, test_hidden: bool = False) -> None:
        """Loads the configuration for a specific task instance."""
        scene_model = self.env.task.scene_name
        tro_filename = self.env.task.get_cached_activity_scene_filename(
            scene_model=scene_model,
            activity_name=self.env.task.activity_name,
            activity_definition_id=self.env.task.activity_definition_id,
            activity_instance_id=instance_id,
        )
        if test_hidden:
            tro_file_path = os.path.join(
                gm.DATA_PATH,
                "2025-challenge-test-instances",
                self.env.task.activity_name,
                f"{tro_filename}-tro_state.json",
            )
        else:
            tro_file_path = os.path.join(
                get_task_instance_path(scene_model),
                f"json/{scene_model}_task_{self.env.task.activity_name}_instances/{tro_filename}-tro_state.json",
            )
        with open(tro_file_path, "r") as f:
            tro_state = recursively_convert_to_torch(json.load(f))
        for tro_key, tro_state in tro_state.items():
            if tro_key == "robot_poses":
                presampled_robot_poses = tro_state
                robot_pos = presampled_robot_poses[self.robot.model_name][0]["position"]
                robot_quat = presampled_robot_poses[self.robot.model_name][0]["orientation"]
                self.robot.set_position_orientation(robot_pos, robot_quat)
                self.env.scene.write_task_metadata(key=tro_key, data=tro_state)
            else:
                self.env.task.object_scope[tro_key].load_state(tro_state, serialized=False)

        for _ in range(25):
            og.sim.step_physics()
            for entity in self.env.task.object_scope.values():
                if not entity.is_system and entity.exists:
                    entity.keep_still()

        self.env.scene.update_initial_file()
        self.env.scene.reset()

    def _preprocess_obs(self, obs: dict) -> dict:
        """Preprocess the observation dictionary before passing it to the policy."""
        obs = flatten_obs_dict(obs)
        base_pose = self.robot.get_position_orientation()
        cam_rel_poses = []
        for camera_name in ROBOT_CAMERA_NAMES["R1Pro"].values():
            camera = self.robot.sensors[camera_name.split("::")[1]]
            direct_cam_pose = camera.camera_parameters["cameraViewTransform"]
            if np.allclose(direct_cam_pose, np.zeros(16)):
                cam_rel_poses.append(
                    th.cat(T.relative_pose_transform(*(camera.get_position_orientation()), *base_pose))
                )
            else:
                cam_pose = T.mat2pose(th.tensor(np.linalg.inv(np.reshape(direct_cam_pose, [4, 4]).T), dtype=th.float32))
                cam_rel_poses.append(th.cat(T.relative_pose_transform(*cam_pose, *base_pose)))
        obs["robot_r1::cam_rel_poses"] = th.cat(cam_rel_poses, axis=-1)
        obs["task_id"] = th.tensor([TASK_NAMES_TO_INDICES[self.cfg.task.name]], dtype=th.int64)
        return obs

    def _write_video(self) -> None:
        """Write the current robot observations to video (for preview video)."""
        if ROBOT_CAMERA_NAMES["R1Pro"]["head"] + "::rgb" not in self.obs:
            return
        left_wrist_rgb = cv2.resize(
            self.obs[ROBOT_CAMERA_NAMES["R1Pro"]["left_wrist"] + "::rgb"].numpy(),
            (224, 224),
        )
        right_wrist_rgb = cv2.resize(
            self.obs[ROBOT_CAMERA_NAMES["R1Pro"]["right_wrist"] + "::rgb"].numpy(),
            (224, 224),
        )
        head_rgb = cv2.resize(
            self.obs[ROBOT_CAMERA_NAMES["R1Pro"]["head"] + "::rgb"].numpy(),
            (448, 448),
        )
        write_video(
            np.expand_dims(np.hstack([np.vstack([left_wrist_rgb, right_wrist_rgb]), head_rgb]), 0),
            video_writer=self.video_writer,
            batch_size=1,
            mode="rgb",
        )

    def reset(self) -> None:
        """Reset the environment, policy, and compute metrics."""
        self.obs = self._preprocess_obs(self.env.reset()[0])
        for metric in self.metrics:
            metric.start_callback(self.env)
        self.policy.reset()
        self.n_success_trials, self.n_trials = 0, 0

    def start_episode_recording(self):
        """Start recording for the current episode."""
        self._diagnostic_stop_at_step = None
        if self.data_recorder is not None:
            self.data_recorder.start_episode()
        try:
            self.bddl_tracker.start(self.env, self.robot)
        except Exception as e:
            logger.warning(f"Failed to start BDDL state tracker: {e}")

    def end_episode_recording(self, success: bool) -> bool:
        """End recording and save data if successful."""
        if self.data_recorder is not None:
            bddl_results = None
            predicate_catalog = None
            try:
                # Terminal flush: commit any pending predicate flips that didn't
                # reach 50-step stability before the sim exited. Critical for
                # success=true episodes where the goal-satisfying flip triggers
                # terminated=true only a few steps after the flip — without this
                # the goal flip is dropped from bddl_transitions.json.
                try:
                    flushed = self.bddl_tracker.flush_pending()
                    if flushed:
                        logger.info(
                            f"BDDL terminal flush: committed {len(flushed)} "
                            f"pending transitions: {flushed}"
                        )
                except Exception as flush_exc:
                    logger.warning(f"BDDL flush_pending failed: {flush_exc}")

                instance_id = (self.data_recorder.demo_id % 10000) // 10
                bddl_results = self.bddl_tracker.get_results(
                    task_name=self.cfg.task.name,
                    instance_id=instance_id,
                    demo_id=self.data_recorder.demo_id,
                    total_steps=self.data_recorder.step_count,
                    success=success,
                )
                predicate_catalog = self.bddl_tracker.get_predicate_catalog(
                    task_name=self.cfg.task.name,
                    instance_id=instance_id,
                )
            except Exception as e:
                logger.warning(f"Failed to get BDDL tracking results: {e}")
            # Pull HF-format skill annotation from the policy (if it implements
            # the hook — currently only HybridPolicy does). Best-effort; never
            # fail the episode save if the policy can't produce one.
            skill_ann = None
            try:
                if hasattr(self.policy, "build_skill_annotation"):
                    skill_ann = self.policy.build_skill_annotation(
                        int(self.data_recorder.step_count)
                    )
            except Exception as e:
                logger.warning(f"Failed to build skill annotation: {e}")
            return self.data_recorder.end_episode(
                success,
                env=self.env,
                bddl_transitions=bddl_results,
                predicate_catalog=predicate_catalog,
                skill_annotation=skill_ann,
            )
        return False

    def _finalize_episode_recording(self, instance_id: int, episode_idx: int, demo_id: int, success: bool = False) -> None:
        """Flush the current HDF5 trajectory to disk and tag it with metadata."""
        if self.hdf5_recorder is None:
            return
        prev_traj_count = self.hdf5_recorder.traj_count
        self.hdf5_recorder.flush_current_traj()
        if self.hdf5_recorder.traj_count > prev_traj_count:
            demo_idx = self.hdf5_recorder.traj_count - 1
            demo_group = self.hdf5_recorder.hdf5_file["data"][f"demo_{demo_idx}"]
            self.hdf5_recorder.add_metadata(group=demo_group, name="task_name", data=self.cfg.task.name)
            self.hdf5_recorder.add_metadata(group=demo_group, name="instance_id", data=instance_id)
            self.hdf5_recorder.add_metadata(group=demo_group, name="episode_idx", data=episode_idx)
            self.hdf5_recorder.add_metadata(group=demo_group, name="demo_id", data=demo_id)
            self.hdf5_recorder.add_metadata(group=demo_group, name="success", data=success)

    def _finalize_data_collection(self, output_folder: str = None) -> None:
        """Flush any pending HDF5 data, close the recorder, and split into per-episode files."""
        if self.hdf5_recorder is None:
            return
        if len(self.hdf5_recorder.current_traj_history) > 0:
            self.hdf5_recorder.flush_current_traj()
        # Grab filename before save_data() closes the file
        combined_path = self.hdf5_recorder.hdf5_file.filename
        self.hdf5_recorder.save_data()

        # Split the combined HDF5 into per-episode files under success/ and failure/
        if os.path.exists(combined_path) and output_folder is not None:
            self._split_hdf5_by_outcome(combined_path, output_folder)

    def _split_hdf5_by_outcome(self, combined_path: str, output_folder: str) -> None:
        """
        Split a combined HDF5 file into per-episode HDF5 files under success/ and failure/,
        matching the same naming convention as parquet and video files:
          {output_folder}/{outcome}/2025-challenge-demos/trajectories/task-{task_id:04d}/episode_{demo_id:08d}.hdf5
        """
        try:
            with h5py.File(combined_path, "r") as src:
                if "data" not in src:
                    logger.warning("No data group found in HDF5 file, skipping split")
                    return

                success_count = 0
                failure_count = 0

                for demo_name in sorted(src["data"].keys()):
                    demo_group = src["data"][demo_name]
                    # Skip the initial empty stub demo that the data-collection
                    # wrapper creates during env init: it has no `demo_id`
                    # attribute (only finalized episodes get one tagged on
                    # via _finalize_episode_recording).
                    if "demo_id" not in demo_group.attrs:
                        logger.info(
                            f"Skipping un-tagged demo group {demo_name!r} "
                            f"(no demo_id attr — initial wrapper stub)"
                        )
                        continue
                    is_success = bool(demo_group.attrs.get("success", False))
                    demo_id = int(demo_group.attrs["demo_id"])
                    task_id = demo_id // 10000

                    outcome = "success" if is_success else "failure"
                    episode_dir = os.path.join(
                        output_folder, outcome, "2025-challenge-demos", "trajectories", f"task-{task_id:04d}"
                    )
                    os.makedirs(episode_dir, exist_ok=True)
                    episode_path = os.path.join(episode_dir, f"episode_{demo_id:08d}.hdf5")

                    with h5py.File(episode_path, "w") as dst:
                        # Copy top-level attributes from source
                        for attr_name, attr_val in src.attrs.items():
                            dst.attrs[attr_name] = attr_val
                        dst.create_group("data")
                        src["data"].copy(demo_name, dst["data"], name="demo_0")
                        dst["data"].attrs["num_demos"] = 1

                    if is_success:
                        success_count += 1
                    else:
                        failure_count += 1
                    logger.info(f"Saved HDF5 episode to {episode_path}")

                logger.info(f"Split HDF5: {success_count} success, {failure_count} failure episodes")

            # All per-episode files are written; the combined staging HDF5 is
            # no longer needed. Delete it so per-job staging dirs don't
            # accumulate ~85MB of dead data across a 400-job batch.
            try:
                os.remove(combined_path)
                logger.info(f"Removed staging HDF5: {combined_path}")
            except Exception as e:
                logger.warning(f"Could not remove staging HDF5 {combined_path}: {e}")

        except Exception as e:
            logger.error(f"Error splitting HDF5 file: {e}")
            logger.info(f"Combined HDF5 file is still available at: {combined_path}")

    def __enter__(self):
        signal(SIGINT, self._sigint_handler)
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        logger.info("")
        logger.info("=" * 50)
        logger.info(f"Total success trials: {self.n_success_trials}")
        logger.info(f"Total trials: {self.n_trials}")
        if self.n_trials > 0:
            logger.info(f"Success rate: {self.n_success_trials / self.n_trials}")
        logger.info("=" * 50)
        logger.info("")
        if exc_type is not None:
            traceback.print_exception(exc_type, exc_value, exc_tb)
        self.video_writer = None
        self._finalize_data_collection(output_folder=self.output_folder)
        self.env.close()
        og.shutdown()

    def _sigint_handler(self, signal_received, frame):
        logger.warning("SIGINT or CTRL-C detected.\n")
        self.__exit__(None, None, None)
        sys.exit(0)


if __name__ == "__main__":
    register_omegaconf_resolvers()
    # open yaml from task path
    with hydra.initialize_config_dir(f"{Path(getsourcefile(lambda: 0)).parents[0]}/configs", version_base="1.1"):
        config = hydra.compose("datacollect_config", overrides=sys.argv[1:])
    OmegaConf.resolve(config)
    # Allow per-instance episode count to be set via CLI override
    # (`++num_eval_episodes=5`). The macro module locks values after first
    # access — `macros.unlocked()` re-opens it for write.
    _n_eps = int(getattr(config, "num_eval_episodes", 1))
    if _n_eps != 1:
        with macros.unlocked():
            m.NUM_EVAL_EPISODES = _n_eps
    # set headless mode
    gm.HEADLESS = config.headless
    # set video path
    if config.write_video:
        video_path = Path(config.log_path).expanduser() / "videos"
        video_path.mkdir(parents=True, exist_ok=True)

    # Set output folder for direct recording
    output_folder = getattr(config, "output_folder", None)
    if output_folder is None:
        output_folder = Path(config.log_path).expanduser() / "direct_output"
    else:
        output_folder = Path(output_folder).expanduser()
    output_folder.mkdir(parents=True, exist_ok=True)

    # Set default HDF5 recording path under output_folder/_staging/ (will be split into
    # per-episode files under success/ and failure/ at the end, matching parquet/video naming)
    task_id_for_path = TASK_NAMES_TO_INDICES[config.task.name]
    if getattr(config, "recording_path", None) is None:
        traj_dir = output_folder / "_staging" / "2025-challenge-demos" / "trajectories" / f"task-{task_id_for_path:04d}"
        traj_dir.mkdir(parents=True, exist_ok=True)
        config.recording_path = str(traj_dir / f"{config.task.name}.hdf5")
    else:
        traj_dir = Path(config.recording_path).expanduser().parent
        traj_dir.mkdir(parents=True, exist_ok=True)

    # Recording options
    record_rgb = getattr(config, "record_rgb", True)
    record_depth = getattr(config, "record_depth", True)

    assert not (
        config.eval_on_train_instances and config.test_hidden
    ), "Cannot eval on train instances and test hidden instances simultaneously."
    if config.test_hidden:
        logger.info("You are evaluating on hidden test instances! This is for internal use only.")

    # get run instances
    if config.eval_on_train_instances:
        logger.info(
            "You are evaluating on training instances, set eval_on_train_instances to False for test instances."
        )
        task_idx = TASK_NAMES_TO_INDICES[config.task.name]
        with open(os.path.join(gm.DATA_PATH, "2025-challenge-task-instances", "metadata", "episodes.jsonl"), "r") as f:
            episodes = [json.loads(line) for line in f]
        instances_to_run = []
        for episode in episodes:
            if episode["episode_index"] // 1e4 == task_idx:
                instances_to_run.append(str(int((episode["episode_index"] // 10) % 1e3)))
        if config.eval_instance_ids:
            assert set(config.eval_instance_ids).issubset(
                set(range(m.NUM_TRAIN_INSTANCES))
            ), f"eval instance ids must be in range({m.NUM_TRAIN_INSTANCES})"
            instances_to_run = [instances_to_run[i] for i in config.eval_instance_ids]
    elif config.test_hidden:
        instances_to_run = (
            config.eval_instance_ids if config.eval_instance_ids is not None else set(range(m.NUM_EVAL_INSTANCES))
        )
        assert set(instances_to_run).issubset(
            set(range(m.NUM_EVAL_INSTANCES))
        ), f"eval instance ids must be in range({m.NUM_EVAL_INSTANCES})"
    else:
        # load csv file first to determine number of available instances
        task_instance_csv_path = os.path.join(
            gm.DATA_PATH, "2025-challenge-task-instances", "metadata", "test_instances.csv"
        )
        with open(task_instance_csv_path, "r") as f:
            lines = list(csv.reader(f))[1:]
        assert (
            lines[TASK_NAMES_TO_INDICES[config.task.name]][1] == config.task.name
        ), f"Task name from config {config.task.name} does not match task name from csv"
        test_instances = lines[TASK_NAMES_TO_INDICES[config.task.name]][2].strip().split(",")
        num_available_instances = len(test_instances)

        instances_to_run = (
            config.eval_instance_ids if config.eval_instance_ids is not None else set(range(min(m.NUM_EVAL_INSTANCES, num_available_instances)))
        )
        assert set(instances_to_run).issubset(
            set(range(num_available_instances))
        ), f"eval instance ids must be in range({num_available_instances})"
        instances_to_run = [int(test_instances[i]) for i in instances_to_run]

    # establish metrics
    metrics = {}
    metrics_path = Path(config.log_path).expanduser() / "metrics"
    metrics_path.mkdir(parents=True, exist_ok=True)

    task_id = TASK_NAMES_TO_INDICES[config.task.name]

    with EvaluatorWithDirectRecording(config) as evaluator:
        evaluator.output_folder = str(output_folder)
        logger.info("Starting evaluation with direct data recording...")

        for idx in instances_to_run:
            evaluator.reset()
            evaluator.load_task_instance(idx, test_hidden=config.test_hidden)
            logger.info(f"Starting task instance {idx} for evaluation...")

            for epi in range(m.NUM_EVAL_EPISODES):
                evaluator.reset()
                done = False

                # Compute demo_id: task_id * 10000 + instance_id * 10 + episode_id
                demo_id = task_id * 10000 + int(idx) * 10 + epi

                # Setup recorder for this episode
                evaluator.setup_recorder(
                    output_folder=str(output_folder),
                    demo_id=demo_id,
                    record_rgb=record_rgb,
                    record_depth=record_depth,
                )
                evaluator.start_episode_recording()

                if config.write_video:
                    video_name = str(video_path) + f"/{config.task.name}_{idx}_{epi}.mp4"
                    evaluator.video_writer = create_video_writer(
                        fpath=video_name,
                        resolution=(448, 672),
                    )

                # run metric start callbacks
                for metric in evaluator.metrics:
                    metric.start_callback(evaluator.env)

                success = False
                while not done:
                    terminated, truncated, info = evaluator.step()
                    if terminated or truncated:
                        done = True
                        success = info["done"]["success"]
                    # Local patch (behavior1k_mp): allow the policy to signal
                    # early termination once its orchestrator has emitted all
                    # planned actions. Avoids running the env for thousands of
                    # zero/hold frames after the orchestrated trajectory ends.
                    elif (
                        getattr(config, "early_terminate_on_policy_done", True)
                        and hasattr(evaluator.policy, "is_done")
                        and evaluator.policy.is_done()
                    ):
                        truncated = True
                        done = True
                        success = info["done"]["success"]
                        logger.info(
                            f"Policy reported done at step {evaluator.env._current_step}; "
                            f"early-truncating episode (success={success})."
                        )
                    if config.write_video:
                        evaluator._write_video()
                    if evaluator.env._current_step % 1000 == 0:
                        logger.info(f"Current step: {evaluator.env._current_step}")

                # End recording (parquet + video)
                saved = evaluator.end_episode_recording(success)
                if saved:
                    logger.info(f"Data saved for demo_id={demo_id}")

                # Finalize HDF5 recording for this episode
                evaluator._finalize_episode_recording(instance_id=int(idx), episode_idx=epi, demo_id=demo_id, success=success)

                # run metric end callbacks
                for metric in evaluator.metrics:
                    metric.end_callback(evaluator.env)

                logger.info(f"Evaluation finished at step {evaluator.env._current_step}.")
                logger.info(f"Evaluation exit state: terminated={terminated}, truncated={truncated}, success={success}")
                logger.info(f"Total trials: {evaluator.n_trials}")
                logger.info(f"Total success trials: {evaluator.n_success_trials}")

                # gather metric results and write to file
                for metric in evaluator.metrics:
                    metrics.update(metric.gather_results())
                with open(metrics_path / f"{config.task.name}_{idx}_{epi}.json", "w") as f:
                    json.dump(metrics, f)

                # reset video writer
                if config.write_video:
                    evaluator.video_writer = None
                    logger.info(f"Saved preview video to {video_name}")
