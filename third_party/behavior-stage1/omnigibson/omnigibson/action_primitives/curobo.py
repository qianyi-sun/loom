# NOTE (embodiedClaw_recovery modifications, 2026-06-11) — two ADDITIVE extensions for the
# failure-recovery pipeline; behavior is unchanged for all existing callers:
#   1. compute_trajectories(orient_free=...): position-only goals via PoseCostMetric
#      reach_partial_pose with the angular reach weights zeroed (default False = old behavior).
#   2. direction_to_world_delta(): ROBOT-CENTRIC direction -> world displacement, the single
#      frame authority for the recovery move_gripper primitive.

import math
from enum import Enum
from typing import Dict, Any, Optional

import numpy as np
import torch as th  # MUST come before importing omni!!!

import omnigibson as og
import omnigibson.lazy as lazy
import omnigibson.utils.transform_utils as T
from omnigibson.macros import create_module_macros
from omnigibson.prims.rigid_dynamic_prim import RigidDynamicPrim
from omnigibson.robots.holonomic_base_robot import HolonomicBaseRobot
from omnigibson.utils.constants import JointType
from omnigibson.utils.python_utils import multi_dim_linspace


# Gives 1 - 5% better speedup, according to https://github.com/NVlabs/curobo/discussions/245#discussioncomment-9265692
th.backends.cudnn.benchmark = True
th.backends.cuda.matmul.allow_tf32 = True
th.backends.cudnn.allow_tf32 = True

# Create settings for this module
m = create_module_macros(module_path=__file__)

m.HOLONOMIC_BASE_PRISMATIC_JOINT_LIMIT = 5.0  # meters
m.HOLONOMIC_BASE_REVOLUTE_JOINT_LIMIT = math.pi * 2  # radians

m.DEFAULT_COLLISION_ACTIVATION_DISTANCE = 0.005
m.DEFAULT_ATTACHED_OBJECT_SCALE = 0.8


class CuRoboEmbodimentSelection(str, Enum):
    BASE = "base"
    ARM = "arm"
    # Right-canonical arm variant: same kinematics as ARM but with ee_link on the RIGHT
    # eef. curobo fully optimizes/gates only the canonical ee_link's pose goal through
    # trajopt (auxiliary-link goals arrive stale — see the KNOWN LIMITATION note in
    # compute_trajectories), so each arm needs its own canonical embodiment. No
    # standardized asset yaml exists for this; callers construct the config explicitly
    # (see embodiedClaw_recovery unified_backend).
    ARM_RIGHT = "arm_right"
    DEFAULT = "default"


def create_world_mesh_collision(tensor_args, obb_cache_size=10, mesh_cache_size=2048, max_distance=0.05):
    """
    Creates a CuRobo WorldMeshCollision to use for collision checking

    Args:
        tensor_args (TensorDeviceType): Tensor device information
        obb_cache_size (int): Cache size for number of oriented bounding boxes supported in the collision world
        mesh_cache_size (int): Cache size for number of meshes supported in the collision world
        max_distance (float): maximum distance when checking collisions (see curobo source code)

    Returns:
        MeshCollisionWorld: collision world used to check against for collisions
    """
    world_cfg = lazy.curobo.geom.sdf.world.WorldCollisionConfig.load_from_dict(
        dict(
            cache={"obb": obb_cache_size, "mesh": mesh_cache_size},
            n_envs=1,
            checker_type=lazy.curobo.geom.sdf.world.CollisionCheckerType.MESH,
            max_distance=max_distance,
        ),
        tensor_args=tensor_args,
    )

    # To update, run world_coll_checker.load_collision_model(obstacles)
    return lazy.curobo.geom.sdf.utils.create_collision_checker(world_cfg)



def torso3_usd_sign_fix_needed():
    """Whether curobo's UsdKinematicsParser mis-signs R1Pro ``torso_joint3`` on THIS stack.

    The R1Pro USD encodes torso_joint3 as axis "Y" inside a body frame flipped 180deg
    about X; on the Isaac-5.1 / OmniGibson-3.8.x assets curobo's parser collapses that
    flip to identity and applies +Y (opposite of the sim/URDF -Y), so the sign must be
    negated via joint_offset. On Isaac-4.5 / OmniGibson-3.7.x the parse is CORRECT and
    negating would break every torso-coupled plan.

    Deliberate version ALLOWLIST: og.__version__ is pinned 1:1 to the Isaac runtime
    (3.7.x = Isaac 4.5, 3.8.x = Isaac 5.1). An UNKNOWN version RAISES — verify the new
    stack by FK-comparing curobo vs sim for torso_joint3 and extend this gate; never
    let a new tree silently inherit either branch. (A build-time FK probe is unsafe
    here: set_joint_positions on a seeded state can displace welded held objects.)
    """
    ver = getattr(og, "__version__", None)
    if ver is None:
        raise RuntimeError("omnigibson.__version__ missing: cannot decide the torso_joint3 sign fix")
    if ver.startswith("3.8"):
        return True   # Isaac 5.1 assets: parser bug present
    if ver.startswith("3.7"):
        return False  # Isaac 4.5 assets: parser correct — do NOT negate
    raise RuntimeError(
        f"OmniGibson {ver}: torso_joint3 sign-fix behavior unverified for this version. "
        "FK-compare curobo vs sim for torso_joint3 on this stack, then extend "
        "torso3_usd_sign_fix_needed()."
    )


class CuRoboMotionGenerator:
    """
    Class for motion generator using CuRobo backend
    """

    def __init__(
        self,
        robot,
        robot_cfg_path=None,
        robot_usd_path=None,
        device="cuda:0",
        motion_cfg_kwargs=None,
        batch_size=2,
        use_cuda_graph=True,
        debug=False,
        use_default_embodiment_only=False,
        collision_activation_distance=m.DEFAULT_COLLISION_ACTIVATION_DISTANCE,
        joint_limit_overrides=None,
    ):
        """
        Args:
            robot (BaseRobot): Robot for which to generate motion plans
            robot_cfg_path (None or str): If specified, the path to the robot configuration to use. If None, will
                try to use a pre-configured one directly from curobo based on the robot class of @robot
            robot_usd_path (None or str): If specified, the path to the robot USD file to use. If None, will
                try to use a pre-configured one directly from curobo based on the robot class of @robot
            device (str): Which device to use for curobo
            motion_cfg_kwargs (None or dict): If specified, keyward arguments to pass to
                MotionGenConfig.load_from_robot_config(...)
            batch_size (int): Size of batches for computing trajectories. This must be FIXED
            use_cuda_graph (bool): Whether to use CUDA graph for motion generation or not
            debug (bool): Whether to debug generation or not, setting this True will set use_cuda_graph to False implicitly
            use_default_embodiment_only (bool): Whether to use only the default embodiment for the robot or not
            collision_activation_distance (float): Distance threshold at which a collision with the world is detected.
                Increasing this value will make the motion planner more conservative in its planning with respect
                to the underlying sphere representation of the robot. Note that this does not affect self-collisions detection.
            joint_limit_overrides (None or dict): If specified, {joint_name: (lower, upper)} position-limit
                overrides applied to every embodiment's robot config BEFORE the solvers are built (post-hoc
                mutation would not propagate into the solvers' captured bound tensors). NOTE
                (embodiedClaw_recovery modification, 2026-06-12): used to clamp the torso joints around their
                seeded posture so recovery plans keep the trunk still unless the margin allows the motion.
        """
        # Only support one scene for now -- verify that this is the case
        assert len(og.sim.scenes) == 1

        # Store internal variables
        self._tensor_args = lazy.curobo.types.base.TensorDeviceType(device=th.device(device))
        self.debug = debug
        self.robot = robot
        self.robot_joint_names = list(robot.joints.keys())
        self.batch_size = batch_size

        # Load robot config and usd paths and make sure paths point correctly
        robot_cfg_path_dict = robot.curobo_path if robot_cfg_path is None else robot_cfg_path
        if not isinstance(robot_cfg_path_dict, dict):
            robot_cfg_path_dict = {CuRoboEmbodimentSelection.DEFAULT: robot_cfg_path_dict}
        if use_default_embodiment_only:
            robot_cfg_path_dict = {
                CuRoboEmbodimentSelection.DEFAULT: robot_cfg_path_dict[CuRoboEmbodimentSelection.DEFAULT]
            }
        robot_usd_path = robot.usd_path if robot_usd_path is None else robot_usd_path

        # This will be shared across all MotionGen instances
        world_coll_checker = create_world_mesh_collision(
            self._tensor_args, obb_cache_size=10, mesh_cache_size=2048, max_distance=0.05
        )

        usd_help = lazy.curobo.util.usd_helper.UsdHelper()
        usd_help.stage = og.sim.stage
        self.usd_help = usd_help

        self.mg = dict()
        self.ee_link = dict()
        self.additional_links = dict()
        self.base_link = dict()

        # NOTE (traj_guidance, 2026-07-06): demo-trajectory guidance state. Guided solves run on
        # a DEDICATED lazily-built MotionGen with use_cuda_graph=False (a python cost hook patched
        # onto a graph-captured solver would never execute on replay); it shares the world
        # collision checker so obstacle updates apply to both. See
        # learning/embodiedClaw_recovery/TRAJ_GUIDANCE_APPROACH.md.
        self._world_coll_checker = None
        self._guided_build_src = dict()  # emb_sel -> (robot_cfg_obj, motion_kwargs)
        self._guided_mg = dict()  # emb_sel -> MotionGen
        self._guidance_ref = None  # (batch, horizon, dof_active) tracking reference, on device
        self._guidance_seed = None  # same tensor, used to overwrite trajopt seed block 0
        self._guidance_weight = 0.0

        # Grab mapping from robot joint name to index
        reset_qpos = self.robot.reset_joint_pos
        joint_idx_mapping = {joint.joint_name: i for i, joint in enumerate(self.robot.joints.values())}
        for emb_sel, robot_cfg_path in robot_cfg_path_dict.items():
            content_path = lazy.curobo.types.file_path.ContentPath(
                robot_config_absolute_path=robot_cfg_path, robot_usd_absolute_path=robot_usd_path
            )
            robot_cfg_dict = lazy.curobo.cuda_robot_model.util.load_robot_yaml(content_path)["robot_cfg"]
            robot_cfg_dict["kinematics"]["use_usd_kinematics"] = True

            # Automatically populate the locked joints and retract config from the robot values
            for joint_name, lock_val in robot_cfg_dict["kinematics"]["lock_joints"].items():
                if lock_val is None:
                    joint_idx = joint_idx_mapping[joint_name]
                    robot_cfg_dict["kinematics"]["lock_joints"][joint_name] = reset_qpos[joint_idx]
            if robot_cfg_dict["kinematics"]["cspace"]["retract_config"] is None:
                robot_cfg_dict["kinematics"]["cspace"]["retract_config"] = [
                    reset_qpos[joint_idx_mapping[joint_name]]
                    for joint_name in robot_cfg_dict["kinematics"]["cspace"]["joint_names"]
                ]

            self.ee_link[emb_sel] = robot_cfg_dict["kinematics"]["ee_link"]
            # RobotConfig.from_dict will append ee_link to link_names, so we make a copy here.
            self.additional_links[emb_sel] = robot_cfg_dict["kinematics"]["link_names"].copy()
            self.base_link[emb_sel] = robot_cfg_dict["kinematics"]["base_link"]

            robot_cfg_obj = lazy.curobo.types.robot.RobotConfig.from_dict(robot_cfg_dict, self._tensor_args)

            if isinstance(robot, HolonomicBaseRobot):
                self.update_joint_limits(robot_cfg_obj, emb_sel)

            # NOTE (embodiedClaw_recovery modification, 2026-06-12): caller-specified
            # per-joint position-limit overrides (see __init__ docstring).
            if joint_limit_overrides:
                joint_limits = robot_cfg_obj.kinematics.kinematics_config.joint_limits
                for joint_name, (lo, hi) in joint_limit_overrides.items():
                    if joint_name in joint_limits.joint_names:
                        joint_idx = joint_limits.joint_names.index(joint_name)
                        joint_limits.position[0][joint_idx] = lo
                        joint_limits.position[1][joint_idx] = hi

            motion_kwargs = dict(
                trajopt_tsteps=32,
                collision_checker_type=lazy.curobo.geom.sdf.world.CollisionCheckerType.MESH,
                use_cuda_graph=use_cuda_graph,
                num_ik_seeds=128,
                num_batch_ik_seeds=128,
                num_batch_trajopt_seeds=1,
                num_trajopt_noisy_seeds=1,
                ik_opt_iters=100,
                optimize_dt=True,
                num_trajopt_seeds=4,
                # num_graph_seeds=1: curobo's BATCH path only supports the graph planner with
                # num_graph_seeds==1 (else it warns and skips the graph entirely). Needed so the
                # graph/PRM planner (global reconfiguration, enabled per-solve via
                # enable_graph_attempt) is actually usable for orientation-focused reorients.
                num_graph_seeds=1,
                interpolation_dt=og.sim.get_sim_step_dt(),
                collision_activation_distance=collision_activation_distance,
                self_collision_check=True,
                maximum_trajectory_dt=None,
                fixed_iters_trajopt=True,
                finetune_trajopt_iters=100,
                finetune_dt_scale=1.05,
            )
            if motion_cfg_kwargs is not None:
                motion_kwargs.update(motion_cfg_kwargs)

            motion_gen_config = lazy.curobo.wrap.reacher.motion_gen.MotionGenConfig.load_from_robot_config(
                robot_cfg=robot_cfg_obj,
                world_model=None,
                world_coll_checker=world_coll_checker,
                tensor_args=self._tensor_args,
                store_trajopt_debug=self.debug,
                **motion_kwargs,
            )
            self.mg[emb_sel] = lazy.curobo.wrap.reacher.motion_gen.MotionGen(motion_gen_config)
            # traj_guidance: keep the build sources so the guided (no-cuda-graph) twin can be
            # built lazily with identical kinematics/limits/costs.
            self._world_coll_checker = world_coll_checker
            self._guided_build_src[emb_sel] = (robot_cfg_obj, dict(motion_kwargs))

        # R1Pro USD torso_joint3 SIGN FIX (og51 fork, re-applied after the 2026-06 agentic-fork
        # swap dropped it; the mop wrapper mirrors this for its rebuilt generators). ISAAC-5.1-ONLY:
        # see torso3_usd_sign_fix_needed() — on 4.5 the parse is correct and this must not run.
        _torso3_fix = torso3_usd_sign_fix_needed()
        for mg in self.mg.values():
            # Both arms hang off torso_link4 (downstream of torso3), so the wrong sign lifts the
            # whole upper body ~0.70 m in Z -> curobo collision-checks the robot at the wrong
            # height -> false INVALID_START_STATE_WORLD_COLLISION on valid reaches. usd_flip_joints
            # can't express a same-axis revolute sign flip, so negate the joint via joint_offset
            # (the mechanism curobo's URDF parser uses for a "-Y" axis).
            _kc = mg.kinematics.kinematics_config
            _act = list(mg.kinematics.joint_names)
            if _torso3_fix and "torso_joint3" in _act and not getattr(mg, "_torso3_sign_fixed", False):
                _t3 = _act.index("torso_joint3")
                _jm = _kc.joint_map.detach().cpu().tolist()
                _off = _kc.joint_offset_map.view(-1, 2).clone()
                _rows = [_i for _i, _a in enumerate(_jm) if int(_a) == _t3]
                assert _rows, "torso_joint3 not found in joint_offset_map"
                for _r in _rows:
                    _off[_r, 0] = -_off[_r, 0]
                _kc.joint_offset_map.copy_(_off.view(-1).contiguous())
                mg._torso3_sign_fixed = True
            # enable_graph=True warms the GRAPH/PRM planner too — it runs graph_planner.warmup with
            # the main rollout's retract_config (already on this generator's device), which migrates
            # the graph planner's buffers onto the curobo device. Without this the graph planner's
            # tensors (e.g. retract_config in forward_bound_pos_warp) stay on the default device and a
            # GPU-split solve (curobo on cuda:1, sim on cuda:0) dies with a device-mismatch the moment
            # the graph planner runs. A non-zero warmup_joint_delta is required so the graph warmup
            # actually exercises (start != goal).
            #
            # BUT (2026-06-25): tie this to use_cuda_graph. In EAGER mode (use_cuda_graph=False, the
            # single-GPU recovery config) the GPU-split device-migration reason does NOT apply, and
            # warming the graph/PRM planner here captures CUDA graphs that make MoP's RRT-Connect
            # variable-batch check_collisions sweep segfault. MoP's own RRT curobo builds EAGER with
            # enable_graph=False and never crashes — match it.
            # CORRECTION (2026-07-22, merge reconciliation): use_cuda_graph is NOT a proxy for
            # split-GPU — the validated recovery layout is curobo on cuda:1 WITH graphs off
            # (MOP_CUROBO_USE_CUDA_GRAPH=0). Without the graph warmup the graph planner's rollout
            # buffers (retract_config in bound_cost) stay on cuda:0 and every reorient/js solve
            # dies with a device mismatch (live-hit, 320 replay on 5.1). Gate on the ACTUAL device
            # split so both concerns hold: single-GPU eager stays graph-free (RRT segfault
            # avoidance), split-GPU always migrates.
            _dev = self._tensor_args.device
            _split_gpu = _dev.type == "cuda" and (_dev.index or 0) != 0
            mg.warmup(enable_graph=(use_cuda_graph or _split_gpu), warmup_js_trajopt=False,
                      batch=batch_size, warmup_joint_delta=0.1)

            # Make sure all cuda graphs have been warmed up
            for solver in [mg.ik_solver, mg.trajopt_solver, mg.finetune_trajopt_solver]:
                if solver.solver.use_cuda_graph_metrics:
                    assert solver.solver.safety_rollout._metrics_cuda_graph_init
                    if isinstance(solver, lazy.curobo.wrap.reacher.trajopt.TrajOptSolver):
                        assert solver.interpolate_rollout._metrics_cuda_graph_init
                for opt in solver.solver.optimizers:
                    if opt.use_cuda_graph:
                        assert opt.cu_opt_init

    def update_joint_limits(self, robot_cfg_obj, emb_sel):
        joint_limits = robot_cfg_obj.kinematics.kinematics_config.joint_limits
        for joint_name in self.robot.base_joint_names:
            if joint_name in joint_limits.joint_names:
                joint_idx = joint_limits.joint_names.index(joint_name)
                # Manually specify joint limits for the base_footprint_x/y/rz
                if self.robot.joints[joint_name].joint_type == JointType.JOINT_PRISMATIC:
                    joint_limits.position[0][joint_idx] = -m.HOLONOMIC_BASE_PRISMATIC_JOINT_LIMIT
                else:
                    # Needs to be -2pi to 2pi, instead of -pi to pi, otherwise the planning success rate is much lower
                    joint_limits.position[0][joint_idx] = -m.HOLONOMIC_BASE_REVOLUTE_JOINT_LIMIT

                joint_limits.position[1][joint_idx] = -joint_limits.position[0][joint_idx]

    def save_visualization(self, q, file_path, emb_sel=CuRoboEmbodimentSelection.DEFAULT):
        # Update obstacles
        self.update_obstacles()

        # Get robot collision spheres
        cu_js = lazy.curobo.types.state.JointState(
            position=self.tensor_args.to_device(q),
            joint_names=self.robot_joint_names,
        ).get_ordered_joint_state(self.mg[emb_sel].kinematics.joint_names)
        sph = self.mg[emb_sel].kinematics.get_robot_as_spheres(cu_js.position)
        robot_world = lazy.curobo.geom.types.WorldConfig(sphere=sph[0])

        # Combine all obstacles into a single mesh
        mesh_world = self.mg[emb_sel].world_model.get_mesh_world(merge_meshes=True)
        robot_world.add_obstacle(mesh_world.mesh[0])
        robot_world.save_world_as_mesh(file_path)

    def direction_to_world_delta(self, direction, distance):
        """ROBOT-CENTRIC direction -> world-frame displacement tensor (3,).

        front = where the robot base faces, back = behind it, left/right = the robot's own
        left/right (base yaw only; up/down stay world-vertical). Lives HERE so every motion
        primitive shares one frame authority instead of rolling its own conversion.

        Deliberately NOT routed through is_local planning: curobo's local frame is
        @self.base_link (for holonomic robots the root "base_footprint_x", the head of the
        virtual-joint chain), which does NOT carry the base yaw — the semantic robot frame
        is the physical base, robot.get_position_orientation().
        """
        axes = {
            "up": (0.0, 0.0, 1.0), "down": (0.0, 0.0, -1.0),
            "left": (0.0, 1.0, 0.0), "right": (0.0, -1.0, 0.0),
            "front": (1.0, 0.0, 0.0), "back": (-1.0, 0.0, 0.0),
        }
        d = th.tensor(axes[direction], dtype=th.float32) * float(distance)
        rot = T.quat2mat(self.robot.get_position_orientation()[1])
        yaw = th.atan2(rot[1, 0], rot[0, 0])
        c, s = th.cos(yaw), th.sin(yaw)
        return th.stack([c * d[0] - s * d[1], s * d[0] + c * d[1], d[2]])

    # Scene structural elements whose meshes/AABBs enclose the robot at runtime (the floor it
    # stands on, walls/ceilings/roof around it). For arm-only planning (locked base) these are
    # benign, always-present contacts that otherwise make the graph start gate reject every plan
    # with INVALID_START_STATE_WORLD_COLLISION. A working cuRobo-on-BEHAVIOR recovery planner skips
    # exactly these by name prefix.
    STRUCTURAL_NAME_PREFIXES = ("floors_", "ceilings_", "roof_", "walls_")

    def update_obstacles(self, ignore_objects=None, include_structural=True):
        """
        Updates internal world collision cache representation based on sim state

        Args:
            ignore_objects (None or list of DatasetObject): If specified, objects that should
                be ignored when updating obstacles
            include_structural (bool): If False, the ground plane (og.sim.floor_plane) AND the
                scene's structural elements (``floors_*`` / ``ceilings_*`` / ``roof_*`` / ``walls_*``)
                are NOT added as collision obstacles. The mobile base is ALWAYS in contact with its
                support, and structural meshes enclose the robot, so for arm-only planning (locked
                base) they are benign always-present contacts that otherwise make the graph start
                gate reject every plan. Default True keeps non-recovery callers unchanged.
        """
        obstacles = {"cuboid": None, "sphere": None, "mesh": [], "cylinder": None, "capsule": None}
        robot_transform = T.pose_inv(T.pose2mat(self.robot.root_link.get_position_orientation()))

        if include_structural and og.sim.floor_plane is not None:
            prim = og.sim.floor_plane.prim.GetChildren()[0]
            m = lazy.curobo.util.usd_helper.get_mesh_attrs(
                prim, cache=self.usd_help._xform_cache, transform=robot_transform.numpy()
            )
            obstacles["mesh"].append(m)

        for obj in self.robot.scene.objects:
            if obj == self.robot:
                continue
            if obj.visual_only:
                continue
            if not include_structural and obj.name.startswith(self.STRUCTURAL_NAME_PREFIXES):
                continue
            if ignore_objects is not None and obj in ignore_objects:
                continue
            for link in obj.links.values():
                for collision_mesh in link.collision_meshes.values():
                    assert (
                        collision_mesh.geom_type == "Mesh"
                    ), f"collision_mesh {collision_mesh.prim_path} is not a mesh, but a {collision_mesh.geom_type}"
                    obj_pose = T.pose2mat(collision_mesh.get_position_orientation())
                    pose = robot_transform @ obj_pose
                    pos, orn = T.mat2pose(pose)
                    # xyzw -> wxyz
                    orn = orn[[3, 0, 1, 2]]
                    m = lazy.curobo.geom.types.Mesh(
                        name=collision_mesh.prim_path,
                        pose=th.cat([pos, orn]).tolist(),
                        vertices=collision_mesh.points.numpy(),
                        faces=collision_mesh.faces.numpy(),
                        scale=collision_mesh.get_world_scale().numpy(),
                    )
                    obstacles["mesh"].append(m)

        world = lazy.curobo.geom.types.WorldConfig(**obstacles)
        world = world.get_collision_check_world()
        self.mg[CuRoboEmbodimentSelection.DEFAULT].update_world(world)

    def check_collisions(
        self,
        q,
        initial_joint_pos=None,
        self_collision_check=True,
        skip_obstacle_update=False,
        attached_obj=None,
        attached_obj_scale=None,
        emb_sel=None,
    ):
        """
        Checks collisions between the sphere representation of the robot and the rest of the current scene

        Args:
            q (th.tensor): (N, D)-shaped tensor, representing N-total different joint configurations to check
                collisions against the world
            initial_joint_pos (None or th.tensor): If specified, the initial joint positions to set the locked joints.
                Default is the current joint positions of the robot
            self_collision_check (bool): Whether to check self-collisions or not
            skip_obstacle_update (bool): Whether to skip updating the obstacles in the world collision checker
            attached_obj (None or Dict[str, BaseObject]): If specified, a dictionary where the keys are the end-effector
                link names and the values are the corresponding BaseObject instances to attach to that link
            attached_obj_scale (None or Dict[str, float]): If specified, a dictionary where the keys are the end-effector
                link names and the values are the corresponding scale to apply to the attached object

        Returns:
            th.tensor: (N,)-shaped tensor, where each value is True if in collision, else False
        """
        # check_collisions defaults to the default embodiment (all joints actuated); callers may
        # pass emb_sel to query a specific embodiment's sphere set (e.g. ARM, to match the solver
        # the recovery actually plans with — the default embodiment carries base/foot spheres that
        # always sit in the floor margin and confound a start-validity read).
        if emb_sel is None:
            emb_sel = CuRoboEmbodimentSelection.DEFAULT

        # Update obstacles
        if not skip_obstacle_update:
            self.update_obstacles()

        q_pos = self.robot.get_joint_positions() if initial_joint_pos is None else initial_joint_pos
        q_pos = q_pos.unsqueeze(0)
        cu_joint_state = lazy.curobo.types.state.JointState(
            position=self._tensor_args.to_device(q_pos),
            joint_names=self.robot_joint_names,
        )

        # Update the locked joints with the current joint positions
        self.update_locked_joints(cu_joint_state, emb_sel)

        # Compute kinematics to get corresponding sphere representation
        cu_js = lazy.curobo.types.state.JointState(
            position=self.tensor_args.to_device(q),
            joint_names=self.robot_joint_names,
        ).get_ordered_joint_state(self.mg[emb_sel].kinematics.joint_names)

        # Attach objects if specified
        attached_info = self._attach_objects_to_robot(
            attached_obj=attached_obj,
            attached_obj_scale=attached_obj_scale,
            cu_js_batch=cu_js,
            emb_sel=emb_sel,
        )

        robot_spheres = self.mg[emb_sel].compute_kinematics(cu_js).robot_spheres
        # (N_samples, n_spheres, 4) --> (N_samples, 1, n_spheres, 4)
        robot_spheres = robot_spheres.unsqueeze(dim=1)

        with th.no_grad():
            collision_dist = (
                self.mg[emb_sel].rollout_fn.primitive_collision_constraint.forward(robot_spheres).squeeze(1)
            )
            collision_results = collision_dist > 0.0
            if self_collision_check:
                self_collision_dist = (
                    self.mg[emb_sel].rollout_fn.robot_self_collision_constraint.forward(robot_spheres).squeeze(1)
                )
                self_collision_results = self_collision_dist > 0.0
                collision_results = collision_results | self_collision_results

        # Detach objects before returning
        self._detach_objects_from_robot(attached_info, emb_sel)

        # Return results
        return collision_results  # shape (B,)

    def check_start_validity(self, q, initial_joint_pos=None, attached_obj=None,
                             attached_obj_scale=None, skip_obstacle_update=False, emb_sel=None):
        """CuRobo's OWN start-state validity check (MotionGen.check_start_state) for @emb_sel.

        This is the exact gate plan_single uses, so it is RELIABLE for any embodiment — unlike
        check_collisions, whose manual sphere query misreads non-default embodiments. Sets up the
        locked joints + attaches @attached_obj exactly as a solve would, then returns
        (valid: bool, status) where status is an INVALID_START_STATE_* enum when invalid.

        Args mirror check_collisions; @q is a (1, D) joint config.
        """
        if emb_sel is None:
            emb_sel = CuRoboEmbodimentSelection.DEFAULT
        if not skip_obstacle_update:
            self.update_obstacles()
        q_pos = self.robot.get_joint_positions() if initial_joint_pos is None else initial_joint_pos
        cu_joint_state = lazy.curobo.types.state.JointState(
            position=self._tensor_args.to_device(q_pos.unsqueeze(0)),
            joint_names=self.robot_joint_names,
        )
        self.update_locked_joints(cu_joint_state, emb_sel)
        cu_js = lazy.curobo.types.state.JointState(
            position=self.tensor_args.to_device(q),
            joint_names=self.robot_joint_names,
        ).get_ordered_joint_state(self.mg[emb_sel].kinematics.joint_names)
        attached_info = self._attach_objects_to_robot(
            attached_obj=attached_obj, attached_obj_scale=attached_obj_scale,
            cu_js_batch=cu_js, emb_sel=emb_sel,
        )
        # MotionGen.check_start_state has a side effect: to classify the failure it disables both
        # collision constraints and only re-enables robot_self_collision_constraint when the start
        # is invalid for a JOINT-LIMIT reason. On its WORLD_COLLISION return path it leaves self-
        # collision checking globally DISABLED, silently blinding every later consumer (e.g.
        # find_collision_free_start_path, which then reads self_score=0 on a self-colliding start).
        # Snapshot and restore both constraints' enabled flags so this probe is side-effect-free.
        rollout = self.mg[emb_sel].rollout_fn
        prim_on = rollout.primitive_collision_constraint.enabled
        self_on = rollout.robot_self_collision_constraint.enabled
        try:
            valid, status = self.mg[emb_sel].check_start_state(cu_js)
        finally:
            self._detach_objects_from_robot(attached_info, emb_sel)
            (rollout.primitive_collision_constraint.enable_cost if prim_on
             else rollout.primitive_collision_constraint.disable_cost)()
            (rollout.robot_self_collision_constraint.enable_cost if self_on
             else rollout.robot_self_collision_constraint.disable_cost)()
        return bool(valid), status

    def find_collision_free_start_path(
        self,
        movable_joint_names,
        initial_joint_pos=None,
        attached_obj=None,
        attached_obj_scale=None,
        max_joint_delta=0.35,
        num_samples=512,
        interpolation_steps=40,
        skip_obstacle_update=False,
        emb_sel=None,
    ):
        """Find a short, validated exit path from an invalid (world- or self-colliding) start.

        The normal graph planner rejects an invalid start before searching. This helper keeps the
        complete world and every payload attached, samples only @movable_joint_names around the
        live configuration, and returns the smallest local displacement whose dense interpolation
        ends free of both world and self collision, never exceeds the start's world-collision cost,
        and whose self-collision penetration only ever decreases (intersecting links separate, are
        never driven deeper or through each other). It returns None when the start is already valid
        or no conservative exit can be found.

        This is deliberately geometry-driven: it does not know object identities, scene layout,
        directions, or reset poses. The caller must still revalidate the physical state after
        executing the returned open-loop path before invoking the normal planner.
        """
        if emb_sel is None:
            emb_sel = CuRoboEmbodimentSelection.DEFAULT
        if not skip_obstacle_update:
            self.update_obstacles()

        q_full = self.robot.get_joint_positions() if initial_joint_pos is None else initial_joint_pos
        q_full = q_full.clone()
        cu_full = lazy.curobo.types.state.JointState(
            position=self._tensor_args.to_device(q_full.unsqueeze(0)),
            joint_names=self.robot_joint_names,
        )
        self.update_locked_joints(cu_full, emb_sel)

        kin = self.mg[emb_sel].kinematics
        kin_names = list(kin.joint_names)
        movable = [kin_names.index(name) for name in movable_joint_names if name in kin_names]
        if not movable:
            print("[curobo] start-contact search skipped: no requested joints in embodiment", flush=True)
            return None

        cu_start = cu_full.get_ordered_joint_state(kin_names)
        attached_info = self._attach_objects_to_robot(
            attached_obj=attached_obj,
            attached_obj_scale=attached_obj_scale,
            cu_js_batch=cu_start,
            emb_sel=emb_sel,
        )

        rollout = self.mg[emb_sel].rollout_fn
        # This routine's world/self scores are meaningless if either collision constraint is off.
        # A prior MotionGen.check_start_state on a world-colliding start can leave them disabled
        # (its failure-category probe only re-enables self-collision for a joint-limit failure), so
        # force both on. self_collision_check=True with total_spheres>0 makes "enabled" the correct
        # resting state, so we deliberately leave them on afterward rather than restore corruption.
        rollout.primitive_collision_constraint.enable_cost()
        rollout.robot_self_collision_constraint.enable_cost()

        def _collision_metrics(q_batch, include_cost=False):
            js = lazy.curobo.types.state.JointState(position=q_batch, joint_names=kin_names)
            spheres = self.mg[emb_sel].compute_kinematics(js).robot_spheres.unsqueeze(1)
            with th.no_grad():
                world = rollout.primitive_collision_constraint.forward(spheres)
                self_collision = rollout.robot_self_collision_constraint.forward(spheres)
                world = world.reshape(q_batch.shape[0], -1).sum(dim=-1)
                self_collision = self_collision.reshape(q_batch.shape[0], -1).sum(dim=-1)
                if not include_cost:
                    return world, self_collision, None
                # Some MotionGen rollouts configure only a collision constraint, without the
                # optional optimization cost object. The constraint is still CuRobo's canonical
                # start-validity metric and is a conservative score fallback for exit validation.
                collision_cost = getattr(rollout, "primitive_collision_cost", None)
                world_cost = world if collision_cost is None else collision_cost.forward(spheres)
                world_cost = world_cost.reshape(q_batch.shape[0], -1).sum(dim=-1)
                return world, self_collision, world_cost

        try:
            start = cu_start.position[0]
            start_world, start_self, start_cost = _collision_metrics(start.unsqueeze(0), include_cost=True)
            print(f"[curobo] start-contact search: movable_joints={len(movable)} "
                  f"world_score={float(start_world[0]):.6g} "
                  f"self_score={float(start_self[0]):.6g}", flush=True)
            if float(start_world[0]) <= 0.0 and float(start_self[0]) <= 0.0:
                print("[curobo] start-contact search skipped: start already valid", flush=True)
                return None

            limits = kin.get_joint_limits().position
            lower, upper = limits[0], limits[1]
            n_move = len(movable)
            movable_idx = th.as_tensor(movable, device=start.device, dtype=th.long)

            # Axis probes make narrow one-joint exits easy to find. Sobol samples cover coupled
            # exits deterministically and more evenly than process-global pseudorandom sampling.
            scales = [max_joint_delta * f for f in (0.125, 0.25, 0.5, 1.0)]
            axis_offsets = []
            for scale in scales:
                eye = th.eye(n_move, device=start.device, dtype=start.dtype) * float(scale)
                axis_offsets.extend([eye, -eye])
            axis_offsets = th.cat(axis_offsets, dim=0)

            per_scale = max(1, int(math.ceil(num_samples / len(scales))))
            sobol = th.quasirandom.SobolEngine(dimension=n_move, scramble=False)
            unit = sobol.draw(per_scale * len(scales)).to(device=start.device, dtype=start.dtype)
            unit = unit * 2.0 - 1.0
            sampled_offsets = []
            for i, scale in enumerate(scales):
                sampled_offsets.append(unit[i * per_scale:(i + 1) * per_scale] * float(scale))
            offsets = th.cat([axis_offsets, *sampled_offsets], dim=0)

            candidates = start.unsqueeze(0).repeat(offsets.shape[0], 1)
            candidates[:, movable_idx] += offsets
            candidates = th.maximum(candidates, lower.unsqueeze(0) + 1e-4)
            candidates = th.minimum(candidates, upper.unsqueeze(0) - 1e-4)

            world, self_collision, _ = _collision_metrics(candidates)
            feasible = (world <= 0.0) & (self_collision <= 0.0)
            feasible_idx = th.where(feasible)[0]
            if feasible_idx.numel() == 0:
                print(f"[curobo] start-contact search found no valid endpoint among "
                      f"{len(candidates)} samples", flush=True)
                return None
            print(f"[curobo] start-contact search found {int(feasible_idx.numel())}/"
                  f"{len(candidates)} valid endpoints; validating interpolations", flush=True)

            displacement = candidates[feasible_idx] - start.unsqueeze(0)
            rank = th.sum(displacement[:, movable_idx] ** 2, dim=-1)
            ordered = feasible_idx[th.argsort(rank)[:64]]
            alpha = th.linspace(
                0.0, 1.0, interpolation_steps + 1, device=start.device, dtype=start.dtype
            )
            paths = start.view(1, 1, -1) + alpha.view(1, -1, 1) * (
                candidates[ordered].unsqueeze(1) - start.view(1, 1, -1)
            )
            flat = paths.reshape(-1, start.shape[0])
            path_world, path_self, path_cost = _collision_metrics(flat, include_cost=True)
            path_world = path_world.view(paths.shape[0], paths.shape[1])
            path_self = path_self.view(paths.shape[0], paths.shape[1])
            path_cost = path_cost.view(paths.shape[0], paths.shape[1])

            start_world_value = float(start_world[0])
            start_self_value = float(start_self[0])
            start_cost_value = float(start_cost[0])
            for i in range(paths.shape[0]):
                if float(th.max(path_world[i])) > start_world_value + 1e-6:
                    continue
                if float(th.max(path_cost[i])) > start_cost_value + 1e-5:
                    continue
                # Self-collision penetration may not exceed the start's anywhere along the path:
                # separating intersecting robot links only lowers it, whereas driving them deeper
                # or through each other spikes it past the start (the failure mode that makes
                # blindly interpolating out of a self-colliding start unsafe). A self-free start
                # bounds this at 0, recovering the original "never enters self-collision" rule.
                if float(th.max(path_self[i])) > start_self_value + 1e-6:
                    continue
                # Once the interpolation has left collision (world AND self), it may not re-enter.
                colliding = (path_world[i] > 0.0) | (path_self[i] > 0.0)
                first_valid = th.where(~colliding)[0]
                if first_valid.numel() == 0 or bool(th.any(colliding[int(first_valid[0]):])):
                    continue
                # Do not return a point sitting numerically on the contact boundary. Require the
                # final quarter of the interpolation to remain valid, leaving joint-space margin
                # for controller tracking error and small simulator/collision-model differences.
                if int(first_valid[0]) > int(0.75 * interpolation_steps):
                    continue

                selected = paths[i]
                full_path = q_full.unsqueeze(0).repeat(selected.shape[0], 1)
                full_index = {name: idx for idx, name in enumerate(self.robot_joint_names)}
                for joint_idx, name in enumerate(kin_names):
                    if name in full_index:
                        full_path[:, full_index[name]] = selected[:, joint_idx].to(full_path.device)
                print(f"[curobo] start-contact search accepted endpoint rank {i + 1}/"
                      f"{paths.shape[0]}", flush=True)
                return full_path
            print("[curobo] start-contact search rejected all valid endpoint interpolations", flush=True)
            return None
        finally:
            self._detach_objects_from_robot(attached_info, emb_sel)

    def update_locked_joints(self, cu_joint_state, emb_sel):
        """
        Updates the locked joints and fixed transforms for the given embodiment selection
        This is needed to update curobo robot model about the current joint positions from Isaac.

        Args:
            cu_joint_state (JointState): JointState object representing the current joint positions
            emb_sel (CuRoboEmbodimentSelection): Which embodiment selection to use for updating locked joints
        """
        kc = self.mg[emb_sel].kinematics.kinematics_config
        # Update the lock joint state position
        kc.lock_jointstate.position = cu_joint_state.get_ordered_joint_state(kc.lock_jointstate.joint_names).position[0]
        # Update all the fixed transforms between the parent links and the child links of these joints
        for i, joint_name in enumerate(kc.lock_jointstate.joint_names):
            joint = self.robot.joints[joint_name]
            joint_pos = kc.lock_jointstate.position[i]
            child_link_name = joint.body1.split("/")[-1]

            # Compute the fixed transform between the parent link and the child link
            # Note that we cannot directly query the parent and child link poses from OG
            # because the cu_joint_state might not represent the current joint position in OG

            jf_to_cf_pose = joint.local_position_1, joint.local_orientation_1
            # Compute the transform from child frame to joint frame
            cf_to_jf_pose = T.invert_pose_transform(*jf_to_cf_pose)

            # Compute the transform from the joint frame to the joint frame moved by the joint position
            if joint.joint_type == JointType.JOINT_FIXED:
                jf_to_jf_moved_pos = th.zeros(3)
                jf_to_jf_moved_quat = th.tensor([0.0, 0.0, 0.0, 1.0])
            elif joint.joint_type == JointType.JOINT_PRISMATIC:
                jf_to_jf_moved_pos = th.tensor([0.0, 0.0, 0.0])
                jf_to_jf_moved_pos[["X", "Y", "Z"].index(joint.axis)] = joint_pos
                jf_to_jf_moved_quat = th.tensor([0.0, 0.0, 0.0, 1.0])
            elif joint.joint_type == JointType.JOINT_REVOLUTE:
                jf_to_jf_moved_pos = th.zeros(3)
                axis = th.zeros(3)
                axis[["X", "Y", "Z"].index(joint.axis)] = 1.0
                jf_to_jf_moved_quat = T.axisangle2quat(axis * joint_pos.cpu())
            else:
                raise NotImplementedError(f"Joint type {joint.joint_type} not supported")

            # Compute the transform from the child frame to the joint frame moved by the joint position
            cf_to_jf_moved_pose = T.pose_transform(jf_to_jf_moved_pos, jf_to_jf_moved_quat, *cf_to_jf_pose)

            # Compute the transform from the joint frame moved by the joint position to the parent frame
            jf_moved_to_pf_pose = joint.local_position_0, joint.local_orientation_0

            # Compute the transform from the child frame to the parent frame
            cf_to_pf_pose = T.pose_transform(*jf_moved_to_pf_pose, *cf_to_jf_moved_pose)
            cf_to_pf_pose = T.pose2mat(cf_to_pf_pose)

            link_idx = kc.link_name_to_idx_map[child_link_name]
            kc.fixed_transforms[link_idx] = cf_to_pf_pose

    def solve_ik_batch(
        self,
        start_state: Any,
        goal_pose: Any,
        plan_config: Any,
        link_poses: Optional[Any] = None,
        emb_sel=CuRoboEmbodimentSelection.DEFAULT,
    ):
        """Find IK solutions to reach a batch of goal poses from a batch of start joint states.

        Args:
            start_state: Start joint states of the robot. When planning from a non-static state,
                i.e, when velocity or acceleration is non-zero, set :attr:`MotionGen.optimize_dt`
                to False.
            goal_pose: Goal poses for the end-effector.ik_
            plan_config: Planning parameters for motion generation.
            link_poses: Goal poses for each link in the robot when planning for multiple links.

        Returns:
            IKResult: Result of IK solution. Check :attr:`IKResult.success`
                attribute to check which indices of the batch were successful.
            bool: Whether the IK solution was successful for the batch.
            JointState: Joint state of the robot at the goal pose.
        """
        solve_state = self.mg[emb_sel]._get_solve_state(
            lazy.curobo.wrap.reacher.types.ReacherSolveType.BATCH, plan_config, goal_pose, start_state
        )
        result = self.mg[emb_sel]._solve_ik_from_solve_state(
            goal_pose,
            solve_state,
            start_state,
            plan_config.use_nn_ik_seed,
            plan_config.partial_ik_opt,
            link_poses,
        )
        # If any of the IK seeds is successful
        success = result.success.any(dim=1)
        # Set non-successful error to infinity
        result.error[~result.success].fill_(float("inf"))
        # Get the index of the minimum error
        min_error_idx = result.error.argmin(dim=1)
        # Get the joint state with the minimum error
        joint_state = result.js_solution[range(result.js_solution.shape[0]), min_error_idx]
        joint_state = [joint_state[i] for i in range(joint_state.shape[0])]
        return result, success, joint_state

    def plan_batch(
        self,
        start_state: Any,
        goal_pose: Any,
        plan_config: Any,
        link_poses: Optional[Any] = None,
        emb_sel=CuRoboEmbodimentSelection.DEFAULT,
    ):
        """Plan a batch of trajectories from a batch of start joint states to a batch of goal poses.

        Args:
            start_state: Start joint states of the robot. When planning from a non-static state,
                i.e, when velocity or acceleration is non-zero, set :attr:`MotionGen.optimize_dt`
                to False.
            goal_pose: Goal poses for the end-effector.
            plan_config: Planning parameters for motion generation.
            link_poses: Goal poses for each link in the robot when planning for multiple links.

        Returns:
            MotionGenResult: Result of IK solution. Check :attr:`MotionGenResult.success`
                attribute to check which indices of the batch were successful.
            bool: Whether the IK solution was successful for the batch.
            JointState: Joint state of the robot at the goal pose.
        """
        result = self.mg[emb_sel].plan_batch(start_state, goal_pose, plan_config, link_poses=link_poses)
        success = result.success
        if result.interpolated_plan is None:
            joint_state = [None] * goal_pose.batch
        elif result.interpolated_plan.position.ndim < 3:
            # curobo's finetune-trajopt path (motion_gen.py ~3866-3873) returns a SINGLE,
            # already-trimmed 2D interpolated_plan even from plan_batch (it trims to
            # path_buffer_last_tstep[0]). get_paths() then assumes a 3D batch — it indexes
            # interpolated_plan[x] over range(len(...)), yielding a 1D JointState whose
            # trim_trajectory raises "JointState does not have horizon". Take that one
            # trajectory as a batch-of-1, trimmed the same way get_paths would.
            _last = result.path_buffer_last_tstep[0] if result.path_buffer_last_tstep is not None else 0
            print(f"[curobo.plan_batch] single 2D interpolated_plan "
                  f"{tuple(result.interpolated_plan.position.shape)} (finetune path) -> 1-batch path",
                  flush=True)
            joint_state = [result.interpolated_plan.trim_trajectory(0, _last)]
        else:
            joint_state = result.get_paths()

        return result, success, joint_state

    def guidance_horizon(self, emb_sel=CuRoboEmbodimentSelection.DEFAULT):
        """Trajopt horizon a guidance reference must be retimed to (see compute_trajectories)."""
        return int(self.mg[emb_sel].trajopt_solver.action_horizon)

    def _install_guidance_hooks(self, mg):
        """Patch @mg's trajopt/finetune rollouts with a joint-space reference tracking cost and
        its trajopt seed pipeline with reference seeding. @mg MUST be built with
        use_cuda_graph=False — a captured graph replays recorded kernels and would never execute
        these python hooks.

        Tracking term: w * ||q_(b,h) - ref_(b,h)||^2 added to the rollout cost. The reference row
        for each rollout batch element is picked via the goal buffer's batch_pose_idx — the same
        problem-mapping the pose cost uses — so it stays correct regardless of curobo's
        seed-major/problem-major flattening conventions.
        """
        owner = self

        for solver in (mg.trajopt_solver, mg.finetune_trajopt_solver):
            for rollout in solver.get_all_rollout_instances():
                if not isinstance(rollout, lazy.curobo.rollout.arm_reacher.ArmReacher):
                    continue

                def cost_fn(state, action_batch=None, _orig=rollout.cost_fn, _ro=rollout):
                    c = _orig(state, action_batch)
                    ref, w = owner._guidance_ref, owner._guidance_weight
                    if ref is None or w <= 0.0:
                        return c
                    q = state.state_seq.position
                    if q.dim() != 3 or q.shape[1] != ref.shape[1] or q.shape[2] != ref.shape[2]:
                        return c  # not a horizon-shaped rollout (e.g. differently-interpolated pass)
                    gb = getattr(_ro, "_goal_buffer", None)
                    idx = getattr(gb, "batch_pose_idx", None) if gb is not None else None
                    if idx is not None and idx.numel() == q.shape[0] and int(idx.max()) < ref.shape[0]:
                        r = ref[idx.view(-1).long()]
                    elif q.shape[0] % ref.shape[0] == 0:
                        r = ref.repeat(q.shape[0] // ref.shape[0], 1, 1)
                    else:
                        return c
                    track = w * ((q - r) ** 2).sum(dim=-1)
                    return c + (track if c.dim() == 2 else track.sum(dim=-1))

                rollout.cost_fn = cost_fn

        def get_seed_set(goal, seed_traj=None, seed_success=None, num_seeds=None, batch_mode=False,
                         _orig=mg.trajopt_solver.get_seed_set):
            out = _orig(goal, seed_traj=seed_traj, seed_success=seed_success, num_seeds=num_seeds,
                        batch_mode=batch_mode)
            ref = owner._guidance_seed
            # Only take over the pure linear-seed case; graph-planner seeds (seed_traj not None)
            # must never be clobbered. Seeds flatten seed-major, so rows [0:batch] are seed 0 of
            # every problem.
            if (
                ref is not None
                and seed_traj is None
                and out.dim() == 3
                and out.shape[1:] == ref.shape[1:]
                and out.shape[0] >= ref.shape[0]
            ):
                out[: ref.shape[0]] = ref
            return out

        mg.trajopt_solver.get_seed_set = get_seed_set

    def _get_guided_mg(self, emb_sel):
        if emb_sel not in self._guided_mg:
            # A failed build (typically CUDA OOM next to the sim) is cached as None so every
            # later guided solve fails FAST to the vanilla fallback instead of re-attempting a
            # doomed multi-hundred-MB build (which also leaks fragments each retry).
            self._guided_mg[emb_sel] = None
            robot_cfg_obj, motion_kwargs = self._guided_build_src[emb_sel]
            motion_kwargs = dict(motion_kwargs)
            motion_kwargs["use_cuda_graph"] = False
            print(f"[curobo] building guided (no-cuda-graph) MotionGen for {emb_sel} ...", flush=True)
            th.cuda.empty_cache()
            cfg = lazy.curobo.wrap.reacher.motion_gen.MotionGenConfig.load_from_robot_config(
                robot_cfg=robot_cfg_obj,
                world_model=None,
                world_coll_checker=self._world_coll_checker,
                tensor_args=self._tensor_args,
                store_trajopt_debug=False,
                **motion_kwargs,
            )
            mg = lazy.curobo.wrap.reacher.motion_gen.MotionGen(cfg)
            # NO warmup: with use_cuda_graph=False there is nothing to capture, warp kernels are
            # already JIT-compiled by the vanilla instance, and warmup's workspace is exactly the
            # peak allocation that OOMs when curobo shares the sim GPU. (Graph-planner device
            # migration only matters for the GPU-split setup, which is unusable here anyway.)
            self._install_guidance_hooks(mg)
            self._guided_mg[emb_sel] = mg
            print(f"[curobo] guided MotionGen for {emb_sel} ready (warmup skipped)", flush=True)
        if self._guided_mg[emb_sel] is None:
            raise RuntimeError(f"guided MotionGen for {emb_sel} unavailable (earlier build failed)")
        return self._guided_mg[emb_sel]

    def compute_trajectories(
        self,
        target_pos,
        target_quat,
        initial_joint_pos=None,
        is_local=False,
        max_attempts=5,
        timeout=2.0,
        ik_fail_return=5,
        enable_finetune_trajopt=True,
        finetune_attempts=1,
        return_full_result=False,
        success_ratio=None,
        attached_obj=None,
        attached_obj_scale=None,
        motion_constraint=None,
        orient_free=False,
        partial_pose_masks=None,
        partial_pose_pos_tol=None,
        partial_pose_rot_tol=None,
        partial_pose_start_bias=None,
        enforce_link_goal=None,
        enable_graph_attempt=None,
        skip_obstacle_update=False,
        ik_only=False,
        ik_world_collision_check=True,
        emb_sel=CuRoboEmbodimentSelection.DEFAULT,
        guidance_reference=None,
        guidance_weight=0.0,
    ):
        """
        Computes the robot joint trajectory to reach the desired @target_pos and @target_quat

        Args:
            target_pos (Dict[str, th.Tensor] or th.Tensor): The torch tensor shape is either (3,) or (N, 3)
                where each entry is an individual (x,y,z) position to reach with the default end-effector link specified
                @self.ee_link[emb_sel]. If a dictionary is given, the keys should be the end-effector links and
                the values should be the corresponding (N, 3) tensors
            target_quat (Dict[str, th.Tensor] or th.Tensor): The torch tensor shape is either (4,) or (N, 4)
                where each entry is an individual (x,y,z,w) quaternion to reach with the default end-effector link specified
                @self.ee_link[emb_sel]. If a dictionary is given, the keys should be the end-effector links and
                the values should be the corresponding (N, 4) tensors
            initial_joint_pos (None or th.Tensor): If specified, the initial joint positions to start the trajectory.
                Default is the current joint positions of the robot
            is_local (bool): Whether @target_pos and @target_quat are specified in the robot's local frame or the world
                global frame
            max_attempts (int): Maximum number of attempts for trying to compute a valid trajectory
            timeout (float): Maximum time in seconds allowed to solve the motion generation problem
            ik_fail_return (None or int): Number of IK attempts allowed before returning a failure. Set this to a
                low value (5) to save compute time when an unreachable goal is given
            enable_finetune_trajopt (bool): Whether to enable timing reparameterization for a smoother trajectory
            finetune_attempts (int): Number of attempts to run finetuning trajectory optimization. Every attempt will
                increase the `MotionGenPlanConfig.finetune_dt_scale` by `MotionGenPlanConfig.finetune_dt_decay` as a
                path couldn't be found with the previous smaller dt
            return_full_result (bool): Whether to return a list of raw MotionGenResult object(s) or a 2-tuple of
                (success, results); the default is the latter
            success_ratio (None or float): If set, specifies the fraction of successes necessary given self.batch_size.
                If None, will automatically be the smallest ratio (1 / self.batch_size), i.e: any nonzero number of
                successes
            attached_obj (None or Dict[str, BaseObject]): If specified, a dictionary where the keys are the end-effector
                link names and the values are the corresponding BaseObject instances to attach to that link
            attached_obj_scale (None or Dict[str, float]): If specified, a dictionary where the keys are the end-effector
                link names and the values are the corresponding scale to apply to the attached object
            motion_constraint (None or List[float]): If specified, the motion constraint vector is a 6D vector controlling
                end-effector movement (angular first, linear next): [qx, qy, qz, x, y, z]. Setting any component to 1.0
                locks that axis, forcing the planner to reach the target using only the remaining unlocked axes.
                Details can be found here: https://curobo.org/advanced_examples/3_constrained_planning.html
            orient_free (bool): If True, the GOAL orientation of the canonical end-effector
                (@self.ee_link[emb_sel]) is left free: the pose cost uses reach_partial_pose with the
                angular reach weights zeroed ([qx,qy,qz,x,y,z] -> [0,0,0,1,1,1]), so the planner only
                needs to reach the target position. Applies ONLY to the canonical ee_link goal
                (include_link_pose stays False), so pose-held auxiliary links keep their full pose.
            partial_pose_masks (None or Dict[str, List[float]]): If specified, per-link partial-pose
                reach masks keyed by end-effector link name; each value is a 6D weight vector
                ([qx,qy,qz,x,y,z], angular first) where a 0.0 entry FREES that DOF for that link.
                Unlike @orient_free (and curobo's PoseCostMetric helper, which is restricted to the
                canonical ee_link), this writes the mask directly onto EACH link's own goal AND
                convergence PoseCost, so ANY arm — canonical or a pose-held auxiliary link — can take
                a partial goal in one solve (e.g. {right_eef_link: [1,1,1,0,0,0]} = reach the goal
                orientation anywhere in space). The mask is reset to full pose after planning.
            partial_pose_start_bias (None or float): If specified (with @partial_pose_masks), the
                null-space selection weight applied to the IK stage for this solve, so among the
                pose-feasible branches of a loose partial goal the one nearest the START config is
                returned (see the inline rationale at the bias block). Restored after planning.
            enforce_link_goal (None or str): If specified, the goal link whose target must be
                REACHED by each returned trajectory: successful paths are FK-checked at their
                final waypoint against this link's target (respecting partial masks/tolerances)
                and demoted to failure when short. Needed because curobo's own success gates the
                canonical ee_link only — auxiliary-link goals can otherwise "succeed" unreached.
            skip_obstacle_update (bool): Whether to skip updating the obstacles in the world collision checker
            ik_only (bool): Whether to only run the IK solver and not the trajectory optimization
            ik_world_collision_check (bool): Whether to check for collisions in the world when running the IK solver for ik_only mode
            emb_sel (CuRoboEmbodimentSelection): Which embodiment selection to use for computing trajectories
            guidance_reference (None or th.Tensor): If specified, a (batch_size, horizon,
                n_robot_joints) FULL-ROBOT joint-position reference (robot_joint_names order,
                horizon = self.guidance_horizon(emb_sel)); each batch element is seeded with and
                cost-tracked against its own reference row. The solve runs on the dedicated
                no-cuda-graph MotionGen (built lazily on first use).
            guidance_weight (float): Weight of the joint-space tracking cost
                w * ||q_t - q_ref_t||^2 added to the trajopt rollouts (only with
                @guidance_reference). Trajectory length is still governed by the stock
                smoothness/limit costs, so this weight balances demo-similarity against them.
        Returns:
            2-tuple or list of MotionGenResult: If @return_full_result is True, will return a list of raw MotionGenResult
                object(s) computed from internal batch trajectory computations. If it is False, will return 2-tuple
                (success, results), where success is a (N,)-shaped boolean tensor representing whether each requested
                target pos / quat successfully generated a motion plan, and results is a (N,)-shaped array of
                corresponding JointState objects.
        """
        # Previously, this would silently fail so we explicitly check for out-of-range joint limits here
        # This may be fixed in a recent version of CuRobo? See https://github.com/NVlabs/curobo/discussions/288
        # relevant_joint_positions_normalized = (
        #     lazy.curobo.types.state.JointState(
        #         position=self.tensor_args.to_device(self.robot.get_joint_positions(normalized=True)),
        #         joint_names=self.robot_joint_names,
        #     )
        #     .get_ordered_joint_state(self.mg[emb_sel].kinematics.joint_names)
        #     .position
        # )

        # if not th.all(th.abs(relevant_joint_positions_normalized) < 0.99):
        #     print("Robot is near joint limits! No trajectory will be computed")
        #     return None, None if not return_full_result else None

        if guidance_reference is not None:
            assert not ik_only, "guidance applies to trajopt solves only"
            ref = guidance_reference
            assert ref.dim() == 3 and ref.shape[0] == self.batch_size, (
                f"guidance_reference must be (batch_size={self.batch_size}, horizon, n_robot_joints), "
                f"got {tuple(ref.shape)}"
            )
            mg_guided = self._get_guided_mg(emb_sel)
            horizon = int(mg_guided.trajopt_solver.action_horizon)
            assert ref.shape[1] == horizon, f"reference horizon {ref.shape[1]} != trajopt horizon {horizon}"
            # Full-robot -> the guided solver's active-joint order (same joint set as self.mg's).
            ref_active = (
                lazy.curobo.types.state.JointState(
                    position=self._tensor_args.to_device(ref),
                    joint_names=self.robot_joint_names,
                )
                .get_ordered_joint_state(mg_guided.kinematics.joint_names)
                .position.contiguous()
            )
            mg_prev = self.mg[emb_sel]
            self.mg[emb_sel] = mg_guided
            self._guidance_ref = ref_active
            self._guidance_seed = ref_active
            self._guidance_weight = float(guidance_weight)
            try:
                # Re-enter the vanilla body against the swapped guided instance: all existing
                # partial-pose/tolerance/attach machinery runs unchanged on it.
                return self.compute_trajectories(
                    target_pos=target_pos,
                    target_quat=target_quat,
                    initial_joint_pos=initial_joint_pos,
                    is_local=is_local,
                    max_attempts=max_attempts,
                    timeout=timeout,
                    ik_fail_return=ik_fail_return,
                    enable_finetune_trajopt=enable_finetune_trajopt,
                    finetune_attempts=finetune_attempts,
                    return_full_result=return_full_result,
                    success_ratio=success_ratio,
                    attached_obj=attached_obj,
                    attached_obj_scale=attached_obj_scale,
                    motion_constraint=motion_constraint,
                    orient_free=orient_free,
                    partial_pose_masks=partial_pose_masks,
                    partial_pose_pos_tol=partial_pose_pos_tol,
                    partial_pose_rot_tol=partial_pose_rot_tol,
                    partial_pose_start_bias=partial_pose_start_bias,
                    enforce_link_goal=enforce_link_goal,
                    enable_graph_attempt=enable_graph_attempt,
                    skip_obstacle_update=skip_obstacle_update,
                    ik_only=ik_only,
                    ik_world_collision_check=ik_world_collision_check,
                    emb_sel=emb_sel,
                    guidance_reference=None,
                )
            finally:
                self._guidance_ref = None
                self._guidance_seed = None
                self._guidance_weight = 0.0
                self.mg[emb_sel] = mg_prev

        if not skip_obstacle_update:
            self.update_obstacles()

        # If target_pos and target_quat are torch tensors, it's assumed that they correspond to the default ee_link
        if isinstance(target_pos, th.Tensor):
            target_pos = {self.ee_link[emb_sel]: target_pos}
        if isinstance(target_quat, th.Tensor):
            target_quat = {self.ee_link[emb_sel]: target_quat}

        assert target_pos.keys() == target_quat.keys(), "Expected target_pos and target_quat to have the same keys!"

        # Make sure tensor shapes are (N, 3) and (N, 4)
        target_pos = {k: v if len(v.shape) == 2 else v.unsqueeze(0) for k, v in target_pos.items()}
        target_quat = {k: v if len(v.shape) == 2 else v.unsqueeze(0) for k, v in target_quat.items()}

        for link_name in target_pos.keys():
            target_pos_link = target_pos[link_name]
            target_quat_link = target_quat[link_name]
            if not is_local:
                # Convert target pose to base link *in the eyes of curobo*.
                # For stationary arms (e.g. Franka), it is @robot.root_link / @robot.base_footprint_link_name ("base_link")
                # For holonomic robots (e.g. Tiago, R1), it is @robot.root_link ("base_footprint_x"), not @robot.base_footprint_link_name ("base_link")
                curobo_base_link_name = self.base_link[emb_sel]
                robot_pos, robot_quat = self.robot.links[curobo_base_link_name].get_position_orientation()
                target_pose = th.zeros((target_pos_link.shape[0], 4, 4))
                target_pose[:, 3, 3] = 1.0
                target_pose[:, :3, :3] = T.quat2mat(target_quat_link)
                target_pose[:, :3, 3] = target_pos_link
                inv_robot_pose = T.pose_inv(T.pose2mat((robot_pos, robot_quat)))
                target_pose = inv_robot_pose.view(1, 4, 4) @ target_pose
                target_pos_link = target_pose[:, :3, 3]
                target_quat_link = T.mat2quat(target_pose[:, :3, :3])

            # Map xyzw -> wxyz quat
            target_quat_link = target_quat_link[:, [3, 0, 1, 2]]

            # Make sure tensors are on device and contiguous
            target_pos_link = self._tensor_args.to_device(target_pos_link).contiguous()
            target_quat_link = self._tensor_args.to_device(target_quat_link).contiguous()

            target_pos[link_name] = target_pos_link
            target_quat[link_name] = target_quat_link

        # Define the plan config
        plan_cfg = lazy.curobo.wrap.reacher.motion_gen.MotionGenPlanConfig(
            # enable_graph_attempt=N falls back to curobo's GRAPH/PRM planner to seed trajopt after
            # N failed trajopt attempts — the global planner finds collision-free paths through
            # large reconfigurations that local trajopt alone cannot (curobo's standard pipeline;
            # OFF by default here). Needed for orientation-focused reorients whose reachable goal
            # config is far in joint space from the start.
            enable_graph=enable_graph_attempt is not None,
            # num_graph_seeds==1 is required for the graph planner in BATCH mode (else curobo
            # warns + skips the graph). It must be set on the PLAN config (read at solve time),
            # not just the generator config.
            num_graph_seeds=(1 if enable_graph_attempt is not None else None),
            max_attempts=max_attempts,
            timeout=timeout,
            enable_graph_attempt=enable_graph_attempt,
            ik_fail_return=ik_fail_return,
            enable_finetune_trajopt=enable_finetune_trajopt,
            finetune_attempts=finetune_attempts,
            success_ratio=1.0 / self.batch_size if success_ratio is None else success_ratio,
        )

        # Add the pose cost metric
        if self.ee_link[emb_sel] in target_pos and (motion_constraint is not None or orient_free):
            metric_kwargs = {}
            if motion_constraint is not None:
                metric_kwargs.update(
                    hold_partial_pose=True, hold_vec_weight=self._tensor_args.to_device(motion_constraint)
                )
            if orient_free:
                # Position-only goal: zero the angular reach weights so the solver may pick any
                # final orientation for the canonical ee_link.
                metric_kwargs.update(
                    reach_partial_pose=True,
                    reach_vec_weight=self._tensor_args.to_device([0.0, 0.0, 0.0, 1.0, 1.0, 1.0]),
                )
            plan_cfg.pose_cost_metric = lazy.curobo.wrap.reacher.motion_gen.PoseCostMetric(**metric_kwargs)

        # Construct initial state
        if initial_joint_pos is None:
            q_pos = th.stack([self.robot.get_joint_positions()] * self.batch_size, axis=0)
            q_vel = th.stack([self.robot.get_joint_velocities()] * self.batch_size, axis=0)
            q_eff = th.stack([self.robot.get_joint_efforts()] * self.batch_size, axis=0)
        else:
            q_pos = th.stack([initial_joint_pos] * self.batch_size, axis=0)
            q_vel = th.zeros_like(q_pos)
            q_eff = th.zeros_like(q_pos)

        cu_joint_state = lazy.curobo.types.state.JointState(
            position=self._tensor_args.to_device(q_pos),
            # TODO: Ideally these should be nonzero, but curobo fails to compute a solution if so
            # See this note from https://curobo.org/get_started/2b_isaacsim_examples.html
            # Motion generation only generates motions when the robot is static.
            # cuRobo has an experimental mode to optimize from non-static states.
            # You can try this by passing --reactive to motion_gen_reacher.py.
            # This mode will have lower success than the static mode as now the optimization
            # has to account for the robot’s current velocity and acceleration.
            # The weights have also not been tuned for reactive mode.
            velocity=self._tensor_args.to_device(q_vel) * 0.0,
            acceleration=self._tensor_args.to_device(q_eff) * 0.0,
            jerk=self._tensor_args.to_device(q_eff) * 0.0,
            joint_names=self.robot_joint_names,
        )

        # Update the locked joints with the current joint positions
        self.update_locked_joints(cu_joint_state, emb_sel)

        cu_js_batch = cu_joint_state.get_ordered_joint_state(self.mg[emb_sel].kinematics.joint_names)

        # Attach object to robot if requested
        attached_info = self._attach_objects_to_robot(
            attached_obj=attached_obj,
            attached_obj_scale=attached_obj_scale,
            cu_js_batch=cu_js_batch,
            emb_sel=emb_sel,
        )

        all_rollout_fns = [
            fn
            for fn in self.mg[emb_sel].get_all_rollout_instances()
            if isinstance(fn, lazy.curobo.rollout.arm_reacher.ArmReacher)
        ]

        def _pose_cost_pair(rollout_fn, link_name):
            """The (goal, convergence) PoseCost pair for @link_name. The canonical ee_link uses
            goal_cost/pose_convergence; auxiliary links use the per-link cost dicts. Both are
            PoseCost instances (own vec_weight tensor) exposing reach_partial_pose/reach_full_pose,
            so a partial mask on one link does not affect any other link."""
            if link_name == self.ee_link[emb_sel]:
                return rollout_fn.goal_cost, rollout_fn.pose_convergence
            return rollout_fn._link_pose_costs[link_name], rollout_fn._link_pose_convergence[link_name]

        if partial_pose_masks:
            print(f"[curobo] partial_pose_masks requested {({k: list(v) for k, v in partial_pose_masks.items()})} "
                  f"(rotation-first [qx,qy,qz,x,y,z], 0=free); ee_link={self.ee_link[emb_sel]!r}, "
                  f"target links={list(target_pos.keys())}", flush=True)

        # (link convergence position-threshold, original value) pairs saved when a position
        # ALLOWANCE is applied, so it can be restored after the solve (it would otherwise leak
        # a loose tolerance into the next plan on this cached MotionGen).
        _pos_tol_saved = []
        # Same as _pos_tol_saved but for the ROTATION convergence threshold (_vec_convergence[0]):
        # a coarse "make it roughly upright" reorient treats the held object as already-converged
        # once its axis is within @partial_pose_rot_tol of the target, so a start already inside the
        # tolerance needs no large motion. Saved+restored so it does not leak to the next plan.
        _rot_tol_saved = []
        # The per-rollout _vec_convergence[1] loosened below only relaxes the pose-cost CONVERGENCE
        # term. The IK SUCCESS GATE that decides IK_FAIL compares the seed's position error against
        # a SEPARATE solver-level scalar, IKSolver.position_threshold (cuRobo default 0.005 m), which
        # _vec_convergence does not touch. So an orientation-focused reorient with pos_tol=0.20 still
        # has its IK rejected unless the seed reaches within ~5 mm of the exact target position —
        # which an arm-only (torso-locked) reorient generally cannot, yielding IK_FAIL even though
        # 20 cm of position slack was requested. Loosen the gate to the same tolerance; saved+reset.
        _ik_pos_threshold_saved = None
        # Enable/disable costs based on whether the end-effector is in the target position
        for rollout_fn in all_rollout_fns:
            (
                rollout_fn.goal_cost.enable_cost()
                if self.ee_link[emb_sel] in target_pos
                else rollout_fn.goal_cost.disable_cost()
            )
            (
                rollout_fn.pose_convergence.enable_cost()
                if self.ee_link[emb_sel] in target_pos
                else rollout_fn.pose_convergence.disable_cost()
            )
            for additional_link in self.additional_links[emb_sel]:
                (
                    rollout_fn._link_pose_costs[additional_link].enable_cost()
                    if additional_link in target_pos
                    else rollout_fn._link_pose_costs[additional_link].disable_cost()
                )
                (
                    rollout_fn._link_pose_convergence[additional_link].enable_cost()
                    if additional_link in target_pos
                    else rollout_fn._link_pose_convergence[additional_link].disable_cost()
                )
            # Per-link PARTIAL-pose masks: zero-weight DOF become FREE for THAT link only.
            # Applied to the goal AND convergence PoseCost so the freed DOF are ignored by the
            # success threshold too; reset to full pose after the solve so it doesn't leak into
            # the next plan on this cached MotionGen.
            if partial_pose_masks:
                for link_name, mask in partial_pose_masks.items():
                    if link_name in target_pos:
                        mvec = self._tensor_args.to_device(mask)
                        for c in _pose_cost_pair(rollout_fn, link_name):
                            c.reach_partial_pose(mvec)
                            # Position ALLOWANCE: loosen this link's position convergence
                            # threshold to @partial_pose_pos_tol so a held-position goal accepts a
                            # small drift. An orientation-focused reorient often needs the gripper
                            # to move >5mm (curobo default) to reach the goal orientation; with no
                            # slack the success gate is never met and it TRAJOPT_FAILs. Saved+reset.
                            if partial_pose_pos_tol is not None:
                                _pos_tol_saved.append((c, float(c._vec_convergence[1])))
                                c._vec_convergence[1] = float(partial_pose_pos_tol)
                            if partial_pose_rot_tol is not None:
                                _rot_tol_saved.append((c, float(c._vec_convergence[0])))
                                c._vec_convergence[0] = float(partial_pose_rot_tol)

        # Loosen the IK success gate (solver-level scalar) to match the requested position tolerance,
        # so the IK stage accepts a seed that meets the orientation goal within @partial_pose_pos_tol
        # of the target position instead of demanding the ~5 mm default. The position COST still pulls
        # the held object toward the target, so the accepted solution stays as close as the kinematics
        # allow; this only stops the gate from rejecting an otherwise-valid orientation-focused reorient.
        _ik_rot_threshold_saved = None
        if partial_pose_masks and (partial_pose_pos_tol is not None or partial_pose_rot_tol is not None):
            _iks = self.mg[emb_sel].ik_solver
            if partial_pose_pos_tol is not None:
                _ik_pos_threshold_saved = (_iks, _iks.position_threshold)
                _iks.position_threshold = float(partial_pose_pos_tol)
            # Same gate fix for ROTATION: the IK success gate compares against the solver-level
            # rotation_threshold (default ~0.05 rad), independent of the per-rollout convergence
            # vector, so a coarse reorient must loosen it here too or IK_FAILs at ~5 deg off-axis.
            if partial_pose_rot_tol is not None:
                _ik_rot_threshold_saved = (_iks, _iks.rotation_threshold)
                _iks.rotation_threshold = float(partial_pose_rot_tol)

        # Bias the RETURNED IK branch toward the START configuration. curobo already seeds AND
        # regularizes IK with the start config (motion_gen passes it as retract_config), but the
        # term that SELECTS which converged seed is returned (null_space_error, weight 0.001 from
        # base_cfg convergence.null_space_cfg) is negligible against pose error — so among the many
        # pose-feasible branches of a loose partial-pose goal (spin free + position slack) an
        # arbitrary, possibly distant arm branch wins and the trajectory sweeps a wide arc
        # (measured 12.9 rad of joint travel for a 16 deg held-object reorient). Boosting the
        # weight for THIS solve re-ranks only among pose-feasible seeds (success is gated
        # separately by the position/rotation thresholds), so the nearest feasible branch is
        # returned; when the near branch is truly infeasible the +1000 infeasibility penalty still
        # sends the solve to a far branch. In-place update_weight is cuda-graph safe; saved and
        # restored so the boost does not leak into other solves on this cached MotionGen.
        _null_w_saved = []
        if partial_pose_masks and partial_pose_start_bias is not None:
            for rollout_fn in self.mg[emb_sel].ik_solver.get_all_rollout_instances():
                nc = getattr(rollout_fn, "null_convergence", None)
                if nc is not None and getattr(nc, "weight", None) is not None:
                    _null_w_saved.append((nc, nc.weight.clone()))
                    nc.update_weight(float(partial_pose_start_bias))

        if ik_only:
            for rollout_fn in self.mg[emb_sel].ik_solver.get_all_rollout_instances():
                (
                    rollout_fn.primitive_collision_cost.enable_cost()
                    if ik_world_collision_check
                    else rollout_fn.primitive_collision_cost.disable_cost()
                )
                (
                    rollout_fn.primitive_collision_constraint.enable_cost()
                    if ik_world_collision_check
                    else rollout_fn.primitive_collision_constraint.disable_cost()
                )

        # Determine how many internal batches we need to run based on submitted size
        num_targets = next(iter(target_pos.values())).shape[0]
        remainder = num_targets % self.batch_size
        n_batches = math.ceil(num_targets / self.batch_size)

        # If ee_link is not in target_pos, add trivial target poses to avoid errors
        if self.ee_link[emb_sel] not in target_pos:
            target_pos[self.ee_link[emb_sel]] = self._tensor_args.to_device(th.zeros((num_targets, 3)))
            target_quat[self.ee_link[emb_sel]] = self._tensor_args.to_device(th.zeros((num_targets, 4)))
            target_quat[self.ee_link[emb_sel]][..., 0] = 1.0

        # KNOWN LIMITATION (verified live 2026-07-03, probes auxgate_a1-a3): goals keyed to a
        # NON-canonical link reach the IK stage's goal buffer correctly, but the TRAJOPT stage
        # evaluates that link's goal as the CURRENT START pose — the optimizer holds the arm at
        # its start and reports position_error~1e-6 "success" with a 1-waypoint plan. Callers
        # MUST pass @enforce_link_goal so such vacuous successes are demoted; producing
        # aux-link motions reliably requires the IK -> plan_single_js route (region-goal
        # recipe) until the trajopt link-goal buffer path is fixed upstream.

        # Run internal batched calls
        results, successes, paths = [], self._tensor_args.to_device(th.tensor([], dtype=th.bool)), []
        try:
            for i in range(n_batches):
                # We're using a remainder if we're on the final batch and our remainder is nonzero
                using_remainder = (i == n_batches - 1) and remainder > 0
                offset_idx = self.batch_size * i
                end_idx = remainder if using_remainder else self.batch_size

                ik_goal_batch_by_link = dict()
                for link_name in target_pos.keys():
                    target_pos_link = target_pos[link_name]
                    target_quat_link = target_quat[link_name]

                    batch_target_pos = target_pos_link[offset_idx : offset_idx + end_idx]
                    batch_target_quat = target_quat_link[offset_idx : offset_idx + end_idx]

                    # Pad the goal if we're in our final batch
                    if using_remainder:
                        new_batch_target_pos = self._tensor_args.to_device(th.zeros((self.batch_size, 3)))
                        new_batch_target_pos[:end_idx] = batch_target_pos
                        new_batch_target_pos[end_idx:] = batch_target_pos[-1]
                        batch_target_pos = new_batch_target_pos
                        new_batch_target_quat = self._tensor_args.to_device(th.zeros((self.batch_size, 4)))
                        new_batch_target_quat[:end_idx] = batch_target_quat
                        new_batch_target_quat[end_idx:] = batch_target_quat[-1]
                        batch_target_quat = new_batch_target_quat

                    # Create IK goal
                    ik_goal_batch = lazy.curobo.types.math.Pose(
                        position=batch_target_pos,
                        quaternion=batch_target_quat,
                        name=link_name,
                    )

                    ik_goal_batch_by_link[link_name] = ik_goal_batch

                # Run batched planning
                if self.debug:
                    self.mg[emb_sel].store_debug_in_result = True

                # Pop the main ee_link goal
                main_ik_goal_batch = ik_goal_batch_by_link.pop(self.ee_link[emb_sel])

                # If no other goals (e.g. no second end-effector), set to None
                if len(ik_goal_batch_by_link) == 0:
                    ik_goal_batch_by_link = None

                plan_fn = self.plan_batch if not ik_only else self.solve_ik_batch
                result, success, joint_state = plan_fn(
                    cu_js_batch, main_ik_goal_batch, plan_cfg, link_poses=ik_goal_batch_by_link, emb_sel=emb_sel
                )
                if self.debug:
                    breakpoint()

                # Append results
                results.append(result)
                successes = th.concatenate([successes, success[:end_idx]])
                paths += joint_state[:end_idx]

            # AUX-LINK SUCCESS GATE (opt-in via @enforce_link_goal): curobo's success flows through
            # the canonical ee_link's convergence, so a goal keyed to any OTHER link (the
            # non-canonical arm's eef, a held-object tool frame) can be reported successful without
            # the trajectory reaching it (verified live: a right-arm 10cm goal returned success with
            # the final waypoint 0.094m short — the no-op grasp approaches that closed on air).
            # FK-check each successful path's final waypoint against that link's target (respecting
            # partial masks + tolerances) and demote fakes to failure.
            if enforce_link_goal is not None and not ik_only and len(paths) and enforce_link_goal in target_pos:
                link = enforce_link_goal
                mask = list((partial_pose_masks or {}).get(link) or []) or None
                pos_tol = (float(partial_pose_pos_tol)
                           if (mask is not None and partial_pose_pos_tol is not None) else 0.01)
                if mask is not None and partial_pose_rot_tol is not None:
                    # partial_pose_rot_tol arrives in curobo's sin(theta/2) convention
                    rot_tol = 2.0 * math.asin(max(0.0, min(1.0, float(partial_pose_rot_tol))))
                else:
                    rot_tol = 0.1
                free_axes = [] if mask is None else [j for j in range(3) if float(mask[j]) == 0.0]
                # orient_free (position-only goals) legitimately leaves the final orientation
                # to the solver — the nominal goal quat is NOT a requirement, so the gate must
                # check position only (verified live: this gate demoted 18 perfectly-positioned
                # carries, pos_err=0.0000m, purely on the freed orientation's 1-1.9rad drift).
                # orient_free only ever applies to the canonical ee_link (the wrapper falls
                # back to a full-pose goal for any other link).
                rot_gated = not (orient_free and link == self.ee_link[emb_sel])
                kin = self.mg[emb_sel].kinematics
                tgt_p_all, tgt_q_all = target_pos[link], target_quat[link]
                for i in range(len(paths)):
                    if paths[i] is None or not bool(successes[i]):
                        continue
                    try:
                        q_last = paths[i].position[-1].view(1, -1)
                        state = kin.get_state(q_last)
                        if link == self.ee_link[emb_sel]:
                            fk_p = state.ee_position.view(-1)
                            fk_q = state.ee_quaternion.view(-1)
                        else:
                            lp = (getattr(state, "link_pose", None) or {}).get(link)
                            if lp is None:
                                print(f"[curobo] enforce_link_goal: kinematics does not expose "
                                      f"{link!r}; gate skipped", flush=True)
                                break
                            fk_p = lp.position.view(-1)
                            fk_q = lp.quaternion.view(-1)
                    except Exception as exc:  # noqa: BLE001 — a gate failure must not kill the solve
                        print(f"[curobo] enforce_link_goal FK failed ({str(exc)[:80]}); gate skipped",
                              flush=True)
                        break
                    dp = fk_p - tgt_p_all[i]
                    if mask is not None:
                        dp = dp * self._tensor_args.to_device(th.tensor(mask[3:6], dtype=th.float32))
                    pos_err = float(th.norm(dp))
                    rot_err = 0.0
                    if not rot_gated:
                        pass  # orientation freed by the caller — position gate only
                    elif not free_axes:  # all rotation DOF constrained -> full geodesic
                        dot = float(th.abs(th.sum(fk_q * tgt_q_all[i])))
                        rot_err = 2.0 * math.acos(min(1.0, dot))
                    elif len(free_axes) == 1:  # spin about one axis free -> that axis must align
                        j = free_axes[0]
                        R_fk = T.quat2mat(fk_q[[1, 2, 3, 0]].cpu())
                        R_tg = T.quat2mat(tgt_q_all[i][[1, 2, 3, 0]].cpu())
                        rot_err = math.acos(float(th.clamp(th.sum(R_fk[:, j] * R_tg[:, j]),
                                                           -1.0, 1.0)))
                    if pos_err > pos_tol or rot_err > rot_tol:
                        successes[i] = False
                        print(f"[curobo] enforce_link_goal: demoted FAKE success (elem {i}, "
                              f"link={link!r}, pos_err={pos_err:.4f}m vs {pos_tol}, "
                              f"rot_err={rot_err:.3f}rad vs {rot_tol:.3f})", flush=True)
        finally:
            # Restore per-solve solver state even when the solve RAISES (e.g. the
            # first-solve warp allocation failure that _solve retries around): without
            # this, loosened tolerances/masks and a still-attached object leak into the
            # cached MotionGen and silently poison every later plan in the process.
            # Reset any partial-pose masks so the next plan on this cached MotionGen starts from a
            # full pose (curobo only auto-resets the canonical-ee PoseCostMetric path, not these).
            if partial_pose_masks:
                for rollout_fn in all_rollout_fns:
                    for link_name in partial_pose_masks:
                        if link_name in target_pos:
                            for c in _pose_cost_pair(rollout_fn, link_name):
                                c.reach_full_pose()
                for c, orig in _pos_tol_saved:  # restore the loosened position thresholds
                    c._vec_convergence[1] = orig
                for c, orig in _rot_tol_saved:  # restore the loosened rotation thresholds
                    c._vec_convergence[0] = orig
            if _ik_pos_threshold_saved is not None:  # restore the loosened IK success gate
                _ik_pos_threshold_saved[0].position_threshold = _ik_pos_threshold_saved[1]
            if _ik_rot_threshold_saved is not None:
                _ik_rot_threshold_saved[0].rotation_threshold = _ik_rot_threshold_saved[1]
            for nc, orig_w in _null_w_saved:  # restore the start-bias selection weight
                nc.weight.copy_(orig_w)

            # Detach attached object if it was attached
            self._detach_objects_from_robot(attached_info, emb_sel)

        if return_full_result:
            return results
        else:
            return successes, paths

    def path_to_joint_trajectory(self, path, get_full_js=True, emb_sel=CuRoboEmbodimentSelection.DEFAULT):
        """
        Converts raw path from motion generator into joint trajectory sequence

        Args:
            path (JointState): Joint state path to convert into joint trajectory
            get_full_js (bool): Whether to get the full joint state
            emb_sel (CuRoboEmbodimentSelection): Which embodiment to use for the robot

        Returns:
            torch.tensor: (T, D) tensor representing the interpolated joint trajectory
                to reach the desired @target_pos, @target_quat configuration, where T is the number of interpolated
                steps and D is the number of robot joints.
        """
        cmd_plan = self.mg[emb_sel].get_full_js(path) if get_full_js else path
        return cmd_plan.get_ordered_joint_state(self.robot_joint_names).position

    def add_linearly_interpolated_waypoints(self, traj: th.Tensor, max_inter_dist=0.01):
        """
        Adds waypoints to the joint trajectory so that the joint position distance
        between each pairs of neighboring waypoints is less than @max_inter_dist

        Args:
            traj: (T, D) tensor representing the joint trajectory
            max_inter_dist (float): Maximum joint position distance between two neighboring waypoints

        Returns:
            torch.tensor: (T', D) tensor representing the interpolated joint trajectory
        """
        assert len(traj) > 1, "Plan must have at least 2 waypoints to interpolate"
        interpolated_plan = []
        for i in range(len(traj) - 1):
            # Calculate maximum difference across all dimensions
            max_diff = (traj[i + 1] - traj[i]).abs().max()
            num_intervals = math.ceil(max_diff.item() / max_inter_dist)
            interpolated_plan += multi_dim_linspace(traj[i], traj[i + 1], num_intervals, endpoint=False)

        interpolated_plan.append(traj[-1])
        return th.stack(interpolated_plan)

    def path_to_eef_trajectory(
        self, path, return_axisangle=False, emb_sel=CuRoboEmbodimentSelection.DEFAULT
    ) -> Dict[str, th.Tensor]:
        """
        Converts raw path from motion generator into end-effector trajectory sequence in the robot frame.
        This trajectory sequence can be executed by an IKController, although there is no guaranteee that
        the controller will output the same joint trajectory as the one computed by cuRobo.

        Args:
            path (JointState): Joint state path to convert into joint trajectory
            return_axisangle (bool): Whether to return the interpolated orientations in quaternion or axis-angle representation
            emb_sel (CuRoboEmbodimentSelection): Which embodiment to use for the robot

        Returns:
            Dict[str, torch.Tensor]: Mapping eef link names to (T, [6, 7])-shaped array where each entry is is the (x,y,z) position
            and (x,y,z,w) quaternion (if @return_axisangle is False) or (ax, ay, az) axis-angle orientation, specified in the robot frame.
        """
        # If the base-only embodiment is selected, the eef links stay the same, return the current eef poses in the robot frame
        if emb_sel == CuRoboEmbodimentSelection.BASE:
            link_poses = dict()
            for arm_name in self.robot.arm_names:
                link_name = self.robot.eef_link_names[arm_name]
                position, orientation = self.robot.get_relative_eef_pose(arm_name)
                if return_axisangle:
                    orientation = T.quat2axisangle(orientation)
                link_poses[link_name] = th.cat([position, orientation], dim=-1)
            return link_poses

        cmd_plan = self.mg[emb_sel].get_full_js(path)
        robot_state = self.mg[emb_sel].kinematics.compute_kinematics(path)

        link_poses = dict()

        for link_name, poses in robot_state.link_poses.items():
            position = poses.position
            # wxyz -> xyzw
            orientation = poses.quaternion[:, [1, 2, 3, 0]]

            # If the robot is holonomic, we need to transform the poses to the base link frame
            if isinstance(self.robot, HolonomicBaseRobot):
                base_link_position = th.zeros_like(position)
                base_link_position[:, 0] = cmd_plan.position[:, cmd_plan.joint_names.index("base_footprint_x_joint")]
                base_link_position[:, 1] = cmd_plan.position[:, cmd_plan.joint_names.index("base_footprint_y_joint")]
                base_link_euler = th.zeros_like(position)
                base_link_euler[:, 2] = cmd_plan.position[:, cmd_plan.joint_names.index("base_footprint_rz_joint")]
                base_link_orientation = T.euler2quat(base_link_euler)
                position, orientation = T.relative_pose_transform(
                    position, orientation, base_link_position, base_link_orientation
                )

            if return_axisangle:
                orientation = T.quat2axisangle(orientation)
            link_poses[link_name] = th.cat([position, orientation], dim=-1)

        return link_poses

    @property
    def tensor_args(self):
        """
        Returns:
            TensorDeviceType: tensor arguments used by this CuRobo instance
        """
        return self._tensor_args

    def _attach_objects_to_robot(
        self,
        attached_obj,
        attached_obj_scale,
        cu_js_batch,
        emb_sel,
    ):
        """
        Helper function to attach objects to the robot.

        Args:
            attached_obj (None or Dict[str, BaseObject]): Dictionary mapping end-effector
                link names to corresponding BaseObject instances
            attached_obj_scale (None or Dict[str, float]): Dictionary mapping end-effector
                link names to corresponding scale values
            cu_js_batch (JointState): CuRobo joint state object ordered according to kinematics
            emb_sel (CuRoboEmbodimentSelection): Which embodiment selection to use

        Returns:
            list: List of attached object information for detachment
        """
        if attached_obj is None:
            return []

        attached_info = []
        for ee_link_name, obj in attached_obj.items():
            assert isinstance(obj, RigidDynamicPrim), "attached_object should be a RigidDynamicPrim object"
            obj_paths = [geom.prim_path for geom in obj.collision_meshes.values()]
            assert len(obj_paths) <= 32, f"Expected obj_paths to be at most 32, got: {len(obj_paths)}"

            position, quaternion = self.robot.links[ee_link_name].get_position_orientation()
            # xyzw to wxyz
            quaternion = quaternion[[3, 0, 1, 2]]
            ee_pose = lazy.curobo.types.math.Pose(position=position, quaternion=quaternion).to(self._tensor_args)

            scale = m.DEFAULT_ATTACHED_OBJECT_SCALE if attached_obj_scale is None else attached_obj_scale[ee_link_name]

            # CuRobo's trimesh sphere fitting samples with NumPy's process-global RNG when no
            # seed is supplied. Without controlling it, the same grasp receives a different
            # collision model on every solve/check, so a state can alternate between valid and
            # colliding. Preserve caller RNG state while making attachment geometry repeatable.
            np_rng_state = np.random.get_state()
            np.random.seed(0)
            try:
                self.mg[emb_sel].attach_objects_to_robot(
                    joint_state=cu_js_batch,
                    object_names=obj_paths,
                    ee_pose=ee_pose,
                    link_name=self.robot.curobo_attached_object_link_names[ee_link_name],
                    scale=scale,
                    pitch_scale=1.0,
                    merge_meshes=True,
                )
            finally:
                np.random.set_state(np_rng_state)

            attached_info.append(
                {"obj_paths": obj_paths, "link_name": self.robot.curobo_attached_object_link_names[ee_link_name]}
            )

        return attached_info

    def _detach_objects_from_robot(
        self,
        attached_info,
        emb_sel,
    ):
        """
        Helper function to detach previously attached objects from the robot.

        Args:
            attached_info (list): List of dictionaries containing object paths and link names
                returned by _attach_objects_to_robot
            emb_sel (CuRoboEmbodimentSelection): Which embodiment selection to use
        """
        for info in attached_info:
            self.mg[emb_sel].detach_object_from_robot(
                object_names=info["obj_paths"],
                link_name=info["link_name"],
            )
