"""Object state for detecting partial support at a surface edge."""

import math
import time

import numpy as np
import omnigibson as og
import omnigibson.utils.transform_utils as T
import torch as th
from shapely.geometry import GeometryCollection, MultiPoint, MultiPolygon, Polygon
from shapely.ops import unary_union

from omnigibson.object_states.aabb import AABB
from omnigibson.object_states.kinematics_mixin import KinematicsMixin
from omnigibson.object_states.object_state_base import BooleanStateMixin, RelativeObjectState
from omnigibson.object_states.on_top import OnTop
from omnigibson.utils.constants import PrimType
from omnigibson.utils.sampling_utils import raytest_batch
from omnigibson.utils.ui_utils import create_module_logger

log = create_module_logger(module_name=__name__)

# Rays are intentionally cast from a contour inside the physical silhouette.
# This avoids classifying numerical contact at the exact mesh boundary as an
# overhang while remaining usable for narrow objects such as pens.
FOOTPRINT_INSET = 0.005
BOUNDARY_SAMPLE_SPACING = 0.005
RAY_VERTICAL_MARGIN = 0.01
MAX_RAY_LENGTH = 5.0
MIN_MISS_HIT_RATIO = 0.1
_MIN_PROJECTED_TRIANGLE_AREA = 1e-10
_MIN_BOUNDARY_SAMPLES = 4


def _polygon_parts(geometry):
    """Return every polygon contained in a possibly mixed Shapely geometry."""
    if geometry is None or geometry.is_empty:
        return []
    if isinstance(geometry, Polygon):
        return [geometry]
    if isinstance(geometry, (MultiPolygon, GeometryCollection)):
        parts = []
        for child in geometry.geoms:
            parts.extend(_polygon_parts(child))
        return parts
    return []


def _union_projected_triangles(projected_triangles):
    """Build a polygonal footprint from projected ``(3, 2)`` triangles."""
    polygons = []
    for triangle in projected_triangles:
        triangle = np.asarray(triangle, dtype=float)
        if triangle.shape != (3, 2) or not np.isfinite(triangle).all():
            continue
        polygon = Polygon(triangle)
        if polygon.area > _MIN_PROJECTED_TRIANGLE_AREA:
            polygons.append(polygon)

    if not polygons:
        return GeometryCollection()

    footprint = unary_union(polygons)
    if not footprint.is_valid:
        footprint = footprint.buffer(0)
    parts = _polygon_parts(footprint)
    return unary_union(parts) if parts else GeometryCollection()


def _collision_footprint(obj, world_to_output=None):
    """Project collision geometry into the requested frame's XY plane."""
    projected_triangles = []
    primitive_footprints = []

    for link in obj.links.values():
        for mesh in link.collision_meshes.values():
            points = mesh.transform_local_points_to_world(mesh.points)
            if points is None or len(points) < 3:
                continue
            if world_to_output is not None:
                points = T.transform_points(points, world_to_output)
            points_xy = points[:, :2].detach().cpu().numpy()

            if mesh.geom_type == "Mesh" and mesh.faces is not None and len(mesh.faces) > 0:
                faces = mesh.faces.detach().cpu().numpy()
                projected_triangles.extend(points_xy[faces])
                continue

            # Primitive collision shapes are convex, so their projected point
            # hull is their exact polygonal footprint at the mesh resolution.
            hull = MultiPoint(points_xy).convex_hull
            if isinstance(hull, Polygon) and hull.area > _MIN_PROJECTED_TRIANGLE_AREA:
                primitive_footprints.append(hull)

    geometries = []
    triangle_footprint = _union_projected_triangles(projected_triangles)
    if not triangle_footprint.is_empty:
        geometries.append(triangle_footprint)
    geometries.extend(primitive_footprints)
    if not geometries:
        return GeometryCollection()

    footprint = unary_union(geometries)
    if not footprint.is_valid:
        footprint = footprint.buffer(0)
    parts = _polygon_parts(footprint)
    return unary_union(parts) if parts else GeometryCollection()


def _sample_inset_boundary(footprint, inset=FOOTPRINT_INSET, spacing=BOUNDARY_SAMPLE_SPACING):
    """Uniformly sample all rings of a footprint after an inward offset."""
    if footprint is None or footprint.is_empty or inset < 0 or spacing <= 0:
        return []

    inset_footprint = footprint.buffer(-inset) if inset else footprint
    points = []
    seen = set()
    for polygon in _polygon_parts(inset_footprint):
        rings = [polygon.exterior, *polygon.interiors]
        for ring in rings:
            length = float(ring.length)
            if length <= 0:
                continue
            count = max(_MIN_BOUNDARY_SAMPLES, int(math.ceil(length / spacing)))
            for index in range(count):
                point = ring.interpolate(length * index / count)
                xy = (float(point.x), float(point.y))
                key = (round(xy[0], 9), round(xy[1], 9))
                if key not in seen:
                    seen.add(key)
                    points.append(xy)
    return points


def _has_partial_support(target_hits):
    """Return whether misses are sufficiently frequent relative to support hits."""
    hit_count, _, ratio = _partial_support_stats(target_hits)
    if hit_count == 0:
        return False
    return ratio >= MIN_MISS_HIT_RATIO


def _partial_support_stats(target_hits):
    """Return support-hit count, miss count, and miss/hit ratio."""
    hit_count = sum(target_hits)
    miss_count = len(target_hits) - hit_count
    ratio = miss_count / hit_count if hit_count else math.inf
    return hit_count, miss_count, ratio


def _path_is_part_of_object(path, obj):
    if not path:
        return False
    path = str(path)
    root_path = str(obj.prim_path).rstrip("/")
    if path == root_path or path.startswith(f"{root_path}/"):
        return True
    return any(path == str(link.prim_path) for link in obj.links.values())


def _hit_matches_support(result, support):
    """Return whether a ray hit belongs to the target support object."""
    if not result.get("hit"):
        return False
    return (
        _path_is_part_of_object(result.get("rigidBody"), support)
        or _path_is_part_of_object(result.get("collision"), support)
    )


def _ray_hits_support(results, support):
    """Return whether any hit along a ray belongs to the target support."""
    return any(_hit_matches_support(result, support) for result in results)


class AtEdge(KinematicsMixin, RelativeObjectState, BooleanStateMixin):
    """Whether a rigid object is partially overhanging a rigid support object."""

    @classmethod
    def get_dependencies(cls):
        deps = super().get_dependencies()
        deps.add(OnTop)
        return deps

    def _set_value(self, other, new_value):
        raise NotImplementedError("AtEdge does not support set_value().")

    def _initialize(self):
        super()._initialize()
        start = time.perf_counter()
        world_to_object = th.linalg.inv(self.obj.scaled_transform)
        footprint = _collision_footprint(self.obj, world_to_output=world_to_object)
        sample_xy = _sample_inset_boundary(footprint)
        self._local_boundary_points = th.tensor(
            [[x, y, 0.0] for x, y in sample_xy],
            dtype=th.float32,
        )
        self._footprint_cache_seconds = time.perf_counter() - start
        self._world_boundary_cache_step = None
        self._world_boundary_cache_xy = None

    @property
    def footprint_cache_seconds(self):
        return self._footprint_cache_seconds

    def _get_world_boundary_xy(self):
        """Transform the cached local contour at most once per simulation step."""
        step = og.sim.current_time_step_index
        if self._world_boundary_cache_step != step:
            world_points = T.transform_points(self._local_boundary_points, self.obj.scaled_transform)
            self._world_boundary_cache_xy = world_points[:, :2]
            self._world_boundary_cache_step = step
            return self._world_boundary_cache_xy, True
        return self._world_boundary_cache_xy, False

    def _get_value(self, other):
        diagnostic = self.get_diagnostic(other)
        if diagnostic["reason"] != "ray_result":
            self._log_diagnostic(other, **diagnostic)
            return False

        is_at_edge = diagnostic["is_at_edge"]
        self._log_diagnostic(other, **diagnostic)
        return is_at_edge

    def get_diagnostic(self, other):
        """Return structured AtEdge diagnostic data for evaluator-side logging."""
        timings = {
            "on_top_seconds": 0.0,
            "world_transform_seconds": 0.0,
            "raycast_seconds": 0.0,
        }
        if self.obj.prim_type != PrimType.RIGID or other.prim_type != PrimType.RIGID:
            return {"reason": "non_rigid", "timings": timings}

        start = time.perf_counter()
        is_on_top = self.obj.states[OnTop].get_value(other)
        timings["on_top_seconds"] = time.perf_counter() - start
        if not is_on_top:
            return {"reason": "not_on_top", "timings": timings}

        start = time.perf_counter()
        sample_xy, transform_performed = self._get_world_boundary_xy()
        timings["world_transform_seconds"] = time.perf_counter() - start
        if len(sample_xy) < 2:
            return {
                "reason": "too_few_samples",
                "sample_count": len(sample_xy),
                "timings": timings,
                "world_transform_performed": transform_performed,
            }

        obj_lower, obj_upper = self.obj.states[AABB].get_value()
        other_lower, _ = other.states[AABB].get_value()
        object_bottom_z = float(obj_lower[2])
        start_z = float(obj_upper[2]) + RAY_VERTICAL_MARGIN
        natural_end_z = min(object_bottom_z, float(other_lower[2])) - RAY_VERTICAL_MARGIN
        end_z = max(natural_end_z, start_z - MAX_RAY_LENGTH)
        if start_z <= end_z:
            return {
                "reason": "invalid_ray_span",
                "timings": timings,
                "world_transform_performed": transform_performed,
            }

        starts = th.cat(
            (sample_xy, th.full((len(sample_xy), 1), start_z, device=sample_xy.device)),
            dim=1,
        )
        ends = th.cat(
            (sample_xy, th.full((len(sample_xy), 1), end_z, device=sample_xy.device)),
            dim=1,
        )
        ignore_bodies = [str(link.prim_path) for link in self.obj.links.values()]
        start = time.perf_counter()
        results = raytest_batch(
            start_points=starts,
            end_points=ends,
            only_closest=False,
            ignore_bodies=ignore_bodies,
        )
        timings["raycast_seconds"] = time.perf_counter() - start
        # Treat any target-object hit along the ray as support. This assumes
        # lower structures (for example, legs) stay within the top surface's
        # XY footprint; exceptional support models require separate handling.
        target_hits = [
            _ray_hits_support(ray_results, other)
            for ray_results in results
        ]
        hit_count, miss_count, ratio = _partial_support_stats(target_hits)
        is_at_edge = _has_partial_support(target_hits)
        return {
            "reason": "ray_result",
            "hit_count": hit_count,
            "miss_count": miss_count,
            "ratio": ratio,
            "is_at_edge": is_at_edge,
            "sample_count": len(target_hits),
            "timings": timings,
            "world_transform_performed": transform_performed,
        }

    def _log_diagnostic(
        self,
        other,
        reason,
        hit_count=None,
        miss_count=None,
        ratio=None,
        is_at_edge=False,
        sample_count=None,
        **_,
    ):
        """Log compact diagnostics so edge thresholds can be tuned from rollout logs."""
        support_name = getattr(other, "name", str(other.prim_path))
        if not hasattr(self, "_at_edge_ratio_log_cache"):
            self._at_edge_ratio_log_cache = {}

        ratio_for_cache = round(ratio, 3) if ratio is not None and math.isfinite(ratio) else ratio
        signature = (reason, hit_count, miss_count, ratio_for_cache, is_at_edge, sample_count)
        if self._at_edge_ratio_log_cache.get(support_name) == signature:
            return
        self._at_edge_ratio_log_cache[support_name] = signature

        if ratio is None:
            ratio_text = "n/a"
        else:
            ratio_text = f"{ratio:.3f}" if math.isfinite(ratio) else "inf"
        log.info(
            "AtEdge diagnostic obj=%s support=%s reason=%s samples=%s hits=%s misses=%s "
            "miss_hit_ratio=%s threshold=%.3f result=%s",
            self.obj.name,
            support_name,
            reason,
            sample_count,
            hit_count,
            miss_count,
            ratio_text,
            MIN_MISS_HIT_RATIO,
            is_at_edge,
        )
