"""Exact portable files for activating one already-prepared pool controller."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import PurePosixPath
from types import MappingProxyType
from uuid import UUID

from loom_capacity_executor.runtime import ActivationRuntimeDocumentV2
from loom_capacity_manager.executable_contracts import (
    canonical_executable_bytes,
    retained_prepared_activation_matches,
)
from loom_cli.capacity_control_plane import (
    CapacityPoolExecutorProfile,
    render_capacity_pool_executor_active_config,
    render_capacity_pool_executor_active_service_environment,
    render_capacity_pool_executor_configs,
    render_capacity_pool_executor_service_environment,
    render_capacity_pool_inventory_policies,
)

from .protected_capacity_execution_preparation_component import (
    PreparedControllerRequest,
    prepared_executor_profile_sha256,
)
from .protected_controller_admission import ADMISSION_CA_PATH, ControllerAdmissionBundle

_MAX_BYTES = 4 * 1024 * 1024


def _canonical(value: object) -> bytes:
    return (
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)
        + "\n"
    ).encode("ascii")


@dataclass(frozen=True, slots=True)
class ActiveControllerRequest:
    """A stable operation over derived files; it is not manager activation authority."""

    operation_id: UUID
    prepared: PreparedControllerRequest
    profile: CapacityPoolExecutorProfile
    document: ActivationRuntimeDocumentV2
    admission: ControllerAdmissionBundle | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        if (
            not isinstance(self.operation_id, UUID)
            or self.operation_id.int == 0
            or not isinstance(self.prepared, PreparedControllerRequest)
            or not isinstance(self.profile, CapacityPoolExecutorProfile)
            or not isinstance(self.document, ActivationRuntimeDocumentV2)
        ):
            raise ValueError("active controller request identity is invalid")
        # Revalidate model copies at this authority boundary without target I/O.
        profile = CapacityPoolExecutorProfile.model_validate_json(self.profile.model_dump_json())
        document = ActivationRuntimeDocumentV2.model_validate_json(self.document.model_dump_json())
        prepared = PreparedControllerRequest.from_bytes(self.prepared.to_bytes())
        pool = next((pool for pool in profile.pools if pool.pool_id == self.pool_id), None)
        if (
            pool != prepared.prerequisite.binding
            or profile.executor_image != prepared.prerequisite.image
            or profile.service_user != prepared.prerequisite.service_user
            or prepared.profile_sha256 != prepared_executor_profile_sha256(profile)
            or not retained_prepared_activation_matches(prepared.execution, document.execution)
            or document.pool_id != self.pool_id
        ):
            raise ValueError("active controller differs from its installed preparation")
        assert pool is not None
        state = PurePosixPath(pool.state_directory)
        if document.handoff_directory != str(
            state / "handoff"
        ) or document.admission_directory != str(state / "admission"):
            raise ValueError("active controller runtime directories differ from its private state")
        expected_prepared = {
            pool.config_file: render_capacity_pool_executor_configs(profile)[self.pool_id].encode(
                "ascii"
            ),
            str(
                PurePosixPath(pool.config_file).with_name(f"{self.pool_id}-inventory-policy.json")
            ): render_capacity_pool_inventory_policies(profile)[self.pool_id].encode("ascii"),
            "/etc/loom-capacity-executor/service.env": render_capacity_pool_executor_service_environment(
                profile, self.pool_id
            ).encode("ascii"),
        }
        if dict(prepared.files) != expected_prepared:
            raise ValueError("active controller prepared files differ from the bound profile")
        if self.admission is not None:
            admission = ControllerAdmissionBundle.from_dict(self.admission.to_dict())
            admission.files(document)
        # These renderers validate all execution, profile, manifest and Slurm bindings.
        _ = self.files
        if len(self.to_bytes()) > _MAX_BYTES:
            raise ValueError("active controller request is too large")

    @property
    def pool_id(self) -> str:
        return self.prepared.pool_id

    @property
    def transport_authority_sha256(self) -> str:
        return self.prepared.transport_authority_sha256

    @property
    def files(self) -> Mapping[str, bytes]:
        config = PurePosixPath(self.prepared.prerequisite.binding.config_file)
        return MappingProxyType(
            {
                str(
                    config.with_name(f"{self.pool_id}-active.json")
                ): render_capacity_pool_executor_active_config(
                    self.profile, self.pool_id, self.document
                ).encode("ascii"),
                str(
                    config.with_name(f"{self.pool_id}-activation-runtime.json")
                ): canonical_executable_bytes(self.document),
                "/etc/loom-capacity-executor/active-service.env": (render_capacity_pool_executor_active_service_environment(
                    self.profile, self.pool_id, self.document
                ) + (f"PGSSLROOTCERT={ADMISSION_CA_PATH}\n" if self.admission is not None else "")).encode("ascii"),
            }
        )

    @property
    def request_sha256(self) -> str:
        return hashlib.sha256(self.to_bytes()).hexdigest()

    def to_bytes(self) -> bytes:
        return _canonical(
            {
                "schema_version": 1,
                "operation_id": str(self.operation_id),
                "prepared": json.loads(self.prepared.to_bytes()),
                "profile": self.profile.model_dump(mode="json"),
                "document": self.document.model_dump(mode="json"),
                **({"admission": self.admission.to_dict()} if self.admission is not None else {}),
            }
        )

    @classmethod
    def from_bytes(cls, payload: bytes) -> ActiveControllerRequest:
        if not isinstance(payload, bytes) or not 0 < len(payload) <= _MAX_BYTES:
            raise ValueError("active controller request is not bounded bytes")
        try:
            value = json.loads(payload)
            if (
                not isinstance(value, dict)
                or set(value) not in ({"schema_version", "operation_id", "prepared", "profile", "document"},
                    {"schema_version", "operation_id", "prepared", "profile", "document", "admission"})
                or not isinstance(value["operation_id"], str)
                or type(value["schema_version"]) is not int
                or value["schema_version"] != 1
                or _canonical(value) != payload
            ):
                raise ValueError("active controller request is not canonical")
            result = cls(
                operation_id=UUID(value["operation_id"]),
                prepared=PreparedControllerRequest.from_bytes(_canonical(value["prepared"])),
                profile=CapacityPoolExecutorProfile.model_validate_json(
                    json.dumps(value["profile"])
                ),
                document=ActivationRuntimeDocumentV2.model_validate_json(
                    json.dumps(value["document"])
                ),
                admission=ControllerAdmissionBundle.from_dict(value["admission"]) if "admission" in value else None,
            )
        except (TypeError, KeyError, UnicodeError) as exc:
            raise ValueError("active controller request is invalid") from exc
        if result.to_bytes() != payload:
            raise ValueError("active controller request normalization changed")
        return result


@dataclass(frozen=True, slots=True)
class ActiveControllerEvidence:
    """Exact installed files and unit states for one retained activation operation."""

    operation_id: UUID
    pool_id: str
    request_sha256: str
    transport_authority_sha256: str
    file_sha256: Mapping[str, str]
    unit_active_state: Mapping[str, str]
    unit_file_state: Mapping[str, str]

    def __post_init__(self) -> None:
        prefix = "loom-capacity-pool-executor"
        units = {
            prefix + suffix
            for suffix in (
                ".service",
                "-prepared.service",
                "-prepared.timer",
                "-active.service",
                "-active.timer",
            )
        }
        paths = {
            f"/etc/loom-capacity-executor/{self.pool_id}-active.json",
            f"/etc/loom-capacity-executor/{self.pool_id}-activation-runtime.json",
            "/etc/loom-capacity-executor/active-service.env",
        }
        if (
            not isinstance(self.operation_id, UUID)
            or self.operation_id.int == 0
            or self.pool_id not in {"oldlab", "gb10"}
            or set(self.file_sha256) != paths
            or set(self.unit_active_state) != units
            or set(self.unit_file_state) != units
        ):
            raise ValueError("active controller evidence identity is invalid")
        for digest in (
            self.request_sha256,
            self.transport_authority_sha256,
            *self.file_sha256.values(),
        ):
            if (
                not isinstance(digest, str)
                or len(digest) != 64
                or any(c not in "0123456789abcdef" for c in digest)
                or digest == "0" * 64
            ):
                raise ValueError("active controller evidence digest is invalid")
        for unit in units:
            pair = (self.unit_active_state[unit], self.unit_file_state[unit])
            if unit.endswith("-active.timer"):
                allowed = {("inactive", "disabled"), ("inactive", "enabled"), ("active", "enabled")}
            elif unit.endswith("-active.service"):
                allowed = {("inactive", "static"), ("activating", "static"), ("active", "static")}
            else:
                allowed = {("inactive", "disabled" if unit.endswith(".timer") else "static")}
            if pair not in allowed:
                raise ValueError("active controller unit evidence is invalid")
        if (
            self.state == "staged"
            and self.unit_active_state[prefix + "-active.service"] != "inactive"
        ):
            raise ValueError("active controller service runs without its timer authority")
        for name in ("file_sha256", "unit_active_state", "unit_file_state"):
            object.__setattr__(
                self, name, MappingProxyType(dict(sorted(getattr(self, name).items())))
            )

    @property
    def state(self) -> str:
        timer = "loom-capacity-pool-executor-active.timer"
        if self.unit_active_state[timer] == "active":
            return "active"
        return "enabling" if self.unit_file_state[timer] == "enabled" else "staged"

    def to_bytes(self) -> bytes:
        return _canonical(
            {
                "schema_version": 1,
                "operation_id": str(self.operation_id),
                "pool_id": self.pool_id,
                "request_sha256": self.request_sha256,
                "transport_authority_sha256": self.transport_authority_sha256,
                "file_sha256": dict(self.file_sha256),
                "unit_active_state": dict(self.unit_active_state),
                "unit_file_state": dict(self.unit_file_state),
                "state": self.state,
            }
        )

    @classmethod
    def from_bytes(cls, payload: bytes) -> ActiveControllerEvidence:
        if not isinstance(payload, bytes) or not 0 < len(payload) <= 65536:
            raise ValueError("active controller evidence is not bounded bytes")
        try:
            value = json.loads(payload)
            fields = {
                "operation_id",
                "pool_id",
                "request_sha256",
                "transport_authority_sha256",
                "file_sha256",
                "unit_active_state",
                "unit_file_state",
            }
            if (
                not isinstance(value, dict)
                or set(value) != fields | {"schema_version", "state"}
                or type(value["schema_version"]) is not int
                or value["schema_version"] != 1
                or not isinstance(value["operation_id"], str)
            ):
                raise ValueError("active controller evidence schema is invalid")
            result = cls(
                **(
                    {key: value[key] for key in fields}
                    | {"operation_id": UUID(value["operation_id"])}
                )
            )
            if result.to_bytes() != payload:
                raise ValueError("active controller evidence is not canonical")
            return result
        except (TypeError, KeyError, UnicodeError) as exc:
            raise ValueError("active controller evidence is invalid") from exc
