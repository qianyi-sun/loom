"""Explicit active runtime construction; unavailable admission permits recovery."""

from __future__ import annotations

import re
from collections.abc import Mapping
from contextlib import AsyncExitStack
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from time import monotonic
from types import MappingProxyType
from uuid import UUID

from loom.personal_dev_capacity import (
    CapacityManagerPersonalDevProjector,
    PersonalDevCapacityManagerConnection,
    PersonalDevCapacityProjectionError,
)
from loom.personal_dev_capacity_runtime import PersonalDevCapacityStatusReader
from loom.personal_dev_membership_admission import (
    PersonalDevMembershipAcceptanceBindingV1,
    PersonalDevMembershipAdmissionError,
    PersonalDevMembershipAdmissionInterlock,
    parse_membership_acceptance_binding,
)
from loom.personal_dev_membership_client import CapacityManagerPersonalDevMembershipClient
from loom.personal_dev_membership_successor import (
    PersonalDevMembershipSuccessorBindingV1,
    parse_membership_successor_plan,
)
from loom_capacity_agent.client import (
    DemandReporterTLSFiles,
    read_owner_only_bearer_token,
    read_owner_only_bytes,
)
from loom_capacity_manager.contracts import MAX_CONTRACT_BYTES
from loom_service.config import LoomServiceSettings
from loom_service.personal_dev_lifecycle import (
    PersonalDevCapacityRuntime,
    PersonalDevMembershipRuntime,
    build_personal_dev_capacity_installation,
)


@dataclass(frozen=True, slots=True)
class PersonalDevMembershipServiceAdmission:
    """Authenticate the observer, then check the full delegated execution fence.

    The observer's legacy status object proves its identity only. It is never
    substituted for the complete execution checkpoint or manager mutation CAS.
    """

    interlock: PersonalDevMembershipAdmissionInterlock
    observer: CapacityManagerPersonalDevProjector
    observer_principal_id: str

    async def assert_admission_ready(self, *, now: datetime) -> None:
        started = monotonic()
        if (
            now.tzinfo is None
            or now.utcoffset() is None
            or not self.interlock.binding.started_at <= now < self.interlock.binding.expires_at
        ):
            raise PersonalDevMembershipAdmissionError("membership acceptance window is not open")
        try:
            identity = await self.observer.current_manager_binding()
        except PersonalDevCapacityProjectionError as exc:
            raise PersonalDevMembershipAdmissionError("membership observer unavailable") from exc
        if (
            identity.observer_principal_id != self.observer_principal_id
            or identity.authority_incarnation != self.interlock.binding.execution.authority_incarnation
        ):
            raise PersonalDevMembershipAdmissionError("membership observer identity changed")
        await self.interlock.assert_admission_ready(
            now=now + timedelta(seconds=monotonic() - started)
        )


def load_membership_successor_bindings(
    settings: LoomServiceSettings,
    authority: PersonalDevMembershipAcceptanceBindingV1,
) -> Mapping[UUID, PersonalDevMembershipSuccessorBindingV1]:
    filename = settings.personal_dev_membership_successor_plan_file
    digest = settings.personal_dev_membership_successor_plan_sha256
    if not filename and not digest:
        return MappingProxyType({})
    if not filename or not digest or settings.personal_dev_runtime_mode != "membership-v1":
        raise ValueError("successor plan requires paired file/digest in membership mode")
    return parse_membership_successor_plan(
        read_owner_only_bytes(Path(filename), max_bytes=MAX_CONTRACT_BYTES),
        expected_plan_sha256=digest, current_authority=authority,
    )


async def build_personal_dev_membership_runtime(
    settings: LoomServiceSettings,
) -> PersonalDevCapacityRuntime:
    """No network admission at startup: a valid config can run recovery-only."""
    if (
        settings.personal_dev_runtime_mode != "membership-v1"
        or not settings.dev_instances_enabled
        or not settings.personal_dev_builder_enabled
        or settings.personal_dev_acceptance_binding_json != "{}"
        or settings.personal_dev_acceptance_plan_sha256
        or settings.personal_dev_operational_binding_json != "{}"
        or settings.personal_dev_operational_plan_sha256
    ):
        raise RuntimeError("personal-dev membership mode or mixed bindings are invalid")
    try:
        binding = parse_membership_acceptance_binding(
            settings.personal_dev_membership_binding_json,
            expected_plan_sha256=settings.personal_dev_membership_plan_sha256,
        )
        successor_bindings = load_membership_successor_bindings(settings, binding)
        observer_id = settings.personal_dev_membership_observer_principal_id
        if (
            re.fullmatch(r"[a-z0-9-]{1,128}", observer_id) is None
            or observer_id == binding.management_principal_id
        ):
            raise ValueError("observer must be a separate current principal")
    except (OSError, TypeError, ValueError) as exc:
        raise RuntimeError("personal-dev membership binding is invalid") from exc

    installer, kubectl, connection = build_personal_dev_capacity_installation(
        settings, membership_execution=binding.execution
    )
    observer_connection = PersonalDevCapacityManagerConnection(
        manager_origin=connection.manager_origin,
        bearer_token_file=settings.personal_dev_capacity_observer_bearer_token_file,
        tls_files=DemandReporterTLSFiles(
            ca_file=settings.personal_dev_capacity_observer_ca_file,
            certificate_file=settings.personal_dev_capacity_observer_certificate_file,
            private_key_file=settings.personal_dev_capacity_observer_private_key_file,
        ),
    )
    try:
        observer_token = read_owner_only_bearer_token(observer_connection.bearer_token_file)
        if observer_token == read_owner_only_bearer_token(connection.bearer_token_file):
            raise ValueError("observer and delegate credentials must differ")
        for observer_path, other_paths in (
            (settings.personal_dev_capacity_observer_certificate_file, (
                settings.personal_dev_capacity_lifecycle_certificate_file,
                settings.personal_dev_capacity_certificate_file,
            )),
            (settings.personal_dev_capacity_observer_private_key_file, (
                settings.personal_dev_capacity_lifecycle_private_key_file,
                settings.personal_dev_capacity_private_key_file,
            )),
        ):
            observed = read_owner_only_bytes(observer_path)
            if any(observed == read_owner_only_bytes(path) for path in other_paths):
                raise ValueError("observer transport identity must remain separate")
        async with AsyncExitStack() as cleanup:
            client = CapacityManagerPersonalDevMembershipClient.from_files(connection)
            cleanup.push_async_callback(client.aclose)
            observer = CapacityManagerPersonalDevMembershipClient.from_files(observer_connection)
            cleanup.push_async_callback(observer.aclose)
            projector = CapacityManagerPersonalDevProjector.from_files(observer_connection)
            cleanup.push_async_callback(projector.aclose)
            admission = PersonalDevMembershipServiceAdmission(
                interlock=PersonalDevMembershipAdmissionInterlock(binding=binding, client=client),
                observer=projector,
                observer_principal_id=observer_id,
            )
            runtime = PersonalDevCapacityRuntime(
                installer=installer,
                projector=projector,
                status_reader=PersonalDevCapacityStatusReader(
                    kubectl=kubectl,
                    database_admin_url=str(settings.dev_instance_database_admin_url),
                    projector=projector,
                ),
                acceptance_interlock=None,
                operational_interlock=None,
                membership=PersonalDevMembershipRuntime(
                    installer=installer, client=client, observer=observer,
                    admission=admission, management_principal_id=binding.management_principal_id,
                    successor_bindings=successor_bindings,
                ),
                owned_membership_clients=(client, observer),
            )
            cleanup.pop_all()
            return runtime
    except (OSError, TypeError, ValueError) as exc:
        raise RuntimeError("personal-dev membership credentials are invalid") from exc
