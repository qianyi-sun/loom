"""Expected application shape carried by the protected source/image release.

Regenerate with scripts/build_application_schema_reference.py in an isolated
trusted checkout. Never replace this pin with an observation of a live database.
The installed code must itself have passed protected release admission; this
module neither authenticates its own installation nor authorizes a transfer.
"""

from dataclasses import dataclass
from typing import Literal

from loom.application_schema_inventory import ApplicationSchemaInventory


@dataclass(frozen=True, slots=True)
class ApplicationSchemaReference:
    format_version: int
    profile: str
    application_head: str
    guard_head: str
    postgres_image: str
    postgres_major: int
    object_count: int
    inventory_sha256: str


class ApplicationSchemaReferenceError(RuntimeError):
    """An observation does not match the bundled protected-release reference."""


ApplicationSchemaProfile = Literal[
    "legacy-owner",
    "sealed-owner",
    "staging-readonly-legacy-owner",
    "staging-readonly-sealed-owner",
]
ApplicationSchemaAclProfile = Literal["application-only", "staging-readonly"]


def application_schema_profile(
    *,
    ownership: Literal["legacy-owner", "sealed-owner"],
    acl_profile: ApplicationSchemaAclProfile,
) -> ApplicationSchemaProfile:
    """Select a fixed pair from trusted operation requirements, never live ACLs.

    This does not admit role attributes, membership or process authority. Those
    remain independent protected-caller requirements for either ACL profile.
    """
    if ownership not in {"legacy-owner", "sealed-owner"}:
        raise ApplicationSchemaReferenceError("application schema ownership profile is invalid")
    if acl_profile == "application-only":
        return ownership
    if acl_profile == "staging-readonly":
        return (
            "staging-readonly-legacy-owner"
            if ownership == "legacy-owner"
            else "staging-readonly-sealed-owner"
        )
    raise ApplicationSchemaReferenceError("application schema ACL profile is invalid")


def application_reference_postgres_image(*, postgres_major: int = 16) -> str:
    """Fixed independently provisioned reference images; never a live DB address."""
    if type(postgres_major) is not int or postgres_major not in {16, 17}:
        raise ApplicationSchemaReferenceError(
            "application reference PostgreSQL major is unsupported"
        )
    digest = (
        "60f4761b9035e0b8d5218f701a8c3382f641bf12b1604822574cf5be3baeb537"
        if postgres_major == 16
        else "304ab813518754228f9f792f79d6da36359b82d8ecf418096c636725f8c930ad"
    )
    return "docker.io/library/postgres@sha256:" + digest


def application_schema_reference(
    *, profile: ApplicationSchemaProfile = "legacy-owner", postgres_major: int = 16
) -> ApplicationSchemaReference:
    """Select one bundled profile, never a caller-selected digest."""
    profiles: tuple[ApplicationSchemaProfile, ...] = (
        "legacy-owner",
        "sealed-owner",
        "staging-readonly-legacy-owner",
        "staging-readonly-sealed-owner",
    )
    if profile not in profiles:
        raise ApplicationSchemaReferenceError("application schema reference profile is invalid")
    image = application_reference_postgres_image(postgres_major=postgres_major)
    digests = {
        16: (
            "ebe929a7e0aeafdc9635b8057f4e35a9548fd5ddea9a900d16a6e1f8356677cd",
            "4004bc803c1b2f34d847b2d697d08b82d4d439108281ecd9ad0bdd7dbb232cce",
            "cb1ce7d26a26c35e010da1cc023d6977736a0490444e82d737b0d7a8ac36e2d7",
            "1195111c215e4bac289e8eeac39ef0e1655aed5682ae6a8673db34f4bf9d34a4",
        ),
        17: (
            "9cefe03124a862f02045294c8cfb65090bb8360fe37dc8bb460971c903cbf937",
            "1a4e2140e730682e5edf8a8e0740b95eb8ed51fbc23bf992876f160578ff3db2",
            "0b7662d95ff0320448acffef140436a113bec487932c65b5bb59a23ae45694ba",
            "a81fb3aacc203e475b0a5496a668851a3802b707c3e2c7bd23b0d0095748b423",
        ),
    }
    return ApplicationSchemaReference(
        format_version=1,
        profile=profile,
        application_head="0142",
        guard_head="guard_0033",
        postgres_image=image,
        postgres_major=postgres_major,
        object_count=7227 if profile in {"legacy-owner", "staging-readonly-legacy-owner"} else 7229,
        inventory_sha256=digests[postgres_major][profiles.index(profile)],
    )


def require_application_schema_reference(
    observed: ApplicationSchemaInventory, *, profile: ApplicationSchemaProfile = "legacy-owner"
) -> None:
    """Compare only; caller still owns trusted role binding, quiescence and locks."""
    if type(observed.postgres_major) is not int or observed.postgres_major not in {16, 17}:
        raise ApplicationSchemaReferenceError("application schema differs from trusted reference")
    expected = application_schema_reference(profile=profile, postgres_major=observed.postgres_major)
    if (
        observed.postgres_major != expected.postgres_major
        or len(observed.objects) != expected.object_count
        or observed.sha256 != expected.inventory_sha256
    ):
        raise ApplicationSchemaReferenceError("application schema differs from trusted reference")
