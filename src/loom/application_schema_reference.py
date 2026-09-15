"""Expected application shape carried by the protected source/image release.

Regenerate with scripts/build_application_schema_reference.py in an isolated
trusted checkout. Never replace this pin with an observation of a live database.
The installed code must itself have passed protected release admission; this
module neither authenticates its own installation nor authorizes a transfer.
"""

from dataclasses import dataclass
from typing import Literal

from loom.application_database_connection import ApplicationDatabaseConnection
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
    "cnpg-staging-legacy-owner",
    "cnpg-staging-sealed-owner",
]
ApplicationSchemaRevision = Literal["0147/guard_0035", "0142/guard_0035", "0134/guard_0030"]

ApplicationSchemaAclProfile = Literal["application-only", "staging-readonly", "cnpg-staging"]


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
    if acl_profile == "cnpg-staging":
        return "cnpg-staging-legacy-owner" if ownership == "legacy-owner" else "cnpg-staging-sealed-owner"
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
    *, profile: ApplicationSchemaProfile = "legacy-owner", postgres_major: int = 16,
    revision: ApplicationSchemaRevision = "0147/guard_0035",
) -> ApplicationSchemaReference:
    """Select one bundled profile, never a caller-selected digest."""
    profiles: tuple[ApplicationSchemaProfile, ...] = (
        "legacy-owner",
        "sealed-owner",
        "staging-readonly-legacy-owner",
        "staging-readonly-sealed-owner",
        "cnpg-staging-legacy-owner",
        "cnpg-staging-sealed-owner",
    )
    if profile not in profiles:
        raise ApplicationSchemaReferenceError("application schema reference profile is invalid")
    application_head, guard_head = application_schema_revisions(revision)
    image = application_reference_postgres_image(postgres_major=postgres_major)
    digests = {
        16: (
            "ebe929a7e0aeafdc9635b8057f4e35a9548fd5ddea9a900d16a6e1f8356677cd",
            "4004bc803c1b2f34d847b2d697d08b82d4d439108281ecd9ad0bdd7dbb232cce",
            "cb1ce7d26a26c35e010da1cc023d6977736a0490444e82d737b0d7a8ac36e2d7",
            "1195111c215e4bac289e8eeac39ef0e1655aed5682ae6a8673db34f4bf9d34a4",
            "2067fdacce89b23a408aaace7073481ac88da475da902296df63419db4cf5928",
            "531ab6160258f5c6c6d18f11c7bb4b4c3c71c1426b4000e1711a21558fbba0f0",
        ),
        17: (
            "9cefe03124a862f02045294c8cfb65090bb8360fe37dc8bb460971c903cbf937",
            "1a4e2140e730682e5edf8a8e0740b95eb8ed51fbc23bf992876f160578ff3db2",
            "0b7662d95ff0320448acffef140436a113bec487932c65b5bb59a23ae45694ba",
            "a81fb3aacc203e475b0a5496a668851a3802b707c3e2c7bd23b0d0095748b423",
            "884d94fbae7e3745aee947e50861b5557489d6907e3b112c47e8648fc591fb84",
            "a4457cf147a7d554b8f81aae84f29d167d8ba95c8387154ac3af4e7f8b2f6958",
        ),
    }
    if revision == "0147/guard_0035":
        digests = {
            16: (
                "cd9df3002c56fa6906adcaca9ffb45a8b8f240a8d8de4c89790389d1eb193c1a",
                "5be91422b4e7a2ab2c59415bbf2edc6ab5447c29bf62f077a83357a5722e0dc5",
                "187d87b0033f64fbe28b6bc23be1aee4954565fa41eb3fbd946061f3181362a8",
                "a8faa62c6a387f257fe80a169c9b3549cbbfe66c72657f66a91a0bbdd159cd1d",
                "fc2a7f1f58033d874a25c572640fc41c60902577bcf7b3966df223a12433c8cf",
                "c983ce9b23190acc6c1bd961f8321015e1f7c503a7cfd19f47accfddd66af69c",
            ),
            17: (
                "b356ab6e6d5863074fd9d4b17ff39873d1394bac1302a416528fd91f6f9ce3cb",
                "b1e4729946d5ceb2bdb79d3d54aab358586396c161b1367a36f16145af510859",
                "40e2aee16d15d014e2f4b81136a4beca2abd74a3efc2750822e1bdf8a5fe2620",
                "e9a095362c1ed2d4f5d42ea588e760c79879833b78db166f8435c283d3d469f9",
                "b4dae7583feaf2aaeaece1286266169e3460474639ecc431abf82e045c557b19",
                "a0d0fe934b3b90fe17ec0813da52727377782b883410e2139fc9f829c973782b",
            ),
        }
    if revision == "0134/guard_0030":
        digests = {
            16: (
                "a2c2a2e9824cf4725c419b8d89a7048cbcc1442adb204568883a472beb7b0e88",
                "a1a2c41eef3f1ca21bc30df5ac678ad938f89afcd5b4ac0a548d7faa9d1a603c",
                "57460e2347b17e9ccac1e5dab948b5c972451fc263dadc397c0423ade9c0a778",
                "2d11dacc37b5f4d9c1d43909b460ea97e35abb5cfa8057bade5f428801a7940f",
                "53ffe9fd252bb1d9721d824c1daf8ab37d6a68331a5ba3676ab2ffccfe462aca",
                "c93f6308f981abdbb1367f40304b98baabb29a6c5aecc41a8ba5bc179dfd206e",
            ),
            17: (
                "70d58c2b86cdf7f468a54199b49bdb67cfad15f35f153cf67ea561b017bee44c",
                "5328eb98a654d63a000d857686661823ef773ab199b301791d5b8098dbf365be",
                "5e2d78ad51c96a807f1cad34c766724ee0010583f23db8dd569a1af0a508e3d9",
                "6cc8d99cd72c20fc0e16f81a4db496f1d0aa791ef99efd0e3f667d101f2a975b",
                "8285ace9fc884225975fffbe835e322fbd439e3ce7b2a99c1510fd4d3b7f9122",
                "962bc27cd4e1e0bbf8c33909d69322355c184705953a4b6246c8f2347bf5b2b4",
            ),
        }
    return ApplicationSchemaReference(
        format_version=1,
        profile=profile,
        application_head=application_head,
        guard_head=guard_head,
        postgres_image=image,
        postgres_major=postgres_major,
        object_count={"0147/guard_0035": 7232, "0142/guard_0035": 7227, "0134/guard_0030": 6736}[revision]
        + (0 if profile in {"legacy-owner", "staging-readonly-legacy-owner", "cnpg-staging-legacy-owner"} else 2),
        inventory_sha256=digests[postgres_major][profiles.index(profile)],
    )


def require_application_schema_reference(
    observed: ApplicationSchemaInventory, *, profile: ApplicationSchemaProfile = "legacy-owner",
    revision: ApplicationSchemaRevision = "0147/guard_0035",
) -> None:
    """Compare only; caller still owns trusted role binding, quiescence and locks."""
    if type(observed.postgres_major) is not int or observed.postgres_major not in {16, 17}:
        raise ApplicationSchemaReferenceError("application schema differs from trusted reference")
    expected = application_schema_reference(profile=profile, postgres_major=observed.postgres_major, revision=revision)
    if (
        observed.postgres_major != expected.postgres_major
        or len(observed.objects) != expected.object_count
        or observed.sha256 != expected.inventory_sha256
    ):
        raise ApplicationSchemaReferenceError("application schema differs from trusted reference")


def application_schema_revisions(revision: ApplicationSchemaRevision) -> tuple[str, str]:
    """Only reviewed migration pairs can select a reference recipe."""
    if revision == "0147/guard_0035":
        return "0147", "guard_0035"
    if revision == "0142/guard_0035":
        return "0142", "guard_0035"
    if revision == "0134/guard_0030":
        return "0134", "guard_0030"
    raise ApplicationSchemaReferenceError("application schema reference revision is unsupported")


def application_schema_revision(
    *, public_revision: str | None, guard_revision: str | None,
) -> ApplicationSchemaRevision:
    """Select using a protected original checkpoint, never a new live observation."""
    if (public_revision, guard_revision) == ("0147", "guard_0035"):
        return "0147/guard_0035"
    if (public_revision, guard_revision) == ("0142", "guard_0035"):
        return "0142/guard_0035"
    if (public_revision, guard_revision) == ("0134", "guard_0030"):
        return "0134/guard_0030"
    raise ApplicationSchemaReferenceError("application schema reference revision is unsupported")


def require_application_migration_revisions(
    connection: ApplicationDatabaseConnection, *, revision: ApplicationSchemaRevision,
) -> None:
    """Read markers only after the caller admits their catalog definitions.

    Caller retains its transaction, exact schema locks and private guard admission.
    Catalog inventory alone cannot check migration-table contents.
    """
    application_head, guard_head = application_schema_revisions(revision)
    if (
        connection.execute("SELECT version_num FROM public.alembic_version LIMIT 2").fetchall() != [(application_head,)]
        or connection.execute("SELECT version_num FROM loom_capacity_guard.capacity_guard_alembic_version LIMIT 2").fetchall() != [(guard_head,)]
    ):
        raise ApplicationSchemaReferenceError("application or guard migration revision changed")
