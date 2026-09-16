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
    "cnpg-staging-executor-admission",
]
ApplicationSchemaRevision = Literal["0148/guard_0036", "0149/guard_0035", "0148/guard_0035", "0147/guard_0036", "0147/guard_0035", "0142/guard_0035", "0134/guard_0030"]

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
    revision: ApplicationSchemaRevision = "0148/guard_0036",
) -> ApplicationSchemaReference:
    """Select one bundled profile, never a caller-selected digest."""
    if profile == "cnpg-staging-executor-admission":
        if revision not in {"0148/guard_0036", "0147/guard_0036"}:
            raise ApplicationSchemaReferenceError("executor admission reference revision is unsupported")
        image = application_reference_postgres_image(postgres_major=postgres_major)
        admission_digests = {
            "0147/guard_0036": {
                16: "03684efabced805259ebd75fbc9d34966414cbb24e2cd097c3942543f848bdc1",
                17: "1c2621a55dbdee330db9bce9e63e303a786f4824c1b83a191eb70594ce137174",
            },
            "0148/guard_0036": {
                16: "c3813c47394f8f30109f874d50b2a7465f0e44e3a82af9feb08154474c53e0d8",
                17: "c86186641a0d92b902e20f6bb030176a5dfe278647b5b053d6ecf6d65b231fd9",
            },
        }
        application_head, guard_head = application_schema_revisions(revision)
        return ApplicationSchemaReference(
            format_version=1, profile=profile, application_head=application_head, guard_head=guard_head,
            postgres_image=image, postgres_major=postgres_major,
            object_count=7321 if revision == "0148/guard_0036" else 7234,
            inventory_sha256=admission_digests[revision][postgres_major],
        )
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
    if revision == "0149/guard_0035":
        digests = {
            16: (
                "0136bb0a6520c68c77f8d307ff5a91c2952b2fedc344d0a8eee68aefdf121d42",
                "a3eff5e5f5014ba576e6362d7d90d438d002045d8ce45a941d53d2009991dea3",
                "f956d1831a169bcd1db8b48edb7a284361dd23cf0a6184c7e0bc05fff3627d91",
                "0df0e5215518b233e6641fcd7c4fc988cd0c8105cc650b9c432cc6c2baf7ed21",
                "3b12cf9114a6b57b84b22349592acbff4d897a13c859a0b67d90fd8ee79dfaca",
                "9cc238cbf280ab5a8ae7bee4f553da8e2fb44b430a721fc9f64951d0970163f3",
            ),
            17: (
                "fed7506e690ef9c712209361372910411cd39bf2ea204b0fcadb2f230abf433b",
                "d0e71bc2977350c5a6014346519db7f935ec41fade83b99cf16fc2879dfc63c3",
                "f50223042f079e2d8db3bb9fcde3ce43f29a4b60a0abee35f9e7f18e2a1e59bd",
                "d4893601324ba1b1d1ab7a9ae625d9c2eb20a01e4d95bce0502c9b2bc85b154d",
                "60399de17cdaa3fbe7b47ee90629c5d0951419a221a09af657948292cc355073",
                "221eb0e76283f152330103418fe00201e0c91318874acd329465b6c89eca5efa",
            ),
        }
    if revision == "0148/guard_0035":
        digests = {
            16: (
                "715c86c712d51b7d9df1884894dd8844147ea22ca57eb8948f5205e7bd5e8af7",
                "3acd63531654cc3e2838a80e0310b0599782c3b1b1b97fe5bdc772b56bcbef89",
                "02350ad6e3a9d329af15c0318ec000a31f7277b93bc7627826fa2d277543bec1",
                "f2aec22201d949fef464b2bf2b13110c442cd675073bfa1f9d83f499933985ed",
                "1f489c96e79cc9913f7f5425217338f2381587b0869be1ec5fc01222ae713dbf",
                "2f6ee33de89fe2d381ff16ef2a14b87881cc12be63f0325374d9e01f201402d5",
            ),
            17: (
                "08fa6337f448ecc40c438fb1f0fdbe6587a10e50909e037a2c5cf877e4515eec",
                "85a5434b00240a9defb25258027841a5be7b0f680068b0e0f555ff654d7a9e98",
                "946abb6e13217df38e69481db21a0af2c6be7a0610cb5e290fa662b82ce611c9",
                "6e12a995d18c343b657ec74eedf8495ae8cbf923debcee7ca3cd8cd1eb2b1568",
                "d73dbe9514bf0a1765f4eb21eab595ed1d94ffdc6f1f3a4a6de4ee8c0d14d67f",
                "455997397e451a262e3462c12e0b0072c410c99789a158ea2a19b4784ca9b630",
            ),
        }
    # Retained guard_0035 provisioning and its original public grants.
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
    # Freshly provisioned state/output owner grants; historical pins stay separate.
    if revision == "0147/guard_0036":
        digests = {
            16: (
                "cf29cf8051d65317e4fab59ff3d957418c47f4fd3ec983708ed51cc9ddd08f34",
                "780c0e9e9b6b387f84692c31ed5e7a15f6e7388f71f2116123494a75a986874b",
                "25164fc4a3740822fb95d3648ffb0897fe6f9958aa9a8d2ae398c717ffe44cab",
                "0bd1951fae8a22dc6232f9fbb952d2f761d2ac9e823b6eab446d21a5ce01ea34",
                "1d5ada2d1e0962be4e9f5e81859cb50696d7de92e69cdd9b2b25db648894425d",
                "ac26ba9a46016d344214576f8e61002950c69a21af962760027ec21a704b6b19",
            ),
            17: (
                "350eceea485c6a4b3d3ed0afeae1aae0a556062164412e1938829c74a1c4b1fa",
                "cce22f2de1f56e68dec179ddb5bbc10694b5d18f8d136bf23cc9e8a784980a1e",
                "003497fcdf228c6d409f563a3f05fa034a043450a9ce3d759f98fb3a33827767",
                "e7cb28597f4c0fda8d0a94536a52e6d57518febe01c659972f4d8ebe0f4c672c",
                "a588be6346cf38424dc0e8c25fd23c0384e5793540e5cd26ebc537a5bee16953",
                "7f8d454022a2ea15032f37b2eff293216e6c9b255946ea1d4db5b3ef0ea74322",
            ),
        }
    # Reviewed execution revision/start journal, independently provisioned on both majors.
    if revision == "0148/guard_0036":
        digests = {
            16: (
                "25751c5c312e82e66425f159e6bd28a6aec942479141531e3cbcbc237bea7af3",
                "3b27b4dcf93c9faafe7b9a3748fdbf087b720d0df7fc0c1ea823173c4f979261",
                "7615d5bf8cdc07e05f33fba390560247bc23ee52c4b9aeeb834b5cca761dfcf8",
                "c34b9805801d848300c4ac84d981e00d091b8b43e182cd9bbd2f00d8fabd3477",
                "95126b4bf28c378ffe4ca7f8679558b25c053c6b058d6156eef1951d6da310f4",
                "5c417576312d9458955328917330e251dc767a76a780dc3bdf0ed5f964ffba40",
            ),
            17: (
                "f1425a433bf1b3de1872a32dc9e8f7d09b0706fdcda67406f21c86bfe4af43de",
                "a11014bc75572d20f032e83cb9716c9e571d451f48d89fd93c22af7980c0c1ed",
                "006db1667785f9df06865c4cc6c06aab684d97a2ca0b0bc2b4f8795fe887c069",
                "abee76baa8e43634a9c919a2ce42245feabb1fff3e437903744acc8989dfd23f",
                "9f62ad35a906970bdb9d6a9e7e7a389fb97c8811248b95775ad118a8a6039c9e",
                "081e5bd4ac8d6700c5a703121ac7ffde8c8d76088ca5907c0192e005dd18b12d",
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
        object_count={"0149/guard_0035": 7353, "0148/guard_0036": 7319, "0148/guard_0035": 7319, "0147/guard_0036": 7232, "0147/guard_0035": 7232, "0142/guard_0035": 7227, "0134/guard_0030": 6736}[revision]
        + (0 if profile in {"legacy-owner", "staging-readonly-legacy-owner", "cnpg-staging-legacy-owner"} else 2),
        inventory_sha256=digests[postgres_major][profiles.index(profile)],
    )


def require_application_schema_reference(
    observed: ApplicationSchemaInventory, *, profile: ApplicationSchemaProfile = "legacy-owner",
    revision: ApplicationSchemaRevision = "0148/guard_0036",
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
    if revision == "0149/guard_0035":
        return "0149", "guard_0035"
    if revision == "0148/guard_0036":
        return "0148", "guard_0036"
    if revision == "0148/guard_0035":
        return "0148", "guard_0035"
    if revision == "0147/guard_0036":
        return "0147", "guard_0036"
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
    if (public_revision, guard_revision) == ("0149", "guard_0035"):
        return "0149/guard_0035"
    if (public_revision, guard_revision) == ("0148", "guard_0036"):
        return "0148/guard_0036"
    if (public_revision, guard_revision) == ("0148", "guard_0035"):
        return "0148/guard_0035"
    if (public_revision, guard_revision) == ("0147", "guard_0036"):
        return "0147/guard_0036"
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
