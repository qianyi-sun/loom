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
ApplicationSchemaRevision = Literal["0152/guard_0035", "0151/guard_0035", "0150/guard_0035", "0149/guard_0035", "0148/guard_0035", "0147/guard_0035", "0142/guard_0035", "0134/guard_0030"]

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
    revision: ApplicationSchemaRevision = "0152/guard_0035",
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
    if revision == "0152/guard_0035":
        digests = {
            16: (
                "1e38702594cfc421ddfefc646fc1a714155de237bd542aa37731b8d257f9ad29",
                "611f243e00aaa3854f14486d6d643b99b396f255d1cd2093eb1e04cc4975a10a",
                "1ec15df53957b79c8d96a23529b5bfa312dfe2ad95f90ffd5cdd3eab08912047",
                "a87ee3bef402fd6e8ee3027d4eb5cfb6fd4fe51173a61cbdac27918e37663102",
                "1cdfb0eb758a9a8444e33d6c5adf50b8fe40ac2c0755c280f5724217c26eda26",
                "08bfa13a6a62f328caa263e0b405b449929bc2dd48833b6dde5cf09807b6527e",
            ),
            17: (
                "b7ec3390f84461d38037523a5211d53569e45bce4f4c6efb9be41fdd1231c106",
                "e6f09f88550b8be345094edcadf116a1b5a667168554ad4489a814a56ece74d5",
                "bbf5518d4f0502039a023ce60645a7480b6b4b802c9db52b82aabdaa95211ba6",
                "8b8ff0ebf4fa1ff0dc3842181ea37823475d0a1e421da92c6d37cd062b2ccdb0",
                "ccc7356d97dc96c073dc63734a5a5f437eb712d5a259b886bdc7468f4aaf10de",
                "e0b6a2727d58fbe9e22779157896dbbcfc85e925922aabdcbfe586d2bcf1918e",
            ),
        }
    if revision == "0151/guard_0035":
        digests = {
            16: (
                "f16f5fbf25eecf9562889d40b9a99d8d7d795c3256769356be4fc68e4deaf4ce",
                "47a083881f9d19ad1abffba6e0a0b3c296087e1398a900bea8b88ab5cd16552e",
                "df07c79890dcf07b096ef780562db4a8b564bc77070e8465bba15a33ea308b4e",
                "a45a44ae1d4da46dbb9c4a449cef045983ef377b60f6564e548ff73e7fb07932",
                "0811e2acd6975980bff220a1cd39ca84a42a7c4b7c7074416ab36d095397b2b4",
                "0a6ebf9bb8b00818832b830aa3378d137548250523b49c6cf9fa3840405d6807",
            ),
            17: (
                "5de82fcb9585b279807ebe08ef783d2a0928a2959cac2938c89ff4306760039c",
                "6f97fd15f3ae898a77ce5984ab24490e668b23d2d18c6c1dd6cde2619e0616ed",
                "225fdd20a2e7a3f1a67f0ee43f22b00de82bc39ea9d7398a72660b778d5e8986",
                "1704ea86ef9e8cf8b31c83ae2fa3c63cd8cf2c43b5ea9e051ec57fea050c72a0",
                "3c06b66aeeb1294f50b362dab8faf1af7aacb77dc8eba1d76873f10e05df4e03",
                "aa8fd126b3b817dad05f60612feeac936bfc56f2f8a6aa98325647767f0dc96f",
            ),
        }
    if revision == "0150/guard_0035":
        digests = {
            16: (
                "bb5119cb802665e7de1410d8b16535d4b8caa062fc612cb253b4ee309d8da817",
                "7ec1a1f98241a6be531ae13a115ecaf30f576338d51bcc3a2abe05eab463eb55",
                "fb3a79059cb7f35a9fb74927694a73423e87a393fc89e8705bc2db798f581e9c",
                "fb5945359741d7ee1679abc1289df8c574a70106855a4e3ec135ed81aa550e0e",
                "857b56293d7f514f2e2ade73cf3a42ce49f6aecd860d95d2c58e5b596dc290b3",
                "9b56a906e8d2753f61927ae315d184b5145748497cdf44c91050ec0e51ccb08c",
            ),
            17: (
                "8cbc2182d7f29cb917720baae2c5a6f53b27bce6ef155f4343174409b4ba0f05",
                "3b81b3e7886d94d6c893e249680b76a868c888bd7f36ee4a18a7413bbefbaf71",
                "67a9590f4e9be0f04d782783c075afaf6c4781e0fea4dbfcd35593e67f08c90f",
                "8299e127ad3398fd5591af316d0248db52d7304f858b4d590d95f0d9f6e93aef",
                "ce2dccf6a142b5cbdf03e02e23aca0bd72c2067f8e23e62884d8360c38b39535",
                "7dd403490fe2f45a035bb8798b687b06823666eec177a7ae1776c0cf79eb0fc0",
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
        object_count={"0152/guard_0035": 7393, "0151/guard_0035": 7376, "0150/guard_0035": 7374, "0149/guard_0035": 7353, "0148/guard_0035": 7319, "0147/guard_0035": 7232, "0142/guard_0035": 7227, "0134/guard_0030": 6736}[revision]
        + (0 if profile in {"legacy-owner", "staging-readonly-legacy-owner", "cnpg-staging-legacy-owner"} else 2),
        inventory_sha256=digests[postgres_major][profiles.index(profile)],
    )


def require_application_schema_reference(
    observed: ApplicationSchemaInventory, *, profile: ApplicationSchemaProfile = "legacy-owner",
    revision: ApplicationSchemaRevision = "0152/guard_0035",
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
    if revision == "0152/guard_0035":
        return "0152", "guard_0035"
    if revision == "0151/guard_0035":
        return "0151", "guard_0035"
    if revision == "0150/guard_0035":
        return "0150", "guard_0035"
    if revision == "0149/guard_0035":
        return "0149", "guard_0035"
    if revision == "0148/guard_0035":
        return "0148", "guard_0035"
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
    if (public_revision, guard_revision) == ("0152", "guard_0035"):
        return "0152/guard_0035"
    if (public_revision, guard_revision) == ("0151", "guard_0035"):
        return "0151/guard_0035"
    if (public_revision, guard_revision) == ("0150", "guard_0035"):
        return "0150/guard_0035"
    if (public_revision, guard_revision) == ("0149", "guard_0035"):
        return "0149/guard_0035"
    if (public_revision, guard_revision) == ("0148", "guard_0035"):
        return "0148/guard_0035"
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
