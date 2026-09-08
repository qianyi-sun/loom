"""Create-only identity persistence and backing-service reconciliation boundaries."""

from __future__ import annotations

import base64
import copy
import hashlib
import json
import os
import stat
import subprocess
import sys
from pathlib import Path

import pytest
from scripts.ops import nebius_staging_identity_bootstrap as bootstrap


def encoded(value: str) -> str:
    return base64.b64encode(value.encode()).decode()


class Adapter:
    def __init__(self) -> None:
        self.documents = {
            "loom-secrets": {
                "metadata": {"name": "loom-secrets", "namespace": bootstrap.NAMESPACE},
                "data": {
                    "minio-access-key": encoded("canonical-root"),
                    "minio-secret-key": encoded("root-secret-never-display"),
                    "unrelated-certificate": encoded("line1\nline2\n"),
                },
            }
        }
        self.created: list[str] = []
        self.exec_calls: list[tuple[str, bytes]] = []
        self.tokens: set[bytes] = set()
        self.conflict = False

    def get_secret(self, namespace: str, name: str) -> dict | None:
        assert namespace == bootstrap.NAMESPACE
        return copy.deepcopy(self.documents.get(name))

    def create_secret(self, document: dict) -> bool:
        name = document["metadata"]["name"]
        assert name not in self.documents
        self.created.append(name)
        self.documents[name] = copy.deepcopy(document)
        if self.conflict:
            # Concurrent winner has different valid material: caller must use
            # readback rather than the material it attempted to create.
            self.documents[name] = bootstrap._new_secret(name)
        return not self.conflict

    def exec_control_plane(self, source: str, input_payload: bytes) -> bytes:
        assert len(self.created) == 4
        assert input_payload.decode() not in source
        self.exec_calls.append((source, input_payload))
        created = input_payload not in self.tokens
        self.tokens.add(input_payload)
        return json.dumps({"collector_created": int(created), "collector_verified": 1}).encode()


class Mc:
    def __init__(self, adapter: Adapter) -> None:
        self.adapter = adapter
        self.calls: list[tuple[list[str], bytes | None, dict]] = []
        self.policy = ""
        self.exists = False
        self.enabled = True
        self.member_of: list = []
        self.fail = False
        self.directories: set[Path] = set()

    def __call__(self, argv, *, env, input, stdout, stderr, timeout, check):
        assert len(self.adapter.created) == 4
        assert stdout == subprocess.PIPE and stderr == subprocess.PIPE
        assert timeout == 60 and not check
        assert "root-secret-never-display" not in str(argv)
        assert env["MC_HOST_loom"].startswith("https://canonical-root:root-secret-never-display@")
        directory = Path(env["MC_CONFIG_DIR"])
        self.directories.add(directory)
        assert stat.S_IMODE(directory.stat().st_mode) == 0o700
        assert stat.S_IMODE((directory / "input-policy.json").stat().st_mode) == 0o600
        assert json.loads((directory / "input-policy.json").read_text()) == bootstrap.INPUT_POLICY
        self.calls.append((list(argv), input, dict(env)))
        if self.fail:
            raise RuntimeError("root-secret-never-display")
        command = argv[2:]
        if command[:1] == ["--resolve"]:
            command = command[2:]
        if command[:3] == ["admin", "user", "info"]:
            if not self.exists:
                return subprocess.CompletedProcess(
                    argv,
                    1,
                    json.dumps(
                        {
                            "status": "error",
                            "error": {"cause": {"error": {"Code": "XMinioAdminNoSuchUser"}}},
                        }
                    ).encode(),
                    b"",
                )
            result = {
                "status": "success",
                "userStatus": "enabled" if self.enabled else "disabled",
                "policyName": self.policy,
                "memberOf": self.member_of,
            }
        else:
            result = {"status": "success"}
            if command[:3] == ["admin", "user", "add"]:
                assert command == ["admin", "user", "add", "loom"]
                user, password = input.decode().splitlines()
                assert user == bootstrap.MINIO_USER
                assert password not in str(argv)
                self.exists = True
            if command[:3] == ["admin", "policy", "attach"]:
                self.policy = bootstrap.MINIO_POLICY
        return subprocess.CompletedProcess(argv, 0, json.dumps(result).encode(), b"")


def reconcile(adapter: Adapter, mc: Mc) -> dict:
    return bootstrap.bootstrap_identities(
        adapter, minio_endpoint="https://staging.example:19443", run=mc
    )


def test_seed_is_separate_create_only_phase() -> None:
    adapter = Adapter()
    first = bootstrap.seed_identities(adapter)
    before = copy.deepcopy(adapter.documents)
    second = bootstrap.seed_identities(adapter)
    assert first == {
        "secrets_created": 4,
        "secrets_verified": 4,
        "database_role_secrets_verified": 2,
    }
    assert second["secrets_created"] == 0
    assert adapter.documents == before
    assert not adapter.exec_calls
    for name, role in bootstrap.DB_SECRETS.items():
        document = adapter.documents[name]
        assert document["type"] == "kubernetes.io/basic-auth"
        assert document["metadata"]["labels"]["cnpg.io/reload"] == "true"
        assert base64.b64decode(document["data"]["username"]).decode() == role


def test_durable_bootstrap_then_repeat_does_not_rotate() -> None:
    adapter = Adapter()
    mc = Mc(adapter)
    first = reconcile(adapter, mc)
    before = copy.deepcopy(adapter.documents)
    second = reconcile(adapter, mc)
    assert first["secrets_created"] == 4 and first["collector_created"] == 1
    assert second["secrets_created"] == 0 and second["collector_created"] == 0
    assert second["minio_identity_verified"] == 1
    assert adapter.documents == before
    assert len(adapter.tokens) == 1
    assert all(type(value) is int for value in second.values())
    assert sum(row[0][2:5] == ["admin", "policy", "attach"] for row in mc.calls) == 1
    assert all(not directory.exists() for directory in mc.directories)


def test_private_minio_route_preserves_hostname_and_tls_verification() -> None:
    adapter = Adapter()
    mc = Mc(adapter)
    result = bootstrap.bootstrap_identities(
        adapter,
        minio_endpoint="https://staging.example:19443",
        minio_resolve="10.253.176.1",
        run=mc,
    )
    assert result["minio_identity_verified"] == 1
    assert mc.calls
    for argv, _, environment in mc.calls:
        assert argv[:4] == ["mc", "--json", "--resolve", "staging.example:19443=10.253.176.1"]
        assert "--insecure" not in argv
        assert "MC_INSECURE" not in environment
        assert environment["MC_HOST_loom"].endswith("@staging.example:19443")


@pytest.mark.parametrize(
    "address",
    [
        "127.0.0.1",
        "192.0.2.10",
        "8.8.8.8",
        "169.254.1.1",
        "0.0.0.0",
        "::1",
        "10.20.30.1/32",
        "10.20.30.1:19443",
        "staging.example=10.20.30.1",
        "",
    ],
)
def test_private_minio_route_rejects_unscoped_or_ambiguous_addresses(address: str) -> None:
    adapter = Adapter()
    mc = Mc(adapter)
    with pytest.raises(bootstrap.BootstrapError):
        bootstrap.bootstrap_identities(
            adapter,
            minio_endpoint="https://staging.example:19443",
            minio_resolve=address,
            run=mc,
        )
    assert not mc.calls
    assert not adapter.created


@pytest.mark.parametrize(
    "endpoint",
    [
        "http://staging.example:19443",
        "https://user:password@staging.example:19443",
        "https://staging.example:19443/path",
        "https://staging.example:19443?x=1",
        "https://staging.example:19443#part",
        "https://10.20.30.1:19443",
    ],
)
def test_private_minio_route_requires_certificate_dns_https_origin(endpoint: str) -> None:
    adapter = Adapter()
    mc = Mc(adapter)
    with pytest.raises(bootstrap.BootstrapError):
        bootstrap.bootstrap_identities(
            adapter,
            minio_endpoint=endpoint,
            minio_resolve="10.253.176.1",
            run=mc,
        )
    assert not mc.calls
    assert not adapter.created


def test_creation_conflict_reads_winners_stable_secrets() -> None:
    adapter = Adapter()
    adapter.conflict = True
    result = reconcile(adapter, Mc(adapter))
    assert result["secrets_created"] == 0
    assert adapter.exec_calls[0][1] == base64.b64decode(
        adapter.documents[bootstrap.COLLECTOR_SECRET]["data"]["token"]
    )


@pytest.mark.parametrize("mutation", ["empty", "owner", "type", "password", "username", "reload"])
def test_existing_partial_or_foreign_db_identity_is_never_replaced(mutation: str) -> None:
    adapter = Adapter()
    bootstrap.seed_identities(adapter)
    name = next(iter(bootstrap.DB_SECRETS))
    document = adapter.documents[name]
    if mutation == "empty":
        document["data"] = {}
    elif mutation == "owner":
        document["metadata"]["labels"][bootstrap.OWNER_LABEL] = "somebody-else"
    elif mutation == "type":
        document["type"] = "Opaque"
    elif mutation == "password":
        document["data"]["password"] = "invalid-base64!"
    elif mutation == "username":
        document["data"]["username"] = encoded("postgres")
    else:
        document["metadata"]["labels"].pop("cnpg.io/reload")
    before = copy.deepcopy(adapter.documents)
    with pytest.raises(bootstrap.BootstrapError):
        reconcile(adapter, Mc(adapter))
    assert adapter.documents == before
    assert not adapter.exec_calls


@pytest.mark.parametrize("name", [bootstrap.MINIO_SECRET, bootstrap.COLLECTOR_SECRET])
def test_partial_other_identities_fail_without_regeneration(name: str) -> None:
    adapter = Adapter()
    bootstrap.seed_identities(adapter)
    adapter.documents[name]["data"] = {"missing": encoded("wrong")}
    before = copy.deepcopy(adapter.documents)
    with pytest.raises(bootstrap.BootstrapError):
        reconcile(adapter, Mc(adapter))
    assert adapter.documents == before


def test_minio_failure_preserves_seeds_and_redacts_and_cleans_up() -> None:
    adapter = Adapter()
    mc = Mc(adapter)
    mc.fail = True
    with pytest.raises(bootstrap.BootstrapError) as caught:
        reconcile(adapter, mc)
    assert "root-secret" not in str(caught.value)
    assert len(adapter.created) == 4 and not adapter.exec_calls
    assert all(not directory.exists() for directory in mc.directories)
    before = copy.deepcopy(adapter.documents)
    mc.fail = False
    assert reconcile(adapter, mc)["secrets_created"] == 0
    assert adapter.documents == before


@pytest.mark.parametrize("state", ["disabled", "broader-policy", "group"])
def test_existing_minio_unrelated_or_disabled_identity_is_not_overwritten(state: str) -> None:
    adapter = Adapter()
    mc = Mc(adapter)
    mc.exists = True
    if state == "disabled":
        mc.enabled = False
    elif state == "broader-policy":
        mc.policy = "readwrite"
    else:
        mc.member_of = [{"name": "admins"}]
    with pytest.raises(bootstrap.BootstrapError):
        reconcile(adapter, mc)
    assert len(mc.calls) == 1
    assert not adapter.exec_calls


def test_minio_policy_is_only_canonical_input_read_access() -> None:
    statements = bootstrap.INPUT_POLICY["Statement"]
    assert {action for statement in statements for action in statement["Action"]} == {
        "s3:GetObject",
        "s3:ListBucket",
        "s3:GetBucketLocation",
    }
    assert {resource for statement in statements for resource in statement["Resource"]} == {
        "arn:aws:s3:::loom-staging-artifacts",
        "arn:aws:s3:::loom-staging-artifacts/*",
    }


@pytest.mark.parametrize(
    "endpoint",
    [
        "http://public.example",
        "https://user:password@example",
        "https://example/path",
        "https://example?secret=x",
        "https://example#frag",
        "https://example:0",
        "file:///tmp/x",
    ],
)
def test_minio_root_cannot_be_sent_to_an_ambiguous_endpoint(endpoint: str) -> None:
    with pytest.raises(bootstrap.BootstrapError):
        bootstrap._minio_host(endpoint, {"minio-access-key": "root", "minio-secret-key": "pwd"})


def test_root_url_quotes_credentials_without_modifying_input() -> None:
    root = {"minio-access-key": "root@user", "minio-secret-key": "x/y:#@"}
    assert bootstrap._minio_host("http://127.0.0.1:19000", root) == (
        "http://root%40user:x%2Fy%3A%23%40@127.0.0.1:19000"
    )


def test_collector_adapter_failure_never_leaks_source_error() -> None:
    adapter = Adapter()

    def fail(source, input_payload):
        raise RuntimeError(input_payload.decode())

    adapter.exec_control_plane = fail
    with pytest.raises(bootstrap.BootstrapError) as caught:
        reconcile(adapter, Mc(adapter))
    assert "loom_ecc_" not in str(caught.value)


@pytest.mark.skipif(
    not os.environ.get("LOOM_NEBIUS_BOOTSTRAP_TEST_MINIO_ENDPOINT"),
    reason="requires an explicitly disposable empty MinIO and an installed mc binary",
)
def test_minio_identity_real_backend() -> None:
    import boto3
    from botocore.exceptions import ClientError

    endpoint = os.environ["LOOM_NEBIUS_BOOTSTRAP_TEST_MINIO_ENDPOINT"]
    binary = os.environ["LOOM_NEBIUS_BOOTSTRAP_TEST_MC_BINARY"]
    assert Path(binary).is_file() and os.access(binary, os.X_OK)
    root = {"minio-access-key": "canonical-root", "minio-secret-key": "root-secret-never-display"}
    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=root["minio-access-key"],
        aws_secret_access_key=root["minio-secret-key"],
        region_name="us-east-1",
    )
    assert client.list_buckets()["Buckets"] == []
    for bucket in ("loom-staging-artifacts", "loom-staging-trajectories"):
        client.create_bucket(Bucket=bucket)
        client.put_object(Bucket=bucket, Key="input", Body=b"fixture-input")
    values = {"access-key": bootstrap.MINIO_USER, "secret-key": "b" * 64}
    for _ in range(2):
        bootstrap._reconcile_minio(
            values, root, endpoint=endpoint, binary=binary, runner=subprocess.run
        )
    reader = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=values["access-key"],
        aws_secret_access_key=values["secret-key"],
        region_name="us-east-1",
    )
    assert (
        reader.get_object(Bucket="loom-staging-artifacts", Key="input")["Body"].read()
        == b"fixture-input"
    )
    assert reader.list_objects_v2(Bucket="loom-staging-artifacts")["KeyCount"] == 1
    for operation in (
        lambda: reader.put_object(Bucket="loom-staging-artifacts", Key="no", Body=b"no"),
        lambda: reader.get_object(Bucket="loom-staging-trajectories", Key="input"),
    ):
        with pytest.raises(ClientError) as caught:
            operation()
        assert caught.value.response["ResponseMetadata"]["HTTPStatusCode"] == 403
    import tempfile

    with tempfile.TemporaryDirectory(prefix="loom-nebius-mc-disable-") as directory:
        env = {
            "PATH": os.defpath,
            "MC_HOST_loom": bootstrap._minio_host(endpoint, root),
            "MC_CONFIG_DIR": directory,
        }
        disabled = subprocess.run(
            [binary, "--json", "admin", "user", "disable", "loom", bootstrap.MINIO_USER],
            env=env,
            capture_output=True,
            timeout=30,
            check=False,
        )
        assert disabled.returncode == 0
        with pytest.raises(bootstrap.BootstrapError, match="disabled"):
            bootstrap._reconcile_minio(
                values, root, endpoint=endpoint, binary=binary, runner=subprocess.run
            )
        observed = subprocess.run(
            [binary, "--json", "admin", "user", "info", "loom", bootstrap.MINIO_USER],
            env=env,
            capture_output=True,
            timeout=30,
            check=False,
        )
        assert observed.returncode == 0
        assert json.loads(observed.stdout)["userStatus"] == "disabled"


@pytest.mark.skipif(
    not os.environ.get("LOOM_NEBIUS_BOOTSTRAP_TEST_DB_URL"),
    reason="requires an explicitly disposable empty PostgreSQL database named loom",
)
def test_collector_registration_real_postgres() -> None:
    """Run only against the disposable test database selected by the caller."""
    from sqlalchemy import create_engine, text
    from sqlalchemy.engine import make_url

    url = os.environ["LOOM_NEBIUS_BOOTSTRAP_TEST_DB_URL"]
    assert make_url(url).database == "loom"
    engine = create_engine(url)
    with engine.begin() as conn:
        conn.execute(
            text("""CREATE TABLE tokens (
            token_hash bytea PRIMARY KEY, type text NOT NULL, scopes text[] NOT NULL,
            team_id uuid, issued_at timestamptz NOT NULL, expires_at timestamptz,
            revoked_at timestamptz)""")
        )
    token = "loom_ecc_" + "a" * 64
    env = {
        **os.environ,
        "LOOM_ENV": "staging",
        "LOOM_NAMESPACE": "loom-staging",
        "LOOM_CP_DB_URL": url,
    }

    def register(**changes: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            [sys.executable, "-c", bootstrap.COLLECTOR_REGISTER_SOURCE],
            input=token.encode(),
            capture_output=True,
            env={**env, **changes},
            timeout=30,
            check=False,
        )

    try:
        first, second = register(), register()
        assert first.returncode == second.returncode == 0
        assert json.loads(first.stdout)["collector_created"] == 1
        assert json.loads(second.stdout)["collector_created"] == 0
        for column, value in (
            ("revoked_at", "now()"),
            ("expires_at", "now() + interval '30 days'"),
            ("scopes", "ARRAY['admin:tokens']"),
            ("type", "'admin'"),
        ):
            with engine.begin() as conn:
                conn.execute(text(f"UPDATE tokens SET {column} = {value}"))
            failed = register()
            assert failed.returncode == 1
            assert failed.stderr == b"collector registration failed\n"
            assert not failed.stdout
            with engine.begin() as conn:
                observed = conn.execute(text(f"SELECT {column} FROM tokens")).scalar_one()
                assert observed is not None  # bootstrap did not clear the refusal condition
                conn.execute(
                    text(
                        "UPDATE tokens SET revoked_at=NULL, expires_at=NULL, "
                        "scopes=ARRAY['execution:capacity:observe'], type='worker'"
                    )
                )
        assert register(LOOM_ENV="development").returncode == 1
        assert register(LOOM_NAMESPACE="loom").returncode == 1
        with engine.begin() as conn:
            rows = conn.execute(text("SELECT token_hash FROM tokens")).all()
            assert rows == [(hashlib.sha256(token.encode()).digest(),)]
    finally:
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE tokens"))
        engine.dispose()
