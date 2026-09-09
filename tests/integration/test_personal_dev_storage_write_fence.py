"""A delayed old installer must not inject credentials into a new namespace."""

import asyncio
import json
from dataclasses import replace
from uuid import uuid4

import pytest

from loom.dev_instance_runtime import DevInstanceRuntimeError, KubectlClient, KubectlSecretVault
from loom.personal_dev_capacity_runtime import KubectlPersonalDevCapacityInstaller
from loom.personal_dev_incarnation_storage import personal_dev_storage_annotations
from tests.integration.test_personal_dev_storage_namespace import (
    disposable_storage_kubectl,  # noqa: F401
)
from tests.unit.test_personal_dev_storage_runtime_identity import _bound_claim


async def test_delayed_capacity_seed_cannot_overwrite_recreated_namespace(
    disposable_storage_kubectl,  # noqa: F811
):
    kubectl = disposable_storage_kubectl
    old = _bound_claim()
    identity = old.operation.storage_binding.identity
    vault = KubectlSecretVault(
        kubectl,
        "postgresql://admin:fixture@database.example/postgres",
        protected_worker_runtime=True,
    )
    installer = KubectlPersonalDevCapacityInstaller(kubectl=kubectl, database=None, config=None)
    await vault.store(identity, "b" * 32)
    old_credentials = await installer._credentials(old, identity)
    await installer._persist_credentials(old, identity, old_credentials)
    await kubectl.delete_storage_namespace(identity)

    storage = old.operation.storage_binding.model_copy(update={"subject_incarnation": uuid4()})
    current = replace(
        old,
        environment=replace(
            old.environment,
            storage_binding=storage,
            subject_incarnation=storage.subject_incarnation,
        ),
        operation=replace(
            old.operation, storage_binding=storage, subject_incarnation=storage.subject_incarnation
        ),
        attempt=replace(old.attempt, subject_incarnation=storage.subject_incarnation),
    )
    await vault.store(storage.identity, "c" * 32)
    credentials = await installer._credentials(current, storage.identity)
    await installer._persist_credentials(current, storage.identity, credentials)
    before = await kubectl.read_secret_optional(
        identity.namespace, "loom-capacity-agent-credentials"
    )

    # Resume the old invocation after its namespace/credential preflight has
    # already returned. New read checks at the caller cannot fence this write.
    with pytest.raises(DevInstanceRuntimeError):
        await installer._persist_credentials(old, identity, old_credentials)
    assert (
        await kubectl.read_secret_optional(
            identity.namespace,
            "loom-capacity-agent-credentials",
        )
        == before
    )


@pytest.mark.parametrize(
    "pause_at", ("before_create", "after_create", "before_replace", "before_replace_absent")
)
async def test_secret_two_phase_write_fences_namespace_replacement(
    disposable_storage_kubectl,
    pause_at,  # noqa: F811
):
    from loom.personal_dev_storage_secret_write import write_storage_secret

    kubectl = disposable_storage_kubectl
    identity = _bound_claim().operation.storage_binding.identity
    await kubectl.apply(
        json.dumps(
            {
                "apiVersion": "v1",
                "kind": "Namespace",
                "metadata": {
                    "name": identity.namespace,
                    "annotations": personal_dev_storage_annotations(identity),
                },
            }
        )
    )
    paused, resume = asyncio.Event(), asyncio.Event()
    transmitted = []

    class PausedWriter:
        async def run(self, argv, *, stdin=None, timeout_seconds=120):
            document = json.loads(stdin) if stdin is not None else None
            is_secret = isinstance(document, dict) and document.get("kind") == "Secret"
            if is_secret and "create" in argv:
                # No credential data may cross the unguarded CREATE boundary.
                assert document.get("data", {}) == {}
                assert document.get("stringData", {}) == {}
                transmitted.append(document)
            if is_secret and (
                (pause_at == "before_create" and "create" in argv)
                or (pause_at.startswith("before_replace") and "replace" in argv)
            ):
                paused.set()
                await resume.wait()
            result = await kubectl.runner.run(argv, stdin=stdin, timeout_seconds=timeout_seconds)
            if is_secret and pause_at == "after_create" and "create" in argv:
                paused.set()
                await resume.wait()
            return result

    writer = KubectlClient("kubectl", runner=PausedWriter())
    document = {
        "apiVersion": "v1",
        "kind": "Secret",
        "type": "Opaque",
        "metadata": {"name": "storage-write-probe", "namespace": identity.namespace},
        "stringData": {"password": "must-not-cross-incarnations"},
    }
    task = asyncio.create_task(write_storage_secret(writer, identity, document))
    try:
        await asyncio.wait_for(paused.wait(), timeout=15)
        await kubectl.delete_storage_namespace(identity)
        current = identity.storage_binding.model_copy(
            update={"subject_incarnation": uuid4()}
        ).identity
        await kubectl.apply(
            json.dumps(
                {
                    "apiVersion": "v1",
                    "kind": "Namespace",
                    "metadata": {
                        "name": current.namespace,
                        "annotations": personal_dev_storage_annotations(current),
                    },
                }
            )
        )
        before = None
        if pause_at not in ("before_create", "before_replace_absent"):
            await kubectl.apply(json.dumps({**document, "stringData": {"password": "successor"}}))
            before = await kubectl.read_secret_optional(current.namespace, "storage-write-probe")
        resume.set()
        with pytest.raises(DevInstanceRuntimeError):
            await asyncio.wait_for(task, timeout=15)
        if pause_at == "before_create":
            raw = await kubectl.runner.run(
                kubectl._argv(
                    "get",
                    "secret",
                    "storage-write-probe",
                    "--namespace",
                    current.namespace,
                    "-o",
                    "json",
                )
            )
            # Kubernetes omits the data field for an empty Secret.
            assert not json.loads(raw.stdout).get("data")
        else:
            after = await kubectl.read_secret_optional(current.namespace, "storage-write-probe")
            assert after == before
        assert len(transmitted) == 1
    finally:
        resume.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_two_phase_secret_retry_recovers_empty_placeholder_and_preserves_uid(
    disposable_storage_kubectl,  # noqa: F811
):
    from loom.personal_dev_storage_secret_write import write_storage_secret

    kubectl = disposable_storage_kubectl
    identity = _bound_claim().operation.storage_binding.identity
    await kubectl.apply(
        json.dumps(
            {
                "apiVersion": "v1",
                "kind": "Namespace",
                "metadata": {
                    "name": identity.namespace,
                    "annotations": personal_dev_storage_annotations(identity),
                },
            }
        )
    )

    class LostReply:
        async def run(self, argv, *, stdin=None, timeout_seconds=120):
            result = await kubectl.runner.run(argv, stdin=stdin, timeout_seconds=timeout_seconds)
            if "create" in argv:
                raise DevInstanceRuntimeError("lost empty-create reply")
            return result

    document = {
        "apiVersion": "v1",
        "kind": "Secret",
        "type": "Opaque",
        "metadata": {"name": "storage-write-probe", "namespace": identity.namespace},
        "stringData": {"password": "first"},
    }
    with pytest.raises(DevInstanceRuntimeError, match="lost"):
        await write_storage_secret(KubectlClient("kubectl", runner=LostReply()), identity, document)
    raw = await kubectl.runner.run(
        kubectl._argv(
            "get",
            "secret",
            "storage-write-probe",
            "--namespace",
            identity.namespace,
            "-o",
            "json",
        )
    )
    placeholder = json.loads(raw.stdout)
    assert not placeholder.get("data")
    uid = placeholder["metadata"]["uid"]
    await write_storage_secret(kubectl, identity, document)
    await write_storage_secret(
        kubectl, identity, {**document, "stringData": {"password": "second"}}
    )
    assert await kubectl.read_secret(identity.namespace, "storage-write-probe") == {
        "password": b"second"
    }
    with pytest.raises(DevInstanceRuntimeError, match="initialized"):
        await write_storage_secret(kubectl, identity, document, create_only=True)
    raw = await kubectl.runner.run(
        kubectl._argv(
            "get",
            "secret",
            "storage-write-probe",
            "--namespace",
            identity.namespace,
            "-o",
            "json",
        )
    )
    assert json.loads(raw.stdout)["metadata"]["uid"] == uid
    assert await kubectl.read_secret(identity.namespace, "storage-write-probe") == {
        "password": b"second"
    }


async def test_secret_writer_rejects_older_operation_epoch(
    disposable_storage_kubectl,  # noqa: F811
):
    from loom.personal_dev_storage_secret_write import write_storage_secret

    kubectl = disposable_storage_kubectl
    identity = _bound_claim().operation.storage_binding.identity
    await kubectl.apply(
        json.dumps(
            {
                "apiVersion": "v1",
                "kind": "Namespace",
                "metadata": {
                    "name": identity.namespace,
                    "annotations": personal_dev_storage_annotations(identity),
                },
            }
        )
    )
    document = {
        "apiVersion": "v1",
        "kind": "Secret",
        "type": "Opaque",
        "metadata": {"name": "storage-write-probe", "namespace": identity.namespace},
        "stringData": {"password": "newer"},
    }
    await write_storage_secret(kubectl, identity, document, operation_epoch=2)
    with pytest.raises(DevInstanceRuntimeError, match="epoch"):
        await write_storage_secret(
            kubectl, identity, {**document, "stringData": {"password": "older"}}, operation_epoch=1
        )
    assert await kubectl.read_secret(identity.namespace, "storage-write-probe") == {
        "password": b"newer"
    }


async def test_capacity_seed_cas_loser_stops_before_database_mutation(
    disposable_storage_kubectl,  # noqa: F811
):
    kubectl = disposable_storage_kubectl
    claim = _bound_claim()
    identity = claim.operation.storage_binding.identity
    await KubectlSecretVault(
        kubectl,
        "postgresql://admin:fixture@database.example/postgres",
        protected_worker_runtime=True,
    ).store(identity, "b" * 32)
    installer = KubectlPersonalDevCapacityInstaller(kubectl=kubectl, database=None, config=None)
    first = await installer._credentials(claim, identity)
    await installer._persist_credentials(claim, identity, first)
    paused, resume = asyncio.Event(), asyncio.Event()
    database_calls = []

    class Database:
        async def converge(self, **kwargs):
            database_calls.append(kwargs)
            raise AssertionError("losing credential writer reached SQL")

    class PauseReplace:
        async def run(self, argv, *, stdin=None, timeout_seconds=120):
            if "replace" in argv:
                paused.set()
                await resume.wait()
            return await kubectl.runner.run(argv, stdin=stdin, timeout_seconds=timeout_seconds)

    loser = KubectlPersonalDevCapacityInstaller(
        kubectl=KubectlClient("kubectl", runner=PauseReplace()),
        database=Database(),
        config=None,
    )
    task = asyncio.create_task(loser.converge(claim))
    try:
        await asyncio.wait_for(paused.wait(), timeout=15)
        winner = replace(first, reporter_token="w" * 48, reporter_incarnation=uuid4())
        await installer._persist_credentials(claim, identity, winner)
        resume.set()
        with pytest.raises(DevInstanceRuntimeError):
            await asyncio.wait_for(task, timeout=15)
        assert database_calls == []
        assert (
            await installer._credentials(claim, identity)
        ).reporter_token == winner.reporter_token
    finally:
        resume.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_capacity_seed_lost_final_reply_recovers_persisted_credentials(
    disposable_storage_kubectl,  # noqa: F811
):
    kubectl = disposable_storage_kubectl
    claim = _bound_claim()
    identity = claim.operation.storage_binding.identity
    await KubectlSecretVault(
        kubectl,
        "postgresql://admin:fixture@database.example/postgres",
        protected_worker_runtime=True,
    ).store(identity, "b" * 32)

    class LostReply:
        async def run(self, argv, *, stdin=None, timeout_seconds=120):
            result = await kubectl.runner.run(argv, stdin=stdin, timeout_seconds=timeout_seconds)
            if "replace" in argv:
                raise DevInstanceRuntimeError("lost credential write reply")
            return result

    writer = KubectlPersonalDevCapacityInstaller(
        kubectl=KubectlClient("kubectl", runner=LostReply()),
        database=None,
        config=None,
    )
    credentials = await writer._credentials(claim, identity)
    with pytest.raises(DevInstanceRuntimeError, match="lost"):
        await writer._persist_credentials(claim, identity, credentials)
    retry = KubectlPersonalDevCapacityInstaller(kubectl=kubectl, database=None, config=None)
    retained = await retry._credentials(claim, identity)
    assert retained.reporter_token == credentials.reporter_token
    assert retained.agent_password == credentials.agent_password
    assert retained.observer_password == credentials.observer_password
    await retry._persist_credentials(claim, identity, retained)


async def test_prepared_same_operation_seed_writer_preserves_completed_winner(
    disposable_storage_kubectl,  # noqa: F811
):
    kubectl = disposable_storage_kubectl
    claim = _bound_claim()
    identity = claim.operation.storage_binding.identity
    await KubectlSecretVault(
        kubectl,
        "postgresql://admin:fixture@database.example/postgres",
        protected_worker_runtime=True,
    ).store(identity, "b" * 32)
    installer = KubectlPersonalDevCapacityInstaller(kubectl=kubectl, database=None, config=None)
    first = await installer._credentials(claim, identity)
    delayed = await installer._credentials(claim, identity)
    assert delayed.reporter_token != first.reporter_token
    await installer._persist_credentials(claim, identity, first)
    # Unlike the CAS test, the delayed writer starts its fresh GET only after
    # the winner has completed its PUT and is allowed to continue to SQL.
    with pytest.raises(DevInstanceRuntimeError):
        await installer._persist_credentials(claim, identity, delayed)
    retained = await installer._credentials(claim, identity)
    assert retained.reporter_token == first.reporter_token
    assert retained.agent_password == first.agent_password
    assert retained.observer_password == first.observer_password
    await installer._persist_credentials(claim, identity, retained)


@pytest.mark.parametrize(
    "populated,race", ((False, None), (True, None), (False, "fill"), (False, "replace"))
)
async def test_new_incarnation_recovers_only_empty_stale_seed_placeholder(
    disposable_storage_kubectl,
    populated,
    race,  # noqa: F811
):
    kubectl = disposable_storage_kubectl
    old = _bound_claim()
    storage = old.operation.storage_binding.model_copy(update={"subject_incarnation": uuid4()})
    current = replace(
        old,
        environment=replace(
            old.environment,
            storage_binding=storage,
            subject_incarnation=storage.subject_incarnation,
        ),
        operation=replace(
            old.operation, storage_binding=storage, subject_incarnation=storage.subject_incarnation
        ),
        attempt=replace(old.attempt, subject_incarnation=storage.subject_incarnation),
    )
    await KubectlSecretVault(
        kubectl,
        "postgresql://admin:fixture@database.example/postgres",
        protected_worker_runtime=True,
    ).store(storage.identity, "b" * 32)
    stale = {
        "apiVersion": "v1",
        "kind": "Secret",
        "type": "Opaque",
        "metadata": {
            "name": "loom-capacity-agent-credentials",
            "namespace": storage.identity.namespace,
            "annotations": {
                **personal_dev_storage_annotations(old.operation.storage_binding.identity),
                "loom.dev/storage-namespace-uid": str(uuid4()),
                "loom.dev/storage-write-phase": "empty",
                "loom.dev/storage-operation-epoch": str(old.operation.operation_epoch),
            },
        },
    }
    if populated:
        stale["stringData"] = {"password": "must-preserve"}
    created = await kubectl.runner.run(
        kubectl._argv("create", "-f", "-", "-o", "json"), stdin=json.dumps(stale)
    )
    old_uid = json.loads(created.stdout)["metadata"]["uid"]

    class RaceDelete:
        async def run(self, argv, *, stdin=None, timeout_seconds=120):
            if "delete" in argv and any("/secrets/" in part for part in argv):
                if race == "replace":
                    await kubectl.runner.run(
                        kubectl._argv(
                            "delete",
                            "secret",
                            stale["metadata"]["name"],
                            "--namespace",
                            storage.identity.namespace,
                        )
                    )
                    await kubectl.runner.run(
                        kubectl._argv("create", "-f", "-"),
                        stdin=json.dumps(
                            {
                                **stale,
                                "stringData": {"password": "raced"},
                            }
                        ),
                    )
                elif race == "fill":
                    current_secret = await kubectl.runner.run(
                        kubectl._argv(
                            "get",
                            "secret",
                            stale["metadata"]["name"],
                            "--namespace",
                            storage.identity.namespace,
                            "-o",
                            "json",
                        )
                    )
                    filled = json.loads(current_secret.stdout)
                    filled["stringData"] = {"password": "raced"}
                    await kubectl.runner.run(
                        kubectl._argv("replace", "-f", "-"), stdin=json.dumps(filled)
                    )
            return await kubectl.runner.run(argv, stdin=stdin, timeout_seconds=timeout_seconds)

    installer = KubectlPersonalDevCapacityInstaller(
        kubectl=KubectlClient("kubectl", runner=RaceDelete()) if race else kubectl,
        database=None,
        config=None,
    )
    if populated:
        with pytest.raises(DevInstanceRuntimeError):
            await installer._credentials(current, storage.identity)
        assert await kubectl.read_secret(storage.identity.namespace, stale["metadata"]["name"]) == {
            "password": b"must-preserve"
        }
    else:
        credentials = await installer._credentials(current, storage.identity)
        if race:
            with pytest.raises(DevInstanceRuntimeError):
                await installer._persist_credentials(current, storage.identity, credentials)
            assert await kubectl.read_secret(
                storage.identity.namespace, stale["metadata"]["name"]
            ) == {"password": b"raced"}
        else:
            await installer._persist_credentials(current, storage.identity, credentials)
            assert (
                await installer._credentials(current, storage.identity)
            ).reporter_token == credentials.reporter_token
    raw = await kubectl.runner.run(
        kubectl._argv(
            "get",
            "secret",
            stale["metadata"]["name"],
            "--namespace",
            storage.identity.namespace,
            "-o",
            "json",
        )
    )
    assert (json.loads(raw.stdout)["metadata"]["uid"] == old_uid) is (populated or race == "fill")
