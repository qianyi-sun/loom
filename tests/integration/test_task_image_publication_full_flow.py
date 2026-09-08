"""Real Go/guard/API/PostgreSQL/verification flow with explicit fake node/build I/O."""

from __future__ import annotations

import asyncio
import os
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Event, Thread
from uuid import UUID, uuid4

import httpx
import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from sqlalchemy import select, update

from loom.db.schema import (
    TaskImageBuildSessionGeneration,
    TaskImageMaterialization,
    TaskImagePublicationEnvelope,
    TaskImagePublicationJob,
    TaskImagePublicationKey,
    TaskImagePublicationState,
)
from loom_task_image_authority.api import create_app
from loom_task_image_authority.bundle_capability import TaskImageBundleCapabilityProvider
from loom_task_image_authority.publication_signing import (
    DistributedKeysetSnapshot,
    PublicationKeyRecord,
)
from loom_task_image_builder_guard.authority import AuthorityClient
from loom_task_image_builder_guard.errors import GuardError
from tests.integration import test_task_image_projection_store as projection
from tests.integration.test_task_image_authority_api import _HEADERS, _FakeBundleBackend, _settings
from tests.integration.test_task_image_authority_materializations import _queued_materialization
from tests.integration.test_task_image_publication_jobs import (
    registry_authority_session as registry_authority_session,
)
from tests.integration.test_task_image_publication_worker import Distribution, Signer, _worker
from tests.unit.test_task_image_builder_guard_service import _Attachment, _service
from tests.unit.test_task_image_oci_verification import _graph
from tests.unit.test_task_image_registry_reader import _issuer, _Response
from tests.unit.test_task_image_registry_reader import tls_registry as tls_registry
from tests.unit.test_task_image_registry_reader import token_key as token_key


class _ASGIAuthority(AuthorityClient):
    """Replace only HTTP byte I/O; inherit all production operation/parsing code.

    TLS/mTLS transport has its own socket tests. This fixture does not claim to
    exercise the node kernel, TLS authority endpoint, or production signer.
    """

    def __init__(self, config, client, loop, registry, reader, root):
        self._config, self.client, self.loop = config, client, loop
        self._now = lambda: datetime.now(UTC)
        self.registry, self.reader, self.root = registry, reader, root
        self.operations = []
        self.jobs = asyncio.Queue()

    def _request(self, route, body, *, expected_status, method="PUT", maximum_bytes=None):
        async def request():
            response = await self.client.request(method, route, json=body)
            self.operations.append((method, route, response.status_code))
            statuses = (expected_status,) if isinstance(expected_status, int) else expected_status
            if response.status_code not in statuses:
                raise GuardError("fixture_http_status_" + str(response.status_code))
            payload = response.content
            if len(payload) > (maximum_bytes or self._config.max_response_bytes):
                raise GuardError("fixture_http_response_oversized")
            if route.endswith("registry-credential"):
                repository = response.json()["repository"]
                for digest, value in self.reader.objects.items():
                    kind = "manifest" if digest == self.root.digest else "blob"
                    self.registry.routes[f"/v2/{repository}/{kind}s/{digest}"] = _Response(
                        headers=[
                            (
                                "Content-Type",
                                self.root.media_type
                                if kind == "manifest"
                                else "application/octet-stream",
                            ),
                            ("Docker-Content-Digest", digest),
                        ],
                        chunks=(value[: len(value) // 2], value[len(value) // 2 :]),
                    )
            if route.endswith("publication-submit"):
                self.jobs.put_nowait(UUID(body["operation_id"]))
            return payload

        return asyncio.run_coroutine_threadsafe(request(), self.loop).result(timeout=6)


async def test_go_guard_http_worker_commits_exact_receipt_after_source_refresh(
    tmp_path,
    monkeypatch,
    isolated_migration_postgres_url,
    registry_authority_session,
    tls_registry,
    token_key,
):
    helper_value = os.environ.get("LOOM_GO_V2_TEST_BINARY")
    if helper_value is None:
        if os.environ.get("LOOM_GO_V2_TEST_REQUIRED") == "1":
            pytest.fail("LOOM_GO_V2_TEST_BINARY is required")
        pytest.skip("LOOM_GO_V2_TEST_BINARY not configured")
    helper = Path(helper_value)
    assert helper.is_file()
    now = datetime.now(UTC)
    monkeypatch.setattr(projection, "NOW", now - timedelta(seconds=10))
    reader, root, _ = _graph(arch="arm64")
    issuer = _issuer(tls_registry, token_key)
    private = Ed25519PrivateKey.generate()
    key = PublicationKeyRecord(
        "http-publication-1", private.public_key().public_bytes_raw(), now.replace(microsecond=0)
    )
    distribution = DistributedKeysetSnapshot(
        1,
        0,
        (key.key_id,),
        now.replace(microsecond=0),
        (now + timedelta(minutes=10)).replace(microsecond=0),
    )

    def clock():
        return datetime.now(UTC)

    signer = Signer(private, key, distribution, clock)
    async with registry_authority_session() as session:
        await projection._release_grant(session, expires_at=now + timedelta(hours=2))
        materialization = await _queued_materialization(session)
        materialization_id = materialization.id
        session.add(
            TaskImagePublicationKey(
                key_id=key.key_id,
                public_key=key.public_key,
                activated_at=key.activated_at,
                status="active",
            )
        )
        await session.execute(update(TaskImagePublicationState).values(keyset_version=1))
        await session.commit()
    settings = _settings(tmp_path, isolated_migration_postgres_url)
    bundle = TaskImageBundleCapabilityProvider(
        backend=_FakeBundleBackend(),
        public_https_origin="https://objects.example",
        expected_bucket="loom-bundles",
        maximum_objects=2000,
        maximum_bytes=512 * 1024 * 1024,
        url_expiry_seconds=600,
    )
    app = create_app(
        settings, now_factory=clock, bundle_capability_provider=bundle, registry_token_issuer=issuer
    )
    ready = Event()
    service, ledger, peer, _, _ = _service(
        tmp_path, ready=ready.set, now_factory=clock, max_packet_bytes=32768
    )
    service.config = replace(
        service.config,
        identity=replace(service.config.identity, supervisor_sha256=projection.SUPERVISOR_SHA256),
        slurm=replace(
            service.config.slurm, request_sha256=projection._request().slurm_request_sha256
        ),
        containment=replace(
            service.config.containment,
            containment_policy_sha256="4" * 64,
            resource_profile_sha256="5" * 64,
        ),
    )
    peer.executable_sha256 = projection.SUPERVISOR_SHA256
    service._uuid = uuid4
    # Fake node evidence names exact logical children; only opening its directory
    # capability maps to a disposable local directory. No authority payload edits.
    service.containment.containment_policy_sha256 = "4" * 64
    service.containment.resource_limits_sha256 = "5" * 64
    logical_egress = _Attachment.containment_root + "/build-egress"
    real_directory = service.containment.build_egress_cgroup
    prepare = service.containment.prepare

    def prepare_node(*args):
        attachment = prepare(*args)
        attachment.build_egress_cgroup = logical_egress
        return attachment

    service.containment.prepare = prepare_node
    open_directory = service._open_directory_capability

    def open_node_directory(path):
        assert path == logical_egress
        return open_directory(str(real_directory))

    service._open_directory_capability = open_node_directory
    failures = []

    def run_guard():
        try:
            service.start()
        except BaseException as exc:
            failures.append(exc)

    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="https://authority.example",
            headers=_HEADERS,
            trust_env=False,
        ) as client:
            authority = _ASGIAuthority(
                service.config.authority,
                client,
                asyncio.get_running_loop(),
                tls_registry,
                reader,
                root,
            )
            service.authority = authority
            thread = Thread(target=run_guard)
            thread.start()
            assert await asyncio.to_thread(ready.wait, 3)
            worker = _worker(
                registry_authority_session,
                tls_registry,
                (None, issuer, signer, Distribution(distribution), clock),
            )

            async def verify():
                operation = await authority.jobs.get()
                return await worker.run(operation)

            verifying = asyncio.create_task(verify())
            process = None
            try:
                process = await asyncio.create_subprocess_exec(
                    str(helper),
                    "-test.v",
                    "-test.run=^TestGoPublicationHTTPOrchestratorHelper$",
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    env=os.environ
                    | {
                        "LOOM_GO_HTTP_HELPER": "1",
                        "LOOM_GO_HTTP_SOCKET": str(service.config.protocol.socket_path),
                        "LOOM_GO_HTTP_ROOT": root.digest,
                        "LOOM_GO_HTTP_ROOT_SIZE": str(root.size),
                        "LOOM_GO_HTTP_REGISTRY": tls_registry.origin,
                        "LOOM_GO_HTTP_REGISTRY_KEY": issuer.key_id,
                    },
                )
                stdout, stderr = await asyncio.wait_for(process.communicate(), 40)
                assert process.returncode == 0, (stdout + stderr).decode() + repr(
                    authority.operations
                )
                receipt = await asyncio.wait_for(verifying, 10)
                assert receipt.component_count == 1
                assert any(path.endswith("publication-poll") for _, path, _ in authority.operations)
                assert not any(
                    path.endswith(("/release", "/fail")) for _, path, _ in authority.operations
                )
                assert b"loom_tibs_" not in stdout + stderr
            finally:
                if process is not None and process.returncode is None:
                    process.kill()
                    await process.wait()
                verifying.cancel()
                await asyncio.gather(verifying, return_exceptions=True)
                service.stop()
                await asyncio.to_thread(thread.join, 7)
                service.close()
                ledger.close()
            assert not thread.is_alive() and not failures
    async with registry_authority_session() as session:
        row = await session.get(TaskImageMaterialization, materialization_id)
        assert row.state == "ready" and row.ready_at is not None and row.registry_images
        job = (await session.scalars(select(TaskImagePublicationJob))).one()
        assert job.state == "completed"
        assert len(list(await session.scalars(select(TaskImagePublicationEnvelope)))) == 1
        assert len(list(await session.scalars(select(TaskImageBuildSessionGeneration)))) >= 2
