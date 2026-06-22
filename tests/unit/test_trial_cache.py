"""Unit tests for `loom_worker.trial_cache` (#317 Phase 1).

Covers the pure helpers + the orchestration in `resolve_trial_image`
with mocked Docker client + mocked cp_client. Real Docker is
exercised in `tests/integration/test_trial_cache_e2e.py`.
"""
from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock
from uuid import UUID, uuid4

import pytest
from docker.errors import ImageNotFound

from loom_worker import trial_cache

# ─── Stubs ──────────────────────────────────────────────────────────


class _StubSettings:
    """Mirror of the codegen'd WorkerSettings.trial_cache_* fields."""

    def __init__(
        self,
        *,
        trial_cache_registry_repo: str = "",
        trial_cache_registry_pull_timeout_sec: float = 15.0,
        trial_cache_base_image_pull_timeout_sec: float = 1800.0,
        trial_cache_ttl_hours: int = 168,
        trial_cache_min_free_gb: int = 20,
        trial_cache_build_lock_timeout_sec: float = 1800.0,
    ) -> None:
        self.trial_cache_registry_repo = trial_cache_registry_repo
        self.trial_cache_registry_pull_timeout_sec = (
            trial_cache_registry_pull_timeout_sec
        )
        self.trial_cache_base_image_pull_timeout_sec = (
            trial_cache_base_image_pull_timeout_sec
        )
        self.trial_cache_ttl_hours = trial_cache_ttl_hours
        self.trial_cache_min_free_gb = trial_cache_min_free_gb
        self.trial_cache_build_lock_timeout_sec = (
            trial_cache_build_lock_timeout_sec
        )


class _StubAdapter:
    def __init__(self, install_script: str | None) -> None:
        self.install_script = install_script
        self.name = "stub"


class _StubCPClient:
    """In-memory cache-slot store mimicking the CP HTTP routes."""

    def __init__(self) -> None:
        self.slots: dict[str, UUID] = {}
        self.refresh_calls: list[tuple[str, UUID]] = []
        self.release_calls: list[tuple[str, UUID]] = []

    async def claim_trial_cache_slot(
        self, cache_key: str, worker_id: UUID, *, ttl_sec: float,
    ) -> bool:
        if cache_key in self.slots:
            return self.slots[cache_key] == worker_id
        self.slots[cache_key] = worker_id
        return True

    async def trial_cache_slot_exists(self, cache_key: str) -> bool:
        return cache_key in self.slots

    async def release_trial_cache_slot(
        self, cache_key: str, worker_id: UUID,
    ) -> None:
        self.release_calls.append((cache_key, worker_id))
        if self.slots.get(cache_key) == worker_id:
            del self.slots[cache_key]

    async def refresh_trial_cache_slot(
        self, cache_key: str, worker_id: UUID, *, ttl_sec: float,
    ) -> bool:
        self.refresh_calls.append((cache_key, worker_id))
        return self.slots.get(cache_key) == worker_id


def _stub_docker(*, locally: dict[str, str] | None = None) -> MagicMock:
    """Mocked docker client. `locally` maps tag → digest for images
    treated as "already pulled" locally."""
    client = MagicMock()
    locally = locally or {}

    def _get(ref: str) -> Any:
        if ref in locally:
            img = MagicMock()
            img.id = locally[ref]
            return img
        raise ImageNotFound(f"image {ref!r} not found locally")

    client.images.get = MagicMock(side_effect=_get)
    return client


# ─── _normalize_install_script ──────────────────────────────────────


@pytest.mark.parametrize("raw", [None, "", "   ", "\n\n", "\t  \n  "])
def test_normalize_install_script_blank_returns_none(raw: str | None) -> None:
    assert trial_cache._normalize_install_script(raw) is None


def test_normalize_install_script_real_passthrough() -> None:
    body = "set -euo pipefail\napt-get install -y curl"
    assert trial_cache._normalize_install_script(body) == body


# ─── _cache_key ─────────────────────────────────────────────────────


def test_cache_key_is_32_hex_chars() -> None:
    key = trial_cache._cache_key(
        task_image_digest="sha256:abc", install_script="pip install foo==1",
    )
    assert len(key) == 32
    assert all(c in "0123456789abcdef" for c in key)


def test_cache_key_changes_when_digest_changes() -> None:
    a = trial_cache._cache_key(
        task_image_digest="sha256:aaa", install_script="x",
    )
    b = trial_cache._cache_key(
        task_image_digest="sha256:bbb", install_script="x",
    )
    assert a != b


def test_cache_key_changes_when_script_changes() -> None:
    a = trial_cache._cache_key(
        task_image_digest="sha256:abc", install_script="x",
    )
    b = trial_cache._cache_key(
        task_image_digest="sha256:abc", install_script="y",
    )
    assert a != b


def test_cache_key_deterministic() -> None:
    a = trial_cache._cache_key(
        task_image_digest="sha256:abc", install_script="x",
    )
    b = trial_cache._cache_key(
        task_image_digest="sha256:abc", install_script="x",
    )
    assert a == b


# ─── _pull_or_get_digest ────────────────────────────────────────────


@pytest.mark.asyncio
async def test_pull_or_get_digest_passthrough_on_digest_input() -> None:
    client = _stub_docker()
    out = await trial_cache._pull_or_get_digest(
        client, "sha256:abc123", timeout_sec=10,
    )
    assert out == "sha256:abc123"
    # No pull call
    client.images.pull.assert_not_called()


@pytest.mark.asyncio
async def test_pull_or_get_digest_inspects_when_locally_present() -> None:
    client = _stub_docker(locally={"python:3.11-slim": "sha256:cached"})
    out = await trial_cache._pull_or_get_digest(
        client, "python:3.11-slim", timeout_sec=10,
    )
    assert out == "sha256:cached"
    client.images.pull.assert_not_called()


@pytest.mark.asyncio
async def test_pull_or_get_digest_pulls_when_missing() -> None:
    pulled = {"called": False, "digest": "sha256:newly-pulled"}

    def _pull(ref: str) -> None:
        pulled["called"] = True

    def _get(ref: str) -> Any:
        if not pulled["called"]:
            raise ImageNotFound(ref)
        img = MagicMock()
        img.id = pulled["digest"]
        return img

    client = MagicMock()
    client.images.pull = MagicMock(side_effect=_pull)
    client.images.get = MagicMock(side_effect=_get)

    out = await trial_cache._pull_or_get_digest(
        client, "python:3.11-slim", timeout_sec=10,
    )
    assert out == "sha256:newly-pulled"
    assert pulled["called"]


@pytest.mark.asyncio
async def test_pull_or_get_digest_timeout_raises_cache_error() -> None:
    def _slow_pull(ref: str) -> None:
        import time
        time.sleep(0.5)  # exceeds the 0.05s timeout below

    client = MagicMock()
    client.images.pull = MagicMock(side_effect=_slow_pull)
    client.images.get = MagicMock(side_effect=ImageNotFound("x"))

    with pytest.raises(trial_cache.TrialCacheError, match="timed out pulling"):
        await trial_cache._pull_or_get_digest(
            client, "python:3.11-slim", timeout_sec=0.05,
        )


# ─── resolve_trial_image ────────────────────────────────────────────


@pytest.mark.asyncio
async def test_resolve_trial_image_no_install_returns_task_image() -> None:
    """Oracle / litellm / in-box adapters have no install_script."""
    out = await trial_cache.resolve_trial_image(
        task_image="python:3.11-slim",
        adapter=_StubAdapter(install_script=None),
        settings=_StubSettings(),
        cp_client=_StubCPClient(),
        worker_id=uuid4(),
        docker_client=_stub_docker(),
    )
    assert out == "python:3.11-slim"


@pytest.mark.asyncio
async def test_resolve_trial_image_blank_install_returns_task_image() -> None:
    out = await trial_cache.resolve_trial_image(
        task_image="python:3.11-slim",
        adapter=_StubAdapter(install_script="   \n  "),
        settings=_StubSettings(),
        cp_client=_StubCPClient(),
        worker_id=uuid4(),
        docker_client=_stub_docker(),
    )
    assert out == "python:3.11-slim"


@pytest.mark.asyncio
async def test_resolve_trial_image_missing_attr_returns_task_image() -> None:
    """Phase 1 ships install_script on 3 adapters; the 9 legacy adapters
    (codex, gemini-cli, etc.) don't yet declare the field. getattr-default
    must keep them passthrough until Phase 2 adds the attribute."""
    class _LegacyAdapter:
        pass

    out = await trial_cache.resolve_trial_image(
        task_image="python:3.11-slim",
        adapter=_LegacyAdapter(),  # type: ignore[arg-type]
        settings=_StubSettings(),
        cp_client=_StubCPClient(),
        worker_id=uuid4(),
        docker_client=_stub_docker(),
    )
    assert out == "python:3.11-slim"


@pytest.mark.asyncio
async def test_resolve_trial_image_local_cache_hit() -> None:
    """When the layered tag already exists locally, no claim needed."""
    digest = "sha256:base-digest"
    install = "set -e\npip install --break-system-packages foo==1.0.0"
    key = trial_cache._cache_key(
        task_image_digest=digest, install_script=install,
    )
    layered_tag = f"loom-trial-cache:{key}"
    client = _stub_docker(locally={
        "python:3.11-slim": digest,
        layered_tag: "sha256:layered",
    })
    cp = _StubCPClient()
    out = await trial_cache.resolve_trial_image(
        task_image="python:3.11-slim",
        adapter=_StubAdapter(install_script=install),
        settings=_StubSettings(),
        cp_client=cp,
        worker_id=uuid4(),
        docker_client=client,
    )
    assert out == layered_tag
    # No slot claim needed
    assert cp.slots == {}


@pytest.mark.asyncio
async def test_resolve_trial_image_builder_path() -> None:
    """Cache miss → claim slot → build → release. Verifies the
    happy-path orchestration (without exercising real Docker build)."""
    digest = "sha256:base"
    install = "echo install\npip install --break-system-packages foo==1"
    client = _stub_docker(locally={"python:3.11-slim": digest})

    # Stub out the build so we don't actually invoke Docker.
    build_calls: list[dict[str, Any]] = []

    def _fake_build(**kwargs: Any) -> None:
        build_calls.append(kwargs)
        # After "building", the image exists locally too.
        client.images.get = MagicMock(side_effect=lambda ref: (
            MagicMock(id="sha256:layered")
            if ref.startswith("loom-trial-cache:") or ref == "python:3.11-slim"
            else (_ for _ in ()).throw(ImageNotFound(ref))
        ))

    import loom_worker.trial_cache as tc

    cp = _StubCPClient()
    worker_id = uuid4()

    # Replace the sync build helper with our stub
    import unittest.mock
    with unittest.mock.patch.object(
        tc, "_build_layered_image_sync", side_effect=_fake_build,
    ):
        out = await tc.resolve_trial_image(
            task_image="python:3.11-slim",
            adapter=_StubAdapter(install_script=install),
            settings=_StubSettings(),
            cp_client=cp,
            worker_id=worker_id,
            docker_client=client,
        )

    expected_key = trial_cache._cache_key(
        task_image_digest=digest, install_script=install,
    )
    assert out == f"loom-trial-cache:{expected_key}"
    assert len(build_calls) == 1
    # Slot released after build
    assert (expected_key, worker_id) in cp.release_calls


@pytest.mark.asyncio
async def test_resolve_trial_image_waiter_path_polls_cheaply() -> None:
    """When another worker holds the slot, the waiter polls via
    `trial_cache_slot_exists` (cheap SELECT) rather than re-firing
    `claim_trial_cache_slot` every iteration."""
    digest = "sha256:base"
    install = "echo install"
    expected_key = trial_cache._cache_key(
        task_image_digest=digest, install_script=install,
    )
    layered_tag = f"loom-trial-cache:{expected_key}"

    other_worker = uuid4()
    cp = _StubCPClient()
    cp.slots[expected_key] = other_worker  # other worker holds the slot

    # After 2 polls, the slot disappears AND the image appears locally
    # (simulating other worker finishing + pushing).
    poll_count = {"n": 0}

    async def fake_sleep(_s: float) -> None:
        poll_count["n"] += 1
        if poll_count["n"] >= 2:
            # Other builder finished
            del cp.slots[expected_key]
            client._has_layered = True

    client = _stub_docker(locally={"python:3.11-slim": digest})
    client._has_layered = False

    original_get = client.images.get

    def _get_with_layered(ref: str) -> Any:
        if ref == layered_tag and client._has_layered:
            img = MagicMock()
            img.id = "sha256:layered"
            return img
        return original_get(ref)

    client.images.get = MagicMock(side_effect=_get_with_layered)

    out = await trial_cache.resolve_trial_image(
        task_image="python:3.11-slim",
        adapter=_StubAdapter(install_script=install),
        settings=_StubSettings(),
        cp_client=cp,
        worker_id=uuid4(),
        docker_client=client,
        sleep=fake_sleep,
    )
    assert out == layered_tag
    # We never became the builder
    assert cp.release_calls == []
