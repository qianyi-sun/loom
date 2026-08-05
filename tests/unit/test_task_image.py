from __future__ import annotations

from pathlib import PurePosixPath
from types import SimpleNamespace
from typing import Any

import pytest
from docker.errors import ImageNotFound

from loom.driver import task_image
from loom.driver.task_image import (
    RUNTIME_ARM64_FALLBACK_BASES,
    TERMINUS_2_FULL_IMAGE,
    TaskImageBuildError,
    dockerfile_uses_runtime_arm64_fallback_base,
    resolve_task_image,
    task_image_tag,
)
from loom.models.task import (
    AgentDefaults,
    EnvironmentConfig,
    TaskConfig,
    TaskMetadata,
    VerifierDefaults,
)


def _task_config(
    *,
    docker_image: str | None = None,
    dockerfile: str | None = None,
    docker_build_context: str | None = None,
) -> TaskConfig:
    return TaskConfig(
        schema_version="1",
        task=TaskMetadata(id="team-bench/task-1", name="Task 1"),
        environment=EnvironmentConfig(
            os="linux",
            docker_image=docker_image,
            dockerfile=(PurePosixPath(dockerfile) if dockerfile else None),
            docker_build_context=(
                PurePosixPath(docker_build_context) if docker_build_context else None
            ),
        ),
        agent=AgentDefaults(name="oracle"),
        verifier=VerifierDefaults(name="pytest"),
    )


class _FakeImages:
    def __init__(
        self,
        *,
        cached: bool = False,
        cached_images: set[str] | None = None,
        image_attrs: dict[str, dict[str, Any]] | None = None,
    ) -> None:
        self.cached = cached
        self.cached_images = cached_images or set()
        self.image_attrs = image_attrs or {}
        self.get_calls: list[str] = []
        self.build_calls: list[dict[str, Any]] = []

    def get(self, image: str) -> object:
        self.get_calls.append(image)
        if self.cached or image in self.cached_images:
            return SimpleNamespace(attrs=self.image_attrs.get(image, {}))
        raise ImageNotFound("missing")

    def build(self, **kwargs: Any) -> tuple[object, list[object]]:
        self.build_calls.append(kwargs)
        tag = kwargs.get("tag")
        if isinstance(tag, str):
            self.cached_images.add(tag)
        return object(), []


class _FakeDockerClient:
    def __init__(
        self,
        images: _FakeImages,
        *,
        info: dict[str, Any] | None = None,
    ) -> None:
        self.images = images
        self._info = info or {}
        self.closed = False

    def info(self) -> dict[str, Any]:
        return self._info

    def close(self) -> None:
        self.closed = True


async def test_resolve_task_image_prefers_explicit_docker_image(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    called = False

    def fail_from_env() -> object:
        nonlocal called
        called = True
        raise AssertionError("docker client should not be created")

    monkeypatch.setattr(task_image.docker, "from_env", fail_from_env)

    image = await resolve_task_image(
        task_config=_task_config(docker_image="python:3.11-alpine"),
        task_dir=tmp_path,
        task_checksum="abc123",
    )

    assert image == "python:3.11-alpine"
    assert called is False


async def test_resolve_task_image_builds_and_caches_dockerfile_image(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    task_dir = tmp_path / "task"
    dockerfile = task_dir / "environment" / "Dockerfile"
    dockerfile.parent.mkdir(parents=True)
    dockerfile.write_text("FROM alpine:3.19\n")
    fake_images = _FakeImages()
    fake_client = _FakeDockerClient(fake_images)
    monkeypatch.setattr(task_image.docker, "from_env", lambda: fake_client)
    cfg = _task_config(dockerfile="environment/Dockerfile")

    image = await resolve_task_image(
        task_config=cfg,
        task_dir=task_dir,
        task_checksum="abc123",
    )

    assert image == task_image_tag(cfg, task_checksum="abc123")
    # First `.get()` is the fast-path cache probe (#275); second is
    # `_ensure_dockerfile_image`'s own pre-build check once the slot
    # has been claimed.
    assert fake_images.get_calls == [image, image]
    assert fake_images.build_calls == [
        {
            "path": str(task_dir),
            "dockerfile": "environment/Dockerfile",
            "tag": image,
            "rm": True,
            "forcerm": True,
            "pull": False,
            "labels": {
                "loom.task_id": "team-bench/task-1",
                "loom.task_checksum": "abc123",
                "loom.task_dockerfile": "environment/Dockerfile",
            },
        }
    ]
    assert fake_client.closed is True


async def test_resolve_task_image_prewarms_terminus_2_base_on_arm64_linux(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    task_dir = tmp_path / "task"
    dockerfile = task_dir / "environment" / "Dockerfile"
    dockerfile.parent.mkdir(parents=True)
    dockerfile.write_text(
        "FROM mictern2/terminus2-full:latest\nRUN echo ready\n",
    )
    fake_images = _FakeImages()
    fake_client = _FakeDockerClient(fake_images)
    monkeypatch.setattr(task_image.docker, "from_env", lambda: fake_client)
    monkeypatch.setattr(
        task_image,
        "platform",
        SimpleNamespace(
            system=lambda: "Linux",
            machine=lambda: "aarch64",
        ),
        raising=False,
    )
    cfg = _task_config(dockerfile="environment/Dockerfile")

    image = await resolve_task_image(
        task_config=cfg,
        task_dir=task_dir,
        task_checksum="abc123",
    )

    # Duplicate `image` entry is the pre-build cache probe (#275); the
    # third element is the terminus-2 base pre-warm check.
    assert fake_images.get_calls == [
        image,
        image,
        "mictern2/terminus2-full:latest",
    ]
    base_build, task_build = fake_images.build_calls
    assert base_build["tag"] == "mictern2/terminus2-full:latest"
    assert base_build["dockerfile"] == "Dockerfile"
    assert base_build["rm"] is True
    assert base_build["forcerm"] is True
    assert base_build["pull"] is False
    assert base_build["labels"] == {
        "loom.managed_base": "terminus-2-arm64",
        "loom.managed_base.upstream": "mictern2/terminus2-full:latest",
    }
    assert task_build["path"] == str(task_dir)
    assert task_build["dockerfile"] == "environment/Dockerfile"
    assert task_build["tag"] == image
    assert fake_client.closed is True


async def test_resolve_task_image_prewarms_terminus_2_base_for_arm64_daemon(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    task_dir = tmp_path / "task"
    dockerfile = task_dir / "environment" / "Dockerfile"
    dockerfile.parent.mkdir(parents=True)
    dockerfile.write_text(
        "FROM mictern2/terminus2-full:latest\nRUN echo ready\n",
    )
    fake_images = _FakeImages()
    fake_client = _FakeDockerClient(
        fake_images,
        info={"OSType": "linux", "Architecture": "aarch64"},
    )
    monkeypatch.setattr(task_image.docker, "from_env", lambda: fake_client)
    monkeypatch.setattr(
        task_image,
        "platform",
        SimpleNamespace(
            system=lambda: "Linux",
            machine=lambda: "x86_64",
        ),
        raising=False,
    )
    cfg = _task_config(dockerfile="environment/Dockerfile")

    await resolve_task_image(
        task_config=cfg,
        task_dir=task_dir,
        task_checksum="abc123",
    )

    assert [call["tag"] for call in fake_images.build_calls] == [
        "mictern2/terminus2-full:latest",
        task_image_tag(cfg, task_checksum="abc123"),
    ]


async def test_resolve_task_image_rebuilds_non_managed_terminus_2_tag_on_arm64(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    task_dir = tmp_path / "task"
    dockerfile = task_dir / "environment" / "Dockerfile"
    dockerfile.parent.mkdir(parents=True)
    dockerfile.write_text("FROM mictern2/terminus2-full:latest\n")
    fake_images = _FakeImages(
        cached_images={"mictern2/terminus2-full:latest"},
        image_attrs={
            "mictern2/terminus2-full:latest": {
                "Architecture": "amd64",
                "Config": {"Labels": {}},
            },
        },
    )
    fake_client = _FakeDockerClient(fake_images)
    monkeypatch.setattr(task_image.docker, "from_env", lambda: fake_client)
    monkeypatch.setattr(
        task_image,
        "platform",
        SimpleNamespace(
            system=lambda: "Linux",
            machine=lambda: "aarch64",
        ),
        raising=False,
    )
    cfg = _task_config(dockerfile="environment/Dockerfile")

    await resolve_task_image(
        task_config=cfg,
        task_dir=task_dir,
        task_checksum="abc123",
    )

    assert [call["tag"] for call in fake_images.build_calls] == [
        "mictern2/terminus2-full:latest",
        task_image_tag(cfg, task_checksum="abc123"),
    ]


async def test_resolve_task_image_passes_docker_api_timeout(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    task_dir = tmp_path / "task"
    dockerfile = task_dir / "environment" / "Dockerfile"
    dockerfile.parent.mkdir(parents=True)
    dockerfile.write_text("FROM alpine:3.19\n")
    fake_images = _FakeImages()
    fake_client = _FakeDockerClient(fake_images)
    from_env_timeouts: list[float | None] = []

    def _from_env(*, timeout: float | None = None) -> _FakeDockerClient:
        from_env_timeouts.append(timeout)
        return fake_client

    monkeypatch.setattr(task_image.docker, "from_env", _from_env)

    await resolve_task_image(
        task_config=_task_config(dockerfile="environment/Dockerfile"),
        task_dir=task_dir,
        task_checksum="abc123",
        docker_api_timeout_sec=900.0,
    )

    # The fast-path cache probe (#275) opens its own docker.from_env
    # client with the same timeout, so we now see two clients created.
    assert from_env_timeouts == [900.0, 900.0]


async def test_resolve_task_image_builds_from_explicit_context(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    task_dir = tmp_path / "task"
    build_context = task_dir / ".loom-build" / "client"
    build_context.mkdir(parents=True)
    (build_context / "Dockerfile").write_text("FROM alpine:3.19\n")
    (build_context / "protected.txt").write_text("build-only\n")
    (task_dir / "instruction.md").write_text("visible to the agent\n")
    fake_images = _FakeImages()
    fake_client = _FakeDockerClient(fake_images)
    monkeypatch.setattr(task_image.docker, "from_env", lambda: fake_client)
    cfg = _task_config(
        dockerfile=".loom-build/client/Dockerfile",
        docker_build_context=".loom-build/client",
    )

    image = await resolve_task_image(
        task_config=cfg,
        task_dir=task_dir,
        task_checksum="abc123",
    )

    assert image == task_image_tag(cfg, task_checksum="abc123")
    assert fake_images.build_calls[0]["path"] == str(build_context)
    assert fake_images.build_calls[0]["dockerfile"] == "Dockerfile"


async def test_resolve_task_image_reuses_cached_dockerfile_image(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    task_dir = tmp_path / "task"
    dockerfile = task_dir / "environment" / "Dockerfile"
    dockerfile.parent.mkdir(parents=True)
    dockerfile.write_text("FROM alpine:3.19\n")
    fake_images = _FakeImages(cached=True)
    fake_client = _FakeDockerClient(fake_images)
    monkeypatch.setattr(task_image.docker, "from_env", lambda: fake_client)

    image = await resolve_task_image(
        task_config=_task_config(dockerfile="environment/Dockerfile"),
        task_dir=task_dir,
        task_checksum="abc123",
    )

    assert image.startswith("loom-task:")
    assert fake_images.get_calls == [image]
    assert fake_images.build_calls == []


async def test_resolve_task_image_rejects_oversized_build_context_files(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    task_dir = tmp_path / "task"
    dockerfile = task_dir / "environment" / "Dockerfile"
    dockerfile.parent.mkdir(parents=True)
    dockerfile.write_text("FROM alpine:3.19\n")
    (task_dir / "instruction.md").write_text("do it\n")
    fake_images = _FakeImages()
    fake_client = _FakeDockerClient(fake_images)
    monkeypatch.setattr(task_image.docker, "from_env", lambda: fake_client)
    monkeypatch.setenv("LOOM_TASK_IMAGE_BUILD_MAX_FILES", "1")

    with pytest.raises(TaskImageBuildError, match="file limit"):
        await resolve_task_image(
            task_config=_task_config(dockerfile="environment/Dockerfile"),
            task_dir=task_dir,
            task_checksum="abc123",
        )

    assert fake_images.build_calls == []
    assert fake_client.closed is True


async def test_resolve_task_image_rejects_oversized_build_context_bytes(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    task_dir = tmp_path / "task"
    dockerfile = task_dir / "environment" / "Dockerfile"
    dockerfile.parent.mkdir(parents=True)
    dockerfile.write_text("FROM alpine:3.19\n")
    fake_images = _FakeImages()
    fake_client = _FakeDockerClient(fake_images)
    monkeypatch.setattr(task_image.docker, "from_env", lambda: fake_client)
    monkeypatch.setenv("LOOM_TASK_IMAGE_BUILD_MAX_BYTES", "1")

    with pytest.raises(TaskImageBuildError, match="byte limit"):
        await resolve_task_image(
            task_config=_task_config(dockerfile="environment/Dockerfile"),
            task_dir=task_dir,
            task_checksum="abc123",
        )

    assert fake_images.build_calls == []
    assert fake_client.closed is True


async def test_resolve_task_image_rejects_missing_dockerfile(tmp_path) -> None:
    task_dir = tmp_path / "task"
    task_dir.mkdir()

    with pytest.raises(TaskImageBuildError, match="environment/Dockerfile"):
        await resolve_task_image(
            task_config=_task_config(dockerfile="environment/Dockerfile"),
            task_dir=task_dir,
            task_checksum="abc123",
        )


async def test_resolve_task_image_rejects_dockerfile_path_traversal(tmp_path) -> None:
    task_dir = tmp_path / "task"
    task_dir.mkdir()

    with pytest.raises(TaskImageBuildError, match="inside the task bundle"):
        await resolve_task_image(
            task_config=_task_config(dockerfile="../Dockerfile"),
            task_dir=task_dir,
            task_checksum="abc123",
        )


class _BuildErrorImages:
    """Docker images namespace whose .build() raises BuildError with
    a populated build_log (Phase 3a #319 — tail surfaces in error)."""

    def __init__(self, *, log_lines: list[dict[str, Any]], reason: str = "RUN failed") -> None:
        self._log = log_lines
        self._reason = reason

    def get(self, image: str) -> object:
        raise ImageNotFound("missing")

    def build(self, **kwargs: Any) -> tuple[object, list[object]]:
        from docker.errors import BuildError

        raise BuildError(reason=self._reason, build_log=iter(self._log))


async def test_resolve_task_image_surfaces_build_log_tail(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    """When `docker build` fails, the TaskImageBuildError must include
    the tail of the build_log so operators see WHY the RUN failed
    (e.g. pip's actual stderr), not just the failing command. Real
    motivating case from #316: `pip install pytest-jsonreport` failed
    because the canonical package name is `pytest-json-report`."""
    task_dir = tmp_path / "task"
    (task_dir / "environment").mkdir(parents=True)
    (task_dir / "environment" / "Dockerfile").write_text(
        "FROM python:3.11-slim\nRUN pip install pytest-jsonreport\n",
    )

    log_lines = [
        {"stream": "Step 1/2 : FROM python:3.11-slim\n"},
        {"stream": "Step 2/2 : RUN pip install pytest-jsonreport\n"},
        {
            "stream": "ERROR: Could not find a version that satisfies the "
            "requirement pytest-jsonreport\n"
        },
        {"stream": "ERROR: No matching distribution found for pytest-jsonreport\n"},
    ]
    fake_images = _BuildErrorImages(log_lines=log_lines)
    fake_client = _FakeDockerClient(fake_images)
    monkeypatch.setattr(task_image.docker, "from_env", lambda: fake_client)

    with pytest.raises(TaskImageBuildError) as exc:
        await resolve_task_image(
            task_config=_task_config(dockerfile="environment/Dockerfile"),
            task_dir=task_dir,
            task_checksum="abc123",
        )

    msg = str(exc.value)
    assert "failed to build Docker image" in msg
    assert "build log" in msg.lower()
    assert "No matching distribution" in msg
    assert "pytest-jsonreport" in msg
    assert fake_client.closed is True


async def test_resolve_task_image_truncates_build_log_to_tail(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    """Build logs from real Dockerfiles can be hundreds of lines;
    only the trailing _BUILD_LOG_TAIL_LINES are surfaced so the
    error message fits in API/SPA fields."""
    task_dir = tmp_path / "task"
    (task_dir / "environment").mkdir(parents=True)
    (task_dir / "environment" / "Dockerfile").write_text(
        "FROM scratch\n",
    )

    # 200 lines of noise, then the actual error
    noise = [{"stream": f"noise line {i}\n"} for i in range(200)]
    error = [{"stream": "FINAL_ERROR: this should appear\n"}]
    fake_images = _BuildErrorImages(log_lines=noise + error)
    fake_client = _FakeDockerClient(fake_images)
    monkeypatch.setattr(task_image.docker, "from_env", lambda: fake_client)

    with pytest.raises(TaskImageBuildError) as exc:
        await resolve_task_image(
            task_config=_task_config(dockerfile="environment/Dockerfile"),
            task_dir=task_dir,
            task_checksum="abc123",
        )

    msg = str(exc.value)
    assert "FINAL_ERROR" in msg
    # Early noise shouldn't survive truncation
    assert "noise line 0" not in msg
    assert "noise line 50" not in msg
    assert exc.value.diagnostic_detail is not None
    assert "noise line 0" in exc.value.diagnostic_detail
    assert "noise line 50" in exc.value.diagnostic_detail
    assert "FINAL_ERROR" in exc.value.diagnostic_detail


async def test_resolve_task_image_empty_build_log_still_raises(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    """If BuildError has no log (rare — happens when docker daemon
    rejects the build before starting), we still raise TaskImageBuildError
    with the docker-py reason and don't append an empty 'build log:' section."""
    task_dir = tmp_path / "task"
    (task_dir / "environment").mkdir(parents=True)
    (task_dir / "environment" / "Dockerfile").write_text("FROM scratch\n")

    fake_images = _BuildErrorImages(log_lines=[], reason="daemon rejected build")
    fake_client = _FakeDockerClient(fake_images)
    monkeypatch.setattr(task_image.docker, "from_env", lambda: fake_client)

    with pytest.raises(TaskImageBuildError) as exc:
        await resolve_task_image(
            task_config=_task_config(dockerfile="environment/Dockerfile"),
            task_dir=task_dir,
            task_checksum="abc123",
        )

    msg = str(exc.value)
    assert "daemon rejected build" in msg
    assert "build log" not in msg.lower()


class TestRuntimeArm64FallbackBases:
    """Registry + Dockerfile probe for #342."""

    def test_terminus_2_full_is_a_fallback_base(self) -> None:
        assert TERMINUS_2_FULL_IMAGE in RUNTIME_ARM64_FALLBACK_BASES

    def test_dockerfile_with_terminus_2_from_is_detected(self, tmp_path) -> None:
        dockerfile = tmp_path / "Dockerfile"
        dockerfile.write_text(
            f"# comment\nFROM {TERMINUS_2_FULL_IMAGE}\nRUN echo ok\n",
        )
        assert dockerfile_uses_runtime_arm64_fallback_base(dockerfile) is True

    def test_dockerfile_with_docker_io_qualifier_is_detected(
        self,
        tmp_path,
    ) -> None:
        dockerfile = tmp_path / "Dockerfile"
        dockerfile.write_text(
            f"FROM docker.io/{TERMINUS_2_FULL_IMAGE}\n",
        )
        assert dockerfile_uses_runtime_arm64_fallback_base(dockerfile) is True

    def test_dockerfile_with_unrelated_base_is_not_detected(
        self,
        tmp_path,
    ) -> None:
        dockerfile = tmp_path / "Dockerfile"
        dockerfile.write_text("FROM python:3.11-slim\nRUN echo ok\n")
        assert dockerfile_uses_runtime_arm64_fallback_base(dockerfile) is False

    def test_dockerfile_with_arg_before_from_still_detected(
        self,
        tmp_path,
    ) -> None:
        dockerfile = tmp_path / "Dockerfile"
        dockerfile.write_text(
            f"ARG VERSION=1\nFROM {TERMINUS_2_FULL_IMAGE}\n",
        )
        assert dockerfile_uses_runtime_arm64_fallback_base(dockerfile) is True


# ──────────────────────────────────────────────────────────────────────
# #275: build slot serialization
# ──────────────────────────────────────────────────────────────────────


import contextlib as _contextlib  # noqa: E402


class _TrackingSlot:
    """Fake `BuildSlotProvider` returning an async context manager that
    records enter/exit calls."""

    def __init__(self) -> None:
        self.enter_calls = 0
        self.exit_calls = 0

    def __call__(self) -> _contextlib.AbstractAsyncContextManager[None]:
        outer = self

        @_contextlib.asynccontextmanager
        async def _ctx():
            outer.enter_calls += 1
            try:
                yield
            finally:
                outer.exit_calls += 1

        return _ctx()


async def test_resolve_task_image_enters_build_slot_only_when_building(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    """#275: on a cold task cache the daemon build slot must be
    entered before the Docker build starts, so a burst of trials can
    only run N=max_concurrent apt-get / dpkg operations at once."""
    task_dir = tmp_path / "task"
    dockerfile = task_dir / "environment" / "Dockerfile"
    dockerfile.parent.mkdir(parents=True)
    dockerfile.write_text("FROM alpine:3.19\n")
    fake_images = _FakeImages()
    fake_client = _FakeDockerClient(fake_images)
    monkeypatch.setattr(task_image.docker, "from_env", lambda: fake_client)
    slot = _TrackingSlot()

    image = await resolve_task_image(
        task_config=_task_config(dockerfile="environment/Dockerfile"),
        task_dir=task_dir,
        task_checksum="abc123",
        build_slot_provider=slot,
    )

    assert image.startswith("loom-task:")
    assert slot.enter_calls == 1
    assert slot.exit_calls == 1
    assert fake_images.build_calls, "build should have run inside the slot"


async def test_resolve_task_image_skips_slot_on_cache_hit(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    """#275 fast path: an already-cached task image must NOT enter the
    build slot. Steady-state trial dispatch must not pay the slot HTTP
    round-trip on every trial once the image is warm."""
    task_dir = tmp_path / "task"
    dockerfile = task_dir / "environment" / "Dockerfile"
    dockerfile.parent.mkdir(parents=True)
    dockerfile.write_text("FROM alpine:3.19\n")
    fake_images = _FakeImages(cached=True)
    fake_client = _FakeDockerClient(fake_images)
    monkeypatch.setattr(task_image.docker, "from_env", lambda: fake_client)
    slot = _TrackingSlot()

    await resolve_task_image(
        task_config=_task_config(dockerfile="environment/Dockerfile"),
        task_dir=task_dir,
        task_checksum="abc123",
        build_slot_provider=slot,
    )

    assert slot.enter_calls == 0
    assert slot.exit_calls == 0
    assert fake_images.build_calls == []


async def test_resolve_task_image_skips_slot_when_docker_image_declared(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    """A task declaring `environment.docker_image` never triggers a
    build, so the slot must never be entered — the check must short-
    circuit before any docker.from_env call happens."""
    slot = _TrackingSlot()
    result = await resolve_task_image(
        task_config=_task_config(docker_image="alpine:3.19"),
        task_dir=tmp_path,
        task_checksum="abc123",
        build_slot_provider=slot,
    )
    assert result == "alpine:3.19"
    assert slot.enter_calls == 0
    assert slot.exit_calls == 0


async def test_resolve_task_image_releases_slot_on_build_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    """Slot must be released even when the Docker build raises, so a
    failed setup can't leak a daemon-wide build slot indefinitely and
    starve subsequent trials."""
    task_dir = tmp_path / "task"
    dockerfile = task_dir / "environment" / "Dockerfile"
    dockerfile.parent.mkdir(parents=True)
    dockerfile.write_text("FROM alpine:3.19\n")

    class _RaisingImages(_FakeImages):
        def build(self, **_kwargs: object) -> object:  # type: ignore[override]
            from docker.errors import BuildError

            raise BuildError("simulated", build_log=iter([]))

    fake_client = _FakeDockerClient(_RaisingImages())
    monkeypatch.setattr(task_image.docker, "from_env", lambda: fake_client)
    slot = _TrackingSlot()

    with pytest.raises(task_image.TaskImageBuildError):
        await resolve_task_image(
            task_config=_task_config(dockerfile="environment/Dockerfile"),
            task_dir=task_dir,
            task_checksum="abc123",
            build_slot_provider=slot,
        )

    assert slot.enter_calls == 1
    assert slot.exit_calls == 1, (
        "slot must be released via the async-context exit path even when the build raises"
    )


# ── #1169: base task-image registry pull/push (contained pools) ──────────────


class _RegistryFakeImages:
    """Fake docker images store with registry pull/push/tag support."""

    def __init__(self, *, local: set[str] | None = None, registry: set[str] | None = None) -> None:
        self.local: set[str] = set(local or ())
        self.registry: set[str] = set(registry or ())
        self.get_calls: list[str] = []
        self.pull_calls: list[str] = []
        self.push_calls: list[tuple[str, str]] = []
        self.build_calls: list[dict[str, Any]] = []

    def get(self, ref: str) -> object:
        self.get_calls.append(ref)
        if ref in self.local:
            store = self

            class _Img:
                def tag(self, repository: str, tag: str) -> bool:
                    store.local.add(f"{repository}:{tag}")
                    return True

            return _Img()
        raise ImageNotFound("missing")

    def pull(self, ref: str) -> object:
        self.pull_calls.append(ref)
        if ref in self.registry:
            self.local.add(ref)
            return SimpleNamespace()
        raise ImageNotFound("registry miss")

    def build(self, **kwargs: Any) -> tuple[object, list[object]]:
        self.build_calls.append(kwargs)
        tag = kwargs.get("tag")
        if isinstance(tag, str):
            self.local.add(tag)
        return object(), []

    def push(self, repository: str, tag: str, stream: bool = True, decode: bool = True):
        self.push_calls.append((repository, tag))
        self.registry.add(f"{repository}:{tag}")
        return iter([{"status": "Pushed"}])


def test_registry_tag_for_splits_on_last_colon_for_ported_registry() -> None:
    assert (
        task_image._registry_tag_for("loom-task:deadbeef", "192.168.50.13:5000/loom-task")
        == "192.168.50.13:5000/loom-task:deadbeef"
    )


async def test_contained_worker_pulls_base_image_from_registry_instead_of_building(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    task_dir = tmp_path / "task"
    dockerfile = task_dir / "environment" / "Dockerfile"
    dockerfile.parent.mkdir(parents=True)
    dockerfile.write_text("FROM alpine:3.19\n")
    cfg = _task_config(dockerfile="environment/Dockerfile")
    tag = task_image_tag(cfg, task_checksum="abc123")
    registry_repo = "192.168.50.13:5000/loom-task"
    registry_tag = f"{registry_repo}:{tag.rpartition(':')[2]}"
    images = _RegistryFakeImages(registry={registry_tag})  # present in registry, not local
    monkeypatch.setattr(task_image.docker, "from_env", lambda *a, **k: _FakeDockerClient(images))

    image = await resolve_task_image(
        task_config=cfg,
        task_dir=task_dir,
        task_checksum="abc123",
        require_containment=True,  # a build would be refused
        registry_repo=registry_repo,
    )

    assert image == tag
    assert images.pull_calls == [registry_tag]  # pulled...
    assert images.build_calls == []  # ...never built (which would have been refused)


async def test_non_contained_builder_pushes_base_image_to_registry_on_miss(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    task_dir = tmp_path / "task"
    dockerfile = task_dir / "environment" / "Dockerfile"
    dockerfile.parent.mkdir(parents=True)
    dockerfile.write_text("FROM alpine:3.19\n")
    cfg = _task_config(dockerfile="environment/Dockerfile")
    tag = task_image_tag(cfg, task_checksum="abc123")
    registry_repo = "192.168.50.13:5000/loom-task"
    key = tag.rpartition(":")[2]
    images = _RegistryFakeImages()  # empty registry → pull miss → build → push
    monkeypatch.setattr(task_image.docker, "from_env", lambda *a, **k: _FakeDockerClient(images))

    image = await resolve_task_image(
        task_config=cfg,
        task_dir=task_dir,
        task_checksum="abc123",
        require_containment=False,
        registry_repo=registry_repo,
    )

    assert image == tag
    assert images.build_calls  # built locally
    assert images.push_calls == [(registry_repo, key)]  # populated registry for pullers


async def test_contained_worker_refuses_build_when_registry_misses(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    from loom.driver.build_containment import ImageBuildForbiddenError

    task_dir = tmp_path / "task"
    dockerfile = task_dir / "environment" / "Dockerfile"
    dockerfile.parent.mkdir(parents=True)
    dockerfile.write_text("FROM alpine:3.19\n")
    cfg = _task_config(dockerfile="environment/Dockerfile")
    images = _RegistryFakeImages()  # empty registry → miss → containment refuses to build
    monkeypatch.setattr(task_image.docker, "from_env", lambda *a, **k: _FakeDockerClient(images))

    with pytest.raises(ImageBuildForbiddenError):
        await resolve_task_image(
            task_config=cfg,
            task_dir=task_dir,
            task_checksum="abc123",
            require_containment=True,
            registry_repo="192.168.50.13:5000/loom-task",
        )
    assert images.pull_calls  # tried the registry before refusing
    assert images.build_calls == []


async def test_no_registry_configured_keeps_local_only_behaviour(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    task_dir = tmp_path / "task"
    dockerfile = task_dir / "environment" / "Dockerfile"
    dockerfile.parent.mkdir(parents=True)
    dockerfile.write_text("FROM alpine:3.19\n")
    cfg = _task_config(dockerfile="environment/Dockerfile")
    images = _RegistryFakeImages()
    monkeypatch.setattr(task_image.docker, "from_env", lambda *a, **k: _FakeDockerClient(images))

    await resolve_task_image(task_config=cfg, task_dir=task_dir, task_checksum="abc123")
    # registry_repo defaults None → no pull, no push (unchanged behaviour).
    assert images.pull_calls == []
    assert images.push_calls == []
