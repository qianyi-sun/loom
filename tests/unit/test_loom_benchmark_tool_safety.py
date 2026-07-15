"""upload_task_dir + import_cmd guard against poisoned upstream
records that could leak files outside the benchmark namespace or
smuggle path-traversal into the S3 prefix (Plan 14 audit follow-ups)."""

from __future__ import annotations

import json
import stat
from pathlib import Path

import pytest

from loom.trajectory.storage import BUNDLE_FILE_METADATA_NAME, FakeObjectStore
from loom_benchmark_tool.import_cmd import _validate_instance_id
from loom_benchmark_tool.upload import upload_task_dir


async def test_upload_task_dir_rejects_empty_prefix(tmp_path: Path) -> None:
    (tmp_path / "task.toml").write_text("x = 1\n")
    with pytest.raises(ValueError, match="non-empty prefix"):
        await upload_task_dir(
            store=FakeObjectStore(),
            bucket="b",
            prefix="",
            task_dir=tmp_path,
        )


async def test_upload_task_dir_rejects_traversal_prefix(tmp_path: Path) -> None:
    (tmp_path / "task.toml").write_text("x = 1\n")
    with pytest.raises(ValueError, match="traversal"):
        await upload_task_dir(
            store=FakeObjectStore(),
            bucket="b",
            prefix="humaneval/../escape/",
            task_dir=tmp_path,
        )


async def test_upload_task_dir_rejects_absolute_prefix(tmp_path: Path) -> None:
    (tmp_path / "task.toml").write_text("x = 1\n")
    with pytest.raises(ValueError, match="traversal or absolute"):
        await upload_task_dir(
            store=FakeObjectStore(),
            bucket="b",
            prefix="/escape/",
            task_dir=tmp_path,
        )


async def test_upload_task_dir_normal_prefix_works(tmp_path: Path) -> None:
    (tmp_path / "task.toml").write_text("x = 1\n")
    (tmp_path / "solution").mkdir()
    (tmp_path / "solution" / "s.py").write_text("pass\n")
    store = FakeObjectStore()
    n = await upload_task_dir(
        store=store,
        bucket="b",
        prefix="humaneval/HumanEval/0/",
        task_dir=tmp_path,
    )
    assert n == 2
    assert ("b", "humaneval/HumanEval/0/task.toml") in store.objects
    assert ("b", "humaneval/HumanEval/0/solution/s.py") in store.objects


async def test_upload_download_roundtrip_restores_executable_mode(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "task.toml").write_text("x = 1\n")
    (source / "verifier").mkdir()
    verifier = source / "verifier" / "run-tests.sh"
    verifier.write_text("#!/bin/sh\nexit 0\n")
    verifier.chmod(0o751)
    store = FakeObjectStore()

    count = await upload_task_dir(
        store=store,
        bucket="b",
        prefix="terminal-bench/task/",
        task_dir=source,
    )
    out = tmp_path / "out"
    downloaded = await store.download_prefix(
        bucket="b",
        prefix="terminal-bench/task/",
        out_dir=out,
    )

    assert count == downloaded == 2
    assert stat.S_IMODE((out / "task.toml").stat().st_mode) == 0o644
    assert stat.S_IMODE((out / "verifier" / "run-tests.sh").stat().st_mode) == 0o755
    metadata = json.loads(
        store.objects[("b", f"terminal-bench/task/{BUNDLE_FILE_METADATA_NAME}")],
    )
    assert metadata["files"]["verifier/run-tests.sh"] == {"mode": "0755"}


async def test_download_rejects_unsafe_file_mode_metadata(tmp_path: Path) -> None:
    store = FakeObjectStore()
    prefix = "terminal-bench/task/"
    store.objects[("b", f"{prefix}task.toml")] = b"x = 1\n"
    store.objects[("b", f"{prefix}{BUNDLE_FILE_METADATA_NAME}")] = json.dumps(
        {
            "schema_version": 1,
            "files": {"task.toml": {"mode": "4755"}},
        },
    ).encode()

    with pytest.raises(ValueError, match="unsafe file mode"):
        await store.download_prefix(bucket="b", prefix=prefix, out_dir=tmp_path / "out")


async def test_upload_task_dir_rejects_pytorch_index_as_sole_index_for_pypi_deps(
    tmp_path: Path,
) -> None:
    dockerfile = tmp_path / "environment" / "Dockerfile"
    dockerfile.parent.mkdir()
    dockerfile.write_text(
        "FROM python:3.13-bookworm\n"
        "RUN pip install torch torchvision pyyaml "
        "--index-url https://download.pytorch.org/whl/cpu\n",
    )
    (tmp_path / "task.toml").write_text("x = 1\n")

    with pytest.raises(ValueError, match="package-specific pip index"):
        await upload_task_dir(
            store=FakeObjectStore(),
            bucket="b",
            prefix="source-useful/task/",
            task_dir=tmp_path,
        )


async def test_upload_task_dir_rejects_pip3_pytorch_sole_index(
    tmp_path: Path,
) -> None:
    dockerfile = tmp_path / "environment" / "Dockerfile"
    dockerfile.parent.mkdir()
    dockerfile.write_text(
        "FROM python:3.13-bookworm\n"
        "RUN pip3 install torch pyyaml "
        "--index-url https://download.pytorch.org/whl/cpu\n",
    )
    (tmp_path / "task.toml").write_text("x = 1\n")

    with pytest.raises(ValueError, match="package-specific pip index"):
        await upload_task_dir(
            store=FakeObjectStore(),
            bucket="b",
            prefix="source-useful/task/",
            task_dir=tmp_path,
        )


async def test_upload_task_dir_rejects_python_versioned_module_pip_pytorch_sole_index(
    tmp_path: Path,
) -> None:
    dockerfile = tmp_path / "environment" / "Dockerfile"
    dockerfile.parent.mkdir()
    dockerfile.write_text(
        "FROM python:3.13-bookworm\n"
        "RUN python3.13 -m pip install torch pyyaml "
        "--index-url https://download.pytorch.org/whl/cpu\n",
    )
    (tmp_path / "task.toml").write_text("x = 1\n")

    with pytest.raises(ValueError, match="package-specific pip index"):
        await upload_task_dir(
            store=FakeObjectStore(),
            bucket="b",
            prefix="source-useful/task/",
            task_dir=tmp_path,
        )


async def test_upload_task_dir_allows_pytorch_extra_index_for_pypi_deps(
    tmp_path: Path,
) -> None:
    dockerfile = tmp_path / "environment" / "Dockerfile"
    dockerfile.parent.mkdir()
    dockerfile.write_text(
        "FROM python:3.13-bookworm\n"
        "RUN pip install torch torchvision pyyaml "
        "--extra-index-url https://download.pytorch.org/whl/cpu\n",
    )
    (tmp_path / "task.toml").write_text("x = 1\n")

    count = await upload_task_dir(
        store=FakeObjectStore(),
        bucket="b",
        prefix="source-useful/task/",
        task_dir=tmp_path,
    )

    assert count == 2


async def test_upload_task_dir_rejects_moving_npm_latest_with_fixed_node_major(
    tmp_path: Path,
) -> None:
    dockerfile = tmp_path / "environment" / "Dockerfile"
    dockerfile.parent.mkdir()
    dockerfile.write_text(
        "FROM node:18-bookworm\nRUN npm install -g npm@latest\n",
    )
    (tmp_path / "task.toml").write_text("x = 1\n")

    with pytest.raises(ValueError, match="npm@latest"):
        await upload_task_dir(
            store=FakeObjectStore(),
            bucket="b",
            prefix="source-useful/task/",
            task_dir=tmp_path,
        )


async def test_upload_task_dir_rejects_dns_runtime_mutation(
    tmp_path: Path,
) -> None:
    dockerfile = tmp_path / "environment" / "Dockerfile"
    dockerfile.parent.mkdir()
    dockerfile.write_text(
        "FROM debian:bookworm\n"
        "COPY broken_resolv.conf /app/broken_resolv.conf\n"
        "RUN cp /app/broken_resolv.conf /etc/resolv.conf\n",
    )
    (tmp_path / "task.toml").write_text("x = 1\n")

    with pytest.raises(ValueError, match="TASK_COMPAT_DNS_MUTATION"):
        await upload_task_dir(
            store=FakeObjectStore(),
            bucket="b",
            prefix="source-useful/task/",
            task_dir=tmp_path,
        )


async def test_upload_task_dir_rejects_environment_app_path_mismatch(
    tmp_path: Path,
) -> None:
    dockerfile = tmp_path / "environment" / "Dockerfile"
    dockerfile.parent.mkdir()
    dockerfile.write_text(
        "FROM debian:bookworm\n"
        "COPY . /app/\n"
        "RUN chmod +x /app/setup_repo.sh && /app/setup_repo.sh\n",
    )
    (tmp_path / "environment" / "setup_repo.sh").write_text("#!/bin/sh\n")
    (tmp_path / "task.toml").write_text("x = 1\n")

    with pytest.raises(ValueError, match="TASK_COMPAT_APP_PATH_MISSING"):
        await upload_task_dir(
            store=FakeObjectStore(),
            bucket="b",
            prefix="source-useful/task/",
            task_dir=tmp_path,
        )


async def test_upload_task_dir_rejects_moving_npm_latest_after_nodesource_setup(
    tmp_path: Path,
) -> None:
    dockerfile = tmp_path / "environment" / "Dockerfile"
    dockerfile.parent.mkdir()
    dockerfile.write_text(
        "FROM ubuntu:24.04\n"
        "RUN curl -fsSL https://deb.nodesource.com/setup_18.x | bash - "
        "&& npm install -g npm@latest\n",
    )
    (tmp_path / "task.toml").write_text("x = 1\n")

    with pytest.raises(ValueError, match="Node 18"):
        await upload_task_dir(
            store=FakeObjectStore(),
            bucket="b",
            prefix="source-useful/task/",
            task_dir=tmp_path,
        )


async def test_upload_task_dir_rejects_npm_i_latest_with_fixed_node_major(
    tmp_path: Path,
) -> None:
    dockerfile = tmp_path / "environment" / "Dockerfile"
    dockerfile.parent.mkdir()
    dockerfile.write_text(
        "FROM node:18-bookworm\nRUN npm i -g npm@latest\n",
    )
    (tmp_path / "task.toml").write_text("x = 1\n")

    with pytest.raises(ValueError, match="npm@latest"):
        await upload_task_dir(
            store=FakeObjectStore(),
            bucket="b",
            prefix="source-useful/task/",
            task_dir=tmp_path,
        )


async def test_upload_task_dir_resets_node_major_on_new_stage(
    tmp_path: Path,
) -> None:
    dockerfile = tmp_path / "environment" / "Dockerfile"
    dockerfile.parent.mkdir()
    dockerfile.write_text(
        "FROM node:18-bookworm AS frontend\n"
        "RUN node --version\n"
        "FROM ubuntu:24.04\n"
        "RUN npm install -g npm@latest\n",
    )
    (tmp_path / "task.toml").write_text("x = 1\n")

    count = await upload_task_dir(
        store=FakeObjectStore(),
        bucket="b",
        prefix="source-useful/task/",
        task_dir=tmp_path,
    )

    assert count == 2


async def test_upload_task_dir_reports_nodesource_major_from_current_stage(
    tmp_path: Path,
) -> None:
    dockerfile = tmp_path / "environment" / "Dockerfile"
    dockerfile.parent.mkdir()
    dockerfile.write_text(
        "FROM node:22-bookworm AS frontend\n"
        "RUN node --version\n"
        "FROM ubuntu:24.04\n"
        "RUN curl -fsSL https://deb.nodesource.com/setup_18.x | bash - "
        "&& npm install -g npm@latest\n",
    )
    (tmp_path / "task.toml").write_text("x = 1\n")

    with pytest.raises(ValueError, match="Node 18"):
        await upload_task_dir(
            store=FakeObjectStore(),
            bucket="b",
            prefix="source-useful/task/",
            task_dir=tmp_path,
        )


async def test_upload_task_dir_rejects_setup_copy_without_app_parent(
    tmp_path: Path,
) -> None:
    dockerfile = tmp_path / "environment" / "Dockerfile"
    dockerfile.parent.mkdir()
    dockerfile.write_text(
        "FROM ubuntu:24.04\nRUN mkdir -p /tmp/setup && cp -r /tmp/setup /app/project\n",
    )
    (tmp_path / "task.toml").write_text("x = 1\n")

    with pytest.raises(ValueError, match="/app"):
        await upload_task_dir(
            store=FakeObjectStore(),
            bucket="b",
            prefix="source-useful/task/",
            task_dir=tmp_path,
        )


async def test_upload_task_dir_rejects_tmp_app_mkdir_before_app_copy(
    tmp_path: Path,
) -> None:
    dockerfile = tmp_path / "environment" / "Dockerfile"
    dockerfile.parent.mkdir()
    dockerfile.write_text(
        "FROM ubuntu:24.04\nRUN mkdir -p /tmp/app && cp -r /tmp/setup /app/project\n",
    )
    (tmp_path / "task.toml").write_text("x = 1\n")

    with pytest.raises(ValueError, match="before creating the /app"):
        await upload_task_dir(
            store=FakeObjectStore(),
            bucket="b",
            prefix="source-useful/task/",
            task_dir=tmp_path,
        )


async def test_upload_task_dir_rejects_non_recursive_app_child_mkdir(
    tmp_path: Path,
) -> None:
    dockerfile = tmp_path / "environment" / "Dockerfile"
    dockerfile.parent.mkdir()
    dockerfile.write_text(
        "FROM ubuntu:24.04\nRUN mkdir /app/project && cp -r /tmp/setup /app/project\n",
    )
    (tmp_path / "task.toml").write_text("x = 1\n")

    with pytest.raises(ValueError, match="before creating the /app"):
        await upload_task_dir(
            store=FakeObjectStore(),
            bucket="b",
            prefix="source-useful/task/",
            task_dir=tmp_path,
        )


async def test_upload_task_dir_rejects_setup_copy_before_late_app_mkdir(
    tmp_path: Path,
) -> None:
    dockerfile = tmp_path / "environment" / "Dockerfile"
    dockerfile.parent.mkdir()
    dockerfile.write_text(
        "FROM ubuntu:24.04\nRUN cp -r /tmp/setup /app/project && mkdir -p /app\n",
    )
    (tmp_path / "task.toml").write_text("x = 1\n")

    with pytest.raises(ValueError, match="before creating the /app"):
        await upload_task_dir(
            store=FakeObjectStore(),
            bucket="b",
            prefix="source-useful/task/",
            task_dir=tmp_path,
        )


async def test_upload_task_dir_resets_app_parent_on_new_stage(
    tmp_path: Path,
) -> None:
    dockerfile = tmp_path / "environment" / "Dockerfile"
    dockerfile.parent.mkdir()
    dockerfile.write_text(
        "FROM ubuntu:24.04 AS base\n"
        "RUN mkdir -p /app\n"
        "FROM ubuntu:24.04\n"
        "RUN cp -r /tmp/setup /app/project\n",
    )
    (tmp_path / "task.toml").write_text("x = 1\n")

    with pytest.raises(ValueError, match="before creating the /app"):
        await upload_task_dir(
            store=FakeObjectStore(),
            bucket="b",
            prefix="source-useful/task/",
            task_dir=tmp_path,
        )


async def test_upload_task_dir_rejects_setup_archive_without_app_parent(
    tmp_path: Path,
) -> None:
    dockerfile = tmp_path / "environment" / "Dockerfile"
    dockerfile.parent.mkdir()
    dockerfile.write_text(
        "FROM ubuntu:24.04\nRUN tar -czf /app/base-fs.tar.gz /tmp/setup\n",
    )
    (tmp_path / "task.toml").write_text("x = 1\n")

    with pytest.raises(ValueError, match="/app"):
        await upload_task_dir(
            store=FakeObjectStore(),
            bucket="b",
            prefix="source-useful/task/",
            task_dir=tmp_path,
        )


async def test_upload_task_dir_rejects_broad_trailing_true_after_setup_chain(
    tmp_path: Path,
) -> None:
    dockerfile = tmp_path / "environment" / "Dockerfile"
    dockerfile.parent.mkdir()
    dockerfile.write_text(
        "FROM ubuntu:24.04\n"
        "RUN mkdir -p /app && cp -r /tmp/setup /app/project "
        "&& cd /app/project && git remote remove origin 2>/dev/null || true\n",
    )
    (tmp_path / "task.toml").write_text("x = 1\n")

    with pytest.raises(ValueError, match=r"trailing .*\|\| true"):
        await upload_task_dir(
            store=FakeObjectStore(),
            bucket="b",
            prefix="source-useful/task/",
            task_dir=tmp_path,
        )


async def test_upload_task_dir_allows_scoped_optional_git_remote_remove(
    tmp_path: Path,
) -> None:
    dockerfile = tmp_path / "environment" / "Dockerfile"
    dockerfile.parent.mkdir()
    dockerfile.write_text(
        "FROM ubuntu:24.04\n"
        "RUN mkdir -p /app && cp -r /tmp/setup /app/project "
        "&& cd /app/project && (git remote remove origin 2>/dev/null || true)\n",
    )
    (tmp_path / "task.toml").write_text("x = 1\n")

    count = await upload_task_dir(
        store=FakeObjectStore(),
        bucket="b",
        prefix="source-useful/task/",
        task_dir=tmp_path,
    )

    assert count == 2


def test_validate_instance_id_accepts_normal_ids() -> None:
    _validate_instance_id("HumanEval/0")
    _validate_instance_id("inst-1")
    _validate_instance_id("swe-bench-verified/django__django-12345")
    _validate_instance_id("MMLU/abstract_algebra/0")
    _validate_instance_id("v1.0+r2")


def test_validate_instance_id_rejects_traversal() -> None:
    with pytest.raises(ValueError, match=r"empty / \.\. / \. segments"):
        _validate_instance_id("..")
    with pytest.raises(ValueError, match=r"empty / \.\. / \. segments"):
        _validate_instance_id("foo/../bar")
    with pytest.raises(ValueError, match=r"empty / \.\. / \. segments"):
        _validate_instance_id("foo/./bar")
    with pytest.raises(ValueError, match=r"empty / \.\. / \. segments"):
        _validate_instance_id("/leading-slash")
    with pytest.raises(ValueError, match=r"empty / \.\. / \. segments"):
        _validate_instance_id("trailing-slash/")


def test_validate_instance_id_rejects_specials() -> None:
    with pytest.raises(ValueError, match="characters outside"):
        _validate_instance_id("foo bar")  # space
    with pytest.raises(ValueError, match="characters outside"):
        _validate_instance_id("foo;rm")
    with pytest.raises(ValueError, match="characters outside"):
        _validate_instance_id('id"quote')
    with pytest.raises(ValueError, match="characters outside"):
        _validate_instance_id("id\nnewline")
    with pytest.raises(ValueError, match="characters outside"):
        _validate_instance_id("id\x00nul")
