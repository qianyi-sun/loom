"""LIVE Modal integration. Skipped unless explicitly opted-in.

Run::

    LOOM_RUN_MODAL_INTEGRATION=1 \\
        MODAL_TOKEN_ID=ak-... MODAL_TOKEN_SECRET=as-... \\
        pytest tests/integration/test_modal_driver_live.py -v -s

Each pass creates and tears down 1–2 Modal sandboxes. Estimated cost
< $0.02 per full run. The GPU subtest is additionally gated on
``LOOM_RUN_MODAL_GPU_INTEGRATION=1`` (~$0.01 extra).
"""

from __future__ import annotations

import os
from pathlib import Path, PurePosixPath

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("LOOM_RUN_MODAL_INTEGRATION") != "1"
    or not os.environ.get("MODAL_TOKEN_ID")
    or not os.environ.get("MODAL_TOKEN_SECRET"),
    reason=(
        "Modal live integration disabled "
        "(set LOOM_RUN_MODAL_INTEGRATION=1 + MODAL_TOKEN_ID/SECRET)"
    ),
)


async def test_live_start_exec_stop_cpu_only() -> None:
    from loom_drivers.modal.config import ModalConfig
    from loom_drivers.modal.driver import ModalDriver

    drv = ModalDriver(
        image="python:3.12-slim",
        config=ModalConfig.from_env(),
    )
    await drv.start()
    try:
        r = await drv.exec("echo hi && python -c 'print(2+2)'")
        assert r.return_code == 0
        assert b"hi" in r.stdout
        assert b"4" in r.stdout
    finally:
        await drv.stop()


async def test_live_exec_streaming_yields_chunks() -> None:
    from loom_drivers.modal.config import ModalConfig
    from loom_drivers.modal.driver import ModalDriver

    drv = ModalDriver(
        image="python:3.12-slim",
        config=ModalConfig.from_env(),
    )
    await drv.start()
    try:
        handle = await drv.exec_streaming(
            ["sh", "-c", "for i in 1 2 3; do echo line$i; sleep 0.05; done"],
            env_vars={},
            cwd=PurePosixPath("/workspace"),
        )
        buf = b""
        async for chunk in handle.stdout:
            buf += chunk
        rc = await handle.wait()
        assert rc == 0
        assert b"line1" in buf and b"line3" in buf
    finally:
        await drv.stop()


async def test_live_upload_download_roundtrip(tmp_path: Path) -> None:
    from loom_drivers.modal.config import ModalConfig
    from loom_drivers.modal.driver import ModalDriver

    payload = b"hello-from-modal-test\n"
    src = tmp_path / "src.bin"
    src.write_bytes(payload)

    drv = ModalDriver(
        image="python:3.12-slim",
        config=ModalConfig.from_env(),
    )
    await drv.start()
    try:
        await drv.upload(src, PurePosixPath("/workspace/sample.bin"))
        dst = tmp_path / "dst.bin"
        await drv.download(PurePosixPath("/workspace/sample.bin"), dst)
        assert dst.read_bytes() == payload
    finally:
        await drv.stop()


@pytest.mark.skipif(
    os.environ.get("LOOM_RUN_MODAL_GPU_INTEGRATION") != "1",
    reason=(
        "GPU live test disabled (costs ~$0.01); set "
        "LOOM_RUN_MODAL_GPU_INTEGRATION=1"
    ),
)
async def test_live_gpu_a10_nvidia_smi() -> None:
    from loom_drivers.modal.config import ModalConfig
    from loom_drivers.modal.driver import ModalDriver

    drv = ModalDriver(
        image="nvidia/cuda:12.2.0-base-ubuntu22.04",
        gpu="A10",
        config=ModalConfig.from_env(),
    )
    await drv.start()
    try:
        r = await drv.exec(
            "nvidia-smi --query-gpu=name --format=csv,noheader",
        )
        assert r.return_code == 0
        assert b"A10" in r.stdout or b"NVIDIA" in r.stdout
    finally:
        await drv.stop()
