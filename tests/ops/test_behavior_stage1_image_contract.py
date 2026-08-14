from __future__ import annotations

import argparse
import hashlib
import importlib.util
import io
import json
import os
import shutil
import subprocess
import tarfile
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = REPO_ROOT / "deploy/behavior-stage1-sim/image_contract.py"
RNG_MODULE_PATH = REPO_ROOT / "deploy/behavior-stage1-sim/pipeline_rng.py"
EXTRACTOR_MODULE_PATH = REPO_ROOT / "deploy/behavior-stage1-sim/extract_runtime_asset.py"
DOCKERFILE = REPO_ROOT / "deploy/Dockerfile.behavior-stage1-sim"


def _module() -> ModuleType:
    spec = importlib.util.spec_from_file_location("behavior_stage1_image_contract", MODULE_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _rng_module() -> ModuleType:
    spec = importlib.util.spec_from_file_location("behavior_stage1_pipeline_rng", RNG_MODULE_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _extractor_module() -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        "behavior_stage1_runtime_asset_extractor", EXTRACTOR_MODULE_PATH
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _runtime_archive(
    path: Path,
    entries: list[tuple[str, bytes | str, str]],
) -> None:
    with tarfile.open(path, "w:xz") as archive:
        for name, value, kind in entries:
            info = tarfile.TarInfo(name)
            if kind == "dir":
                info.type = tarfile.DIRTYPE
                info.mode = 0o755
                archive.addfile(info)
            elif kind == "file":
                assert isinstance(value, bytes)
                info.size = len(value)
                info.mode = 0o555
                archive.addfile(info, io.BytesIO(value))
            else:
                assert kind == "symlink" and isinstance(value, str)
                info.type = tarfile.SYMTYPE
                info.mode = 0o777
                info.linkname = value
                archive.addfile(info)


def test_runtime_asset_extractor_is_confined_and_preserves_internal_symlinks(
    tmp_path: Path,
) -> None:
    module = _extractor_module()
    archive = tmp_path / "asset.tar.xz"
    _runtime_archive(
        archive,
        [
            ("asset", b"", "dir"),
            ("asset/bin", b"", "dir"),
            ("asset/bin/tool.real", b"runtime\n", "file"),
            ("asset/bin/tool", "tool.real", "symlink"),
        ],
    )
    output = tmp_path / "out"
    module.extract_runtime_asset(archive, output, "asset")
    assert (output / "bin/tool").is_symlink()
    assert (output / "bin/tool").read_bytes() == b"runtime\n"
    for directory in [output, *(path for path in output.rglob("*") if path.is_dir())]:
        os.chmod(directory, 0o755)
    shutil.rmtree(output)

    unsafe = tmp_path / "unsafe.tar.xz"
    _runtime_archive(
        unsafe,
        [("asset", b"", "dir"), ("asset/link", "../../escape", "symlink")],
    )
    with pytest.raises(module.RuntimeAssetError, match="escapes"):
        module.extract_runtime_asset(unsafe, tmp_path / "unsafe-out", "asset")


def _sha256(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def test_distribution_freeze_is_canonical_and_first_visible_wins(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _module()

    @dataclass(frozen=True)
    class Distribution:
        name: str
        version: str

        @property
        def metadata(self) -> dict[str, str]:
            return {"Name": self.name}

    observed = [
        Distribution("Z_pkg", "1.0"),
        Distribution("a.pkg", "2.0"),
        Distribution("A-PKG", "9.9"),
    ]
    monkeypatch.setattr(module, "distributions", lambda: observed)
    payload = b'[{"name":"a-pkg","version":"2.0"},{"name":"z-pkg","version":"1.0"}]\n'
    assert module._distribution_freeze_digest() == ("sha256:" + hashlib.sha256(payload).hexdigest())


def test_build_manifest_binds_all_repository_and_source_evidence(tmp_path: Path) -> None:
    module = _module()
    source_lock = REPO_ROOT / "deploy/behavior-stage1-sim/source-lock.json"
    sim_lock = REPO_ROOT / "deploy/behavior-stage1-sim/sim.requirements.lock.txt"
    loom_runtime_lock = REPO_ROOT / "deploy/behavior-stage1-sim/loom-runtime.requirements.lock.txt"
    vla_lock = REPO_ROOT / "deploy/behavior-stage1-sim/vla.uv.lock"
    evidence = tmp_path / "source-evidence.json"
    evidence.write_bytes(
        json.dumps(
            {
                "integration_patches": [
                    {
                        "name": "omnigibson-readonly-isaac-kit",
                        "path": "omnigibson/omnigibson/simulator.py",
                        "result_sha256": "sha256:71129b407097684e6efbc4a2eb271c1bf9e6bafcb6a814d78198c8e550e7d09a",
                        "source_sha256": "sha256:7211956ce0d787b63f4f3860cd0fced063a5c6c9035af4de93e39191fec7570b",
                    },
                    {
                        "name": "openpi-transformers-cache-type",
                        "path": "openpi/src/openpi/models_pytorch/gemma_pytorch.py",
                        "result_sha256": "sha256:4f75d3647fadb7d00c0fee884579cf5a3ef33a6af53a3908fc237358d9606cf5",
                        "source_sha256": "sha256:08fd8d750519f0fb44fc5173311e50a30f4c8f32c02e51244b4f8e47b32cd52f",
                    },
                ],
                "schema_version": "loom.behavior-stage1-image-source-evidence.v1",
                "source_lock_sha256": _sha256(source_lock),
                "sources": [],
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        + b"\n"
    )
    output = tmp_path / "compatibility-manifest.json"
    module.build_manifest(
        argparse.Namespace(
            build_sha="a" * 40,
            build_tree_sha="b" * 40,
            loom_runtime_lock=loom_runtime_lock,
            output=output,
            pipeline_rng_patch=RNG_MODULE_PATH,
            sim_lock=sim_lock,
            source_evidence=evidence,
            source_lock=source_lock,
            vla_lock=vla_lock,
        )
    )
    payload = output.read_bytes()
    manifest = json.loads(payload)
    assert (
        payload
        == json.dumps(manifest, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()
        + b"\n"
    )
    assert manifest["source_lock_sha256"] == _sha256(source_lock)
    assert manifest["source_evidence_sha256"] == _sha256(evidence)
    assert manifest["platform"] == "linux/amd64"
    assert manifest["pipeline_rng_patch_sha256"] == _sha256(RNG_MODULE_PATH)
    assert manifest["loom_runtime_lock_sha256"] == _sha256(loom_runtime_lock)
    source_authority = json.loads(source_lock.read_text(encoding="utf-8"))
    assert (
        manifest["sim_distribution_freeze_sha256"]
        == source_authority["sim_python"]["accepted_freeze_sha256"]
    )
    assert (
        manifest["vla_distribution_freeze_sha256"]
        == source_authority["vla_python"]["accepted_freeze_sha256"]
    )
    assert manifest["application_features"] == ["isaac-sim-5.1", "omnigibson-3.8"]
    assert manifest["cuda_userspace_version"] == "12.6"
    assert manifest["provider_assets"] == []
    assert manifest["gpu_contract"] == {
        "count_exact": 2,
        "memory_mib_min_each": 16000,
        "model_exact": "NVIDIA GeForce RTX 5080",
        "ordered_roles": ["sim", "vla"],
    }


def test_gpu_observation_preserves_exact_oldlab_cuda_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _module()
    observed = "\n".join(
        [
            "0, GPU-aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee, NVIDIA GeForce RTX 5080, 16303, 575.64.03",
            "1, GPU-11111111-2222-3333-4444-555555555555, NVIDIA GeForce RTX 5080, 16303, 575.64.03",
        ]
    )
    monkeypatch.setattr(
        module.subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess(args[0], 0, observed, ""),
    )
    devices, driver = module._gpu_observation("570.00")
    assert driver == "575.64.03"
    assert [(item["logical_index"], item["role"]) for item in devices] == [
        (0, "sim"),
        (1, "vla"),
    ]


@pytest.mark.parametrize(
    "observed,reason",
    [
        (
            "0, GPU-aaaaaaaa, NVIDIA GeForce RTX 5080, 16303, 575.64.03\n",
            "exactly two",
        ),
        (
            "0, GPU-aaaaaaaa, NVIDIA A100, 40960, 575.64.03\n"
            "1, GPU-bbbbbbbb, NVIDIA A100, 40960, 575.64.03\n",
            "OLDLAB contract",
        ),
        (
            "0, GPU-aaaaaaaa, NVIDIA GeForce RTX 5080, 16303, 560.1\n"
            "1, GPU-bbbbbbbb, NVIDIA GeForce RTX 5080, 16303, 560.1\n",
            "older",
        ),
    ],
)
def test_gpu_observation_rejects_capacity_or_driver_drift(
    monkeypatch: pytest.MonkeyPatch,
    observed: str,
    reason: str,
) -> None:
    module = _module()
    monkeypatch.setattr(
        module.subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess(args[0], 0, observed, ""),
    )
    with pytest.raises(module.ImageContractError, match=reason):
        module._gpu_observation("570.00")


def test_probe_accepts_one_exact_marker_and_rejects_ambiguous_logs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _module()
    marker = "LOOM_STAGE1_SIM_PROBE="
    monkeypatch.setattr(
        module.subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            args[0], 0, f'upstream log\n{marker}{{"healthy":true}}\n', ""
        ),
    )
    assert module._probe(["probe"], "simulator", marker=marker) == {"healthy": True}

    monkeypatch.setattr(
        module.subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            args[0], 0, f"{marker}{{}}\n{marker}{{}}\n", ""
        ),
    )
    with pytest.raises(module.ImageContractError, match="marker is not unique"):
        module._probe(["probe"], "simulator", marker=marker)


def test_preflight_scratch_is_private_fresh_and_confined(tmp_path: Path) -> None:
    module = _module()
    root = tmp_path / "scratch"
    root.mkdir(mode=0o700)
    module._prepare_preflight_scratch(root)
    assert sorted(path.relative_to(root).as_posix() for path in root.rglob("*")) == [
        "cache",
        "home",
        "omnigibson",
        "omnigibson/appdata",
        "tmp",
    ]
    with pytest.raises(module.ImageContractError, match="not fresh"):
        module._prepare_preflight_scratch(root)


def test_preflight_emits_the_worker_contract_after_real_runtime_probe_shapes(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    module = _module()
    digest = "sha256:" + "a" * 64
    source_digest = "sha256:" + "b" * 64
    rng_digest = "sha256:" + "c" * 64
    manifest = {key: None for key in module._MANIFEST_KEYS}
    manifest.update(
        {
            "cuda_userspace_version": "12.6",
            "minimum_nvidia_driver": "570.00",
            "pipeline_rng_patch_sha256": rng_digest,
            "platform": "linux/amd64",
            "schema_version": "loom.behavior-stage1-image-compatibility.v1",
            "source_evidence_sha256": source_digest,
            "source_lock_sha256": digest,
        }
    )
    monkeypatch.setattr(module, "_load_canonical", lambda *args, **kwargs: manifest)
    monkeypatch.setattr(module, "_verify_distribution_freezes", lambda value: None)
    monkeypatch.setattr(module, "_prepare_preflight_scratch", lambda: None)
    monkeypatch.setattr(module.platform, "machine", lambda: "x86_64")
    monkeypatch.setattr(
        module,
        "_gpu_observation",
        lambda minimum: (
            [
                {"device_uuid": "GPU-aaaaaaaa", "model": "NVIDIA GeForce RTX 5080"},
                {"device_uuid": "GPU-bbbbbbbb", "model": "NVIDIA GeForce RTX 5080"},
            ],
            "575.64.03",
        ),
    )

    def fake_sha(path: Path) -> str:
        if path.name == "image_contract.py":
            return digest
        if path.name == "source-evidence.json":
            return source_digest
        return rng_digest

    calls: list[tuple[str, dict[str, str] | None, str]] = []

    def fake_probe(
        argv: list[str],
        label: str,
        *,
        marker: str,
        environment: dict[str, str] | None = None,
    ) -> dict[str, object]:
        calls.append((label, environment, " ".join(argv)))
        if label == "simulator":
            return {
                "egl": True,
                "isaac": True,
                "omnigibson": "3.8.0",
                "python": "3.11.13",
            }
        return {
            "b1k": "0.1.0",
            "cuda": "12.6",
            "cuda_tensor": True,
            "python": "3.11.13",
        }

    monkeypatch.setattr(module, "_sha256", fake_sha)
    monkeypatch.setattr(module, "_probe", fake_probe)
    module.preflight(
        argparse.Namespace(
            json=True,
            platform_manifest_digest="sha256:" + "d" * 64,
            preflight_digest=digest,
        )
    )

    observation = json.loads(capsys.readouterr().out)
    assert set(observation) == {
        "concurrent_vla_isaac_healthy",
        "cpu_arch",
        "cuda_userspace_version",
        "device_models",
        "egl_healthy",
        "isaac_healthy",
        "omnigibson_healthy",
        "platform_manifest_digest",
        "preflight_digest",
        "visible_device_uuids",
        "vla_healthy",
    }
    assert observation["platform_manifest_digest"] == "sha256:" + "d" * 64
    assert observation["preflight_digest"] == digest
    assert observation["visible_device_uuids"] == ["GPU-aaaaaaaa", "GPU-bbbbbbbb"]
    assert observation["concurrent_vla_isaac_healthy"] is False
    assert {label for label, _, _ in calls} == {"simulator", "VLA"}
    simulator_call = next(item for item in calls if item[0] == "simulator")
    vla_call = next(item for item in calls if item[0] == "VLA")
    assert simulator_call[1] is not None and simulator_call[1]["CUDA_VISIBLE_DEVICES"] == "0"
    assert "og.launch(device='cuda:0')" in simulator_call[2]
    assert "og.sim.render()" in simulator_call[2]
    assert vla_call[1] == {"CUDA_VISIBLE_DEVICES": "1"}
    assert "torch.ones(1,device='cuda')" in vla_call[2]


def test_simulator_seed_hook_latches_uint32_and_rejects_drift() -> None:
    module = _rng_module()
    with pytest.raises(RuntimeError, match="not initialized"):
        module.pipeline_seed()
    module.set_pipeline_seed(0)
    module.set_pipeline_seed(0)
    assert module.pipeline_seed() == 0
    with pytest.raises(RuntimeError, match="drift"):
        module.set_pipeline_seed(1)
    with pytest.raises(ValueError, match="uint32"):
        _rng_module().set_pipeline_seed(True)


def test_stage1_dockerfile_is_single_platform_closed_and_source_locked() -> None:
    value = DOCKERFILE.read_text(encoding="utf-8")
    assert value.startswith("# syntax=docker/dockerfile:1.7\n")
    assert (
        "FROM nvcr.io/nvidia/isaac-sim@sha256:"
        "93b0f99635ab126fb5b33298d513c11520f119f0ee60ff8414ccef67ea977829"
    ) in value
    assert "--from=stage1-sources" in value
    assert "git clone" not in value and "git fetch" not in value
    assert "--require-hashes" in value and "uv sync --frozen" in value
    assert "autobuild-2026-08-13-17-03" in value
    assert "b33b9c56b28dbc709a7938e2461d34caefc897a6090ac02da8fc55f82d6d5451" in value
    assert "/opt/ffmpeg/LICENSE.txt" in value
    assert "/opt/loom/bin/sim-python" in value
    assert "/opt/loom/venv-vla/bin/python" in value
    assert "src/loom_worker/pipeline_attempt_workspace.py" in value
    assert "src/loom_worker/pipeline_live_preview.py" in value
    assert "HF_HUB_OFFLINE=1" in value and "TRANSFORMERS_OFFLINE=1" in value
    assert "USER 65532:65532" in value
    assert "ENTRYPOINT []" in value and "CMD []" in value
    assert "LOOM_BUILD_SHA" in value and "LOOM_BUILD_TREE_SHA" in value
    assert "/isaac-sim/apps/omnigibson_5_1_0.kit" in value
    assert "cmp \\" in value


def test_source_lock_vendors_runtime_only_and_omits_the_known_absolute_symlink() -> None:
    value = json.loads(
        (REPO_ROOT / "deploy/behavior-stage1-sim/source-lock.json").read_text(encoding="utf-8")
    )
    omnigibson = next(item for item in value["sources"] if item["name"] == "omnigibson")
    assert omnigibson["visibility"] == "vendored-runtime"
    assert omnigibson["vendor_path"] == "third_party/behavior-stage1/omnigibson"
    assert omnigibson["excluded_upstream_entries"] == [
        "OmniGibson/omnigibson/learning/configs/policy/hybrid_mp.yaml"
    ]
    projection = REPO_ROOT / omnigibson["vendor_path"]
    assert not (projection / "omnigibson/examples").exists()
    assert {path.name for path in (projection / "omnigibson/learning").iterdir()} == {
        "__init__.py",
        "configs",
        "eval_data_gen_par_save_all.py",
        "policies.py",
        "utils",
        "wrappers",
    }
    files = tuple(path for path in projection.rglob("*") if path.is_file())
    assert len(files) <= 300
    assert sum(path.stat().st_size for path in files) <= 8 * 1024 * 1024
    assert not any(
        path.name.startswith("test_") or path.name.endswith("_test.py") for path in files
    )
