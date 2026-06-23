from __future__ import annotations

import stat
from pathlib import Path

from loom_cli.__main__ import main


def test_slurm_deploy_generates_vllm_bundle_without_printing_secret(
    monkeypatch, tmp_path: Path, capsys,
) -> None:
    monkeypatch.setattr(
        "loom_cli.inference_cmd._new_provider_auth_value",
        lambda: "loom_inference_deterministic-secret",
    )
    out_dir = tmp_path / "lux-qwen"

    rc = main([
        "inference", "deploy", "slurm",
        "--model", "Qwen/Qwen2.5-Coder-7B-Instruct",
        "--served-model-name", "qwen2.5-coder-7b-instruct",
        "--output-dir", str(out_dir),
        "--partition", "compute",
        "--gres", "gpu:h100:1",
        "--time-limit", "2-00:00:00",
        "--venv", "/pm/qy/uv_envs/vcbm",
        "--expose", "user-provided",
        "--endpoint-url", "http://202.78.161.51:18001/v1",
        "--no-submit",
    ])

    assert rc == 0
    stdout = capsys.readouterr().out
    assert "Registration fields" in stdout
    assert "base_url: http://202.78.161.51:18001/v1" in stdout
    assert "loom providers create" in stdout
    assert "--api-key file:" in stdout
    assert "deterministic-secret" not in stdout

    key_file = out_dir / "secrets" / "provider-api-key"
    env_file = out_dir / "secrets" / "vllm.env"
    sbatch_file = out_dir / "run-vllm.sbatch"
    launcher = out_dir / "launch-vllm.py"
    healthcheck = out_dir / "healthcheck.sh"
    register_json = out_dir / "loom-registration.json"

    assert key_file.read_text() == "loom_inference_deterministic-secret\n"
    assert stat.S_IMODE(key_file.stat().st_mode) == 0o600
    assert stat.S_IMODE(env_file.stat().st_mode) == 0o600

    sbatch_text = sbatch_file.read_text()
    assert "#SBATCH --partition=compute" in sbatch_text
    assert "#SBATCH --gres=gpu:h100:1" in sbatch_text
    assert "#SBATCH --time=2-00:00:00" in sbatch_text
    assert "Qwen/Qwen2.5-Coder-7B-Instruct" in sbatch_text
    assert "qwen2.5-coder-7b-instruct" in sbatch_text
    assert "python launch-vllm.py" in sbatch_text
    assert "deterministic-secret" not in sbatch_text

    launcher_text = launcher.read_text()
    assert "vllm.entrypoints.openai.api_server" in launcher_text
    assert "deterministic-secret" not in launcher_text

    health_text = healthcheck.read_text()
    assert "/v1/models" in health_text
    assert "/v1/chat/completions" in health_text
    assert "Authorization: Bearer ${LOOM_INFERENCE_API_KEY}" in health_text

    registration = register_json.read_text()
    assert "qwen2.5-coder-7b-instruct" in registration
    assert "http://202.78.161.51:18001/v1" in registration
    assert "deterministic-secret" not in registration


def test_slurm_deploy_requires_endpoint_for_user_provided_exposure(
    tmp_path: Path, capsys,
) -> None:
    rc = main([
        "inference", "deploy", "slurm",
        "--model", "Qwen/Qwen2.5-Coder-7B-Instruct",
        "--served-model-name", "qwen2.5-coder-7b-instruct",
        "--output-dir", str(tmp_path / "bundle"),
        "--expose", "user-provided",
        "--no-submit",
    ])

    assert rc == 2
    assert "--endpoint-url is required" in capsys.readouterr().err


def test_slurm_deploy_bastion_forward_generates_forward_helper(
    tmp_path: Path, capsys,
) -> None:
    out_dir = tmp_path / "lux-forward"

    rc = main([
        "inference", "deploy", "slurm",
        "--model", "/pm/models/checkpoint",
        "--served-model-name", "local-checkpoint",
        "--output-dir", str(out_dir),
        "--expose", "bastion-forward",
        "--bastion-public-host", "bastion.example.com",
        "--bastion-public-port", "18001",
        "--port", "8001",
        "--no-submit",
    ])

    assert rc == 0
    out = capsys.readouterr().out
    assert "base_url: http://bastion.example.com:18001/v1" in out
    assert "start-bastion-forward.sh" in out

    start_forward = (out_dir / "start-bastion-forward.sh").read_text()
    forward_py = (out_dir / "forward-bastion.py").read_text()
    assert "--listen-port 18001" in start_forward
    assert "--target-port 8001" in start_forward
    assert "asyncio.start_server" in forward_py
