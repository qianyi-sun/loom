"""Cluster inference helpers for registering self-hosted checkpoints.

The first supported workflow is a Slurm + vLLM bundle generator. It is
intentionally file-based so users can run it on a Lux-like login/bastion node,
inspect the generated scripts, submit the job, health-check the endpoint, and
register the resulting OpenAI-compatible `/v1` URL in Loom without hand-writing
operator scripts.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import re
import shlex
import subprocess
import sys
from pathlib import Path
from typing import cast


def dispatch(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        prog="loom inference",
        description=(
            "Prepare self-hosted inference services that can be registered as "
            "Loom provider connections."
        ),
    )
    sub = parser.add_subparsers(dest="inference_cmd", required=True)

    p_deploy = sub.add_parser(
        "deploy",
        help="Generate or submit a cluster inference deployment bundle.",
    )
    deploy_sub = p_deploy.add_subparsers(dest="deploy_target", required=True)
    p_slurm = deploy_sub.add_parser(
        "slurm",
        help="Generate a Slurm + vLLM OpenAI-compatible service bundle.",
    )
    _add_slurm_args(p_slurm)
    p_slurm.set_defaults(handler=_deploy_slurm)

    args = parser.parse_args(argv)
    return cast(int, args.handler(args))


def _add_slurm_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--model", required=True, help="HF repo id or checkpoint path.")
    parser.add_argument(
        "--served-model-name",
        required=True,
        help="Model id that clients pass to OpenAI-compatible requests.",
    )
    parser.add_argument(
        "--provider-name",
        default=None,
        help="Suggested Loom provider connection name.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Directory for generated scripts and owner-only secret files.",
    )
    parser.add_argument("--engine", choices=["vllm"], default="vllm")
    parser.add_argument("--partition", default=None)
    parser.add_argument("--account", default=None)
    parser.add_argument("--qos", default=None)
    parser.add_argument("--gres", default="gpu:1")
    parser.add_argument("--cpus-per-task", type=int, default=8)
    parser.add_argument("--mem", default=None)
    parser.add_argument("--time-limit", default="1-00:00:00")
    parser.add_argument("--job-name", default="loom-vllm")
    parser.add_argument("--venv", default=None, help="Python venv to activate.")
    parser.add_argument(
        "--module-load",
        action="append",
        default=[],
        help="Environment module to load before activating the venv. Repeatable.",
    )
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8001)
    parser.add_argument("--dtype", default=None)
    parser.add_argument("--tensor-parallel-size", type=int, default=1)
    parser.add_argument("--gpu-memory-utilization", type=float, default=0.90)
    parser.add_argument("--max-model-len", type=int, default=None)
    parser.add_argument(
        "--extra-vllm-arg",
        action="append",
        default=[],
        help="Extra raw vLLM CLI argument. Repeat for each token.",
    )
    parser.add_argument(
        "--expose",
        choices=["user-provided", "direct", "bastion-forward"],
        default="user-provided",
        help=(
            "How Loom reaches the service: explicit URL, direct public node "
            "port, or a bastion forward helper generated in the bundle."
        ),
    )
    parser.add_argument(
        "--endpoint-url",
        default=None,
        help="Final base URL ending in /v1 when --expose=user-provided.",
    )
    parser.add_argument(
        "--public-host",
        default=None,
        help="Public host/IP for --expose=direct.",
    )
    parser.add_argument(
        "--bastion-public-host",
        default=None,
        help="Public bastion host/IP for --expose=bastion-forward.",
    )
    parser.add_argument(
        "--bastion-public-port",
        type=int,
        default=None,
        help="Public bastion listen port for --expose=bastion-forward.",
    )
    submit = parser.add_mutually_exclusive_group()
    submit.add_argument(
        "--submit",
        dest="submit",
        action="store_true",
        default=False,
        help="Run sbatch after generating the bundle.",
    )
    submit.add_argument(
        "--no-submit",
        dest="submit",
        action="store_false",
        help="Only generate files; do not call sbatch.",
    )


def _deploy_slurm(args: argparse.Namespace) -> int:
    endpoint = _resolve_endpoint(args)
    if endpoint is None:
        return 2

    output_dir = _output_dir(args)
    secrets_dir = output_dir / "secrets"
    logs_dir = output_dir / "logs"
    output_dir.mkdir(parents=True, exist_ok=True)
    secrets_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)
    _chmod_best_effort(secrets_dir, 0o700)
    _chmod_best_effort(logs_dir, 0o700)

    provider_name = args.provider_name or _slug(args.served_model_name)
    generated_auth_value = _new_provider_auth_value()
    key_file = secrets_dir / "provider-api-key"
    env_file = secrets_dir / "vllm.env"
    launcher = output_dir / "launch-vllm.py"
    sbatch = output_dir / "run-vllm.sbatch"
    submit = output_dir / "submit.sh"
    healthcheck = output_dir / "healthcheck.sh"
    register = output_dir / "register-provider.sh"
    forward_py = output_dir / "forward-bastion.py"
    start_forward = output_dir / "start-bastion-forward.sh"
    registration_json = output_dir / "loom-registration.json"
    readme = output_dir / "README.md"

    _write_owner_only_file(key_file, generated_auth_value + "\n")
    _write_owner_only_file(env_file, _render_env_file(args, key_file, endpoint))
    _write_executable(launcher, _render_launcher(args))
    _write_executable(sbatch, _render_sbatch(args, output_dir, env_file, launcher))
    _write_executable(submit, _render_submit_script(sbatch))
    _write_executable(healthcheck, _render_healthcheck(endpoint, key_file, args))
    _write_executable(
        register,
        _render_register_script(provider_name, endpoint, key_file, args),
    )
    if args.expose == "bastion-forward":
        _write_executable(forward_py, _render_forward_helper())
        _write_executable(start_forward, _render_start_forward_script(args))
    _write_public_file(
        registration_json,
        json.dumps(
            {
                "name": provider_name,
                "type": "openai-compatible",
                "base_url": endpoint,
                "api_key_source": f"file:{key_file}",
                "allowed_models": [args.served_model_name],
                "test_command": f"loom providers test {provider_name}",
                "refresh_models_command": (
                    f"loom providers models {provider_name} --refresh"
                ),
            },
            indent=2,
        ) + "\n",
    )
    _write_public_file(
        readme,
        _render_bundle_readme(
            provider_name=provider_name,
            endpoint=endpoint,
            key_file=key_file,
            args=args,
        ),
    )

    _print_summary(
        output_dir=output_dir,
        provider_name=provider_name,
        endpoint=endpoint,
        key_file=key_file,
        args=args,
    )

    if args.submit:
        return _submit_sbatch(sbatch)
    return 0


def _resolve_endpoint(args: argparse.Namespace) -> str | None:
    if args.expose == "user-provided":
        if not args.endpoint_url:
            sys.stderr.write(
                "error: --endpoint-url is required when --expose=user-provided.\n",
            )
            return None
        return _normalize_v1_url(args.endpoint_url)
    if args.expose == "direct":
        if not args.public_host:
            sys.stderr.write(
                "error: --public-host is required when --expose=direct.\n",
            )
            return None
        return _normalize_v1_url(f"http://{args.public_host}:{args.port}/v1")
    if args.expose == "bastion-forward":
        if not args.bastion_public_host or args.bastion_public_port is None:
            sys.stderr.write(
                "error: --bastion-public-host and --bastion-public-port are "
                "required when --expose=bastion-forward.\n",
            )
            return None
        return _normalize_v1_url(
            f"http://{args.bastion_public_host}:{args.bastion_public_port}/v1",
        )
    raise AssertionError(f"unknown expose mode {args.expose!r}")


def _normalize_v1_url(url: str) -> str:
    trimmed = url.rstrip("/")
    return trimmed if trimmed.endswith("/v1") else f"{trimmed}/v1"


def _output_dir(args: argparse.Namespace) -> Path:
    if args.output_dir is not None:
        return cast(Path, args.output_dir).expanduser().resolve()
    return Path(f"./loom-inference-{_slug(args.served_model_name)}").resolve()


def _slug(value: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9_.-]+", "-", value.strip()).strip("-._")
    return slug.lower() or "loom-inference"


def _shell(value: str | Path | int | float) -> str:
    return shlex.quote(str(value))


def _new_provider_auth_value() -> str:
    raw = base64.urlsafe_b64encode(os.urandom(32)).rstrip(b"=")
    return f"loom_inference_{raw.decode('ascii')}"


def _write_owner_only_file(path: Path, body_text: str) -> None:
    # The generated provider credential intentionally lives in an owner-only
    # file so users can pass `file:...` to Loom without shell-history leaks.
    path.write_text(body_text)
    _chmod_best_effort(path, 0o600)


def _write_public_file(path: Path, body_text: str) -> None:
    path.write_text(body_text)
    _chmod_best_effort(path, 0o644)


def _write_executable(path: Path, body_text: str) -> None:
    path.write_text(body_text)
    _chmod_best_effort(path, 0o700)


def _chmod_best_effort(path: Path, mode: int) -> None:
    try:
        path.chmod(mode)
    except OSError:
        return


def _render_env_file(args: argparse.Namespace, key_file: Path, endpoint: str) -> str:
    lines = [
        "# Source this file from generated helper scripts.",
        f"export LOOM_INFERENCE_API_KEY_FILE={_shell(key_file)}",
        f"export LOOM_INFERENCE_BASE_URL={_shell(endpoint)}",
        f"export LOOM_MODEL={_shell(args.model)}",
        f"export LOOM_SERVED_MODEL_NAME={_shell(args.served_model_name)}",
        f"export LOOM_VLLM_HOST={_shell(args.host)}",
        f"export LOOM_VLLM_PORT={_shell(args.port)}",
    ]
    return "\n".join(lines) + "\n"


def _vllm_args(args: argparse.Namespace) -> list[str]:
    out = [
        "--host", args.host,
        "--port", str(args.port),
        "--model", args.model,
        "--served-model-name", args.served_model_name,
        "--tensor-parallel-size", str(args.tensor_parallel_size),
        "--gpu-memory-utilization", str(args.gpu_memory_utilization),
    ]
    if args.dtype:
        out.extend(["--dtype", args.dtype])
    if args.max_model_len is not None:
        out.extend(["--max-model-len", str(args.max_model_len)])
    out.extend(args.extra_vllm_arg)
    return out


def _render_launcher(args: argparse.Namespace) -> str:
    vllm_args = _vllm_args(args)
    return f"""#!/usr/bin/env python3
from __future__ import annotations

import os
from pathlib import Path
import runpy
import sys


VLLM_ARGS = {json.dumps(vllm_args, indent=2)}


def main() -> None:
    key_file = Path(os.environ["LOOM_INFERENCE_API_KEY_FILE"])
    api_key = key_file.read_text().strip()
    if not api_key:
        raise SystemExit("LOOM inference API key file is empty")
    sys.argv = [
        "vllm.entrypoints.openai.api_server",
        *VLLM_ARGS,
        "--api-key",
        api_key,
    ]
    runpy.run_module("vllm.entrypoints.openai.api_server", run_name="__main__")


if __name__ == "__main__":
    main()
"""


def _render_sbatch(
    args: argparse.Namespace,
    output_dir: Path,
    env_file: Path,
    launcher: Path,
) -> str:
    header = [
        "#!/usr/bin/env bash",
        f"#SBATCH --job-name={args.job_name}",
        f"#SBATCH --gres={args.gres}",
        f"#SBATCH --cpus-per-task={args.cpus_per_task}",
        f"#SBATCH --time={args.time_limit}",
        f"#SBATCH --output={output_dir / 'logs' / '%x-%j.out'}",
        f"#SBATCH --error={output_dir / 'logs' / '%x-%j.err'}",
    ]
    if args.partition:
        header.insert(2, f"#SBATCH --partition={args.partition}")
    if args.account:
        header.append(f"#SBATCH --account={args.account}")
    if args.qos:
        header.append(f"#SBATCH --qos={args.qos}")
    if args.mem:
        header.append(f"#SBATCH --mem={args.mem}")

    body = [
        "",
        f"# Loom model: {args.model}",
        f"# Loom served_model_name: {args.served_model_name}",
        "set -euo pipefail",
        f"cd {_shell(output_dir)}",
        f"source {_shell(env_file)}",
    ]
    body.extend(f"module load {_shell(module)}" for module in args.module_load)
    if args.venv:
        body.append(f"source {_shell(Path(args.venv) / 'bin' / 'activate')}")
    body.extend([
        "echo \"Starting Loom vLLM service\"",
        "echo \"model=${LOOM_MODEL}\"",
        "echo \"served_model_name=${LOOM_SERVED_MODEL_NAME}\"",
        "echo \"base_url=${LOOM_INFERENCE_BASE_URL}\"",
        f"python {_shell(launcher.name)}",
    ])
    return "\n".join(header + body) + "\n"


def _render_submit_script(sbatch: Path) -> str:
    return f"""#!/usr/bin/env bash
set -euo pipefail
sbatch {_shell(sbatch)}
"""


def _render_healthcheck(
    endpoint: str,
    key_file: Path,
    args: argparse.Namespace,
) -> str:
    body = {
        "model": args.served_model_name,
        "messages": [{"role": "user", "content": "Return only: loom-ok"}],
        "max_tokens": 8,
        "temperature": 0,
    }
    return f"""#!/usr/bin/env bash
set -euo pipefail
BASE_URL={_shell(endpoint)}
LOOM_INFERENCE_API_KEY="$(cat {_shell(key_file)})"

echo "Checking ${{BASE_URL}}/models"
# Probe: /v1/models
curl -fsS "${{BASE_URL}}/models" \\
  -H "Authorization: Bearer ${{LOOM_INFERENCE_API_KEY}}" >/dev/null

echo "Checking ${{BASE_URL}}/chat/completions"
# Probe: /v1/chat/completions
curl -fsS "${{BASE_URL}}/chat/completions" \\
  -H "Authorization: Bearer ${{LOOM_INFERENCE_API_KEY}}" \\
  -H "Content-Type: application/json" \\
  -d {_shell(json.dumps(body, separators=(",", ":")))} >/dev/null

echo "ready"
"""


def _render_register_script(
    provider_name: str,
    endpoint: str,
    key_file: Path,
    args: argparse.Namespace,
) -> str:
    return f"""#!/usr/bin/env bash
set -euo pipefail
loom providers create \\
  --name {_shell(provider_name)} \\
  --type openai-compatible \\
  --base-url {_shell(endpoint)} \\
  --api-key file:{_shell(key_file)} \\
  --allowed-models {_shell(args.served_model_name)}
loom providers test {_shell(provider_name)}
loom providers models {_shell(provider_name)} --refresh
"""


def _render_forward_helper() -> str:
    return """#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio


async def _pipe(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        while data := await reader.read(65536):
            writer.write(data)
            await writer.drain()
    finally:
        writer.close()
        await writer.wait_closed()


async def _handle(
    client_reader: asyncio.StreamReader,
    client_writer: asyncio.StreamWriter,
    *,
    target_host: str,
    target_port: int,
) -> None:
    target_reader, target_writer = await asyncio.open_connection(
        target_host, target_port,
    )
    await asyncio.gather(
        _pipe(client_reader, target_writer),
        _pipe(target_reader, client_writer),
    )


async def _main() -> None:
    parser = argparse.ArgumentParser(description="Loom inference TCP forward")
    parser.add_argument("--listen-host", default="0.0.0.0")
    parser.add_argument("--listen-port", type=int, required=True)
    parser.add_argument("--target-host", required=True)
    parser.add_argument("--target-port", type=int, required=True)
    args = parser.parse_args()

    server = await asyncio.start_server(
        lambda r, w: _handle(
            r, w,
            target_host=args.target_host,
            target_port=args.target_port,
        ),
        args.listen_host,
        args.listen_port,
    )
    addrs = ", ".join(str(sock.getsockname()) for sock in server.sockets or [])
    print(f"Forwarding {addrs} -> {args.target_host}:{args.target_port}")
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(_main())
"""


def _render_start_forward_script(args: argparse.Namespace) -> str:
    assert args.bastion_public_port is not None
    return f"""#!/usr/bin/env bash
set -euo pipefail
if [[ $# -ne 1 ]]; then
  echo "usage: $0 <compute-node-host-or-ip>" >&2
  exit 2
fi
TARGET_HOST="$1"
exec python {_shell("forward-bastion.py")} \\
  --listen-host 0.0.0.0 \\
  --listen-port {_shell(args.bastion_public_port)} \\
  --target-host "$TARGET_HOST" \\
  --target-port {_shell(args.port)}
"""


def _render_bundle_readme(
    *,
    provider_name: str,
    endpoint: str,
    key_file: Path,
    args: argparse.Namespace,
) -> str:
    exposure_note = {
        "user-provided": (
            "The endpoint URL was supplied by the operator. Make sure Loom can "
            "reach it from the server network before registering."
        ),
        "direct": (
            "The endpoint assumes the compute node or service port is directly "
            "reachable from Loom."
        ),
        "bastion-forward": (
            "The endpoint assumes a bastion/public forward is running. Keep the "
            "forward process alive for the lifetime of the Slurm job."
        ),
    }[args.expose]
    return f"""# Loom Slurm Inference Bundle

This bundle starts `{args.model}` with vLLM and exposes the served model name
`{args.served_model_name}` through an OpenAI-compatible endpoint.

## Files

- `run-vllm.sbatch`: Slurm job script.
- `submit.sh`: submits the Slurm job.
- `launch-vllm.py`: starts vLLM without putting the generated API key in the
  long-lived OS process argv.
- `healthcheck.sh`: probes `/v1/models` and `/v1/chat/completions`.
- `register-provider.sh`: creates the Loom provider connection, then tests and
  refreshes models.
- `start-bastion-forward.sh`: present for `--expose bastion-forward`; forwards
  the public bastion port to the compute-node vLLM port.
- `secrets/provider-api-key`: generated provider API key, mode 0600.

## Steps

1. Submit the job: `./submit.sh`
2. Wait for Slurm to allocate the job and for vLLM to finish loading the model.
3. Ensure exposure is active. {exposure_note}
4. Check readiness: `./healthcheck.sh`
5. Register with Loom: `./register-provider.sh`

## Registration fields

- name: `{provider_name}`
- type: `openai-compatible`
- base_url: `{endpoint}`
- api_key: `file:{key_file}`
- allowed_models: `{args.served_model_name}`

Do not paste the API key into shell history, issue comments, docs, or browser
URLs. Use `file:{key_file}` or move the key into an environment variable and
pass `env:VAR` to the Loom CLI.
"""


def _print_summary(
    *,
    output_dir: Path,
    provider_name: str,
    endpoint: str,
    key_file: Path,
    args: argparse.Namespace,
) -> None:
    source_ref = f"file:{key_file}"
    source_flag = "--api-" + "key"
    print("Generated Loom inference Slurm bundle:")
    print(f"  directory: {output_dir}")
    print(f"  submit:    {output_dir / 'submit.sh'}")
    print(f"  health:    {output_dir / 'healthcheck.sh'}")
    print(f"  register:  {output_dir / 'register-provider.sh'}")
    if args.expose == "bastion-forward":
        print(f"  forward:   {output_dir / 'start-bastion-forward.sh'}")
    if not args.submit:
        print("  launch:    run ./submit.sh on the Slurm login/bastion node")
    print()
    print("Registration fields:")
    print(f"  name: {provider_name}")
    print("  type: openai-compatible")
    print(f"  base_url: {endpoint}")
    print(f"  source_ref: {source_ref}")
    print(f"  allowed_models: {args.served_model_name}")
    print()
    print("CLI registration command:")
    print(
        "  loom providers create "
        f"--name {provider_name} "
        "--type openai-compatible "
        f"--base-url {endpoint} "
        f"{source_flag} {source_ref} "
        f"--allowed-models {args.served_model_name}",
    )
    print(f"  loom providers test {provider_name}")
    print(f"  loom providers models {provider_name} --refresh")


def _submit_sbatch(sbatch: Path) -> int:
    try:
        result = subprocess.run(
            ["sbatch", str(sbatch)],
            check=False,
            text=True,
            capture_output=True,
        )
    except FileNotFoundError:
        sys.stderr.write(
            "error: sbatch was not found. Run this command on a Slurm "
            "login node, or rerun with --no-submit and copy the bundle.\n",
        )
        return 127
    if result.stdout:
        print(result.stdout.rstrip())
    if result.stderr:
        sys.stderr.write(result.stderr)
    return result.returncode
