"""Opt-in local Harbor smoke: real native sandboxes, zero external model calls.

Run with LOOM_TERMINUS_SMOKE_IMAGE and LOOM_TERMINUS_CONTROLLER_IMAGE pointing
to locally prepared images. Every container has --network none. The only model
provider is a deterministic HTTP stub inside the controller container.
"""

from __future__ import annotations

import asyncio
import base64
import json
import os
import shutil
import subprocess
import sys
import threading
import tomllib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from uuid import uuid4


def test_native_harbor_manifest_and_isolated_verifier(tmp_path: Path) -> None:
    import pytest

    repository = Path(__file__).resolve().parents[2]
    task_image = os.environ["LOOM_TERMINUS_SMOKE_IMAGE"]
    controller_image = os.environ["LOOM_TERMINUS_CONTROLLER_IMAGE"]
    prefix = "loom-terminus-smoke-" + uuid4().hex[:12]
    names = [prefix + "-task", prefix + "-verifier"]
    volumes = [prefix + "-task-socket", prefix + "-verifier-socket"]
    binary = tmp_path / "loom-sandbox-runtime"
    subprocess.run(["go", "build", "-o", str(binary), "./cmd/loom-sandbox-runtime"],
                   cwd=repository, env={**os.environ, "GOOS": "linux", "GOARCH": "amd64", "CGO_ENABLED": "0"}, check=True)
    binary.chmod(0o755)
    evidence = tmp_path / "evidence"
    evidence.mkdir(mode=0o777)
    evidence.chmod(0o777)

    def docker(*args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(["docker", *args], text=True, capture_output=True, check=True, timeout=180)

    try:
        for name, volume, role in zip(names, volumes, ("task-sandbox", "verifier-sandbox"), strict=True):
            docker("volume", "create", volume)
            docker("run", "--rm", "--network", "none", "--user", "0", "--entrypoint", "/bin/sh",
                   "-v", volume + ":/socket", task_image, "-c", "chown 65532:65532 /socket")
            socket_dir = "/loom/sandboxes/" + role
            docker("run", "-d", "--name", name, "--network", "none", "--user", "65532:65532",
                   "--cap-drop", "ALL", "--security-opt", "no-new-privileges",
                   "-v", f"{binary}:/loom/bin/loom-sandbox-runtime:ro",
                   "-v", f"{volume}:{socket_dir}", "--entrypoint", "/loom/bin/loom-sandbox-runtime",
                   task_image, "--socket", socket_dir + "/sandbox.sock")
        result = docker(
            "run", "--rm", "--network", "none", "--user", "65532:65532",
            "--cap-drop", "ALL", "--security-opt", "no-new-privileges",
            "-v", f"{repository}:/checkout:ro", "-v", f"{evidence}:/evidence",
            "-v", volumes[0] + ":/loom/sandboxes/task-sandbox",
            "-v", volumes[1] + ":/loom/sandboxes/verifier-sandbox",
            "-e", "PYTHONPATH=/checkout/src:/checkout", "-e", "PYTHONDONTWRITEBYTECODE=1",
            "-e", "HOME=/tmp/loom-home", "--workdir", "/evidence", "--entrypoint", "python",
            controller_image, "/checkout/tests/integration/test_nebius_terminus_e2e.py", "--inside",
        )
        (evidence / "controller.stdout").write_text(result.stdout)
        (evidence / "controller.stderr").write_text(result.stderr)
        report = json.loads((evidence / "report.json").read_text())
        assert report["reward"] == 1
        assert report["native_turns"] >= 2
        assert report["gateway_calls"] == report["typed_calls"]
        assert report["private_inputs_hidden"] is True
        assert report["external_model_calls"] == 0
    except subprocess.CalledProcessError as exc:
        pytest.fail(f"local controller failed: {exc.stdout}\n{exc.stderr}")
    finally:
        for name in names:
            subprocess.run(["docker", "rm", "-f", name], capture_output=True)
        for volume in volumes:
            subprocess.run(["docker", "volume", "rm", volume], capture_output=True)


async def _verify_harbor_tool_identity() -> None:
    """Exercise the installed, build-patched Harbor source, including installs."""
    from types import SimpleNamespace

    from harbor.agents.terminus_2.tmux_session import TmuxSession

    class Environment:
        def __init__(self, installed: bool) -> None:
            self.installed = installed
            self.calls: list[tuple[str, object]] = []

        async def exec(self, command: str, *, user: object = None, **_kwargs: object) -> object:
            self.calls.append((command, user))
            if user == "root":
                raise PermissionError("installation requires root")
            if command in {"tmux -V", "asciinema --version"}:
                code, stdout = (0 if self.installed else 1), "version"
            elif command == "uname -s":
                code, stdout = 0, "Linux"
            elif "os-release" in command:
                code, stdout = 0, 'ID=ubuntu\n'
            elif "which apt-get" in command:
                code, stdout = 0, ""
            else:
                raise AssertionError(f"Unexpected probe: {command}")
            return SimpleNamespace(return_code=code, stdout=stdout, stderr="")

    for installed in (True, False):
        environment = Environment(installed)
        session = TmuxSession(
            session_name="probe-test", environment=environment,
            logging_path=Path("/tmp/probe.pane"), local_asciinema_recording_path=None,
            remote_asciinema_recording_path=Path("/tmp/probe.cast"),
        )
        if installed:
            await session._install_recording_tools()
            assert environment.calls == [("tmux -V", None), ("asciinema --version", None)]
        else:
            try:
                await session._install_recording_tools()
            except PermissionError as exc:
                assert str(exc) == "installation requires root"
            else:
                raise AssertionError("Missing tools did not request privileged installation")
            command, user = environment.calls[-1]
            assert user == "root" and "apt-get install -y tmux asciinema" in command
            assert all(user is None for _, user in environment.calls[:-1])


async def _inside() -> None:
    from loom.models.task import TaskConfig
    from loom.models.trial import TrialConfig
    from loom.service_execution_sandbox_task import run_agent, run_verifier

    Path.home().mkdir(parents=True, exist_ok=True)
    await _verify_harbor_tool_identity()
    source = Path("/checkout/deploy/catalog/nebius-terminal-bench/file-archive-manifest")
    workspace = Path("/evidence/workspace")
    shutil.copytree(source / "original", workspace)
    shutil.copyfile(source / "verifier/run.sh", workspace / "verifier/run.sh")
    raw = tomllib.loads((workspace / "task.toml").read_text())
    raw["environment"].update({"cpu_arch": "x86_64", "baseline_network_policy": {"kind": "gateway-only"}})
    raw["verifier"].pop("user", None)
    raw["verifier"]["args"]["script_path"] = "verifier/run.sh"
    task = TaskConfig.model_validate(raw)
    trial = TrialConfig(agent_name="terminus-2", agent_model={"provider": "openai", "name": "glm-5.2"},
                        override_agent_timeout_sec=90, request_params={"temperature": 0.2})
    trial_id, team_id = uuid4(), uuid4()
    ledger: list[dict[str, object]] = []
    solution = base64.b64encode((source / "original/solution/solve.sh").read_bytes()).decode()
    command = (
        "test ! -e /app/tests/test_outputs.py && test ! -e /app/verifier/run.sh "
        "&& test ! -e /app/solution/solve.sh && printf '%s' '"
        + solution + "' | base64 -d | bash && touch /app/private-inputs-hidden\n"
    )

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args: object) -> None:
            pass

        def do_GET(self) -> None:
            assert self.path == "/internal/loom/llm-calls"
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"trial_id": str(trial_id), "team_id": str(team_id),
                                        "step_id": "agent", "items": ledger}).encode())

        def do_POST(self) -> None:
            assert self.path == "/openai/v1/chat/completions"
            request = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            assert request["model"] == "glm-5.2"
            assert request["temperature"] == 0.2
            index = len(ledger)
            assert index < 5, "unexpected additional model retry"
            ledger.append({
                "id": str(uuid4()), "trial_id": str(trial_id), "step_id": "agent",
                "model": "glm-5.2", "dialect": "openai_facade", "input_tokens": 10 + index,
                "output_tokens": 5 + index, "cost_usd": 0.0, "rate_card_hash": "local-stub",
                "provider_extras": {},
            })
            message = {"analysis": "Produce and verify the manifest", "plan": "Execute the deterministic fixture solver",
                       "commands": [{"keystrokes": command, "duration": 1}] if index == 0 else [],
                       "task_complete": index > 0}
            body = {"id": "chatcmpl-local-" + str(index), "object": "chat.completion", "created": 1,
                    "model": "glm-5.2", "choices": [{"index": 0, "message": {"role": "assistant", "content": json.dumps(message)},
                                                      "finish_reason": "stop"}],
                    "usage": {"prompt_tokens": 10 + index, "completion_tokens": 5 + index, "total_tokens": 15 + 2 * index}}
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(body).encode())

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    os.environ["LOOM_GATEWAY_URL"] = f"http://127.0.0.1:{server.server_port}"
    os.environ["LOOM_TASK_ARTIFACTS_JSON"] = '["archive_manifest.json","build_manifest.py","private-inputs-hidden"]'
    try:
        await run_agent(workspace, task, trial)
        await run_verifier(workspace, task, trial)
    finally:
        server.shutdown()
        server.server_close()
        thread.join()
    native = json.loads((workspace / ".loom/agent/harbor/trajectory.json").read_text())
    events = [json.loads(line) for line in (workspace / ".loom/agent/trajectory.jsonl").read_text().splitlines()]
    verifier = json.loads((workspace / ".loom/verifier/output.json").read_text())
    report = {"reward": verifier["rewards"]["resolved"],
              "native_turns": sum(step["source"] == "agent" for step in native["steps"]),
              "gateway_calls": len(ledger), "typed_calls": sum(event["kind"] == "llm_call" for event in events),
              "private_inputs_hidden": (workspace / ".loom/collected/private-inputs-hidden").exists(),
              "external_model_calls": 0}
    Path("/evidence/report.json").write_text(json.dumps(report, indent=2))
    print(json.dumps(report))


if __name__ == "__main__" and "--inside" in sys.argv:
    asyncio.run(_inside())
else:
    import pytest

    test_native_harbor_manifest_and_isolated_verifier = pytest.mark.skipif(
        not os.environ.get("LOOM_TERMINUS_SMOKE_IMAGE"),
        reason="local prepared task image required",
    )(test_native_harbor_manifest_and_isolated_verifier)
