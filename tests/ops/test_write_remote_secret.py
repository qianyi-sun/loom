from __future__ import annotations

import importlib.util
import subprocess
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
_SCRIPT = _ROOT / "scripts" / "ops" / "write_remote_secret.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("write_remote_secret", _SCRIPT)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_remote_secret_writer_passes_secret_only_as_ssh_stdin(capsys) -> None:
    module = _load_module()
    secret = "gho_FAKE_TOKEN_DO_NOT_PRINT_1234567890"
    calls: list[dict[str, object]] = []

    def fake_run(args, **kwargs):
        calls.append({"args": list(args), "kwargs": dict(kwargs)})
        if args == ["gh", "auth", "token"]:
            return subprocess.CompletedProcess(args, 0, secret + "\n", "")
        if args[:4] == ["ssh", "-o", "BatchMode=yes", "platform-dev"]:
            assert kwargs["input"] == secret.encode()
            return subprocess.CompletedProcess(
                args,
                0,
                "remote_secret_file_written mode=600 size=38 path=/remote/token\n",
                "",
            )
        raise AssertionError(f"unexpected command: {args!r}")

    rc = module.main(
        [
            "--host",
            "platform-dev",
            "--remote-path",
            "/remote/token",
            "--",
            "gh",
            "auth",
            "token",
        ],
        run=fake_run,
    )

    captured = capsys.readouterr()
    assert rc == 0
    assert "remote_secret_file_written mode=600" in captured.out
    assert secret not in captured.out
    assert secret not in captured.err
    for call in calls:
        assert secret not in " ".join(str(part) for part in call["args"])

    ssh_call = calls[1]
    remote_script = ssh_call["args"][-1]
    assert ssh_call["args"][4].startswith("bash -lc ")
    assert "cat > \"$tmp\"" in remote_script
    assert "<<" not in remote_script


def test_remote_secret_writer_rejects_empty_command_output(capsys) -> None:
    module = _load_module()

    def fake_run(args, **kwargs):
        return subprocess.CompletedProcess(args, 0, "\n", "")

    rc = module.main(
        [
            "--host",
            "platform-dev",
            "--remote-path",
            "/remote/token",
            "--",
            "gh",
            "auth",
            "token",
        ],
        run=fake_run,
    )

    captured = capsys.readouterr()
    assert rc == 2
    assert "secret command produced no output" in captured.err
