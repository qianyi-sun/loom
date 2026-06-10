"""`loom serve <spec> --name X` — foreground launcher that
auto-registers the server in user config and removes the entry on
shutdown.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from loom_cli.__main__ import main


def test_serve_subcommand_exists() -> None:
    """Smoke: `loom serve --help` exits 0 (argparse recognised the
    subcommand)."""
    with pytest.raises(SystemExit) as exc:
        main(["serve", "--help"])
    assert exc.value.code == 0


def test_serve_launches_and_registers_in_config(
    tmp_xdg_home: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Happy path: serve calls launch_vllm, persists [local.X] to
    config, and would block until shutdown (we patch the blocker)."""
    import loom_cli.serve_cmd as sc
    import loom_cli.vllm_runner as vr

    fake_info = vr.VLLMServerInfo(
        base_url="http://localhost:8234/v1",
        served_model_name="meta-llama/Llama-3.1-8B-Instruct",
        pid=99999,
    )
    monkeypatch.setattr(sc, "launch_vllm", lambda spec: fake_info)
    monkeypatch.setattr(sc, "stop_one", lambda proc: None)

    block_called: list[bool] = []

    async def _no_block() -> None:
        block_called.append(True)

    monkeypatch.setattr(sc, "_block_until_shutdown", _no_block)

    rc = main([
        "serve",
        "hf:meta-llama/Llama-3.1-8B-Instruct",
        "--name", "llama8b",
    ])
    assert rc == 0
    assert block_called == [True]

    # Config entry written then removed (cleanup happens after block)
    from loom_cli.config import load_config
    cfg = load_config()
    assert "llama8b" not in cfg.local_providers


def test_serve_launch_failure_returns_2_without_writing_config(
    tmp_xdg_home: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import loom_cli.serve_cmd as sc

    def _raise(_spec):
        raise RuntimeError("vLLM bind failed everywhere")

    monkeypatch.setattr(sc, "launch_vllm", _raise)

    rc = main([
        "serve",
        "hf:meta-llama/Llama-3.1-8B-Instruct",
        "--name", "llama8b",
    ])
    assert rc == 2

    from loom_cli.config import load_config
    cfg = load_config()
    assert "llama8b" not in cfg.local_providers


def test_serve_missing_vllm_dep_returns_2(
    tmp_xdg_home: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import loom_cli.serve_cmd as sc
    import loom_cli.vllm_runner as vr

    def _raise(_spec):
        raise vr.MissingVLLMDependencyError("pip install loom[vllm]")

    monkeypatch.setattr(sc, "launch_vllm", _raise)

    rc = main([
        "serve",
        "hf:meta-llama/Llama-3.1-8B-Instruct",
    ])
    assert rc == 2


def test_serve_default_name_uses_model_slug(
    tmp_xdg_home: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import loom_cli.serve_cmd as sc
    import loom_cli.vllm_runner as vr

    fake_info = vr.VLLMServerInfo(
        base_url="http://localhost:8234/v1",
        served_model_name="meta-llama/Llama-3.1-8B-Instruct",
        pid=99999,
    )
    monkeypatch.setattr(sc, "launch_vllm", lambda spec: fake_info)
    monkeypatch.setattr(sc, "stop_one", lambda proc: None)

    captured: list[str] = []

    async def _no_block() -> None:
        # While blocked, snapshot the config to see what name was used
        from loom_cli.config import load_config
        cfg = load_config()
        captured.extend(cfg.local_providers.keys())

    monkeypatch.setattr(sc, "_block_until_shutdown", _no_block)

    rc = main([
        "serve",
        "hf:meta-llama/Llama-3.1-8B-Instruct",
        # No --name given → default to slug.
    ])
    assert rc == 0
    assert "llama-3-1-8b-instruct" in captured


def test_serve_rejects_already_registered_name(
    tmp_xdg_home: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If `local/<name>` already exists in config, `loom serve --name <name>`
    refuses with exit 2 rather than silently overwriting."""
    from loom_cli.config import set_local_provider
    set_local_provider("llama8b", base_url="http://existing.example/v1")

    import loom_cli.serve_cmd as sc
    # Should never be called because name-collision check happens first
    monkeypatch.setattr(sc, "launch_vllm", lambda spec: pytest.fail(
        "launch_vllm should not be called on name collision"))

    rc = main([
        "serve",
        "hf:meta-llama/Llama-3.1-8B-Instruct",
        "--name", "llama8b",
    ])
    assert rc == 2

    # Existing config entry is preserved
    from loom_cli.config import load_config
    cfg = load_config()
    assert cfg.local_providers["llama8b"].base_url == "http://existing.example/v1"


def test_serve_rejects_bare_model_id_no_prefix(
    tmp_xdg_home: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`loom serve meta-llama/X` (no hf: prefix, no leading /)
    is a typo — should error early before invoking vLLM."""
    import loom_cli.serve_cmd as sc
    monkeypatch.setattr(sc, "launch_vllm", lambda spec: pytest.fail(
        "launch_vllm should not be called on bad spec"))

    with pytest.raises(SystemExit, match="hf:<org>/<name> or a path"):
        main([
            "serve",
            "meta-llama/Llama-3.1-8B-Instruct",
        ])


def test_serve_rejects_hf_id_without_org(
    tmp_xdg_home: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`loom serve hf:no-slash` is invalid — mirrors `loom run`'s check."""
    import loom_cli.serve_cmd as sc
    monkeypatch.setattr(sc, "launch_vllm", lambda spec: pytest.fail(
        "launch_vllm should not be called on bad spec"))

    with pytest.raises(SystemExit, match="<org>/<name>"):
        main(["serve", "hf:no-slash"])
