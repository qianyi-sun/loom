"""`_resolve_adapter` end of the remap path (issue #234, PR-2).

`run_import` is heavy (testcontainer + S3 + DB), but `_resolve_adapter`
isolates the only piece that knows about remaps. Tests inject a stub
adapter into REGISTRY, point `_resolve_adapter` at a TOML file, and
assert the returned adapter has the remap's id + upstream — not the
inherit's.
"""
from __future__ import annotations

from pathlib import Path

import pytest
from loom_benchmarks.base import UpstreamSource
from loom_benchmarks.registry import REGISTRY

from loom_benchmark_tool.import_cmd import _resolve_adapter


class _StubAdapter:
    name = "humaneval"
    display_name = "HumanEval"
    upstream_source = UpstreamSource(
        kind="huggingface", locator="openai/openai_humaneval",
    )
    license_spdx = "MIT"
    license_url = "https://example.org/upstream-LICENSE"
    series = "code"
    splits: tuple[str, ...] = ("test",)

    def list_instances(self, *, source_dir: Path, split: str):  # type: ignore[no-untyped-def]
        return iter(())

    def convert_instance(self, instance, *, out_dir: Path):  # type: ignore[no-untyped-def]
        raise NotImplementedError


@pytest.fixture
def stub_humaneval(monkeypatch: pytest.MonkeyPatch) -> _StubAdapter:
    stub = _StubAdapter()
    monkeypatch.setitem(REGISTRY, "humaneval", stub)
    return stub


def _write_toml(path: Path, body: str) -> Path:
    p = path / "benchmarks.toml"
    p.write_text(body)
    return p


def test_resolve_registry_adapter_passthrough(
    stub_humaneval: _StubAdapter,
) -> None:
    """Plain (non-remap) benchmark passes through to REGISTRY."""
    assert _resolve_adapter("humaneval") is stub_humaneval


def test_resolve_unknown_raises(
    tmp_path: Path, stub_humaneval: _StubAdapter,
) -> None:
    cfg = _write_toml(tmp_path, "schema_version = 1\n")
    with pytest.raises(KeyError, match="no benchmark adapter"):
        _resolve_adapter("totally-unknown", benchmarks_config_path=cfg)


def test_resolve_remap_overrides_name_and_upstream(
    tmp_path: Path, stub_humaneval: _StubAdapter,
) -> None:
    cfg = _write_toml(tmp_path, """\
schema_version = 1

[[remap]]
id = "humaneval-fork"
inherit = "humaneval"
display_name = "HumanEval (fork)"
upstream_kind = "huggingface"
upstream_locator = "myorg/humaneval-fork"
license_spdx = "Apache-2.0"
license_url = "https://example.org/fork-LICENSE"
""")
    remapped = _resolve_adapter(
        "humaneval-fork", benchmarks_config_path=cfg,
    )
    # Identity overrides
    assert remapped.name == "humaneval-fork"
    assert remapped.upstream_source.kind == "huggingface"
    assert remapped.upstream_source.locator == "myorg/humaneval-fork"
    assert remapped.license_spdx == "Apache-2.0"
    assert remapped.license_url == "https://example.org/fork-LICENSE"
    # Inherited (no override): splits, list_instances/convert_instance
    assert remapped.splits == ("test",)
    # The base adapter is untouched (we copied)
    assert stub_humaneval.name == "humaneval"
    assert stub_humaneval.upstream_source.locator == "openai/openai_humaneval"
    assert stub_humaneval.license_spdx == "MIT"


def test_resolve_remap_with_splits_override(
    tmp_path: Path, stub_humaneval: _StubAdapter,
) -> None:
    cfg = _write_toml(tmp_path, """\
schema_version = 1

[[remap]]
id = "humaneval-fork-v2"
inherit = "humaneval"
display_name = "fork"
upstream_kind = "git"
upstream_locator = "https://git.example.org/x.git"
license_spdx = "MIT"
license_url = "https://example.org/L"
splits = ["test", "extra"]
""")
    remapped = _resolve_adapter(
        "humaneval-fork-v2", benchmarks_config_path=cfg,
    )
    assert remapped.splits == ("test", "extra")


def test_resolve_remap_with_unknown_inherit_raises(
    tmp_path: Path,
) -> None:
    cfg = _write_toml(tmp_path, """\
schema_version = 1

[[remap]]
id = "fork-of-nothing"
inherit = "no-such-adapter"
display_name = "x"
upstream_kind = "huggingface"
upstream_locator = "x/y"
license_spdx = "MIT"
license_url = "https://example.org/L"
""")
    with pytest.raises(KeyError, match="not in REGISTRY"):
        _resolve_adapter(
            "fork-of-nothing", benchmarks_config_path=cfg,
        )


def test_resolve_with_no_toml_and_unknown_benchmark_raises(
    tmp_path: Path,
) -> None:
    # No benchmarks.toml at all (path returns None from resolve_config_path).
    with pytest.raises(KeyError, match="no benchmark adapter"):
        _resolve_adapter(
            "unknown", benchmarks_config_path=tmp_path / "nope.toml",
        )


def test_resolve_remap_overrides_display_name(
    tmp_path: Path, stub_humaneval: _StubAdapter,
) -> None:
    """Regression for #234 review: import_cmd writes `display_name`
    from the resolved adapter, so the remap's display_name MUST be
    surfaced on the adapter — otherwise an import-before-sync flow
    would write the base's display_name into the benchmarks row.
    """
    cfg = _write_toml(tmp_path, """\
schema_version = 1

[[remap]]
id = "humaneval-fork-display"
inherit = "humaneval"
display_name = "HumanEval (fork display)"
upstream_kind = "huggingface"
upstream_locator = "x/y"
license_spdx = "MIT"
license_url = "https://example.org/L"
""")
    remapped = _resolve_adapter(
        "humaneval-fork-display", benchmarks_config_path=cfg,
    )
    assert remapped.display_name == "HumanEval (fork display)"
    # Base is untouched
    assert stub_humaneval.display_name == "HumanEval"


class _WritingStubAdapter:
    """Adapter whose `convert_instance` actually exercises `self.name`
    so we can assert it ends up in the on-disk task.toml + the
    returned ConvertedTask.task_id."""

    name = "stub-base"
    display_name = "Stub"
    upstream_source = UpstreamSource(kind="huggingface", locator="stub/base")
    license_spdx = "MIT"
    license_url = "https://example.org/L"
    series = "stub"
    splits: tuple[str, ...] = ("test",)

    def list_instances(self, *, source_dir: Path, split: str):  # type: ignore[no-untyped-def]
        return iter(())

    def convert_instance(self, instance, *, out_dir: Path):  # type: ignore[no-untyped-def]
        # Exercise the exact pattern HumanEval / SWE-Bench use.
        task_id = f"{self.name}/{instance['id']}"
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / "task.toml").write_text(
            f'schema_version = "1"\n'
            f"[task]\n"
            f'id = "{task_id}"\n',
        )
        from loom_benchmarks.base import ConvertedTask
        return ConvertedTask(
            task_id=task_id, checksum="x" * 64,
            license_spdx=self.license_spdx, warnings=(),
        )


def test_remap_propagates_into_convert_instance_outputs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """End-to-end: after remap resolution, calling convert_instance on
    the remapped adapter writes the remap's id into the on-disk task.toml
    AND the returned ConvertedTask. Guards against future refactors that
    might read cls.name instead of self.name and silently break remaps.
    """
    monkeypatch.setitem(REGISTRY, "stub-base", _WritingStubAdapter())
    cfg = _write_toml(tmp_path, """\
schema_version = 1

[[remap]]
id = "stub-remap"
inherit = "stub-base"
display_name = "Stub Remap"
upstream_kind = "huggingface"
upstream_locator = "stub/fork"
license_spdx = "MIT"
license_url = "https://example.org/L"
""")
    remapped = _resolve_adapter(
        "stub-remap", benchmarks_config_path=cfg,
    )
    out_dir = tmp_path / "out"
    converted = remapped.convert_instance({"id": "ex1"}, out_dir=out_dir)

    assert converted.task_id == "stub-remap/ex1"
    on_disk = (out_dir / "task.toml").read_text()
    assert 'id = "stub-remap/ex1"' in on_disk
    # And the base must NOT have leaked the override (deepcopy guard).
    base = REGISTRY["stub-base"]
    assert base.name == "stub-base"
    assert base.upstream_source.locator == "stub/base"
