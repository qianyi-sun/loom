"""Preserved declarations and overridden image defaults need honest diagnostics."""

from pathlib import Path

import pytest

from tests.loom_cli.test_local_compatibility_report import _report, _write_bundle


def _harbor_bundle(tmp_path, dockerfile):
    bundle = _write_bundle(tmp_path, "harbor-shell")
    (bundle / "task.toml").write_text('version = "1.0"\n[metadata]\nname = "Harbor shell fixture"\n')
    (bundle / "environment/Dockerfile").write_text(dockerfile)
    return bundle


@pytest.mark.parametrize("shell", [
    "sh", "/bin/sh", "/usr/bin/sh", "bash", "/bin/bash", "/usr/bin/bash",
    "zsh", "/bin/zsh", "/usr/bin/zsh",
])
def test_harbor_bare_shell_cmd_records_reference_runner_conversion(tmp_path, capsys, shell):
    bundle = _harbor_bundle(tmp_path, f'FROM ubuntu:24.04\nCMD ["{shell}"]\n')
    before = {path: path.read_bytes() for path in bundle.rglob("*") if path.is_file()}

    rc, payload = _report(tmp_path, capsys)

    assert rc == 0
    report = payload["compatibility_report"]["tasks"][0]
    assert report["status"] == "converted"
    assert report["diagnostics"] == []
    change, = [item for item in report["changes"] if item["field"] == "environment.dockerfile.CMD"]
    assert change["category"] == "equivalent_conversion"
    assert change["before"] == f'["{shell}"]' and change["after"] is None
    assert change["source_location"] == f"{bundle}/environment/Dockerfile:2"
    assert "Harbor" in change["reason"] and "Bash" in change["reason"]
    assert payload["compatibility_report"]["runtime_verified"] is False
    assert any("registry-image startup" in item for item in payload["compatibility_report"]["limitations"])
    assert {path: path.read_bytes() for path in bundle.rglob("*") if path.is_file()} == before


@pytest.mark.parametrize("directive", [
    'CMD ["bash", "-c", "initialize"]', 'CMD ["bash", "--login"]',
    'CMD ["/custom/bash"]', 'CMD ["/start.sh"]', 'CMD bash',
    'CMD ["bash", 1]', 'CMD ["bash",]', 'ENTRYPOINT ["bash"]',
])
def test_harbor_real_or_unrecognized_startup_still_requires_review(tmp_path, capsys, directive):
    _harbor_bundle(tmp_path, "FROM ubuntu:24.04\n" + directive + "\n")

    rc, payload = _report(tmp_path, capsys)

    assert rc == 1
    report = payload["compatibility_report"]["tasks"][0]
    assert any(item["code"] == "dockerfile_startup_overridden" for item in report["diagnostics"])
    assert not any(item["field"] == "environment.dockerfile.CMD" for item in report["changes"])


def test_harbor_shell_conversion_preserves_inherited_entrypoint_diagnostic(tmp_path, capsys):
    bundle = _harbor_bundle(tmp_path, 'FROM ubuntu:24.04 AS base\nENTRYPOINT ["/start.sh"]\n'
                            'CMD ["bash"]\nFROM base\n')

    rc, payload = _report(tmp_path, capsys)

    assert rc == 1
    report = payload["compatibility_report"]["tasks"][0]
    diagnostic, = report["diagnostics"]
    assert diagnostic["source_location"] == f"{bundle}/environment/Dockerfile:2"
    assert "ENTRYPOINT" in diagnostic["reason"]
    change, = [item for item in report["changes"] if item["field"] == "environment.dockerfile.CMD"]
    assert change["source_location"] == f"{bundle}/environment/Dockerfile:3"


@pytest.mark.parametrize("final", ["FROM ubuntu:24.04\n", 'FROM base\nENTRYPOINT ["/start.sh"]\n'])
def test_harbor_unused_or_reset_shell_cmd_is_not_reported_as_conversion(tmp_path, capsys, final):
    _harbor_bundle(tmp_path, 'FROM ubuntu:24.04 AS base\nCMD ["bash"]\n' + final)

    _, payload = _report(tmp_path, capsys)

    report = payload["compatibility_report"]["tasks"][0]
    assert not any(item["field"] == "environment.dockerfile.CMD" for item in report["changes"])


def test_preserved_identities_require_runtime_qualification(tmp_path, capsys):
    bundle = _write_bundle(tmp_path, "identity", user="root")
    path = bundle / "task.toml"
    path.write_text(path.read_text().replace('[verifier]\n', '[verifier]\nuser = "root"\n'))

    rc, payload = _report(tmp_path, capsys)

    assert rc == 1
    report = payload["compatibility_report"]["tasks"][0]
    assert report["admission_passed"] is True
    diagnostics = {item["code"]: item for item in report["diagnostics"]}
    for code in ("task_identity", "verifier_identity"):
        assert "preserved" in diagnostics[code]["reason"]
        assert "supports_task_identity" in diagnostics[code]["suggested_action"]
    assert not {"environment.user", "verifier.user"} & {item["field"] for item in report["changes"]}
    assert payload["compatibility_report"]["runtime_verified"] is False


def test_preserved_web_allowlist_is_not_reported_as_replaced(tmp_path, capsys):
    _write_bundle(tmp_path, "web", network_policies_supported=["web-allowlist"],
                  baseline_network_policy={"kind": "web-allowlist", "destinations": [
                      {"host": "example.com", "protocol": "https"},
                  ]})

    rc, payload = _report(tmp_path, capsys)

    assert rc == 1
    report = payload["compatibility_report"]["tasks"][0]
    assert report["admission_passed"] is True
    diagnostics = {item["code"]: item for item in report["diagnostics"]}
    assert "preserved" in diagnostics["runtime_egress"]["reason"]
    assert "supports_task_web_egress" in diagnostics["runtime_egress"]["suggested_action"]
    assert "network_policy_change" not in diagnostics
    assert not any("network" in item["field"] for item in report["changes"])


@pytest.mark.parametrize("directive", [
    'ENTRYPOINT ["/entrypoint.sh"]',
    'CMD ["python3", "-m", "http.server"]',
    'CMD ["/bin/bash"]',
    'USER root',
])
def test_image_runtime_declarations_cannot_silently_be_overridden(tmp_path, capsys, directive):
    bundle = _write_bundle(tmp_path, "image")
    dockerfile = bundle / "environment/Dockerfile"
    dockerfile.write_text("FROM ubuntu:24.04\n" + directive + "\n")
    before = {path: path.read_bytes() for path in bundle.rglob("*") if path.is_file()}

    rc, payload = _report(tmp_path, capsys)

    assert rc == 1
    report = payload["compatibility_report"]["tasks"][0]
    assert report["admission_passed"] is True
    code = "dockerfile_user_overridden" if directive.startswith("USER") else "dockerfile_startup_overridden"
    diagnostic = next(item for item in report["diagnostics"] if item["code"] == code)
    assert diagnostic["source_location"] == f"{dockerfile}:2"
    assert diagnostic["category"] == "unsupported_conversion"
    assert directive.split()[0] in diagnostic["reason"]
    assert {path: path.read_bytes() for path in bundle.rglob("*") if path.is_file()} == before


def test_runtime_diagnostics_follow_stage_inheritance_and_source_lines(tmp_path, capsys):
    bundle = _write_bundle(tmp_path, "inherited")
    (bundle / "environment/Dockerfile").write_text(
        "FROM ubuntu:24.04 AS base\n"
        'ENTRYPOINT ["/start.sh"]\n'
        "USER root\n"
        "FROM ubuntu:24.04 AS unused\n"
        'CMD ["/unused.sh"]\n'
        "FROM base\n"
        "RUN <<'SCRIPT'\nUSER fictional\nCMD imaginary\nSCRIPT\n"
    )

    rc, payload = _report(tmp_path, capsys)

    assert rc == 1
    diagnostics = payload["compatibility_report"]["tasks"][0]["diagnostics"]
    assert [(item["code"], Path(item["source_location"]).name) for item in diagnostics] == [
        ("dockerfile_startup_overridden", "Dockerfile:2"),
        ("dockerfile_user_overridden", "Dockerfile:3"),
    ]


@pytest.mark.parametrize("final", [
    "FROM ubuntu:24.04\n",
    "FROM base\nENTRYPOINT []\nCMD []\nUSER 65532:65532\n",
])
def test_unused_or_cleared_image_defaults_do_not_block_conversion(tmp_path, capsys, final):
    bundle = _write_bundle(tmp_path, "cleared")
    (bundle / "environment/Dockerfile").write_text(
        'FROM ubuntu:24.04 AS base\nENTRYPOINT ["/old.sh"]\nCMD ["serve"]\nUSER root\n' + final
    )

    rc, payload = _report(tmp_path, capsys)

    assert rc == 0
    assert payload["compatibility_report"]["tasks"][0]["diagnostics"] == []


@pytest.mark.parametrize(("source_user", "task_user", "home"), [
    ("root", "0:0", "/root"),
    ("0:0", "root", "/root"),
    ("1200:1300", "1200:1300", "/home/task"),
])
def test_explicit_matching_user_is_preserved_but_needs_ready_runtime(
    tmp_path, capsys, source_user, task_user, home,
):
    bundle = _write_bundle(tmp_path, "identity", user=task_user, environment={"HOME": home})
    (bundle / "environment/Dockerfile").write_text(f"FROM ubuntu:24.04\nUSER {source_user}\n")

    rc, payload = _report(tmp_path, capsys)

    assert rc == 1
    report = payload["compatibility_report"]["tasks"][0]
    assert report["admission_passed"] is True
    codes = {item["code"] for item in report["diagnostics"]}
    assert "task_identity" in codes
    assert "dockerfile_user_overridden" not in codes


@pytest.mark.parametrize("startup", [[], ["/entrypoint.sh", "/bin/true"]])
def test_service_declaration_requires_qualification_and_explicit_initializer(tmp_path, capsys, startup):
    bundle = _write_bundle(tmp_path, "service", service_lifecycle={
        "startup_command": startup, "readiness": {"command": "test -f /tmp/ready"},
    })
    (bundle / "environment/Dockerfile").write_text('FROM ubuntu:24.04\nENTRYPOINT ["/entrypoint.sh"]\n')

    rc, payload = _report(tmp_path, capsys)

    assert rc == 1
    report = payload["compatibility_report"]["tasks"][0]
    diagnostics = {item["code"]: item for item in report["diagnostics"]}
    assert "service_lifecycle_ready" in diagnostics["service_lifecycle"]["suggested_action"]
    assert ("dockerfile_startup_overridden" in diagnostics) is (not startup)


def test_schema_only_report_does_not_apply_runtime_overrides(tmp_path, capsys):
    bundle = _write_bundle(tmp_path, "schema")
    (bundle / "environment/Dockerfile").write_text('FROM ubuntu:24.04\nENTRYPOINT ["/start.sh"]\nUSER root\n')

    rc, payload = _report(tmp_path, capsys, profile=False)

    assert rc == 0
    assert payload["compatibility_report"]["tasks"][0]["status"] == "schema_valid"


def test_prepared_input_still_reports_the_original_user_override(tmp_path, capsys):
    bundle = _write_bundle(tmp_path, "prepared", dockerfile="environment/Dockerfile.loom-nebius")
    original = "FROM ubuntu:24.04\nUSER root\n"
    (bundle / "environment/Dockerfile").write_text(original)
    (bundle / "environment/Dockerfile.loom-nebius").write_text(original + "USER 65532:65532\n")

    rc, payload = _report(tmp_path, capsys)

    assert rc == 1
    report = payload["compatibility_report"]["tasks"][0]
    assert report["admission_passed"] is True
    diagnostic = next(item for item in report["diagnostics"] if item["code"] == "dockerfile_user_overridden")
    assert diagnostic["source_location"] == f"{bundle}/environment/Dockerfile:2"
