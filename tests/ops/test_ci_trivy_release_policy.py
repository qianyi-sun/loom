from __future__ import annotations

import hashlib
import stat
import subprocess
import sys
from datetime import date
from pathlib import Path

import pytest
import yaml
from scripts import write_trivy_release_policy as policy

REPO_ROOT = Path(__file__).resolve().parents[2]
POLICY_SCRIPT = REPO_ROOT / "scripts/write_trivy_release_policy.py"
CONFIG_BYTES = (
    b"exit-code: 1\n"
    b"pkg:\n"
    b"  types:\n"
    b"    - os\n"
    b"    - library\n"
    b"scan:\n"
    b"  scanners:\n"
    b"    - vuln\n"
    b"severity:\n"
    b"  - CRITICAL\n"
    b"timeout: 10m0s\n"
    b"vulnerability:\n"
    b"  ignore-unfixed: false\n"
)
CONFIG_SHA256 = "35492da1d08b142bd1489ac54ecdedab62634b7b3095a37cebbe10b61df1adac"
IGNORE_BYTES = (
    b"vulnerabilities:\n"
    b"  - id: CVE-2026-13221\n"
    b"    purls:\n"
    b'      - "pkg:deb/debian/perl-base"\n'
    b"    expired_at: 2026-09-12\n"
    b"    statement: No fixed Debian package was available on 2026-08-12.\n"
    b"  - id: CVE-2026-42496\n"
    b"    purls:\n"
    b'      - "pkg:deb/debian/perl-base"\n'
    b"    expired_at: 2026-09-12\n"
    b"    statement: No fixed Debian package was available on 2026-08-12.\n"
    b"  - id: CVE-2026-57433\n"
    b"    purls:\n"
    b'      - "pkg:deb/debian/perl-base"\n'
    b"    expired_at: 2026-09-12\n"
    b"    statement: No fixed Debian package was available on 2026-08-12.\n"
    b"  - id: CVE-2026-8376\n"
    b"    purls:\n"
    b'      - "pkg:deb/debian/perl-base"\n'
    b"    expired_at: 2026-09-12\n"
    b"    statement: No fixed Debian package was available on 2026-08-12.\n"
)
IGNORE_SHA256 = "83156c673c73bc58e7848876fe2144f36e7ab2dc147b7a6a55a41bfa2a88ee29"


def test_policy_writer_ignores_checkout_local_trivy_overrides(tmp_path: Path) -> None:
    checkout = tmp_path / "checkout"
    controlled = tmp_path / "controlled"
    checkout.mkdir()
    controlled.mkdir()
    (checkout / "trivy.yaml").write_text("exit-code: 0\n", encoding="utf-8")
    (checkout / ".trivyignore").write_text("CVE-ALL\n", encoding="utf-8")
    config = controlled / "loom-trivy-release.yaml"
    ignore = controlled / "loom-trivy-release.ignore.yaml"

    result = subprocess.run(
        [
            sys.executable,
            str(POLICY_SCRIPT),
            "--config-file",
            str(config),
            "--ignore-file",
            str(ignore),
        ],
        cwd=checkout,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout == ""
    assert config.read_bytes() == CONFIG_BYTES
    assert hashlib.sha256(config.read_bytes()).hexdigest() == CONFIG_SHA256
    assert ignore.read_bytes() == IGNORE_BYTES
    assert hashlib.sha256(ignore.read_bytes()).hexdigest() == IGNORE_SHA256
    assert stat.S_ISREG(config.lstat().st_mode)
    assert stat.S_ISREG(ignore.lstat().st_mode)
    assert not config.is_symlink()
    assert not ignore.is_symlink()
    assert (checkout / "trivy.yaml").read_text(encoding="utf-8") == "exit-code: 0\n"
    assert (checkout / ".trivyignore").read_text(encoding="utf-8") == "CVE-ALL\n"


def test_policy_writer_rejects_expired_temporary_exceptions(tmp_path: Path) -> None:
    with pytest.raises(policy.TrivyPolicyError, match="expired"):
        policy.write_release_policy(
            tmp_path / "trivy.yaml",
            tmp_path / "trivy.ignore",
            today=date(2026, 9, 12),
        )

    assert list(tmp_path.iterdir()) == []


def test_policy_writer_requires_structured_ignore_file_extension(tmp_path: Path) -> None:
    with pytest.raises(policy.TrivyPolicyError, match="YAML"):
        policy.write_release_policy(
            tmp_path / "trivy.yaml",
            tmp_path / "trivy.ignore",
            today=date(2026, 8, 12),
        )

    assert list(tmp_path.iterdir()) == []


def test_workflow_scans_only_the_controlled_absolute_policy_files() -> None:
    workflow = yaml.safe_load(
        (REPO_ROOT / ".github/workflows/images.yml").read_text(encoding="utf-8")
    )

    for job_name, scan_name in (
        ("build", "Scan native image archive"),
        ("publish", "Scan trusted image archive"),
    ):
        steps = workflow["jobs"][job_name]["steps"]
        scan_index = next(
            index for index, step in enumerate(steps) if step.get("name") == scan_name
        )
        policy_step = steps[scan_index - 1]
        scan = steps[scan_index]

        assert policy_step["name"] == "Generate controlled Trivy policy"
        assert policy_step["run"].strip() == (
            "python3 scripts/write_trivy_release_policy.py \\\n"
            "  --config-file /tmp/loom-trivy-release.yaml \\\n"
            "  --ignore-file /tmp/loom-trivy-release.ignore.yaml"
        )
        assert "python3 scripts/install_trivy.py" in scan["run"]
        assert "--config /tmp/loom-trivy-release.yaml" in scan["run"]
        assert "--ignorefile /tmp/loom-trivy-release.ignore.yaml" in scan["run"]
