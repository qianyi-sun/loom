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
    b"  - id: CVE-2023-45853\n"
    b"    purls:\n"
    b'      - "pkg:deb/debian/zlib1g"\n'
    b"    expired_at: 2026-09-12\n"
    b"    statement: Debian marked this finding will-not-fix on 2026-08-12; zlib1g is a required dependency of the staging-compatible PostgreSQL 17.4 rehearsal image.\n"
    b"  - id: CVE-2025-7458\n"
    b"    purls:\n"
    b'      - "pkg:deb/debian/libsqlite3-0"\n'
    b"    expired_at: 2026-09-12\n"
    b"    statement: No fixed Debian package was available on 2026-08-12; this package is a required dependency of the staging-compatible PostgreSQL 17.4 rehearsal image.\n"
    b"  - id: CVE-2026-13221\n"
    b"    purls:\n"
    b'      - "pkg:deb/debian/libperl5.36"\n'
    b'      - "pkg:deb/debian/libperl5.40"\n'
    b'      - "pkg:deb/debian/perl"\n'
    b'      - "pkg:deb/debian/perl-base"\n'
    b'      - "pkg:deb/debian/perl-modules-5.36"\n'
    b'      - "pkg:deb/debian/perl-modules-5.40"\n'
    b"    expired_at: 2026-09-12\n"
    b"    statement: No fixed Debian package was available on 2026-08-12; these Perl packages are required by Debian base runtimes, the agent toolchain, and the staging-compatible PostgreSQL 17.4 rehearsal image.\n"
    b"  - id: CVE-2026-42496\n"
    b"    purls:\n"
    b'      - "pkg:deb/debian/libperl5.36"\n'
    b'      - "pkg:deb/debian/libperl5.40"\n'
    b'      - "pkg:deb/debian/perl"\n'
    b'      - "pkg:deb/debian/perl-base"\n'
    b'      - "pkg:deb/debian/perl-modules-5.36"\n'
    b'      - "pkg:deb/debian/perl-modules-5.40"\n'
    b"    expired_at: 2026-09-12\n"
    b"    statement: No fixed Debian package was available on 2026-08-12; these Perl packages are required by Debian base runtimes, the agent toolchain, and the staging-compatible PostgreSQL 17.4 rehearsal image.\n"
    b"  - id: CVE-2026-43185\n"
    b"    purls:\n"
    b'      - "pkg:deb/debian/linux-libc-dev"\n'
    b"    expired_at: 2026-09-12\n"
    b"    statement: No fixed Debian package was available on 2026-08-12; linux-libc-dev is required by the agent sandbox compiler toolchain.\n"
    b"  - id: CVE-2026-57433\n"
    b"    purls:\n"
    b'      - "pkg:deb/debian/libperl5.36"\n'
    b'      - "pkg:deb/debian/libperl5.40"\n'
    b'      - "pkg:deb/debian/perl"\n'
    b'      - "pkg:deb/debian/perl-base"\n'
    b'      - "pkg:deb/debian/perl-modules-5.36"\n'
    b'      - "pkg:deb/debian/perl-modules-5.40"\n'
    b"    expired_at: 2026-09-12\n"
    b"    statement: No fixed Debian package was available on 2026-08-12; these Perl packages are required by Debian base runtimes, the agent toolchain, and the staging-compatible PostgreSQL 17.4 rehearsal image.\n"
    b"  - id: CVE-2026-6653\n"
    b"    purls:\n"
    b'      - "pkg:deb/debian/libxml2"\n'
    b"    expired_at: 2026-09-12\n"
    b"    statement: No fixed Debian package was available on 2026-08-12; this package is a required dependency of the staging-compatible PostgreSQL 17.4 rehearsal image.\n"
    b"  - id: CVE-2026-8376\n"
    b"    purls:\n"
    b'      - "pkg:deb/debian/libperl5.36"\n'
    b'      - "pkg:deb/debian/libperl5.40"\n'
    b'      - "pkg:deb/debian/perl"\n'
    b'      - "pkg:deb/debian/perl-base"\n'
    b'      - "pkg:deb/debian/perl-modules-5.36"\n'
    b'      - "pkg:deb/debian/perl-modules-5.40"\n'
    b"    expired_at: 2026-09-12\n"
    b"    statement: No fixed Debian package was available on 2026-08-12; these Perl packages are required by Debian base runtimes, the agent toolchain, and the staging-compatible PostgreSQL 17.4 rehearsal image.\n"
)
IGNORE_SHA256 = "b09bd1a38036f5e4274586af64616a306590ec33b1e2ac8a73d67ab88d2e4d5a"


def test_temporary_exceptions_cover_only_required_unfixed_packages() -> None:
    exceptions = {item.vulnerability_id: item for item in policy.TRIVY_EXCEPTIONS}

    expected_perl_purls = (
        "pkg:deb/debian/libperl5.36",
        "pkg:deb/debian/libperl5.40",
        "pkg:deb/debian/perl",
        "pkg:deb/debian/perl-base",
        "pkg:deb/debian/perl-modules-5.36",
        "pkg:deb/debian/perl-modules-5.40",
    )
    for vulnerability_id in (
        "CVE-2026-13221",
        "CVE-2026-42496",
        "CVE-2026-57433",
        "CVE-2026-8376",
    ):
        assert exceptions[vulnerability_id].purls == expected_perl_purls

    assert exceptions["CVE-2026-43185"].purls == (
        "pkg:deb/debian/linux-libc-dev",
    )
    assert exceptions["CVE-2025-7458"].purls == ("pkg:deb/debian/libsqlite3-0",)
    assert exceptions["CVE-2026-6653"].purls == ("pkg:deb/debian/libxml2",)
    assert exceptions["CVE-2023-45853"].purls == ("pkg:deb/debian/zlib1g",)


def test_temporary_perl_exceptions_explain_every_required_runtime() -> None:
    exceptions = {item.vulnerability_id: item for item in policy.TRIVY_EXCEPTIONS}
    expected_statement = (
        "No fixed Debian package was available on 2026-08-12; these Perl packages are "
        "required by Debian base runtimes, the agent toolchain, and the staging-compatible "
        "PostgreSQL 17.4 rehearsal image."
    )

    for vulnerability_id in (
        "CVE-2026-13221",
        "CVE-2026-42496",
        "CVE-2026-57433",
        "CVE-2026-8376",
    ):
        assert exceptions[vulnerability_id].statement == expected_statement


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
        assert "python3 scripts/install_trivy.py" not in scan["run"]
        assert "/tmp/loom-trivy-binaries/${ARCHITECTURE}/trivy" in scan["run"]
        assert "--config /tmp/loom-trivy-release.yaml" in scan["run"]
        assert "--ignorefile /tmp/loom-trivy-release.ignore.yaml" in scan["run"]
        assert "--show-suppressed" in scan["run"]
        validation = (
            "python3 scripts/validate_trivy_release_report.py \\\n"
            '  --component "$IMAGE_NAME" \\\n'
            '  --architecture "$ARCHITECTURE" \\\n'
            '  --report "$REPORT" \\\n'
            "  --ignore-file /tmp/loom-trivy-release.ignore.yaml"
        )
        assert scan["run"].rstrip().endswith(validation)
        assert scan["env"]["IMAGE_NAME"] == "${{ matrix.image }}"
        assert scan["env"]["ARCHITECTURE"] == "${{ matrix.architecture }}"

        next_step = steps[scan_index + 1]
        if job_name == "build":
            assert next_step["name"] == "Record candidate archive provenance"
        else:
            assert next_step["name"] == "Record trusted scan digest"


def test_workflow_validates_complete_reports_before_hashing_or_recording() -> None:
    workflow = yaml.safe_load(
        (REPO_ROOT / ".github/workflows/images.yml").read_text(encoding="utf-8")
    )

    build_steps = workflow["jobs"]["build"]["steps"]
    build_names = [step.get("name") for step in build_steps]
    assert build_names.index("Scan native image archive") < build_names.index(
        "Record candidate archive provenance"
    )

    publish_steps = workflow["jobs"]["publish"]["steps"]
    publish_names = [step.get("name") for step in publish_steps]
    assert publish_names.index("Scan trusted image archive") < publish_names.index(
        "Record trusted scan digest"
    )
    digest_step = next(
        step
        for step in publish_steps
        if step.get("name") == "Record trusted scan digest"
    )
    assert 'sha256sum "$report"' in digest_step["run"]
