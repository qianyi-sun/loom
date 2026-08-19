import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_web_lint_treats_warnings_as_failures() -> None:
    package_json = json.loads((ROOT / "web/package.json").read_text())

    assert package_json["scripts"]["lint"] == "eslint . --ext ts,tsx --max-warnings=0"


def test_openapi_typescript_lockfile_uses_safe_js_yaml() -> None:
    package_lock = json.loads((ROOT / "web/package-lock.json").read_text())

    core = package_lock["packages"]["node_modules/@redocly/openapi-core"]
    assert core["dependencies"]["js-yaml"] == "4.2.0"
    assert "node_modules/@redocly/openapi-core/node_modules/js-yaml" not in package_lock["packages"]
    assert package_lock["packages"]["node_modules/js-yaml"]["version"] == "4.2.0"


def test_web_declares_linux_lightningcss_bindings_for_multiarch_builds() -> None:
    package_json = json.loads((ROOT / "web/package.json").read_text())
    package_lock = json.loads((ROOT / "web/package-lock.json").read_text())

    required = {
        "lightningcss-linux-arm64-gnu": "1.32.0",
        "lightningcss-linux-x64-gnu": "1.32.0",
    }

    assert package_json.get("optionalDependencies") == required
    assert package_lock["packages"][""].get("optionalDependencies") == required


def test_web_dockerfile_installs_target_arch_native_bindings() -> None:
    dockerfile = (ROOT / "deploy/Dockerfile.web").read_text()

    assert "ARG TARGETARCH" in dockerfile
    assert "lightningcss-linux-arm64-gnu@1.32.0" in dockerfile
    assert "lightningcss-linux-x64-gnu@1.32.0" in dockerfile
    assert "@rolldown/binding-linux-arm64-gnu" in dockerfile
    assert "@rolldown/binding-linux-x64-gnu" in dockerfile
    assert '"${rolldown_binding}@1.0.3"' in dockerfile
    assert "require('lightningcss')" in dockerfile
    assert "require('${rolldown_binding}')" in dockerfile
