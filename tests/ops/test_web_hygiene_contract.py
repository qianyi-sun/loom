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
