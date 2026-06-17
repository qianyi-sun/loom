import subprocess
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
_SCRIPT = _REPO_ROOT / "scripts" / "ops" / "worker_pool_inventory.sh"


def test_worker_pool_inventory_script_is_valid_bash() -> None:
    subprocess.run(["bash", "-n", str(_SCRIPT)], check=True)


def test_worker_pool_inventory_script_has_no_environment_specific_hosts() -> None:
    text = _SCRIPT.read_text(encoding="utf-8")
    forbidden = (
        "OLD" + "LAB",
        "192" + ".168.",
        "10" + ".",
        "172" + ".16.",
        "platform" + "-dev",
    )
    assert not any(marker in text for marker in forbidden)
