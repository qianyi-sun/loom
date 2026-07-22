from pathlib import Path


def test_result() -> None:
    assert Path("/workspace/result.txt").read_text().strip() == "family-runs-dev-ok"
