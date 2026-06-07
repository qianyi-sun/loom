import os


def test_ok_file() -> None:
    assert os.path.isfile("/workspace/ok.txt")


def test_ready_marker() -> None:
    assert os.path.isfile("/workspace/.ready")
