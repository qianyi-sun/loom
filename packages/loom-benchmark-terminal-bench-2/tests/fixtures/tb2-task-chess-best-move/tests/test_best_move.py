from pathlib import Path


def test_best_move_file_exists() -> None:
    assert Path("/app/best_move.txt").exists()
