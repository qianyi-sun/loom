import os


def test_payload_at_least_100mib() -> None:
    size = os.path.getsize("/workspace/payload.bin")
    assert size >= 100 * 1024 * 1024
