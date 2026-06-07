def test_step1_exists() -> None:
    with open("/workspace/step1.txt") as fh:
        assert fh.read().strip() == "step1"
