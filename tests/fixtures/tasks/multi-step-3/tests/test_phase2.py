def test_step2_exists() -> None:
    with open("/workspace/step2.txt") as fh:
        assert fh.read().strip() == "step2"
