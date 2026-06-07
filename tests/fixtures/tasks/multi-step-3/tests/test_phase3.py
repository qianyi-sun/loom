def test_step3_exists() -> None:
    with open("/workspace/step3.txt") as fh:
        assert fh.read().strip() == "step3"
