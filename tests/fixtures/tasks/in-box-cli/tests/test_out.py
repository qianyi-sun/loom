def test_out_file() -> None:
    with open("/workspace/out.txt") as fh:
        assert fh.read().strip() == "in-box"
