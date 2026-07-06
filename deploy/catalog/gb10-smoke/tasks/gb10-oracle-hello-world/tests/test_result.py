def test_result_file_contains_hello() -> None:
    with open("/workspace/result.txt") as fh:
        assert fh.read().strip() == "hello"
