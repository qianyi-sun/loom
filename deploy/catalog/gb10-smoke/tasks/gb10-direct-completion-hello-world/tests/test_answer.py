def test_answer_file_contains_hello() -> None:
    with open("/workspace/answer.txt") as fh:
        assert fh.read().strip() == "hello"
