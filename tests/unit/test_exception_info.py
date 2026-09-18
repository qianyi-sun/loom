from loom.errors import AgentError, exception_info


def test_adapter_cause_is_retained_without_leaking_secret_or_response_traceback():
    class ContextLengthExceededError(Exception):
        pass

    try:
        try:
            raise ContextLengthExceededError("Authorization: Bearer private-step-token sk-abcdef123456789")
        except ContextLengthExceededError as exc:
            raise AgentError(str(exc)) from exc
    except AgentError as exc:
        info = exception_info(exc)
    assert info.exception_type == "ContextLengthExceededError"
    assert "private-step-token" not in info.model_dump_json()
    assert "sk-abcdef123456789" not in info.model_dump_json()
    assert "traceback" not in info.model_dump()


def test_sdk_exception_type_is_not_replaced_by_its_transport_cause():
    class InternalServerError(Exception):
        pass

    try:
        try:
            raise OSError("transport")
        except OSError as exc:
            raise InternalServerError from exc
    except InternalServerError as exc:
        info = exception_info(exc)
    assert info.exception_type == info.exception_message == "InternalServerError"


def test_adapter_cause_cycle_is_bounded():
    first, second = AgentError("first"), AgentError("second")
    first.__cause__, second.__cause__ = second, first
    assert exception_info(first).exception_type == "AgentError"
