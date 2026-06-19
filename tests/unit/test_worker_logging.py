import logging

from loom_worker.__main__ import _configure_logging


def test_worker_debug_logging_suppresses_s3_sdk_signature_loggers() -> None:
    _configure_logging("debug")

    for logger_name in (
        "boto3",
        "botocore",
        "botocore.auth",
        "botocore.endpoint",
        "s3transfer",
        "urllib3",
    ):
        assert logging.getLogger(logger_name).getEffectiveLevel() >= logging.WARNING
