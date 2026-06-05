from loom.errors import (
    AgentError,
    AgentSetupTimeoutError,
    CapabilityMismatchError,
    ConfigError,
    DriverAlreadyStartedError,
    DriverError,
    DriverExecError,
    DriverNotStartedError,
    LoomError,
    TaskSchemaError,
    TrajectoryError,
    TrajectoryFlushFailedError,
    VerifierError,
    WorkerLostClaimError,
)


def test_root_is_loom_error():
    for cls in [
        DriverError, AgentError, VerifierError, TrajectoryError,
        WorkerLostClaimError, ConfigError,
    ]:
        assert issubclass(cls, LoomError)


def test_driver_subclasses():
    for cls in [DriverAlreadyStartedError, DriverNotStartedError, DriverExecError]:
        assert issubclass(cls, DriverError)


def test_driver_exec_error_carries_payload():
    err = DriverExecError("nope", return_code=1, stdout=b"", stderr=b"oops")
    assert err.return_code == 1
    assert err.stderr == b"oops"
    assert str(err) == "nope"


def test_config_subclasses():
    assert issubclass(TaskSchemaError, ConfigError)
    assert issubclass(CapabilityMismatchError, ConfigError)


def test_trajectory_subclass():
    assert issubclass(TrajectoryFlushFailedError, TrajectoryError)


def test_agent_setup_timeout():
    assert issubclass(AgentSetupTimeoutError, AgentError)
