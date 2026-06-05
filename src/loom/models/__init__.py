"""Loom data models — pure Pydantic, no I/O.

Submodule layout:
- types       : primitive types and scalar enums
- networking  : NetworkPolicy tagged union
- mcp         : MCPConnection
- healthcheck : HealthcheckSpec
- skill       : SkillRef
- capabilities: Capabilities + RequiredCapabilities
- task        : TaskConfig and friends
- trial       : TrialConfig + RetryPolicy
- result      : TrialResult + StepResult
- verifier    : VerifierResult + CheckResult
- trajectory  : TrajectoryEvent catalog
- exec        : ExecResult
"""
