"""Sandbox execution surface — Driver Protocol + concrete implementations.

Submodules (added incrementally as tasks complete):
- base           : Driver Protocol + Capabilities helpers
- fake           : FakeDriver (in-memory, deterministic)
- docker         : DockerDriver (production)
- network_policy : iptables rule application for DockerDriver
"""
