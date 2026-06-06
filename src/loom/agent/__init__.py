"""Agent runtimes (spec §2.1).

Submodules (added incrementally as tasks complete):
- base        : AgentRuntime + InBoxAgentRuntime Protocols + LLMGatewayClient
- oracle      : OracleAgent — deterministic, runs solution/solve.sh
- litellm     : LiteLLMAgent — generic tool-loop via LLM Gateway
- claude_code : ClaudeCodeAgent — in-box runtime tailing /loom/trajectory.jsonl
"""
