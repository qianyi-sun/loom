import json
import unittest
from datetime import datetime, timezone

import httpx

from agentic_data_platform.domain.run_records import ModelConfig, ModelMode, TerminalTurn
from agentic_data_platform.models.providers import (
    ModelCommand,
    ModelProviderContext,
    OpenAICompatibleModelProvider,
    ScriptedModelProvider,
)
from agentic_data_platform.providers.errors import ProviderBoundaryError, ProviderErrorCode


class ModelProviderTest(unittest.TestCase):
    def test_scripted_model_provider_returns_api_commands_in_order(self):
        provider = ScriptedModelProvider(
            model=ModelConfig(
                provider="mock-api",
                model_name="scripted-terminal-agent",
                mode=ModelMode.API,
                prompt_template_version="terminal-agent-v0",
            ),
            commands=[
                ModelCommand(command="python solve.py", cwd="/workspace", model_call_id="call_001"),
                ModelCommand(command="python verify.py", cwd="/workspace", model_call_id="call_002"),
            ],
        )

        context = ModelProviderContext(run_id="run_001", task_instruction="Create the spreadsheet.", turns=[])

        self.assertEqual(provider.next_command(context).command, "python solve.py")
        self.assertEqual(provider.next_command(context).model_call_id, "call_002")
        self.assertIsNone(provider.next_command(context))

    def test_scripted_model_provider_uses_domain_api_only_model_config(self):
        with self.assertRaisesRegex(ValueError, "API-based model access"):
            ScriptedModelProvider(
                model=ModelConfig(
                    provider="local",
                    model_name="local-weight-model",
                    mode="local_weights",
                    prompt_template_version="terminal-agent-v0",
                ),
                commands=[ModelCommand(command="python solve.py")],
            )

    def test_openai_compatible_provider_turns_response_json_into_terminal_command(self):
        requests: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            return httpx.Response(
                200,
                json={
                    "id": "chatcmpl_command_001",
                    "choices": [
                        {
                            "message": {
                                "content": (
                                    '{"action":"run","command":"python solve.py",'
                                    '"cwd":"/workspace"}'
                                )
                            }
                        }
                    ],
                },
            )

        provider = OpenAICompatibleModelProvider(
            model=_api_model("gpt-5-mini"),
            base_url="https://models.example/v1",
            api_key="sk-model-secret",
            http_client=httpx.Client(transport=httpx.MockTransport(handler)),
        )

        command = provider.next_command(
            ModelProviderContext(
                run_id="run_provider_001",
                task_instruction="Create receipts.xlsx from the PDFs.",
                turns=[_turn(stdout="listed receipt files\n")],
            )
        )

        self.assertEqual(
            command,
            ModelCommand(
                command="python solve.py",
                cwd="/workspace",
                model_call_id="chatcmpl_command_001",
            ),
        )
        self.assertEqual(requests[0].url, "https://models.example/v1/chat/completions")
        self.assertEqual(requests[0].headers["authorization"], "Bearer sk-model-secret")
        body = json.loads(requests[0].content)
        self.assertEqual(body["model"], "gpt-5-mini")
        self.assertIn("Create receipts.xlsx", body["messages"][-1]["content"])
        self.assertIn("listed receipt files", body["messages"][-1]["content"])
        self.assertNotIn("sk-model-secret", json.dumps(body))

    def test_openai_compatible_provider_returns_none_for_finish_decision(self):
        provider = OpenAICompatibleModelProvider(
            model=_api_model("gpt-5-mini"),
            base_url="https://models.example/v1",
            api_key="sk-model-secret",
            http_client=httpx.Client(
                transport=httpx.MockTransport(
                    lambda request: httpx.Response(
                        200,
                        json={"choices": [{"message": {"content": '{"action":"finish"}'}}]},
                    )
                )
            ),
        )

        self.assertIsNone(
            provider.next_command(
                ModelProviderContext(
                    run_id="run_provider_finish_001",
                    task_instruction="All files are complete.",
                    turns=[],
                )
            )
        )

    def test_openai_compatible_provider_normalizes_invalid_action_response(self):
        provider = OpenAICompatibleModelProvider(
            model=_api_model("gpt-5-mini"),
            base_url="https://models.example/v1",
            api_key="sk-model-secret",
            http_client=httpx.Client(
                transport=httpx.MockTransport(
                    lambda request: httpx.Response(
                        200,
                        json={"choices": [{"message": {"content": "not json"}}]},
                    )
                )
            ),
        )

        with self.assertRaises(ProviderBoundaryError) as error:
            provider.next_command(
                ModelProviderContext(
                    run_id="run_provider_invalid_001",
                    task_instruction="Create receipts.xlsx.",
                    turns=[],
                )
            )

        self.assertEqual(error.exception.code, ProviderErrorCode.INVALID_REQUEST)
        self.assertIn("terminal action", error.exception.message)
        self.assertNotIn("sk-model-secret", str(error.exception))

    def test_openai_compatible_provider_normalizes_http_failure_without_secret(self):
        provider = OpenAICompatibleModelProvider(
            model=_api_model("gpt-5-mini"),
            base_url="https://models.example/v1",
            api_key="sk-model-secret",
            http_client=httpx.Client(
                transport=httpx.MockTransport(
                    lambda request: httpx.Response(503, text="temporarily unavailable")
                )
            ),
        )

        with self.assertRaises(ProviderBoundaryError) as error:
            provider.next_command(
                ModelProviderContext(
                    run_id="run_provider_http_001",
                    task_instruction="Create receipts.xlsx.",
                    turns=[],
                )
            )

        self.assertEqual(error.exception.code, ProviderErrorCode.UNAVAILABLE)
        self.assertTrue(error.exception.retryable)
        self.assertNotIn("sk-model-secret", str(error.exception))


def _api_model(model_name: str) -> ModelConfig:
    return ModelConfig(
        provider="openai-compatible",
        model_name=model_name,
        mode=ModelMode.API,
        prompt_template_version="terminal-agent-json-v0",
    )


def _turn(*, stdout: str = "") -> TerminalTurn:
    now = datetime(2026, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    return TerminalTurn(
        turn_index=0,
        command="ls receipts",
        cwd="/workspace",
        started_at=now,
        completed_at=now,
        exit_code=0,
        stdout=stdout,
        stderr="",
        changed_paths=["receipts"],
        model_call_id="previous-call",
    )
