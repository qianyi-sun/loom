import unittest

from agentic_data_platform.domain.run_records import ModelConfig, ModelMode
from agentic_data_platform.models.providers import ModelCommand, ModelProviderContext, ScriptedModelProvider


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
