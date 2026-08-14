# Multiple Local Model Servers

CLI mode can run one or more Hugging Face or local-weight models through vLLM,
connect to an existing OpenAI-compatible server, or keep one vLLM process in
the foreground with `loom serve`.

## Foreground server

```bash
loom serve hf:meta-llama/Llama-3.1-8B-Instruct --name llama8b
```

`loom serve` launches vLLM, waits for `/v1/models`, and writes a temporary
`[local_providers.<name>]` entry containing the base URL and served model name.
It blocks until Ctrl-C, SIGTERM, or server exit, then removes the config entry
and stops the process.

Model specs may be `hf:<org>/<model>` or an absolute/relative path. The command
also accepts vLLM host, port, tensor-parallel, memory, context-length, and eager
execution options. Port `0` selects from 8234 upward and retries a later port
when vLLM reports an address-in-use failure.

This is a foreground lifecycle. There is no persistent daemon or separate stop
command.

## Comparing models

Repeat `--model` in one `loom run` invocation:

```bash
loom run --dataset humaneval --agent direct-completion \
  --model hf:meta-llama/Llama-3.1-8B-Instruct \
  --model /models/my-checkpoint \
  --output-dir runs/compare
```

Multiple models run sequentially by default: Loom starts a server, runs all
selected tasks, stops it, then proceeds to the next model. This bounds peak GPU
memory to one model. Output is grouped as
`<output-dir>/<model-slug>/<trial-id>/`.

`--parallel-models` starts all managed servers first and runs their trials
concurrently. The user must provide enough GPU memory and device placement for
all models. Multi-model runs always stop their managed servers; `--keep-alive`
applies only to a single managed model.

With `--local-server URL`, repeated model values are sent as upstream model
IDs to that same already-running server. Managed `hf:` or path specs cannot be
combined with `--local-server`.

The local launcher exists only in CLI mode. Service-mode local endpoints are
configured on the LLM Gateway through `gateway_local_providers` or the
corresponding Gateway settings. See [Local LLMs](local-llm.md) and the
[user guide](../user-guide.md#comparing-multiple-models).
