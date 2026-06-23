# Provider Onboarding

Loom supports two provider setup paths in service mode:

1. A hosted third-party API where the user already has a provider URL and API
   key.
2. A self-hosted checkpoint on a GPU cluster, exposed as an OpenAI-compatible
   HTTP service and then registered in Loom.

Both paths end in the same Loom object: a team-scoped provider connection. Team
owners create it, Loom encrypts the API key at rest, the LLM Gateway uses it
server-side, and users select the connection plus model id when creating a
batch.

## Hosted Third-Party API

Use this path for OpenAI-compatible hosts such as Together, Fireworks, vLLM
behind your own ingress, and provider-native endpoints such as Anthropic or
Google when those types are enabled.

1. In the web app, switch to the team that will own execution, cost, provider
   credentials, members, and API tokens.
2. Open **Providers** -> **New provider connection**.
3. Choose the provider type.
4. Enter a short display name.
5. Enter the provider API root:
   - OpenAI-compatible services should end in `/v1`, for example
     `https://api.together.xyz/v1`.
   - Anthropic and Google should use their documented API root.
6. Paste the API key into the password field. Do not paste keys into issue
   comments, browser URLs, docs, or chat transcripts.
7. Leave **Allowed models** blank to allow all discovered agent-capable models,
   or enter one model id per line to restrict the picker.
8. Create the connection.
9. Open the provider detail page and click **Test connection**.
10. Open **Models**, click **Refresh**, and hide noisy non-chat entries if the
    upstream catalog includes embeddings, rerankers, or tool-only models.
11. Open **New batch**, choose this provider connection, choose a discovered
    model, and submit a small smoke batch before starting large runs.

The CLI path uses secret indirection so keys do not land in shell history:

```bash
export LOOM_API_TOKEN=loom_api_...
loom auth login --server https://loom.example.com --token env:LOOM_API_TOKEN

export PROVIDER_API_KEY=...
loom providers create \
  --name together-prod \
  --type openai-compatible \
  --base-url https://api.together.xyz/v1 \
  --api-key env:PROVIDER_API_KEY \
  --rate-card-provider together

loom providers test together-prod
loom providers models together-prod --refresh
loom providers models together-prod
```

Common failures:

- `401` or `403`: the provider key is invalid, expired, missing entitlement for
  that model, or not propagated yet after rotation.
- Invalid base URL: use the API root, not a provider dashboard URL. For
  OpenAI-compatible services the root should end in `/v1`.
- SSRF or private endpoint denial: expose the service through an approved
  public/tunnel path or ask an admin to enable an explicit team policy.
- Empty model list: run `loom providers models NAME --refresh`; if the endpoint
  does not implement useful discovery, add model ids manually from the web
  Models tab or the provider model API.
- Noisy model list: hide non-agent models in the Models tab.

## GPU Cluster Checkpoint

Use this path when the user owns a checkpoint or Hugging Face model and a
Slurm-accessible GPU cluster. Loom does not need SSH access to the cluster for
normal inference calls. The cluster service only needs to expose an
OpenAI-compatible HTTP endpoint that the Loom server can reach.

### Prerequisites

- A Slurm login or bastion node where the user can submit jobs.
- A shared filesystem visible to the login/bastion node and compute node.
- A Python environment with vLLM installed. On Lux-like clusters this may be an
  existing venv such as `/pm/qy/uv_envs/vcbm`.
- A model path or Hugging Face repo id that the compute node can read or
  download.
- A network exposure plan:
  - direct public node or service port;
  - bastion TCP forward;
  - SSH reverse tunnel;
  - VPN, Tailscale, WireGuard, or other admin-approved path;
  - or a user-provided final URL.

### Generate the Slurm Bundle

Run this from the Slurm login/bastion node:

```bash
loom inference deploy slurm \
  --model Qwen/Qwen2.5-Coder-7B-Instruct \
  --served-model-name qwen2.5-coder-7b-instruct \
  --provider-name lux-qwen25-coder-7b \
  --partition compute \
  --gres gpu:h100:1 \
  --time-limit 2-00:00:00 \
  --venv /pm/qy/uv_envs/vcbm \
  --port 8001 \
  --expose user-provided \
  --endpoint-url http://bastion.example.com:18001/v1 \
  --output-dir ~/loom-inference/lux-qwen25 \
  --no-submit
```

The command creates:

- `run-vllm.sbatch`: Slurm job script.
- `submit.sh`: submits the Slurm job.
- `launch-vllm.py`: starts vLLM without putting the generated API key in the
  long-lived OS process argv.
- `healthcheck.sh`: validates `/v1/models` and `/v1/chat/completions`.
- `register-provider.sh`: creates the Loom provider connection, tests it, and
  refreshes models.
- `secrets/provider-api-key`: generated API key with mode `0600`.
- `loom-registration.json`: non-secret registration fields for scripts or docs.

Use `--submit` instead of `--no-submit` when you want the command to call
`sbatch` immediately after generating the bundle.

### Start, Expose, and Register

```bash
cd ~/loom-inference/lux-qwen25
./submit.sh
```

Wait for Slurm to allocate the job and vLLM to finish loading the model. Then
make the endpoint reachable from Loom. For a bastion-forward setup, keep a
separate TCP forward process alive for the lifetime of the Slurm job. The final
URL registered in Loom must be the public/tunnel URL ending in `/v1`, not the
private compute-node address unless the Loom server is allowed to reach that
private network.

When you generated the bundle with `--expose bastion-forward`, run the forward
helper on the bastion after Slurm assigns a compute node:

```bash
./start-bastion-forward.sh <compute-node-host-or-ip>
```

Probe the service before registering:

```bash
./healthcheck.sh
```

Then authenticate to Loom and register:

```bash
export LOOM_API_TOKEN=loom_api_...
loom auth login --server https://loom.example.com --token env:LOOM_API_TOKEN
./register-provider.sh
```

The generated registration script uses `--api-key file:...`; it does not print
or embed the raw key. After registration, open the provider page in the web app,
confirm the test result is `Ready`, refresh the Models tab if needed, and start
a small smoke batch with the served model.

### Stop and Restart

Use normal Slurm controls:

```bash
squeue --name loom-vllm
scancel <job-id>
./submit.sh
```

If the exposure path is a bastion forward or tunnel, restart that process after
job restarts. If the compute node changes, update the forward target before
running `./healthcheck.sh` again.

### Security Notes

- Do not paste generated inference API keys into shell history, issue comments,
  browser URLs, docs, or chat transcripts.
- Generated key files are owner-only. Preserve those permissions when copying
  a bundle.
- The vLLM launcher reads the key file internally so the generated key is not
  visible as a long-lived OS process argument.
- Loom stores only the provider connection secret reference and redacted
  validation errors. Use `loom providers rotate-key` if the key is exposed.
- Private endpoints remain blocked by Loom's SSRF protections unless an admin
  explicitly allows the team/network path.

### Operator Smoke

For a Lux-like smoke, validate:

1. `loom inference deploy slurm ... --no-submit` creates the bundle and does
   not print the generated key.
2. `./submit.sh` starts a Slurm job on the requested partition/GPU.
3. The service reaches `ready` through `./healthcheck.sh` from the same network
   path that Loom will use.
4. `./register-provider.sh` creates the connection, `loom providers test`
   returns `valid`, and `loom providers models NAME --refresh` lists the served
   model.
5. A small model-backed batch reaches verifier output. A numeric reward of `0`
   is still a platform-successful run; missing verifier output is not.
