# Select a published Harbor runtime

Native Nebius batches can select a published Terminus-2 controller version.
The task Dockerfile or image still supplies the task and verifier environments.
Changing the controller version does not require rebuilding those images or
redeploying the platform.

`GET /api/v1/agents` includes `versions` on each catalog item. The `terminus-2`
versions expose `agent_version`, `harbor_version`, and `loom_bridge_revision`.
Use one of these case-sensitive version labels; arbitrary image URLs are not
accepted. Omit `agent_version` to keep the deployment default.

```sh
loom eval batch create --backend nebius --agent terminus-2 \
  --agent-version harbor-0.18.0-abc123 --provider my-provider \
  --model my-model --benchmark my-benchmark
```

The API equivalent puts `agent_version` in `trial_config`. For a comparison,
put the field on each item in `combinations` (or the CLI's
`--combinations-json`), alongside `agent_name` and `agent_model`. Shared
`trial_config.agent_version` cannot be combined with `combinations`. Generated
combination labels include the selected version.

Selection currently requires automatic native Nebius execution and
`agent_name: terminus-2`; explicitly templated execution and other backends
reject it. At submission the service resolves every selected label once and
freezes its controller image and release metadata in the Batch runtime profile.
Attempts and failed-case reruns retain that profile, even if the deployment
default or the catalog subsequently changes. A new Batch resolves the catalog
again. The existing image-admission records are reused and deduplicated within
the profile. No new admission scheme is introduced.

## Register a release

Publish the controller image and retain the publisher's
`agent-runtime-release.json`. An operator registers that exact file through the
Control Plane with the existing admin credential source:

```sh
loom admin agent-runtime register --cp-url https://control-plane.example \
  --admin-token env:LOOM_ADMIN_TOKEN --release agent-runtime-release.json
```

This calls `PUT /admin/agents/terminus-2/versions/{agent_version}` and requires
`admin:tokens`. The release contains schema
`loom.agent-runtime-release.v1`, runtime contract
`loom.terminus-controller.v1`, an immutable `agent_image_ref`, Harbor package
version and source revision, Loom bridge and publisher source revisions, and
the publisher's existing `image_admission` for that image. Source revisions are
40 lowercase hexadecimal characters. Labels match
`[A-Za-z0-9][A-Za-z0-9._-]{0,127}` and identify the whole controller release,
including Loom changes, rather than only the upstream Harbor version.

The Control Plane verifies the existing admission and stores the release under
the Agent's `(name, version)` key. Replaying the identical file is idempotent.
Rebinding a label to different contents returns HTTP 409, and generic catalog
provisioning cannot overwrite a registered release. Preserve the original JSON
for a retry; generating a new admission timestamp/signature changes the record.

The controller reports its installed Harbor package version and image-baked
source/bridge metadata. Task environment variables and uploaded inputs do not
choose those values. Canonical ATIF output and accounting repair preserve the
Trial's selected release label; older Trials retain their previous fallback.
