const state = {
  user: null,
  projects: [],
  models: [],
  harnesses: [],
  agents: [],
  agentAdaptation: null,
  benchmarks: [],
  tasks: [],
  selectedRunId: null,
  pollTimer: null,
  eventSource: null,
  eventSeq: 0,
};

const el = (id) => document.getElementById(id);

window.addEventListener("DOMContentLoaded", () => {
  el("login-form").addEventListener("submit", onLogin);
  el("logout-button").addEventListener("click", onLogout);
  el("refresh-button").addEventListener("click", refreshAll);
  el("launch-button").addEventListener("click", launchRun);
  el("download-button").addEventListener("click", downloadBundle);
  el("model-select").addEventListener("change", () => {
    refreshAgentAdaptation().catch(showRunError);
  });
  el("agent-select").addEventListener("change", () => {
    refreshAgentAdaptation().catch(showRunError);
  });
  el("project-select").addEventListener("change", () => {
    loadAgents().catch(showRunError);
    refreshDashboard().catch(showRunError);
  });
  el("harness-select").addEventListener("change", () => {
    loadAgents().catch(showRunError);
  });
  el("benchmark-select").addEventListener("change", loadTasksForSelectedBenchmark);
  restoreSession();
});

async function restoreSession() {
  try {
    const payload = await api("/auth/session");
    state.user = payload.user;
    showApp();
    await refreshAll();
  } catch (error) {
    showLogin();
  }
}

async function onLogin(event) {
  event.preventDefault();
  setText("login-error", "");
  const username = el("login-username").value.trim();
  const password = el("login-password").value;
  try {
    const payload = await api("/auth/login", {
      method: "POST",
      body: JSON.stringify({ username, password }),
    });
    state.user = payload.user;
    showApp();
    await refreshAll();
  } catch (error) {
    setText("login-error", error.message);
  }
}

async function onLogout() {
  await api("/auth/logout", { method: "POST" }).catch(() => null);
  clearInterval(state.pollTimer);
  state.pollTimer = null;
  closeRunStream();
  state.user = null;
  state.selectedRunId = null;
  state.eventSeq = 0;
  showLogin();
}

function showLogin() {
  el("login-view").classList.remove("hidden");
  el("app-view").classList.add("hidden");
}

function showApp() {
  el("login-view").classList.add("hidden");
  el("app-view").classList.remove("hidden");
  setText("session-user", state.user ? `${state.user.display_name || state.user.user_id}` : "");
}

async function refreshAll() {
  setText("catalog-state", "Loading");
  await Promise.all([loadProjects(), loadModels(), loadHarnesses(), loadBenchmarks()]);
  await loadAgents();
  await loadTasksForSelectedBenchmark();
  await refreshDashboard();
  if (state.selectedRunId) {
    await refreshRun(state.selectedRunId);
  }
  setText("catalog-state", "Ready");
}

async function loadProjects() {
  const payload = await api("/projects");
  state.projects = payload.projects || [];
  fillSelect(
    el("project-select"),
    state.projects,
    (project) => project.project_id,
    (project) => `${project.name} (${project.project_id})`,
  );
}

async function loadModels() {
  const payload = await api("/models");
  state.models = (payload.models || []).filter((model) => !model.disabled);
  setText("model-state", modelCatalogMessage(payload));
  fillSelect(
    el("model-select"),
    state.models,
    (model) => model.model_id,
    (model) => `${model.display_name} (${model.provider})`,
  );
}

async function loadHarnesses() {
  const payload = await api("/harnesses");
  state.harnesses = payload.harnesses || [];
  fillSelect(
    el("harness-select"),
    state.harnesses,
    (harness) => harness.harness_id,
    (harness) => harness.display_name,
  );
}

async function loadAgents() {
  const select = el("agent-select");
  const harness = selectedHarness();
  if (!harness?.metadata?.harbor_compatible) {
    state.agents = [];
    fillSelect(
      select,
      [{ agent_id: "platform-default", display_name: "Platform default" }],
      (agent) => agent.agent_id,
      (agent) => agent.display_name,
    );
    select.disabled = true;
    setText("agent-state", "");
    return;
  }

  const query = new URLSearchParams({ harness_id: harness.harness_id });
  const project = selectedProject();
  if (project) {
    query.set("project_id", project.project_id);
  }
  const payload = await api(`/agents?${query.toString()}`);
  state.agents = payload.agents || [];
  fillSelect(
    select,
    state.agents,
    (agent) => agent.agent_id,
    (agent) => {
      const required = Array.isArray(agent.required_secret_refs) && agent.required_secret_refs.length
        ? `; requires ${agent.required_secret_refs.join(", ")}`
        : "";
      return `${agent.display_name || agent.agent_id}${required}`;
    },
  );
  const defaultAgent = harness.metadata?.default_agent_id;
  if (defaultAgent && state.agents.some((agent) => agent.agent_id === defaultAgent)) {
    select.value = defaultAgent;
  }
  select.disabled = false;
  await refreshAgentAdaptation();
}

async function refreshAgentAdaptation() {
  const project = selectedProject();
  const model = selectedModel();
  const harness = selectedHarness();
  const agent = selectedAgent();
  if (!project || !model || !harness?.metadata?.harbor_compatible || !agent) {
    state.agentAdaptation = null;
    setText("agent-state", "");
    return null;
  }
  const query = new URLSearchParams({
    project_id: project.project_id,
    harness_id: harness.harness_id,
    agent_id: agent.agent_id,
    model_id: model.model_id,
  });
  if (model.provider_config_id) {
    query.set("provider_config_id", model.provider_config_id);
  }
  const payload = await api(`/harbor/agent-adaptation?${query.toString()}`);
  state.agentAdaptation = payload;
  setText("agent-state", adaptationMessage(payload));
  return payload;
}

async function loadBenchmarks() {
  const payload = await api("/benchmarks");
  state.benchmarks = payload.benchmarks || [];
  fillSelect(
    el("benchmark-select"),
    state.benchmarks,
    (benchmark) => `${benchmark.suite_name}::${benchmark.benchmark_version}`,
    (benchmark) => `${benchmark.suite_name} (${benchmark.source_version || benchmark.benchmark_version})`,
  );
}

async function loadTasksForSelectedBenchmark() {
  const benchmark = selectedBenchmark();
  if (!benchmark) {
    fillSelect(el("task-select"), [], () => "", () => "");
    return;
  }
  const query = new URLSearchParams({
    benchmark_suite: benchmark.suite_name,
    benchmark_version: benchmark.benchmark_version,
  });
  const payload = await api(`/tasks?${query.toString()}`);
  state.tasks = payload.tasks || [];
  fillSelect(
    el("task-select"),
    state.tasks,
    (task) => `${task.task_family}::${task.instance_id}`,
    (task) => `${task.task_family} / ${task.instance_id}`,
  );
}

async function refreshDashboard() {
  const project = selectedProject();
  if (!project) {
    return;
  }
  const payload = await api(`/dashboard/progress?project_id=${encodeURIComponent(project.project_id)}`);
  setText("queue-depth", String(payload.summary?.queue_depth ?? 0));
}

async function launchRun() {
  setText("launch-error", "");
  const project = selectedProject();
  const model = selectedModel();
  const harness = selectedHarness();
  const agent = selectedAgent();
  const benchmark = selectedBenchmark();
  const task = selectedTask();
  if (!project || !model || !harness || !benchmark || !task || (harness.metadata?.harbor_compatible && !agent)) {
    setText("launch-error", "Select a project, model, harness, agent, benchmark, and task.");
    return;
  }
  if (harness.metadata?.harbor_compatible) {
    const adaptation = await refreshAgentAdaptation();
    if (adaptation?.status === "blocked") {
      setText("launch-error", adaptationMessage(adaptation));
      return;
    }
  }
  const runId = `frontend_${Date.now()}`;
  const payload = buildRunPayload({ runId, project, model, harness, benchmark, task, agent });
  try {
    const created = await api("/runs", {
      method: "POST",
      body: JSON.stringify(payload),
    });
    state.selectedRunId = created.run.run_id;
    state.eventSeq = 0;
    el("download-button").disabled = true;
    await refreshRun(state.selectedRunId);
    startPolling(state.selectedRunId);
    startRunStream(state.selectedRunId);
  } catch (error) {
    setText("launch-error", error.message);
  }
}

function buildRunPayload({ runId, project, model, harness, benchmark, task, agent = null }) {
  const instruction =
    task.metadata?.instruction ||
    `Follow ${benchmark.suite_name} task ${task.task_family}/${task.instance_id} from ${task.instruction_ref}.`;
  const launchMetadata = {
    launched_from: "frontend",
    harness_id: harness.harness_id,
  };
  let evaluators;
  if (harness.metadata?.harbor_compatible) {
    launchMetadata.harbor_run = harborRunConfig({ harness, model, task, agent });
    evaluators = [{ evaluator_id: "harbor-verifier", mode: "harbor_verifier" }];
  } else {
    const command = [
      "python - <<'PY'",
      "from pathlib import Path",
      `Path('frontend-output.txt').write_text('frontend run ${runId}\\n')`,
      "print('frontend evaluation smoke complete')",
      "PY",
    ].join("\n");
    launchMetadata.worker_commands = [{ command, cwd: "/workspace", model_call_id: "frontend-call-1" }];
    evaluators = [
      {
        evaluator_id: "mock-judge-v0",
        mode: "llm_judge",
        judge: {
          provider: "mock",
          model_name: "deterministic-judge",
          rubric_version: "frontend-e2e-v0",
        },
      },
    ];
  }
  return {
    run_id: runId,
    project_id: project.project_id,
    owner_team: project.owner_team_id || project.name,
    task: {
      benchmark_suite: benchmark.suite_name,
      benchmark_version: benchmark.benchmark_version,
      task_family: task.task_family,
      instance_id: task.instance_id,
      source_uri: benchmark.source_uri,
      input_artifact_refs: task.input_artifact_refs || [],
      required_artifacts: task.required_artifacts || ["trajectory", "workspace_snapshot", "evaluator_report"],
      metadata: { ...(task.metadata || {}), instruction },
    },
    model: {
      provider: model.provider,
      model_name: model.model_id,
      mode: "api",
      prompt_template_version: "terminal-agent-v0",
      provider_config_id: model.provider_config_id,
      metadata: {},
    },
    runner: {
      kind: harness.runner_kind,
      sandbox_backend: harness.sandbox_backend,
      image: task.runner_image || harness.default_image,
      entrypoint: task.runner_entrypoint || ["python", "-c"],
      internet_access: harness.internet_access,
      resource_limits: harness.resource_limits,
      metadata: {
        ...(harness.metadata || {}),
        runner_contract: task.runner_contract || harness.metadata?.runner_contract,
      },
    },
    evaluators,
    metadata: launchMetadata,
  };
}

function harborRunConfig({ harness, model, task, agent }) {
  const metadata = harness.metadata || {};
  const resourceLimits = harness.resource_limits || {};
  const taskHarborRun = objectValue(task?.metadata?.harbor_run);
  const config = taskHarborRun
    ? { ...taskHarborRun }
    : {
        task_template: nonEmptyString(metadata.harbor_task_template, "harbor-cli-smoke"),
        model_name: nonEmptyString(metadata.harbor_model_name, "smoke/noop"),
      };

  config.agent = harborAgentName(agent) || nonEmptyString(config.agent, nonEmptyString(metadata.harbor_agent, "oracle"));
  config.environment = nonEmptyString(config.environment, nonEmptyString(metadata.harbor_environment, "docker"));
  config.timeout_seconds = positiveInteger(config.timeout_seconds || metadata.harbor_timeout_seconds || resourceLimits.timeout_seconds, 600);
  config.extra_args = stringList(config.extra_args || metadata.harbor_extra_args, ["--n-tasks", "1", "--quiet"]);

  const requiredSecretRefs = stringList(agent?.required_secret_refs, []);
  if (requiredSecretRefs.length) {
    config.agent_required_secret_refs = requiredSecretRefs;
  }
  if (agent?.metadata?.harbor_agent_import_path) {
    config.agent_import_path = agent.metadata.harbor_agent_import_path;
  }
  if (harborAgentNeedsModel(config.agent)) {
    config.model_name = model.model_id;
  } else if (!config.model_name) {
    config.model_name = nonEmptyString(metadata.harbor_model_name, "smoke/noop");
  }
  return config;
}

function startPolling(runId) {
  clearInterval(state.pollTimer);
  state.pollTimer = setInterval(() => refreshRun(runId).catch(showRunError), 3000);
}

function startRunStream(runId) {
  closeRunStream();
  if (typeof EventSource !== "function") {
    return;
  }
  const source = new EventSource(runEventStreamUrl(runId, state.eventSeq));
  state.eventSource = source;
  for (const eventType of runEventTypes()) {
    source.addEventListener(eventType, (event) => onRunStreamEvent(runId, event));
  }
  source.onerror = () => {
    refreshRun(runId).catch(showRunError);
  };
}

function closeRunStream() {
  if (state.eventSource) {
    state.eventSource.close();
    state.eventSource = null;
  }
}

function onRunStreamEvent(runId, event) {
  try {
    const payload = JSON.parse(event.data);
    if (Number.isInteger(payload.seq) && payload.seq > state.eventSeq) {
      state.eventSeq = payload.seq;
    }
  } catch (error) {
    // Ignore malformed live events; polling remains the recovery path.
  }
  refreshRun(runId).catch(showRunError);
}

function runEventStreamUrl(runId, afterSeq) {
  const query = new URLSearchParams({ after_seq: String(afterSeq || 0) });
  return `/runs/${encodeURIComponent(runId)}/stream?${query.toString()}`;
}

function runEventTypes() {
  return [
    "run.created",
    "run.claimed",
    "run.started",
    "run.evaluating",
    "evaluator.completed",
    "evaluator.failed",
    "run.succeeded",
    "run.failed",
    "run.canceled",
    "run.retried",
    "run.worker_failed",
    "run.worker_subprocess_failed",
  ];
}

async function refreshRun(runId) {
  const [detail, telemetry] = await Promise.all([api(`/runs/${runId}`), api(`/runs/${runId}/telemetry`)]);
  state.eventSeq = Math.max(state.eventSeq, eventWatermarkFromDetail(detail));
  renderRun(detail, telemetry);
}

function eventWatermarkFromDetail(detail) {
  const events = Array.isArray(detail?.lifecycle_events) ? detail.lifecycle_events : [];
  return events.reduce((watermark, event) => {
    const seq = Number.isInteger(event?.seq) ? event.seq : 0;
    return Math.max(watermark, seq);
  }, 0);
}

function renderRun(detail, telemetry) {
  const run = detail.run;
  const terminal = ["succeeded", "failed", "canceled"].includes(run.status);
  setText("run-title", run.run_id);
  setText("run-status", run.status);
  setText("artifact-count", String(run.progress?.artifact_count ?? 0));
  setText("sandbox-state", telemetry.sandbox?.status || "n/a");
  setText("cpu-load", formatRatio(telemetry.host?.cpu?.load_per_cpu));
  setText("ram-used", formatPercent(telemetry.host?.memory?.used_ratio));
  setText("queue-depth", String(telemetry.worker?.queue_depth ?? 0));
  setText("evaluator-feedback", run.evaluator?.verbal_feedback_summary || "No evaluator output yet.");
  el("download-button").disabled = !terminal || (run.progress?.artifact_count ?? 0) === 0;

  const lifecycle = detail.lifecycle_events || [];
  el("lifecycle-list").innerHTML = lifecycle.length
    ? lifecycle.map((event) => `<li><strong>${escapeHtml(event.to_status)}</strong> ${escapeHtml(event.event_type)}</li>`).join("")
    : "<li>Created locally in the browser session.</li>";

  const trajectory = detail.trajectory || [];
  setText(
    "trajectory-log",
    trajectory.length
      ? trajectory
          .map((turn) => `$ ${turn.command}\nexit=${turn.exit_code}\n${turn.stdout || ""}${turn.stderr || ""}`)
          .join("\n\n")
      : "No trajectory yet.",
  );
}

function showRunError(error) {
  setText("launch-error", error.message);
}

function downloadBundle() {
  if (!state.selectedRunId) {
    return;
  }
  window.location.assign(`/runs/${state.selectedRunId}/artifact-bundle`);
}

async function api(path, options = {}) {
  const response = await fetch(path, {
    credentials: "same-origin",
    headers: { "Content-Type": "application/json", ...(options.headers || {}) },
    ...options,
  });
  if (!response.ok) {
    let message = `HTTP ${response.status}`;
    try {
      const payload = await response.json();
      message = payload.error?.message || payload.detail || message;
    } catch (error) {
      message = response.statusText || message;
    }
    throw new Error(message);
  }
  return response.json();
}

function fillSelect(select, items, valueFn, labelFn) {
  select.innerHTML = "";
  for (const item of items) {
    const option = document.createElement("option");
    option.value = valueFn(item);
    option.textContent = labelFn(item);
    select.appendChild(option);
  }
}

function selectedProject() {
  return state.projects.find((project) => project.project_id === el("project-select").value);
}

function selectedModel() {
  return state.models.find((model) => model.model_id === el("model-select").value);
}

function selectedHarness() {
  return state.harnesses.find((harness) => harness.harness_id === el("harness-select").value);
}

function selectedAgent() {
  return state.agents.find((agent) => agent.agent_id === el("agent-select").value);
}

function selectedBenchmark() {
  const value = el("benchmark-select").value;
  return state.benchmarks.find((benchmark) => `${benchmark.suite_name}::${benchmark.benchmark_version}` === value);
}

function selectedTask() {
  const value = el("task-select").value;
  return state.tasks.find((task) => `${task.task_family}::${task.instance_id}` === value);
}

function modelCatalogMessage(payload) {
  const status = payload?.catalog?.status || "";
  const firstError = Array.isArray(payload?.errors) && payload.errors.length
    ? String(payload.errors[0].message || "unknown error")
    : "";
  if (status === "discovered") {
    return "Models discovered from provider /models.";
  }
  if (status === "discovered_allowlisted") {
    return "Models discovered from provider /models and filtered by allowlist.";
  }
  if (status === "fallback_static_config") {
    return `Using static model fallback; provider discovery failed: ${firstError || "unknown error"}`;
  }
  if (status === "discovery_failed") {
    return `Model discovery failed: ${firstError || "unknown error"}`;
  }
  if (status === "static_config") {
    return "Using static model list from configuration.";
  }
  if (status === "dev_fallback") {
    return "Using local scripted model fallback.";
  }
  if (firstError) {
    return `Model catalog warning: ${firstError}`;
  }
  return "";
}

function adaptationMessage(payload) {
  if (!payload) {
    return "";
  }
  if (payload.status === "ready") {
    if (!payload.adapter) {
      return "Adapter ready: no model key required.";
    }
    const adapterName = payload.adapter.display_name || payload.adapter.adapter_id || "agent adapter";
    return `Adapter ready: ${adapterName}.`;
  }
  const firstGap = Array.isArray(payload.gaps) && payload.gaps.length
    ? String(payload.gaps[0].message || payload.gaps[0].code || "unknown gap")
    : "unknown gap";
  return `Adapter blocked: ${firstGap}`;
}

function setText(id, value) {
  el(id).textContent = value;
}

function formatRatio(value) {
  return typeof value === "number" ? value.toFixed(2) : "n/a";
}

function formatPercent(value) {
  return typeof value === "number" ? `${Math.round(value * 100)}%` : "n/a";
}

function nonEmptyString(value, fallback) {
  return typeof value === "string" && value.trim() ? value.trim() : fallback;
}

function positiveInteger(value, fallback) {
  return Number.isInteger(value) && value > 0 ? value : fallback;
}

function stringList(value, fallback) {
  return Array.isArray(value) && value.every((item) => typeof item === "string" && item.trim())
    ? value.map((item) => item.trim())
    : [...fallback];
}

function objectValue(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : null;
}

function harborAgentName(agent) {
  if (typeof agent?.metadata?.harbor_agent_name === "string" && agent.metadata.harbor_agent_name.trim()) {
    return agent.metadata.harbor_agent_name.trim();
  }
  if (typeof agent?.agent_id === "string" && agent.agent_id.startsWith("harbor:")) {
    return agent.agent_id.slice("harbor:".length);
  }
  return "";
}

function harborAgentNeedsModel(agentName) {
  return !["oracle", "nop"].includes(nonEmptyString(agentName, "oracle"));
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}
