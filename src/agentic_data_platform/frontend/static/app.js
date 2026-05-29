const state = {
  user: null,
  projects: [],
  models: [],
  harnesses: [],
  benchmarks: [],
  tasks: [],
  selectedRunId: null,
  pollTimer: null,
};

const el = (id) => document.getElementById(id);

window.addEventListener("DOMContentLoaded", () => {
  el("login-form").addEventListener("submit", onLogin);
  el("logout-button").addEventListener("click", onLogout);
  el("refresh-button").addEventListener("click", refreshAll);
  el("launch-button").addEventListener("click", launchRun);
  el("download-button").addEventListener("click", downloadBundle);
  el("project-select").addEventListener("change", refreshDashboard);
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
  state.user = null;
  state.selectedRunId = null;
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

async function loadBenchmarks() {
  const payload = await api("/benchmarks");
  state.benchmarks = payload.benchmarks || [];
  fillSelect(
    el("benchmark-select"),
    state.benchmarks,
    (benchmark) => `${benchmark.suite_name}::${benchmark.benchmark_version}`,
    (benchmark) => `${benchmark.suite_name}`,
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
  const benchmark = selectedBenchmark();
  const task = selectedTask();
  if (!project || !model || !harness || !benchmark || !task) {
    setText("launch-error", "Select a project, model, harness, benchmark, and task.");
    return;
  }
  const runId = `frontend_${Date.now()}`;
  const payload = buildRunPayload({ runId, project, model, harness, benchmark, task });
  try {
    const created = await api("/runs", {
      method: "POST",
      body: JSON.stringify(payload),
    });
    state.selectedRunId = created.run.run_id;
    el("download-button").disabled = true;
    await refreshRun(state.selectedRunId);
    startPolling(state.selectedRunId);
  } catch (error) {
    setText("launch-error", error.message);
  }
}

function buildRunPayload({ runId, project, model, harness, benchmark, task }) {
  const instruction =
    task.metadata?.instruction ||
    `Follow ${benchmark.suite_name} task ${task.task_family}/${task.instance_id} from ${task.instruction_ref}.`;
  const launchMetadata = {
    launched_from: "frontend",
    harness_id: harness.harness_id,
  };
  let evaluators;
  if (harness.metadata?.harbor_compatible) {
    launchMetadata.harbor_run = harborRunConfig(harness);
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

function harborRunConfig(harness) {
  const metadata = harness.metadata || {};
  const resourceLimits = harness.resource_limits || {};
  const config = {
    task_template: nonEmptyString(metadata.harbor_task_template, "harbor-cli-smoke"),
    agent: nonEmptyString(metadata.harbor_agent, "oracle"),
    environment: nonEmptyString(metadata.harbor_environment, "docker"),
    timeout_seconds: positiveInteger(metadata.harbor_timeout_seconds || resourceLimits.timeout_seconds, 600),
    extra_args: stringList(metadata.harbor_extra_args, ["--n-tasks", "1", "--quiet"]),
  };
  if (typeof metadata.harbor_model_name === "string" && metadata.harbor_model_name.trim()) {
    config.model_name = metadata.harbor_model_name.trim();
  }
  return config;
}

function startPolling(runId) {
  clearInterval(state.pollTimer);
  state.pollTimer = setInterval(() => refreshRun(runId).catch(showRunError), 3000);
}

async function refreshRun(runId) {
  const [detail, telemetry] = await Promise.all([api(`/runs/${runId}`), api(`/runs/${runId}/telemetry`)]);
  renderRun(detail, telemetry);
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

function selectedBenchmark() {
  const value = el("benchmark-select").value;
  return state.benchmarks.find((benchmark) => `${benchmark.suite_name}::${benchmark.benchmark_version}` === value);
}

function selectedTask() {
  const value = el("task-select").value;
  return state.tasks.find((task) => `${task.task_family}::${task.instance_id}` === value);
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

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}
