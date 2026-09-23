import { queryKeys } from "../api/queryKeys";
import { useQuery } from "@tanstack/react-query";
import { useSearchParams } from "react-router-dom";
import { api } from "../api";
import { useAuth } from "../auth/useAuth";
import { Input } from "../components/Input";
import { BatchesView } from "./MonitorBatches";
import { SegmentedToggle } from "./MonitorControls";
import { MonitorHealthSummary } from "./MonitorHealth";
import { BATCH_STATE_OPTIONS, stateOptionLabel, TRIAL_STATE_OPTIONS, type View } from "./monitorPresentation";
import { TrialsView } from "./MonitorTrials";

export default function Monitor(): JSX.Element {
  const [searchParams, setSearchParams] = useSearchParams();
  const auth = useAuth();
  const view: View = searchParams.get("view") === "trials" ? "trials" : "batches";
  const batchIdFilter = searchParams.get("batch_id") ?? undefined;
  const search = searchParams.get("q") ?? "";
  const stateFilter = searchParams.get("state") ?? "";
  const teamFilter = searchParams.get("team_id") ?? "";
  const benchmarkFilter = searchParams.get("benchmark_id") ?? "";
  const agentFilter = searchParams.get("agent_name") ?? searchParams.get("agent") ?? "";
  const modelProviderFilter = searchParams.get("model_provider") ?? "";
  const modelNameFilter = searchParams.get("model_name") ?? searchParams.get("model") ?? "";
  const providerConnectionFilter = searchParams.get("provider_connection_id") ?? "";
  const providerModelFilter = searchParams.get("provider_model_id") ?? "";

  const teamsQuery = useQuery({
    queryKey: queryKeys["admin-teams"](auth.isAdmin),
    queryFn: () => api.listAdminTeams(),
    enabled: auth.isAdmin,
  });
  const adminTeams = teamsQuery.data?.items ?? [];
  const selectedTeamKnown = adminTeams.some((team) => team.id === teamFilter);

  const updateParam = (key: string, value: string): void => {
    const next = new URLSearchParams(searchParams);
    if (value) next.set(key, value);
    else next.delete(key);
    setSearchParams(next);
  };
  const updateParamWithAliases = (key: string, value: string, aliases: string[] = []): void => {
    const next = new URLSearchParams(searchParams);
    for (const alias of aliases) next.delete(alias);
    if (value) next.set(key, value);
    else next.delete(key);
    setSearchParams(next);
  };

  const setView = (v: View): void => {
    const next = new URLSearchParams(searchParams);
    next.set("view", v);
    setSearchParams(next);
  };

  const stateOptions = view === "batches" ? BATCH_STATE_OPTIONS : TRIAL_STATE_OPTIONS;

  return (
    <div className="space-y-6">
      <header className="flex flex-wrap items-end justify-between gap-4">
        <div>
          <h1 className="text-2xl font-bold text-slate-900">Monitor</h1>
          <p className="mt-1 text-sm text-slate-500">Live-updating view of your batches and trials.</p>
        </div>
        <SegmentedToggle value={view} onChange={setView} />
      </header>

      <div className="flex flex-wrap items-center gap-3">
        <Input
          value={search}
          onChange={(e) => updateParam("q", e.target.value)}
          placeholder={
            view === "batches" ? "Search batches by name or ID..." : "Search trials by task ID or trial ID..."
          }
          className="max-w-sm"
          aria-label="search"
          title={
            view === "batches"
              ? "Filter batches by human name or batch ID."
              : "Filter trials by task ID or trial ID."
          }
        />
        <label className="flex items-center gap-2 text-sm text-slate-600">
          <span className="text-xs uppercase tracking-wider text-slate-600">State</span>
          <select
            value={stateFilter}
            onChange={(e) => updateParam("state", e.target.value)}
            className="rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm text-slate-700"
            aria-label="filter by state"
            title="Limit the table to one lifecycle state, or choose all."
          >
            <option value="">All states</option>
            {stateOptions.map((s) => (
              <option key={s} value={s}>
                {stateOptionLabel(s)}
              </option>
            ))}
          </select>
        </label>
        {auth.isAdmin ? (
          <label className="flex items-center gap-2 text-sm text-slate-600">
            <span className="text-xs uppercase tracking-wider text-slate-600">Team</span>
            <select
              value={teamFilter}
              onChange={(e) => updateParam("team_id", e.target.value)}
              className="rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm text-slate-700"
              aria-label="filter by team"
              title="Limit results to one internal team. Platform admins can inspect all teams."
            >
              <option value="">All teams</option>
              {teamFilter && !selectedTeamKnown ? <option value={teamFilter}>{teamFilter}</option> : null}
              {adminTeams.map((team) => (
                <option key={team.id} value={team.id}>
                  {team.name}
                </option>
              ))}
            </select>
          </label>
        ) : null}
        <Input
          value={benchmarkFilter}
          onChange={(e) => updateParam("benchmark_id", e.target.value)}
          placeholder="Benchmark"
          className="max-w-[10rem]"
          aria-label="filter by benchmark"
          title="Filter by benchmark id when the API can resolve it."
        />
        <Input
          value={agentFilter}
          onChange={(e) => updateParamWithAliases("agent_name", e.target.value, ["agent"])}
          placeholder="Agent"
          className="max-w-[10rem]"
          aria-label="filter by agent name"
          title="Filter by agent adapter name."
        />
        <Input
          value={modelProviderFilter}
          onChange={(e) => updateParam("model_provider", e.target.value)}
          placeholder="Provider"
          className="max-w-[10rem]"
          aria-label="filter by model provider"
          title="Filter by model provider namespace."
        />
        <Input
          value={modelNameFilter}
          onChange={(e) => updateParamWithAliases("model_name", e.target.value, ["model"])}
          placeholder="Model name"
          className="max-w-[10rem]"
          aria-label="filter by model name"
          title="Filter by model id or model name."
        />
        <Input
          value={providerConnectionFilter}
          onChange={(e) => updateParam("provider_connection_id", e.target.value)}
          placeholder="Provider conn."
          className="max-w-[12rem]"
          aria-label="filter by provider connection"
          title="Filter by provider connection ID."
        />
        <Input
          value={providerModelFilter}
          onChange={(e) => updateParam("provider_model_id", e.target.value)}
          placeholder="Provider model"
          className="max-w-[12rem]"
          aria-label="filter by provider model"
          title="Filter by provider model ID."
        />
        {view === "trials" && batchIdFilter ? (
          <span className="rounded-md border border-slate-200 bg-slate-50 px-2 py-1 text-xs text-slate-700">
            batch_id = <code className="font-mono">{batchIdFilter.slice(0, 8)}</code>
            <button
              type="button"
              className="ml-2 text-slate-500 hover:text-slate-900"
              title="Remove this batch_id filter and show trials from all batches."
              onClick={() => {
                const next = new URLSearchParams(searchParams);
                next.delete("batch_id");
                setSearchParams(next);
              }}
            >
              clear
            </button>
          </span>
        ) : null}
      </div>

      <MonitorHealthSummary
        view={view}
        search={search}
        stateFilter={stateFilter}
        teamFilter={teamFilter}
        benchmarkFilter={benchmarkFilter}
        agentFilter={agentFilter}
        modelProviderFilter={modelProviderFilter}
        modelNameFilter={modelNameFilter}
        providerConnectionFilter={providerConnectionFilter}
        providerModelFilter={providerModelFilter}
        batchId={batchIdFilter}
      />

      {view === "batches" ? (
        <BatchesView
          search={search}
          stateFilter={stateFilter}
          teamFilter={teamFilter}
          benchmarkFilter={benchmarkFilter}
          agentFilter={agentFilter}
          modelProviderFilter={modelProviderFilter}
          modelNameFilter={modelNameFilter}
          providerConnectionFilter={providerConnectionFilter}
          providerModelFilter={providerModelFilter}
        />
      ) : (
        <TrialsView
          search={search}
          stateFilter={stateFilter}
          batchId={batchIdFilter}
          teamFilter={teamFilter}
          benchmarkFilter={benchmarkFilter}
          agentFilter={agentFilter}
          modelProviderFilter={modelProviderFilter}
          modelNameFilter={modelNameFilter}
          providerConnectionFilter={providerConnectionFilter}
          providerModelFilter={providerModelFilter}
        />
      )}
    </div>
  );
}
