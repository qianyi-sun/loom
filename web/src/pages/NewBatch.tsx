import { useState } from "react";
import { AgentModelPicker } from "../components/AgentModelPicker";
import { Button } from "../components/Button";
import { Card } from "../components/Card";
import ErrorState from "../components/ErrorState";
import { Input } from "../components/Input";
import { agentLabel } from "../lib/agentLabel";
import { BatchExportDialog, type BatchExportResult } from "./newBatch/BatchExportDialog";
import { clampInt } from "./newBatch/advancedConfig";
import { DEFAULT_AGENT_NAME } from "./newBatch/formState";
import { FieldLabel } from "./NewBatchFields";
import { MAX_COMBINATIONS } from "./newBatchState";

import { NewBatchAdvancedSettings } from "./NewBatchAdvancedSettings";
import { NewBatchTaskSelection } from "./NewBatchTaskSelection";
import { useNewBatch } from "./useNewBatch";
export default function NewBatch(): JSX.Element {
  const state = useNewBatch();
  const [exportResult, setExportResult] = useState<BatchExportResult | null>(null);
  const exportConfiguration = () => {
    const result = state.buildSubmission();
    if (!result.ok) {
      setExportResult({ error: result.error });
    } else if (result.providerOverrides.some((override) => override.manual_model)) {
      setExportResult({ error: "Save manually entered models in Providers first, then select the saved model before exporting. Export does not save models or change provider connections." });
    } else {
      setExportResult({ payload: result.payload });
    }
  };
  const {
    matchedTaskCount,
    addRow,
    rows,
    removeRow,
    updateRow,
    create,
    currentTeamId,
    releaseScopeText,
    releaseTrialText,
    releaseBackendText,
    releaseProviderText,
    releaseModelText,
    budgetUsd,
    setBudgetUsd,
    budgetPolicy,
    setBudgetPolicy,
    setBudgetConfirmed,
    budgetConfirmed,
    nameSuffix,
    setNameSuffix,
    generatedIdentity,
    totalTrials,
    sumNPerTask,
    agents,
    showLargeFanOutConfirm,
    confirmedLargeFanOut,
    setConfirmedLargeFanOut,
    localError,
    submit,
    tagSelectionPending,
    submitButtonLabel,
  } = state;
  return (
    <div className="space-y-6">
      <header>
        <h1 className="text-2xl font-bold text-slate-900">New batch</h1>
        <p className="mt-1 text-sm text-slate-500">
          Choose the tasks, choose one or more agent/model combinations, then review how many trials Loom will
          launch.
        </p>
      </header>

      {exportResult ? <BatchExportDialog result={exportResult} onClose={() => setExportResult(null)} /> : null}

      <div className="grid grid-cols-1 gap-6 xl:grid-cols-[minmax(0,1.3fr)_minmax(0,1fr)]">
        {/* LEFT column */}
        <div className="min-w-0 space-y-6">
          <NewBatchTaskSelection {...state} />

          <NewBatchAdvancedSettings {...state} />
        </div>

        {/* RIGHT column */}
        <div className="space-y-6">
          <div className="rounded-2xl bg-slate-50 p-5">
            <div className="mb-4 flex items-center justify-between">
              <div>
                <h2 className="text-sm font-semibold text-slate-800">Agent/model combinations</h2>
                <p className="mt-0.5 text-xs text-slate-500">
                  Each row runs the selected tasks with its own agent, model, and samples-per-task count.
                </p>
              </div>
              <Button
                variant="secondary"
                size="sm"
                onClick={addRow}
                disabled={rows.length >= MAX_COMBINATIONS}
                title="Add another agent/model combination to run on the same task slate."
              >
                + Add combination
              </Button>
            </div>
            <div className="space-y-4">
              {rows.map((r, i) => (
                <div key={i} className="space-y-3 rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
                  <div className="flex items-center justify-between">
                    <span className="text-xs font-semibold uppercase tracking-wider text-slate-500">
                      Combination {i + 1}
                    </span>
                    {rows.length > 1 ? (
                      <Button
                        variant="secondary"
                        size="sm"
                        onClick={() => removeRow(i)}
                        title={`Remove combination ${i + 1} from this batch.`}
                      >
                        Remove
                      </Button>
                    ) : null}
                  </div>
                  <AgentModelPicker
                    value={r.picker}
                    onChange={(v) => updateRow(i, { picker: v })}
                    disabled={create.isPending}
                    specificAgentToggle
                    defaultAgentName={DEFAULT_AGENT_NAME}
                    teamId={currentTeamId}
                    allowAgentVersion
                  />
                  <div className="grid grid-cols-2 gap-3">
                    <label className="block">
                      <FieldLabel hint="1 – 100">Samples per task</FieldLabel>
                      <Input
                        type="number"
                        min={1}
                        max={100}
                        step={1}
                        value={r.nPerTask}
                        onChange={(e) =>
                          updateRow(i, {
                            nPerTask: clampInt(e.target.value, 1, 100),
                          })
                        }
                        onBlur={() =>
                          updateRow(i, {
                            nPerTask: clampInt(r.nPerTask, 1, 100) || "1",
                          })
                        }
                        aria-label={`Samples per task (combination ${i + 1})`}
                      />
                    </label>
                    <label className="block">
                      <FieldLabel hint="optional">Label</FieldLabel>
                      <Input
                        value={r.label}
                        onChange={(e) => updateRow(i, { label: e.target.value })}
                        placeholder="auto"
                        aria-label={`Label (combination ${i + 1})`}
                      />
                    </label>
                  </div>
                </div>
              ))}
            </div>
            {rows.length >= MAX_COMBINATIONS ? (
              <p className="mt-3 text-xs text-amber-700">Cap is {MAX_COMBINATIONS} combinations per batch.</p>
            ) : null}
          </div>
          <Card>
            <Card.Header
              title="Release review"
              description="Check the planned task slate and execution readiness before submitting."
              actions={<Button variant="secondary" size="sm" onClick={exportConfiguration}>Export CLI / API</Button>}
            />
            <Card.Body className="space-y-2 text-sm text-slate-700">
              <p>{releaseScopeText}</p>
              <p>{releaseTrialText}</p>
              <p>{releaseBackendText}</p>
              <p>{releaseProviderText}</p>
              <p>{releaseModelText}</p>
            </Card.Body>
          </Card>
          <Card>
            <Card.Header title="Batch budget" description="Set a provider spend limit for this batch." />
            <Card.Body className="space-y-3">
              <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
                <label className="block">
                  <FieldLabel hint="optional">Budget USD</FieldLabel>
                  <Input
                    type="number"
                    min={0}
                    step="0.01"
                    value={budgetUsd}
                    onChange={(e) => setBudgetUsd(e.target.value)}
                    aria-label="Budget USD"
                    placeholder="0.00"
                  />
                </label>
                <label className="block">
                  <FieldLabel>Budget policy</FieldLabel>
                  <select
                    value={budgetPolicy}
                    onChange={(e) => {
                      setBudgetPolicy(e.target.value as "none" | "soft" | "hard");
                      setBudgetConfirmed(false);
                    }}
                    aria-label="Budget policy"
                    className="block w-full rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm text-slate-800"
                  >
                    <option value="none">None</option>
                    <option value="hard">Hard stop</option>
                    <option value="soft">Soft confirm</option>
                  </select>
                </label>
              </div>
              {budgetUsd.trim() && budgetPolicy === "soft" ? (
                <label className="flex items-start gap-2 rounded-lg border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-800">
                  <input
                    type="checkbox"
                    checked={budgetConfirmed}
                    onChange={(e) => setBudgetConfirmed(e.target.checked)}
                    className="mt-0.5 h-4 w-4 rounded border-amber-300"
                  />
                  <span>I understand this soft budget may require confirmation.</span>
                </label>
              ) : null}
            </Card.Body>
          </Card>
          <Card>
            <Card.Header
              title="Generated identity"
              description="The API stores this name and description when the batch is submitted."
            />
            <Card.Body className="space-y-4">
              <label className="block">
                <FieldLabel hint="optional">Name suffix</FieldLabel>
                <Input
                  value={nameSuffix}
                  onChange={(e) => setNameSuffix(e.target.value)}
                  placeholder="canary, paper-table-1, rerun"
                  aria-label="Name suffix"
                  maxLength={80}
                />
              </label>
              <div>
                <FieldLabel>Generated name preview</FieldLabel>
                <p className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-2 text-sm font-medium text-slate-900">
                  {generatedIdentity.name}
                </p>
              </div>
              <div>
                <FieldLabel>Generated description preview</FieldLabel>
                <p className="rounded-lg border border-slate-200 bg-white px-3 py-2 text-xs leading-relaxed text-slate-600">
                  {generatedIdentity.description}
                </p>
              </div>
            </Card.Body>
          </Card>
        </div>
      </div>

      {/* Confirmation banner — hidden when totalTrials is 0 since the
          inline countSummary above already explains why (no tasks
          registered / no parsed ids), and a "Will launch 0 trials"
          banner just adds noise to an already-actionable empty state. */}
      {totalTrials !== undefined && totalTrials > 0 ? (
        <div className="rounded-xl border border-slate-200 bg-white px-4 py-3 text-sm">
          <p className="text-slate-700">
            Will launch{" "}
            <span className="font-semibold text-slate-900">
              {totalTrials} trial{totalTrials === 1 ? "" : "s"}
            </span>{" "}
            (matched {matchedTaskCount ?? 0} task
            {matchedTaskCount === 1 ? "" : "s"} × Σ n_per_task = {sumNPerTask}).
          </p>
          <div className="mt-2 flex flex-wrap gap-2">
            {rows.map((r, i) => {
              const sel = agents.data?.items.find((a) => a.name === r.picker.agentName);
              const modelTxt =
                sel && !sel.needs_model
                  ? "(no model)"
                  : r.picker.modelProvider && r.picker.modelName
                    ? `${r.picker.modelProvider}/${r.picker.modelName}`
                    : "(no model)";
              const lbl = r.label.trim() || `combo${i + 1}`;
              return (
                <span
                  key={i}
                  className="inline-flex items-center gap-1 rounded-md border border-slate-200 bg-slate-50 px-2 py-1 text-xs text-slate-700"
                >
                  <span className="font-semibold">{lbl}</span>
                  <span className="text-slate-500">
                    {agentLabel(sel?.name, r.picker.agentVersion)} · {modelTxt} · n={r.nPerTask}
                  </span>
                </span>
              );
            })}
          </div>
        </div>
      ) : null}

      {showLargeFanOutConfirm ? (
        <label className="flex items-start gap-2 rounded-lg border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-800">
          <input
            type="checkbox"
            checked={confirmedLargeFanOut}
            onChange={(e) => setConfirmedLargeFanOut(e.target.checked)}
            className="mt-0.5 h-4 w-4 rounded border-amber-300"
          />
          <span>I understand this batch will launch {totalTrials} trials.</span>
        </label>
      ) : null}

      {localError ? (
        <div className="rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
          {localError}
        </div>
      ) : null}
      {create.isError ? <ErrorState error={create.error} /> : null}

      <div className="flex items-center justify-end">
        <Button
          variant="primary"
          onClick={() => {
            void submit();
          }}
          disabled={create.isPending || totalTrials === 0 || tagSelectionPending}
          title={
            totalTrials === 0
              ? "Pick benchmarks and a valid agent/model combination before submitting."
              : `Create this batch with ${totalTrials} planned trial${totalTrials === 1 ? "" : "s"}.`
          }
        >
          {submitButtonLabel}
        </Button>
      </div>
    </div>
  );
}
