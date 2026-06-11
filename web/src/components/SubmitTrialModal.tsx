/**
 * Submit-trial dialog. Plan 23: TrialConfig requires explicit
 * `agent_name` + `agent_model` — no fallback to the task's
 * TaskConfig. The dialog collects both up front:
 *
 *   - Agent (required text, default "oracle")
 *   - Model provider + name (optional pair) — leave both blank to
 *     send `agent_model: null` (correct for agents that don't call
 *     an LLM, like oracle or in-box runtimes)
 *
 * Body shape sent to /api/v1/trials:
 *   { task_id, config: { agent_name, agent_model: {provider,name} | null } }
 *
 * For richer multi-trial flows (n-sampling, model comparison), use
 * a Workflow or campaign — this modal stays focused on the
 * single-trial path.
 */
import { useState } from "react";
import { useNavigate } from "react-router-dom";

import { api } from "../api/client";
import { Button } from "./Button";
import ErrorState from "./ErrorState";
import { Input } from "./Input";
import { Modal } from "./Modal";

export interface SubmitTrialModalProps {
  taskId: string;
  open: boolean;
  onClose: () => void;
}

function buildAgentModel(
  provider: string,
  name: string,
): { ok: true; value: { provider: string; name: string } | null }
  | { ok: false; error: string } {
  const p = provider.trim();
  const n = name.trim();
  if (!p && !n) return { ok: true, value: null };
  if (!p || !n) {
    return {
      ok: false,
      error:
        "Model provider and model name must both be set, or both left blank.",
    };
  }
  return { ok: true, value: { provider: p, name: n } };
}

export function SubmitTrialModal({
  taskId,
  open,
  onClose,
}: SubmitTrialModalProps): JSX.Element {
  const navigate = useNavigate();
  const [agentName, setAgentName] = useState("oracle");
  const [modelProvider, setModelProvider] = useState("");
  const [modelName, setModelName] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<unknown>(null);

  const handleSubmit = async (): Promise<void> => {
    setError(null);
    const trimmedAgent = agentName.trim();
    if (!trimmedAgent) {
      setError(new Error("Agent is required."));
      return;
    }
    const agentModel = buildAgentModel(modelProvider, modelName);
    if (!agentModel.ok) {
      setError(new Error(agentModel.error));
      return;
    }
    setSubmitting(true);
    try {
      const result = await api.submitTrial({
        task_id: taskId,
        config: {
          agent_name: trimmedAgent,
          agent_model: agentModel.value,
        },
      });
      onClose();
      navigate(`/trials/${result.trial_id}`);
    } catch (e) {
      setError(e);
    } finally {
      setSubmitting(false);
    }
  };

  const reset = (): void => {
    setError(null);
    setAgentName("oracle");
    setModelProvider("");
    setModelName("");
  };

  return (
    <Modal
      open={open}
      onClose={() => {
        reset();
        onClose();
      }}
      title="Submit trial"
      description="Run this task once against the agent + model you pick."
      size="sm"
      footer={
        <>
          <Button
            variant="secondary"
            onClick={() => {
              reset();
              onClose();
            }}
            disabled={submitting}
          >
            Cancel
          </Button>
          <Button
            variant="primary"
            onClick={handleSubmit}
            disabled={submitting}
          >
            {submitting ? "Submitting…" : "Submit trial"}
          </Button>
        </>
      }
    >
      <div className="space-y-4">
        {error ? <ErrorState error={error} /> : null}
        <p className="text-sm text-slate-600">
          A new trial will be queued for{" "}
          <code className="rounded bg-slate-100 px-1.5 py-0.5 font-mono text-xs text-slate-700">
            {taskId}
          </code>
          .
        </p>
        <label className="block">
          <span className="mb-1 block text-xs font-medium uppercase tracking-wider text-slate-500">
            Agent
          </span>
          <Input
            value={agentName}
            onChange={(e) => setAgentName(e.target.value)}
            placeholder="oracle"
          />
        </label>
        <div className="grid grid-cols-2 gap-3">
          <label className="block">
            <span className="mb-1 block text-xs font-medium uppercase tracking-wider text-slate-500">
              Model provider
            </span>
            <Input
              value={modelProvider}
              onChange={(e) => setModelProvider(e.target.value)}
              placeholder="anthropic"
            />
          </label>
          <label className="block">
            <span className="mb-1 block text-xs font-medium uppercase tracking-wider text-slate-500">
              Model name
            </span>
            <Input
              value={modelName}
              onChange={(e) => setModelName(e.target.value)}
              placeholder="claude-opus-4-7"
            />
          </label>
        </div>
        <p className="text-xs text-slate-500">
          Leave both model fields blank for agents that don't call an LLM
          (oracle, in-box runtimes).
        </p>
      </div>
    </Modal>
  );
}
