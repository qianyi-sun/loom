/**
 * Submit-trial confirm dialog. Posts a fresh trial against the
 * registered task using its built-in agent + verifier defaults.
 *
 * Per-trial agent/model/backend overrides are NOT exposed here:
 * the server-side `TrialConfig` schema (src/loom/models/trial.py) is
 * declared with `extra="forbid"` and accepts only retry/timeout/skip
 * knobs — the agent and model come from the task's registered
 * `TaskConfig`. For richer control (override agent, swap model,
 * compare N models), point users at `loom run` on the CLI; the SPA
 * Submit-trial action stays simple and end-to-end correct.
 *
 * Tracked separately in PR C (Workflows): saved global recipes that
 * pin agent + model + backend up front, with `launch` ↦ Campaign.
 */
import { useState } from "react";
import { useNavigate } from "react-router-dom";

import { api } from "../api/client";
import { Button } from "./Button";
import ErrorState from "./ErrorState";
import { Modal } from "./Modal";

export interface SubmitTrialModalProps {
  taskId: string;
  open: boolean;
  onClose: () => void;
}

export function SubmitTrialModal({
  taskId,
  open,
  onClose,
}: SubmitTrialModalProps): JSX.Element {
  const navigate = useNavigate();
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<unknown>(null);

  const handleSubmit = async (): Promise<void> => {
    setError(null);
    setSubmitting(true);
    try {
      // Plan 23: TrialConfig requires agent_name + agent_model. This
      // modal currently submits the "run with task's oracle agent and
      // no LLM" preset; the upcoming PR F replaces the modal with a
      // picker that lets the user choose both. Until then, the literal
      // "oracle" + null model matches the canary hello-world task.
      const result = await api.submitTrial({
        task_id: taskId,
        config: { agent_name: "oracle", agent_model: null },
      });
      onClose();
      navigate(`/trials/${result.trial_id}`);
    } catch (e) {
      setError(e);
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <Modal
      open={open}
      onClose={() => {
        setError(null);
        onClose();
      }}
      title="Submit trial"
      description="Run this task once against its registered agent + verifier."
      size="sm"
      footer={
        <>
          <Button
            variant="secondary"
            onClick={() => {
              setError(null);
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
      <div className="space-y-3">
        {error ? <ErrorState error={error} /> : null}
        <p className="text-sm text-slate-600">
          A new trial will be queued for{" "}
          <code className="rounded bg-slate-100 px-1.5 py-0.5 font-mono text-xs text-slate-700">
            {taskId}
          </code>{" "}
          using the task's registered configuration. You'll be redirected
          to its detail page on submit.
        </p>
        <p className="text-xs text-slate-500">
          For richer control — pick a different agent, swap the model,
          run with `--parallel-models` — use{" "}
          <code className="rounded bg-slate-100 px-1 py-0.5 font-mono text-xs">
            loom run
          </code>{" "}
          on the CLI. Per-trial overrides via the SPA are coming with
          the Workflows feature.
        </p>
      </div>
    </Modal>
  );
}
