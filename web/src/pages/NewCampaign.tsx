/**
 * Form to create a new campaign. task_filter + trial_config are JSON
 * text areas (the surface is operator-facing and the schema is
 * intentionally flexible); we parse + validate before posting so the
 * caller can fix typos without a server round-trip.
 */

import { useMutation } from "@tanstack/react-query";
import { useState } from "react";
import { useNavigate } from "react-router-dom";

import { api } from "../api/client";
import ErrorState from "../components/ErrorState";

const DEFAULT_FILTER = `{
  "license": "MIT"
}`;
const DEFAULT_TRIAL_CONFIG = `{
  "agent": { "name": "oracle" }
}`;

function tryParse(input: string): {
  ok: true;
  value: Record<string, unknown>;
} | { ok: false; error: string } {
  try {
    const parsed = JSON.parse(input);
    if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
      return { ok: false, error: "expected a JSON object" };
    }
    return { ok: true, value: parsed as Record<string, unknown> };
  } catch (e) {
    return {
      ok: false,
      error: e instanceof Error ? e.message : String(e),
    };
  }
}

export default function NewCampaign(): JSX.Element {
  const [name, setName] = useState("");
  const [description, setDescription] = useState("");
  const [filterText, setFilterText] = useState(DEFAULT_FILTER);
  const [configText, setConfigText] = useState(DEFAULT_TRIAL_CONFIG);
  const [localError, setLocalError] = useState<string | null>(null);

  const navigate = useNavigate();
  const create = useMutation({
    mutationFn: (body: {
      name: string;
      description?: string;
      task_filter: Record<string, unknown>;
      trial_config: Record<string, unknown>;
    }) => api.createCampaign(body),
    onSuccess: (res) => {
      navigate(`/campaigns/${res.campaign_id}`);
    },
  });

  const submit = (): void => {
    setLocalError(null);
    const filter = tryParse(filterText);
    if (!filter.ok) {
      setLocalError(`task_filter: ${filter.error}`);
      return;
    }
    const config = tryParse(configText);
    if (!config.ok) {
      setLocalError(`trial_config: ${config.error}`);
      return;
    }
    create.mutate({
      name: name.trim(),
      description: description.trim() || undefined,
      task_filter: filter.value,
      trial_config: config.value,
    });
  };

  return (
    <>
      <div className="loom-page-header">
        <h1>New campaign</h1>
      </div>

      <div className="loom-card">
        <label style={{ display: "block", marginBottom: "0.8rem" }}>
          <div>Name</div>
          <input
            value={name}
            onChange={(e) => setName(e.target.value)}
            placeholder="e.g. MIT slate — run 7"
            style={{ width: "100%", maxWidth: "400px" }}
          />
        </label>
        <label style={{ display: "block", marginBottom: "0.8rem" }}>
          <div>Description</div>
          <input
            value={description}
            onChange={(e) => setDescription(e.target.value)}
            placeholder="optional"
            style={{ width: "100%", maxWidth: "400px" }}
          />
        </label>
        <div
          style={{
            display: "grid",
            gridTemplateColumns: "1fr 1fr",
            gap: "1rem",
          }}
        >
          <label style={{ display: "block" }}>
            <div>
              Task filter (JSON)
              <span className="loom-muted">
                {" "}
                — keys: license, task_ids, benchmark_id
              </span>
            </div>
            <textarea
              className="loom-mono"
              value={filterText}
              onChange={(e) => setFilterText(e.target.value)}
              rows={8}
              style={{ width: "100%" }}
            />
          </label>
          <label style={{ display: "block" }}>
            <div>Trial config (JSON)</div>
            <textarea
              className="loom-mono"
              value={configText}
              onChange={(e) => setConfigText(e.target.value)}
              rows={8}
              style={{ width: "100%" }}
            />
          </label>
        </div>

        <div style={{ marginTop: "1rem" }}>
          <button
            onClick={submit}
            disabled={!name.trim() || create.isPending}
          >
            {create.isPending ? "Creating…" : "Create campaign"}
          </button>
        </div>

        {localError ? (
          <div className="loom-error" style={{ marginTop: "0.8rem" }}>
            {localError}
          </div>
        ) : null}
        {create.isError ? (
          <div style={{ marginTop: "0.8rem" }}>
            <ErrorState error={create.error} />
          </div>
        ) : null}
      </div>
    </>
  );
}
