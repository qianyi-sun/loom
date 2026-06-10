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
import { Button } from "../components/Button";
import { Card } from "../components/Card";
import ErrorState from "../components/ErrorState";
import { Input, Textarea } from "../components/Input";

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

function FieldLabel({
  children,
  hint,
}: {
  children: React.ReactNode;
  hint?: React.ReactNode;
}): JSX.Element {
  return (
    <div className="mb-1 flex items-baseline justify-between">
      <span className="text-xs font-medium uppercase tracking-wider text-slate-500">
        {children}
      </span>
      {hint ? <span className="text-xs text-slate-400">{hint}</span> : null}
    </div>
  );
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
    <div className="space-y-6">
      <header>
        <h1 className="text-2xl font-bold text-slate-900">New campaign</h1>
        <p className="mt-1 text-sm text-slate-500">
          Submit N trials in one batch — pick the task list with the
          filter, the run shape with the trial config.
        </p>
      </header>

      <Card>
        <Card.Body className="space-y-5">
          <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
            <label className="block">
              <FieldLabel>Name</FieldLabel>
              <Input
                value={name}
                onChange={(e) => setName(e.target.value)}
                placeholder="e.g. MIT slate — run 7"
              />
            </label>
            <label className="block">
              <FieldLabel hint="optional">Description</FieldLabel>
              <Input
                value={description}
                onChange={(e) => setDescription(e.target.value)}
                placeholder="What's this campaign testing?"
              />
            </label>
          </div>

          <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
            <label className="block">
              <FieldLabel hint="keys: license, task_ids, benchmark_id">
                Task filter (JSON)
              </FieldLabel>
              <Textarea
                value={filterText}
                onChange={(e) => setFilterText(e.target.value)}
                rows={8}
              />
            </label>
            <label className="block">
              <FieldLabel hint="agent / model / backend">
                Trial config (JSON)
              </FieldLabel>
              <Textarea
                value={configText}
                onChange={(e) => setConfigText(e.target.value)}
                rows={8}
              />
            </label>
          </div>

          {localError ? (
            <div className="rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
              {localError}
            </div>
          ) : null}
          {create.isError ? <ErrorState error={create.error} /> : null}
        </Card.Body>
        <Card.Footer>
          <div className="flex items-center justify-end">
            <Button
              variant="primary"
              onClick={submit}
              disabled={!name.trim() || create.isPending}
            >
              {create.isPending ? "Creating…" : "Create campaign"}
            </Button>
          </div>
        </Card.Footer>
      </Card>
    </div>
  );
}
