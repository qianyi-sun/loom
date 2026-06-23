/**
 * Models tab on /providers/:id. Refresh + Add manual + Hide/Unhide.
 * Backend returns all cached rows (no pagination); v1 fetches all and
 * provides a client-side filter input. See #167 spec.
 */
import { useQuery } from "@tanstack/react-query";
import { useState } from "react";

import { api, type ProviderConnectionModelEntry } from "../../api/client";
import { Button } from "../Button";
import { Card } from "../Card";
import CommandSnippet from "../CommandSnippet";
import DocsCallout from "../DocsCallout";
import { Input } from "../Input";
import LoadingState from "../LoadingState";
import {
  useAddManualModel,
  useHideModel,
  useRefreshModels,
  useUnhideModel,
} from "../../hooks/providers";
import AddManualModelModal from "./AddManualModelModal";

export type ModelsTabProps = { id: string; connectionName?: string };

export default function ModelsTab({ id, connectionName }: ModelsTabProps): JSX.Element {
  const [filter, setFilter] = useState("");
  const [showAdd, setShowAdd] = useState(false);
  const cliConnection = connectionName ?? id;

  const { data, isLoading, error } = useQuery({
    queryKey: ["providers", id, "models"],
    queryFn: () => api.listProviderConnectionModels(id),
  });

  const refresh = useRefreshModels(id);
  const addManual = useAddManualModel(id);
  const hide = useHideModel(id);
  const unhide = useUnhideModel(id);

  if (isLoading) return <LoadingState />;
  if (error) {
    return (
      <Card>
        <Card.Body>
          <p className="text-red-700">Could not load models.</p>
        </Card.Body>
      </Card>
    );
  }

  const items = (data?.items ?? []).filter((m) =>
    !filter || m.model_id.toLowerCase().includes(filter.toLowerCase()),
  );

  const isHidden = (model: ProviderConnectionModelEntry) =>
    model.visible === false || model.visibility === "hidden";

  const handleAdd = async (model: { model_id: string; display_name?: string }) => {
    await addManual.mutateAsync(model as Parameters<typeof addManual.mutateAsync>[0]);
    setShowAdd(false);
  };

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between gap-2">
        <Input placeholder="Filter models…" value={filter}
          onChange={(e) => setFilter(e.target.value)} className="w-72" />
        <div className="flex gap-2">
          <Button onClick={() => refresh.mutate()} disabled={refresh.isPending}>
            {refresh.isPending ? "Refreshing…" : "Refresh"}
          </Button>
          <Button variant="primary" onClick={() => setShowAdd(true)}>
            Add manual model
          </Button>
        </div>
      </div>
      <DocsCallout title="Model picker guidance" tone="info">
        <p>
          Refreshed visible models appear in New Batch. Hide noisy upstream
          entries, or add a manual model ID when the provider omits it from
          discovery.
        </p>
        <CommandSnippet
          label="Refresh provider model cache"
          command={`loom providers models ${cliConnection} --refresh`}
        />
      </DocsCallout>
      <Card>
        {items.length === 0 ? (
          <Card.Body className="text-center text-sm text-slate-500">
            No models cached. Click <strong>Refresh</strong> to fetch the upstream
            catalog, or <strong>Add manual model</strong> to register one by hand.
          </Card.Body>
        ) : (
          <table className="min-w-full">
            <thead>
              <tr className="border-b border-slate-200 text-left text-xs uppercase tracking-wider text-slate-500">
                <th className="px-4 py-2">Model ID</th>
                <th className="px-4 py-2">Source</th>
                <th className="px-4 py-2">Hidden</th>
                <th className="px-4 py-2"></th>
              </tr>
            </thead>
            <tbody>
              {items.map((m) => {
                const hidden = isHidden(m);
                return (
                  <tr key={m.model_id} className="border-b border-slate-100">
                    <td className="px-4 py-3 font-mono text-sm">{m.model_id}</td>
                    <td className="px-4 py-3 text-sm text-slate-600">{m.source ?? "—"}</td>
                    <td className="px-4 py-3 text-sm">
                      {hidden ? (
                        <span className="rounded bg-slate-100 px-2 py-1 text-xs text-slate-600"
                          title="Hidden models don't appear in New Batch's picker">
                          hidden
                        </span>
                      ) : (<span className="text-slate-400">—</span>)}
                    </td>
                    <td className="px-4 py-3 text-right">
                      {hidden ? (
                        <Button onClick={() => unhide.mutate(m.model_id)} disabled={unhide.isPending}>
                          Unhide
                        </Button>
                      ) : (
                        <Button onClick={() => hide.mutate(m.model_id)} disabled={hide.isPending}>
                          Hide
                        </Button>
                      )}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        )}
      </Card>
      {showAdd && (
        <AddManualModelModal pending={addManual.isPending}
          onClose={() => setShowAdd(false)} onSubmit={handleAdd} />
      )}
    </div>
  );
}
