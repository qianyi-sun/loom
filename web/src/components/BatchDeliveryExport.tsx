import { queryKeys } from "../api/queryKeys";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";

import { api, type DeliveryExport } from "../api";
import { Button } from "./Button";
import ErrorState from "./ErrorState";
import { StatusPill } from "./StatusPill";

const ACTIVE_STATES = new Set(["submitted", "running"]);

function deliveryTrialText(delivery: DeliveryExport | undefined): string {
  const count = delivery?.manifest?.trial_count ?? delivery?.manifest?.task_count;
  return typeof count === "number" ? `${count} trials` : "not prepared";
}

function deliveryObjectText(delivery: DeliveryExport | undefined): string | null {
  const counts = delivery?.manifest?.object_counts;
  if (!counts) return null;
  const trajectories = counts.trajectory ?? 0;
  const atif = counts.atif ?? 0;
  const bundles = counts.trial_bundles ?? 0;
  const bundleFiles = counts.trial_bundle_files ?? 0;
  return `${trajectories} trajectories / ${atif} ATIF / ${bundles} complete Trial bundles (${bundleFiles} files)`;
}

type ReadyDeliveryExport = DeliveryExport & {
  status: "ready";
  download_url: string;
};

function deliveryReady(
  delivery: DeliveryExport | undefined,
): delivery is ReadyDeliveryExport {
  return delivery?.status === "ready" && typeof delivery.download_url === "string";
}

export function BatchDeliveryExport({ batchId, state }: {
  batchId: string;
  state: string;
}): JSX.Element {
  const queryClient = useQueryClient();
  const deliveryQuery = useQuery({
    queryKey: queryKeys["batch-delivery-export"](batchId),
    queryFn: () => api.getBatchDeliveryExport(batchId),
    enabled: !!batchId && !ACTIVE_STATES.has(state),
  });

  const createDeliveryExport = useMutation({
    mutationFn: () => api.createBatchDeliveryExport(batchId),
    onSuccess: (data) => {
      queryClient.setQueryData(["batch-delivery-export", batchId], data);
    },
  });

  const downloadDeliveryExport = useMutation({
    mutationFn: (delivery: DeliveryExport) => {
      if (!delivery.download_url) {
        throw new Error("delivery bundle is not ready");
      }
      return api.downloadBatchDeliveryExport(
        delivery.download_url,
        delivery.archive_filename ?? `${batchId}-delivery.tar.gz`,
      );
    },
  });

  if (ACTIVE_STATES.has(state)) return (
    <p className="text-sm text-slate-600">Batch delivery export becomes available when this run finishes. Completed Trial bundles remain downloadable individually.</p>
  );
  const deliveryExport = createDeliveryExport.data ?? deliveryQuery.data;
  const deliveryStatus = deliveryExport?.status === "ready" ? "ready" : "not ready";
  const deliveryObjects = deliveryObjectText(deliveryExport);
  return <section aria-label="Batch delivery export">
    <p className="mb-2 text-sm text-slate-600">Export final results for this batch, using the existing selection rules for linked reruns. Includes trajectories and complete Trial bundles; artifact metadata alone is not a delivery package.</p>

    <div className="rounded-md border border-slate-200 bg-slate-50 px-3 py-3 text-sm text-slate-800">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="flex flex-wrap items-center gap-2">
            <div className="font-semibold text-slate-900">
              Delivery bundle
            </div>
            <StatusPill
              variant={deliveryReady(deliveryExport) ? "success" : "neutral"}
            >
              {deliveryQuery.isFetching && !deliveryExport
                ? "checking"
                : deliveryStatus}
            </StatusPill>
          </div>
          <div className="mt-1 text-xs text-slate-600">
            {deliveryTrialText(deliveryExport)}
            {deliveryObjects ? ` · ${deliveryObjects}` : ""}
          </div>
          {deliveryExport?.sha256 ? (
            <div className="mt-1 break-all font-mono text-xs text-slate-600">
              sha256:{deliveryExport.sha256}
            </div>
          ) : null}
        </div>
        {deliveryReady(deliveryExport) ? (
          <Button
            variant="secondary"
            onClick={() => downloadDeliveryExport.mutate(deliveryExport)}
            disabled={downloadDeliveryExport.isPending}
            title="Download the prepared archive through the Loom API."
          >
            {downloadDeliveryExport.isPending
              ? "Downloading..."
              : "Download bundle"}
          </Button>
        ) : (
          <Button
            variant="secondary"
            onClick={() => createDeliveryExport.mutate()}
            disabled={createDeliveryExport.isPending}
            title="Create the delivery archive and checksum for this batch family."
          >
            {createDeliveryExport.isPending
              ? "Preparing..."
              : "Prepare bundle"}
          </Button>
        )}
      </div>
      {deliveryQuery.isError ? <ErrorState error={deliveryQuery.error} /> : null}
      {createDeliveryExport.isError ? (
        <ErrorState error={createDeliveryExport.error} />
      ) : null}
      {downloadDeliveryExport.isError ? (
        <ErrorState error={downloadDeliveryExport.error} />
      ) : null}
    </div>
  </section>;
}
