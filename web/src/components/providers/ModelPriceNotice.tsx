import { useQuery } from "@tanstack/react-query";
import { priceCatalogApi, type ProviderPricing } from "../../api/providers";

export default function ModelPriceNotice({
  connection,
  model,
}: {
  connection?: ProviderPricing;
  model?: string;
}) {
  const catalog = useQuery({
    queryKey: ["price-catalogs", connection?.catalog_id],
    queryFn: () => priceCatalogApi.list(),
    enabled:
      connection?.pricing_mode === "catalog" && Boolean(connection.catalog_id),
    staleTime: 60000,
  });
  if (
    !connection ||
    !model ||
    !connection.pricing_mode ||
    connection.legacy_pricing
  )
    return null;
  if (connection.pricing_mode === "usage_only")
    return (
      <p className="text-sm text-slate-600">
        Usage only: tokens are recorded; monetary cost is not applicable.
      </p>
    );
  const selected = catalog.data?.items?.find(
    (c) => c.id === connection.catalog_id,
  );
  const price =
    connection.pricing_mode === "custom"
      ? connection.custom_pricing?.[model]
      : selected?.prices[selected.aliases[model] ?? model];
  if (connection.pricing_mode === "catalog" && catalog.isFetching)
    return <p className="text-sm">Checking model price…</p>;
  if (!price)
    return (
      <p role="status" className="text-sm text-amber-700">
        Price unknown for {model}. You can run with token usage recorded; a hard
        monetary budget requires usable prices.
      </p>
    );
  return (
    <p className="text-sm text-slate-600">
      Estimated rates: input ${price.input_usd_per_1m}, output $
      {price.output_usd_per_1m} per 1M tokens.{" "}
      {selected?.stale
        ? "Catalog update is stale; the last valid prices will be used."
        : "Actual usage determines the estimate."}
    </p>
  );
}
