import type { ModelPrice } from "../../api/providers";

export const priceKeys = [
  "input_usd_per_1m",
  "output_usd_per_1m",
  "cache_read_usd_per_1m",
  "cache_write_usd_per_1m",
] as const;
export type PriceDraft = Record<string, string[]>;
export const toDraft = (prices: Record<string, ModelPrice>): PriceDraft =>
  Object.fromEntries(
    Object.entries(prices).map(([model, price]) => [
      model,
      priceKeys.map((key) => (price[key] == null ? "" : String(price[key]))),
    ]),
  );
export function parseDraft(draft: PriceDraft): Record<string, ModelPrice> {
  return Object.fromEntries(
    Object.entries(draft).map(([model, values]) => {
      if (!model || model.trim() !== model || /\s/.test(model))
        throw new Error("Use exact model IDs without whitespace.");
      if (values[0] === "" || values[1] === "")
        throw new Error(
          `${model}: input and output prices are required. Remove the row to leave it unpriced.`,
        );
      if (
        values.some(
          (v) => v !== "" && (!Number.isFinite(Number(v)) || Number(v) < 0),
        )
      )
        throw new Error(
          `${model}: prices must be finite, nonnegative numbers.`,
        );
      return [
        model,
        Object.fromEntries(
          priceKeys.map((key, i) => [
            key,
            values[i] === "" ? null : Number(values[i]),
          ]),
        ) as unknown as ModelPrice,
      ];
    }),
  );
}
