/**
 * Shared form for Create (api_key required) and Edit (api_key hidden;
 * rotation has its own modal). Advanced section is collapsible:
 * pricing_source / pricing_data / rate_card_provider — defaults apply
 * per provider type if untouched.
 *
 * #167 spec §SPA architecture / §Components.
 */
import { useState } from "react";

import { Button } from "../Button";
import { Input, Textarea } from "../Input";

export type ProviderFormValues = {
  name: string;
  type: string;
  base_url: string;
  api_key?: string;
  allowed_models?: string[];
  pricing_source?: string | null;
  pricing_data?: Record<string, number> | null;
  rate_card_provider?: string | null;
};

export type ProviderFormProps = {
  mode: "create" | "edit";
  initial?: Partial<ProviderFormValues>;
  pending?: boolean;
  onSubmit: (values: ProviderFormValues) => void;
};

const PROVIDER_TYPES = [
  "openai-compatible",
  "anthropic",
  "google",
  "custom",
];

const PRICING_SOURCES = [
  "rate-card",
  "tokens-only",
  "operator-supplied",
];

export default function ProviderForm({
  mode,
  initial,
  pending,
  onSubmit,
}: ProviderFormProps): JSX.Element {
  const [name, setName] = useState(initial?.name ?? "");
  const [type, setType] = useState(initial?.type ?? "openai-compatible");
  const [baseUrl, setBaseUrl] = useState(initial?.base_url ?? "");
  const [apiKey, setApiKey] = useState("");
  const [allowedModelsText, setAllowedModelsText] = useState(
    (initial?.allowed_models ?? []).join("\n"),
  );
  const [showAdvanced, setShowAdvanced] = useState(false);
  const [pricingSource, setPricingSource] = useState(initial?.pricing_source ?? "");
  const [pricingDataText, setPricingDataText] = useState(
    initial?.pricing_data ? JSON.stringify(initial.pricing_data, null, 2) : "",
  );
  const [rateCardProvider, setRateCardProvider] = useState(
    initial?.rate_card_provider ?? "",
  );

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    const allowed = allowedModelsText
      .split(/[\n,]/).map((s) => s.trim()).filter(Boolean);
    let pricingData: Record<string, number> | null = null;
    if (pricingDataText.trim()) {
      try {
        pricingData = JSON.parse(pricingDataText);
      } catch {
        // backend will reject malformed JSON; UI flagging deferred to v1.1
      }
    }
    const values: ProviderFormValues = {
      name,
      type,
      base_url: baseUrl,
      ...(mode === "create" ? { api_key: apiKey } : {}),
      ...(allowed.length ? { allowed_models: allowed } : {}),
      ...(pricingSource ? { pricing_source: pricingSource } : {}),
      ...(pricingData ? { pricing_data: pricingData } : {}),
      ...(rateCardProvider ? { rate_card_provider: rateCardProvider } : {}),
    };
    onSubmit(values);
  };

  return (
    <form onSubmit={handleSubmit} className="space-y-4" noValidate>
      <div>
        <label htmlFor="pf-name" className="block text-sm font-medium text-slate-700">
          Name
        </label>
        <Input
          id="pf-name"
          value={name}
          onChange={(e) => setName(e.target.value)}
          required
        />
      </div>
      <div>
        <label htmlFor="pf-type" className="block text-sm font-medium text-slate-700">
          Type
        </label>
        <select
          id="pf-type"
          value={type}
          onChange={(e) => setType(e.target.value)}
          className="mt-1 block w-full rounded-md border border-slate-300 bg-white px-3 py-2 text-sm shadow-sm"
        >
          {PROVIDER_TYPES.map((t) => (<option key={t} value={t}>{t}</option>))}
        </select>
      </div>
      <div>
        <label htmlFor="pf-base-url" className="block text-sm font-medium text-slate-700">
          Base URL
        </label>
        <Input
          id="pf-base-url"
          type="url"
          value={baseUrl}
          onChange={(e) => setBaseUrl(e.target.value)}
          required
          placeholder="https://api.example.com/v1"
        />
        <p className="mt-1 text-xs text-slate-500">
          For OpenAI-compatible servers, use the API root ending in /v1. For
          Anthropic or Google, use the provider's documented API root.
        </p>
      </div>
      {mode === "create" && (
        <div>
          <label htmlFor="pf-api-key" className="block text-sm font-medium text-slate-700">
            API key
          </label>
          <Input
            id="pf-api-key"
            type="password"
            value={apiKey}
            onChange={(e) => setApiKey(e.target.value)}
            required
            autoComplete="off"
          />
        </div>
      )}
      <div>
        <label htmlFor="pf-allowed" className="block text-sm font-medium text-slate-700">
          Allowed models (optional, one per line)
        </label>
        <Textarea
          id="pf-allowed"
          rows={4}
          value={allowedModelsText}
          onChange={(e) => setAllowedModelsText(e.target.value)}
          placeholder={`gpt-4o\nclaude-3-opus`}
        />
        <p className="mt-1 text-xs text-slate-500">
          Blank means all discovered models are allowed. Add one model ID per
          line to restrict the picker.
        </p>
      </div>
      <div>
        <button
          type="button"
          onClick={() => setShowAdvanced((v) => !v)}
          className="cursor-pointer text-sm font-medium text-slate-600 hover:text-slate-800"
        >
          {showAdvanced ? "▾" : "▸"} Advanced
        </button>
        {showAdvanced && (
          <div className="mt-3 space-y-3 rounded-md border border-slate-200 bg-slate-50 p-3">
            <div>
              <label htmlFor="pf-pricing-source" className="block text-sm font-medium text-slate-700">
                Pricing source
              </label>
              <select
                id="pf-pricing-source"
                value={pricingSource}
                onChange={(e) => setPricingSource(e.target.value)}
                className="mt-1 block w-full rounded-md border border-slate-300 bg-white px-3 py-2 text-sm"
              >
                <option value="">(default for provider type)</option>
                {PRICING_SOURCES.map((p) => (<option key={p} value={p}>{p}</option>))}
              </select>
              <p className="mt-1 text-xs text-slate-500">
                Rate-card uses published pricing. Tokens-only records usage
                without dollars. Operator-supplied uses the JSON below.
              </p>
            </div>
            <div>
              <label htmlFor="pf-pricing-data" className="block text-sm font-medium text-slate-700">
                Pricing data (JSON; only for operator-supplied source)
              </label>
              <Textarea
                id="pf-pricing-data"
                rows={4}
                value={pricingDataText}
                onChange={(e) => setPricingDataText(e.target.value)}
                placeholder='{"input_per_million": 1.50, "output_per_million": 6.00}'
              />
              <p className="mt-1 text-xs text-slate-500">
                Used only when Pricing source is operator-supplied.
              </p>
            </div>
            <div>
              <label htmlFor="pf-rate-card-provider" className="block text-sm font-medium text-slate-700">
                Rate card provider (override)
              </label>
              <Input
                id="pf-rate-card-provider"
                value={rateCardProvider}
                onChange={(e) => setRateCardProvider(e.target.value)}
                placeholder="(default for provider type)"
              />
              <p className="mt-1 text-xs text-slate-500">
                Override the provider namespace used to match this connection
                to published rate-card rows.
              </p>
            </div>
          </div>
        )}
      </div>
      <div className="flex justify-end">
        <Button type="submit" variant="primary" disabled={pending}>
          {mode === "create" ? "Create connection" : "Save changes"}
        </Button>
      </div>
    </form>
  );
}
