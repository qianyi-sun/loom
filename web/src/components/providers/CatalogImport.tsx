import { useState } from "react";
import { priceCatalogApi, type PriceCatalog } from "../../api/providers";
import { Button } from "../Button";
import { Textarea } from "../Input";

export default function CatalogImport({
  catalog,
  onUpdated,
}: {
  catalog: PriceCatalog;
  onUpdated: (next: PriceCatalog) => void;
}) {
  const [content, setContent] = useState("");
  const [format, setFormat] = useState<"csv" | "json">("csv");
  const [preview, setPreview] = useState<{
    added: string[];
    changed: string[];
    removed: string[];
    revision: number;
  } | null>(null);
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  const submit = async (apply: boolean) => {
    setBusy(true);
    setError("");
    try {
      const result = await priceCatalogApi.import(
        catalog.id,
        content,
        format,
        apply && preview ? preview.revision : catalog.revision,
        apply,
      );
      if (apply) {
        onUpdated(result.catalog);
        setPreview(null);
      } else {
        setPreview({ ...result.summary, revision: result.catalog.revision });
      }
    } catch (e) {
      setPreview(null);
      setError(
        String((e as { detail?: string }).detail ?? "Catalog import failed"),
      );
    } finally {
      setBusy(false);
    }
  };
  return (
    <details>
      <summary>Update team catalog (team administrators)</summary>
      <p>
        Replace the full catalog atomically. Existing calls retain their prices.
      </p>
      <select
        aria-label="Catalog import format"
        value={format}
        onChange={(e) => {
          setFormat(e.target.value as "csv" | "json");
          setPreview(null);
        }}
      >
        <option>csv</option>
        <option>json</option>
      </select>
      <Textarea
        aria-label="Catalog import content"
        value={content}
        onChange={(e) => {
          setContent(e.target.value);
          setPreview(null);
        }}
      />
      <Button type="button" disabled={busy} onClick={() => void submit(false)}>
        Preview catalog changes
      </Button>
      {preview && (
        <div>
          <p>
            Added: {preview.added.join(", ") || "none"}. Changed:{" "}
            {preview.changed.join(", ") || "none"}. Removed:{" "}
            {preview.removed.join(", ") || "none"}.
          </p>
          <Button
            type="button"
            disabled={busy}
            onClick={() => void submit(true)}
          >
            Apply catalog changes
          </Button>
        </div>
      )}
      {error && <p role="alert">{error}</p>}
    </details>
  );
}
