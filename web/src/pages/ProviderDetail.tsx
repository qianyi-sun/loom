/**
 * /providers/:id — tab shell + Overview (inline) + Settings (inline).
 * Models tab implemented in T6.
 */
import { useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { Link, useNavigate, useParams, useSearchParams } from "react-router-dom";

import { api } from "../api/client";
import { Button } from "../components/Button";
import { Card } from "../components/Card";
import CommandSnippet from "../components/CommandSnippet";
import DocsCallout from "../components/DocsCallout";
import LoadingState from "../components/LoadingState";
import DeleteConnectionModal from "../components/providers/DeleteConnectionModal";
import ModelsTab from "../components/providers/ModelsTab";
import ProviderForm, {
  type ProviderFormValues,
} from "../components/providers/ProviderForm";
import RotateKeyModal from "../components/providers/RotateKeyModal";
import { StatusPill } from "../components/StatusPill";
import {
  useDeleteConnection,
  useEditConnection,
  useRotateConnectionKey,
  useTestConnection,
} from "../hooks/providers";
import {
  allowedModelsSummary,
  providerStatusSummary,
} from "../lib/providerDisplay";
import { providerSmokeBatchCommand } from "../lib/quickstartSnippets";

type TabName = "overview" | "models" | "settings";
type TestResult = { status: "valid" | "invalid"; last_validation_error?: string | null };
const TAB_NAMES: TabName[] = ["overview", "models", "settings"];

function parseTab(raw: string | null): TabName {
  return TAB_NAMES.includes(raw as TabName) ? (raw as TabName) : "overview";
}

export default function ProviderDetail(): JSX.Element {
  const { id = "" } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const [searchParams, setSearchParams] = useSearchParams();
  const tab = parseTab(searchParams.get("tab"));
  const rawReturnTo = searchParams.get("returnTo");
  const returnTo =
    rawReturnTo && rawReturnTo.startsWith("/") && !rawReturnTo.startsWith("//")
      ? rawReturnTo
      : null;
  const setTab = (nextTab: TabName): void => {
    const next = new URLSearchParams(searchParams);
    if (nextTab === "overview") next.delete("tab");
    else next.set("tab", nextTab);
    setSearchParams(next);
  };
  const { data, isLoading, error } = useQuery({
    queryKey: ["providers", id],
    queryFn: () => api.getProviderConnection(id),
    enabled: !!id,
  });

  if (isLoading) return <LoadingState />;
  if (error) {
    const status =
      typeof error === "object" && error !== null && "status" in error
        ? (error as { status: number }).status
        : 0;
    if (status === 404) {
      return (
        <Card>
          <Card.Body>
            <p className="text-slate-700">Connection not found.</p>
          </Card.Body>
        </Card>
      );
    }
    return (
      <Card>
        <Card.Body>
          <p className="text-red-700">Could not load connection.</p>
        </Card.Body>
      </Card>
    );
  }
  if (!data) return <LoadingState />;

  const conn = data as {
    id: string;
    name: string;
    type: string;
    base_url: string;
    status?: string;
    allowed_models?: string[] | null;
    created_at?: string;
    updated_at?: string;
    last_validated_at?: string | null;
    last_validation_error?: string | null;
    pricing_source?: string | null;
    rate_card_provider?: string | null;
  };

  return (
    <div className="space-y-4">
      <header>
        <h1 className="text-2xl font-bold text-slate-900">{conn.name}</h1>
        <p className="text-sm text-slate-500">{conn.type}</p>
      </header>
      <div role="tablist" className="flex gap-2 border-b border-slate-200">
        {TAB_NAMES.map((t) => (
          <button
            key={t}
            id={`provider-tab-${t}`}
            role="tab"
            aria-selected={tab === t}
            aria-controls={`provider-panel-${t}`}
            onClick={() => setTab(t)}
            className={
              tab === t
                ? "border-b-2 border-accent px-3 py-2 text-sm font-medium text-accent"
                : "px-3 py-2 text-sm font-medium text-slate-600 hover:text-slate-900"
            }
          >
            {t.charAt(0).toUpperCase() + t.slice(1)}
          </button>
        ))}
      </div>
      {tab === "overview" && (
        <div id="provider-panel-overview" role="tabpanel" aria-labelledby="provider-tab-overview">
          <OverviewTab conn={conn} id={id} />
        </div>
      )}
      {tab === "models" && (
        <div id="provider-panel-models" role="tabpanel" aria-labelledby="provider-tab-models">
          {returnTo ? (
            <div className="mb-3 rounded-lg border border-indigo-100 bg-indigo-50 px-3 py-2 text-sm text-indigo-800">
              <Link
                to={returnTo}
                className="font-medium text-accent hover:text-accent-hover"
              >
                Back to New Batch
              </Link>
              <span className="ml-2 text-indigo-700">
                after refreshing, preflighting, or adding the missing model.
              </span>
            </div>
          ) : null}
          <ModelsTab id={id} connectionName={conn.name} />
        </div>
      )}
      {tab === "settings" && (
        <div id="provider-panel-settings" role="tabpanel" aria-labelledby="provider-tab-settings">
          <SettingsTab
            conn={conn}
            id={id}
            onDeleted={() => navigate("/providers")}
          />
        </div>
      )}
    </div>
  );
}

function OverviewTab({
  conn,
  id,
}: {
  conn: {
    name: string;
    type: string;
    base_url: string;
    status?: string;
    allowed_models?: string[] | null;
    last_validation_error?: string | null;
    last_validated_at?: string | null;
  };
  id: string;
}): JSX.Element {
  const test = useTestConnection(id);
  const [testResult, setTestResult] = useState<TestResult | null>(null);
  const [showDetails, setShowDetails] = useState(false);
  const providerStatus = providerStatusSummary(conn.status);
  const allowedModels = allowedModelsSummary(conn.allowed_models);
  const testStatus = testResult
    ? providerStatusSummary(testResult.status)
    : null;

  const handleTest = async () => {
    try {
      const r = await test.mutateAsync();
      setTestResult(r as TestResult);
      setShowDetails(false);
    } catch {
      setTestResult(null);
    }
  };

  return (
    <Card>
      <Card.Body className="space-y-4">
        <dl className="grid grid-cols-2 gap-x-6 gap-y-2 text-sm">
          <dt className="font-medium text-slate-600">Base URL</dt>
          <dd>
            <p>{conn.base_url}</p>
            <p className="mt-1 text-xs text-slate-500">
              OpenAI-compatible root ending in /v1; this is the URL Loom calls
              from the server side.
            </p>
          </dd>
          <dt className="font-medium text-slate-600">Type</dt>
          <dd>{conn.type}</dd>
          <dt className="font-medium text-slate-600">Allowed models</dt>
          <dd>
            <p className="font-medium text-slate-800">{allowedModels.label}</p>
            <p className="mt-1 text-xs text-slate-500">
              {allowedModels.description}
            </p>
          </dd>
          <dt className="font-medium text-slate-600">Status</dt>
          <dd>
            <StatusPill
              variant={providerStatus.variant}
              title={providerStatus.description}
            >
              {providerStatus.label}
            </StatusPill>
            <p className="mt-1 text-xs text-slate-500">
              {providerStatus.description}
            </p>
          </dd>
          {conn.last_validated_at && (
            <>
              <dt className="font-medium text-slate-600">Last tested</dt>
              <dd>{conn.last_validated_at}</dd>
            </>
          )}
        </dl>
        <div>
          <Button onClick={handleTest} disabled={test.isPending} variant="primary">
            {test.isPending ? "Testing…" : "Test connection"}
          </Button>
        </div>
        {testResult && (
          <div className="space-y-2">
            {testStatus ? (
              <>
                <StatusPill
                  variant={testStatus.variant}
                  title={testStatus.description}
                >
                  {testStatus.label}
                </StatusPill>
                <p className="text-xs text-slate-500">
                  {testStatus.description}
                </p>
              </>
            ) : null}
            {testResult.status === "invalid" && (
              <div>
                <button
                  type="button"
                  onClick={() => setShowDetails((v) => !v)}
                  className="cursor-pointer text-sm text-slate-600 hover:text-slate-800"
                >
                  {showDetails ? "▾" : "▸"} Details
                </button>
                {showDetails && (
                  <pre className="mt-2 whitespace-pre-wrap rounded bg-slate-100 p-2 text-xs text-slate-800">
                    {testResult.last_validation_error ||
                      "Test failed; no error details reported by the provider."}
                  </pre>
                )}
              </div>
            )}
            {testResult.status === "invalid" && !showDetails && (
              <p className="text-xs text-slate-500">
                {testResult.last_validation_error ||
                  "Test failed; no error details reported by the provider."}
              </p>
            )}
          </div>
        )}
        <DocsCallout title="Provider next steps" tone="info">
          <p>
            Use these checks after creating or rotating the connection, before
            launching a larger benchmark.
          </p>
          <div className="grid gap-3 lg:grid-cols-2">
            <CommandSnippet
              label="Test provider"
              command={`loom providers test ${conn.name}`}
            />
            <CommandSnippet
              label="Refresh models"
              command={`loom providers models ${conn.name} --refresh`}
            />
          </div>
          <CommandSnippet
            label="One-task provider smoke"
            command={providerSmokeBatchCommand(
              conn.name,
              conn.allowed_models?.[0] ?? "gpt-4o-mini",
            )}
          />
        </DocsCallout>
      </Card.Body>
    </Card>
  );
}

function SettingsTab({
  conn,
  id,
  onDeleted,
}: {
  conn: {
    name: string;
    type: string;
    base_url: string;
    allowed_models?: string[] | null;
    pricing_source?: string | null;
    rate_card_provider?: string | null;
  };
  id: string;
  onDeleted: () => void;
}): JSX.Element {
  const edit = useEditConnection(id);
  const rotate = useRotateConnectionKey(id);
  const del = useDeleteConnection();
  const [showRotate, setShowRotate] = useState(false);
  const [showDelete, setShowDelete] = useState(false);

  const handleEdit = async (values: ProviderFormValues) => {
    await edit.mutateAsync({
      allowed_models: values.allowed_models,
      rate_card_provider: values.rate_card_provider,
    } as Parameters<typeof edit.mutateAsync>[0]);
  };

  const handleRotate = async (newKey: string) => {
    await rotate.mutateAsync(newKey);
    setShowRotate(false);
  };

  const handleDelete = async () => {
    await del.mutateAsync(id);
    setShowDelete(false);
    onDeleted();
  };

  return (
    <div className="space-y-6">
      <Card>
        <Card.Body className="space-y-4">
          <h2 className="text-lg font-semibold">Edit</h2>
          <ProviderForm
            mode="edit"
            initial={{
              name: conn.name,
              type: conn.type,
              base_url: conn.base_url,
              allowed_models: conn.allowed_models ?? [],
              pricing_source: conn.pricing_source ?? null,
              rate_card_provider: conn.rate_card_provider ?? null,
            }}
            pending={edit.isPending}
            onSubmit={handleEdit}
          />
        </Card.Body>
      </Card>
      <Card>
        <Card.Body className="space-y-3 border-l-4 border-red-500 pl-4">
          <h2 className="text-lg font-semibold text-red-700">Danger zone</h2>
          <div className="flex items-center justify-between gap-4 border-t border-slate-200 pt-3">
            <p className="text-sm text-slate-700">
              Rotate API key — replaces the stored credential.
            </p>
            <Button onClick={() => setShowRotate(true)}>Rotate</Button>
          </div>
          <div className="flex items-center justify-between gap-4 border-t border-slate-200 pt-3">
            <p className="text-sm text-slate-700">
              Delete connection — soft-delete, hides from team.
            </p>
            <Button variant="danger" onClick={() => setShowDelete(true)}>
              Delete
            </Button>
          </div>
        </Card.Body>
      </Card>
      {showRotate && (
        <RotateKeyModal
          connectionName={conn.name}
          pending={rotate.isPending}
          onClose={() => setShowRotate(false)}
          onSubmit={handleRotate}
        />
      )}
      {showDelete && (
        <DeleteConnectionModal
          connectionName={conn.name}
          pending={del.isPending}
          onClose={() => setShowDelete(false)}
          onSubmit={handleDelete}
        />
      )}
    </div>
  );
}
