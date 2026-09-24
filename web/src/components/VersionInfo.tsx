/**
 * VersionInfo — the persistent "Nebius · env" / "Build <commit>" entry at
 * the bottom of the sidebar (#2009). Clicking it opens accessible details:
 * full frontend commit (copy + GitHub link), source ref, build time, and
 * the backend's own reported build — a clearly separate identity, since
 * one backend response is evidence for that responding instance only.
 *
 * The loaded frontend identity (`buildInfo.ts`) is a frozen, build-time
 * constant: it never changes for this already-open tab. The "served" check
 * (`useServedFrontendBuild`) is a live, focus-triggered poll that can drift
 * from it after a rollout — surfaced as a non-disruptive update notice with
 * an explicit refresh action, never an automatic reload or lost input.
 */
import { useState } from "react";

import { commitUrl, LOADED_BUILD_INFO, shortRevision } from "../lib/buildInfo";
import {
  frontendUpdateStatus,
  useBackendVersion,
  useServedFrontendBuild,
} from "../lib/buildVersion";
import { Button } from "./Button";
import { CopyableId } from "./CopyableId";
import { Modal } from "./Modal";

export interface VersionInfoProps {
  environmentLabel: string;
}

function displayValue(value: string | null): string {
  return value ?? "unknown";
}

export default function VersionInfo({
  environmentLabel,
}: VersionInfoProps): JSX.Element {
  const [open, setOpen] = useState(false);
  const served = useServedFrontendBuild();
  const backend = useBackendVersion();
  const { hasNewerBuild } = frontendUpdateStatus(
    LOADED_BUILD_INFO.revision,
    served.data,
  );
  const revision = LOADED_BUILD_INFO.revision;
  const href = commitUrl(revision);
  const backendRevision = backend.data?.buildRevision ?? null;

  return (
    <>
      <button
        type="button"
        onClick={() => setOpen(true)}
        aria-label="Deployed version details"
        className="flex w-full flex-col gap-0.5 rounded-md border border-slate-200 bg-slate-50 px-2 py-1.5 text-left hover:bg-slate-100"
      >
        {/* The environment label may be long (e.g. "Nebius integration"),
            so it alone truncates; the revision gets its own line and is
            never clipped, keeping the build identifiable at a glance. */}
        <span className="flex w-full min-w-0 items-center justify-between gap-2">
          <span
            className="min-w-0 truncate font-mono text-[10px] text-slate-600"
            title={`Nebius · ${environmentLabel}`}
          >
            Nebius · {environmentLabel}
          </span>
          {hasNewerBuild ? (
            <span
              aria-hidden="true"
              className="h-1.5 w-1.5 shrink-0 rounded-full bg-accent"
              title="A newer build is available"
            />
          ) : null}
        </span>
        <span
          data-testid="sidebar-build-revision"
          className="whitespace-nowrap font-mono text-[10px] font-medium text-slate-700"
        >
          Build {shortRevision(revision)}
        </span>
      </button>

      <Modal
        open={open}
        onClose={() => setOpen(false)}
        title="Deployed version"
        size="sm"
      >
        <div className="space-y-4 text-sm">
          {hasNewerBuild ? (
            <div
              role="status"
              className="rounded-md border border-accent/30 bg-accent/5 px-3 py-2 text-xs"
            >
              <p className="font-medium text-slate-900">
                A newer frontend build is available.
              </p>
              <p className="mt-1 text-slate-600">
                This page keeps running the build it already loaded. Refresh
                to load the new one — nothing reloads automatically, so any
                unsaved input stays put until you choose to.
              </p>
              <Button
                className="mt-2"
                size="sm"
                onClick={() => window.location.reload()}
              >
                Refresh
              </Button>
            </div>
          ) : null}

          <section aria-label="Frontend build">
            <h3 className="text-xs font-semibold uppercase tracking-wider text-slate-600">
              Frontend (this page)
            </h3>
            <dl className="mt-1 space-y-1">
              <div className="flex items-center justify-between gap-2">
                <dt className="text-slate-500">Commit</dt>
                <dd className="flex items-center gap-2">
                  {revision ? (
                    <>
                      <CopyableId value={revision} chars={12} />
                      {href ? (
                        <a
                          href={href}
                          target="_blank"
                          rel="noreferrer"
                          className="text-xs text-accent hover:underline"
                        >
                          View commit
                        </a>
                      ) : null}
                    </>
                  ) : (
                    <span className="text-xs text-slate-500">
                      local / unknown
                    </span>
                  )}
                </dd>
              </div>
              <div className="flex items-center justify-between gap-2">
                <dt className="text-slate-500">Source ref</dt>
                <dd className="font-mono text-xs text-slate-700">
                  {displayValue(LOADED_BUILD_INFO.sourceRef)}
                </dd>
              </div>
              <div className="flex items-center justify-between gap-2">
                <dt className="text-slate-500">Built</dt>
                <dd className="text-xs text-slate-700">
                  {displayValue(LOADED_BUILD_INFO.buildTime)}
                </dd>
              </div>
            </dl>
          </section>

          <section aria-label="Backend build">
            <h3 className="text-xs font-semibold uppercase tracking-wider text-slate-600">
              Backend (responding instance)
            </h3>
            <p className="mt-1 text-xs text-slate-500">
              One reply is evidence for that instance only, not proof every
              replica has finished rolling out.
            </p>
            <dl className="mt-1 space-y-1">
              <div className="flex items-center justify-between gap-2">
                <dt className="text-slate-500">Commit</dt>
                <dd>
                  {backendRevision ? (
                    <CopyableId value={backendRevision} chars={12} />
                  ) : (
                    <span className="text-xs text-slate-500">unknown</span>
                  )}
                </dd>
              </div>
              <div className="flex items-center justify-between gap-2">
                <dt className="text-slate-500">Built</dt>
                <dd className="text-xs text-slate-700">
                  {displayValue(backend.data?.buildTime ?? null)}
                </dd>
              </div>
            </dl>
          </section>
        </div>
      </Modal>
    </>
  );
}
