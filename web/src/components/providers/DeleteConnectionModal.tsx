import { useState } from "react";

import { Button } from "../Button";
import { Input } from "../Input";

export type DeleteConnectionModalProps = {
  connectionName: string;
  onClose: () => void;
  onSubmit: () => void;
  pending?: boolean;
};

export default function DeleteConnectionModal({
  connectionName, onClose, onSubmit, pending,
}: DeleteConnectionModalProps): JSX.Element {
  const [confirmName, setConfirmName] = useState("");
  const canSubmit = confirmName === connectionName;

  return (
    <div role="dialog" aria-modal="true"
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 p-4">
      <div className="w-full max-w-md rounded-lg bg-white shadow-lg">
        <div className="space-y-4 p-6">
          <h2 className="text-lg font-bold text-red-700">Delete connection</h2>
          <p className="text-sm text-slate-600">
            Soft-delete. Existing trial records keep their <code>provider_connection_id</code>{" "}
            attribution for billing/audit. The connection becomes hidden from your
            team and cannot be used for new trials, but in-flight calls complete
            normally. The encrypted key is retained until Phase 5 cleanup.
          </p>
          <div>
            <label htmlFor="dc-name" className="block text-sm font-medium text-slate-700">
              Type connection name to confirm: <code>{connectionName}</code>
            </label>
            <Input id="dc-name" value={confirmName}
              onChange={(e) => setConfirmName(e.target.value)} />
          </div>
          <div className="flex justify-end gap-2">
            <Button onClick={onClose}>Cancel</Button>
            <Button variant="danger" onClick={onSubmit} disabled={!canSubmit || pending}>
              Delete
            </Button>
          </div>
        </div>
      </div>
    </div>
  );
}
