import { useState } from "react";

import { Button } from "../Button";
import { Input } from "../Input";

export type RotateKeyModalProps = {
  connectionName: string;
  onClose: () => void;
  onSubmit: (newKey: string) => void;
  pending?: boolean;
};

export default function RotateKeyModal({
  connectionName, onClose, onSubmit, pending,
}: RotateKeyModalProps): JSX.Element {
  const [newKey, setNewKey] = useState("");
  const [confirmName, setConfirmName] = useState("");
  const canSubmit = newKey.trim().length > 0 && confirmName === connectionName;

  return (
    <div role="dialog" aria-modal="true"
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 p-4">
      <div className="w-full max-w-md rounded-lg bg-white shadow-lg">
        <div className="space-y-4 p-6">
          <h2 className="text-lg font-bold text-slate-900">Rotate API key</h2>
          <p className="text-sm text-slate-600">
            Replaces the stored API key. Calls in flight may complete with either
            the old or new key depending on timing. New trials use the new key.
          </p>
          <div>
            <label htmlFor="rk-new" className="block text-sm font-medium text-slate-700">
              New API key
            </label>
            <Input id="rk-new" type="password" autoComplete="off"
              value={newKey} onChange={(e) => setNewKey(e.target.value)} />
          </div>
          <div>
            <label htmlFor="rk-name" className="block text-sm font-medium text-slate-700">
              Type connection name to confirm: <code>{connectionName}</code>
            </label>
            <Input id="rk-name" value={confirmName}
              onChange={(e) => setConfirmName(e.target.value)} />
          </div>
          <div className="flex justify-end gap-2">
            <Button onClick={onClose}>Cancel</Button>
            <Button variant="primary"
              onClick={() => onSubmit(newKey)}
              disabled={!canSubmit || pending}>
              Rotate key
            </Button>
          </div>
        </div>
      </div>
    </div>
  );
}
