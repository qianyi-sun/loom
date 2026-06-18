import { useState } from "react";

import { Button } from "../Button";
import { Input } from "../Input";

export type AddManualModelModalProps = {
  onClose: () => void;
  onSubmit: (model: { model_id: string; display_name?: string }) => void;
  pending?: boolean;
};

export default function AddManualModelModal({
  onClose, onSubmit, pending,
}: AddManualModelModalProps): JSX.Element {
  const [modelId, setModelId] = useState("");
  const [displayName, setDisplayName] = useState("");
  const canSubmit = modelId.trim().length > 0;

  return (
    <div role="dialog" aria-modal="true"
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 p-4">
      <div className="w-full max-w-md rounded-lg bg-white shadow-lg">
        <div className="space-y-4 p-6">
          <h2 className="text-lg font-bold text-slate-900">Add manual model</h2>
          <p className="text-sm text-slate-500">
            Manually register a model the upstream catalog doesn&apos;t expose.
            The model will appear in pickers immediately.
          </p>
          <div>
            <label htmlFor="amm-id" className="block text-sm font-medium text-slate-700">
              Model ID
            </label>
            <Input id="amm-id" value={modelId}
              onChange={(e) => setModelId(e.target.value)}
              placeholder="manual/my-model-v1" />
          </div>
          <div>
            <label htmlFor="amm-name" className="block text-sm font-medium text-slate-700">
              Display name (optional)
            </label>
            <Input id="amm-name" value={displayName}
              onChange={(e) => setDisplayName(e.target.value)} />
          </div>
          <div className="flex justify-end gap-2">
            <Button onClick={onClose}>Cancel</Button>
            <Button variant="primary"
              onClick={() => onSubmit({
                model_id: modelId.trim(),
                ...(displayName.trim() ? { display_name: displayName.trim() } : {}),
              })}
              disabled={!canSubmit || pending}>
              Add model
            </Button>
          </div>
        </div>
      </div>
    </div>
  );
}
