import { useId, useState } from "react";

import { Button } from "../Button";
import { Input } from "../Input";
import { Modal } from "../Modal";

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
  const newKeyInputId = useId();
  const confirmNameInputId = useId();
  const canSubmit = newKey.trim().length > 0 && confirmName === connectionName;

  return (
    <Modal
      open
      onClose={onClose}
      title="Rotate API key"
      description="Replaces the stored API key. Calls in flight may complete with either the old or new key depending on timing. New trials use the new key."
      size="sm"
      footer={
        <>
          <Button onClick={onClose}>Cancel</Button>
          <Button
            variant="primary"
            onClick={() => onSubmit(newKey)}
            disabled={!canSubmit || pending}
          >
            Rotate key
          </Button>
        </>
      }
    >
      <div className="space-y-4">
        <div>
          <label
            htmlFor={newKeyInputId}
            className="block text-sm font-medium text-slate-700"
          >
            New API key
          </label>
          <Input
            id={newKeyInputId}
            type="password"
            autoComplete="off"
            value={newKey}
            onChange={(e) => setNewKey(e.target.value)}
          />
        </div>
        <div>
          <label
            htmlFor={confirmNameInputId}
            className="block text-sm font-medium text-slate-700"
          >
            Type connection name to confirm: <code>{connectionName}</code>
          </label>
          <Input
            id={confirmNameInputId}
            value={confirmName}
            onChange={(e) => setConfirmName(e.target.value)}
          />
        </div>
      </div>
    </Modal>
  );
}
