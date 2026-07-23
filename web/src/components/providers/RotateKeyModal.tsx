import { useId, useState } from "react";

import { DestructiveActionDialog } from "../DestructiveActionDialog";
import { Input } from "../Input";

export type RotateKeyModalProps = {
  connectionName: string;
  error?: unknown | null;
  onClose: () => void;
  onSubmit: (newKey: string) => Promise<void>;
  pending?: boolean;
};

export default function RotateKeyModal({
  connectionName,
  error = null,
  onClose,
  onSubmit,
  pending = false,
}: RotateKeyModalProps): JSX.Element {
  const [newKey, setNewKey] = useState("");
  const newKeyInputId = useId();

  return (
    <DestructiveActionDialog
      open
      onClose={onClose}
      title="Rotate API key"
      target={connectionName}
      consequence="The stored credential will be replaced. In-flight calls may use either key depending on timing; new trials use the new key."
      confirmLabel="Rotate API key"
      pendingLabel="Rotating…"
      confirmation={{
        type: "typed",
        expected: connectionName,
        inputLabel: `Type connection name to confirm: ${connectionName}`,
      }}
      pending={pending}
      error={error}
      confirmDisabled={newKey.trim().length === 0}
      onConfirm={() => onSubmit(newKey)}
    >
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
          onChange={(event) => setNewKey(event.target.value)}
          disabled={pending}
        />
      </div>
    </DestructiveActionDialog>
  );
}
