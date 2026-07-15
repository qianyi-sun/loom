import { useId, useState } from "react";

import { Button } from "../Button";
import { Input } from "../Input";
import { Modal } from "../Modal";

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
  const confirmNameInputId = useId();
  const canSubmit = confirmName === connectionName;

  return (
    <Modal
      open
      onClose={onClose}
      title="Delete connection"
      description="Soft-delete this connection while preserving existing trial attribution for billing and audit."
      size="sm"
      footer={
        <>
          <Button onClick={onClose}>Cancel</Button>
          <Button
            variant="danger"
            onClick={onSubmit}
            disabled={!canSubmit || pending}
          >
            Delete
          </Button>
        </>
      }
    >
      <div className="space-y-4">
        <p className="text-sm text-slate-600">
          Existing trial records keep their <code>provider_connection_id</code>{" "}
          attribution. The connection becomes hidden from your team and cannot
          be used for new trials, but in-flight calls complete normally. The
          encrypted key is retained until Phase 5 cleanup.
        </p>
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
